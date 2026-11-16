"""层级目录锁管理器。

资源路径为 (database, table, partition) 前缀树，锁模式 IS/IX/S/SIX/X
遵循 CONTRACT.md 的标准多粒度兼容表。职责：

* 意向锁：子 S 自动取祖先 IS，子 X 自动取祖先 IX，调用方无法绕过层级；
* 重入与幂等：同事务锁可重入，同 request_id 重试不增加持有句柄；
* 公平等待：每个资源一条 FIFO 队列，新请求不能越过队首冲突写者；
* 原子升级：升级等待期间保留原锁，授予后才切换，绝无“先放再抢”窗口；
* 死锁解脱：按等待图找实际等待环，选持有句柄最少的事务为牺牲者，
  牺牲者在自己的线程里抛 DeadlockError，由现有回滚入口释放资源；
* 取消语义：取消等待只移除排队项，不动已经持有的保护锁。

仅关注进程内并发，持久恢复由现有提交层承担。
"""
from collections import deque
from dataclasses import dataclass, field
from copy import deepcopy
import threading
import time
import uuid

from .model import resource

IS, IX, S, SIX, X = 'IS', 'IX', 'S', 'SIX', 'X'
_MODES = (IS, IX, S, SIX, X)

# 兼容表：held 模式下，其他事务哪些 requested 模式可以同时授予。
COMPATIBILITY = {
    IS: {IS, IX, S, SIX},
    IX: {IS, IX},
    S: {IS, S},
    SIX: {IS},
    X: set(),
}

# 锁模式转换表：同一事务在同一资源上叠加两种模式后的最终占用。
# IX + S = SIX，任一遇到 X 即 X。
UNION = {
    IS: {IS: IS, IX: IX, S: S, SIX: SIX, X: X},
    IX: {IS: IX, IX: IX, S: SIX, SIX: SIX, X: X},
    S: {IS: S, IX: SIX, S: S, SIX: SIX, X: X},
    SIX: {IS: SIX, IX: SIX, S: SIX, SIX: SIX, X: X},
    X: {IS: X, IX: X, S: X, SIX: X, X: X},
}

# 后代目标模式对应的祖先意向。
INTENTION = {IS: IS, S: IS, IX: IX, SIX: IX, X: IX}


class LockError(RuntimeError):
    """锁相关错误基类。"""


class LockTimeout(LockError):
    """等待超时，排队项已移除，已持有锁不受影响。"""


class LockCancelled(LockError):
    """等待被显式取消，已持有锁不受影响。"""


class DeadlockError(LockError):
    """检测到等待环，当前事务被选为牺牲者。

    cycle 为实际等待环，形如 [t1, t2, ..., t1]；edges 给出每条等待边
    涉及的资源与双方模式，便于目录层定位是哪些表/分区互相卡住。
    """

    def __init__(self, cycle, edges, victim):
        self.cycle = list(cycle)
        self.edges = list(edges)
        self.victim = victim
        chain = ' -> '.join(self.cycle)
        detail = '; '.join(
            f"{e['waiter']} 等 {e['resource']} 的 {e['wanted']}，"
            f"{e['holder']} 持 {e['held']}"
            for e in self.edges
        )
        super().__init__(f"死锁等待环 {chain}，牺牲事务 {victim}：{detail}")


@dataclass
class _Waiter:
    transaction: str
    request_id: str
    res: tuple
    wanted: str
    granted: bool = False
    canceled: bool = False
    victim: bool = False
    cycle: object = None
    enqueued_at: float = field(default_factory=time.monotonic)


class _ResourceState:
    __slots__ = ('holders', 'waiters')

    def __init__(self):
        # transaction -> [有效模式, {request_id: 该句柄模式}]
        self.holders = {}
        self.waiters = deque()


def new_request_id(transaction):
    return f"{transaction}-{uuid.uuid4().hex[:12]}"


class LockManager:
    """进程内层级锁管理器，所有方法线程安全。"""

    def __init__(self, state_of):
        # state_of(transaction) 返回事务状态；未知事务视为已不存在。
        self._state_of = state_of
        self._cond = threading.Condition(threading.RLock())
        self._resources = {}
        # request_id -> 已完成表级升级的摘要，保证升级重试幂等。
        self._escalations = {}

    # ---------------------------------------------------------------- 内部工具

    def _state(self, res):
        st = self._resources.get(res)
        if st is None:
            st = _ResourceState()
            self._resources[res] = st
        return st

    def _grantable(self, st, waiter):
        """除等待者自身外，wanted 与所有持有者兼容才可授予。"""
        for other, holder in st.holders.items():
            if other != waiter.transaction and waiter.wanted not in COMPATIBILITY[holder[0]]:
                return False
        return True

    def _apply_grant(self, st, waiter):
        holder = st.holders.get(waiter.transaction)
        if holder is None:
            holder = [waiter.wanted, {}]
            st.holders[waiter.transaction] = holder
        else:
            holder[0] = UNION[holder[0]][waiter.wanted]
        handles = holder[1]
        handles[waiter.request_id] = UNION[handles.get(waiter.request_id, IS)][waiter.wanted]
        waiter.granted = True

    def _pump(self):
        """逐个资源按 FIFO 推进队列；队首不通则其后一律等待（防插队）。"""
        changed = True
        while changed:
            changed = False
            for st in self._resources.values():
                queue = st.waiters
                while queue:
                    waiter = queue[0]
                    if waiter.granted:
                        queue.popleft()
                        changed = True
                        continue
                    if waiter.canceled or waiter.victim:
                        queue.popleft()
                        changed = True
                        continue
                    if self._state_of(waiter.transaction) != 'active':
                        waiter.canceled = True
                        queue.popleft()
                        changed = True
                        continue
                    if self._grantable(st, waiter):
                        self._apply_grant(st, waiter)
                        queue.popleft()
                        changed = True
                    else:
                        break

    def _wait_for_graph(self):
        edges = []
        adjacency = {}
        for res, st in self._resources.items():
            for waiter in st.waiters:
                if waiter.granted or waiter.canceled or waiter.victim:
                    continue
                for other, holder in st.holders.items():
                    # 跳过自己：同事务升级时它同时是旧锁持有者与等待者，
                    # 自等待不构成死锁环。
                    if other == waiter.transaction:
                        continue
                    if waiter.wanted not in COMPATIBILITY[holder[0]]:
                        adjacency.setdefault(waiter.transaction, []).append(other)
                        edges.append({
                            'waiter': waiter.transaction,
                            'holder': other,
                            'resource': res,
                            'wanted': waiter.wanted,
                            'held': holder[0],
                        })
        return adjacency, edges

    def _find_cycle(self):
        adjacency, edges = self._wait_for_graph()
        colors = {}
        stack = []

        def visit(node):
            colors[node] = 1
            stack.append(node)
            for nxt in adjacency.get(node, ()):
                mark = colors.get(nxt, 0)
                if mark == 1:
                    index = stack.index(nxt)
                    return stack[index:] + [nxt]
                if mark == 0:
                    found = visit(nxt)
                    if found:
                        return found
            stack.pop()
            colors[node] = 2
            return None

        for node in list(adjacency):
            if colors.get(node, 0) == 0:
                cycle = visit(node)
                if cycle:
                    ring = set(cycle[:-1])
                    ring_edges = [e for e in edges
                                  if e['waiter'] in ring and e['holder'] in ring]
                    return cycle, ring_edges
        return None, []

    def _handle_count(self, transaction):
        return sum(len(st.holders[transaction][1])
                   for st in self._resources.values()
                   if transaction in st.holders)

    def _choose_victim(self, cycle):
        ring = cycle[:-1]
        return min(ring, key=lambda tx: (self._handle_count(tx), tx))

    def _ensure(self, transaction, res, mode, request_id, deadline):
        """在单个资源上取得 mode，必要时排队等待，调用时已持锁。"""
        st = self._state(res)
        # 只有本次调用新入队（或合入新等待项）时才需要跑死锁检测：
        # 等待环只会在新增等待边的那一刻形成，被唤醒后只需 pump 复查。
        check_deadlock = False
        existing = next((w for w in st.waiters
                         if w.transaction == transaction and w.request_id == request_id), None)
        if existing is not None:
            # 同一请求重入等待：合入更强意愿，绝不新增排队项。
            existing.wanted = UNION[existing.wanted][mode]
            waiter = existing
            check_deadlock = True
        else:
            holder = st.holders.get(transaction)
            if holder is not None and request_id in holder[1]:
                # 已授予的同 request_id 重试：幂等，占用数不变。
                return
            wanted = UNION[holder[0]][mode] if holder is not None else mode
            if holder is not None and wanted == holder[0]:
                # 请求模式已被自己的有效模式覆盖（如 IX 上再要 IX/IS）：
                # 仅新增一个冗余句柄，与任何外人都不冲突，不受队列影响。
                # 注意 IX 上要 S 时 wanted=SIX，不属于此分支，必须排队，
                # 因为 IX 与 S 对其他事务并不兼容。
                holder[1][request_id] = mode
                return
            others_ahead = any(w.transaction != transaction for w in st.waiters)
            if not others_ahead and self._grantable(st, _Waiter(transaction, request_id, res, wanted)):
                self._apply_grant(st, _Waiter(transaction, request_id, res, wanted))
                return
            own_waiter = next((w for w in st.waiters if w.transaction == transaction), None)
            if own_waiter is not None:
                # 同事务在该资源已有排队项，并入队伍原位置。
                own_waiter.wanted = UNION[own_waiter.wanted][wanted]
                waiter = own_waiter
                check_deadlock = True
            else:
                waiter = _Waiter(transaction, request_id, res, wanted)
                st.waiters.append(waiter)
                check_deadlock = True

        while True:
            self._pump()
            if waiter.granted:
                return
            if waiter.canceled or self._state_of(transaction) != 'active':
                raise LockCancelled(f"事务 {transaction} 的等待 {request_id} 已取消")
            if waiter.victim:
                raise DeadlockError(waiter.cycle[0], waiter.cycle[1], transaction)

            # 环只可能由本次新等待边引入；唤醒后的后续迭代只等通知，
            # 避免每个等待者在每次空转时都全图扫描。
            if check_deadlock:
                check_deadlock = False
                cycle, ring_edges = self._find_cycle()
                if cycle:
                    victim = self._choose_victim(cycle)
                    for other_st in self._resources.values():
                        for other_waiter in other_st.waiters:
                            if other_waiter.transaction == victim and not other_waiter.granted:
                                other_waiter.victim = True
                                other_waiter.cycle = (cycle, ring_edges)
                    self._cond.notify_all()
                    if victim == transaction:
                        raise DeadlockError(cycle, ring_edges, victim)
                    # 本事务不是牺牲者：等待牺牲者回滚释放后被唤醒。

            if deadline is None:
                self._cond.wait()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    waiter.canceled = True
                    self._pump()
                    self._cond.notify_all()
                    raise LockTimeout(
                        f"事务 {transaction} 等待 {res} 的 {mode} 超时")
                self._cond.wait(remaining)

    # ---------------------------------------------------------------- 对外接口

    def acquire(self, transaction, path, mode, request_id=None, timeout=None):
        """按层级取得资源锁：先逐代祖先意向，再取目标模式。"""
        if mode not in _MODES:
            raise ValueError(f'未知锁模式 {mode}')
        key = resource(path)
        if self._state_of(transaction) != 'active':
            raise ValueError('事务不处于活动状态')
        request_id = request_id or new_request_id(transaction)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            try:
                for depth in range(1, len(key)):
                    self._ensure(transaction, key[:depth], INTENTION[mode],
                                 request_id, deadline)
                self._ensure(transaction, key, mode, request_id, deadline)
            except Exception:
                # 本次申请失败：清掉它沿途已拿到的句柄，别的请求的保护不动。
                self._release_request(transaction, request_id)
                self._pump()
                self._cond.notify_all()
                raise
            return request_id

    def _release_request(self, transaction, request_id):
        for st in self._resources.values():
            holder = st.holders.get(transaction)
            if holder is None or request_id not in holder[1]:
                continue
            del holder[1][request_id]
            if not holder[1]:
                del st.holders[transaction]
            else:
                # 释放句柄可能使有效模式降级，按剩余句柄重新求并集。
                effective = IS
                for handle_mode in holder[1].values():
                    effective = UNION[effective][handle_mode]
                holder[0] = effective

    def release(self, transaction, request_id):
        """释放一个 request_id 在所有资源上的句柄，并推进等待队列。"""
        with self._cond:
            self._release_request(transaction, request_id)
            self._pump()
            self._cond.notify_all()

    def release_all(self, transaction):
        """事务终结时释放全部持有与排队项（提交/回滚完成后调用）。"""
        with self._cond:
            for st in self._resources.values():
                st.holders.pop(transaction, None)
                for waiter in st.waiters:
                    if waiter.transaction == transaction:
                        waiter.canceled = True
            self._escalations = {
                rid: summary for rid, summary in self._escalations.items()
                if not rid.startswith(f"{transaction}-")
            }
            self._pump()
            self._cond.notify_all()

    def cancel(self, transaction, request_id):
        """取消尚未授予的等待，已持有的句柄原样保留。"""
        with self._cond:
            found = False
            for st in self._resources.values():
                for waiter in st.waiters:
                    if (waiter.transaction == transaction
                            and waiter.request_id == request_id
                            and not waiter.granted):
                        waiter.canceled = True
                        found = True
            self._pump()
            self._cond.notify_all()
            return found

    def escalate(self, transaction, table_path, request_id=None, mode=None, timeout=None):
        """把表下多个分区细粒度占用升级为表级占用。

        升级等待期间分区锁一把都不松；表级锁授予后才吸收并释放子句柄，
        因此占用数只减不增。同一 request_id 重试直接返回首次摘要。
        """
        table = resource(table_path)
        if len(table) != 2:
            raise ValueError('表级升级的路径必须是 (database, table)')
        request_id = request_id or new_request_id(transaction)
        with self._cond:
            if request_id in self._escalations:
                return deepcopy(self._escalations[request_id])
            children = sorted(
                res for res, st in self._resources.items()
                if len(res) == 3 and res[:2] == table and transaction in st.holders
            )
            if not children and mode is None:
                raise ValueError('该事务在表下没有可升级的分区占用')
            wanted = mode
            if wanted is None:
                wanted = IS
                for res in children:
                    wanted = UNION[wanted][self._resources[res].holders[transaction][0]]
                # 分区 S/ X 升级到表级对应 S / X；混合取 X。
                wanted = S if wanted == S else X
            deadline = None if timeout is None else time.monotonic() + timeout
            try:
                self._ensure(transaction, table, wanted, request_id, deadline)
            except Exception:
                # 升级失败：回收该 request 沿途已拿到的句柄，
                # 分区上的原细粒度锁一把都不能动（升级本就不应脱手）。
                self._release_request(transaction, request_id)
                self._pump()
                self._cond.notify_all()
                raise

            # 吸收子分区占用：连同这些 request 在祖先路径上的意向句柄
            # 一起收走，否则表上会残留已失去意义的 IX 句柄，占用数只减不增。
            absorbed_requests = set()
            absorbed = []
            for res in children:
                holder = self._resources[res].holders.pop(transaction)
                absorbed_requests.update(holder[1])
                absorbed.append({'resource': list(res), 'handles': sorted(holder[1])})
            if absorbed_requests:
                for res, other_st in tuple(self._resources.items()):
                    # 清理被吸收 request 在表节点及其祖先路径上的意向句柄；
                    # 分区节点已在上面 pop，其他子树不动。升级 request 自身
                    # 不在 absorbed_requests 中，其新表级句柄保留。
                    if len(res) > len(table) or table[:len(res)] != res:
                        continue
                    holder = other_st.holders.get(transaction)
                    if holder is None:
                        continue
                    for rid in absorbed_requests:
                        holder[1].pop(rid, None)
                    if not holder[1]:
                        del other_st.holders[transaction]
                    else:
                        effective = IS
                        for handle_mode in holder[1].values():
                            effective = UNION[effective][handle_mode]
                        holder[0] = effective
            summary = {
                'request_id': request_id,
                'table': list(table),
                'mode': wanted,
                'absorbed': absorbed,
            }
            self._escalations[request_id] = deepcopy(summary)
            self._pump()
            self._cond.notify_all()
            return deepcopy(summary)

    # ---------------------------------------------------------------- 诊断视图

    def snapshot(self):
        """返回当前谁占用、谁排队的静态副本，供目录层与测试观察。"""
        with self._cond:
            result = {}
            for res, st in sorted(self._resources.items()):
                if not st.holders and not st.waiters:
                    continue
                result[res] = {
                    'holders': [
                        {
                            'transaction': tx,
                            'mode': holder[0],
                            'handles': sorted(holder[1].items()),
                        }
                        for tx, holder in sorted(st.holders.items())
                    ],
                    'waiters': [
                        {
                            'transaction': waiter.transaction,
                            'request_id': waiter.request_id,
                            'mode': waiter.wanted,
                        }
                        for waiter in st.waiters
                    ],
                }
            return result

    def wait_for_graph(self):
        """返回等待边与当前等待环，死锁排查用。"""
        with self._cond:
            adjacency, edges = self._wait_for_graph()
            colors = {}
            stack = []
            cycle = None

            def visit(node):
                nonlocal cycle
                colors[node] = 1
                stack.append(node)
                for nxt in adjacency.get(node, ()):
                    mark = colors.get(nxt, 0)
                    if mark == 1:
                        index = stack.index(nxt)
                        cycle = stack[index:] + [nxt]
                        return True
                    if mark == 0 and visit(nxt):
                        return True
                stack.pop()
                colors[node] = 2
                return False

            for node in list(adjacency):
                if colors.get(node, 0) == 0 and visit(node):
                    break
            return {'edges': edges, 'cycle': cycle}
