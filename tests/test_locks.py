"""层级锁语义与并发测试。

覆盖：兼容表与意向路径、重入与 request_id 幂等、FIFO 写者公平、
升级原子性（等待期间原锁不脱手）、死锁等待环与牺牲者回滚时序、
取消语义、细粒度到表级升级、并发压力，以及与 SegmentStore 的端到端。
"""
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from catalog_locks.locks import (
    LockManager, DeadlockError, LockTimeout, LockCancelled,
    IS, IX, S, SIX, X,
)
from catalog_locks.operations import Catalog
from catalog_locks.model import resource


def tx_state(tx_set, identity):
    return 'active' if identity in tx_set else 'aborted'


def wait_until(predicate, timeout=5.0):
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    raise AssertionError('等待条件超时')


def make_lm(txs):
    lm = LockManager(lambda tx: tx_state(txs, tx))
    return lm


class CompatibilityTests(unittest.TestCase):
    def test_compatibility_matrix(self):
        lm = make_lm({'a', 'b', 'c', 'd'})
        # 库级 IS 与所有人兼容：a=IS, b=IX 可共存
        lm.acquire('a', ('db',), IS, request_id='a-is')
        lm.acquire('b', ('db',), IX, request_id='b-ix')
        # IS 读者 c 仍可进入
        lm.acquire('c', ('db',), IS, request_id='c-is')
        snap = lm.snapshot()[('db',)]
        modes = {h['transaction']: h['mode'] for h in snap['holders']}
        self.assertEqual(modes, {'a': IS, 'b': IX, 'c': IS})
        # S 与 IX 不兼容：d 的 S 必须等待
        with self.assertRaises(LockTimeout):
            lm.acquire('d', ('db',), S, request_id='d-s', timeout=0.1)
        # b 释放 IX 后 d 的 S 立即可取
        lm.release('b', 'b-ix')
        lm.acquire('d', ('db',), S, request_id='d-s', timeout=1)
        self.assertEqual(
            next(h for h in lm.snapshot()[('db',)]['holders']
                 if h['transaction'] == 'd')['mode'], S)

    def test_x_blocks_everything(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't'), X)
        with self.assertRaises(LockTimeout):
            lm.acquire('b', ('db', 't'), IS, timeout=0.05)

    def test_six_only_compatible_with_is(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't'), S)
        lm.acquire('a', ('db', 't'), IX, request_id='r2')  # 叠加成 SIX
        self.assertEqual(lm.snapshot()[('db', 't')]['holders'][0]['mode'], SIX)
        with self.assertRaises(LockTimeout):
            lm.acquire('b', ('db', 't'), S, timeout=0.05)
        lm.acquire('b', ('db', 't'), IS, timeout=0.05)  # IS 仍可通过

    def test_intention_path_auto_acquired(self):
        lm = make_lm({'a'})
        lm.acquire('a', ('db', 't', 'p1'), X)
        snap = lm.snapshot()
        self.assertEqual(snap[('db',)]['holders'][0]['mode'], IX)
        self.assertEqual(snap[('db', 't')]['holders'][0]['mode'], IX)
        self.assertEqual(snap[('db', 't', 'p1')]['holders'][0]['mode'], X)

    def test_partition_writers_dont_block_each_other(self):
        # 分区入仓与归档并发：不同分区只共享祖先 IX，互不阻塞
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't', 'p1'), X)
        lm.acquire('b', ('db', 't', 'p2'), X)
        self.assertEqual(len(lm.snapshot()[('db', 't')]['holders']), 2)


class ReentryTests(unittest.TestCase):
    def test_same_transaction_reentrant(self):
        lm = make_lm({'a'})
        lm.acquire('a', ('db', 't'), S, request_id='r1')
        lm.acquire('a', ('db', 't'), S, request_id='r2')
        holder = lm.snapshot()[('db', 't')]['holders'][0]
        self.assertEqual(holder['mode'], S)
        self.assertEqual(len(holder['handles']), 2)

    def test_idempotent_request_id_no_handle_inflation(self):
        lm = make_lm({'a'})
        lm.acquire('a', ('db', 't', 'p'), X, request_id='req-1')
        # 同 request_id 任意重试，包括祖先意向路径，都不增加占用
        for _ in range(5):
            lm.acquire('a', ('db', 't', 'p'), X, request_id='req-1')
        snap = lm.snapshot()
        for res in (('db',), ('db', 't'), ('db', 't', 'p')):
            self.assertEqual(len(snap[res]['holders'][0]['handles']), 1)

    def test_retry_after_failure_does_not_hold_partial(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't'), X)
        with self.assertRaises(LockTimeout):
            lm.acquire('b', ('db', 't', 'p'), X, request_id='fail', timeout=0.05)
        # 失败后 b 不应残留在任何资源（含祖先 IS）
        for state in lm.snapshot().values():
            self.assertTrue(all(h['transaction'] != 'b' for h in state['holders']))

    def test_repeated_waiting_request_does_not_duplicate_queue(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't'), X, request_id='a1')
        done = threading.Event()
        box = {}

        def waiter():
            # 同事务、同 request_id：第一次超时，第二次继续等，
            # 整个过程中该 request 在任何资源上至多产生一个排队项
            try:
                lm.acquire('b', ('db', 't'), X, request_id='w1', timeout=0.05)
            except LockTimeout:
                box['first'] = 'timeout'
            try:
                lm.acquire('b', ('db', 't'), X, request_id='w1', timeout=5)
                box['second'] = 'granted'
            except Exception as exc:  # pragma: no cover
                box['second'] = repr(exc)
            done.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        # 第二次申请入队后，整个目录里 b 只有一个等待项（重试不产生重复）
        wait_until(lambda: any(
            w['transaction'] == 'b'
            for st in lm.snapshot().values() for w in st['waiters']))
        self.assertEqual(
            sum(1 for st in lm.snapshot().values() for w in st['waiters']
                if w['transaction'] == 'b'), 1)
        # 目标资源上 b 尚未被授予；它合法持有祖先 ('db',) 的 IX 意向
        target_holders = lm.snapshot()[('db', 't')]['holders']
        self.assertTrue(all(h['transaction'] != 'b' for h in target_holders))
        ancestor = next(h for h in lm.snapshot()[('db',)]['holders']
                        if h['transaction'] == 'b')
        self.assertEqual(ancestor['mode'], IX)
        a_handle = lm.snapshot()[('db', 't')]['holders'][0]['handles'][0][0]
        lm.release('a', a_handle)
        wait_until(done.is_set, timeout=2)
        thread.join(timeout=2)
        self.assertEqual(box.get('second'), 'granted')
        # 最终 b 只持有一个句柄，重试没有凭空增加占用数
        holder = next(h for h in lm.snapshot()[('db', 't')]['holders']
                      if h['transaction'] == 'b')
        self.assertEqual(len(holder['handles']), 1)
        lm.release_all('b')
        self.assertTrue(done.is_set())


class FairnessTests(unittest.TestCase):
    def test_new_reader_cannot_overtake_waiting_writer(self):
        lm = make_lm({'w', 'r1', 'r2'})
        lm.acquire('r1', ('db', 't'), S, request_id='r1')
        writer_done = threading.Event()
        reader_started = threading.Event()
        reader_done = threading.Event()

        def writer():
            lm.acquire('w', ('db', 't'), X, request_id='w', timeout=10)
            lm.release('w', 'w')  # 写完即释放，后续读者才能被授予
            writer_done.set()

        def reader():
            reader_started.set()
            lm.acquire('r2', ('db', 't'), S, request_id='r2', timeout=10)
            reader_done.set()

        tw = threading.Thread(target=writer)
        tr = threading.Thread(target=reader)
        tw.start()
        # 写者先排队（r1 的 S 挡住 X）
        wait_until(lambda: len(lm.snapshot()[('db', 't')]['waiters']) == 1)
        tr.start()
        reader_started.wait(5)
        wait_until(lambda: len(lm.snapshot()[('db', 't')]['waiters']) == 2)
        # 队首写者还没过去，后来的读者即使与当前 S 兼容也必须等
        self.assertFalse(reader_done.wait(0.2))
        r1_handle = next(
            h['handles'][0][0] for h in lm.snapshot()[('db', 't')]['holders']
            if h['transaction'] == 'r1')
        lm.release('r1', r1_handle)
        wait_until(writer_done.is_set)
        # 写者完成后读者才轮到
        wait_until(reader_done.is_set)
        self.assertTrue(writer_done.is_set())
        tw.join(2); tr.join(2)

    def test_queued_writer_gets_turn_under_read_stream(self):
        # 连续读者流不能饿死已排队的写者
        lm = make_lm({'w'} | {f'r{i}' for i in range(6)})
        lm.acquire('r0', ('db', 't'), S, request_id='r0')
        writer_done = threading.Event()

        def writer():
            lm.acquire('w', ('db', 't'), X, request_id='w')
            writer_done.set()

        tw = threading.Thread(target=writer)
        tw.start()
        wait_until(lambda: lm.snapshot()[('db', 't')]['waiters'])
        results = []

        def reader(i):
            try:
                lm.acquire(f'r{i}', ('db', 't'), S, request_id=f'r{i}', timeout=1.5)
                results.append(i)
            except LockTimeout:
                results.append(f't{i}')

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(reader, i) for i in range(1, 6)]
            for f in futures:
                f.result()
        lm.release('r0', 'r0')
        wait_until(writer_done.is_set)
        tw.join(2)
        self.assertTrue(writer_done.is_set())


class UpgradeAtomicityTests(unittest.TestCase):
    def test_upgrade_keeps_old_lock_until_granted(self):
        c = Catalog()
        c.begin('a'); c.begin('b')
        c.stage('a', ['db', 't', 'p'], {'v': 1}, request_id='a1')  # a 持分区 IX
        c.stage('b', ['db', 't', 'p'], {'v': 2}, request_id='b1', )
        # b 也持同分区 IX（IX/IX 兼容）；a 想升级到分区 X，必须等 b
        upgraded = threading.Event()

        def do_upgrade():
            c.locks.acquire('a', ['db', 't', 'p'], X, request_id='a-up')
            upgraded.set()

        t = threading.Thread(target=do_upgrade)
        t.start()
        wait_until(lambda: c.lock_snapshot()[('db', 't', 'p')]['waiters'])
        snap = c.lock_snapshot()[('db', 't', 'p')]
        # 等待期间 a 仍是 IX 持有者：原锁没有被放掉
        holder_a = next(h for h in snap['holders'] if h['transaction'] == 'a')
        self.assertEqual(holder_a['mode'], IX)
        c.locks.release('b', 'b1')
        wait_until(upgraded.is_set)
        t.join(2)
        self.assertEqual(
            c.lock_snapshot()[('db', 't', 'p')]['holders'][0]['mode'], X)

    def test_six_upgrade_via_s_on_ix(self):
        lm = make_lm({'a'})
        lm.acquire('a', ('db', 't'), IX, request_id='ix')
        lm.acquire('a', ('db', 't'), S, request_id='s')
        self.assertEqual(lm.snapshot()[('db', 't')]['holders'][0]['mode'], SIX)


class DeadlockTests(unittest.TestCase):
    def test_simultaneous_upgrades_form_visible_cycle(self):
        c = Catalog()
        c.begin('a'); c.begin('b')
        # a 持 p1 IX，b 持 p2 IX；各自升级到对方占用分区的 X
        c.stage('a', ['db', 't', 'p1'], 1, request_id='a1')
        c.stage('b', ['db', 't', 'p2'], 2, request_id='b1')
        barrier = threading.Barrier(2)
        errors = {}
        rollback_paused = threading.Event()
        rollback_resume = threading.Event()
        grant_during_rollback = []

        def workload(tx, other_held_partition, rid):
            try:
                barrier.wait()
                c.locks.acquire(tx, ['db', 't', other_held_partition], X,
                                request_id=rid, timeout=10)
            except DeadlockError as exc:
                errors[tx] = exc
                # 牺牲者必须走现有回滚入口，并在真正释放前停住
                steps = c.rollback_steps(tx)
                next(steps)
                rollback_paused.set()
                rollback_resume.wait(5)
                list(steps)

        ta = threading.Thread(target=workload, args=('a', 'p2', 'a-up'))
        tb = threading.Thread(target=workload, args=('b', 'p1', 'b-up'))
        ta.start(); tb.start()

        wait_until(lambda: len(errors) == 1, timeout=5)
        victim = next(iter(errors))
        exc = errors[victim]
        # 能看到实际等待环：两个事务都在环上
        self.assertEqual(set(exc.cycle), {'a', 'b'})
        self.assertTrue(all(e['resource'][0] == 'db' for e in exc.edges))
        # 牺牲者字典序更小者（句柄数相同）
        self.assertEqual(victim, 'a')

        # 回滚未完成前，锁不能授给别人：牺牲者的锁仍在目录里
        rollback_paused.wait(5)
        for res, state in c.lock_snapshot().items():
            for h in state['holders']:
                if h['transaction'] == victim:
                    grant_during_rollback.append(res)
        self.assertTrue(grant_during_rollback)  # 牺牲者锁仍在
        rollback_resume.set()
        ta.join(5); tb.join(5)
        # 回滚完成后牺牲者全部释放，幸存者拿到 X
        remaining = c.lock_snapshot()
        for state in remaining.values():
            self.assertTrue(all(h['transaction'] != victim for h in state['holders']))
        survivor = 'b'
        p = ('db', 't', 'p1')
        self.assertEqual(
            next(h for h in remaining[p]['holders'] if h['transaction'] == survivor)['mode'], X)
        self.assertEqual(c.transactions[victim].state, 'aborted')

    def test_wait_for_graph_reports_edges(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't', 'p1'), X, request_id='a')
        lm.acquire('b', ('db', 't', 'p2'), X, request_id='b')
        enqueued = threading.Event()

        def waiter():
            try:
                lm.acquire('b', ('db', 't', 'p1'), X,
                          request_id='b2', timeout=5)
            except (LockTimeout, DeadlockError, LockCancelled):
                pass

        t = threading.Thread(target=waiter)
        t.start()
        wait_until(lambda: any(
            e['waiter'] == 'b' and e['holder'] == 'a'
            for e in lm.wait_for_graph()['edges']))
        graph = lm.wait_for_graph()
        self.assertTrue(any(e['waiter'] == 'b' and e['holder'] == 'a' for e in graph['edges']))
        lm.cancel('b', 'b2')
        t.join(2)

    def test_victim_prefers_fewer_handles(self):
        lm = make_lm({'a', 'bb'})
        # bb 持有更多句柄，成环时应牺牲句柄少的 a
        lm.acquire('bb', ('db', 't', 'p1'), X, request_id='bb1')
        lm.acquire('bb', ('db', 't', 'p9'), X, request_id='bb9')
        lm.acquire('a', ('db', 't', 'p2'), X, request_id='a1')
        errors = {}

        def cross(waiter, target, rid):
            try:
                lm.acquire(waiter, ('db', 't', target), X,
                          request_id=rid, timeout=5)
            except DeadlockError as exc:
                errors[waiter] = exc
            except (LockCancelled, LockTimeout):
                # 测试收尾释放时，幸存等待者可能随资源清理被取消
                errors.setdefault(waiter, None)

        t1 = threading.Thread(target=cross, args=('bb', 'p2', 'bb-up'))
        t2 = threading.Thread(target=cross, args=('a', 'p1', 'a-up'))
        t1.start(); t2.start()
        wait_until(lambda: len(errors) == 1, timeout=5)
        self.assertIn('a', errors)
        t1.join(2); t2.join(2)
        lm.release_all('bb'); lm.release_all('a')


class CancelTests(unittest.TestCase):
    def test_cancel_wait_keeps_held_locks(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't'), X, request_id='a')
        lm.acquire('b', ('db', 'other'), S, request_id='b-held')
        done = threading.Event()

        def wait_then_cancel():
            try:
                lm.acquire('b', ('db', 't'), X, request_id='b-wait', timeout=10)
            except (LockCancelled, LockTimeout):
                done.set()

        t = threading.Thread(target=wait_then_cancel)
        t.start()
        wait_until(lambda: lm.snapshot()[('db', 't')]['waiters'])
        self.assertTrue(lm.cancel('b', 'b-wait'))
        wait_until(done.is_set)
        t.join(2)
        # 已持有的 S 原封不动
        self.assertEqual(
            lm.snapshot()[('db', 'other')]['holders'][0]['mode'], S)

    def test_timeout_does_not_release_held(self):
        lm = make_lm({'a', 'b'})
        # b 在 t1 正常持有 X（含祖先 IX），a 在独立的 t2 持 X
        lm.acquire('b', ('db', 't1'), X, request_id='b-held')
        lm.acquire('a', ('db', 't2'), X, request_id='a-x')
        # b 申请 t2 的 X 被 a 挡住而超时；它在 t1 上已持有的锁原样保留
        with self.assertRaises(LockTimeout):
            lm.acquire('b', ('db', 't2'), X, request_id='b-up', timeout=0.1)
        holder = lm.snapshot()[('db', 't1')]['holders'][0]
        self.assertEqual(holder['transaction'], 'b')
        self.assertEqual(holder['mode'], X)
        lm.release('a', 'a-x')
        lm.release_all('b')


class EscalationTests(unittest.TestCase):
    def test_partition_escalation_to_table_reduces_occupancy(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't', 'p1'), X, request_id='r1')
        lm.acquire('a', ('db', 't', 'p2'), X, request_id='r2')
        lm.acquire('a', ('db', 't', 'p3'), X, request_id='r3')
        summary = lm.escalate('a', ('db', 't'), request_id='esc')
        self.assertEqual(summary['mode'], X)
        self.assertEqual(len(summary['absorbed']), 3)
        snap = lm.snapshot()
        # 三个分区句柄被吸收，表级只有一个升级句柄
        for p in ('p1', 'p2', 'p3'):
            self.assertNotIn(('db', 't', p), snap)
        self.assertEqual(
            snap[('db', 't')]['holders'][0]['handles'], [('esc', X)])
        # 升级后别人无法再拿表下任何锁
        with self.assertRaises(LockTimeout):
            lm.acquire('b', ('db', 't', 'p4'), X, timeout=0.05)

    def test_escalation_is_idempotent_on_retry(self):
        lm = make_lm({'a'})
        lm.acquire('a', ('db', 't', 'p1'), X, request_id='r1')
        first = lm.escalate('a', ('db', 't'), request_id='esc')
        second = lm.escalate('a', ('db', 't'), request_id='esc')
        self.assertEqual(first, second)
        self.assertEqual(
            len(lm.snapshot()[('db', 't')]['holders'][0]['handles']), 1)

    def test_escalation_blocks_without_dropping_partition_locks(self):
        c = Catalog()
        c.begin('a'); c.begin('b')
        c.stage('a', ['db', 't', 'p1'], 1, request_id='a1')
        c.stage('a', ['db', 't', 'p2'], 2, request_id='a2')
        # b 在表级持 IS：与 a 的 IX 兼容可共存，但 X 与 IS 冲突，
        # 因此能挡住 a 的表级 X 升级，又不会让 b 自己先阻塞
        c.locks.acquire('b', ['db', 't'], IS, request_id='b-is')
        finished = threading.Event()
        summary_box = {}

        def do_escalate():
            summary_box['s'] = c.escalate('a', ['db', 't'], request_id='esc', timeout=10)
            finished.set()

        t = threading.Thread(target=do_escalate)
        t.start()
        wait_until(lambda: c.lock_snapshot()[('db', 't')]['waiters'])
        # 等待期间分区锁仍在，数据保护未出现空窗
        for p in ('p1', 'p2'):
            holders = c.lock_snapshot()[('db', 't', p)]['holders']
            self.assertTrue(any(h['transaction'] == 'a' for h in holders))
        c.locks.release('b', 'b-is')
        wait_until(finished.is_set)
        t.join(2)
        self.assertEqual(summary_box['s']['mode'], X)

    def test_failed_escalation_keeps_partition_locks_and_clears_residue(self):
        lm = make_lm({'a', 'b'})
        lm.acquire('a', ('db', 't', 'p1'), X, request_id='a1')
        lm.acquire('a', ('db', 't', 'p2'), X, request_id='a2')
        # b 的表级 IS 挡住 a 的表级 X 升级（X 与 IS 冲突，IX 与 IS 兼容）
        lm.acquire('b', ('db', 't'), IS, request_id='b-is')
        with self.assertRaises(LockTimeout):
            lm.escalate('a', ('db', 't'), request_id='esc-fail', timeout=0.1)
        snap = lm.snapshot()
        # 升级 request 的残留句柄被全部回收
        for state in snap.values():
            for h in state['holders']:
                self.assertNotIn('esc-fail', dict(h['handles']))
        # 两个分区的原 X 锁完好，保护没有空窗
        for p in ('p1', 'p2'):
            holder = snap[('db', 't', p)]['holders'][0]
            self.assertEqual(holder['transaction'], 'a')
            self.assertEqual(holder['mode'], X)
        # 升级摘要未登记，允许后续用新 request 重新升级
        self.assertNotIn('esc-fail', lm._escalations)
        lm.release('b', 'b-is')
        summary = lm.escalate('a', ('db', 't'), request_id='esc-ok', timeout=2)
        self.assertEqual(summary['mode'], X)


class CatalogIntegrationTests(unittest.TestCase):
    def test_operations_all_go_through_lock_path(self):
        c = Catalog()
        c.begin('a')
        c.load_partition('a', ['db', 't', 'p1'], {'rows': 10}, request_id='load')
        c.archive_partitions('a', 'db', 't',
                             [('p2', {'rows': 1}), ('p3', {'rows': 2})],
                             request_id='arch')
        c.alter_table('a', ['db', 't'], {'schema': 2}, request_id='alt')
        snap = c.lock_snapshot()
        self.assertEqual(snap[('db', 't', 'p1')]['holders'][0]['mode'], IX)
        self.assertEqual(snap[('db', 't')]['holders'][0]['mode'], X)
        c.commit('a')
        # 提交后锁全部归还
        self.assertEqual(c.lock_snapshot(), {})
        c.begin('viewer')
        self.assertEqual(c.read('viewer', ['db', 't', 'p1']), {'rows': 10})

    def test_commit_publishes_and_releases(self):
        c = Catalog()
        c.begin('a')
        c.stage('a', ['db', 't'], 3, request_id='s1')
        c.commit('a')
        c.begin('b')
        self.assertEqual(c.read('b', ['db', 't']), 3)
        self.assertEqual(c.lock_snapshot(), {})

    def test_rollback_completes_before_grant(self):
        c = Catalog()
        c.begin('a'); c.begin('b')
        c.stage('a', ['db', 't', 'p'], 1, request_id='a1')
        granted_during_undo = []
        reader_passed = threading.Event()

        def reader():
            c.locks.acquire('b', ['db', 't', 'p'], S, request_id='b1', timeout=10)
            reader_passed.set()

        t = threading.Thread(target=reader)
        t.start()
        wait_until(lambda: c.lock_snapshot()[('db', 't', 'p')]['waiters'])
        steps = c.rollback_steps('a')
        undo = next(steps)
        self.assertEqual(c.transactions['a'].state, 'rolling_back')
        # 私有写已清理但回滚生成器尚未耗尽：b 仍不能拿到锁
        self.assertFalse(reader_passed.wait(0.2))
        holders = c.lock_snapshot()[('db', 't', 'p')]['holders']
        self.assertTrue(any(h['transaction'] == 'a' for h in holders))
        list(steps)  # 跑完 abort 步骤，触发 finally 释放
        wait_until(reader_passed.is_set)
        t.join(2)
        self.assertEqual(c.transactions['a'].state, 'aborted')
        self.assertEqual(c.lock_snapshot().get(('db', 't', 'p'), {}).get('holders', []),
                         [{'transaction': 'b', 'mode': S,
                           'handles': [('b1', S)]}])

    def test_concurrent_mixed_workload(self):
        # 课程归档、分区入仓、结构操作同时跑：无异常退出，值一致，无死锁残留
        c = Catalog()
        n_tx = 12
        for i in range(n_tx):
            c.begin(f'tx{i}')
        errors = []

        def work(i):
            tx = f'tx{i}'
            try:
                if i % 3 == 0:
                    c.archive_partitions(tx, 'edu', 'learning',
                                         [(f'p{i}-{k}', {'rows': k}) for k in range(3)],
                                         request_id=f'{tx}-arch', timeout=15)
                elif i % 3 == 1:
                    c.load_partition(tx, ['edu', 'learning', f'load{i}'],
                                     {'rows': i}, request_id=f'{tx}-load', timeout=15)
                else:
                    c.alter_table(tx, ['edu', 'courses'], {'schema': i},
                                  request_id=f'{tx}-alt', timeout=15)
                c.commit(tx)
            except DeadlockError:
                list(c.rollback_steps(tx))
            except Exception as exc:  # pragma: no cover - 测试失败信号
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=n_tx) as pool:
            list(pool.map(work, range(n_tx)))
        self.assertEqual(errors, [])
        self.assertEqual(c.lock_snapshot(), {})
        states = {tx.state for tx in c.transactions.values()}
        self.assertTrue(states <= {'committed', 'aborted'})

    def test_end_to_end_with_segment_store(self):
        # 真实存储 + 目录锁：两条事务，一条归档写段，一条读，最终值可查
        import json
        import tempfile
        from pathlib import Path
        from catalog_locks.storage import SegmentStore
        sample = json.loads((Path(__file__).resolve().parents[1]
                             / 'data/sample.json').read_text(encoding='utf-8'))
        with tempfile.TemporaryDirectory() as tmp:
            store = SegmentStore(Path(tmp) / 'store')
            for name, rows in sample.items():
                store.write_segment(name, rows)
            c = Catalog()
            c.begin('archiver'); c.begin('reader')
            c.archive_partitions('archiver', 'education', 'learning',
                                 [('2026-12', {'rows': 7})], request_id='arc')
            # 归档未提交，读者对同分区的 S 会被挡住
            with self.assertRaises(LockTimeout):
                c.locks.acquire('reader', ['education', 'learning', '2026-12'],
                                S, request_id='rd', timeout=0.05)
            c.commit('archiver')
            value = c.read('reader', ['education', 'learning', '2026-12'])
            self.assertEqual(value, {'rows': 7})
            # 存储层扫描统计仍然真实：读一页即累计字节与页数
            scanned = list(store.scan('resources'))
            self.assertEqual(len(scanned), 3)
            self.assertGreater(store.bytes_read, 0)
            self.assertTrue(store.pages_read)


if __name__ == '__main__':
    unittest.main()
