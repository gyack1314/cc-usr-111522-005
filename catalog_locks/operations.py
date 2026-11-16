from copy import deepcopy
from .model import Transaction, resource
from .locks import LockManager, S, X, IX, new_request_id


class Catalog:
    def __init__(self):
        self.values = {}
        self.transactions = {}
        self.audit = []
        self.locks = LockManager(self._transaction_state)

    def _transaction_state(self, identity):
        tx = self.transactions.get(identity)
        return tx.state if tx is not None else 'aborted'

    def begin(self, identity):
        if identity in self.transactions:
            raise ValueError('事务身份已存在')
        tx = Transaction(identity)
        self.transactions[identity] = tx
        return tx

    # ------------------------------------------------------------ 受锁保护的目录操作

    def stage(self, identity, path, value, request_id=None, timeout=None):
        """私有写入：IX 占用目标及祖先意向，保护读目录与私有区。"""
        tx = self.transactions[identity]
        tx.require_active()
        key = resource(path)
        request_id = self.locks.acquire(identity, key, IX,
                                        request_id=request_id, timeout=timeout)
        tx.writes[key] = deepcopy(value)
        tx.events.append(('stage', key))
        return request_id

    def load_partition(self, identity, path, value, request_id=None, timeout=None):
        """分区入仓：分区级 IX 写入，路径长度必须到分区。"""
        key = resource(path)
        if len(key) != 3:
            raise ValueError('分区入仓路径必须是 (database, table, partition)')
        request_id = self.stage(identity, key, value,
                                request_id=request_id, timeout=timeout)
        return request_id

    def alter_table(self, identity, path, value, request_id=None, timeout=None):
        """结构变更：表级 X 占用，与表上任何其他事务的锁冲突。"""
        tx = self.transactions[identity]
        tx.require_active()
        key = resource(path)
        if len(key) != 2:
            raise ValueError('结构变更路径必须是 (database, table)')
        request_id = self.locks.acquire(identity, key, X,
                                        request_id=request_id, timeout=timeout)
        tx.writes[key] = deepcopy(value)
        tx.events.append(('alter', key))
        return request_id

    def archive_partitions(self, identity, database, table, entries,
                           request_id=None, timeout=None):
        """课程归档：一个事务内连续写入同表多个分区，共用一把 IX 句柄链。

        entries 为 [(partition, value), ...]；同 request_id 重试整批写入，
        锁占用数与私有条目都不会凭空翻倍。
        """
        request_id = request_id or new_request_id(identity)
        for partition, value in entries:
            self.stage(identity, [database, table, partition], value,
                       request_id=request_id, timeout=timeout)
        return request_id

    def read(self, identity, path, request_id=None,
             timeout=None, release=True):
        """受保护读取：目标 S + 祖先 IS。

        release=True（默认）读完即还，保持诊断式调用的旧行为；
        release=False 则保留读锁直到提交/回滚，用于长读事务。
        """
        tx = self.transactions[identity]
        tx.require_active()
        key = resource(path)
        own_request = request_id is None
        request_id = self.locks.acquire(
            identity, key, S, request_id=request_id, timeout=timeout)
        try:
            result = deepcopy(tx.writes[key] if key in tx.writes else self.values.get(key))
        finally:
            if release and own_request:
                self.locks.release(identity, request_id)
        return result

    def escalate(self, identity, table_path, request_id=None, mode=None, timeout=None):
        """把本事务在表下的分区占用升级为表级占用，原分区锁授予前不脱手。"""
        return self.locks.escalate(identity, table_path,
                                   request_id=request_id, mode=mode, timeout=timeout)

    def cancel_request(self, identity, request_id):
        """取消排队中的申请，已持有锁不受影响。"""
        return self.locks.cancel(identity, request_id)

    def lock_snapshot(self):
        return self.locks.snapshot()

    def wait_for_graph(self):
        return self.locks.wait_for_graph()

    # ------------------------------------------------------------ 提交与回滚

    def commit(self, identity):
        tx = self.transactions[identity]
        tx.require_active()
        try:
            self.values.update(deepcopy(tx.writes))
            tx.writes.clear()
            tx.state = 'committed'
            self.audit.append(('commit', identity))
        finally:
            # 发布完成后才放锁；release_all 同时唤醒等待者。
            self.locks.release_all(identity)

    def rollback_steps(self, identity):
        tx = self.transactions[identity]
        tx.require_active()
        tx.state = 'rolling_back'
        try:
            for key in sorted(list(tx.writes)):
                del tx.writes[key]
                self.audit.append(('undo', identity, key))
                yield ('undo', key)
            tx.state = 'aborted'
            self.audit.append(('abort', identity))
            yield ('abort', identity)
        finally:
            # 生成器耗尽（所有真实清理步骤跑完）前，锁一直留着，
            # 等待者不会在回滚中途拿到保护中的资源。
            self.locks.release_all(identity)


def describe(store):
    catalog = Catalog()
    catalog.begin('reader')
    for row in store.scan('resources'):
        catalog.values[resource(row['path'])] = row['value']
    return {'resources': [{'path': list(k), 'value': v} for k, v in sorted(catalog.values.items())], 'transaction': catalog.transactions['reader'].state}
