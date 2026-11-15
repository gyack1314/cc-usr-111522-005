from copy import deepcopy
from .model import Transaction,resource

class Catalog:
    def __init__(self):self.values={};self.transactions={};self.audit=[]
    def begin(self,identity):
        if identity in self.transactions:raise ValueError('事务身份已存在')
        tx=Transaction(identity);self.transactions[identity]=tx;return tx
    def stage(self,identity,path,value):
        tx=self.transactions[identity];tx.require_active();key=resource(path)
        tx.writes[key]=deepcopy(value);tx.events.append(('stage',key))
    def read(self,identity,path):
        tx=self.transactions[identity];tx.require_active();key=resource(path)
        return deepcopy(tx.writes[key] if key in tx.writes else self.values.get(key))
    def commit(self,identity):
        tx=self.transactions[identity];tx.require_active()
        self.values.update(deepcopy(tx.writes));tx.writes.clear();tx.state='committed';self.audit.append(('commit',identity))
    def rollback_steps(self,identity):
        tx=self.transactions[identity];tx.require_active();tx.state='rolling_back'
        for key in sorted(list(tx.writes)):
            del tx.writes[key];self.audit.append(('undo',identity,key));yield ('undo',key)
        tx.state='aborted';self.audit.append(('abort',identity));yield ('aborted',identity)

def describe(store):
    catalog=Catalog();catalog.begin('reader')
    for row in store.scan('resources'):catalog.values[resource(row['path'])]=row['value']
    return {'resources':[{'path':list(k),'value':v} for k,v in sorted(catalog.values.items())],'transaction':catalog.transactions['reader'].state}
