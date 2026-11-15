from dataclasses import dataclass,field

def resource(value):
    result=tuple(value)
    if not 1<=len(result)<=3 or any(not isinstance(v,str) or not v for v in result):raise ValueError('目录资源非法')
    return result

@dataclass
class Transaction:
    identity:str
    state:str='active'
    writes:dict=field(default_factory=dict)
    events:list=field(default_factory=list)

    def require_active(self):
        if self.state!='active':raise ValueError('事务不处于活动状态')
