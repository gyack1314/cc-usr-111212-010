"""清单解析与校验：任务定义、分区登记、目标、变更与运行参数。"""
from dataclasses import dataclass
from datetime import date
from .io import integer

def key_of(dataset,day):return f'{dataset}@{day.isoformat()}'

def parse_key(key):
    dataset,_,day=key.rpartition('@')
    if not dataset or not day:raise ValueError('实例键非法:'+key)
    return dataset,date.fromisoformat(day)

def _name(x):
    if not isinstance(x,str) or not x or x.strip()!=x or '@' in x:raise ValueError('名称非法')
    return x

def _text(x):
    if not isinstance(x,str) or not x:raise ValueError('文本缺失')
    return x

def _day(x):
    if not isinstance(x,str):raise ValueError('日期应为ISO字符串')
    return date.fromisoformat(x)

@dataclass(frozen=True)
class Upstream:
    dataset:str
    offsets:tuple  # 排序去重的整数天偏移，父日期=子日期+偏移

@dataclass(frozen=True)
class Task:
    dataset:str
    version:str
    upstreams:tuple

@dataclass(frozen=True)
class Partition:
    dataset:str
    day:date
    hash:str
    status:str  # complete|incomplete
    built_with:str|None  # 登记分区的构建任务版本，缺省视为当前任务版本

@dataclass(frozen=True)
class Change:
    dataset:str
    day:date
    new_hash:str

@dataclass(frozen=True)
class Manifest:
    tasks:dict       # dataset -> Task
    partitions:dict  # 实例键 -> Partition
    targets:tuple    # 实例键，按声明顺序
    start:date
    end:date
    changes:tuple
    lease_seconds:int
    logical_now:int
    max_instances:int
    digest:str

def parse(v,digest):
    if not isinstance(v,dict):raise ValueError('顶层资料应为对象')
    tasks={}
    for t in v['tasks']:
        ds=_name(t['dataset']);version=_text(t['version'])
        if ds in tasks:raise ValueError('任务重复:'+ds)
        ups=[]
        for u in t.get('upstreams',[]):
            uds=_name(u['dataset'])
            offs=u['offsets']
            if not isinstance(offs,list) or not offs:raise ValueError('偏移集合缺失:'+uds)
            for o in offs:
                if type(o) is not int:raise ValueError('偏移应为整数')
            ups.append(Upstream(uds,tuple(sorted(set(offs)))))
        tasks[ds]=Task(ds,version,tuple(ups))
    for t in tasks.values():
        for u in t.upstreams:
            if u.dataset not in tasks:raise ValueError('上游任务未声明:'+u.dataset)
    parts={}
    for p in v['partitions']:
        ds=_name(p['dataset']);day=_day(p['date'])
        if ds not in tasks:raise ValueError('分区任务未声明:'+ds)
        k=key_of(ds,day)
        if k in parts:raise ValueError('分区重复:'+k)
        st=p['status']
        if st not in ('complete','incomplete'):raise ValueError('分区状态未知')
        bw=p.get('built_with')
        if bw is not None:_text(bw)
        parts[k]=Partition(ds,day,_text(p['hash']),st,bw)
    targets=[]
    for t in v['targets']:
        ds=_name(t['dataset']);day=_day(t['date'])
        if ds not in tasks:raise ValueError('目标任务未声明:'+ds)
        k=key_of(ds,day)
        if k in targets:raise ValueError('目标重复:'+k)
        targets.append(k)
    ar=v['allowed_recompute'];start=_day(ar['start']);end=_day(ar['end'])
    if start>end:raise ValueError('重算范围倒置')
    changes=[];seen=set()
    for c in v.get('changed',[]):
        ds=_name(c['dataset']);day=_day(c['date']);k=key_of(ds,day)
        if k not in parts:raise ValueError('变更指向未登记分区:'+k)
        if k in seen:raise ValueError('变更重复:'+k)
        seen.add(k);changes.append(Change(ds,day,_text(c['new_hash'])))
    return Manifest(tasks,parts,tuple(targets),start,end,tuple(changes),
        integer(v['lease_seconds'],1),integer(v['logical_now'],0),integer(v['max_instances'],1),digest)
