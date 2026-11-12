"""核对任务定义与日期词法，不展开实例依赖。"""
from datetime import date
from .io import unique,integer

def summarize(v,root):
    unique(v['tasks'],'dataset');integer(v['lease_seconds'],1)
    for p in v['partitions']:
        date.fromisoformat(p['date'])
        if p['status'] not in ('complete','incomplete'):raise ValueError('分区状态未知')
    for target in v['targets']:date.fromisoformat(target['date'])
    return {'datasets':len(v['tasks']),'registered_partitions':len(v['partitions']),'targets':len(v['targets'])}
