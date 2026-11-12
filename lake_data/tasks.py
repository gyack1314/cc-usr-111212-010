"""可信任务函数注册表：只调用本地函数，不执行资料中的任何代码或shell。"""
import hashlib

def _digest(*parts):
    h=hashlib.sha256()
    for p in parts:
        h.update(str(p).encode('utf8'));h.update(b'|')
    return h.hexdigest()[:16]

def _combine(logic):
    def run(day,inputs):
        body=[f'{k}={inputs[k]}' for k in sorted(inputs)]
        return f'{logic}:'+_digest(day,*body)
    return run

def _ingest(logic):
    def run(day,inputs):
        return f'{logic}:'+_digest(day,'synthetic-ingest')
    return run

def _watermark(day,inputs):
    # 取上游快照摘要的字典序最大值作为代表摘要；非最大者的内容改动不改变输出
    return max(inputs.values())

REGISTRY={
    ('raw','raw1'):_ingest('raw-ingest'),
    ('daily','daily0'):_combine('daily-combine'),  # 与 daily1 同逻辑，用于版本变化演示
    ('daily','daily1'):_combine('daily-combine'),
    ('report','report1'):_combine('report-combine'),
    ('meter','meter1'):_ingest('meter-ingest'),
    ('watermark','wm1'):_watermark,
    ('brief','brief1'):_combine('brief-combine'),
}

def has_function(dataset,version):return (dataset,version) in REGISTRY

def execute(dataset,version,day,inputs):
    fn=REGISTRY.get((dataset,version))
    if fn is None:raise KeyError(f'无可信任务函数:{dataset}/{version}')
    return fn(day,dict(inputs))
