"""按时间依赖闭包规划回填：实例展开、实例级循环、失效传播与阻塞原因。"""
import hashlib,json
from datetime import timedelta
from .model import key_of,parse_key

def _canon(x):return json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':'))

class Overflow(Exception):pass

class Node:
    def __init__(self,key,dataset,day,kind,intrinsic,initial_hash,initial_version,task_version,corrected):
        self.key=key;self.dataset=dataset;self.day=day
        self.kind=kind             # compute|satisfied|external|missing
        self.intrinsic=intrinsic   # None|missing|incomplete
        self.deps=[]               # 依赖实例键（仅compute节点展开）
        self.blocked=[]            # 阻塞原因
        self.content=set()         # 内容失效来源实例键
        self.version_source=False  # 登记构建版本与当前任务版本不一致
        self.version_cause=False   # 自身或上游存在显式任务版本变化
        self.initial_hash=initial_hash
        self.initial_version=initial_version
        self.task_version=task_version
        self.corrected=corrected   # (old,new)|None，本实例被登记更正
        self.expanded=False

class Plan:
    def __init__(self,status,nodes,cycle,digest):
        self.status=status;self.nodes=nodes;self.cycle=cycle;self.digest=digest
    def preview(self):
        out=[]
        for k,n in sorted(self.nodes.items()):
            deps={d:(self.nodes[d].initial_hash if self.nodes[d].kind in ('satisfied','external') else None) for d in n.deps}
            ready=n.kind=='compute' and not n.blocked and all(self.nodes[d].kind!='compute' for d in n.deps)
            out.append({'key':k,'kind':n.kind,'intrinsic':n.intrinsic,'deps':deps,'ready':ready,
                'blocked':list(n.blocked),'content_causes':sorted(n.content),'version_cause':n.version_cause,
                'initial_hash':n.initial_hash,'corrected':n.corrected})
        return {'status':self.status,'plan_sha256':self.digest,'cycle':self.cycle,'instances':out}
    def to_json(self,manifest_sha256,base_id):
        return {'manifest_sha256':manifest_sha256,'base_id':base_id,'status':self.status,'cycle':self.cycle,
            'plan_sha256':self.digest,'preview':self.preview(),
            'nodes':{k:{'dataset':n.dataset,'date':n.day.isoformat(),'kind':n.kind,'intrinsic':n.intrinsic,
                'deps':n.deps,'blocked':n.blocked,'version_cause':n.version_cause,
                'initial_hash':n.initial_hash,'initial_version':n.initial_version,
                'task_version':n.task_version} for k,n in self.nodes.items()}}

def _sccs(keys,edges):
    visited=set();order=[]
    for s in keys:
        if s in visited:continue
        visited.add(s);stack=[(s,iter(sorted(edges[s])))]
        while stack:
            n,it=stack[-1];nxt=None
            for m in it:
                if m not in visited:nxt=m;break
            if nxt is None:order.append(n);stack.pop()
            else:visited.add(nxt);stack.append((nxt,iter(sorted(edges[nxt]))))
    rev={k:[] for k in keys}
    for k in keys:
        for d in edges[k]:rev[d].append(k)
    sccs=[];seen=set()
    for s in reversed(order):
        if s in seen:continue
        comp=[];seen.add(s);stack=[s]
        while stack:
            n=stack.pop();comp.append(n)
            for m in rev[n]:
                if m not in seen:seen.add(m);stack.append(m)
        sccs.append(sorted(comp))
    return sccs

def _cycle_path(comp,edges):
    inset=set(comp);cur=sorted(comp)[0];path=[cur];seen={cur:0}
    while True:
        nxt=sorted(d for d in edges[cur] if d in inset)
        n=nxt[0]
        if n in seen:return path[seen[n]:]+[n]
        seen[n]=len(path);path.append(n);cur=n

def build_plan(m,has_function):
    """m为校验后的清单；has_function(dataset,version)判断可信函数是否存在。"""
    eff={k:p.hash for k,p in m.partitions.items()}
    sources={}
    for c in m.changes:
        k=key_of(c.dataset,c.day);old=eff[k]
        if c.new_hash!=old:sources[k]=(old,c.new_hash)
        eff[k]=c.new_hash
    nodes={}
    def dep_keys(n):
        t=m.tasks[n.dataset];out=[]
        for u in t.upstreams:
            for o in u.offsets:out.append(key_of(u.dataset,n.day+timedelta(days=o)))
        return out
    def ensure(key):
        if key in nodes:return nodes[key]
        if len(nodes)>=m.max_instances:raise Overflow()
        dataset,day=parse_key(key)
        reg=m.partitions.get(key);in_range=m.start<=day<=m.end
        task=m.tasks[dataset];corrected=sources.get(key)
        if reg is not None and reg.status=='complete':
            ver=reg.built_with or task.version
            if not in_range:
                n=Node(key,dataset,day,'external',None,eff[key],ver,task.version,corrected)
            elif ver!=task.version:
                n=Node(key,dataset,day,'compute',None,eff[key],ver,task.version,corrected)
                n.version_source=True
            else:
                n=Node(key,dataset,day,'satisfied',None,eff[key],ver,task.version,corrected)
        else:
            if not in_range:
                n=Node(key,dataset,day,'missing',None,None,None,task.version,corrected)
                n.blocked.append('missing-external')
            else:
                intrinsic='missing' if reg is None else 'incomplete'
                n=Node(key,dataset,day,'compute',intrinsic,None,None,task.version,corrected)
        nodes[key]=n;return n
    def drain():
        found=True
        while found:
            found=False
            for k,n in list(nodes.items()):
                if n.kind=='compute' and not n.expanded:
                    n.expanded=True;found=True
                    for d in dep_keys(n):ensure(d)
    try:
        for k in m.targets:ensure(k)
        drain()
        # 显式任务版本变化：范围内已登记complete分区构建版本落后即失效，与内容变更同路传播
        seeds=list(sources)
        for k,p0 in m.partitions.items():
            if p0.status!='complete':continue
            _,yd=parse_key(k)
            if not(m.start<=yd<=m.end):continue
            task=m.tasks[p0.dataset]
            if (p0.built_with or task.version)==task.version:continue
            ensure(k).version_source=True
            seeds.append(k)
        # 变更沿反向offset关系传播到范围内已登记或目标可达实例，不制造无限未来任务
        seen=set();queue=list(seeds)
        while queue:
            x=queue.pop();xs,xd=parse_key(x)
            for t in m.tasks.values():
                for u in t.upstreams:
                    if u.dataset!=xs:continue
                    for o in u.offsets:
                        y=key_of(t.dataset,xd-timedelta(days=o))
                        if y in seen:continue
                        _,yd=parse_key(y)
                        if not(m.start<=yd<=m.end):continue
                        if y not in m.partitions and y not in nodes:continue
                        seen.add(y);ensure(y);queue.append(y)
        drain()
        # 失效原因与提升的不动点：satisfied被污染则提升为compute并展开其依赖
        while True:
            for n in nodes.values():n.content=set();n.version_cause=False
            grown=True
            while grown:
                grown=False
                for n in nodes.values():
                    acc=set()
                    if n.kind=='compute':  # 变更源的更正由外部给定，自身不因源身份重算
                        if n.key in sources:acc.add(n.key)
                        if n.intrinsic:acc.add(n.key)
                    for d in dep_keys(n):
                        if d in sources:acc.add(d);continue
                        dn=nodes.get(d)
                        if dn is None:continue
                        if dn.kind=='compute':acc|=dn.content
                    vc=n.version_source or any(
                        nodes[d].version_source or nodes[d].version_cause for d in dep_keys(n) if d in nodes)
                    if acc!=n.content or vc!=n.version_cause:
                        n.content=acc;n.version_cause=vc;grown=True
            promoted=[k for k,n in nodes.items() if n.kind=='satisfied' and (n.content or n.version_cause)]
            if not promoted:break
            for k in promoted:nodes[k].kind='compute'
            drain()
    except Overflow:
        return Plan('unresolved',nodes,None,None)
    for n in nodes.values():
        if n.kind=='compute':n.deps=sorted(dep_keys(n))
    # 实例级循环检测：只在待计算实例子图上找具体环
    edges={k:[d for d in n.deps if nodes[d].kind=='compute'] for k,n in nodes.items() if n.kind=='compute'}
    cycle=None
    for comp in _sccs(list(edges),edges):
        if len(comp)>1 or comp[0] in edges[comp[0]]:
            path=_cycle_path(comp,edges)
            if cycle is None:cycle=path
            mark='cycle:'+' -> '.join(path)
            for k in comp:
                if mark not in nodes[k].blocked:nodes[k].blocked.append(mark)
    for k,n in nodes.items():
        if n.kind=='compute' and not has_function(n.dataset,n.task_version):
            n.blocked.append(f'no-trusted-function:{n.dataset}/{n.task_version}')
    grown=True
    while grown:
        grown=False
        for k,n in nodes.items():
            if n.kind!='compute':continue
            for d in n.deps:
                if nodes[d].blocked and f'blocked-dependency:{d}' not in n.blocked:
                    n.blocked.append(f'blocked-dependency:{d}');grown=True
    status='cycle' if cycle else 'ok'
    digest=hashlib.sha256(_canon({'manifest':m.digest,
        'nodes':{k:[n.kind,n.intrinsic,n.deps,n.initial_hash,n.initial_version,n.task_version] for k,n in nodes.items()}}).encode('utf8')).hexdigest()
    return Plan(status,nodes,cycle,digest)
