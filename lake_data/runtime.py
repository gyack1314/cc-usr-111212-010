"""计划执行：逻辑时钟租约、单调fencing token、候选封存与短事务发布、重启恢复。"""
import json,os,threading,uuid
from datetime import timedelta
from pathlib import Path
from . import tasks
from .io import integer
from .model import parse,key_of,parse_key
from .plan import build_plan

class Rejected(Exception):
    def __init__(self,reasons):
        reasons=list(reasons);super().__init__(';'.join(reasons));self.reasons=reasons

class NotClaimable(Exception):
    def __init__(self,key,status,blocked):
        super().__init__(f'实例不可领取:{key}({status})');self.key=key;self.status=status;self.blocked=blocked

def _write(base,name,obj):
    tmp=base/(name+'.tmp')
    tmp.write_text(json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=1)+'\n',encoding='utf8')
    os.replace(tmp,base/name)

def _read(base,name):return json.loads((base/name).read_text(encoding='utf8'))

class Runtime:
    """基准目录内持久化全部状态；每次open即按逻辑时钟对账，等价于重启恢复。"""
    @classmethod
    def open(cls,base):
        base=Path(base)
        if not (base/'plan.json').exists():raise ValueError('基准目录尚未规划,先运行plan')
        return cls(base)

    @classmethod
    def init(cls,base,raw,digest):
        base=Path(base)
        if (base/'plan.json').exists():
            rt=cls(base)
            if rt.plan['manifest_sha256']!=digest:raise ValueError('基准目录已绑定其他清单')
            return rt
        base.mkdir(parents=True,exist_ok=True)
        if any(base.iterdir()):raise ValueError('基准目录非空')
        m=parse(raw,digest)
        plan=build_plan(m,tasks.has_function)
        base_id=uuid.uuid4().hex
        _write(base,'manifest.json',raw)
        _write(base,'plan.json',plan.to_json(digest,base_id))
        catalog={}
        for k,p in m.partitions.items():
            if p.status!='complete':continue
            h=p.hash
            for c in m.changes:
                if key_of(c.dataset,c.day)==k:h=c.new_hash
            catalog[k]={'hash':h,'status':'complete','built_with':p.built_with or m.tasks[p.dataset].version,
                'inputs':'external','evidence':'registered','published_at':m.logical_now,'token':0}
        _write(base,'catalog.json',catalog)
        _write(base,'candidates.json',{})
        _write(base,'evidence.json',[])
        state={'base_id':base_id,'manifest_sha256':digest,'now':m.logical_now,'seq':0,
            'changes':[{'key':key_of(c.dataset,c.day),'dataset':c.dataset,'date':c.day.isoformat(),
                'old':m.partitions[key_of(c.dataset,c.day)].hash,'new':c.new_hash} for c in m.changes],
            'nodes':{k:{'token':0,'lease':None,'published':False,'published_hash':None,'ack':[],'needed':False}
                for k in plan.nodes}}
        _write(base,'state.json',state)
        rt=cls(base)
        rt._recompute(emit=False)
        rt._save()
        return rt

    def __init__(self,base):
        self.base=Path(base);self.lock=threading.RLock()
        self.plan=_read(self.base,'plan.json')
        self.m=parse(_read(self.base,'manifest.json'),self.plan['manifest_sha256'])
        self.state=_read(self.base,'state.json')
        self.catalog=_read(self.base,'catalog.json')
        self.candidates=_read(self.base,'candidates.json')
        self.evidence=_read(self.base,'evidence.json')
        if self.state['base_id']!=self.plan['base_id'] or self.state['manifest_sha256']!=self.plan['manifest_sha256']:
            raise ValueError('基准目录状态不一致')
        self._expire()
        self._recompute(emit=True)
        self._save()

    def _save(self):
        _write(self.base,'state.json',self.state)
        _write(self.base,'catalog.json',self.catalog)
        _write(self.base,'candidates.json',self.candidates)
        _write(self.base,'evidence.json',self.evidence)

    def _event(self,kind,key=None,**detail):
        self.state['seq']+=1
        e={'seq':self.state['seq'],'at':self.state['now'],'kind':kind}
        if key is not None:e['instance']=key
        e.update(detail);self.evidence.append(e)

    def _sources(self):
        return {c['key'] for c in self.state['changes'] if c['old']!=c['new']}

    def _expire(self):
        now=self.state['now']
        for k,st in self.state['nodes'].items():
            lease=st['lease']
            if lease and now>=lease['expires_at']:
                self._event('lease-expired',k,token=lease['token'],worker=lease['worker'])
                st['lease']=None

    def _dep_keys(self,key):
        ds,day=parse_key(key);out=[]
        for u in self.m.tasks[ds].upstreams:
            for o in u.offsets:out.append(key_of(u.dataset,day+timedelta(days=o)))
        return out

    def _effective_hash(self,key):
        h=self.m.partitions[key].hash if key in self.m.partitions else None
        for c in self.state['changes']:
            if c['key']==key:h=c['new']
        return h

    def _ensure_node(self,key):
        if key in self.plan['nodes']:return
        if len(self.plan['nodes'])>=self.m.max_instances:raise ValueError('实例数超上限')
        ds,day=parse_key(key);reg=self.m.partitions.get(key)
        task=self.m.tasks[ds];in_range=self.m.start<=day<=self.m.end
        node={'dataset':ds,'date':day.isoformat(),'kind':None,'intrinsic':None,'deps':[],
            'blocked':[],'version_cause':False,'initial_hash':None,
            'initial_version':None,'task_version':task.version}
        if reg is not None and reg.status=='complete':
            node['initial_hash']=self._effective_hash(key)
            node['initial_version']=reg.built_with or task.version
            node['kind']='external' if not in_range else 'satisfied'
        elif not in_range:
            node['kind']='missing';node['blocked']=['missing-external']
        else:
            node['kind']='compute';node['intrinsic']='missing' if reg is None else 'incomplete'
        self.plan['nodes'][key]=node
        self.state['nodes'][key]={'token':0,'lease':None,'published':False,
            'published_hash':None,'ack':[],'needed':False}
        if node['kind']=='compute':
            for d in self._dep_keys(key):self._ensure_node(d)
            node['deps']=sorted(self._dep_keys(key))

    def _compute_causes(self,src):
        nodes=self.plan['nodes']
        changed={}
        for k,node in nodes.items():
            if k in src:changed[k]=True
            elif node['kind']!='compute':changed[k]=False
            else:
                st=self.state['nodes'][k]
                changed[k]=(not st['published']) or st['published_hash']!=node['initial_hash'] or node['initial_hash'] is None
        causes={k:set() for k in nodes}
        for _ in range(len(nodes)+1):
            grown=False
            for k,node in nodes.items():
                deps=self._dep_keys(k) if node['kind']=='satisfied' else node['deps']
                acc=set()
                if node['kind']=='compute':  # 变更源的更正由外部给定，自身不因源身份重算
                    if k in src:acc.add(k)
                    if node['intrinsic']:acc.add(k)
                for d in deps:
                    if d in src:acc.add(d)
                    elif d in nodes and nodes[d]['kind']=='compute' and changed[d]:acc|=causes[d]
                if acc!=causes[k]:causes[k]=acc;grown=True
            if not grown:break
        return causes

    def _recompute_blocked(self):
        nodes=self.plan['nodes']
        for k,node in nodes.items():
            if node['kind']=='compute' and not tasks.has_function(node['dataset'],node['task_version']):
                mark=f"no-trusted-function:{node['dataset']}/{node['task_version']}"
                if mark not in node['blocked']:node['blocked'].append(mark)
        grown=True
        while grown:
            grown=False
            for k,node in nodes.items():
                if node['kind']!='compute':continue
                for d in node['deps']:
                    if nodes[d]['blocked']:
                        mark=f'blocked-dependency:{d}'
                        if mark not in node['blocked']:node['blocked'].append(mark);grown=True

    def _recompute(self,emit):
        src=self._sources()
        # 运行期提升：更正可污染计划内satisfied实例，提升为compute并补齐依赖
        while True:
            causes=self._compute_causes(src)
            promo=[k for k,n in self.plan['nodes'].items() if n['kind']=='satisfied' and causes[k]]
            if not promo:break
            for k in promo:
                node=self.plan['nodes'][k];node['kind']='compute'
                for d in self._dep_keys(k):self._ensure_node(d)
                node['deps']=sorted(self._dep_keys(k))
                if emit:self._event('promoted',k,causes=sorted(causes[k]))
        self._recompute_blocked()
        causes=self._compute_causes(src)
        self._live_causes=causes
        for k,node in self.plan['nodes'].items():
            st=self.state['nodes'][k]
            if node['kind']!='compute':needed=False
            else:needed=bool(causes[k]-set(st['ack'])) or (node['version_cause'] and not st['published'])
            if emit and st['needed'] and not needed and not st['published'] and node['kind']=='compute':
                self._event('cancelled',k,detail='失效原因已消除(同摘要停止)')
            st['needed']=needed

    def _node(self,key):
        node=self.plan['nodes'].get(key)
        if node is None:raise ValueError('实例不在计划中:'+key)
        return node

    def _st(self,key):
        self._node(key);return self.state['nodes'][key]

    def _usable(self,d):
        if self.state['nodes'][d]['needed']:return False
        cur=self.catalog.get(d)
        return cur is not None and cur['status']=='complete'

    def _status(self,k):
        node=self.plan['nodes'][k];st=self.state['nodes'][k]
        if node['blocked']:return 'blocked'
        kind=node['kind']
        if kind in ('external','missing'):return 'blocked' if kind=='missing' else 'external'
        if kind=='satisfied':return 'satisfied'
        if not st['needed']:return 'done' if st['published'] else 'cancelled'
        if st['lease'] and self.state['now']<st['lease']['expires_at']:return 'running'
        return 'ready' if all(self._usable(d) for d in node['deps']) else 'pending'

    def _check_lease(self,st,worker,token):
        lease=st['lease']
        if lease is None or lease['token']!=token:raise Rejected(['stale-token'])
        if lease['worker']!=worker:raise Rejected(['worker-mismatch'])
        if self.state['now']>=lease['expires_at']:raise Rejected(['lease-expired'])
        return lease

    def claim(self,worker,key=None):
        with self.lock:
            self._expire()
            if key is None:
                ready=sorted(k for k in self.plan['nodes']
                    if self._status(k)=='ready' and self.state['nodes'][k]['lease'] is None)
                if not ready:raise NotClaimable(None,'no-ready',self.summary()['blocked'])
                key=ready[0]
            else:
                self._node(key)
                if self._status(key)!='ready' or self.state['nodes'][key]['lease'] is not None:
                    raise NotClaimable(key,self._status(key),self.plan['nodes'][key]['blocked'])
            st=self.state['nodes'][key]
            st['token']+=1;token=st['token']
            st['lease']={'token':token,'worker':worker,'acquired_at':self.state['now'],
                'expires_at':self.state['now']+self.m.lease_seconds,'attempt':f'{key}#{token}'}
            self._event('claim',key,token=token,worker=worker,expires_at=st['lease']['expires_at'])
            self._recompute(emit=True);self._save()
            return {'instance':key,'token':token,'attempt':st['lease']['attempt'],'expires_at':st['lease']['expires_at']}

    def renew(self,worker,key,token):
        with self.lock:
            lease=self._check_lease(self._st(key),worker,token)
            lease['expires_at']=self.state['now']+self.m.lease_seconds
            self._event('renew',key,token=token,worker=worker,expires_at=lease['expires_at'])
            self._save()
            return {'instance':key,'token':token,'expires_at':lease['expires_at']}

    def seal(self,worker,key,token):
        with self.lock:
            node=self._node(key);st=self.state['nodes'][key]
            lease=self._check_lease(st,worker,token)
            if not st['needed']:raise Rejected(['not-dirty'])
            bad=[d for d in node['deps'] if not self._usable(d)]
            if bad:raise Rejected(['deps-not-ready:'+d for d in bad])
            cid=f'{key}#{token}'
            if cid in self.candidates:raise Rejected(['already-sealed'])
            inputs={d:self.catalog[d]['hash'] for d in node['deps']}
            output=tasks.execute(node['dataset'],node['task_version'],parse_key(key)[1],inputs)
            self.candidates[cid]={'id':cid,'instance':key,'token':token,'worker':worker,
                'attempt':lease['attempt'],'inputs':inputs,'task_version':node['task_version'],
                'output':output,'sealed_at':self.state['now'],'status':'sealed'}
            self._event('sealed',key,token=token,worker=worker,candidate=cid,output=output)
            self._save()
            return dict(self.candidates[cid])

    def publish(self,worker,key,token):
        with self.lock:
            node=self._node(key);st=self.state['nodes'][key]
            cid=f'{key}#{token}';c=self.candidates.get(cid)
            if c is None:raise Rejected(['no-candidate'])
            if c['status']!='sealed':raise Rejected(['candidate-'+c['status']])
            reasons=[];lease=st['lease']
            if lease is None or lease['token']!=token:reasons.append('stale-token')
            elif lease['worker']!=worker:reasons.append('worker-mismatch')
            if lease is not None and self.state['now']>=lease['expires_at']:reasons.append('lease-expired')
            if c['task_version']!=node['task_version']:reasons.append('version-changed')
            for d,h in c['inputs'].items():
                cur=self.catalog.get(d)
                if cur is None or cur['status']!='complete':reasons.append('upstream-not-complete:'+d)
                elif cur['hash']!=h:reasons.append('upstream-changed:'+d)
            if not st['needed']:reasons.append('not-dirty')
            if reasons:
                c['status']='rejected';c['reject_reasons']=reasons
                if lease is not None and lease['token']==token:st['lease']=None  # 本尝试结束，重试须重新领取新token
                self._event('rejected',key,token=token,worker=worker,reasons=reasons,candidate=cid)
                self._save()
                return {'ok':False,'reasons':reasons,'candidate':cid}
            self._recompute(emit=False)
            old=self.catalog.get(key)
            self.catalog[key]={'hash':c['output'],'status':'complete','built_with':c['task_version'],
                'inputs':c['inputs'],'evidence':c['attempt'],'published_at':self.state['now'],'token':token}
            c['status']='published'
            st['published']=True;st['published_hash']=c['output']
            st['ack']=sorted(self._live_causes[key])
            st['lease']=None
            self._event('published',key,token=token,worker=worker,output=c['output'],
                previous=old['hash'] if old else None)
            self._recompute(emit=True);self._save()
            return {'ok':True,'instance':key,'hash':c['output'],'previous':old['hash'] if old else None}

    def complete(self,worker,key,token):
        self.seal(worker,key,token)
        return self.publish(worker,key,token)

    def correct(self,key,new_hash):
        with self.lock:
            if key not in self.m.partitions:raise ValueError('变更指向未登记分区:'+key)
            old=None
            for c in self.state['changes']:
                if c['key']==key:old=c['new']
            if old is None:old=self.m.partitions[key].hash
            if key in self.catalog:old=self.catalog[key]['hash']
            ds,day=parse_key(key)
            self.state['changes'].append({'key':key,'dataset':ds,'date':day.isoformat(),'old':old,'new':new_hash})
            if old!=new_hash:
                if key in self.catalog:
                    self.catalog[key].update({'hash':new_hash,'inputs':'external','evidence':'correct',
                        'published_at':self.state['now']})
                self._event('corrected',key,old=old,new=new_hash)
            else:
                self._event('correct-noop',key,hash=new_hash)
            self._recompute(emit=True);self._save()
            return {'instance':key,'old':old,'new':new_hash,'propagated':old!=new_hash}

    def tick(self,seconds):
        with self.lock:
            self.state['now']+=integer(seconds,0)
            self._expire();self._recompute(emit=True);self._save()
            return {'now':self.state['now']}

    def recover(self):
        with self.lock:
            self._expire();self._recompute(emit=True)
            s=self.summary();self._save();return s

    def summary(self):
        by={}
        for k in self.plan['nodes']:by.setdefault(self._status(k),[]).append(k)
        return {'now':self.state['now'],'plan_status':self.plan['status'],
            'counts':{s:len(v) for s,v in sorted(by.items())},
            'instances':{s:sorted(v) for s,v in sorted(by.items())},
            'blocked':{k:self.plan['nodes'][k]['blocked'] for k in self.plan['nodes'] if self.plan['nodes'][k]['blocked']},
            'pending_causes':{k:sorted(self._live_causes[k]-set(self.state['nodes'][k]['ack']))
                for k in self.plan['nodes'] if self.state['nodes'][k]['needed']},
            'leases':{k:self.state['nodes'][k]['lease'] for k in self.state['nodes'] if self.state['nodes'][k]['lease']},
            'candidates':{c:{'instance':v['instance'],'status':v['status'],'output':v['output']}
                for c,v in self.candidates.items()}}

    def graph(self):
        out=[]
        for k,node in sorted(self.plan['nodes'].items()):
            deps=[{'instance':d,'offset':(parse_key(d)[1]-parse_key(k)[1]).days,'status':self._status(d)}
                for d in node['deps']]
            out.append({'instance':k,'status':self._status(k),'kind':node['kind'],
                'blocked':node['blocked'],'deps':deps})
        return {'plan_status':self.plan['status'],'cycle':self.plan['cycle'],'instances':out}

    def lineage(self,key):
        if key not in self.catalog:raise KeyError('未发布分区:'+key)
        seen=set()
        def walk(k,pinned):
            entry=self.catalog.get(k)
            if entry is None:return {'instance':k,'hash':pinned,'external':True}
            node={'instance':k,'hash':pinned or entry['hash'],'built_with':entry['built_with'],
                'evidence':entry['evidence'],'published_at':entry['published_at']}
            if k in seen:node['cycle']=True;return node
            if entry['inputs']=='external':node['inputs']='external'
            else:
                seen.add(k)
                node['inputs']={d:walk(d,h) for d,h in entry['inputs'].items()}
                seen.discard(k)
            return node
        return walk(key,None)

    def run(self,worker='auto',limit=1000):
        done=0
        while done<limit:
            self.recover()
            try:c=self.claim(worker)
            except NotClaimable:break
            r=self.complete(worker,c['instance'],c['token'])
            if not r['ok']:raise Rejected(r['reasons'])
            done+=1
        return self.recover()
