"""端到端演示：跨年偏移、缺上游、旧worker迟回、同摘要传播停止。"""
import tempfile
from pathlib import Path
from .io import load
from .runtime import Runtime

DATA=Path(__file__).resolve().parent.parent/'data'

def _init(tmp,name):
    v,d=load(DATA/name)
    return Runtime.init(tmp,v,d)

def cross_year():
    lines=[];checks={}
    with tempfile.TemporaryDirectory() as d:
        rt=_init(Path(d),'normal.json')
        lines.append('目标 report@2027-01-01 的依赖闭包按真实公历天跨年推进:')
        for k in ('daily@2027-01-01','report@2027-01-01'):
            lines.append(f"  {k} <- {rt.plan['nodes'][k]['deps']}")
        lines.append(f"  范围外冻结输入: {[k for k,n in rt.plan['nodes'].items() if n['kind']=='external']}")
        s=rt.run()
        lines.append(f"执行结果: {s['counts']}")
        lin=rt.lineage('report@2027-01-01')
        lines.append(f"report@2027-01-01 血缘: {lin['inputs'] and sorted(lin['inputs'])}")
        checks['跨年闭包']=set(rt.plan['nodes']['daily@2027-01-01']['deps'])=={'raw@2026-12-31','raw@2027-01-01'}
        checks['变更日报被重算']=rt.catalog['daily@2026-12-31']['inputs']=={'raw@2026-12-30':'r30','raw@2026-12-31':'r31-new'}
        checks['incomplete不满足依赖_目标已发布']='report@2027-01-01' in rt.catalog
        checks['血缘回链精确']=lin['inputs']['daily@2026-12-31']['inputs']['raw@2026-12-31']['hash']=='r31-new'
    return {'name':'cross-year','lines':lines,'checks':checks}

def missing_upstream():
    lines=[];checks={}
    with tempfile.TemporaryDirectory() as d:
        rt=_init(Path(d),'missing-external-upstream.json')
        p=rt.plan['nodes']
        lines.append(f"阻塞原因: {rt.summary()['blocked']}")
        s=rt.run()
        lines.append(f"执行结果: {s['counts']}")
        checks['缺上游阻塞']=p['raw@2026-12-30']['blocked']==['missing-external']
        checks['阻塞沿依赖传导']='blocked-dependency:raw@2026-12-30' in p['daily@2026-12-31']['blocked']
        checks['不扩大回填到无限历史']='raw@2026-12-29' not in p and 'raw@2026-12-28' not in p
        checks['可算部分仍完成']=s['instances'].get('done')==['daily@2027-01-01']
        checks['目标保持阻塞']='report@2027-01-01' in s['instances'].get('blocked',[])
    return {'name':'missing-upstream','lines':lines,'checks':checks}

def stale_worker():
    lines=[];checks={}
    with tempfile.TemporaryDirectory() as d:
        rt=_init(Path(d),'normal.json')
        c1=rt.claim('w1','daily@2027-01-01')
        s1=rt.seal('w1','daily@2027-01-01',c1['token'])
        lines.append(f"w1 领取 token={c1['token']} 租约到 {c1['expires_at']} 并封存候选 {s1['output']}")
        rt.tick(25)
        c2=rt.claim('w2','daily@2027-01-01')
        lines.append(f"租约20秒已过期,w2 领取新 token={c2['token']}")
        r2=rt.complete('w2','daily@2027-01-01',c2['token'])
        r1=rt.publish('w1','daily@2027-01-01',c1['token'])
        lines.append(f"w1 迟回发布被拒: {r1['reasons']}")
        cand=rt.candidates[f"daily@2027-01-01#{c1['token']}"]
        checks['旧worker不能发布']=r1['ok'] is False and 'stale-token' in r1['reasons']
        checks['正式分区是新worker产物']=rt.catalog['daily@2027-01-01']['hash']==r2['hash']
        checks['旧尝试输出留证']=cand['status']=='rejected' and cand['output']==s1['output']
        checks['token单调不复用']=rt.state['nodes']['daily@2027-01-01']['token']==2
    return {'name':'stale-worker','lines':lines,'checks':checks}

def same_digest_stop():
    lines=[];checks={}
    with tempfile.TemporaryDirectory() as d:
        rt=_init(Path(d),'same-digest-stop.json')
        lines.append(f"计划: {[ (i['key'],i['kind']) for i in rt.plan['preview']['instances'] ]}")
        s=rt.run()
        ev=[(e['kind'],e.get('instance')) for e in rt.evidence]
        lines.append(f"事件: {ev}")
        lines.append(f"执行结果: {s['counts']}")
        checks['重算后摘要相同']=rt.catalog['watermark@2027-01-10']['hash']=='z1'
        checks['下游失效被取消']=s['instances'].get('cancelled')==['brief@2027-01-10']
        checks['下游正式分区未动']=rt.catalog['brief@2027-01-10']['hash']=='b10'
        checks['运行证据仍登记']=rt.catalog['watermark@2027-01-10']['inputs']=={'meter@2027-01-09':'m9-new','meter@2027-01-10':'z1'}
        checks['取消事件留证']=any(e['kind']=='cancelled' and e.get('instance')=='brief@2027-01-10' for e in rt.evidence)
    return {'name':'same-digest-stop','lines':lines,'checks':checks}

DEMOS={'cross-year':cross_year,'missing-upstream':missing_upstream,
    'stale-worker':stale_worker,'same-digest-stop':same_digest_stop}

def run_demo(name):return DEMOS[name]()
