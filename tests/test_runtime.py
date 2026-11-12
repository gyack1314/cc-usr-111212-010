"""执行运行时：端到端、迟回worker、续租、更正重排、同摘要停止、重启恢复、并行。"""
import tempfile,threading,unittest
from datetime import date
from pathlib import Path
from lake_data.io import load
from lake_data.model import parse
from lake_data.runtime import Runtime,Rejected,NotClaimable
from lake_data.tasks import execute

def data_manifest(name):
    v,d=load(Path('data')/name);return v,d

class Rt(unittest.TestCase):
    def init_rt(self,data_file=None,raw=None):
        d=tempfile.TemporaryDirectory();self.addCleanup(d.cleanup)
        if raw is None:raw,digest=data_manifest(data_file)
        else:digest='test'
        return Runtime.init(d.name,raw,digest),d.name

    def test_end_to_end_and_lineage(self):
        rt,_=self.init_rt('normal.json')
        with self.assertRaises(NotClaimable) as e:
            rt.claim('w','report@2027-01-01')
        self.assertEqual(e.exception.status,'pending')
        self.assertNotIn('daily@2027-01-01',rt.catalog)  # incomplete不满足依赖
        s=rt.run()
        self.assertEqual(s['counts'].get('done'),3)
        self.assertEqual(s['blocked'],{})
        lin=rt.lineage('report@2027-01-01')
        d31=lin['inputs']['daily@2026-12-31']
        self.assertEqual(d31['inputs']['raw@2026-12-30']['hash'],'r30')
        self.assertEqual(d31['inputs']['raw@2026-12-31']['hash'],'r31-new')
        d01=lin['inputs']['daily@2027-01-01']
        self.assertEqual(d01['inputs']['raw@2027-01-01']['hash'],'r01')
        self.assertEqual(d01['inputs']['raw@2026-12-31']['hash'],'r31-new')
        self.assertEqual(d31['built_with'],'daily1')

    def test_stale_worker_rejected(self):
        rt,_=self.init_rt('normal.json')
        c1=rt.claim('w1','daily@2027-01-01')
        s1=rt.seal('w1','daily@2027-01-01',c1['token'])
        rt.tick(25)
        c2=rt.claim('w2','daily@2027-01-01')
        self.assertEqual(c2['token'],c1['token']+1)
        r2=rt.complete('w2','daily@2027-01-01',c2['token'])
        self.assertTrue(r2['ok'])
        r1=rt.publish('w1','daily@2027-01-01',c1['token'])
        self.assertFalse(r1['ok'])
        self.assertIn('stale-token',r1['reasons'])
        cand=rt.candidates[f"daily@2027-01-01#{c1['token']}"]
        self.assertEqual(cand['status'],'rejected')
        self.assertEqual(cand['output'],s1['output'])  # 旧尝试留证但不转正
        self.assertEqual(rt.catalog['daily@2027-01-01']['hash'],r2['hash'])
        kinds=[e['kind'] for e in rt.evidence]
        self.assertIn('lease-expired',kinds)
        self.assertIn('rejected',kinds)

    def test_renew_rules(self):
        rt,_=self.init_rt('normal.json')
        c=rt.claim('w1','daily@2027-01-01')
        r=rt.renew('w1','daily@2027-01-01',c['token'])
        self.assertEqual(r['expires_at'],c['expires_at'])
        with self.assertRaises(Rejected) as e:
            rt.renew('w1','daily@2027-01-01',c['token']+1)
        self.assertEqual(e.exception.reasons,['stale-token'])
        with self.assertRaises(Rejected) as e:
            rt.renew('w2','daily@2027-01-01',c['token'])
        self.assertEqual(e.exception.reasons,['worker-mismatch'])
        rt.tick(20)  # now==expires_at 即失效
        with self.assertRaises(Rejected) as e:
            rt.renew('w1','daily@2027-01-01',c['token'])
        self.assertEqual(e.exception.reasons,['stale-token'])  # 租约已被回收
        c2=rt.claim('w2','daily@2027-01-01')
        self.assertEqual(c2['token'],c['token']+1)

    def test_lease_expiry_boundary(self):
        rt,_=self.init_rt('normal.json')
        c=rt.claim('w1','daily@2027-01-01')
        rt.tick(19)
        with self.assertRaises(NotClaimable):
            rt.claim('w2','daily@2027-01-01')
        rt.tick(1)  # 到达 expires_at
        c2=rt.claim('w2','daily@2027-01-01')
        self.assertEqual(c2['token'],c['token']+1)

    def test_correct_rejects_stale_candidate_and_reschedules(self):
        rt,_=self.init_rt('normal.json')
        c=rt.claim('w1','daily@2026-12-31')
        s=rt.seal('w1','daily@2026-12-31',c['token'])
        self.assertEqual(s['inputs']['raw@2026-12-31'],'r31-new')
        rt.correct('raw@2026-12-31','r31-new2')
        r=rt.publish('w1','daily@2026-12-31',c['token'])
        self.assertFalse(r['ok'])
        self.assertIn('upstream-changed:raw@2026-12-31',r['reasons'])
        self.assertTrue(rt.state['nodes']['daily@2026-12-31']['needed'])  # 重新排产
        c2=rt.claim('w1','daily@2026-12-31')
        self.assertEqual(c2['token'],c['token']+1)  # 重试用新token
        s2=rt.seal('w1','daily@2026-12-31',c2['token'])
        self.assertEqual(s2['inputs']['raw@2026-12-31'],'r31-new2')
        r2=rt.publish('w1','daily@2026-12-31',c2['token'])
        self.assertTrue(r2['ok'])
        self.assertEqual(rt.catalog['raw@2026-12-31']['hash'],'r31-new2')
        self.assertTrue(rt.state['nodes']['report@2027-01-01']['needed'])

    def test_same_digest_stop_cancels_downstream(self):
        rt,_=self.init_rt('same-digest-stop.json')
        s=rt.run()
        self.assertEqual(rt.catalog['watermark@2027-01-10']['hash'],'z1')
        self.assertEqual(s['instances'].get('cancelled'),['brief@2027-01-10'])
        self.assertEqual(rt.catalog['brief@2027-01-10']['hash'],'b10')
        self.assertEqual(rt.catalog['watermark@2027-01-10']['inputs'],
            {'meter@2027-01-09':'m9-new','meter@2027-01-10':'z1'})
        self.assertTrue(any(e['kind']=='cancelled' and e.get('instance')=='brief@2027-01-10'
            for e in rt.evidence))

    def test_restart_recovery(self):
        rt,base=self.init_rt('normal.json')
        c=rt.claim('w1','daily@2026-12-31')
        rt2=Runtime.open(base)  # 模拟重启：租约与token计数保留
        self.assertEqual(rt2.state['nodes']['daily@2026-12-31']['token'],c['token'])
        self.assertEqual(rt2.summary()['leases']['daily@2026-12-31']['worker'],'w1')
        rt2.tick(25)
        rt3=Runtime.open(base)  # 再次重启：过期租约被回收，实例可重领
        self.assertEqual(rt3.summary()['leases'],{})
        c2=rt3.claim('w2','daily@2026-12-31')
        self.assertEqual(c2['token'],c['token']+1)
        r=rt3.complete('w2','daily@2026-12-31',c2['token'])
        self.assertTrue(r['ok'])

    def test_parallel_independent_instances(self):
        rt,_=self.init_rt('normal.json')
        c1=rt.claim('w1','daily@2026-12-31')
        c2=rt.claim('w2','daily@2027-01-01')  # 独立分区可同时持有租约
        r2=rt.complete('w2','daily@2027-01-01',c2['token'])  # 完成顺序不限
        r1=rt.complete('w1','daily@2026-12-31',c1['token'])
        self.assertTrue(r1['ok'] and r2['ok'])
        self.assertIn('daily@2026-12-31',rt.catalog)
        self.assertIn('daily@2027-01-01',rt.catalog)

    def test_threaded_workers(self):
        rt,_=self.init_rt('normal.json')
        barrier=threading.Barrier(2);out={}
        def work(w,key):
            c=rt.claim(w,key);barrier.wait()
            out[key]=rt.complete(w,key,c['token'])
        ts=[threading.Thread(target=work,args=('w1','daily@2026-12-31')),
            threading.Thread(target=work,args=('w2','daily@2027-01-01'))]
        for t in ts:t.start()
        for t in ts:t.join()
        self.assertTrue(all(r['ok'] for r in out.values()))

    def test_version_change_downstream_not_cancelled(self):
        h30=execute('daily','daily0',date(2026,12,31),{'raw@2026-12-30':'r30','raw@2026-12-31':'r31'})
        h01=execute('daily','daily0',date(2027,1,1),{'raw@2026-12-31':'r31','raw@2027-01-01':'r01'})
        hp=execute('report','report1',date(2027,1,1),
            {'daily@2026-12-31':h30,'daily@2027-01-01':h01})
        raw={'tasks':[{'dataset':'raw','version':'raw1','upstreams':[]},
            {'dataset':'daily','version':'daily1','upstreams':[{'dataset':'raw','offsets':[-1,0]}]},
            {'dataset':'report','version':'report1','upstreams':[{'dataset':'daily','offsets':[-1,0]}]}],
            'partitions':[{'dataset':'raw','date':'2026-12-30','hash':'r30','status':'complete'},
            {'dataset':'raw','date':'2026-12-31','hash':'r31','status':'complete'},
            {'dataset':'raw','date':'2027-01-01','hash':'r01','status':'complete'},
            {'dataset':'daily','date':'2026-12-31','hash':h30,'status':'complete','built_with':'daily0'},
            {'dataset':'daily','date':'2027-01-01','hash':h01,'status':'complete','built_with':'daily0'},
            {'dataset':'report','date':'2027-01-01','hash':hp,'status':'complete','built_with':'report1'}],
            'targets':[{'dataset':'report','date':'2027-01-01'}],
            'allowed_recompute':{'start':'2026-12-31','end':'2027-01-01'},
            'changed':[],'lease_seconds':20,'logical_now':0,'max_instances':100}
        rt,_=self.init_rt(raw=raw)
        s=rt.run()
        # daily0→daily1同逻辑：内容摘要不变，但版本变化要求重算，下游report不因同摘要停止而取消
        self.assertEqual(rt.catalog['daily@2026-12-31']['hash'],h30)
        self.assertEqual(rt.catalog['daily@2026-12-31']['built_with'],'daily1')
        self.assertEqual(rt.catalog['report@2027-01-01']['hash'],hp)
        published=[e['instance'] for e in rt.evidence if e['kind']=='published']
        self.assertIn('report@2027-01-01',published)
        self.assertEqual(s['counts'].get('done'),3)

    def test_missing_upstream_run(self):
        rt,_=self.init_rt('missing-external-upstream.json')
        s=rt.run()
        self.assertEqual(s['instances'].get('done'),['daily@2027-01-01'])
        self.assertIn('daily@2026-12-31',s['blocked'])
        self.assertIn('report@2027-01-01',s['blocked'])
        self.assertNotIn('report@2027-01-01',rt.catalog)

    def test_candidate_immutable_and_retry_needs_new_token(self):
        rt,_=self.init_rt('normal.json')
        c=rt.claim('w1','daily@2026-12-31')
        rt.seal('w1','daily@2026-12-31',c['token'])
        with self.assertRaises(Rejected) as e:
            rt.seal('w1','daily@2026-12-31',c['token'])
        self.assertEqual(e.exception.reasons,['already-sealed'])

if __name__=='__main__':unittest.main()
