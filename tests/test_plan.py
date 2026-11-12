"""依赖闭包规划：跨年展开、缺上游阻塞、实例环、触顶、版本变化与函数缺失。"""
import unittest
from datetime import date
from pathlib import Path
from lake_data.io import load
from lake_data.model import parse
from lake_data.plan import build_plan
from lake_data.tasks import has_function,execute

def load_manifest(name):
    v,d=load(Path('data')/name);return parse(v,d)

def inline(**kw):
    base={'tasks':[{'dataset':'raw','version':'raw1','upstreams':[]},
        {'dataset':'daily','version':'daily1','upstreams':[{'dataset':'raw','offsets':[-1,0]}]},
        {'dataset':'report','version':'report1','upstreams':[{'dataset':'daily','offsets':[-1,0]}]}],
        'partitions':[],'targets':[],'allowed_recompute':{'start':'2026-12-31','end':'2027-01-01'},
        'changed':[],'lease_seconds':20,'logical_now':0,'max_instances':100}
    base.update(kw);return parse(base,'inline-test')

class PlanTests(unittest.TestCase):
    def test_cross_year_closure(self):
        p=build_plan(load_manifest('normal.json'),has_function)
        self.assertEqual(p.status,'ok')
        self.assertEqual(set(p.nodes['daily@2027-01-01'].deps),{'raw@2026-12-31','raw@2027-01-01'})
        self.assertEqual(set(p.nodes['report@2027-01-01'].deps),{'daily@2026-12-31','daily@2027-01-01'})
        self.assertEqual(p.nodes['raw@2026-12-30'].kind,'external')
        self.assertEqual(p.nodes['raw@2026-12-31'].corrected,('r31','r31-new'))
        self.assertEqual(p.nodes['daily@2026-12-31'].content,{'raw@2026-12-31'})
        self.assertEqual(p.nodes['daily@2027-01-01'].intrinsic,'incomplete')
        self.assertEqual(p.nodes['report@2027-01-01'].intrinsic,'missing')
        self.assertFalse(any(n.blocked for n in p.nodes.values()))
    def test_missing_external_blocks(self):
        p=build_plan(load_manifest('missing-external-upstream.json'),has_function)
        self.assertEqual(p.status,'ok')
        self.assertEqual(p.nodes['raw@2026-12-30'].blocked,['missing-external'])
        self.assertIn('blocked-dependency:raw@2026-12-30',p.nodes['daily@2026-12-31'].blocked)
        self.assertIn('blocked-dependency:daily@2026-12-31',p.nodes['report@2027-01-01'].blocked)
        self.assertNotIn('raw@2026-12-29',p.nodes)
        self.assertEqual(p.nodes['daily@2027-01-01'].kind,'compute')
    def test_instance_cycle(self):
        p=build_plan(load_manifest('instance-cycle.json'),has_function)
        self.assertEqual(p.status,'cycle')
        self.assertIsNotNone(p.cycle)
        self.assertEqual(p.cycle[0],p.cycle[-1])
        for a,b in zip(p.cycle,p.cycle[1:]):
            self.assertIn(b,p.nodes[a].deps)
        for k,n in p.nodes.items():
            if n.blocked and any(r.startswith('cycle:') for r in n.blocked):
                self.assertEqual(n.kind,'compute')
    def test_unresolved_on_cap(self):
        m=inline(partitions=[{'dataset':'raw','date':'2026-12-31','hash':'r31','status':'complete'}],
            targets=[{'dataset':'report','date':'2027-01-01'}],max_instances=2)
        self.assertEqual(build_plan(m,has_function).status,'unresolved')
    def test_noop_change_stops_at_plan(self):
        m=inline(partitions=[{'dataset':'raw','date':'2026-12-31','hash':'r31','status':'complete'},
                {'dataset':'daily','date':'2026-12-31','hash':'d31','status':'complete'}],
            targets=[{'dataset':'daily','date':'2026-12-31'}],
            changed=[{'dataset':'raw','date':'2026-12-31','new_hash':'r31'}])
        p=build_plan(m,has_function)
        self.assertEqual(p.nodes['daily@2026-12-31'].kind,'satisfied')
        self.assertEqual(set(p.nodes),{'daily@2026-12-31'})  # 无差异变更不展开不传播
    def test_version_change_makes_dirty(self):
        h=execute('daily','daily0',date(2026,12,31),{'raw@2026-12-30':'r30','raw@2026-12-31':'r31'})
        m=inline(partitions=[{'dataset':'raw','date':'2026-12-30','hash':'r30','status':'complete'},
                {'dataset':'raw','date':'2026-12-31','hash':'r31','status':'complete'},
                {'dataset':'daily','date':'2026-12-31','hash':h,'status':'complete','built_with':'daily0'}],
            targets=[{'dataset':'daily','date':'2026-12-31'}])
        n=build_plan(m,has_function).nodes['daily@2026-12-31']
        self.assertEqual(n.kind,'compute')
        self.assertTrue(n.version_cause)
        self.assertEqual(n.content,set())
    def test_missing_function_blocks(self):
        m=inline(tasks=[{'dataset':'raw','version':'raw1','upstreams':[]},
                {'dataset':'daily','version':'daily9','upstreams':[{'dataset':'raw','offsets':[0]}]}],
            targets=[{'dataset':'daily','date':'2026-12-31'}])
        p=build_plan(m,has_function)
        self.assertIn('no-trusted-function:daily/daily9',p.nodes['daily@2026-12-31'].blocked)
    def test_dirty_reaches_registered_beyond_targets(self):
        m=inline(partitions=[{'dataset':'raw','date':'2026-12-31','hash':'r31','status':'complete'},
                {'dataset':'daily','date':'2026-12-31','hash':'d31','status':'complete'},
                {'dataset':'report','date':'2026-12-31','hash':'p31','status':'complete'}],
            targets=[{'dataset':'daily','date':'2026-12-31'}],
            changed=[{'dataset':'raw','date':'2026-12-31','new_hash':'r31-new'}])
        p=build_plan(m,has_function)
        self.assertEqual(p.nodes['report@2026-12-31'].kind,'compute')
        self.assertEqual(p.nodes['report@2026-12-31'].content,{'raw@2026-12-31'})
    def test_invalid_manifest_rejected(self):
        with self.assertRaises((ValueError,KeyError,TypeError)):
            inline(targets=[{'dataset':'ghost','date':'2027-01-01'}])
        with self.assertRaises((ValueError,KeyError,TypeError)):
            inline(changed=[{'dataset':'raw','date':'2026-12-31','new_hash':'x'}])
        with self.assertRaises((ValueError,KeyError,TypeError)):
            inline(tasks=[{'dataset':'a','version':'v','upstreams':[{'dataset':'a','offsets':[0.5]}]}])

if __name__=='__main__':unittest.main()
