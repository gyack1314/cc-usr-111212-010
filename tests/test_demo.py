"""四个演示自检与CLI入口冒烟。"""
import json,subprocess,sys,tempfile,unittest
from pathlib import Path
from lake_data.demo import DEMOS,run_demo

ROOT=Path(__file__).resolve().parent.parent

def cli(*args):
    return subprocess.run([sys.executable,'-m','lake_data.cli',*args],
        cwd=ROOT,capture_output=True,text=True,timeout=60)

class DemoTests(unittest.TestCase):
    def test_all_demos(self):
        for name in sorted(DEMOS):
            with self.subTest(demo=name):
                r=run_demo(name)
                self.assertTrue(all(r['checks'].values()),r)
    def test_cli_inspect_still_works(self):
        r=cli('inspect','data/normal.json')
        self.assertEqual(r.returncode,0,r.stderr)
        self.assertEqual(json.loads(r.stdout)['summary']['datasets'],3)
    def test_cli_plan_run_lineage(self):
        with tempfile.TemporaryDirectory() as d:
            r=cli('plan','data/normal.json','--base',d)
            self.assertEqual(r.returncode,0,r.stderr)
            self.assertEqual(json.loads(r.stdout)['status'],'ok')
            r=cli('run','--base',d)
            self.assertEqual(r.returncode,0,r.stderr)
            r=cli('lineage','--base',d,'--instance','report@2027-01-01')
            self.assertEqual(r.returncode,0,r.stderr)
            lin=json.loads(r.stdout)
            self.assertEqual(lin['inputs']['daily@2026-12-31']['inputs']['raw@2026-12-31']['hash'],'r31-new')
    def test_cli_blocked_exit_code(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(cli('plan','data/missing-external-upstream.json','--base',d).returncode,0)
            r=cli('run','--base',d)
            self.assertEqual(r.returncode,1)
            self.assertIn('missing-external',r.stdout)
    def test_cli_cycle_plan(self):
        with tempfile.TemporaryDirectory() as d:
            r=cli('plan','data/instance-cycle.json','--base',d)
            out=json.loads(r.stdout)
            self.assertEqual(out['status'],'cycle')
            self.assertEqual(out['cycle'][0],out['cycle'][-1])
    def test_cli_demo(self):
        r=cli('demo','all')
        self.assertEqual(r.returncode,0,r.stderr+r.stdout)

if __name__=='__main__':unittest.main()
