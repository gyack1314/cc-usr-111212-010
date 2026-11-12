"""湖仓资料工具与按时间依赖闭包的回填计划命令行。"""
import argparse,json,sys
from pathlib import Path
from .io import load
from .domain import summarize
from .runtime import Runtime,Rejected,NotClaimable
from .demo import run_demo,DEMOS

def _out(x):print(json.dumps(x,ensure_ascii=False,indent=2,allow_nan=False))

def main(argv=None):
    p=argparse.ArgumentParser(prog='lake_data.cli',description='湖仓资料与回填计划工具')
    sub=p.add_subparsers(dest='command',required=True)
    s=sub.add_parser('inspect',help='显示原字段概况');s.add_argument('file')
    s=sub.add_parser('plan',help='计划预览并绑定基准目录');s.add_argument('file');s.add_argument('--base',required=True)
    s=sub.add_parser('graph',help='实例依赖图');s.add_argument('--base',required=True)
    s=sub.add_parser('run',help='自动领取执行至无可领取');s.add_argument('--base',required=True)
    for name,help_ in (('claim','领取实例'),('renew','续租'),('seal','执行可信函数并封存候选'),
            ('publish','短事务校验并切换分区指针'),('complete','seal+publish')):
        s=sub.add_parser(name,help=help_);s.add_argument('--base',required=True)
        s.add_argument('--worker',required=True);s.add_argument('--instance',required=name!='claim')
        if name!='claim':s.add_argument('--token',type=int,required=True)
    s=sub.add_parser('correct',help='登记上游更正并重排相关实例')
    s.add_argument('--base',required=True);s.add_argument('--instance',required=True);s.add_argument('--hash',required=True)
    s=sub.add_parser('tick',help='推进逻辑时钟');s.add_argument('--base',required=True);s.add_argument('--seconds',type=int,required=True)
    s=sub.add_parser('recover',help='重启恢复与对账');s.add_argument('--base',required=True)
    s=sub.add_parser('status',help='实例状态与阻塞原因');s.add_argument('--base',required=True)
    s=sub.add_parser('lineage',help='正式分区血缘回链');s.add_argument('--base',required=True);s.add_argument('--instance',required=True)
    s=sub.add_parser('demo',help='运行端到端演示');s.add_argument('name',choices=sorted(DEMOS)+['all'])
    a=vars(p.parse_args(argv))
    cmd=a.pop('command')
    try:
        if cmd=='inspect':
            v,digest=load(a['file'])
            _out({'source_sha256':digest,'summary':summarize(v,Path(a['file']).parent)});return 0
        if cmd=='plan':
            v,digest=load(a['file']);rt=Runtime.init(a['base'],v,digest)
            _out(rt.plan['preview']);return 0 if rt.plan['status']!='unresolved' else 1
        if cmd=='demo':
            names=sorted(DEMOS) if a['name']=='all' else [a['name']]
            ok=True
            for n in names:
                r=run_demo(n)
                for line in r['lines']:print(line)
                _out({'demo':n,'checks':r['checks']})
                ok=ok and all(r['checks'].values())
            return 0 if ok else 1
        rt=Runtime.open(a['base'])
        if cmd=='graph':_out(rt.graph());return 0
        if cmd=='status':_out(rt.summary());return 0
        if cmd=='recover':_out(rt.recover());return 0
        if cmd=='tick':_out(rt.tick(a['seconds']));return 0
        if cmd=='claim':_out(rt.claim(a['worker'],a.get('instance')));return 0
        if cmd=='renew':_out(rt.renew(a['worker'],a['instance'],a['token']));return 0
        if cmd=='seal':_out(rt.seal(a['worker'],a['instance'],a['token']));return 0
        if cmd in ('publish','complete'):
            r=(rt.publish if cmd=='publish' else rt.complete)(a['worker'],a['instance'],a['token'])
            _out(r);return 0 if r['ok'] else 1
        if cmd=='correct':_out(rt.correct(a['instance'],a['hash']));return 0
        if cmd=='lineage':_out(rt.lineage(a['instance']));return 0
        if cmd=='run':
            s=rt.run();_out(s)
            remaining=s['counts'].get('ready',0)+s['counts'].get('pending',0)+s['counts'].get('running',0)
            return 0 if not s['blocked'] and remaining==0 else 1
    except Rejected as e:_out({'rejected':e.reasons});return 2
    except NotClaimable as e:_out({'not_claimable':e.key,'status':e.status,'blocked':e.blocked});return 2
    except (ValueError,KeyError) as e:_out({'error':str(e)});return 2

if __name__=='__main__':sys.exit(main())
