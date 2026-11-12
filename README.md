# 仓库回填计划的时间依赖与版本栅栏

本项目保存湖仓治理的合成资料与原始配置。`lake_data.io`读取JSON清单和资产身份，`lake_data.domain`检查基本字段并给数量概况，`lake_data.cli`输出原资料摘要。字段、数据类型、时间和版本口径见[资料约定](docs/data-contract.md)。数据中的设备、班组、主体和电量均为虚构，不连接电网、生产数仓或人员系统。

Python 3.11及以上，无第三方Python依赖。在项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q lake_data tests
python -m lake_data.cli inspect data/normal.json
python -m lake_data.cli demo all
```

`inspect`只显示原字段概况，不修改源资料。`invalid-config.json`是基本配置错误，其余JSON为有效清单但可含需要领域操作判断的边界资料。null与缺字段应按各字段约定区分，原文件摘要绑定实际字节。

## 按时间依赖闭包的回填计划与安全发布

`lake_data.model`把清单校验为任务（数据集、转换版本、上游日期偏移集合）、分区登记、目标、变更与运行参数；`lake_data.plan`按用户目标范围展开完整必需实例（日期按真实公历天推进，可跨年跨月），将`changed`沿反向偏移传播到范围内已登记或目标可达实例，在具体实例图上检测循环，超`max_instances`报unresolved；范围外依赖只接受已登记complete分区，缺失即blocked且不向无限历史扩大。`lake_data.runtime`在基准目录持久化计划、目录、候选、证据与逻辑时钟：实例只有全部依赖complete且版本确定才可领取；租约`expires_at=领取+lease_seconds`，token每实例每次领取单调递增不复用；输出先封存为不可变候选，发布短事务校验依赖摘要、任务版本与当前token后切换分区指针，计算中上游被更正会拒绝旧候选并重排实例；重算摘要与原发布相同则停止该分支失效传播，运行证据与转换版本更新仍登记；执行只调用`lake_data.tasks`中的本地可信函数。每次`open`即按逻辑时钟对账，等价于重启恢复。

```sh
python -m lake_data.cli plan data/normal.json --base out/plan   # 计划预览与阻塞原因
python -m lake_data.cli graph --base out/plan                   # 实例依赖图
python -m lake_data.cli run --base out/plan                     # 自动领取-执行-发布
python -m lake_data.cli claim|renew|seal|publish|complete --base out/plan --worker w1 [--instance ds@date --token N]
python -m lake_data.cli correct --base out/plan --instance raw@2026-12-31 --hash r31-new2
python -m lake_data.cli tick|recover|status --base out/plan
python -m lake_data.cli lineage --base out/plan --instance report@2027-01-01
python -m lake_data.cli demo all   # 跨年偏移、缺上游、旧worker迟回、同摘要传播停止
```

`data/same-digest-stop.json`是同摘要停止场景：watermark取上游摘要最大值，meter@2027-01-09的更正不改变其输出，brief@2027-01-10的失效因此被取消。运行期更正只在当前计划闭包内重排；新目标或新数据集需对新的基准目录重新`plan`。

Dockerfile固定Linux基础镜像。`docker build -t lake-material .`构建，`docker run --rm lake-material`读取正常清单；`docker run --rm lake-material python -m unittest discover -s tests -v`执行已有检查。所有资料本地可读，无需账号和在线来源。
