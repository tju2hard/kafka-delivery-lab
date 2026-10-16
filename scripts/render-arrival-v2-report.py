"""Freeze independently verified seven-scheme results and render the current report."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'harbor/arrival-marking-comparison/environment'))
from arrival_comparison import CASE_NAMES,SCHEMES,audit_arrivals,assert_outcome
from latency import distribution


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def read_gzip(path):
    with gzip.open(path,'rt') as stream:return [json.loads(line) for line in stream]


def wrapped(values):return [{'key':v['event_id'],'value':json.dumps(v)} for v in values]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verified',type=Path,nargs='+',required=True,
                        help='verified directories produced by the independent verifier')
    parser.add_argument('--audit-only',action='store_true',help='recheck frozen evidence without writing reports')
    args=parser.parse_args()
    results=ROOT/'results';evidence=results/'arrival-v2-evidence';evidence.mkdir(exist_ok=True)
    runs=[];metrics=[]
    for source in args.verified:
        source=source.resolve();trial=source.parent.name if source.name=='verified' else source.name
        checks=json.loads((source/'checks.json').read_text())
        assert checks['report_validation_reward']==1 and not checks['errors']
        report=json.loads((source/'arrival-comparison.json').read_text())
        assert report['schema_version']==2 and [c['name'] for c in report['cases']]==CASE_NAMES
        assert [c['name'] for c in checks['checks']]==CASE_NAMES
        for case in report['cases']:
            for item in case['exported_jsonl']:
                path=source/item['path'];raw=gzip.decompress(path.read_bytes())
                assert digest(path)==item['gzip_sha256']
                assert hashlib.sha256(raw).hexdigest()==item['uncompressed_sha256']
                assert len(raw.splitlines())==item['rows']
            directory=source/'data'/case['name']
            inputs=wrapped(read_gzip(directory/'input.jsonl.gz'))
            for view in ('read_uncommitted','read_committed'):
                outputs=read_gzip(directory/f'output-{view}.jsonl.gz')
                assert audit_arrivals(inputs,wrapped(outputs))==case['views'][view]['audit']
                observations=read_gzip(directory/f'observations-{view}.jsonl.gz')
                assert [json.loads(r['value']) for r in observations]==outputs
                elapsed=[r['observed_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in observations]
                assert distribution(elapsed)==case['views'][view]['visibility_ms']
                if view=='read_committed':
                    metrics.append({'trial':trial,'case':case['name'],'samples':len(elapsed),
                                    'mean_visibility_ms':sum(elapsed)/len(elapsed),
                                    'p99_visibility_ms':distribution(elapsed)['p99']})
            assert_outcome(case)
        if args.audit_only:
            manifest_path=source/'native.sha256.json'
            if manifest_path.exists():
                for name,expected in json.loads(manifest_path.read_text()).items():
                    assert digest(source/name)==expected,name
            runs.append({'trial':trial,**report})
            continue
        destination=evidence/trial
        shutil.copytree(source,destination,dirs_exist_ok=True)
        native_manifest={}
        for case in report['cases']:
            worker=Path(case['worker_directory'])
            paths=list(worker.glob('*.json'))+list(worker.glob('fault-*.stderr'))
            for path in paths:
                target=destination/'native'/case['name']/path.name;target.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(path,target)
                native_manifest[str(target.relative_to(destination))]=digest(target)
        for path in source.parent.glob('broker-*/server.properties'):
            target=destination/'environment'/path.parent.name/path.name;target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(path,target)
            native_manifest[str(target.relative_to(destination))]=digest(target)
        (destination/'native.sha256.json').write_text(json.dumps(native_manifest,indent=2)+'\n')
        runs.append({'trial':trial,**report})
    assert len({r['trial'] for r in runs})==len(runs)
    if args.audit_only:
        manifest=results/'arrival-v2-inputs.sha256.json'
        if manifest.exists():
            for name,expected in json.loads(manifest.read_text()).items():
                assert digest(evidence/'inputs'/name)==expected,name
        print(json.dumps({'audit':'passed','runs':len(runs),'independent_views':len(runs)*len(CASE_NAMES)*2}))
        return
    (results/'arrival-v2-summary.json').write_text(json.dumps({'runs':runs},ensure_ascii=False,indent=2)+'\n')
    (results/'arrival-v2-readable-metrics.json').write_text(json.dumps({'rows':metrics},indent=2)+'\n')
    sources=['AGENTS.md','pom.xml','scripts/build.sh','scripts/run-arrival-native.py',
             'scripts/render-arrival-v2-report.py','scripts/render-arrival-report.py','tests/test_arrival_marking.py']
    sources += [str(p.relative_to(ROOT)) for p in (ROOT/'src/main/java/lab').glob('*.java')]
    sources += [str(p.relative_to(ROOT)) for p in (ROOT/'harbor/arrival-marking-comparison').rglob('*')
                if p.is_file() and p.suffix in ('.py','.yaml','.md','.toml','.sh') and 'runtime' not in p.parts]
    sources += ['harbor/arrival-marking-comparison/environment/Dockerfile']
    manifest={}
    for name in sorted(set(sources)):
        target=evidence/'inputs'/name;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(ROOT/name,target);manifest[name]=digest(target)
    (results/'arrival-v2-inputs.sha256.json').write_text(json.dumps(manifest,indent=2)+'\n')
    runtime=ROOT/'harbor/arrival-marking-comparison/environment/runtime'
    runtime_manifest={str(p.relative_to(runtime)):digest(p) for p in sorted(runtime.rglob('*')) if p.is_file()}
    (results/'arrival-v2-runtime.sha256.json').write_text(json.dumps(runtime_manifest,indent=2)+'\n')

    def case(run,name):return next(c for c in run['cases'] if c['name']==name)
    def count(name,key):
        values=[case(r,name)['views']['read_committed']['audit'][key] for r in runs]
        return str(min(values)) if min(values)==max(values) else f'{min(values)}～{max(values)}'
    def timing(name,key):
        values=[r[key]/1000 for r in metrics if r['case']==name]
        return f'{values[0]:.2f}' if len(values)==1 else f'{min(values):.2f}～{max(values):.2f}'
    issues={'flink-state-eos':'本次已提交结果正确','flink-state-alo':'恢复重发，首次也多输出',
        'streams-direct-eos':'本次已提交结果正确','redis-mark-before':'首次少标1；消息未漏',
        'redis-mark-after':'首次多输出1；重发1条','state-redis-before':'首次少1；漏发1条',
        'state-redis-after':'首次多输出1；重发1条'}
    lines=['# 七种方案：首次标记、消息交付与可见时间','',
        '每场输入9000条，3000个业务 ID 各出现三次。**全部到达都要保留：正确输出是9000条，首次3000、重复6000。** '
        '本文“去重判断”只决定首次/重复标记；sink 的发送登记只尝试跳过同一次输入的恢复重放。','',
        f'以下是{len(runs)}轮真实 Kafka／Redis／Flink／Streams 进程实验，由独立消费者复读并逐条审计。'
        '恢复行包含故障等待；不同方案的故障位置见后文，不能把它们当成同一种随机故障。','',
        '| 方案 | 场景 | 输出总数 | 首次标记数 | 重复标记数 | 平均可见延迟（秒） | p99可见延迟（秒） | 结果 |',
        '|---|---|---:|---:|---:|---:|---:|---|']
    for prefix,mode,fault,label in SCHEMES:
        for recovery in (False,True):
            name=prefix+('-recovery' if recovery else '-normal')
            row=[label,'故障恢复' if recovery else '正常',count(name,'output_arrivals'),count(name,'false_markers'),
                 count(name,'true_markers'),timing(name,'mean_visibility_ms'),timing(name,'p99_visibility_ms'),
                 issues[prefix] if recovery else '本次正确']
            lines.append('| '+' | '.join(row)+' |')
    lines += ['',
        '**先登记 Redis 的两种问题不同：** 纯 Redis 标记方案保住了9000条消息，但其中一次首次被改成重复；'
        'state 标记＋Redis 发送登记方案保留了 state 算出的标记值，却漏掉了整条首次消息。'
        '**后登记的两种方案都在 ACK 后、登记前故障，恢复后多输出一次首次消息。**','',
        '平均值是所有实际输出的算术平均；计时从输入 send 前开始，到目标 read_committed 消费者第一次读到对应输出为止，'
        '包括积压、处理、写入、事务提交和观察等待。重发记录也纳入统计；漏掉的输入没有延迟样本，不能靠漏发让耗时显得更好。'
        '这里的输出用真实 Kafka 模拟 Talos，未调用真实 Talos 服务。'
        'Redis 使用逐条同步调用、AOF always，未做 pipeline；表中时间不是 Redis 的性能上限。','',
        '## 每种方案把判断放在哪里','',
        '| 方案 | 业务首次/重复由谁判断 | sink 的执行顺序 | Redis 已存在时 |',
        '|---|---|---|---|',
        '| 事务／非事务输出 | 上游 Flink keyed state，按 event_id | 输出带标记 JSON；事务输出随 CK 提交 | 无 Redis |',
        '| 直接 Streams EOS | Streams 持久化 state，按 event_id | 状态、源 offset、输出在 Kafka 事务中提交 | 无 Redis |',
        '| 纯 Redis：先登记 | sink 内 Redis，按 event_id | 原子检查并更新 seen → 加标记 → send/ACK | 标重复，仍然输出 |',
        '| 纯 Redis：后登记 | sink 内 Redis，按 event_id | 查 seen → 加标记 → send/ACK → 更新 seen | 标重复，仍然输出 |',
        '| state＋Redis：先登记 | 上游 Flink state，按 event_id | 按 input_seq 查并预约 → send/ACK | 跳过该次输入；预约不代表已发送 |',
        '| state＋Redis：后登记 | 上游 Flink state，按 event_id | 按 input_seq 查 sent → send/ACK → 登记 sent | 跳过该次输入 |','',
        '“纯 Redis”指业务首次/重复判断完全由 Redis 完成，不使用 Flink 业务 state；'
        '仍使用 Flink source/sink 读取输入、执行发送和恢复源进度，没有中间转发 topic。','',
        '**纯 Redis 完全可以“查 Redis → 写 Kafka → 再登记 Redis”。** 本实验已单独实现这条链路；'
        '它的 seen 键代表该业务 ID 是否出现过，不承担逐次输入的发送确认。state＋Redis 方案则把两件事分开：'
        'state 算业务标记，Redis 按 input_seq 记发送进度。生产中该编号需要稳定，例如源 topic／partition／offset，不能每次重试重新生成。','',
        '以上四条 Redis 链路的检查、登记、send/ACK 均在同一个 sink.invoke 中，仍不是跨系统原子事务。'
        '后登记方案在本次单 writer 下运行；并发 writer 的检查与写入竞争、Redis 数据丢失、登记过期后重放都未覆盖。','',
        '## 如何定点暴露问题','',
        '每次 Redis 恢复实验都先完成真实空 checkpoint，再让第一条业务输入进入 sink。'
        '任务在 Redis 已更新／尚未 send，或 Kafka 已 ACK／Redis 尚未更新的位置停住。'
        '独立客户端读取 Redis 和 Kafka 原始／已提交视图，确认窗口存在后释放任务抛 IOException，Flink 在原 JVM 自动恢复。'
        '选中的是源分区内该业务 ID 的首次输入，见证文件保存了 input_seq、false 标记、Redis 值和 Kafka ACK／offset。','',
        '事务与非事务 Flink 对照仍注入真实 snapshotState IOException，CK=30秒；'
        '直接 Streams EOS 在数据 ACK 后、事务提交前 SIGKILL 自有 JVM，再恢复同一 application.id 和状态目录。'
        '100ms 是 Streams 提交配置，实测可见时间见表，不是100ms延迟保证。','',
        '## Flink 转发那条链路为什么移出','',
        '旧链路“Flink 普通转发 → 中间 Kafka → Streams EOS”用来观察 EOS 的作用范围：'
        'Flink 已多写进中间 topic 的新 offset，会被下游视为新输入。它是额外诊断，已移出当前七方案比较。'
        '原来的14402条 Redis 结果来自旧的业务标记方案和 checkpoint 故障，不能替代本次定点窗口结果。','',
        '- [逐轮数据、原始／已提交计数与故障见证](ARRIVAL_DETAILS.md)',
        '- [旧版主报告](ARRIVAL_LEGACY_COMPARISON.md)／[旧版明细](ARRIVAL_LEGACY_DETAILS.md)',
        '- [单独的90000条持续负载诊断](STATE_STORE_PROBE.md)',
        '- [可复核 JSONL、原生见证与冻结源代码](arrival-v2-evidence/)／[新摘要](arrival-v2-summary.json)','']
    # Keep historical measurements and their original hashes intact.
    for name in ('COMPARISON','DETAILS'):
        old=results/f'ARRIVAL_{name}.md';archive=results/f'ARRIVAL_LEGACY_{name}.md'
        if old.exists() and not archive.exists():
            archive.write_text(old.read_text().replace('(ARRIVAL_DETAILS.md)','(ARRIVAL_LEGACY_DETAILS.md)')
                               .replace('(ARRIVAL_COMPARISON.md)','(ARRIVAL_LEGACY_COMPARISON.md)'))
    (results/'ARRIVAL_COMPARISON.md').write_text('\n'.join(lines))
    detail=['# 七方案逐轮审计与故障证据','',
            '主表见 [方案比较](ARRIVAL_COMPARISON.md)。缺失 input_seq 与额外重放分别统计；'
            '源标记错误表示同一 input_seq 的业务标记不符，输出顺序错误还会揭示多条 false 或首条就是 true。','',
            '固定依赖：Kafka Broker 3.7.0、Kafka Java Client／Streams 3.4.0、Flink 1.20.4、'
            'Flink Kafka Connector 3.4.0-1.20、Redis 7.2.6、Python 3.10.12。'
            '具体 JAR 和编译 class 的 SHA-256 见 [运行时清单](arrival-v2-runtime.sha256.json)。','']
    for run in runs:
        detail += [f"## {run['trial']}",'',f"版本：`{json.dumps(run['versions'],ensure_ascii=False)}`。",
                   f"执行：`{run.get('execution',{}).get('kind','container')}`；"
                   f"Java：`{run.get('execution',{}).get('java_version','见镜像固定版本').splitlines()[0]}`。",'',
                   '| 场景 | 原始条数 | 已提交条数 | 首次／重复 | 缺失输入 | 额外重放 | 源标记错误 | 输出顺序标记错误 | 无首次的ID | 多首次的ID |',
                   '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
        for c in run['cases']:
            a=c['views']['read_committed']['audit'];raw=c['views']['read_uncommitted']['audit']
            detail.append(f"| {c['name']} | {raw['output_arrivals']} | {a['output_arrivals']} | {a['false_markers']}／{a['true_markers']} | {a['missing_arrivals']} | {a['replayed_arrival_records']} | {a['flag_mismatches_against_source']} | {a['flag_mismatches_in_output_order']} | {a['business_ids_without_false']} | {a['business_ids_with_multiple_false']} |")
        detail += ['','每场已提交 distinct(event_id)=3000，完整业务载荷错误=0；这些总数不代表交付合同通过。','']
        for c in run['cases']:
            if c.get('sink_gap'):
                gap=c['sink_gap'][0];v=gap['marked_value'];boundary=c['boundary_verification']
                detail += [f"### {c['name']}",'',
                    f"故障输入：`event_id={v['event_id']}`，`input_seq={v['input_seq']}`，`is_duplicate=false`。"
                    f"故障位置 `{gap['stage']}`；Redis 键 `{gap['redis_key']}`，值 `{gap['redis_value']}`；"
                    f"Kafka ACK=`{gap['kafka_acked']}`、offset=`{gap['kafka_offset']}`。",'',
                    f"释放故障前独立读取：raw={boundary['raw_records']}，committed={boundary['committed_records']}，Redis=`{boundary['redis_value']}`。"
                    '恢复 attempt≥1 且 is_restored=true；全量处理后完成真实 checkpoint。', '',
                    f"[原生见证](arrival-v2-evidence/{run['trial']}/native/{c['name']}/sink-gap.json)／"
                    f"[外部读取见证](arrival-v2-evidence/{run['trial']}/native/{c['name']}/boundary-verification.json)",'']
        detail += [f"完整 [独立验证报告](arrival-v2-evidence/{run['trial']}/arrival-comparison.json)／"
                   f"[检查结果](arrival-v2-evidence/{run['trial']}/checks.json)。",'']
    detail += ['## 口径与限制','',
        '事务保证针对 read_committed；raw 中的中止事务数据仍计数。单机三 Broker、复制因子3、min ISR2、acks=all；'
        '单输入分区、处理并行度1；每场四线程目标3000次到达/秒，共9000次。'
        '业务 seen 使用滚动一小时处理时间窗口，每次到达刷新；发送登记也保留一小时。'
        'Redis 为逐条同步连接、AOF always，未做 pipeline。只通过 warmup 确认 worker／消费分配就绪，'
        '首次业务 state／输出路径未单独预热；本次仍是约3秒发送完9000条的整批统计。'
        '未真实等待一小时、未测多 writer、Broker／磁盘丢失、扩分区、长期稳态吞吐。'
        '故障验证时的主动停顿包含在恢复延迟中，不代表自然故障概率或正常服务延迟。','',
        '构建：`bash scripts/build.sh`。本地三 Broker 入口为 `scripts/run-arrival-native.py --help`；'
        '容器入口仍为 `harbor/arrival-marking-comparison`。'
        '报告命令：`python3 scripts/render-arrival-v2-report.py --verified results/local/<run>/verified [...]`。','']
    detail += ['离线复核：`python3 scripts/render-arrival-report.py --audit-only --verified results/arrival-v2-evidence/<run>`。','']
    (results/'ARRIVAL_DETAILS.md').write_text('\n'.join(detail))
    print(json.dumps({'report':'results/ARRIVAL_COMPARISON.md','runs':len(runs),'cases_per_run':len(CASE_NAMES)}))


if __name__=='__main__':main()
