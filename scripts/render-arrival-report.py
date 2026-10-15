"""Publish only independently verified, frozen native all-arrivals experiments."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
ROOT=Path(__file__).resolve().parents[1]
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def values(path):
    with gzip.open(path,'rt',encoding='utf-8') as f:return [json.loads(line) for line in f]
def percentile(xs,p):
    xs=sorted(xs);return xs[int((len(xs)-1)*p)]

def plain_language_report(runs):
    """Put the three user-facing choices before the detailed fault controls."""
    def case(run,prefix,recovery=False):
        name=prefix+('-recovery' if recovery else '-normal')
        return next(c for c in run['cases'] if c['name']==name)
    def count(prefix,recovery,key):
        counts={case(r,prefix,recovery)['views']['read_committed']['audit'][key] for r in runs}
        assert len(counts)==1,(prefix,recovery,key,counts)
        return next(iter(counts))
    def seconds(prefix,recovery,metric):
        times=[case(r,prefix,recovery)['views']['read_committed']['visibility_ms'][metric]/1000 for r in runs]
        return f'{min(times):.1f}～{max(times):.1f}秒'
    schemes=(('Flink 事务写 Kafka（CK＝30秒）','flink-state-eos'),
             ('直接 Kafka Streams EOS','streams-direct-eos'),
             ('Redis 先登记，再普通写 Kafka','redis-sink-alo'))
    for run in runs:
        assert run['source_arrivals_per_case']==9000 and run['unique_business_ids']==3000
    lines=['# 9000条消息：不重不漏、标记正确、秒级可见','',
        '**目标是让每次原始输入只输出一次、不漏消息、重复标记正确，同时让下游在秒级看到消息。**'
        '“不重”指故障恢复不能额外多写；原本就输入了三次的同一个业务 ID，三次都要保留。','',
        '这次用同一套规则测试三种主要方案：Flink 事务写 Kafka、Redis 配合普通 Kafka 写入、'
        '直接 Kafka Streams EOS。每种方案都测了正常运行和故障恢复，完整重复了两轮。','',
        '**本次两轮实验中，直接 Streams EOS 保持了9000条输出且标记正确；'
        '正常运行99%的消息在约6.5～6.9秒内可见，包含崩溃恢复时为约18.6～20.3秒。**'
        '这说明当前原型达到数秒量级的正常可见延迟，还没有证明1秒以内可见。'
        'Flink 事务方案同样保持消息正确，但本次 CK＝30秒，正常可见等待约为几十秒。'
        'Redis 先登记方案则在恢复后多写了消息。下面分别列出条数和实际等待时间。','',
        '## 先说清楚这9000条','',
        '**一共9000条输入，包含3000个业务 ID，每个 ID 出现三次。重复消息也必须输出。**','',
        '比如同一个 ID 来三次，下游应收到三条：第一条标“首次”，后两条标“重复”。'
        'JSON 中分别写成 `is_duplicate=false` 和 `is_duplicate=true`。','',
        '**正确结果：下游一共收到9000条，其中3000条标“首次”、6000条标“重复”；'
        '即使程序出故障再恢复，也应该保持这个结果。**','',
        '## 最后收到多少条，重复标记是否正确','',
        '下面的条数在两轮实验中完全一致，消息内容、每次输入的输出次数和重复标记均逐条核对。'
        '这里统计的是下游业务消费者真正能读到的消息。','',
        '| 方案 | 正常时收到 | 故障恢复后收到 | 恢复后的首次 / 重复标记 | 不重不漏且标记正确？ |',
        '|---|---:|---:|---:|---|']
    for label,prefix in schemes:
        normal=count(prefix,False,'output_arrivals');recovered=count(prefix,True,'output_arrivals')
        first=count(prefix,True,'false_markers');duplicate=count(prefix,True,'true_markers')
        passed=count(prefix,True,'arrival_contract_pass')
        verdict='是，两轮均正确' if passed else f'否，额外收到{recovered-9000}条'
        lines.append(f'| {label} | {normal}条 | {recovered}条 | {first} / {duplicate}条 | {verdict} |')
    lines += ['| Redis 先写 Kafka，收到成功回复后再登记 | 尚未测试 | 尚未测试 | 尚未测试 | 本次缺少这项结果 |','',
        '正常运行时，三个已测方案都是9000条，标记都是3000条首次、6000条重复。','',
        '**Redis 先登记方案的问题在恢复后：原来的9000条都还在，另外多出了5402条重放消息。**'
        '这5402条也被标成了重复，所以最终变成3000条首次、11402条重复，总共14402条。'
        '它没有达到“原始每次输入只输出一次”的目标。','',
        '## 正常运行时，下游等多久能看到','',
        '从“发送这条输入消息”开始计时，到“下游消费者第一次读到这条输出消息”为止。'
        '因此这里包含处理、排队、写入 Kafka 和事务提交等待，表示实际端到端可见时间。','',
        '| 方案 | 一半消息在多久内可见（p50） | 99%的消息在多久内可见（p99） |',
        '|---|---:|---:|']
    for label,prefix in schemes:
        lines.append(f'| {label} | {seconds(prefix,False,"p50")} | {seconds(prefix,False,"p99")} |')
    lines += ['| Redis 写成功后再登记 | 尚未测试 | 尚未测试 |','',
        '表中范围是两轮测得的分位数，例如“5.7～6.0秒”表示两轮 p50 分别落在这个范围，'
        '不是每条消息都恰好等这么久。Redis 当前逐条同步执行，未做批量优化，这个速度不代表 Redis 的性能上限。','',
        '## 如果发生故障，多久能看到','',
        '这一张表把故障、停机恢复和重放等待也算进去。故障恢复的时间与正常延迟分开看。','',
        '| 方案 | 一半输出在多久内可见（p50） | 99%的输出在多久内可见（p99） |',
        '|---|---:|---:|']
    for label,prefix in schemes:
        lines.append(f'| {label} | {seconds(prefix,True,"p50")} | {seconds(prefix,True,"p99")} |')
    lines += ['| Redis 写成功后再登记 | 尚未测试 | 尚未测试 |','',
        'Redis 故障行统计实际收到的14402条输出，包含额外重放；其他两行各统计9000条。'
        'Flink 的故障是一次真实 CK 异常导致自动恢复；直接 Streams 的故障是消息写入获确认、'
        '但事务尚未提交时杀死并重启本实验进程。两者不是完全相同的故障注入方式。','',
        '## CK＝30秒和 Streams＝100ms，分别是什么意思','',
        '- **Flink 事务写入：等对应 CK 成功，提交后下游才能看到那一批消息。** 本次 CK 是30秒。'
        '如果配置成3分钟，提交等待就受3分钟的 CK 周期及 CK 完成耗时影响；单条消息等待多久还取决于它在周期中的位置。',
        '- **Streams EOS：以100ms为目标周期提交 Kafka 事务，不需要等 Flink CK。** '
        '这是提交周期；本次实测端到端 p50 仍为5.7～6.0秒，尚未达到100ms端到端可见。',
        '- 如果 Streams 前面仍接着一个要等 CK 的 Flink 事务输出，它仍要先等上游把消息提交出来。','',
        '## 之前表里的18000条，是哪来的','',
        '18000条来自一项额外的串联对照：**Flink 普通转发→中间 Kafka→Streams EOS**。'
        '这项对照与直接 Streams 的9000条结果分开看。','',
        '1. 先完成一次没有业务消息的 CK，保存进度0。',
        '2. 输入9000条，Flink 普通转发先写出9000条。',
        '3. 下一次 CK 失败，作业恢复到进度0，又把这9000条写了一遍。',
        '4. 中间 Kafka 已经有18000条，后面的 Streams 因而收到18000条、输出18000条。','',
        '**直接 Streams EOS 两轮始终是9000条输入、9000条已提交输出，标记正确。**'
        '串联对照的18000条说明上游重放已经多写，整条链路没有达到目标。','',
        '## Redis 两种顺序，目前测到了哪一步','',
        '- **先登记：** 查 Redis→登记“见过”→发送 Kafka。本次已测，两轮 CK 失败恢复后均为14402条。',
        '- **后登记：** 查 Redis→发送 Kafka→收到成功回复→登记“见过”。当前保留全部消息的实验尚未测试。','',
        '两步之间发生故障时，先登记可能使首次消息在恢复后被错标为重复；后登记可能让已经写出的首次消息再被写出、并再次标为首次。'
        '这些是两种顺序需要单独验证的窗口，本次 CK 失败没有逐一覆盖它们。'
        '此前 [过滤重复 ID 的实验](DEDUPLICATION.md) 测过两种顺序，但那里的重复消息会跳过输出，结果不能套用到这次。','',
        '## 实验范围和完整记录','',
        '本次为合成 JSON，单机、单分区、处理并行度1，四线程目标每秒发送3000条，约3秒发完9000条。'
        '配置了1小时重复判断窗口，但没有实际等待1小时测试到期。'
        '这是短批次实验，没有证明长期稳定吞吐或生产延迟；首次业务处理路径也未单独做充分预热。','',
        '“下游可见”按 `read_committed` 消费者统计。原始 Kafka 日志可能保留中止事务的数据，不能把它的条数当成业务消费者最终收到的条数。','',
        '- [两轮逐场明细、额外对照和复现命令](ARRIVAL_DETAILS.md)',
        '- [9条真实实验输入](examples/arrival-marked-input.jsonl) / [对应9条带标记输出](examples/arrival-marked-output.jsonl)',
        '- [全部输入、输出和观测 JSONL](arrival-evidence/) / [机器可读摘要](arrival-summary.json)',
        '- [验证记录](arrival-validation.json) / [离线复核与反例检查](arrival-offline-controls.json)','']
    return '\n'.join(lines)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--baseline',required=True);parser.add_argument('--oracle',nargs=2,required=True);args=parser.parse_args()
    results=ROOT/'results';evidence=results/'arrival-evidence';evidence.mkdir(exist_ok=True)
    manifest=results/'arrival-inputs.sha256.json'
    for name,expected in json.loads(manifest.read_text()).items():
        assert any(p.exists() and hashlib.sha256(gzip.decompress(p.read_bytes()) if p.suffix=='.gz' else p.read_bytes()).hexdigest()==expected for p in (ROOT/name,evidence/'inputs'/name,evidence/'inputs'/(name+'.gz'))),name
    metadata=[];runs=[]
    for name,reward in [(args.baseline,0),*((n,1) for n in args.oracle)]:
        trial=results/'trials'/name;native=json.loads((trial/'result.json').read_text());checks=json.loads((trial/'verifier/checks.json').read_text())
        assert native['exception_info'] is None and native['verifier_result']['rewards']['reward']==reward
        assert checks['report_validation_reward']==reward and len(checks['checks'])==(10 if reward else 0)
        if reward:assert not checks['errors']
        metadata.append({'trial':name,**{k:native[k] for k in ('started_at','finished_at','exception_info','verifier_result')}})
        shutil.copyfile(trial/'verifier/checks.json',evidence/(name+'-checks.json'))
        if not reward:continue
        report=json.loads((trial/'verifier/arrival-comparison.json').read_text())
        for case in report['cases']:
            for info in case['exported_jsonl']:
                path=trial/'verifier'/info['path'];assert digest(path)==info['gzip_sha256']
                with gzip.open(path,'rb') as stream:raw=stream.read()
                assert hashlib.sha256(raw).hexdigest()==info['uncompressed_sha256'] and len(raw.splitlines())==info['rows']
        shutil.copyfile(trial/'verifier/arrival-comparison.json',evidence/(name+'-comparison.json'))
        shutil.copytree(trial/'verifier/data',evidence/name/'data',dirs_exist_ok=True)
        runs.append({'trial':name,**report})
    assert len({r['run_id'] for r in runs})==2
    (results/'arrival-summary.json').write_text(json.dumps({'input_manifest_sha256':digest(manifest),'native_trials':metadata,'runs':runs},ensure_ascii=False,indent=2)+'\n')
    (evidence/'trials.json').write_text(json.dumps(metadata,indent=2)+'\n')
    labels={'flink-state-alo':'Flink state＋普通 sink','redis-sink-alo':'先登记 Redis→普通 Kafka 发送',
        'streams-direct-eos':'原始 Kafka→Streams EOS','flink-alo-streams-eos':'Flink 非事务转发→中间 Kafka→Streams EOS','flink-state-eos':'Flink state＋事务 sink'}
    lines=['# 全量输出＋重复标记：总量、标记与 Kafka sink 延迟','',
        '**每次输入都输出，重复业务 ID 只增加 is_duplicate=true，绝不因为重复而跳过 sink。**','',
        '每场9000条合成 JSON、3000个 ID 各三次；四线程目标3000次到达/秒，约3秒短批次。'
        '理想结果：输出9000条，去重后 ID 数3000，false标记3000条，true标记6000条。'
        '“去重后 ID 数”只是统计 distinct(event_id)，不会删除输出。按源 Kafka 分区 offset 定义首次。'
        '滚动1小时处理时间窗口，每次到达刷新。单机、单分区、处理并行度1，三Broker、副本3、minISR2。','',
        f'Harbor 基线 `{args.baseline}` 正常得0；`{args.oracle[0]}`、`{args.oracle[1]}` 两轮正常得1，'
        '每轮10场，由独立原生消费者复读并逐条比较 JSON、源序号、标记和观测时间。'
        'reward=1表示报告有效，不表示所有被比较方案都满足交付要求。','',
        '## 输入总数与 Streams 的事务范围','',
        '**9000条是每个场景的原始输入总数，已包含3000个 ID 各出现三次；不是9000条唯一 ID 再加重复消息。**','',
        '直接 Streams EOS 两轮恢复后都是9000条。串联链路在 Flink 非事务恢复时，'
        '把原始输入重放写入中间 Kafka，中间 topic 已有18000条不同 offset 的记录。'
        '下游 Streams 保留每次输入并增加标记，因此读取18000条、输出18000条。'
        '该串联链路未达到原始9000条输入对应9000条输出的端到端目标。','',
        '| 运行 | 恢复链路 | 原始输入总数 | Streams 实际入口条数 | 最终已提交输出 |',
        '|---|---|---:|---:|---:|']
    for run in runs:
        for name in ('streams-direct-eos-recovery','flink-alo-streams-eos-recovery'):
            case=next(c for c in run['cases'] if c['name']==name)
            audit=case['views']['read_committed']['audit']
            # For the bridge, the native verifier freshly reread the staging
            # topic and asserted its business-record count equals output count.
            entry_count=audit['output_arrivals'] if case.get('staging_topic') else audit['source_arrivals']
            label='原始 Kafka→Streams EOS' if not case.get('staging_topic') else 'Flink 非事务转发→中间 Kafka→Streams EOS'
            lines.append(f"| {run['trial']} | {label} | {audit['source_arrivals']} | {entry_count} | {audit['output_arrivals']} |")
    lines += ['',
        'EOS 协调的是 Streams 实际入口的 offset、状态和输出；上游向中间 topic 额外写入的新 offset，'
        '会被视为新的输入记录。业务 ID 重复标记不等于识别并消除原始输入的故障重放。','',
        '## Redis 两种顺序的实验覆盖','',
        '| 顺序 | 本次全量输出标记实验 | 关键故障窗口 |',
        '|---|---|---|',
        '| 查 Redis→登记 seen/刷新 TTL→写 Kafka | 已测正常与 CK 失败恢复，两轮 | Redis 已更新、Kafka 尚未成功写入时故障，恢复后首次消息可能被错误标为重复 |',
        '| 查 Redis→写 Kafka→等待 ACK→登记 seen/刷新 TTL | 本次尚未测试 | Kafka 已写入、Redis 尚未更新时故障，重放可能再次写出 false；普通发送也可能额外多写 |','',
        '本次 Redis 的14402条仅属于“先登记”实现。上表的关键窗口说明不是本次 CK 故障'
        '已经逐一注入的结果。此前 [ID 过滤实验](DEDUPLICATION.md) 测过先登记和后登记，'
        '但那里重复 ID 会跳过 sink，与当前保留全部到达的语义不同，不能套用其数字。','']
    for run in runs:
        lines += [f"## {run['trial']}",'',
            f"本轮实际源发送速率 {min(c['generation']['achieved_send_rate_per_sec'] for c in run['cases']):.1f}～{max(c['generation']['achieved_send_rate_per_sec'] for c in run['cases']):.1f} 条/秒；每个 ID 三次到达。",'',
            '| 方案 | 场景 | 已提交总条数 | 去重后 ID 数 | false/true 标记 | 缺失输入 | 故障额外重放 | 源首次标记错误 | 输出顺序标记错误 | 已提交 p50/p99 ms | 原始可见 p50 ms |',
            '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
        for c in run['cases']:
            recovered=c['name'].endswith('-recovery');prefix=c['name'].rsplit('-',1)[0]
            a=c['views']['read_committed']['audit'];d=c['views']['read_committed']['visibility_ms'];raw=c['views']['read_uncommitted']['visibility_ms']
            lines.append(f"| {labels[prefix]} | {'故障恢复' if recovered else '正常'} | {a['output_arrivals']} | {a['output_unique_business_ids']} | {a['false_markers']}/{a['true_markers']} | {a['missing_arrivals']} | {a['replayed_arrival_records']} | {a['flag_mismatches_against_source']} | {a['flag_mismatches_in_output_order']} | {d['p50']}/{d['p99']} | {raw['p50']} |")
        lines += ['']
    lines += ['## 延迟与故障口径','',
        '- 主延迟：每条输入 send 前的 ingested_at_ms → 目标 read_committed 消费者首次观察到对应输出；包含输入积压、处理、Kafka 写入、事务提交及消费者观察。',
        '- 原始可见延迟：read_uncommitted 观察时间；事务数据可能已写入但尚未对业务消费者可见。JSONL 落盘是审计副本，计时终点是消费者读到消息，而非文件 fsync。',
        '- 故障行包含从最初发送到恢复后可见的等待；额外重放记录也作为实际输出纳入延迟样本。不能把它当作正常延迟或只挑首次输出隐藏缺陷。',
        '- Flink CK=30秒，先完成真实空 CK 后开始业务输入。故障场景在真实 snapshotState 抛一次 IOException，Flink 在同一存活 JVM 中自动恢复；核对原生 attempt、isRestored 和恢复成功 CK。',
        '- 直接 Streams EOSv2、commit=100ms。故障发生在真实输出 ACK 后、事务提交前；独立确认 raw 中有记录、committed 为空后 SIGKILL 本实验 JVM，用同一 application.id/state 目录恢复。此故障与 Flink CK 失败不同。',
        '- Redis 的精确 seen-key 判断、刷新 TTL、增加标记、普通 Kafka send/ACK 均位于 sink.invoke。登记 seen 在发送前，二者不构成原子事务。使用单连接同步 Lua＋AOF always，未做 pipeline；性能不是 Redis 上限。',
        '- 直接 Streams 的 state changelog、源 offset 和输出事务协调；Flink 普通转发的重放已成为中间 Kafka 中的新记录，下游 EOS 不会自动识别为同一次源输入。',
        '- 比较 false/true 总数之外，还分别检查源 input_seq 的首次标记、最终输出顺序的标记、每个输入序号的次数和完整 JSON。相同总量或相同 distinct ID 数不代表正确。','',
        '原始日志可保留中止事务数据，EOS 的不多不少针对 read_committed。100ms 是配置，实测延迟见表，不是100ms SLA。'
        '本轮只测约3秒批次，热身确认分配/作业启动，未单独预热首次业务 state/输出路径。'
        '未测试跨小时到期、Broker/磁盘丢失、扩分区、跨机时钟或长期稳态吞吐。','',
        '完整数据：[arrival-evidence/](arrival-evidence/)，摘要：[arrival-summary.json](arrival-summary.json)。','',
        '```bash','bash scripts/build.sh','harbor trials start -p harbor/arrival-marking-comparison -a oracle '+chr(92),
        '  --trial-name arrival-comparison --trials-dir results/trials','```','']
    detail_lines=['# 两轮逐场实验明细','',
        '先看 [通俗对比报告](ARRIVAL_COMPARISON.md)，了解9000条消息最后收到多少条、标记是否正确、下游多久能看到。'
        '本页保留原始实验配置、额外对照、逐轮计数和复现命令。','']+lines[2:]
    (results/'ARRIVAL_DETAILS.md').write_text('\n'.join(detail_lines))
    (results/'ARRIVAL_COMPARISON.md').write_text(plain_language_report(runs))
    print(json.dumps({'report':'results/ARRIVAL_COMPARISON.md','runs':args.oracle,'cases_per_run':10}))
if __name__=='__main__':main()
