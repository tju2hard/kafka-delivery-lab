"""Publish only native, independently verified checkpoint-failure measurements."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--baseline',required=True)
    parser.add_argument('--oracle',nargs=2,required=True)
    args=parser.parse_args()
    results=ROOT/'results';evidence=results/'recovery-evidence';evidence.mkdir(exist_ok=True)
    manifest=results/'recovery-inputs.sha256.json'
    for name,digest in json.loads(manifest.read_text()).items():
        assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==digest,name
    metadata=[];reports=[]
    for name,reward in [(args.baseline,0),*((n,1) for n in args.oracle)]:
        trial=results/'trials'/name
        native=json.loads((trial/'result.json').read_text())
        assert native['exception_info'] is None
        assert native['verifier_result']['rewards']['reward']==reward
        checks=json.loads((trial/'verifier/checks.json').read_text())
        assert checks['report_validation_reward']==reward
        assert len(checks['checks'])==(6 if reward else 0)
        if reward:assert not checks['errors']
        metadata.append({'trial':name,**{k:native[k] for k in ('started_at','finished_at','exception_info','verifier_result')}})
        shutil.copyfile(trial/'verifier/checks.json',evidence/(name+'-checks.json'))
        if reward:
            shutil.copyfile(trial/'verifier/recovery-comparison.json',evidence/(name+'-comparison.json'))
            shutil.copytree(trial/'verifier/data',evidence/name/'data',dirs_exist_ok=True)
            reports.append(json.loads((trial/'verifier/recovery-comparison.json').read_text()))
    assert len({r['run_id'] for r in reports})==2
    summary={'report_reward_meaning':'complete truthful recovery report; individual delivery contracts are separate',
             'input_manifest_sha256':hashlib.sha256(manifest.read_bytes()).hexdigest(),'native_trials':metadata,
             'runs':[{'trial':name,**report} for name,report in zip(args.oracle,reports)]}
    (results/'recovery-summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    (evidence/'trials.json').write_text(json.dumps(metadata,indent=2)+'\n')
    lines=['# Checkpoint 失败后：消息完整性与 ID 标记', '',
        '每个场景四线程目标每秒3000条，输入30000条 JSON，10000个业务 ID 各出现三次。'
        '保留每次到达，按滚动1小时处理时间标记 is_duplicate。Flink 周期 CK=30秒，Streams commit=100ms。', '',
        '**主实验是 checkpoint 失败，而不是人工杀进程。** '
        '先完成真实空 checkpoint，再开始业务输入。场景分为：', '',
        '- `*-timeout`：真实 snapshotState 阶段暂停8秒，超过5秒 CK 超时；容忍一次失败。'
        '要求原生 abort 通知，作业保持 attempt0，下一次 CK 成功。',
        '- `*-restart`：真实 snapshotState 中抛一次 IOException，容忍次数0；'
        'Flink 自动恢复作业。要求原生 attempt≥1、isRestored=true 和恢复后的成功 CK。'
        'JVM 始终存活，controller 没有用 kill/relaunch 触发故障。', '',
        '最终完成后强杀本实验进程仅用于清理，不算故障注入。', '',
        f'Harbor 基线 `{args.baseline}` 得0；`{args.oracle[0]}`、`{args.oracle[1]}` '
        '两次完整实验均得1。1表示报告经过独立 Kafka 消费者逐条核对正确，'
        '不表示所有方案满足交付要求。', '',
        '## 已提交视图：逐条比较源输入与最终 JSON', '',
        '| 运行 | 场景 | 输入 | 已提交输出 | 多出的重放记录 | 缺失 | 负载错误 | 标记错误 | 每次输入恰好一次且标记正确 |',
        '|---|---|---:|---:|---:|---:|---:|---:|---|']
    for name,report in zip(args.oracle,reports):
        for case in report['cases']:
            a=case['views']['read_committed']['audit']
            lines.append(f"| {name} | {case['name']} | {a['source_arrivals']} | {a['output_arrivals']} | {a['replayed_arrival_records']} | {a['missing_arrivals']} | {a['payload_mismatches']} | {a['flag_mismatches_in_output_order']} | {'通过该场景' if a['arrival_contract_pass'] else '不满足'} |")
    lines+=['', '标记错误按最终输出的实际顺序核对：同一业务 ID 第一次应为 false，后续为 true。'
        '重复写入同一个 input_seq 仍是重放缺陷，即使它被正确标成 true。'
        '源 input_seq 唯一，payload、event_id、ingested_at_ms 必须完整保留。', '',
        '## 原始视图与事务可见性', '',
        '| 运行 | 场景 | 原始数据条数 | 已提交数据条数 | false 标记 | true 标记 | 多次 false 的 ID 数 | 从未 false 的 ID 数 |',
        '|---|---|---:|---:|---:|---:|---:|---:|']
    for name,report in zip(args.oracle,reports):
        for case in report['cases']:
            raw=case['views']['read_uncommitted']['audit'];a=case['views']['read_committed']['audit']
            lines.append(f"| {name} | {case['name']} | {raw['output_arrivals']} | {a['output_arrivals']} | {a['false_markers']} | {a['true_markers']} | {a['business_ids_with_multiple_false']} | {a['business_ids_without_false']} |")
    lines+=['', '事务恢复以 read_committed 检查输入/输出一致性。原始视图包含中止事务的数据，'
        '不能把这些原始数据与已提交业务消息混成一个计数。', '',
        '## 如何解释不同方案', '',
        '- CK 超时且没有状态回滚时，不能仅凭 CK 失败就推断非事务输出重复。'
        '这就是两个 tolerated-timeout 对照的作用。',
        '- Flink state＋非事务 sink：恢复点之后的输出不能随状态一起回滚；'
        '重放可能多写记录，回滚后的 seen 状态也可能再次产生 false。',
        '- Flink 内 Redis 标记＋非事务 sink：Redis 状态不随 CK 回滚；'
        '重放与输出的标记可能不同。需分别看额外记录、重复 false，以及有没有 ID 从未获得 false。'
        '当前 Redis 是逐条同步 Lua＋AOF/always，吞吐与排空速度不是方案上限。',
        '- 非事务 Flink→Streams EOS：下游事务保护中间 Kafka 的读取与输出，'
        '上游重放已经形成新的中间记录。当前实现保留这些记录并标记，不会自动删除它们。',
        '- Flink 事务 sink：恢复时将未完成事务排除在已提交视图之外，'
        '配合源进度和状态恢复，核对每个 input_seq 是否仅有一个正确结果。', '',
        '机制解释不能替代上面的实测数字。当前只覆盖单机、单分区、一次受控 CK 故障；'
        '没有验证 Broker 断电、磁盘损坏、rebalance 或实际等待1小时到期。', '',
        '## 代码与复现', '',
        '- [真实 CK 异常与自动恢复](../src/main/java/lab/CheckpointFailureDemo.java)',
        '- [controller 与逐条审计](../harbor/id-marking-recovery/environment/recovery.py)',
        '- [独立 Kafka 复读验证](../harbor/id-marking-recovery/tests/verify.py)',
        '- [全部输入/输出 JSONL、native witness 与哈希](recovery-evidence/)', '',
        '```bash','bash scripts/build.sh',
        'harbor trials start -p harbor/id-marking-recovery -a oracle \\',
        '  --trial-name my-recovery --trials-dir results/trials','```','',
        '每个场景的 `verifier/data/<case>/` 包含 input.jsonl.gz、output-read_committed.jsonl.gz、'
        'output-read_uncommitted.jsonl.gz。这些文件来自实际消费者，已逐条核对完整 JSON。', '']
    (results/'RECOVERY.md').write_text('\n'.join(lines))
    print(json.dumps({'report':'results/RECOVERY.md','runs':args.oracle,'cases_per_run':6}))


if __name__=='__main__':main()
