# Checkpoint 失败实验：首次完整验证通过，复验进行中

配置：CK30秒、滚动1小时 ID 有效期、四线程目标3000条/秒，每场景30000条 JSON。

首次 `recovery-oracle-01` 已正常完成，Harbor报告验证得1；六个场景均由独立消费者重新逐条核对。第二次干净环境复验尚未完成，本文件不是最终稳定性结论。

报告得1只说明报告完整且与 Kafka 一致，不能理解为所有交付方案均正确。

| 场景 | 原始输出 | 已提交输出 | 已提交重放 | 已提交缺失 | 已提交标记错误 |
|---|---:|---:|---:|---:|---:|
| state-alo-timeout | 30000 | 30000 | 0 | 0 | 0 |
| state-eos-timeout | 30000 | 30000 | 0 | 0 | 0 |
| state-alo-restart | 60000 | 60000 | 30000 | 0 | 10000 |
| redis-alo-restart | 36973 | 36973 | 6973 | 0 | 0 |
| streams-after-alo-restart | 60000 | 60000 | 30000 | 0 | 0 |
| state-eos-restart | 60000 | 30000 | 0 | 0 | 0 |

`*-timeout` 使用真实8秒 snapshot 延迟触发5秒 CK 超时，容忍一次失败；原生abort通知已核对，任务保持attempt0，没有重启。

`*-restart` 在真实snapshotState抛一次IOException，由Flink自动恢复任务。原生attempt1和isRestored=true已核对。controller没有通过kill/relaunch JVM制造故障。

事务以read_committed核对业务消息一致性；原始视图包含中止事务记录。Redis和Streams在本轮标记正确，但非事务上游重放仍增加最终输出条数。

全部输入与两个输出视图均落盘为JSONL.gz：[首次原生报告和数据](recovery-evidence/)。最终报告将在复验后覆盖此文件。
