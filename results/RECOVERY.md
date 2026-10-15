# Checkpoint 失败实验：验证进行中

当前主实验配置：30秒 CK、滚动1小时 ID 有效期、四线程目标3000条/秒，
每个场景30000条 JSON。输入和输出都保留 input_seq，最终落盘 JSONL。

截至当前首次运行，已经观察到：

| 场景 | 源输入 | 已提交输出 | 重放 | 错误标记 | 原生任务状态 |
|---|---:|---:|---:|---:|---|
| CK超时，非事务sink，容忍一次失败 | 30000 | 30000 | 0 | 0 | attempt0，没有重启 |
| CK超时，事务sink，容忍一次失败 | 30000 | 30000 | 0 | 0 | attempt0，没有重启 |
| snapshot异常，state＋非事务sink | 30000 | 60000 | 30000 | 10000 | attempt1，isRestored=true |

以上来自首次原生运行的已完成场景，不是完整验证结论。Redis、下游Streams、
事务恢复对照及整体验证仍在运行；本报告会在完整复验后替换。

故障来自真实 snapshotState 的超时或 IOException，由 Flink 自动执行失败策略。
controller 不通过强杀/重启 JVM 来制造故障。已提交视图与包含中止事务的原始
视图分开统计，单个策略的交付正确性也与 Harbor 报告验证得分分开。

代码：[CheckpointFailureDemo.java](../src/main/java/lab/CheckpointFailureDemo.java)、
[恢复controller](../harbor/id-marking-recovery/environment/recovery.py)。
