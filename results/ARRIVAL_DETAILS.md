# 两轮逐场实验明细

先看 [通俗对比报告](ARRIVAL_COMPARISON.md)，了解9000条消息最后收到多少条、标记是否正确、下游多久能看到。本页保留原始实验配置、额外对照、逐轮计数和复现命令。

**每次输入都输出，重复业务 ID 只增加 is_duplicate=true，绝不因为重复而跳过 sink。**

每场9000条合成 JSON、3000个 ID 各三次；四线程目标3000次到达/秒，约3秒短批次。理想结果：输出9000条，去重后 ID 数3000，false标记3000条，true标记6000条。“去重后 ID 数”只是统计 distinct(event_id)，不会删除输出。按源 Kafka 分区 offset 定义首次。滚动1小时处理时间窗口，每次到达刷新。单机、单分区、处理并行度1，三Broker、副本3、minISR2。

Harbor 基线 `arrival-baseline-01` 正常得0；`arrival-oracle-01`、`arrival-oracle-02` 两轮正常得1，每轮10场，由独立原生消费者复读并逐条比较 JSON、源序号、标记和观测时间。reward=1表示报告有效，不表示所有被比较方案都满足交付要求。

## 输入总数与 Streams 的事务范围

**9000条是每个场景的原始输入总数，已包含3000个 ID 各出现三次；不是9000条唯一 ID 再加重复消息。**

直接 Streams EOS 两轮恢复后都是9000条。串联链路在 Flink 非事务恢复时，把原始输入重放写入中间 Kafka，中间 topic 已有18000条不同 offset 的记录。下游 Streams 保留每次输入并增加标记，因此读取18000条、输出18000条。该串联链路未达到原始9000条输入对应9000条输出的端到端目标。

| 运行 | 恢复链路 | 原始输入总数 | Streams 实际入口条数 | 最终已提交输出 |
|---|---|---:|---:|---:|
| arrival-oracle-01 | 原始 Kafka→Streams EOS | 9000 | 9000 | 9000 |
| arrival-oracle-01 | Flink 非事务转发→中间 Kafka→Streams EOS | 9000 | 18000 | 18000 |
| arrival-oracle-02 | 原始 Kafka→Streams EOS | 9000 | 9000 | 9000 |
| arrival-oracle-02 | Flink 非事务转发→中间 Kafka→Streams EOS | 9000 | 18000 | 18000 |

EOS 协调的是 Streams 实际入口的 offset、状态和输出；上游向中间 topic 额外写入的新 offset，会被视为新的输入记录。业务 ID 重复标记不等于识别并消除原始输入的故障重放。

## Redis 两种顺序的实验覆盖

| 顺序 | 本次全量输出标记实验 | 关键故障窗口 |
|---|---|---|
| 查 Redis→登记 seen/刷新 TTL→写 Kafka | 已测正常与 CK 失败恢复，两轮 | Redis 已更新、Kafka 尚未成功写入时故障，恢复后首次消息可能被错误标为重复 |
| 查 Redis→写 Kafka→等待 ACK→登记 seen/刷新 TTL | 本次尚未测试 | Kafka 已写入、Redis 尚未更新时故障，重放可能再次写出 false；普通发送也可能额外多写 |

本次 Redis 的14402条仅属于“先登记”实现。上表的关键窗口说明不是本次 CK 故障已经逐一注入的结果。此前 [ID 过滤实验](DEDUPLICATION.md) 测过先登记和后登记，但那里重复 ID 会跳过 sink，与当前保留全部到达的语义不同，不能套用其数字。

## arrival-oracle-01

本轮实际源发送速率 2941.9～2999.8 条/秒；每个 ID 三次到达。

| 方案 | 场景 | 已提交总条数 | 去重后 ID 数 | false/true 标记 | 缺失输入 | 故障额外重放 | 源首次标记错误 | 输出顺序标记错误 | 已提交 p50/p99 ms | 原始可见 p50 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Flink state＋普通 sink | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 2122/2574 | 2122 |
| 先登记 Redis→普通 Kafka 发送 | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 41037/71798 | 41035 |
| 原始 Kafka→Streams EOS | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 5690/6864 | 5539 |
| Flink 非事务转发→中间 Kafka→Streams EOS | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 4579/5485 | 4478 |
| Flink state＋事务 sink | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 27791/29132 | 1105 |
| Flink state＋普通 sink | 故障恢复 | 18000 | 3000 | 6000/12000 | 0 | 9000 | 0 | 3000 | 2137/29134 | 2147 |
| 先登记 Redis→普通 Kafka 发送 | 故障恢复 | 14402 | 3000 | 3000/11402 | 0 | 5402 | 1801 | 0 | 58018/103893 | 58018 |
| 原始 Kafka→Streams EOS | 故障恢复 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 18262/18643 | 18201 |
| Flink 非事务转发→中间 Kafka→Streams EOS | 故障恢复 | 18000 | 3000 | 3000/15000 | 0 | 9000 | 3000 | 0 | 5708/31232 | 5613 |
| Flink state＋事务 sink | 故障恢复 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 33814/35137 | 2316 |

## arrival-oracle-02

本轮实际源发送速率 2979.2～2998.8 条/秒；每个 ID 三次到达。

| 方案 | 场景 | 已提交总条数 | 去重后 ID 数 | false/true 标记 | 缺失输入 | 故障额外重放 | 源首次标记错误 | 输出顺序标记错误 | 已提交 p50/p99 ms | 原始可见 p50 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Flink state＋普通 sink | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 2298/2805 | 2270 |
| 先登记 Redis→普通 Kafka 发送 | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 37981/68538 | 37981 |
| 原始 Kafka→Streams EOS | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 5999/6547 | 5872 |
| Flink 非事务转发→中间 Kafka→Streams EOS | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 4943/5219 | 4859 |
| Flink state＋事务 sink | 正常 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 26693/28193 | 1800 |
| Flink state＋普通 sink | 故障恢复 | 18000 | 3000 | 6000/12000 | 0 | 9000 | 0 | 3000 | 1923/29049 | 1929 |
| 先登记 Redis→普通 Kafka 发送 | 故障恢复 | 14402 | 3000 | 3000/11402 | 0 | 5402 | 1801 | 0 | 53423/99296 | 53423 |
| 原始 Kafka→Streams EOS | 故障恢复 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 19943/20309 | 19798 |
| Flink 非事务转发→中间 Kafka→Streams EOS | 故障恢复 | 18000 | 3000 | 3000/15000 | 0 | 9000 | 3000 | 0 | 4074/28934 | 3912 |
| Flink state＋事务 sink | 故障恢复 | 9000 | 3000 | 3000/6000 | 0 | 0 | 0 | 0 | 48491/49820 | 2026 |

## 延迟与故障口径

- 主延迟：每条输入 send 前的 ingested_at_ms → 目标 read_committed 消费者首次观察到对应输出；包含输入积压、处理、Kafka 写入、事务提交及消费者观察。
- 原始可见延迟：read_uncommitted 观察时间；事务数据可能已写入但尚未对业务消费者可见。JSONL 落盘是审计副本，计时终点是消费者读到消息，而非文件 fsync。
- 故障行包含从最初发送到恢复后可见的等待；额外重放记录也作为实际输出纳入延迟样本。不能把它当作正常延迟或只挑首次输出隐藏缺陷。
- Flink CK=30秒，先完成真实空 CK 后开始业务输入。故障场景在真实 snapshotState 抛一次 IOException，Flink 在同一存活 JVM 中自动恢复；核对原生 attempt、isRestored 和恢复成功 CK。
- 直接 Streams EOSv2、commit=100ms。故障发生在真实输出 ACK 后、事务提交前；独立确认 raw 中有记录、committed 为空后 SIGKILL 本实验 JVM，用同一 application.id/state 目录恢复。此故障与 Flink CK 失败不同。
- Redis 的精确 seen-key 判断、刷新 TTL、增加标记、普通 Kafka send/ACK 均位于 sink.invoke。登记 seen 在发送前，二者不构成原子事务。使用单连接同步 Lua＋AOF always，未做 pipeline；性能不是 Redis 上限。
- 直接 Streams 的 state changelog、源 offset 和输出事务协调；Flink 普通转发的重放已成为中间 Kafka 中的新记录，下游 EOS 不会自动识别为同一次源输入。
- 比较 false/true 总数之外，还分别检查源 input_seq 的首次标记、最终输出顺序的标记、每个输入序号的次数和完整 JSON。相同总量或相同 distinct ID 数不代表正确。

原始日志可保留中止事务数据，EOS 的不多不少针对 read_committed。100ms 是配置，实测延迟见表，不是100ms SLA。本轮只测约3秒批次，热身确认分配/作业启动，未单独预热首次业务 state/输出路径。未测试跨小时到期、Broker/磁盘丢失、扩分区、跨机时钟或长期稳态吞吐。

完整数据：[arrival-evidence/](arrival-evidence/)，摘要：[arrival-summary.json](arrival-summary.json)。

```bash
bash scripts/build.sh
harbor trials start -p harbor/arrival-marking-comparison -a oracle \
  --trial-name arrival-comparison --trials-dir results/trials
```
