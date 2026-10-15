# Kafka Streams：业务 ID 过滤＋EOSv2

已实现真正的过滤：输入 Kafka key=event_id，持久化 RocksDB state 保存 last-seen，按1小时处理时间窗口去重，重复到达刷新状态但不转发，首次完整 JSON 写入 Kafka。state changelog、输入 offset 和输出在 EOSv2 事务中提交；commit.interval.ms=100。这条链路不包含 Flink，不等待 Flink checkpoint。

每个场景四线程目标3000次到达/秒，9000条合成 JSON，3000个业务 ID 各三次。9000是本轮人为选择的约3秒短批次，不是 Streams 数量限制；重复率也是受控测试配置。处理线程数1，源/目标均为1分区，三个 Broker、副本3、min ISR2。以相同输入规模的正常 Flink state＋事务 KafkaSink、CK30秒作为延迟对照。

原生 baseline `streams-filter-baseline-02` 正常得0；`streams-filter-oracle-01`、`streams-filter-oracle-02` 两轮均正常得1，各3个场景经独立 Kafka 消费者逐条核对。

| 运行 | 场景 | 原始输出 | 已提交输出 | 缺失 ID | 已提交额外重复 | 负载/首次选择错误 |
|---|---|---:|---:|---:|---:|---:|
| streams-filter-oracle-01 | streams-eos-filter-100ms | 3000 | 3000 | 0 | 0 | 0 |
| streams-filter-oracle-01 | streams-eos-filter-ack-crash | 3001 | 3000 | 0 | 0 | 0 |
| streams-filter-oracle-01 | flink-eos-filter-30s | 3000 | 3000 | 0 | 0 | 0 |
| streams-filter-oracle-02 | streams-eos-filter-100ms | 3000 | 3000 | 0 | 0 | 0 |
| streams-filter-oracle-02 | streams-eos-filter-ack-crash | 3002 | 3000 | 0 | 0 | 0 |
| streams-filter-oracle-02 | flink-eos-filter-30s | 3000 | 3000 | 0 | 0 | 0 |

## 实测已提交可见延迟

起点为每条源 JSON 的 ingested_at_ms，终点为预先分配的 read_committed 消费者首次观察到输出。原生观测记录、offset 和完整输出逐条匹配，并由验证器重算分位数。warmup 确认应用启动和源消费分配完成；未单独扣除首次业务 state/输出路径初始化。这是短批次实验，没有长时间稳态压测。故障行包含停机、恢复和未决事务的等待，不作为正常运行延迟。

| 运行 | 场景 | 实际源发送/s | p50 ms | p95 ms | p99 ms | 最大 ms | 100ms内/样本数 |
|---|---|---:|---:|---:|---:|---:|---|
| streams-filter-oracle-01 | streams-eos-filter-100ms | 2994.6 | 5563 | 5803 | 5889 | 5902 | 0/3000 |
| streams-filter-oracle-01 | streams-eos-filter-ack-crash | 2996.7 | 21880 | 22249 | 22301 | 22330 | 0/3000 |
| streams-filter-oracle-01 | flink-eos-filter-30s | 2997.9 | 27273 | 28497 | 28634 | 28727 | 0/3000 |
| streams-filter-oracle-02 | streams-eos-filter-100ms | 2975.8 | 6405 | 6796 | 6844 | 6872 | 0/3000 |
| streams-filter-oracle-02 | streams-eos-filter-ack-crash | 2979.7 | 18708 | 18908 | 18985 | 19002 | 0/3000 |
| streams-filter-oracle-02 | flink-eos-filter-30s | 2995.3 | 26902 | 28169 | 28316 | 28383 | 0/3000 |

## 分开观察写入与提交可见性

每条消息的起点是源 Producer 在 send 前写入 JSON 的 ingested_at_ms；终点是预先分配的目标 read_committed 消费者首次读到该条记录的 observed_at_ms。另有 read_uncommitted 消费者记录原始可见时间。

下表第三段按同一 Kafka 输出 offset 和完整 value 逐条配对后求分位数，**不是直接相减两个 p50。** 它包含两个独立消费者的网络与调度差异，近似观察事务可见等待，不是 Broker 内部精确 commit 耗时；一般可能出现负观测差。

| 运行 | 正常场景 | 源→原始可见 p50 ms | 源→已提交可见 p50 ms | 配对观察差 p50 ms | 配对观察差 p99 ms | 负观察差样本 |
|---|---|---:|---:|---:|---:|---:|
| streams-filter-oracle-01 | streams-eos-filter-100ms | 5416 | 5563 | 135 | 228 | 0 |
| streams-filter-oracle-01 | flink-eos-filter-30s | 389 | 27273 | 26888 | 28051 | 0 |
| streams-filter-oracle-02 | streams-eos-filter-100ms | 6270 | 6405 | 121 | 239 | 0 |
| streams-filter-oracle-02 | flink-eos-filter-30s | 433 | 26902 | 26268 | 27869 | 0 |

本轮 Streams 的主要延迟已经发生在目标原始日志可见之前，不能将全部端到端时间归为 EOS 提交等待；该段具体瓶颈尚未单独定位。

**本轮正常 Streams 并未在两次运行中都达到 p99≤100ms。** 100ms 是事务提交配置，不能写成已经实现的端到端 SLA。

## 故障如何验证

ProducerInterceptor 在真实输出 Produce ACK 回调中写 witness，并暂停 Sender，让事务保持未提交。controller 独立读到 ACK offset 对应的原始数据，确认已提交视图为空，再 SIGKILL 自己启动的 JVM。随后以相同 application.id 和持久 state 目录重启。核对两个不同 PID 的原生 RUNNING 生命周期、state restore 回调、最终完整业务输出，并由 AdminClient 独立确认源 committed offset 已越过全部9000条输入及 warmup。

验证器还要求：故障 ACK offset 保留在原始视图，而在已提交视图中消失；最终每个 ID 只有一条完整首次 JSON。原始视图中的中止事务记录仍可能重复，**EOS 保证针对 read_committed，不是原始 Kafka 日志物理不重复。**

此处是 Streams 事务提交前的进程故障，不是 Flink CK 失败。本报告的 Flink CK30s 行是正常延迟对照；真实 CK snapshot 异常及自动恢复另见 [业务 ID 故障实验](DEDUPLICATION.md)。故障实验 transaction.timeout.ms=10000，用于限定遗留事务恢复等待；生产配置要结合最长处理和恢复时间重新定。未测试 Broker/磁盘故障、扩分区、多机故障及实际等待1小时到期。Streams 的1小时按 last-seen 逻辑判断，过期 key 按访问覆盖，尚未实现后台过期清理。当前吞吐和延迟属于这份原型与共享单机环境。

## 代码与复现

- [Streams 过滤与事务 ACK 故障](../src/main/java/lab/StreamsDeduplicationDemo.java)
- [controller 与同负载 Flink 对照](../harbor/streams-id-filter/environment/streams_filter.py)
- [独立消费者、offset 和计时检查](../harbor/streams-id-filter/tests/verify.py)
- [完整源/输出/观测 JSONL 及哈希](streams-filter-evidence/)

```bash
bash scripts/build.sh
harbor trials start -p harbor/streams-id-filter -a oracle \
  --trial-name streams-filter --trials-dir results/trials
```

官方处理与配置依据：[Kafka Streams EOSv2](https://kafka.apache.org/34/streams/developer-guide/config-streams/)。
