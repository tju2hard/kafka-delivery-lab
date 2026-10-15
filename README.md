# kafka-delivery-lab

比较非事务转发、幂等 Producer、Redis、Outbox、日志扫描恢复、Kafka 事务和
Flink checkpoint 对消息交付的实际影响。当前是工程实验仓库，不是生产库，
也不是 TalentAI 出题包；不调用模型。

## 当前需求：全量输出，只标记重复

`harbor/arrival-marking-comparison` 对比五种方案在正常与故障恢复时的最终总条数、
去重后 ID 数、首次/重复标记、输入重放和 Kafka sink 可见延迟。每场9000次到达，
3000个 ID 各三次，四线程目标3000条/秒；理想输出仍为9000条，3000 false、6000 true。
重复消息必须输出。下面的 ID 过滤实验是不同语义，不能作为本需求的结果。

两轮原生实验及逐条复核已完成，见 [总量、重复标记与延迟对比](results/ARRIVAL_COMPARISON.md)。
实际 JSON 小样：[9条输入](results/examples/arrival-marked-input.jsonl)、
[9条带标记输出](results/examples/arrival-marked-output.jsonl)。

实现：[ArrivalStreamsDemo](src/main/java/lab/ArrivalStreamsDemo.java)、
[ArrivalCheckpointDemo](src/main/java/lab/ArrivalCheckpointDemo.java)。

## Kafka Streams：过滤重复 ID＋EOSv2

已实现 [StreamsDeduplicationDemo](src/main/java/lab/StreamsDeduplicationDemo.java)：
输入以 event_id 为 Kafka key，用持久化 state 做1小时去重，只转发首次 JSON，
EOSv2 协调 source offset、state changelog 和输出，commit.interval.ms=100。
这条链路没有 Flink，不等待30秒 CK。相同 ID 必须进入相同源分区；本实验固定1分区。

当前是每场景9000次到达、3000个 ID 各三次的约3秒短批次，四线程目标3000条/秒。
三项实验包括正常 Streams、真实 ACK 后且事务未提交时 SIGKILL/恢复，以及正常
Flink state＋事务 KafkaSink/CK30s 的同规模延迟对照。最终 JSON、源进度、原始和
已提交视图均由独立消费者复核，详见 [Streams 实测报告](results/STREAMS_FILTER.md)。
**100ms 是提交配置；当前实测端到端可见延迟仍为秒级，未达到100ms目标。**
按同一输出 offset 配对的“原始可见→已提交可见”观察差 p50 为121～135ms；
当前主要延迟已发生在目标原始日志可见之前，具体瓶颈尚未单独定位。

```bash
bash scripts/build.sh
harbor trials start -p harbor/streams-id-filter -a oracle \
  --trial-name streams-filter --trials-dir results/trials
```

## 按业务 ID 过滤：Redis 能否模拟事务的不重不漏

该过滤实验要求同一个业务 ID 最终只输出首次到达的一条完整 JSON，重复 ID 直接跳过
sink。新实现见 [DeduplicationDemo](src/main/java/lab/DeduplicationDemo.java)，
Harbor 入口为 `harbor/id-deduplication`。本轮每场景四线程目标3000条/秒，输入
9000条、3000个 ID 各三次；按完整业务 ID 集合和首次 JSON 核对，不能只比总数。

Redis 检查、Kafka 发送和 Redis 登记都在 `RedisSink.invoke()` 内：

```text
查 Redis sent → 已有则跳过 → 无则发送 Kafka → 等待 ACK → SET sent（1小时 TTL）
```

同一函数并不形成跨 Redis/Kafka 的原子提交。ACK 后、登记前发生故障，恢复会
重发；先登记再发则可能漏发。新实验在两个窗口分别抛真实任务异常，由 Flink
从初始 CK 自动恢复；另测真实 CK 失败及 state＋非事务/事务 sink。CK=30秒，
Redis 使用精确键而非 Bloom filter，重复到达刷新 TTL，单分区、parallelism=1。
“窗口很短”是风险大小问题，不等于 exactly-once 保证；本实验不估算自然故障概率。

两轮原生复验结果见 [不重不漏对比报告](results/DEDUPLICATION.md)，正常过滤的小例子
见 [9次输入与3条输出](results/examples/deduplication.md)。
可行路线及其条件见 [Kafka EOS 与 MQ 原生幂等追加](docs/feasible-exactly-once.md)；
其中 RabbitMQ Streams/Pulsar 是已核实官方机制的设计，尚未在本仓库实测。

事务对照的“不重不漏”只针对 `read_committed`；Kafka 原始日志仍可能存在中止事务
的数据。两种视图分别核对，避免把事务可见性称为原始日志物理不重复。

```bash
bash scripts/build.sh
harbor trials start -p harbor/id-deduplication -a oracle \
  --trial-name business-deduplication --trials-dir results/trials
```

## CK 失败与重复标记实验

新增 [失败与恢复报告](results/RECOVERY.md) 区分两种故障：真实 snapshot 延迟导致
checkpoint 超时但作业继续；以及 snapshot IOException 导致 Flink 自动从上次成功
checkpoint 恢复。两者都不靠 controller 强杀/重启 JVM 来制造故障。

CK=30秒，状态有效期为滚动1小时，输入仍为四线程目标每秒3000条，共3万条。
比较非事务 state 标记、非事务 Redis 标记、非事务 Flink→Streams EOS，以及
Flink 事务 sink。逐条统计丢失、源序号重放、负载错误和标记错误；事务原始视图
中的中止记录与已提交业务消息分开计数。

```bash
bash scripts/build.sh
harbor trials start -p harbor/id-marking-recovery -a oracle \
  --trial-name checkpoint-failure --trials-dir results/trials
python3 scripts/compare-recovery-jsonl.py \
  --input results/trials/checkpoint-failure/verifier/data/state-alo-restart/input.jsonl.gz \
  --output results/trials/checkpoint-failure/verifier/data/state-alo-restart/output-read_committed.jsonl.gz
```

离线检查退出码1表示发现预期的交付/标记缺陷。恢复检查的标记按最终输出顺序
判断，源序号重放另计；正常一对一负载使用下面的 `compare-jsonl.py`。

## JSON 标记与负载实验

此前的标记例子保留每次到达，按 `event_id` 标记 `is_duplicate`，并非删除重复消息。
去重有效期为滚动1小时处理时间，每次到达刷新；连续1小时未出现后重新标记首次。
Flink 使用 `keyBy(event_id)`＋带 TTL 的 `ValueState`；Streams 用 last-seen state；
Redis 用 Lua 原子检查并刷新 TTL。此旧标记实现登记的是“见过”，发生在 Kafka
sink 前，不能把它理解成上面新实现的“Kafka 已成功输出”。

四个线程目标每秒3000条，共30000条 JSON，10000个 ID 各出现三次。对比普通
Java 状态、Redis、Streams EOS/100ms、Flink 非事务/30s CK、Flink 非事务→
Streams EOS，以及 Flink 事务/30s CK。每种方式都消费真实 Kafka 输入并写回 Kafka，
最后由消费者落盘 JSONL，逐条比较序号、完整负载和标记。

实测见 [JSON 负载报告](results/TAGGING.md)。

```bash
bash scripts/build.sh
harbor trials start -p harbor/id-marking-latency -a oracle \
  --trial-name json-load --trials-dir results/trials
python3 scripts/compare-jsonl.py \
  --input results/trials/json-load/verifier/data/streams-eos-100ms/input.jsonl.gz \
  --output results/trials/json-load/verifier/data/streams-eos-100ms/output.jsonl.gz
```

生成器目标速率和实测速率分别记录，100ms commit 不等于100ms端到端延迟。
当前处理链路单分区、parallelism=1，数据生成器四线程并行；六种方式顺序运行。
本轮无故障注入，不把正常运行的数据一致性当成故障恢复保证，也未实际等待1小时到期。
Redis 标记实现为单连接逐条同步 Lua，AOF 使用 appendfsync=always，未做命令 pipeline。
这会影响排空积压的速度；对比的是这些具体实现，不代表各类方案的性能上限。
已有强杀/恢复实验与新负载实验的区别见 [异常注入说明](docs/fault-injection.md)。

## 原始日志交付实验

目标：每个已接受的业务事件最终写入 Kafka，且 Kafka 原始日志里只有一条该
事件的数据记录。消费者过滤重复和事务可见性单独统计，不能替代这个目标。

按主路线分为 **3种非事务实现＋1种事务对照**：

| 主路线 | 代码入口 | 实际使用的机制 |
|---|---|---|
| 直接转发 | `Lab.forward` | 普通/幂等 KafkaProducer、外部恢复游标 |
| Flink 去重 | `FlinkRun` | keyed state、checkpoint、非事务 KafkaSink |
| Redis 去重 | `Lab.forward` | SET NX、发送后登记、带 owner token 的租约 |
| 事务对照 | `Lab.kafkaTransaction`、`FlinkRun` | offset/output 同事务、Flink 事务 KafkaSink |

Outbox 和日志扫描是补充策略。各实现是可运行的实验原型，不是已经证明正确的
生产组件。Kafka 原生事务用于 Kafka 内部原子提交，不自动覆盖外部数据库事务。

## 实现和故障

| 实现 | 需要验证的边界 |
|---|---|
| 普通 Producer | Broker 已持久化，但 ACK 在网络中丢失 |
| 幂等 Producer | 同会话网络重试、应用重复 send、崩溃后新会话重放 |
| Redis 先占位再发 | 占位成功、发 Kafka 前被杀，恢复是否漏发 |
| Redis 先发后记 | Kafka ACK 后、Redis 记录前被杀，恢复是否重发 |
| Redis PENDING/租约 | 最后一次 ownership 检查后租约过期，新旧 writer 都发 |
| SQLite Outbox | 数据库已保存待发事件，Kafka ACK 后标记 SENT 前被杀 |
| Kafka 同 key/compaction | 活跃 segment 中重复 key 是否会在追加时被拒绝 |
| 重启扫描 Kafka | 已 ACK 的记录可查；旧网络请求在扫描后才到达时是否还安全 |
| 单独 Kafka 事务＋外部游标 | commit 成功、游标落盘前被杀 |
| Kafka 输入 offset 和输出同事务 | commit 前后分别被杀，比较原始和已提交视图 |
| Flink keyed state＋非事务 KafkaSink | Kafka 已写、下一次 checkpoint 尚未完成时恢复 |
| Flink keyed state＋事务 KafkaSink | 相同恢复路径，以及正常运行的可见延迟 |

每个实验使用独立 topic 和目录。事件 ID/负载是明确标注的合成实验输入，
不是生产数据。Kafka 为三节点、副本数3、min ISR2、acks=all；Redis 开启
AOF/always。Flink 使用真实运行时、真实 KafkaSink 和实际完成的 checkpoint。

网络故障代理只在专用 FAULT listener 上工作，不改消息。ACK 丢失实验先解析
成功的 ProduceResponse 再丢弃；延迟请求实验保留旧进程已经交出的完整请求，
在旧进程被杀、新 writer 扫描并发布后才转发。没有用“随机重启没有重复”作证明。

## 运行

```bash
bash scripts/build.sh
harbor trials start -p harbor/delivery-matrix -a nop \
  --trial-name baseline-01 --trials-dir results/trials
harbor trials start -p harbor/delivery-matrix -a oracle \
  --trial-name matrix-01 --trials-dir results/trials
```

`scripts/build.sh` 只在宿主下载公开 Maven 依赖，准备自包含 Docker build context。
当前宿主构建使用脚本指定的本地 JDK17；Harbor CLI 为0.24.0。
容器运行时没有外网、没有 Docker socket，也不使用生产凭证。Broker 和 Redis
的生命周期由 Harbor Compose 管理，controller 会清理自己启动的进程。

**Harbor reward=1 表示完整实验报告经独立消费者核对无误，不表示所有方案都
满足 exactly-once。** 每种方案的真实结果在 `verifier/comparison.json` 的
`physical_contract_pass`、`committed_view_contract_pass`、重复数、丢失列表里。
NOP 没有报告，报告验证得0；Oracle 执行全部实验，再独立重读 Kafka 核对报告。

固定版本：Kafka Broker3.7.0 / Java Client3.4.0、Redis7.2.6、Flink1.20.4 /
Kafka Connector3.4.0-1.20、Python3.10.12、主运行容器 Java17.0.19。
新增标记实验使用 Kafka Streams3.4.0。构建新增依赖前的原始实验输入哈希保留在
`results/validation-inputs.sha256.json`，原 `pom.xml` 和构建脚本存于
`results/evidence/delivery-matrix-inputs/`；新增实验有独立输入清单和原生报告。
这些是可复现的实验版本，不是生产版本推荐。主镜像各构建阶段及 Kafka/Redis
服务镜像均固定 digest。

实验不证明任意故障下的正确性。当前未测 Broker 宕机、磁盘丢失、扩分区、
保留期过期及生产吞吐；可见延迟是此拓扑下的观察值。

设计与官方依据见 [docs/design.md](docs/design.md)。实测结果见
[对比报告](results/COMPARISON.md) 和 [结构化摘要](results/summary.json)。
报告可从两次独立核对的原生结果重新生成：

```bash
python3 scripts/render-report.py --baseline closure-baseline \
  --oracle development-01 closure-oracle-01
```
