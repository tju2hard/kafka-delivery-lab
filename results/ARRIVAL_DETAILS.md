# 七方案逐轮审计与故障证据

主表见 [方案比较](ARRIVAL_COMPARISON.md)。缺失 input_seq 与额外重放分别统计；源标记错误表示同一 input_seq 的业务标记不符，输出顺序错误还会揭示多条 false 或首条就是 true。

固定依赖：Kafka Broker 3.7.0、Kafka Java Client／Streams 3.4.0、Flink 1.20.4、Flink Kafka Connector 3.4.0-1.20、Redis 7.2.6、Python 3.10.12。具体 JAR 和编译 class 的 SHA-256 见 [运行时清单](arrival-v2-runtime.sha256.json)。

## arrival-v2-native-01

版本：`{"kafka_broker": "3.7.0", "kafka_streams": "3.4.0", "flink": "1.20.4", "redis": "7.2.6", "python": "3.10.12"}`。
执行：`native processes`；Java：`openjdk version "17.0.20.1" 2026-08-18 LTS`。

| 场景 | 原始条数 | 已提交条数 | 首次／重复 | 缺失输入 | 额外重放 | 源标记错误 | 输出顺序标记错误 | 无首次的ID | 多首次的ID |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| flink-state-eos-normal | 9000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| flink-state-eos-recovery | 18000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| flink-state-alo-normal | 9000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| flink-state-alo-recovery | 18000 | 18000 | 6000／12000 | 0 | 9000 | 0 | 3000 | 0 | 3000 |
| streams-direct-eos-normal | 9000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| streams-direct-eos-recovery | 9002 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| redis-mark-before-normal | 9000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| redis-mark-before-recovery | 9000 | 9000 | 2999／6001 | 0 | 0 | 1 | 1 | 1 | 0 |
| redis-mark-after-normal | 9000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| redis-mark-after-recovery | 9001 | 9001 | 3001／6000 | 0 | 1 | 0 | 1 | 0 | 1 |
| state-redis-before-normal | 9000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| state-redis-before-recovery | 8999 | 8999 | 2999／6000 | 1 | 0 | 0 | 1 | 1 | 0 |
| state-redis-after-normal | 9000 | 9000 | 3000／6000 | 0 | 0 | 0 | 0 | 0 | 0 |
| state-redis-after-recovery | 9001 | 9001 | 3001／6000 | 0 | 1 | 0 | 1 | 0 | 1 |

每场已提交 distinct(event_id)=3000，完整业务载荷错误=0；这些总数不代表交付合同通过。

### redis-mark-before-recovery

故障输入：`event_id=event-00000`，`input_seq=2`，`is_duplicate=false`。故障位置 `after-redis`；Redis 键 `arrival-native-redis-mark-before-recovery-out:seen:event-00000`，值 `seen`；Kafka ACK=`False`、offset=`-1`。

释放故障前独立读取：raw=0，committed=0，Redis=`seen`。恢复 attempt≥1 且 is_restored=true；全量处理后完成真实 checkpoint。

[原生见证](arrival-v2-evidence/arrival-v2-native-01/native/redis-mark-before-recovery/sink-gap.json)／[外部读取见证](arrival-v2-evidence/arrival-v2-native-01/native/redis-mark-before-recovery/boundary-verification.json)

### redis-mark-after-recovery

故障输入：`event_id=event-00000`，`input_seq=0`，`is_duplicate=false`。故障位置 `after-ack`；Redis 键 `arrival-native-redis-mark-after-recovery-out:seen:event-00000`，值 `None`；Kafka ACK=`True`、offset=`0`。

释放故障前独立读取：raw=1，committed=1，Redis=`None`。恢复 attempt≥1 且 is_restored=true；全量处理后完成真实 checkpoint。

[原生见证](arrival-v2-evidence/arrival-v2-native-01/native/redis-mark-after-recovery/sink-gap.json)／[外部读取见证](arrival-v2-evidence/arrival-v2-native-01/native/redis-mark-after-recovery/boundary-verification.json)

### state-redis-before-recovery

故障输入：`event_id=event-00000`，`input_seq=0`，`is_duplicate=false`。故障位置 `after-redis`；Redis 键 `arrival-native-state-redis-before-recovery-out:sent:0`，值 `reserved`；Kafka ACK=`False`、offset=`-1`。

释放故障前独立读取：raw=0，committed=0，Redis=`reserved`。恢复 attempt≥1 且 is_restored=true；全量处理后完成真实 checkpoint。

[原生见证](arrival-v2-evidence/arrival-v2-native-01/native/state-redis-before-recovery/sink-gap.json)／[外部读取见证](arrival-v2-evidence/arrival-v2-native-01/native/state-redis-before-recovery/boundary-verification.json)

### state-redis-after-recovery

故障输入：`event_id=event-00000`，`input_seq=1`，`is_duplicate=false`。故障位置 `after-ack`；Redis 键 `arrival-native-state-redis-after-recovery-out:sent:1`，值 `None`；Kafka ACK=`True`、offset=`0`。

释放故障前独立读取：raw=1，committed=1，Redis=`None`。恢复 attempt≥1 且 is_restored=true；全量处理后完成真实 checkpoint。

[原生见证](arrival-v2-evidence/arrival-v2-native-01/native/state-redis-after-recovery/sink-gap.json)／[外部读取见证](arrival-v2-evidence/arrival-v2-native-01/native/state-redis-after-recovery/boundary-verification.json)

完整 [独立验证报告](arrival-v2-evidence/arrival-v2-native-01/arrival-comparison.json)／[检查结果](arrival-v2-evidence/arrival-v2-native-01/checks.json)。

## 口径与限制

事务保证针对 read_committed；raw 中的中止事务数据仍计数。单机三 Broker、复制因子3、min ISR2、acks=all；单输入分区、处理并行度1；每场四线程目标3000次到达/秒，共9000次。业务 seen 使用滚动一小时处理时间窗口，每次到达刷新；发送登记也保留一小时。Redis 为逐条同步连接、AOF always，未做 pipeline。只通过 warmup 确认 worker／消费分配就绪，首次业务 state／输出路径未单独预热；本次仍是约3秒发送完9000条的整批统计。未真实等待一小时、未测多 writer、Broker／磁盘丢失、扩分区、长期稳态吞吐。故障验证时的主动停顿包含在恢复延迟中，不代表自然故障概率或正常服务延迟。

构建：`bash scripts/build.sh`。本地三 Broker 入口为 `scripts/run-arrival-native.py --help`；容器入口仍为 `harbor/arrival-marking-comparison`。报告命令：`python3 scripts/render-arrival-v2-report.py --verified results/local/<run>/verified [...]`。

离线复核：`python3 scripts/render-arrival-report.py --audit-only --verified results/arrival-v2-evidence/<run>`。
