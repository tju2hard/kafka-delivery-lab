# 状态后端与持续输入定位（附录）

主报告的9000条短批次数据保持原样，本页为额外定位：正常场景改为每秒3000条、持续30秒，共90000条。顺序为 RocksDB、内存、内存、RocksDB，各使用新应用和新 topic；EOS、100ms提交、changelog、1小时逻辑重复判断规则均保留。使用 JFR 采样，有观测开销，不能直接当作无采样生产性能。

固定按输入发送时间取0～3秒、10～20秒、20～30秒三个窗口，窗口在实验前确定。所有90000条仍做完整总量和标记核对。

| 顺序/后端 | 输入/已提交输出 | 首次/重复标记 | 最后10秒样本数 | 最后10秒端到端 p50 / p99（ms） |
|---|---:|---:|---:|---:|
| streams-rocksdb-long-1 | 90000 / 90000 | 30000 / 60000 | 30002 | 2607 / 4707 |
| streams-memory-long-2 | 90000 / 90000 | 30000 / 60000 | 30001 | 165 / 756 |
| streams-memory-long-3 | 90000 / 90000 | 30000 / 60000 | 29999 | 122 / 232 |
| streams-rocksdb-long-4 | 90000 / 90000 | 30000 / 60000 | 30000 | 104 / 204 |

后跑的 RocksDB 同样能在固定后期窗口达到约200ms的 p99，因此现有结果不支持“6秒主要就是 RocksDB 导致、换内存就解决”的结论。各方案启动初期都有明显积压，持续运行后可见延迟下降；尚未定位到具体阻塞方法，不能把全部原因归给某个组件。局部窗口达到亚秒不代表整场 p99、每次启动或生产环境都有同样保证。

另做内存状态的9000条 ACK 后、事务提交前崩溃恢复：最终9000条，3000条首次、6000条重复，逐条核对通过。这验证了该受控故障下的恢复，没有测试所有故障、跨小时清理或大规模内存容量。

Flink 的旧 FsStateBackend 是内存状态与文件系统 CK 存储的组合；Kafka Streams 的内存状态仍可通过 Kafka changelog 恢复。两者的“本地状态存储”和“持久化/恢复途径”需要分别看。依据：[Flink 后端说明](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/state_backends/#fsstatebackend)、[Streams 状态存储说明](https://kafka.apache.org/34/streams/developer-guide/processor-api/#state-stores)。

- [完整数据与观测文件](state-probe-evidence/) / [机器可读结果、逐场验证和源码哈希](state-probe-summary.json)
- [主报告：9000条的总量、标记和平均/p99时间](ARRIVAL_COMPARISON.md)

复现（只管理本实验启动的服务，原始 JFR 和日志保存在 results/local/）：

```bash
bash scripts/build.sh
docker build -q -t kafka-delivery-lab/streams-state-probe:local harbor/arrival-marking-comparison/environment
bash scripts/run-streams-state-probe.sh
```
