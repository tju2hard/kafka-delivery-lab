# Flink six-hour numeric-ID dedup: heap vs RocksDB

This is a standalone module of `kafka-delivery-lab`, not a Kafka Streams test.
It runs **actual Flink 1.20.4** local MiniClusters, in separate JVM processes,
sequentially. There are no external brokers, credentials, production endpoints,
HDFS, S3, paid models, or artificial value padding.

## Contract and input

- A fixed-seed (`20261114`) binary fixture mixes approximately **70% new IDs and
  30% duplicates**. Duplicates are uniformly sampled from IDs already generated.
- Each ID is an int64. Unique IDs use a fixed invertible 64-bit multiplication
  to avoid simply ascending keys. This is synthetic data, not a claim that its
  distribution equals production user IDs.
- `keyBy(id)` + `ValueState<Boolean>` + **6-hour processing-time TTL**.
  `OnCreateAndWrite` and `NeverReturnExpired`; every arrival writes `true`, so a
  duplicate refreshes the six-hour inactivity horizon. This is NOT a fixed
  six-hour horizon from the first arrival, nor event-time TTL.
- Only the first arrival for an unexpired ID is emitted downstream. The test
  completes inside the TTL window; no six-hour wait, expiry cleanup, recovery,
  or exactly-once sink guarantee is tested.
- Output contains `(id, input_seq)`. An independent Python oracle rereads the
  fixture and verifies all expected IDs, zero extra/missing/duplicate IDs, and
  **the exact first input sequence for each output ID**. It also checks the
  input/unique/duplicate counters, rather than trusting totals alone.

## Comparison

1. `HashMapStateBackend` (heap) + full checkpoints.
2. `EmbeddedRocksDBStateBackend(false)` + full checkpoints.
3. `EmbeddedRocksDBStateBackend(true)` + incremental checkpoints.

All cases share the fixture, parallelism 2, source parallelism 1, max parallelism
128, JVM `-Xms512m -Xmx2g`, direct-memory limit 1 GiB, managed memory 512 MiB,
network memory 64 MiB, two-CPU affinity and `ActiveProcessorCount=2`. These are
**limits/configuration, not a 1 GiB total-process budget**. Actual RSS, retained
heap and RocksDB directory size must be reported separately. JobManager and
TaskManager share one JVM in this MiniCluster, so RSS includes both.

Checkpoints are aligned, every 5 s, concurrency 1, timeout 120 s; three retained.
Both backends write to the same kind of **local filesystem**, on the same cloud
host. This isolates state implementation without pretending to benchmark an
HDFS cluster or S3 upload bandwidth. Large-scale cases are repeated in reverse
backend order. Each run starts with empty state and a fresh JVM/local DB.

## Reproduce

Requires a full **JDK 17** (experiment used Amazon Corretto 17.0.21.0.11), Maven
3.9.9, Linux `/proc`, and at least two available CPUs. No Docker required.

```bash
cd experiments/flink-state-backends
mvn -q compile dependency:copy-dependencies -DincludeScope=runtime
python3 test_runner.py
python3 run.py --output ../../results/local/my-state-run \
  --java "$JAVA_HOME/bin/java" --keys 250000 --label scale250k
python3 run.py --output ../../results/local/my-state-run \
  --java "$JAVA_HOME/bin/java" --keys 1000000 --label scale1m \
  --backends rocks-incremental,rocks-full,heap
python3 run.py --output ../../results/local/my-state-run \
  --java "$JAVA_HOME/bin/java" --keys 10000000 --label scale10m-r1
python3 run.py --output ../../results/local/my-state-run \
  --java "$JAVA_HOME/bin/java" --keys 10000000 --label scale10m-r2 \
  --backends rocks-incremental,rocks-full,heap
python3 summarize.py ../../results/local/my-state-run > summary.json
```

Use a **new label** for a new run: the runner refuses to overwrite an existing
run directory. Fixtures can be reused after a SHA-256 check. The runner stops
only its own Java subprocess on timeout/interruption; timeout defaults to 900 s.
Do not run cases concurrently, as that invalidates the resource comparison.

## Measurement meaning and boundaries

- Throughput = all inputs / (source start → last state operation completion).
  This is **whole-batch growing-state throughput**, including JIT warmup and
  checkpoint work that overlaps the batch. It is not steady-state maximum QPS,
  a recovery test, a Kafka sink benchmark, or a production SLA.
- Latency = event construction at source → state operation / downstream sink
  callback. Percentiles are **upper bounds of log2 microsecond buckets**.
  Buffered file writes are closed before verification, but callback latency is
  not disk fsync or Kafka-committed visibility latency.
- Source is backpressure-aware and shares the JVM. It does not model an external
  Kafka backlog continuing to grow beyond the bounded network buffers.
- JVM CPU/heap/GC and `/proc` RSS/I/O are sampled about once per second; REST task
  busy/backpressure metrics are asynchronous. Short runs have too few active
  samples for a meaningful CPU or backpressure conclusion.
- Baseline and retained heap are measured after explicit `System.gc()`, before
  input and after draining plus at least three further checkpoint completions (some may have been triggered before drain). These explicit
  GCs are outside the throughput interval. Retained heap includes framework
  overhead. Multi-scale differences are an **approximate marginal cost**, not
  exact object-layout accounting. Do not multiply a small-run total RSS by ID
  count or call a Long ID an eight-byte state entry.
- RocksDB uses native memory and OS page cache. Its Java heap is not its total
  memory. Directory bytes are the sum of logical file lengths, not uniquely
  allocated disk blocks; hard links can count twice. `/proc write_bytes` covers
  the process including checkpoint and output files, not just RocksDB writes.
- REST checkpoint `checkpointed_size` is bytes attributed to this checkpoint;
  `state_size` is the represented full state. In incremental mode they differ.
  Post-load (no new records) checkpoints are labeled separately: near-zero
  incremental bytes there do not imply near-zero bytes under continuous writes.
- Source file offsets and output files are **not checkpointed/transactional**.
  The experiment intentionally disables restart. Do not reuse it as a production
  fault-tolerant source/sink or infer restored output correctness from a pass.

References: [Flink state backends](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/state_backends/),
[Flink TTL](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl).
