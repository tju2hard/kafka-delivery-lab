# 业务 ID 过滤例子

`deduplicated-input.jsonl` 和 `deduplicated-output.jsonl` 来自真实完成的
`deduplication-oracle-02 / redis-after-normal`，是合成实验数据的实际 Kafka 复读，
不是手写输出。抽取最先输出的三个业务 ID，输入各出现三次，共九条；输出各一条，
共三条。输出保留首次源分区到达的完整 JSON，后续同 ID 直接跳过 sink。

```bash
python3 scripts/compare-business-jsonl.py \
  --input results/examples/deduplicated-input.jsonl \
  --output results/examples/deduplicated-output.jsonl
```

检查应通过。这个小样例只展示正常过滤形态，故障后的不重不漏必须看完整
[故障实验报告](../DEDUPLICATION.md)。旧的 `marked-*.jsonl` 属于保留全部
到达并打标记的另一种契约。
