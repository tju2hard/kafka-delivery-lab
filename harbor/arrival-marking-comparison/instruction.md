Run `python3 /app/arrival_comparison.py --output /app/arrival-report.json`.

Engineering comparison: preserve EVERY source arrival and add is_duplicate and
marked_at_ms. Never filter business-ID repeats. Each scenario has 9000 synthetic
JSON arrivals, 3000 IDs appearing three times, four generator threads targeting
3000 arrivals/s. Source partition offset defines first arrival. Window: rolling
one-hour processing time, refreshed on repeats. One partition/processing thread.

Compare five chains under normal operation and controlled recovery: Flink
keyed state + nontransactional sink; Redis exact seen ledger INSIDE sink +
nontransactional send; direct Kafka Streams EOSv2; nontransactional Flink
forwarding + Streams EOSv2; and Flink keyed state + transactional KafkaSink.
Flink CK=30s, input starts after a real empty CK completes. Flink fault is a
one-time real snapshot IOException and native automatic task restore without
killing its JVM. Direct Streams commit=100ms, fault is output ACK before commit,
verify raw data and no committed data before SIGKILL/restart of owned JVM only.

Read raw and committed outputs separately. Independently audit every input_seq,
complete payload, unique business IDs, false/true flags, source replay and flag
errors. Expected successful committed output: 9000 rows, 3000 false, 6000 true.
Report deficiencies in nontransactional recovery cases honestly; a valid report
does not mean every strategy passes the delivery contract. Measure send-to-first
native observation latency per actual output, export real JSONL and hashes.
Report normal latency separately from recovery-inclusive latency. Do not call
100ms commit a latency SLA. Use real Kafka/Flink/Redis/Streams, stop only owned
processes. This is not a platform submission or a production capacity benchmark.
