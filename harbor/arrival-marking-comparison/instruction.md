Run `python3 /app/arrival_comparison.py --output /app/arrival-report.json`.

Engineering comparison: preserve EVERY source arrival and add is_duplicate and
marked_at_ms. Never filter business-ID repeats. Each scenario has 9000 synthetic
JSON arrivals, 3000 IDs appearing three times, four generator threads targeting
3000 arrivals/s. Source partition offset defines first arrival. Window: rolling
one-hour processing time, refreshed on repeats. One partition/processing thread.

Compare seven chains, each normally and after controlled recovery (14 cases):
Flink keyed state + transactional/nontransactional sink; direct Streams EOSv2;
pure Redis business markers with seen updated before send or after Kafka ACK;
Flink state business markers + Redis arrival send ledger, reserved before send
or confirmed after Kafka ACK. The old Flink forwarding + Streams bridge is not
part of this matrix. Business markers use event_id; send ledgers use input_seq,
never event_id. An existing send ledger skips only that exact input arrival.
Redis checks, updates, marker calculation (in pure Redis cases), send and ACK
all belong inside sink.invoke. Pure Redis cases always forward every arrival.

Flink CK=30s, input starts after a real empty CK completes. State transaction and
nontransaction controls inject one real snapshot IOException. Redis cases hold
the FIRST business arrival precisely after Redis update/before Kafka send or
after Kafka ACK/before Redis update. An independent client verifies Redis and
both output views while held, then releases a real sink IOException. Flink must
restore automatically in the same JVM from the initial checkpoint. Record the
fault input_seq, is_duplicate=false, Redis key/value, ACK/offset and observation.
Direct Streams commit=100ms, fault is output ACK before commit; verify raw data
and no committed data before SIGKILL/restart of its owned JVM only.

Read raw and committed outputs separately. Independently audit every input_seq,
complete payload, unique business IDs, false/true flags, source replay and flag
errors. Expected successful committed output: 9000 rows, 3000 false, 6000 true.
Specifically expose pure Redis before-send marker loss (9000/2999/6001), pure
Redis after-ACK extra first (9001/3001/6000), state/Redis before-send arrival
loss (8999/2999/6000), and state/Redis after-ACK replay (9001/3001/6000), where
numbers mean total/false/true. These are controlled single-first-arrival gaps,
not general probabilities or arbitrary checkpoint-failure outcomes.
Report deficiencies in nontransactional recovery cases honestly; a valid report
does not mean every strategy passes the delivery contract. Measure send-to-first
native observation latency per actual output, export real JSONL and hashes.
Report normal latency separately from recovery-inclusive latency. Do not call
100ms commit a latency SLA. Use real Kafka/Flink/Redis/Streams, stop only owned
processes. This is not a platform submission or a production capacity benchmark.
