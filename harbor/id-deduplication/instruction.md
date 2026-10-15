Run the local delivery experiment and write `/app/deduplication-report.json`.

The contract is one unmodified first-arrival JSON per business ID in Kafka,
with neither missing IDs nor duplicate records. Repeated IDs must be filtered
before writing Kafka. Do not use the earlier all-arrivals marking contract.

Use the supplied real Kafka/Redis/Flink lab: four producer threads target
3000 arrivals/s, 9000 synthetic arrivals per case, 3000 business IDs appearing
three times each. Deduplication lasts one hour of processing time, refreshed
on arrivals. Use a 30-second checkpoint period and complete an initial empty
checkpoint before producing business data.

Compare Redis-after-ACK normally and through an actual checkpoint failure;
Redis-before-send with a native task exception after Redis reservation but
before Kafka send; Redis-after-ACK with a native exception after Kafka ACK but
before Redis confirmation; keyed state with nontransactional KafkaSink and
keyed state with transactional KafkaSink through actual checkpoint failure.
The Redis operations belong inside the sink function. Let Flink automatically
restore from its checkpoint in the same JVM; controller kill/relaunch is not
a failure injection. Final owned-process cleanup is required.

Report actual source/output topic names, producer measurements, checkpoint
and restored-attempt witnesses, sink-gap witnesses and full JSONL file hashes.
Audit missing IDs, duplicate business records and complete first-arrival
payloads separately in read_uncommitted and read_committed. A successful
experiment report must describe failures honestly, rather than requiring all
strategies to satisfy the delivery contract. This is a synthetic engineering
experiment, not a platform task submission or a production guarantee.
