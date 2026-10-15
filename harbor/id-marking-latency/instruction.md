# Parallel JSON ID marking and visibility experiment

Run six real-process scenarios: local in-memory ID marking; exact Redis SET NX
marking; Kafka Streams EOS/100ms commit; Flink 30s CK/nontransactional sink;
Flink 30s CK/nontransactional -> staging Kafka -> Streams EOS/100ms; and
Flink 30s CK/transactional sink. All inputs and outputs are JSON values.

Use a rolling one-hour processing-time deduplication horizon, refreshed on every
arrival; an expired ID becomes a first arrival again. Use Flink keyed state TTL,
Redis atomic lookup/refresh with TTL, and Streams last-seen state.

Use four rate-controlled producer threads targeting 3000 arrivals/sec for
30000 arrivals per scenario. Keep input_seq unique and deliberately repeat each
business event_id three times. Preserve every arrival and its full nested JSON
payload; annotate first arrivals with is_duplicate=false and later ones true.
Define first occurrence by input Kafka partition offset, not concurrent send
sequence. Key messages by event_id. Consumers must sink actual messages to
JSONL files. Independently reconsume Kafka, compare all input/output payloads,
sequences and flags, and export gzip-compressed JSONL plus measured rates,
latencies and native checkpoint evidence. Do not infer achieved rate or latency
from configuration. /app/tagging-report.json is the report interface.

This is an engineering experiment, not a TalentAI task or production SLA.
The Harbor reward certifies a complete correct report, not a latency target or
failure-recovery guarantee. No paid models and no fabricated timing.
