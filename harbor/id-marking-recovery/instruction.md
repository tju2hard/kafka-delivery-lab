# Actual checkpoint failure and recovery experiment

Generate 30000 JSON arrivals at a target 3000/s, using four producer threads.
Use a rolling one-hour ID horizon refreshed on each arrival. Keep duplicate
arrivals and annotate them. Compare failed Flink checkpoints with a transactional
KafkaSink, nontransactional state marking, Redis external marking, and a
nontransactional Flink bridge followed by Kafka Streams EOS/100ms.

Start with an actual completed empty periodic checkpoint. Configure 30-second
periodic checkpoints. Exercise two failures separately: (1) 8s snapshot stall
exceeding a 5s checkpoint timeout, tolerating one failure, and (2) one IOException
inside the real snapshotState callback, with automatic Flink job restart from
the last successful checkpoint. The controller must not kill/relaunch the JVM
to manufacture the failure. Record native abort/restart/restore witnesses.

Export actual input and committed output JSONL; independently reconsume Kafka,
compare complete payloads, missing and replayed input_seq, and false/true markers.
Keep raw aborted records separate from read_committed results. Write the report
to /app/recovery-report.json. Harbor reward means the report is complete and
truthful, not that all strategies pass the delivery contract. Synthetic lab
data, no model calls, not a TalentAI submission.
