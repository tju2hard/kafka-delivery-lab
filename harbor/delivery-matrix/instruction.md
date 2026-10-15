# Kafka delivery experiment

Run `python3 /app/run_lab.py --output /app/report.json` and retain its native
observations. The report compares delivery strategies using real Kafka, Redis
and Flink processes and explicitly synthetic event batches.

The required business target is exactly one physical Kafka data record for
each accepted event without losing events on recovery. Record raw Kafka and
read_committed views separately. Expected counterexamples must be retained.
A complete, truthful experimental report is the deliverable; the report must
not claim every tested strategy satisfies the target.

Kafka has three replicated brokers. No external network or credentials are
needed. Each fault uses a witnessed barrier before process termination or
packet release. Report latency as observation latency under this lab topology,
not a production throughput estimate.
