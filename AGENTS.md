# Kafka Delivery Lab

This is an engineering feasibility experiment, not a TalentAI submission or a
model-difficulty task. Use real Kafka/Redis/Flink processes. Generated event IDs
are explicitly synthetic laboratory inputs. Never claim a passing Harbor report
means every delivery strategy satisfies the user's delivery contract.

The original delivery experiment requires one physical Kafka data record per
accepted business event without loss after recovery. The JSON marking/load
experiment preserves every arrival (including repeated business IDs), adding
is_duplicate under a rolling one-hour processing-time horizon refreshed on each
arrival. Do not confuse these two contracts. Count read_uncommitted and
read_committed separately. Aborted data still counts in the physical view.
Keep fault schedules deterministic and distinguish infrastructure errors from
observed duplicate/lost records. Do not use paid models. Publish only to a remote
explicitly authorized by the user; the user authorized tju2hard/kafka-delivery-lab.

The current `id-deduplication` experiment filters repeated IDs before sink: one
unmodified first source-partition arrival per business ID, without loss. Redis
check/send/confirmation belong inside the sink function. Distinguish a
reservation before send from a confirmed-sent ledger after Kafka ACK. The latter
must skip IDs already confirmed, not merely mark and forward them. Verify both
orders' failure windows; a shared function is not a shared atomic transaction.
Keep this business-ID contract separate from the earlier all-arrivals marking
contract. Kafka transactions protect the committed view; raw aborted records
can still violate the original physical-log-only-once requirement.

`streams-id-filter` implements actual duplicate filtering with Kafka Streams
EOSv2, persistent one-hour state and 100ms commit configuration. Inputs must
already be keyed/partitioned by event_id. Measure actual committed visibility;
never call 100ms configuration a 100ms latency SLA. Its controlled fault is a
process crash after data ACK but before Kafka transaction commit, not a Flink
checkpoint failure. Its CK30s Flink case is a normal latency reference.

Keep runtime/image/dependency versions pinned. Test reports must identify exact
versions, fault witnesses, raw counts, committed counts and visibility times.
Stop only this lab's owned processes and services. No Docker socket in the image.
