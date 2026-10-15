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

Keep runtime/image/dependency versions pinned. Test reports must identify exact
versions, fault witnesses, raw counts, committed counts and visibility times.
Stop only this lab's owned processes and services. No Docker socket in the image.
