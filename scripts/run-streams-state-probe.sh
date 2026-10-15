#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
probe_project="streams-state-$(date -u +%Y%m%d%H%M%S)"
probe_output="$PWD/results/local/$probe_project"
mkdir -p "$probe_output"
compose=(docker compose -p "$probe_project" -f "$PWD/harbor/arrival-marking-comparison/environment/docker-compose.yaml" -f "$PWD/experiments/streams-state-probe/compose.yaml")
probe_container=''
cleanup() {
  local prior_status=$?
  trap - EXIT
  if [[ -n "$probe_container" ]]; then
    docker cp "$probe_container:/app/state-probe-runs" "$probe_output/" >"$probe_output/copy.log" 2>&1 || true
  fi
  "${compose[@]}" down --timeout 10 --remove-orphans >"$probe_output/cleanup.log" 2>&1 || prior_status=1
  echo "diagnostic_exit=$prior_status artifacts=$probe_output"
  if (( prior_status != 0 )); then tail -25 "$probe_output/driver.log" 2>/dev/null || true; fi
  exit "$prior_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${compose[@]}" up -d --wait --wait-timeout 180 >"$probe_output/startup.log" 2>&1
probe_container=$("${compose[@]}" ps -q main)
docker cp scripts/profile-streams-state.py "$probe_container:/app/profile-streams-state.py"
docker exec "$probe_container" /opt/java/openjdk/bin/java -XshowSettings:system -version >"$probe_output/jvm-system.txt" 2>&1
timeout --signal=TERM --kill-after=30s 1500s docker exec "$probe_container" python3 /app/profile-streams-state.py >"$probe_output/driver.log" 2>&1
