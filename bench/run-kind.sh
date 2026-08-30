#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results_dir="$repo_root/bench/results"
address="localhost:15051"
mkdir -p "$results_dir"

kubectl -n apollo port-forward service/control-plane 15051:50051 >"${TMPDIR:-/tmp}/apollo-port-forward.log" 2>&1 &
port_forward_pid=$!
trap 'kill "$port_forward_pid" 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do
  if (cd "$repo_root/control-plane" && go run ./cmd/healthcheck -address "$address"); then
    break
  fi
  sleep 0.25
done

run_load() {
  local output=$1
  shift
  (cd "$repo_root/control-plane" && go run ./cmd/loadgen -address "$address" -output "$output" "$@")
}

run_throughput_group() {
  local replicas=$1
  kubectl -n apollo scale deployment/worker --replicas="$replicas"
  kubectl -n apollo rollout status deployment/worker --timeout=120s
  run_load "${TMPDIR:-/tmp}/apollo-warmup.json" -rate 800 -duration 2s -concurrency 256 -task-mix add=0,sleep=100,cpu=0 -sleep-ms 10
  for trial in $(seq 1 5); do
    run_load "$results_dir/throughput-${replicas}-worker-${trial}.json" -rate 800 -duration 4s -concurrency 256 -task-mix add=0,sleep=100,cpu=0 -sleep-ms 10
  done
}

run_failure_case() {
  local attempts=$1
  local output=$2
  kubectl -n apollo scale deployment/worker --replicas=4
  kubectl -n apollo rollout status deployment/worker --timeout=120s
  run_load "$output" -rate 30 -duration 6s -concurrency 128 -task-mix add=0,sleep=100,cpu=0 -sleep-ms 500 -max-attempts "$attempts" &
  local load_pid=$!
  sleep 2
  "$repo_root/scripts/kill-worker.sh"
  wait "$load_pid"
}

run_throughput_group 1
run_throughput_group 4
run_failure_case 1 "$results_dir/failure-retries-disabled.json"
run_failure_case 3 "$results_dir/failure-retries-enabled.json"
python3 "$repo_root/bench/summarize.py" "$results_dir" "$repo_root/docs/benchmarks/latest.md"
