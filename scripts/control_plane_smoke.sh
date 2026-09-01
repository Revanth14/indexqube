#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
iq_bin="${IQ_BIN:-$repo_root/bin/iq}"
smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/indexqube-control-smoke.XXXXXX")"
workspace="$smoke_root/workspace"
state_dir="$smoke_root/state"
daemon_pid=""

cleanup() {
  if [[ -n "$daemon_pid" ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  rm -rf -- "$smoke_root"
}
trap cleanup EXIT

if [[ ! -x "$iq_bin" ]]; then
  printf 'iq binary not found at %s; run `make build-iq` first\n' "$iq_bin" >&2
  exit 1
fi

mkdir -p "$workspace" "$state_dir"
git -C "$workspace" init -q
git -C "$workspace" config user.email smoke@indexqube.local
git -C "$workspace" config user.name 'IndexQube Smoke'
printf 'control-plane smoke\n' >"$workspace/README.md"
git -C "$workspace" add README.md
git -C "$workspace" commit -q -m initial

ports="$(python3 - <<'PY'
import socket
ports = []
for _ in range(2):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    ports.append(str(sock.getsockname()[1]))
    sock.close()
print(" ".join(ports))
PY
)"
read -r proxy_port control_port <<<"$ports"
control_url="http://127.0.0.1:$control_port"

start_daemon() {
  INDEXQUBE_HOME="$state_dir" "$iq_bin" daemon \
    --addr "127.0.0.1:$proxy_port" \
    --control-addr "127.0.0.1:$control_port" \
    >"$smoke_root/daemon.log" 2>&1 &
  daemon_pid=$!
  for _ in {1..100}; do
    if curl -fsS "$control_url/control/healthz" >/dev/null 2>&1; then
      return
    fi
    sleep 0.05
  done
  printf 'daemon failed to become ready\n' >&2
  sed -n '1,200p' "$smoke_root/daemon.log" >&2
  exit 1
}

stop_daemon() {
  kill -TERM "$daemon_pid"
  wait "$daemon_pid" || true
  daemon_pid=""
}

start_daemon

task_log="$smoke_root/task.log"
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task --backend fake --workspace "$workspace" --write \
  '[fake:mutate] create durable evidence' >"$task_log" 2>&1
task_id="$(sed -n 's/.*\[iq\] task \([^ ]*\) via.*/\1/p' "$task_log" | head -n 1)"
if [[ -z "$task_id" ]]; then
  printf 'failed to read task id\n' >&2
  sed -n '1,200p' "$task_log" >&2
  exit 1
fi

show_output="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task show "$task_id")"
grep -q 'Status: open' <<<"$show_output"
grep -q 'Files changed (workspace-authoritative):' <<<"$show_output"
if grep -q '^Attention:' <<<"$show_output"; then
  printf 'fake task produced an evidence mismatch\n%s\n' "$show_output" >&2
  exit 1
fi
stop_daemon
start_daemon
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task show "$task_id" | grep -q 'Evidence:'
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" continue "$task_id" 'confirm the task survived restart' >/dev/null

if [[ "${IQ_SMOKE_REAL_CODEX:-0}" == "1" ]]; then
  real_log="$smoke_root/real-codex.log"
  INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task --backend codex --workspace "$workspace" --write \
    'Create codex-smoke.txt containing exactly: IndexQube real Codex write smoke passed' >"$real_log" 2>&1
  real_task_id="$(sed -n 's/.*\[iq\] task \([^ ]*\) via.*/\1/p' "$real_log" | head -n 1)"
  if [[ -z "$real_task_id" ]]; then
    printf 'failed to read real Codex task id\n' >&2
    sed -n '1,200p' "$real_log" >&2
    exit 1
  fi
  real_show="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task show "$real_task_id")"
  grep -q 'Status: open' <<<"$real_show"
  grep -q 'added codex-smoke.txt' <<<"$real_show"
  if grep -q '^Attention:' <<<"$real_show"; then
    printf 'real Codex task produced an evidence mismatch\n%s\n' "$real_show" >&2
    exit 1
  fi
  if [[ "$(cat "$workspace/codex-smoke.txt")" != 'IndexQube real Codex write smoke passed' ]]; then
    printf 'real Codex smoke file content did not match\n' >&2
    exit 1
  fi
fi

printf 'control-plane smoke passed: %s\n' "$task_id"
