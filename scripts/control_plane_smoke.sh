#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
iq_bin="${IQ_BIN:-$repo_root/bin/iq}"
smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/indexqube-control-smoke.XXXXXX")"
workspace="$smoke_root/workspace"
state_dir="$smoke_root/state"
daemon_pid=""
approval_task_pid=""
cancel_task_pid=""

cleanup() {
	if [[ -n "$cancel_task_pid" ]] && kill -0 "$cancel_task_pid" 2>/dev/null; then
		kill -TERM "$cancel_task_pid" 2>/dev/null || true
		wait "$cancel_task_pid" 2>/dev/null || true
	fi
	if [[ -n "$approval_task_pid" ]] && kill -0 "$approval_task_pid" 2>/dev/null; then
		kill -TERM "$approval_task_pid" 2>/dev/null || true
		wait "$approval_task_pid" 2>/dev/null || true
	fi
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

read_control_token() {
  python3 -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["token"])' \
    "$state_dir/control-auth.json"
}

authenticated_control_status() {
  local token="$1"
  printf 'header = "Authorization: Bearer %s"\n' "$token" | \
    curl --config - --silent --output /dev/null --write-out '%{http_code}' \
      "$control_url/control/healthz"
}

start_daemon() {
  INDEXQUBE_HOME="$state_dir" "$iq_bin" daemon \
    --addr "127.0.0.1:$proxy_port" \
    --control-addr "127.0.0.1:$control_port" \
    >"$smoke_root/daemon.log" 2>&1 &
  daemon_pid=$!
  for _ in {1..100}; do
    if [[ -f "$state_dir/control-auth.json" ]]; then
      token="$(read_control_token)"
      if [[ "$(authenticated_control_status "$token")" == "200" ]]; then
        return
      fi
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
initial_control_token="$(read_control_token)"
python3 -c 'import os, stat, sys; assert stat.S_IMODE(os.stat(sys.argv[1]).st_mode) == 0o700; assert stat.S_IMODE(os.stat(sys.argv[2]).st_mode) == 0o600' \
  "$state_dir" "$state_dir/control-auth.json"
if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' "$control_url/control/healthz")" != "401" ]]; then
  printf 'control API accepted an unauthenticated health request\n' >&2
  exit 1
fi
if [[ "$(authenticated_control_status 'wrong-token')" != "401" ]]; then
  printf 'control API accepted an invalid credential\n' >&2
  exit 1
fi

dashboard_url="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" dashboard --workspace "$workspace" --no-open)"
dashboard_cookie_jar="$smoke_root/dashboard-cookies"
if [[ "$(curl --silent --output /dev/null --cookie-jar "$dashboard_cookie_jar" --write-out '%{http_code}' "$dashboard_url")" != "303" ]]; then
  printf 'dashboard ticket exchange failed\n' >&2
  exit 1
fi
if [[ "$(curl --silent --output /dev/null --cookie "$dashboard_cookie_jar" --write-out '%{http_code}' "$control_url/control/ui/")" != "200" ]]; then
  printf 'dashboard session could not read the UI\n' >&2
  exit 1
fi
dashboard_context="$(curl --silent --cookie "$dashboard_cookie_jar" "$control_url/control/v1/dashboard-context")"
python3 -c 'import json,os,sys; assert os.path.realpath(json.loads(sys.argv[1])["workspace"]) == os.path.realpath(sys.argv[2])' "$dashboard_context" "$workspace"
if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' "$dashboard_url")" != "401" ]]; then
  printf 'dashboard ticket was reusable\n' >&2
  exit 1
fi

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
rotated_control_token="$(read_control_token)"
if [[ "$rotated_control_token" == "$initial_control_token" ]]; then
  printf 'control API credential did not rotate across daemon restart\n' >&2
  exit 1
fi
if [[ "$(authenticated_control_status "$initial_control_token")" != "401" ]]; then
  printf 'control API accepted the prior daemon credential after restart\n' >&2
  exit 1
fi
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task show "$task_id" | grep -q 'Evidence:'
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" continue "$task_id" 'confirm the task survived restart' >/dev/null

INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task close "$task_id" | grep -q 'closed'
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task close "$task_id" | grep -q 'unchanged'
if INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" continue "$task_id" 'closed tasks reject continuation' >/dev/null 2>&1; then
  printf 'closed task unexpectedly accepted a continuation\n' >&2
  exit 1
fi
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task reopen "$task_id" | grep -q 'open'
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task reopen "$task_id" | grep -q 'unchanged'

cancel_log="$smoke_root/cancel-task.log"
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task --backend fake --workspace "$workspace" \
  '[fake:sleep] wait for durable cancellation' >"$cancel_log" 2>&1 &
cancel_task_pid=$!
cancel_task_id=""
for _ in {1..100}; do
  cancel_task_id="$(sed -n 's/.*\[iq\] task \([^ ]*\) via.*/\1/p' "$cancel_log" | head -n 1)"
  if [[ -n "$cancel_task_id" ]]; then
    break
  fi
  sleep 0.05
done
if [[ -z "$cancel_task_id" ]]; then
  printf 'failed to read cancellation task id\n' >&2
  sed -n '1,200p' "$cancel_log" >&2
  exit 1
fi
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" cancel "$cancel_task_id" | grep -q 'requested'
wait "$cancel_task_pid" 2>/dev/null || true
cancel_task_pid=""
INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" cancel "$cancel_task_id" | grep -q 'completed'
cancel_show="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
  "$iq_bin" task show "$cancel_task_id")"
grep -q 'Cancellations:' <<<"$cancel_show"
grep -q 'cancelled' <<<"$cancel_show"

run_real_read_smoke() {
  local backend="$1"
  local log="$smoke_root/real-${backend}-read.log"
  if ! INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task --backend "$backend" --workspace "$workspace" \
    'Read README.md and report its first line without changing any files.' >"$log" 2>&1; then
    printf 'real %s read-only task failed\n' "$backend" >&2
    sed -n '1,240p' "$log" >&2
    sed -n '1,240p' "$smoke_root/daemon.log" >&2
    exit 1
  fi
  real_read_task_id="$(sed -n 's/.*\[iq\] task \([^ ]*\) via.*/\1/p' "$log" | head -n 1)"
  if [[ -z "$real_read_task_id" ]]; then
    printf 'failed to read real %s task id\n' "$backend" >&2
    sed -n '1,200p' "$log" >&2
    exit 1
  fi
  real_read_show="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task show "$real_read_task_id")"
  grep -q 'Status: open' <<<"$real_read_show"
  grep -q "Backend: $backend" <<<"$real_read_show"
  if grep -q '^Attention:' <<<"$real_read_show"; then
    printf 'real %s read-only task produced an evidence mismatch\n%s\n' "$backend" "$real_read_show" >&2
    exit 1
  fi
}

if [[ "${IQ_SMOKE_REAL_CODEX_READ:-0}" == "1" ]]; then
  run_real_read_smoke codex
fi

if [[ "${IQ_SMOKE_REAL_CLAUDE:-0}" == "1" ]]; then
  run_real_read_smoke claude
fi

if [[ "${IQ_SMOKE_REAL_HANDOFF:-0}" == "1" ]]; then
  run_real_read_smoke codex
  handoff_source_task_id="$real_read_task_id"
  handoff_log="$smoke_root/real-handoff.log"
  if ! INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" handoff "$handoff_source_task_id" --to claude \
    'Read README.md and confirm the canonical handoff context arrived. Do not change files.' >"$handoff_log" 2>&1; then
    printf 'real Codex-to-Claude handoff failed\n' >&2
    sed -n '1,240p' "$handoff_log" >&2
    sed -n '1,240p' "$smoke_root/daemon.log" >&2
    exit 1
  fi
  handoff_show="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task show "$handoff_source_task_id")"
  grep -q 'Status: open' <<<"$handoff_show"
  grep -q 'Handoffs:' <<<"$handoff_show"
  grep -q 'codex -> claude' <<<"$handoff_show"
  grep -q 'explicit_handoff' <<<"$handoff_show"
fi

if [[ "${IQ_SMOKE_REAL_CODEX:-0}" == "1" ]]; then
  real_log="$smoke_root/real-codex.log"
  if ! INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task --backend codex --workspace "$workspace" --write \
    'Create codex-smoke.txt containing exactly: IndexQube real Codex write smoke passed' >"$real_log" 2>&1; then
    printf 'real Codex task failed\n' >&2
    sed -n '1,240p' "$real_log" >&2
    sed -n '1,240p' "$smoke_root/daemon.log" >&2
    exit 1
  fi
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

if [[ "${IQ_SMOKE_REAL_APPROVAL:-0}" == "1" ]]; then
  approval_log="$smoke_root/real-approval.log"
  approved_file="$smoke_root/approved-outside-workspace.txt"
  INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task --backend codex --workspace "$workspace" --write \
    "Create $approved_file outside the Git workspace. Its entire content must be the words 'IndexQube durable approval smoke passed' with no punctuation; a trailing newline is allowed. Request approval when the sandbox requires it." \
    >"$approval_log" 2>&1 &
  approval_task_pid=$!
  approval_task_id=""
  approval_count=0
  for _ in {1..240}; do
    if [[ -z "$approval_task_id" ]]; then
      approval_task_id="$(sed -n 's/.*\[iq\] task \([^ ]*\) via.*/\1/p' "$approval_log" | head -n 1)"
    fi
    if [[ -n "$approval_task_id" ]]; then
      pending_approvals="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
        "$iq_bin" approvals --task "$approval_task_id" | sed -n '2,$s/\t.*//p')"
      while IFS= read -r approval_id; do
        if [[ -z "$approval_id" ]]; then
          continue
        fi
        INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
          "$iq_bin" approve "$approval_id" >/dev/null
        approval_count=$((approval_count + 1))
      done <<<"$pending_approvals"
    fi
    if ! kill -0 "$approval_task_pid" 2>/dev/null; then
      break
    fi
    sleep 0.25
  done
  if kill -0 "$approval_task_pid" 2>/dev/null; then
    printf 'real Codex approval task did not finish\n' >&2
    sed -n '1,240p' "$approval_log" >&2
    exit 1
  fi
  if ! wait "$approval_task_pid"; then
    approval_task_pid=""
    printf 'real Codex approval task failed\n' >&2
    sed -n '1,240p' "$approval_log" >&2
    exit 1
  fi
  approval_task_pid=""
  if [[ "$approval_count" -lt 1 ]]; then
    printf 'real Codex task completed without exercising an approval\n' >&2
    sed -n '1,240p' "$approval_log" >&2
    exit 1
  fi
  if [[ "$(cat "$approved_file")" != 'IndexQube durable approval smoke passed' ]]; then
    printf 'approved outside-workspace file content did not match\n' >&2
	printf 'actual content: <%s>\n' "$(cat "$approved_file" 2>/dev/null || true)" >&2
	sed -n '1,240p' "$approval_log" >&2
    exit 1
  fi
  approval_show="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" task show "$approval_task_id")"
  grep -q 'Status: open' <<<"$approval_show"
  grep -q 'Approvals:' <<<"$approval_show"
  grep -q 'approved' <<<"$approval_show"
fi

backup_path="$state_dir/smoke-backup.db"
INDEXQUBE_HOME="$state_dir" "$iq_bin" backup --output "$backup_path" >/dev/null
test -s "$backup_path"
doctor_output="$(INDEXQUBE_HOME="$state_dir" "$iq_bin" doctor)"
grep -q 'task database: ok' <<<"$doctor_output"
grep -q 'control API: ok' <<<"$doctor_output"
grep -q 'telemetry: disabled (default)' <<<"$doctor_output"
metrics_json="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" "$iq_bin" metrics --json)"
python3 -c 'import json,sys; m=json.loads(sys.argv[1]); assert m["tasks_total"] >= 1; assert m["turns_total"] >= 1; assert "successful_latency" in m; assert "verification_outcomes" in m' "$metrics_json"

printf 'control-plane smoke passed: %s\n' "$task_id"
