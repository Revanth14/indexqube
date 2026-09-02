#!/usr/bin/env bash
set -euo pipefail

if [[ "${INDEXQUBE_ALPHA_CONFIRM:-}" != "1" ]]; then
  printf 'Set INDEXQUBE_ALPHA_CONFIRM=1 to run real Codex and Claude read-only tasks.\n' >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_repo="${1:-$repo_root}"
target_repo="$(cd "$target_repo" && pwd)"
iq_bin="${IQ_BIN:-$repo_root/bin/iq}"
if [[ ! -d "$target_repo/.git" && ! -f "$target_repo/.git" ]]; then
  printf 'alpha target is not a Git workspace: %s\n' "$target_repo" >&2
  exit 2
fi
if [[ ! -x "$iq_bin" ]]; then
  make -C "$repo_root" build-iq
fi

alpha_root="$(mktemp -d "${TMPDIR:-/tmp}/indexqube-real-alpha.XXXXXX")"
state_dir="$alpha_root/state"
report="${INDEXQUBE_ALPHA_REPORT:-${TMPDIR:-/tmp}/indexqube-alpha-$(date -u +%Y%m%dT%H%M%SZ).json}"
daemon_pid=""
cleanup() {
  if [[ -n "$daemon_pid" ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  rm -rf -- "$alpha_root"
}
trap cleanup EXIT

mkdir -p "$state_dir"
ports="$(python3 - <<'PY'
import socket
values = []
for _ in range(2):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    values.append(str(sock.getsockname()[1]))
    sock.close()
print(" ".join(values))
PY
)"
read -r proxy_port control_port <<<"$ports"
proxy_addr="127.0.0.1:$proxy_port"
control_addr="127.0.0.1:$control_port"
control_url="http://$control_addr"

before_head="$(git -C "$target_repo" rev-parse HEAD)"
before_status="$(git -C "$target_repo" status --porcelain=v1 --untracked-files=all)"
INDEXQUBE_HOME="$state_dir" IQ_TELEMETRY=off "$iq_bin" daemon --addr "$proxy_addr" --control-addr "$control_addr" >"$alpha_root/daemon.log" 2>&1 &
daemon_pid=$!
for _ in {1..80}; do
  if INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" "$iq_bin" metrics --json >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
if ! INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" "$iq_bin" metrics --json >/dev/null 2>&1; then
  printf 'alpha daemon did not become ready\n' >&2
  sed -n '1,200p' "$alpha_root/daemon.log" >&2
  exit 1
fi

run_task() {
  local backend="$1"
  local log="$alpha_root/$backend.log"
  if ! INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
      "$iq_bin" task --workspace "$target_repo" --backend "$backend" --pin \
      'Inspect the repository architecture and name one concrete reliability risk. Read only: do not change files or run mutating commands.' >"$log" 2>&1; then
    printf '%s alpha task failed\n' "$backend" >&2
    sed -n '1,240p' "$log" >&2
    exit 1
  fi
  sed -n 's/.*\[iq\] task \(task_[a-f0-9]*\) via.*/\1/p' "$log" | head -n 1
}

codex_task="$(run_task codex)"
claude_task="$(run_task claude)"
if [[ -z "$codex_task" || -z "$claude_task" ]]; then
  printf 'alpha task IDs were not captured\n' >&2
  exit 1
fi
handoff_log="$alpha_root/handoff.log"
if ! INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" \
    "$iq_bin" handoff "$codex_task" --to claude \
    'Confirm the canonical handoff context is coherent and add one read-only observation.' >"$handoff_log" 2>&1; then
  printf 'real repository handoff failed\n' >&2
  sed -n '1,240p' "$handoff_log" >&2
  exit 1
fi

after_head="$(git -C "$target_repo" rev-parse HEAD)"
after_status="$(git -C "$target_repo" status --porcelain=v1 --untracked-files=all)"
if [[ "$before_head" != "$after_head" || "$before_status" != "$after_status" ]]; then
  printf 'read-only alpha changed the target repository; inspect it before continuing\n' >&2
  git -C "$target_repo" status --short >&2
  exit 1
fi

metrics="$(INDEXQUBE_HOME="$state_dir" INDEXQUBE_CONTROL_URL="$control_url" "$iq_bin" metrics --json)"
ALPHA_REPO="$target_repo" ALPHA_COMMIT="$after_head" ALPHA_CODEX_TASK="$codex_task" \
  ALPHA_CLAUDE_TASK="$claude_task" ALPHA_METRICS="$metrics" ALPHA_REPORT="$report" python3 - <<'PY'
import json, os
payload = {
    "schema_version": 1,
    "repository": os.environ["ALPHA_REPO"],
    "commit": os.environ["ALPHA_COMMIT"],
    "read_only_workspace_unchanged": True,
    "codex_task": os.environ["ALPHA_CODEX_TASK"],
    "claude_task": os.environ["ALPHA_CLAUDE_TASK"],
    "codex_to_claude_handoff": True,
    "metrics": json.loads(os.environ["ALPHA_METRICS"]),
}
with open(os.environ["ALPHA_REPORT"], "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY

printf 'real-repository alpha passed\n'
printf 'report: %s\n' "$report"
