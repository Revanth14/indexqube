#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
GATEWAY_URL="${GATEWAY_URL%/}"
SESSION_KEY="${ALPHA_SESSION_KEY:-indexqube-alpha-$(date +%s)-$$}"

json_get() {
  local json="$1"
  local path="$2"
  JSON_PAYLOAD="$json" JSON_PATH="$path" node - <<'NODE'
const payload = JSON.parse(process.env.JSON_PAYLOAD || "{}");
const parts = String(process.env.JSON_PATH || "").split(".").filter(Boolean);
let value = payload;
for (const part of parts) {
  value = value == null ? undefined : value[part];
}
if (value == null) {
  process.stdout.write("");
} else if (typeof value === "object") {
  process.stdout.write(JSON.stringify(value));
} else {
  process.stdout.write(String(value));
}
NODE
}

optimize() {
  local body="$1"
  curl -fsS \
    -X POST "$GATEWAY_URL/v1/optimize" \
    -H "Accept: application/json" \
    -H "Content-Type: text/plain; charset=utf-8" \
    -H "X-IQ-Session-Key: $SESSION_KEY" \
    --data-binary "$body"
}

assert_mode() {
  local response="$1"
  local expected="$2"
  local actual
  actual="$(json_get "$response" "mode")"
  if [[ "$actual" != "$expected" ]]; then
    printf 'expected mode %s, got %s\nresponse: %s\n' "$expected" "$actual" "$response" >&2
    exit 1
  fi
}

assert_positive() {
  local response="$1"
  local field="$2"
  local value
  value="$(json_get "$response" "$field")"
  if [[ -z "$value" || "$value" -le 0 ]]; then
    printf 'expected %s to be positive, got %s\nresponse: %s\n' "$field" "${value:-<empty>}" "$response" >&2
    exit 1
  fi
}

print_receipt() {
  local label="$1"
  local response="$2"
  local mode tokens_saved bytes_saved blocks_seen blocks_pruned blocks_skipped
  mode="$(json_get "$response" "mode")"
  tokens_saved="$(json_get "$response" "estimated_tokens_saved")"
  bytes_saved="$(json_get "$response" "bytes_saved")"
  blocks_seen="$(json_get "$response" "stats.blocks_seen")"
  blocks_pruned="$(json_get "$response" "stats.blocks_pruned")"
  blocks_skipped="$(json_get "$response" "stats.blocks_skipped")"
  printf '%-10s mode=%-9s tokens_saved=%-5s bytes_saved=%-5s blocks=%s/%s/%s\n' \
    "$label" "$mode" "${tokens_saved:-0}" "${bytes_saved:-0}" \
    "${blocks_seen:-0}" "${blocks_pruned:-0}" "${blocks_skipped:-0}"
}

alpha_prompt() {
  local changed_return="$1"
  cat <<PROMPT
Please review this alpha smoke-test file.

\`\`\`go alpha/smoke.go
package alpha

func Calculate(input int) int {
	stable := input
	stable += 1
	stable += 2
	stable += 3
	stable += 4
	stable += 5
	stable += 6
	stable += 7
	stable += 8
	stable += 9
	stable += 10
	stable += 11
	stable += 12
	stable += 13
	stable += 14
	stable += 15
	stable += 16
	stable += 17
	stable += 18
	stable += 19
	stable += 20
	if stable < 0 {
		return 0
	}
	return $changed_return
}
\`\`\`
PROMPT
}

printf 'IndexQube alpha check\n'
printf 'Gateway: %s\n' "$GATEWAY_URL"
printf 'Session: %s\n\n' "$SESSION_KEY"

if ! curl -fsS "$GATEWAY_URL/healthz" >/dev/null; then
  printf 'gateway is not reachable. Start it with: make dev\n' >&2
  exit 1
fi

warmup_body="$(alpha_prompt "stable + 1")"
diff_body="$(alpha_prompt "stable + 2")"

warmup_response="$(optimize "$warmup_body")"
assert_mode "$warmup_response" "warmup"
print_receipt "warmup" "$warmup_response"

unchanged_response="$(optimize "$warmup_body")"
assert_mode "$unchanged_response" "unchanged"
assert_positive "$unchanged_response" "estimated_tokens_saved"
print_receipt "unchanged" "$unchanged_response"

diff_response="$(optimize "$diff_body")"
assert_mode "$diff_response" "diff"
assert_positive "$diff_response" "estimated_tokens_saved"
print_receipt "diff" "$diff_response"

printf '\nalpha check passed\n'
