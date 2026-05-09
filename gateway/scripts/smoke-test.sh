#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${INDEXQUBE_BASE_URL:-http://127.0.0.1:8080}"
TOKEN="${INDEXQUBE_DEV_TOKEN:-iq-dev-local}"

curl -fsS "$BASE_URL/healthz" >/dev/null
curl -fsS "$BASE_URL/readyz" >/dev/null

curl -fsS \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -N \
  "$BASE_URL/v1/messages" \
  -d '{
    "model": "claude-sonnet-4-6",
    "max_tokens": 64,
    "stream": true,
    "messages": [
      {"role": "user", "content": "Reply with exactly: IndexQube smoke OK"}
    ]
  }'

printf '\n'
