#!/usr/bin/env bash
# deploy-gateway.sh — run this ON the DigitalOcean Droplet to pull the latest
# gateway binary from GitHub Releases and restart the systemd service.
#
# Usage:
#   ./deploy-gateway.sh                    # pulls latest release
#   ./deploy-gateway.sh v0.3.1             # pulls a specific tag
#
# Requires: curl, jq (sudo apt install -y jq)

set -euo pipefail

REPO="Revanth14/indexqube"
BINARY_NAME="indexqube-gateway-linux-amd64"
INSTALL_PATH="/opt/indexqube/indexqube-gateway"
SERVICE_NAME="indexqube-gateway"

TAG="${1:-}"

# ── Resolve tag ──────────────────────────────────────────────────────────────
if [[ -z "$TAG" ]]; then
  echo "▶ Fetching latest release tag from GitHub..."
  TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | jq -r '.tag_name')
fi

echo "▶ Target release: ${TAG}"

# ── Download ─────────────────────────────────────────────────────────────────
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${BINARY_NAME}"
TMP_BIN="/tmp/${BINARY_NAME}"

echo "▶ Downloading ${DOWNLOAD_URL} ..."
curl -fsSL -o "${TMP_BIN}" "${DOWNLOAD_URL}"
chmod +x "${TMP_BIN}"

# ── Verify checksum (optional but recommended) ────────────────────────────────
CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${TAG}/checksums.txt"
TMP_CHECKSUMS="/tmp/checksums.txt"
if curl -fsSL -o "${TMP_CHECKSUMS}" "${CHECKSUMS_URL}" 2>/dev/null; then
  echo "▶ Verifying checksum..."
  cd /tmp && grep "${BINARY_NAME}" "${TMP_CHECKSUMS}" | sha256sum --check --status
  echo "✅ Checksum OK"
  cd -
else
  echo "⚠️  No checksums.txt found for this release — skipping verification"
fi

# ── Deploy ────────────────────────────────────────────────────────────────────
echo "▶ Installing to ${INSTALL_PATH} ..."
mv "${TMP_BIN}" "${INSTALL_PATH}"

echo "▶ Restarting ${SERVICE_NAME} ..."
systemctl restart "${SERVICE_NAME}"
sleep 2
systemctl status "${SERVICE_NAME}" --no-pager -l

echo ""
echo "✅ Gateway ${TAG} deployed and running."
echo ""
echo "▶ Verifying /v1/telemetry endpoint..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:8080/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{"machine_id":"deploy-test","upstream_status":200}')

if [[ "$HTTP_CODE" == "204" ]]; then
  echo "✅ /v1/telemetry is live (HTTP 204)"
else
  echo "❌ /v1/telemetry returned HTTP ${HTTP_CODE} — check logs:"
  echo "   journalctl -u ${SERVICE_NAME} -n 50"
fi
