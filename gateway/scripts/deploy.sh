#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${INDEXQUBE_APP_DIR:-/opt/indexqube}"
BIN_NAME="indexqube-gateway"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
GATEWAY_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"

if [ "$(id -u)" -ne 0 ]; then
  printf 'Run on the droplet with sudo: sudo bash gateway/scripts/deploy.sh\n' >&2
  exit 1
fi

if ! id -u indexqube >/dev/null 2>&1; then
  useradd --system --home-dir "$APP_DIR" --shell /usr/sbin/nologin indexqube
fi

install -d -o root -g root /etc/indexqube
install -d -o indexqube -g indexqube "$APP_DIR"

(cd "$GATEWAY_DIR" && go build -trimpath -o "$APP_DIR/$BIN_NAME" ./cmd/gateway)
chown indexqube:indexqube "$APP_DIR/$BIN_NAME"
chmod 0755 "$APP_DIR/$BIN_NAME"

install -m 0644 "$GATEWAY_DIR/deployments/systemd/indexqube-gateway.service" /etc/systemd/system/indexqube-gateway.service

if [ ! -f /etc/indexqube/gateway.env ]; then
  install -m 0600 -o root -g root \
    "$GATEWAY_DIR/deployments/gateway.env.example" /etc/indexqube/gateway.env
  printf 'Created /etc/indexqube/gateway.env from gateway.env.example. Edit ANTHROPIC_API_KEY before starting.\n'
fi

systemctl daemon-reload
systemctl enable indexqube-gateway
systemctl restart indexqube-gateway
systemctl --no-pager --full status indexqube-gateway
