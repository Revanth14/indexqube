#!/bin/bash
set -e

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
[ "$ARCH" = "x86_64" ] && ARCH="amd64"
[ "$ARCH" = "arm64" ]  && ARCH="arm64"
[ "$ARCH" = "aarch64" ] && ARCH="arm64"

BINARY="iq-${OS}-${ARCH}"
URL="https://github.com/Revanth14/indexqube/releases/latest/download/${BINARY}"

echo "Downloading IndexQube ${BINARY}..."
curl -fsSL "$URL" -o /tmp/iq
chmod +x /tmp/iq

echo "Installing to /usr/local/bin (may ask for password)..."
sudo mv /tmp/iq /usr/local/bin/iq

echo ""
echo "IndexQube installed successfully"
echo ""
echo "Usage:"
echo "  iq              # start Claude Code with optimizer"
echo ""
echo "To disable telemetry:"
echo "  export IQ_TELEMETRY=off"
