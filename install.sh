#!/bin/bash
set -e

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
[ "$ARCH" = "x86_64" ]  && ARCH="amd64"
[ "$ARCH" = "aarch64" ] && ARCH="arm64"

# Must match the asset names produced by gateway-release.yml: iq-<os>-<arch>
BINARY="iq-${OS}-${ARCH}"
URL="https://github.com/Revanth14/indexqube/releases/latest/download/${BINARY}"
INSTALL_DIR="$HOME/.local/bin"
INSTALL_NAME="iq"

echo ""
echo "  Installing iq..."
echo "  Platform: ${OS}/${ARCH}"
echo ""

# Ensure install directory exists
mkdir -p "$INSTALL_DIR"

# Download
TMP="$(mktemp)"
curl -fsSL "$URL" -o "$TMP" || {
    echo "  Error: failed to download from $URL"
    echo "  Supported platforms: linux/amd64, darwin/amd64, darwin/arm64"
    rm -f "$TMP"
    exit 1
}
chmod +x "$TMP"
mv "$TMP" "$INSTALL_DIR/$INSTALL_NAME"

echo "  ✓ Installed to $INSTALL_DIR/$INSTALL_NAME"

# Add ~/.local/bin to PATH in ~/.zshrc if not already present
ZSHRC="$HOME/.zshrc"
if grep -qF '.local/bin' "$ZSHRC" 2>/dev/null; then
    echo "  ✓ ~/.local/bin already in PATH"
else
    {
        echo ""
        echo "# Added by iq installer"
        echo 'export PATH="$HOME/.local/bin:$PATH"'
    } >> "$ZSHRC"
    echo "  ✓ Added ~/.local/bin to PATH in ~/.zshrc"
    echo "    Run: source ~/.zshrc   (or open a new terminal)"
fi

echo ""
echo "  Usage:"
echo "    iq start        # Start the local IndexQube daemon"
echo "    iq setup        # Configure detected agents with backups"
echo "    iq claude       # One-shot Claude Code wrapper"
echo "    iq help         # Show all commands"
echo ""
echo "  Local-first by default. Works with Claude Pro and OpenAI-compatible agents."
echo ""
