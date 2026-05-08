#!/usr/bin/env bash
# Pre-packaging validation for the IndexQube VS Code extension.
# Run before packaging the VSIX or sharing with beta users.
# Exit code: 0 = all checks passed, 1 = one or more checks failed.

set -euo pipefail

PASS=0
FAIL=1
status=0

ok()   { printf '  [PASS] %s\n' "$1"; }
fail() { printf '  [FAIL] %s\n' "$1"; status=1; }
info() { printf '\n==> %s\n' "$1"; }

# ---------------------------------------------------------------------------
info "Gateway build"
# ---------------------------------------------------------------------------

if make build-gateway > /dev/null 2>&1; then
    ok "make build-gateway"
else
    fail "make build-gateway — build failed"
fi

# ---------------------------------------------------------------------------
info "Gateway tests"
# ---------------------------------------------------------------------------

if make test > /dev/null 2>&1; then
    ok "make test"
else
    fail "make test — one or more gateway test packages failed"
fi

# ---------------------------------------------------------------------------
info "Extension production bundle"
# ---------------------------------------------------------------------------

if (cd vscode-extension && npm run package > /dev/null 2>&1); then
    ok "npm run package"
else
    fail "npm run package — webpack production build failed"
fi

# ---------------------------------------------------------------------------
info "Extension package.json sanity"
# ---------------------------------------------------------------------------

PKG=vscode-extension/package.json
if [ -f "$PKG" ]; then
    ok "package.json exists"
else
    fail "package.json missing at $PKG"
fi

if grep -q '"publisher"' "$PKG"; then
    ok "publisher field present"
else
    fail "publisher field missing in package.json — required by vsce"
fi

if grep -q '"version"' "$PKG"; then
    ok "version field present"
else
    fail "version field missing in package.json"
fi

# ---------------------------------------------------------------------------
info "README present"
# ---------------------------------------------------------------------------

if [ -f "vscode-extension/README.md" ]; then
    ok "vscode-extension/README.md exists"
else
    fail "vscode-extension/README.md missing — required by vsce"
fi

# ---------------------------------------------------------------------------
info ".vscodeignore present"
# ---------------------------------------------------------------------------

if [ -f "vscode-extension/.vscodeignore" ]; then
    ok "vscode-extension/.vscodeignore exists"
else
    fail "vscode-extension/.vscodeignore missing — VSIX will include src/ and node_modules/"
fi

# ---------------------------------------------------------------------------
info "Gateway binary runnable"
# ---------------------------------------------------------------------------

GATEWAY_BIN="${GATEWAY_BIN:-bin/indexqube-gateway}"
if [ -f "$GATEWAY_BIN" ] && [ -x "$GATEWAY_BIN" ]; then
    ok "gateway binary exists and is executable"
else
    fail "gateway binary not found or not executable at $GATEWAY_BIN"
fi

# ---------------------------------------------------------------------------
info "dist/vsix directory"
# ---------------------------------------------------------------------------

VSIX_DIR="${VSIX_DIR:-vscode-extension/dist}"
if [ -d "$VSIX_DIR" ] && ls "$VSIX_DIR"/*.vsix > /dev/null 2>&1; then
    VSIX=$(ls "$VSIX_DIR"/*.vsix | head -1)
    ok "VSIX found: $VSIX"
else
    printf '  [SKIP] No VSIX in %s — run: make package-vsix\n' "$VSIX_DIR"
fi

# ---------------------------------------------------------------------------
printf '\n'
if [ "$status" -eq 0 ]; then
    printf '[ALL CHECKS PASSED] Ready to package.\n'
else
    printf '[CHECKS FAILED] Fix the issues above before packaging.\n'
fi
exit $status
