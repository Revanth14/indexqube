#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/indexqube-installer-test.XXXXXX")"
trap 'rm -rf -- "$test_root"' EXIT

case "$(uname -s)" in Darwin) target_os=darwin ;; Linux) target_os=linux ;; *) exit 0 ;; esac
case "$(uname -m)" in x86_64|amd64) target_arch=amd64 ;; arm64|aarch64) target_arch=arm64 ;; *) exit 0 ;; esac

make_release() {
  local version="$1"
  local release="$test_root/$version"
  local stage="$test_root/stage-$version"
  local asset="indexqube_${version}_${target_os}_${target_arch}.tar.gz"
  mkdir -p "$release" "$stage"
  printf '#!/bin/sh\nif [ "${1:-}" = "--version" ]; then printf "iq %s\\n"; exit 0; fi\n' "$version" >"$stage/iq"
  chmod 0755 "$stage/iq"
  tar -C "$stage" -czf "$release/$asset" iq
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$release" && sha256sum "$asset" > checksums.txt)
  else
    (cd "$release" && shasum -a 256 "$asset" > checksums.txt)
  fi
}

make_release v1.0.0
make_release v1.1.0
install_dir="$test_root/bin"
INDEXQUBE_RELEASE_DIR="$test_root/v1.0.0" "$repo_root/scripts/install.sh" --version v1.0.0 --install-dir "$install_dir" >/dev/null
test "$("$install_dir/iq" --version)" = 'iq v1.0.0'
INDEXQUBE_RELEASE_DIR="$test_root/v1.1.0" "$repo_root/scripts/install.sh" --version v1.1.0 --install-dir "$install_dir" >/dev/null
test "$("$install_dir/iq" --version)" = 'iq v1.1.0'
test "$("$install_dir/iq.previous" --version)" = 'iq v1.0.0'
printf 'tamper' >>"$test_root/v1.0.0/indexqube_v1.0.0_${target_os}_${target_arch}.tar.gz"
if INDEXQUBE_RELEASE_DIR="$test_root/v1.0.0" "$repo_root/scripts/install.sh" --version v1.0.0 --install-dir "$install_dir" >/dev/null 2>&1; then
  printf 'tampered release unexpectedly installed\n' >&2
  exit 1
fi
test "$("$install_dir/iq" --version)" = 'iq v1.1.0'
"$repo_root/scripts/install.sh" --rollback --install-dir "$install_dir" >/dev/null
test "$("$install_dir/iq" --version)" = 'iq v1.0.0'
test "$("$install_dir/iq.previous" --version)" = 'iq v1.1.0'
printf 'installer update and rollback test passed\n'
