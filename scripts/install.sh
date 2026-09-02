#!/bin/sh
set -eu

repo="${INDEXQUBE_GITHUB_REPO:-Revanth14/indexqube}"
version="${INDEXQUBE_VERSION:-latest}"
install_dir="${INDEXQUBE_INSTALL_DIR:-}"
release_dir="${INDEXQUBE_RELEASE_DIR:-}"
require_attestation="${INDEXQUBE_REQUIRE_ATTESTATION:-0}"
rollback=0

usage() {
  printf 'Usage: install.sh [--version VERSION] [--install-dir DIR] [--require-attestation] [--rollback]\n'
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      version="$2"; shift 2 ;;
    --install-dir)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      install_dir="$2"; shift 2 ;;
    --require-attestation)
      require_attestation=1; shift ;;
    --rollback)
      rollback=1; shift ;;
    --help|-h)
      usage; exit 0 ;;
    *)
      printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ -z "$install_dir" ]; then
  install_dir="${HOME:?HOME is required}/.local/bin"
fi
target="$install_dir/iq"
previous="$install_dir/iq.previous"

mkdir -p "$install_dir"
if [ -L "$target" ] || [ -L "$previous" ]; then
  printf 'refusing symbolic-link install or rollback target in %s\n' "$install_dir" >&2
  exit 1
fi

if [ "$rollback" -eq 1 ]; then
  [ -f "$previous" ] || { printf 'no rollback binary at %s\n' "$previous" >&2; exit 1; }
  swap="$install_dir/.iq-rollback-$$"
  if [ -f "$target" ]; then
    cp "$target" "$swap"
  fi
  cp "$previous" "$install_dir/.iq-install-$$"
  chmod 0755 "$install_dir/.iq-install-$$"
  mv "$install_dir/.iq-install-$$" "$target"
  if [ -f "$swap" ]; then
    mv "$swap" "$previous"
  fi
  printf 'Rolled back IndexQube: %s\n' "$("$target" --version)"
  exit 0
fi

if [ "$version" = "latest" ]; then
  [ -z "$release_dir" ] || { printf 'set --version when using INDEXQUBE_RELEASE_DIR\n' >&2; exit 2; }
  latest_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest")"
  version="${latest_url##*/}"
fi
case "$version" in
  v[0-9]*)
    case "$version" in
      *[!A-Za-z0-9._-]*) printf 'invalid release version: %s\n' "$version" >&2; exit 2 ;;
    esac ;;
  *) printf 'invalid release version: %s\n' "$version" >&2; exit 2 ;;
esac

case "$(uname -s)" in
  Darwin) target_os=darwin ;;
  Linux) target_os=linux ;;
  *) printf 'unsupported operating system: %s\n' "$(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64) target_arch=amd64 ;;
  arm64|aarch64) target_arch=arm64 ;;
  *) printf 'unsupported architecture: %s\n' "$(uname -m)" >&2; exit 1 ;;
esac

asset="indexqube_${version}_${target_os}_${target_arch}.tar.gz"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/indexqube-install.XXXXXX")"
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT HUP INT TERM

fetch() {
  name="$1"
  destination="$2"
  if [ -n "$release_dir" ]; then
    cp "$release_dir/$name" "$destination"
  else
    curl -fsSL "https://github.com/$repo/releases/download/$version/$name" -o "$destination"
  fi
}

fetch "$asset" "$tmp/$asset"
fetch checksums.txt "$tmp/checksums.txt"
expected="$(awk -v name="$asset" '$2 == name || $2 == "*" name { print $1; exit }' "$tmp/checksums.txt")"
[ -n "$expected" ] || { printf 'release checksum does not list %s\n' "$asset" >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
else
  actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
fi
[ "$actual" = "$expected" ] || { printf 'checksum verification failed for %s\n' "$asset" >&2; exit 1; }

if [ -n "$release_dir" ]; then
  : # Offline/local release fixtures cannot have a GitHub attestation.
elif command -v gh >/dev/null 2>&1; then
  gh attestation verify "$tmp/$asset" --repo "$repo" >/dev/null
elif [ "$require_attestation" -eq 1 ]; then
  printf 'GitHub CLI is required for signed provenance verification; install gh or omit --require-attestation\n' >&2
  exit 1
else
  printf 'Checksum verified. Install gh to verify the release Sigstore attestation as well.\n' >&2
fi

mkdir -p "$tmp/unpack"
tar -xzf "$tmp/$asset" -C "$tmp/unpack"
[ -f "$tmp/unpack/iq" ] || { printf 'release archive does not contain iq\n' >&2; exit 1; }
chmod 0755 "$tmp/unpack/iq"
candidate_version="$("$tmp/unpack/iq" --version)"
[ "$candidate_version" = "iq $version" ] || { printf 'release version mismatch: %s\n' "$candidate_version" >&2; exit 1; }

if [ -e "$target" ] && [ ! -f "$target" ]; then
  printf 'refusing to replace non-regular file %s\n' "$target" >&2
  exit 1
fi
if [ -f "$target" ]; then
  cp "$target" "$install_dir/.iq-previous-$$"
  chmod 0755 "$install_dir/.iq-previous-$$"
  mv "$install_dir/.iq-previous-$$" "$previous"
fi
cp "$tmp/unpack/iq" "$install_dir/.iq-install-$$"
chmod 0755 "$install_dir/.iq-install-$$"
mv "$install_dir/.iq-install-$$" "$target"

if [ "$("$target" --version)" != "iq $version" ]; then
  if [ -f "$previous" ]; then
    cp "$previous" "$target"
    chmod 0755 "$target"
  else
    rm -f -- "$target"
  fi
  printf 'post-install validation failed; restored the previous binary\n' >&2
  exit 1
fi

printf 'Installed %s at %s\n' "$("$target" --version)" "$target"
case ":${PATH:-}:" in
  *":$install_dir:"*) ;;
  *) printf 'Add %s to PATH, then run: iq\n' "$install_dir" ;;
esac
if [ -f "$previous" ]; then
  printf 'Rollback is available with: %s --rollback --install-dir %s\n' "$0" "$install_dir"
fi
