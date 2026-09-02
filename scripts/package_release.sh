#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${1:-}"
dist_dir="${2:-$repo_root/dist}"

if [[ ! "$version" =~ ^v[0-9][0-9A-Za-z._-]*$ ]] && [[ "$version" != "dev" ]]; then
  printf 'usage: %s VERSION [DIST_DIR] (VERSION must start with v and a digit)\n' "$0" >&2
  exit 2
fi

mkdir -p "$dist_dir"
dist_dir="$(cd "$dist_dir" && pwd)"
rm -f -- "$dist_dir"/indexqube_*.tar.gz "$dist_dir/install.sh" "$dist_dir/checksums.txt"
rm -rf -- "$dist_dir/stage"
mkdir -p "$dist_dir/stage"

source_epoch="${SOURCE_DATE_EPOCH:-$(git -C "$repo_root" show -s --format=%ct HEAD)}"

build_bundle() {
  local target_os="$1"
  local target_arch="$2"
  local name="indexqube_${version}_${target_os}_${target_arch}"
  local stage="$dist_dir/stage/$name"
  mkdir -p "$stage"
  (
    cd "$repo_root/gateway"
    CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" go build -trimpath \
      -ldflags="-s -w -X main.version=$version" -o "$stage/iq" ./cmd/iq
    CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" go build -trimpath \
      -ldflags="-s -w" -o "$stage/indexqube-gateway" ./cmd/gateway
  )
  cp "$repo_root/README.md" "$stage/README.md"
  cp "$repo_root/scripts/install.sh" "$stage/install.sh"
  chmod 0755 "$stage/iq" "$stage/indexqube-gateway" "$stage/install.sh"
  python3 "$repo_root/scripts/reproducible_tar.py" "$stage" "$dist_dir/$name.tar.gz" "$source_epoch"
}

for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
  build_bundle "${target%/*}" "${target#*/}"
done

cp "$repo_root/scripts/install.sh" "$dist_dir/install.sh"
(
  cd "$dist_dir"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum indexqube_*.tar.gz install.sh | LC_ALL=C sort -k2 > checksums.txt
  else
    shasum -a 256 indexqube_*.tar.gz install.sh | LC_ALL=C sort -k2 > checksums.txt
  fi
)
rm -rf -- "$dist_dir/stage"
printf 'release bundles written to %s\n' "$dist_dir"
