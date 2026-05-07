#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
extension_dir="$repo_root/extension"
output="${EXTENSION_ZIP:-$repo_root/dist/indexqube-extension.zip}"

required_files=(
  manifest.json
  background.js
  content.css
  content.js
  popup.css
  popup.html
  popup.js
  options.css
  options.html
  options.js
)

for file in "${required_files[@]}"; do
  if [[ ! -f "$extension_dir/$file" ]]; then
    printf 'missing extension asset: %s\n' "$file" >&2
    exit 1
  fi
done

mkdir -p "$(dirname "$output")"
rm -f "$output"

zip_args=("${required_files[@]}")
if [[ -d "$extension_dir/icons" ]]; then
  zip_args+=(icons)
fi

(
  cd "$extension_dir"
  zip -qr "$output" "${zip_args[@]}" \
    -x '*.DS_Store' \
    -x '__MACOSX/*' \
    -x '*/__MACOSX/*'
)

printf 'Packaged extension: %s\n' "$output"
