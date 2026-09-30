#!/bin/sh
set -eu
version=${1:-v0.1.0}
if [ "$version" != "v0.1.0" ]; then
  echo 'This release script targets the current CLI version v0.1.0' >&2
  exit 1
fi
mkdir -p dist
output_dir=$(pwd)/dist
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT HUP INT TERM
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do
  target_os=${target%/*}
  target_arch=${target#*/}
  archive="stalefill_${version}_${target_os}_${target_arch}"
  folder="$stage/$archive"
  mkdir -p "$folder"
  binary=stalefill
  if [ "$target_os" = windows ]; then binary=stalefill.exe; fi
  CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" go build -trimpath -buildvcs=false -ldflags='-s -w' -o "$folder/$binary" ./cmd/stalefill
  cp LICENSE README.md go.mod CONTRIBUTING.md CHANGELOG.md "$folder/"
  mkdir -p "$folder/docs/assets" "$folder/docs/traces" "$folder/examples"
  cp docs/*.md "$folder/docs/"
  cp docs/assets/stalefill.svg "$folder/docs/assets/"
  cp docs/traces/*.json "$folder/docs/traces/"
  cp examples/*.json "$folder/examples/"
  if [ "$target_os" = windows ]; then
    # zip otherwise updates existing archives and could retain stale members.
    rm -f "dist/$archive.zip"
    (cd "$stage" && zip -q -r "$output_dir/$archive.zip" "$archive")
  else
    tar -C "$stage" -czf "dist/$archive.tar.gz" "$archive"
  fi
done
(cd dist && shasum -a 256 stalefill_"$version"_*.tar.gz stalefill_"$version"_*.zip > SHA256SUMS)
