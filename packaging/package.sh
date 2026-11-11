#!/bin/sh
# Build the release binary and the .deb, .rpm and Arch packages into dist/.
#
# Needs cargo, clang, pkg-config, the PipeWire development files and nfpm
# (https://nfpm.goreleaser.com). The release workflow runs it on Debian 12,
# so the binary links against an old glibc and runs on newer distributions.
# The packages are for the machine it runs on: x86-64 or ARM64.
set -eu
cd "$(dirname "$0")/.."

version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
machine=$(uname -m)
case "$machine" in
    x86_64) NFPM_ARCH=amd64 ;;
    aarch64) NFPM_ARCH=arm64 ;;
    *) echo "unsupported machine $machine" >&2; exit 1 ;;
esac
export NFPM_ARCH

cargo build --release --locked

rm -rf dist
mkdir -p dist/stage
# The unit in the repository runs a cargo install; the packages install to /usr/bin.
sed 's|^ExecStart=.*|ExecStart=/usr/bin/sound-cast|; /^# Installed with/d' \
    sound-cast.service > dist/stage/sound-cast.service

for packager in deb rpm archlinux; do
    VERSION=$version nfpm package --config packaging/nfpm.yaml --packager "$packager" --target dist/
done

cp target/release/sound-cast "dist/sound-cast-$version-$machine-linux"
(cd dist && sha256sum sound-cast[-_]* > "SHA256SUMS-$machine")
ls -l dist
