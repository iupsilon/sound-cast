#!/bin/sh
# Build the release binary and the .deb, .rpm and Arch packages into dist/.
#
# Needs cargo, clang, pkg-config, the PipeWire development files and nfpm
# (https://nfpm.goreleaser.com). The release workflow runs it on Debian 12,
# so the binary links against an old glibc and runs on newer distributions.
set -eu
cd "$(dirname "$0")/.."

version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

cargo build --release --locked

rm -rf dist
mkdir -p dist/stage
# The unit in the repository runs a cargo install; the packages install to /usr/bin.
sed 's|^ExecStart=.*|ExecStart=/usr/bin/sound-cast|; /^# Installed with/d' \
    sound-cast.service > dist/stage/sound-cast.service

for packager in deb rpm archlinux; do
    VERSION=$version nfpm package --config packaging/nfpm.yaml --packager "$packager" --target dist/
done

cp target/release/sound-cast "dist/sound-cast-$version-x86_64-linux"
(cd dist && sha256sum sound-cast[-_]* > SHA256SUMS)
ls -l dist
