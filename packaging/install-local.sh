#!/bin/bash
# Build sound-cast and the Plasma widget from these checkouts, as they are
# (uncommitted changes included), into Arch packages, and install them.
#
#   packaging/install-local.sh [--build-only] [WIDGET_DIR]
#
# WIDGET_DIR is the plasma-applet-sound-cast checkout, ../plasma-applet-sound-cast
# by default; without it only sound-cast is built. The packages go to
# dist/local/. Then pacman installs them (sudo asks for the password), the
# service restarts and the widget is reloaded with plasmashell.
#
# Arch and derivatives only (makepkg, pacman). Needs the PKGBUILDs' build
# dependencies: cargo, clang, git, gettext, make.
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)

build_only=false
if [[ ${1:-} == --build-only ]]; then
    build_only=true
    shift
fi
widget=${1:-$here/../plasma-applet-sound-cast}

out=$here/dist/local
rm -rf "$out"
mkdir -p "$out"

# Builds the PKGBUILD of a checkout from a tarball of its files instead of the
# git tag it names. $1: checkout, $2: the directory the PKGBUILD works in.
build() {
    local repo=$1 name=$2 work
    work=$(mktemp -d)
    # Tracked files plus new ones not ignored, as they are on disk now.
    (cd "$repo" && git ls-files -z --cached --others --exclude-standard) \
        | (cd "$repo" && tar --null -T - --transform "s|^|$name/|" -cf "$work/$name.tar")
    sed -e "s|^source=.*|source=(\"$name.tar\")|" \
        "$repo/packaging/arch/PKGBUILD" > "$work/PKGBUILD"
    (cd "$work" && PKGDEST=$out makepkg --force --cleanbuild --noconfirm)
    rm -rf "$work"
}

echo "== sound-cast"
build "$here" sound-cast
if [[ -f $widget/packaging/arch/PKGBUILD ]]; then
    echo "== Plasma widget"
    build "$(cd "$widget" && pwd)" plasma-applet-sound-cast
else
    echo "== no widget checkout at $widget: skipped"
fi
ls -l "$out"
$build_only && exit 0

sudo pacman -U "$out"/*.pkg.tar.zst
systemctl --user daemon-reload
if systemctl --user is-active --quiet sound-cast; then
    systemctl --user restart sound-cast
fi
sound-cast --version
# Plasma keeps the old widget code until plasmashell starts again.
if pgrep -x plasmashell >/dev/null && [[ -f $widget/packaging/arch/PKGBUILD ]]; then
    systemctl --user restart plasma-plasmashell
fi
