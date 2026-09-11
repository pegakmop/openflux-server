#!/bin/bash
set -e

# Builds the Go core (mobile/ package) into an .aar for the Android app.
# Unlike build_android.sh (which cross-compiles the CLI binary directly),
# this uses `gomobile bind`, which resolves the NDK toolchain itself - it
# just needs ANDROID_HOME/ANDROID_NDK_HOME pointed at a real SDK and NDK
# install. Run this from the repo root (it needs to be next to go.mod).
#
# Usage: ./build_android_aar.sh [output-libs-dir]
# Defaults to ../app/app/libs, which is correct if you have this repo
# (openflux-server) and openflux-app checked out side by side with those
# exact folder names (as in the original monorepo layout). If you cloned
# openflux-app somewhere else, pass its app/libs directory explicitly, e.g.:
#   ./build_android_aar.sh ../openflux-app/app/libs

: "${ANDROID_HOME:?Set ANDROID_HOME to your Android SDK path}"
: "${ANDROID_NDK_HOME:?Set ANDROID_NDK_HOME to your Android NDK path (e.g. \$ANDROID_HOME/ndk/<version>)}"

OUTPUT_DIR="${1:-../app/app/libs}"
OUTPUT_AAR="$OUTPUT_DIR/openflux.aar"

mkdir -p "$OUTPUT_DIR"

if ! command -v gomobile >/dev/null 2>&1; then
    echo "gomobile not found on PATH - installing it (one-time setup)..."
    go install golang.org/x/mobile/cmd/gomobile@latest
fi

# gomobile bind needs golang.org/x/mobile recorded as a tool dependency of
# this module (see `go help tool`); this is a no-op if it's already there.
go get -tool golang.org/x/mobile/cmd/gobind

echo "Building $OUTPUT_AAR (androidapi 26, arm64/arm/x86_64)..."
# -checklinkname=0: the MAX transport (transport/oneme) pulls in
# github.com/wlynxg/anet, which uses //go:linkname to reach into net's
# internals - restricted by Go's linker since 1.23 unless told otherwise
# (see anet's own README). Without this flag the link step fails with
# "invalid reference to net.zoneCache".
gomobile bind -target=android -androidapi 26 -ldflags="-checklinkname=0" -o "$OUTPUT_AAR" ./mobile

echo "Build successful: $OUTPUT_AAR"
