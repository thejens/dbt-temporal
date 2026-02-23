#!/usr/bin/env bash
# Build the ADBC PostgreSQL driver from source with statically linked OpenSSL,
# libpq, libpgcommon, and libpgport. The CDN-shipped driver uses
# -undefined dynamic_lookup which leaves OpenSSL/libpq symbols unresolved,
# causing SIGSEGV on macOS ARM64 at dlopen time.
set -euo pipefail

ADBC_REPO="https://github.com/dbt-labs/arrow-adbc"
ADBC_REV="2caa4db73b5dee142d71a10e3b3905715c6d99db"
SRC_DIR="/private/tmp/arrow-adbc-build"
DYLIB_NAME="libadbc_driver_postgresql-0.21.0+dbt0.21.0.dylib"
DEST_DIR="$HOME/Library/Caches/com.getdbt/adbc/aarch64-apple-darwin"

# --- Prerequisites -----------------------------------------------------------

check_prereq() {
    if ! command -v "$1" &>/dev/null; then
        echo "error: $1 not found" >&2; exit 1
    fi
}

check_brew_pkg() {
    if ! brew --prefix "$1" &>/dev/null; then
        echo "error: brew package $1 not installed (brew install $1)" >&2; exit 1
    fi
}

check_prereq cmake
check_prereq brew
check_brew_pkg libpq
check_brew_pkg openssl@3

LIBPQ_PREFIX="$(brew --prefix libpq)"
OPENSSL_PREFIX="$(brew --prefix openssl@3)"

# --- Clone / update source ---------------------------------------------------

if [ -d "$SRC_DIR/.git" ]; then
    echo "=> Updating existing checkout"
    git -C "$SRC_DIR" fetch origin
else
    echo "=> Cloning arrow-adbc"
    rm -rf "$SRC_DIR"
    git clone "$ADBC_REPO" "$SRC_DIR"
fi

git -C "$SRC_DIR" checkout "$ADBC_REV"

# --- Patch BuildUtils.cmake --------------------------------------------------
# Remove -undefined dynamic_lookup so the linker errors on unresolved symbols
# instead of silently deferring them to dlopen time.

BUILDUTILS="$SRC_DIR/c/cmake_modules/BuildUtils.cmake"
if grep -q 'set(ARG_SHARED_LINK_FLAGS "-undefined dynamic_lookup' "$BUILDUTILS"; then
    echo "=> Patching BuildUtils.cmake"
    sed -i '' '/set(ARG_SHARED_LINK_FLAGS "-undefined dynamic_lookup/s|^|# PATCHED: |' "$BUILDUTILS"
elif grep -q 'PATCHED.*-undefined dynamic_lookup' "$BUILDUTILS"; then
    echo "=> BuildUtils.cmake already patched"
else
    echo "error: could not find -undefined dynamic_lookup in BuildUtils.cmake" >&2
    exit 1
fi

# --- Configure & Build -------------------------------------------------------

BUILD_DIR="$SRC_DIR/build"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# libpgcommon_shlib.a / libpgport_shlib.a provide the frontend (non-_private)
# symbols that libpq.a needs (pg_char_to_encoding, SCRAM, base64, etc.).
# They are linked normally (not -force_load) so only referenced objects are pulled in.
LINKER_FLAGS="
    $LIBPQ_PREFIX/lib/libpgcommon_shlib.a
    $LIBPQ_PREFIX/lib/libpgport_shlib.a
    $OPENSSL_PREFIX/lib/libssl.a
    $OPENSSL_PREFIX/lib/libcrypto.a
    -lgssapi_krb5"

echo "=> Configuring cmake"
PKG_CONFIG_PATH="$LIBPQ_PREFIX/lib/pkgconfig:$OPENSSL_PREFIX/lib/pkgconfig" \
cmake -S "$SRC_DIR/c" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DADBC_DRIVER_POSTGRESQL=ON \
    -DADBC_DRIVER_MANAGER=OFF \
    -DADBC_DRIVER_FLIGHTSQL=OFF \
    -DADBC_BUILD_TESTS=OFF \
    -DADBC_BUILD_BENCHMARKS=OFF \
    -DADBC_WITH_VENDORED_FMT=ON \
    -DADBC_WITH_VENDORED_NANOARROW=ON \
    -DCMAKE_SHARED_LINKER_FLAGS="$LINKER_FLAGS"

echo "=> Building"
make -C "$BUILD_DIR" -j "$(sysctl -n hw.logicalcpu)"

# --- Install ------------------------------------------------------------------

BUILT_DYLIB="$(ls "$BUILD_DIR"/driver/postgresql/libadbc_driver_postgresql.*.*.*.dylib 2>/dev/null | head -1)"
if [ -z "$BUILT_DYLIB" ]; then
    echo "error: built dylib not found" >&2; exit 1
fi

mkdir -p "$DEST_DIR"
cp "$BUILT_DYLIB" "$DEST_DIR/$DYLIB_NAME"
echo "=> Installed to $DEST_DIR/$DYLIB_NAME"

# --- Verify -------------------------------------------------------------------

echo "=> Verifying"
python3 -c "
import ctypes, sys
try:
    ctypes.CDLL('$DEST_DIR/$DYLIB_NAME')
    print('  OK: dylib loads successfully')
except OSError as e:
    print(f'  FAIL: {e}', file=sys.stderr); sys.exit(1)
"

echo "Done."
