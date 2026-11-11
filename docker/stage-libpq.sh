#!/bin/sh
# Stage libpq, and every library it needs that a distroless base lacks, for
# copying into that base. Runs in a debian:bookworm-slim build stage.
#
#   stage-libpq.sh <base-root> <out-dir>
#
# <base-root> is the distroless image's filesystem, copied into the build stage;
# <out-dir> receives a tree to `COPY` over it.
#
# Why: dbt's Postgres ADBC driver (downloaded from dbt's CDN on first use)
# links libpq.so.5 dynamically, and distroless ships no libpq. Every other
# driver dbt downloads needs nothing beyond glibc/libstdc++, which the cc image
# has. Libraries the base already carries (glibc, libssl, ...) are left out on
# purpose: two copies of glibc in one image is how a process ends up loading a
# libc that does not match its dynamic loader.
#
# For each staged package, its Debian copyright file and its dpkg status
# stanza go along too: the first because distributing the library carries its
# license, the second so image scanners (which read distroless's
# /var/lib/dpkg/status.d/) see the package and report its CVEs.
set -eu

base=$1
out=$2

apt-get update
apt-get install -y --no-install-recommends libpq5

libpq=$(ldconfig -p | awk '/libpq\.so\.5 /{print $NF; exit}')
[ -n "$libpq" ] || { echo "libpq.so.5 not found after install" >&2; exit 1; }

packages=""
for lib in "$libpq" $(ldd "$libpq" | awk '/=> \//{print $3}'); do
  name=$(basename "$lib")
  if [ -n "$(find "$base/lib" "$base/usr/lib" -name "$name" -print -quit 2>/dev/null)" ]; then
    continue
  fi
  real=$(readlink -f "$lib")
  # The soname is what the loader looks up; store the file under it, in the
  # multiarch directory the loader searches by default.
  dest="$out/usr/lib/$(basename "$(dirname "$real")")"
  mkdir -p "$dest"
  cp "$real" "$dest/$name"
  packages="$packages $(dpkg -S "*/$(basename "$real")" | head -1 | cut -d: -f1)"
done

for pkg in $(echo "$packages" | tr ' ' '\n' | sort -u); do
  mkdir -p "$out/usr/share/doc/$pkg" "$out/var/lib/dpkg/status.d"
  cp "/usr/share/doc/$pkg/copyright" "$out/usr/share/doc/$pkg/copyright"
  dpkg -s "$pkg" > "$out/var/lib/dpkg/status.d/$pkg"
done

echo "staged:" && find "$out" -type f | sort
