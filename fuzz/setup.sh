#!/usr/bin/env bash
set -euo pipefail

REDIS_REF=559d143b4e0409c452f7d229ba420954b6b647a2
REDIS_URL=https://github.com/redis/redis.git
MIN_CLANG_MAJOR=22

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${FUZZ_OUT:-$ROOT/bin/fuzz}"
REDIS_SRC="${REDIS_SRC:-}"
JOBS="${JOBS:-8}"
OS="$(uname -s)"

if [[ -n ${CC:-} ]]; then
  CLANG="$CC"
elif [[ $OS == Darwin ]]; then
  CLANG=/opt/homebrew/opt/llvm/bin/clang
else
  CLANG="$(command -v clang)"
fi

clang_major="$("$CLANG" -dumpversion | cut -d. -f1)"
if (( clang_major < MIN_CLANG_MAJOR )); then
  echo "setup.sh: $CLANG is clang $clang_major; need clang >= $MIN_CLANG_MAJOR" >&2
  exit 1
fi

echo "==> submodules"
git -C "$ROOT" submodule update --init --recursive

redis_dir="$OUT/redis-${REDIS_REF:0:9}-asan"
redis_server="$redis_dir/src/redis-server"
if [[ -x $redis_server ]]; then
  echo "==> redis-server: $redis_server (already built)"
else
  echo "==> redis-server: building ${REDIS_REF:0:9} with AddressSanitizer"
  rm -rf "$redis_dir"
  mkdir -p "$redis_dir"
  if [[ -n $REDIS_SRC ]]; then
    git -C "$REDIS_SRC" archive "$REDIS_REF" | tar -x -C "$redis_dir"
  else
    git -C "$redis_dir" init -q
    git -C "$redis_dir" fetch -q --depth 1 "$REDIS_URL" "$REDIS_REF"
    git -C "$redis_dir" checkout -q FETCH_HEAD
  fi
  make -j"$JOBS" -C "$redis_dir/src" redis-server SANITIZER=address MALLOC=libc CC="$CLANG"
fi

echo "==> redisearch.so: building with SAN=address"
PATH="$(dirname "$CLANG"):$PATH" "$ROOT/build.sh" SAN=address

module="$(find "$ROOT"/bin/*-debug-asan -maxdepth 0 -type d -not -type l | head -1)/search-community/redisearch.so"
[[ -f $module ]] || { echo "setup.sh: module not found at $module" >&2; exit 1; }

preload=""
if [[ $OS != Darwin ]] && command -v g++ > /dev/null; then
  preload="$(g++ -print-file-name=libstdc++.so.6)"
fi

cat > "$OUT/env.sh" <<EOF
export REDIS_SERVER="$redis_server"
export REDISEARCH_MODULE="$module"
export RS_GLOBAL_DTORS=1
export ASAN_OPTIONS="halt_on_error=1:detect_odr_violation=0:detect_leaks=1"
export LSAN_OPTIONS="suppressions=$ROOT/tests/memcheck/asan.supp:print_suppressions=0"
${preload:+export LD_PRELOAD="$preload"}
EOF

echo "==> wrote $OUT/env.sh"
cat "$OUT/env.sh"
