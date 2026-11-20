#!/usr/bin/env bash
set -euo pipefail

REDIS_REF=559d143b4e0409c452f7d229ba420954b6b647a2
REDIS_URL=https://github.com/redis/redis.git
MIN_CLANG_MAJOR=22

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/bin/fuzz"
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

echo "==> redisearch.so: building with SAN=address FUZZ_COV=1"
PATH="$(dirname "$CLANG"):$PATH" "$ROOT/build.sh" FUZZ_COV=1 "$@"

module="$(find "$ROOT"/bin/*-debug-asan-fuzz -maxdepth 0 -type d -not -type l | head -1)/search-community/redisearch.so"
[[ -f $module ]] || { echo "setup.sh: module not found at $module" >&2; exit 1; }

# The planted module is the same build with fuzz/redisearch/planted/planted.patch applied:
# known bugs for testing the fuzzer itself. Both rebuilds are incremental.
echo "==> planted redisearch.so"
patch="$ROOT/fuzz/redisearch/planted/planted.patch"
planted="$OUT/planted/redisearch.so"
git -C "$ROOT" apply "$patch"
trap 'git -C "$ROOT" apply -R "$patch"' EXIT
PATH="$(dirname "$CLANG"):$PATH" "$ROOT/build.sh" FUZZ_COV=1 "$@"
mkdir -p "$(dirname "$planted")"
cp "$module" "$planted"
git -C "$ROOT" apply -R "$patch"
trap - EXIT
PATH="$(dirname "$CLANG"):$PATH" "$ROOT/build.sh" FUZZ_COV=1 "$@"

# RedisJSON, built the way tests/pytests/runtests.sh builds it: REJSON_PATH
# names a prebuilt module, REJSON_BRANCH a branch to build (default master).
# RedisJSON's sanitizer build assumes Linux (a target triple with an
# environment, an ELF linker flag), so on macOS it is built without one.
echo "==> rejson.so"
rejson_san=address
[[ $OS == Darwin ]] && rejson_san=""
set +u
ROOT="$ROOT" BINROOT="$(dirname "$(dirname "$module")")" SAN="$rejson_san" \
  source "$ROOT/tests/deps/setup_rejson.sh"
set -u
cd "$ROOT"

# The fuzzer finds the binaries under these names.
ln -sf "$redis_server" "$OUT/redis-server"
ln -sf "$module" "$OUT/redisearch.so"
ln -sf "$JSON_BIN_PATH" "$OUT/rejson.so"
symbolizer="$(dirname "$CLANG")/llvm-symbolizer"
[[ -x $symbolizer ]] || symbolizer="$(command -v llvm-symbolizer)"
ln -sf "$symbolizer" "$OUT/llvm-symbolizer"
rm -f "$OUT/env.sh"
echo "==> binaries in $OUT"
ls -l "$OUT"
