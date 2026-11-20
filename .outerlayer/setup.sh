#!/usr/bin/env bash
# Prepares an OuterLayer build checkout to build and test RediSearch.
# OuterLayer runs this before every build, as the build user, and snapshots the result while this
# file and the version files on master stay unchanged, so every step must be cheap once done.
# System packages need root. OuterLayer documents that this script cannot install them, so it
# installs them only when root or passwordless sudo is there, and otherwise fails naming them.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

run_step() {
  local label=$1
  shift
  local started=$SECONDS
  echo "==> $label"
  "$@"
  echo "==> $label completed in $((SECONDS - started))s"
}

export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$HOME/.local/redis/bin:$PATH"
mkdir -p "$HOME/.local/bin"

# Build agents run commands in login and non-login shells alike.
profile_marker="# RediSearch build tools"
for rc in "$HOME/.bash_profile" "$HOME/.profile" "$HOME/.bashrc"; do
  if ! grep -Fqx "$profile_marker" "$rc" 2>/dev/null; then
    cat >> "$rc" <<'EOF'

# RediSearch build tools
[[ -f "$HOME/.cargo/env" ]] && . "$HOME/.cargo/env"
export PATH="$HOME/.local/bin:$HOME/.local/redis/bin:$PATH"
[[ -d "$HOME/.local/llvm/lib" ]] && export LIBCLANG_PATH="$HOME/.local/llvm/lib"
EOF
  fi
done

run_step "Describe the build host" bash -c '
  uname -srm; grep -E "^(NAME|VERSION_ID)=" /etc/os-release || true
  echo "user: $(id -un) uid=$(id -u)"; nproc; free -g | head -2 || true; df -h "$HOME" | tail -1
  if [[ $(id -u) -eq 0 ]]; then echo "root: yes"
  elif sudo -n true 2>/dev/null; then echo "root: passwordless sudo"
  else echo "root: none"; fi
  for t in clang-21 clang clang++ cmake make gcc g++ pkg-config python3 curl git lcov rsync unzip; do
    printf "%-10s %s\n" "$t" "$(command -v "$t" || echo missing)"
  done
  pkg-config --modversion openssl 2>/dev/null && echo "openssl headers: yes" || echo "openssl headers: missing"'

if [[ "$(git rev-parse --is-shallow-repository)" == "true" ]]; then
  run_step "Fetch full Git history" git fetch --unshallow --tags origin
fi
run_step "Initialize submodules" git submodule update --init --recursive

# OuterLayer's managed sandboxes mount system directories read-only, root or not.
if [[ ! -w /var/lib/apt && $(id -u) -eq 0 ]] || ! { [[ $(id -u) -eq 0 ]] || sudo -n true 2>/dev/null; }; then
  install_mode=none
elif [[ $(id -u) -eq 0 ]]; then
  install_mode=""
elif sudo -n test -w /var/lib/apt 2>/dev/null; then
  install_mode=sudo
else
  install_mode=none
fi

# COV=1 includes the LCOV that release coverage builds require.
check_dependencies() {
  (cd .install && env CHECK_DEPS=1 SKIP_BOOST=1 COV=1 bash ./install_script.sh "${install_mode/none/}")
}

check_setup_tools() {
  local missing=0 tool
  for tool in curl tar lcov genhtml geninfo rsync unzip; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "Missing setup tool: $tool" >&2
      missing=1
    fi
  done
  return "$missing"
}

source .install/LLVM_VERSION.sh

# Without root, LLVM goes in the home directory from the release tarball .install/install_llvm.sh
# uses, with the versioned names build.sh looks up linked onto PATH.
install_llvm_in_home() {
  local dir="$HOME/.local/llvm" bin tool target tmp
  if [[ "$(cat "$dir/.version" 2>/dev/null)" != "$LLVM_FULL_VERSION" ]]; then
    tmp="$(mktemp -d)"
    curl -fSL --retry 3 -o "$tmp/llvm.tar.xz" \
      "https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_FULL_VERSION/LLVM-$LLVM_FULL_VERSION-Linux-X64.tar.xz"
    rm -rf "$dir"
    mkdir -p "$dir"
    tar -xf "$tmp/llvm.tar.xz" -C "$dir" --strip-components=1
    rm -rf "$tmp"
    printf '%s\n' "$LLVM_FULL_VERSION" > "$dir/.version"
  fi
  bin="$dir/bin"
  for tool in "clang-$LLVM_VERSION" "clang++-$LLVM_VERSION" "lld-$LLVM_VERSION" \
    "ld.lld-$LLVM_VERSION" ld.lld llvm-config; do
    target="$bin/$tool"
    [[ -e "$target" ]] || target="$bin/${tool%-"$LLVM_VERSION"}"
    ln -sf "$target" "$HOME/.local/bin/$tool"
  done
  # The system linker's LTO plugin is the distro LLVM's, which cannot read this LLVM's bitcode, so
  # an `-flto` link (Redis's jemalloc, LTO module builds) must use this LLVM's lld. Clang reads
  # these files beside its binary, and an unused option in them raises no warning.
  for driver in clang clang++; do
    printf '%s\n' '-fuse-ld=lld' > "$bin/$driver.cfg"
  done
}

if [[ $install_mode == none ]]; then
  run_step "Install LLVM $LLVM_FULL_VERSION in the home directory" install_llvm_in_home
  export LIBCLANG_PATH="$HOME/.local/llvm/lib"
elif ! check_dependencies; then
  run_step "Install build and test dependencies" \
    bash -c "cd .install && env SKIP_BOOST=1 COV=1 bash ./install_script.sh $install_mode"
fi

install_rust_test_dependencies() {
  local fingerprint marker nightly
  fingerprint="$(sha256sum \
    .install/test_deps/install_rust_deps.sh \
    .rust-nightly \
    .nextest-version \
    .cheadergen-version \
    .cargo-llvm-cov-version | sha256sum | cut -d' ' -f1)"
  marker="$HOME/.cache/redisearch-rust-test-deps"
  nightly="$(<.rust-nightly)"

  if [[ -f "$marker" && "$(<"$marker")" == "$fingerprint" ]] &&
    rustup component list --installed --toolchain "$nightly" 2>/dev/null | grep -q '^miri'; then
    return 0
  fi

  if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
      sh -s -- -y --default-toolchain none --no-modify-path
  fi
  bash .install/test_deps/install_rust_deps.sh "${install_mode/none/}"
  mkdir -p "$(dirname "$marker")"
  printf '%s\n' "$fingerprint" > "$marker"
}

run_step "Install Rust test dependencies" install_rust_test_dependencies

if ! check_dependencies; then
  echo "Build dependencies are still missing; the list above names them." >&2
  exit 1
fi
check_setup_tools ||
  echo "note: coverage builds (COV=1) cannot run until LCOV is in the image" >&2

install_cargo_mutants() {
  local version=27.1.0
  if cargo mutants --version 2>/dev/null | grep -Fqx "cargo-mutants $version"; then
    return 0
  fi
  # Built from source: the prebuilt binary needs a newer glibc than some images have.
  cargo install --locked --force cargo-mutants@"$version"
}

run_step "Install cargo-mutants" install_cargo_mutants

install_python_test_dependencies() {
  if ! command -v uv >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | env UV_NO_MODIFY_PATH=1 sh
  fi
  uv sync --locked --all-packages
}

run_step "Install Python test dependencies" install_python_test_dependencies

# tests/pytests/test_alter_selective_backfill.py drives swamp.
install_swamp() (
  local version=20260825.013404.0-sha.59b73704
  local checksum=cecc630d8ad8eb2b5c8dda7a2758dd38ee5dd9a74357a04b2418f60fa4536975
  local swamp="$HOME/.local/bin/swamp"
  local archive tmp

  if [[ -x "$swamp" ]] && "$swamp" --version 2>/dev/null | grep -Fqx "swamp $version"; then
    return
  fi

  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  archive="swamp-$version-binary-linux-x86_64.tar.gz"
  curl -fsSL --proto '=https' --proto-redir '=https' --retry 3 --retry-delay 5 \
    "https://artifacts.swamp-club.com/swamp/$version/binary/linux/x86_64/$archive" \
    -o "$tmp/$archive"
  printf '%s  %s\n' "$checksum" "$tmp/$archive" | sha256sum --check
  tar -xzf "$tmp/$archive" -C "$tmp" swamp
  mkdir -p "$(dirname "$swamp")"
  install -m 0755 "$tmp/swamp" "$swamp"
)

run_step "Install swamp" install_swamp

versioned_clang="$(command -v "clang-$LLVM_VERSION" || command -v clang)"
versioned_clangxx="$(command -v "clang++-$LLVM_VERSION" || command -v clang++)"

# Without root, unversioned compiler names go in ~/.local/bin, ahead of the system's.
for pair in "clang:$versioned_clang" "clang++:$versioned_clangxx" \
  "cc:$versioned_clang" "c++:$versioned_clangxx"; do
  ln -sf "${pair#*:}" "$HOME/.local/bin/${pair%%:*}"
done

build_redis() {
  # Keep the flow-test server aligned with the Redis revision CI uses.
  local sanitizer=$1
  local redis_ref=559d143b4e0409c452f7d229ba420954b6b647a2
  local redis_url=https://github.com/redis/redis.git
  local redis_src="$HOME/.cache/redisearch-redis-src"
  local redis_prefix="$HOME/.local/redis${sanitizer:+-$sanitizer}"

  if [[ -x "$redis_prefix/bin/redis-server" &&
        "$(cat "$redis_prefix/.redis-ref" 2>/dev/null)" == "$redis_ref" ]]; then
    return
  fi

  if [[ ! -d "$redis_src/.git" ]]; then
    rm -rf "$redis_src"
    git init -q "$redis_src"
    git -C "$redis_src" remote add origin "$redis_url"
  fi
  git -C "$redis_src" fetch --depth=1 origin "$redis_ref"
  git -C "$redis_src" checkout --detach --force FETCH_HEAD
  git -C "$redis_src" clean -fdx
  rm -rf "$redis_prefix"
  make -C "$redis_src" -j"$(nproc)" PREFIX="$redis_prefix" BUILD_TLS=yes \
    CC="$versioned_clang" CXX="$versioned_clangxx" LD="$versioned_clang" \
    SANITIZER="$sanitizer" install
  printf '%s\n' "$redis_ref" > "$redis_prefix/.redis-ref"
}

run_step "Install the flow-test Redis server" build_redis ""
run_step "Install the ASan flow-test Redis server" build_redis address

cat > "$HOME/.local/bin/redis-server" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ ${SANITIZER:-} == address || ${SANITIZER:-} == addr ]]; then
  exec "$HOME/.local/redis-address/bin/redis-server" "$@"
fi
exec "$HOME/.local/redis/bin/redis-server" "$@"
EOF
chmod +x "$HOME/.local/bin/redis-server"

run_step "Verify the login-shell toolchain" env -i HOME="$HOME" PATH=/usr/local/bin:/usr/bin:/bin \
  /bin/bash -lc 'for t in cargo rustc cargo-nextest cargo-mutants cargo-hakari cheadergen uv swamp \
    redis-server cmake clang cc c++ python3; do command -v "$t" || { echo "missing: $t"; exit 1; }; done'

echo "==> Build setup complete"
