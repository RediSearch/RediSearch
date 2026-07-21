#!/usr/bin/env bash
set -eo pipefail
processor=$(uname -m)
OS_TYPE=$(uname -s)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
NIGHTLY_VERSION=$(cat "$REPO_ROOT/.rust-nightly")
REQUIRED_CHEADERGEN_VERSION=$(cat "$REPO_ROOT/.cheadergen-version")
# The repo pins an exact toolchain in rust-toolchain.toml. Use it as our baseline
# instead of installing a separate `stable`: nothing needs a distinct `stable`
# (all builds resolve to this pinned version via the override), and a separate
# `stable` just duplicates the same toolchain on disk.
PINNED_VERSION=$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$REPO_ROOT/rust-toolchain.toml" | head -1)
if [[ -z "$PINNED_VERSION" ]]; then
    echo "Could not determine the pinned toolchain from $REPO_ROOT/rust-toolchain.toml" >&2
    exit 1
fi

should_generate_headers() {
    case "${REDISEARCH_GENERATE_HEADERS:-1}" in
        0|OFF|off|false|FALSE|False|NO|no)
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

install_rustup() {
    echo "${1:-Installing Rust toolchain via rustup-init...}"
    # --default-toolchain none: don't let rustup-init install `stable`; the
    # repo's pinned toolchain is installed explicitly below.
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
    if [[ -f "$HOME/.cargo/env" ]]; then
        # shellcheck disable=SC1091
        source "$HOME/.cargo/env"
    fi
    export PATH="$HOME/.cargo/bin:$PATH"
    hash -r
}

# Skip the rustup-init download/run if rustup and cargo are already on PATH,
# or reachable from the standard ~/.cargo install location. A previous run
# of this script may have installed them in a parent shell whose PATH we
# did not inherit.
if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi
export PATH="$HOME/.cargo/bin:$PATH"
hash -r

if ! command -v rustup >/dev/null 2>&1 || ! command -v cargo >/dev/null 2>&1; then
    install_rustup "Installing Rust toolchain via rustup-init..."
else
    echo "rustup ($(rustup --version 2>/dev/null | head -1)) and cargo already installed - skipping rustup-init"
fi

# Install the repo's pinned toolchain explicitly (idempotent - a no-op when the
# exact version is already present, since it's a fixed version, not a channel).
# We intentionally don't set a rustup default: in-repo invocations resolve the
# toolchain via rust-toolchain.toml, and the few out-of-repo invocations name it
# explicitly (`cargo +<pinned>`, `rustup run <pinned>`, `+<nightly>`).
# Use --profile=minimal so rustdoc HTML (~800 MB/toolchain; nothing here serves
# it) isn't pulled in. minimal omits clippy and rustfmt, which `make lint`/`make
# fmt` run on this toolchain, so request them explicitly. `rustup toolchain
# install` only adds components, so this never removes anything a pre-existing
# toolchain already has.
rustup toolchain install --profile=minimal "$PINNED_VERSION" -c clippy -c rustfmt

# If `cargo` is a distro binary instead of a rustup proxy, `cargo +<pinned>` will
# not work. Install rustup-init to create the standard ~/.cargo/bin proxies so
# later CI steps that run bare `cargo` respect rust-toolchain.toml.
if ! cargo +"$PINNED_VERSION" -vV >/dev/null 2>&1; then
    install_rustup "Installing rustup-init to create a rustup-proxied cargo..."
    rustup toolchain install --profile=minimal "$PINNED_VERSION" -c clippy -c rustfmt
fi

rustup_bin="$(command -v rustup)"
cargo_bin="$(command -v cargo)"
rustup_bin_dir="$(dirname "$rustup_bin")"
cargo_bin_dir="$(dirname "$cargo_bin")"

# GitHub Actions runs each step in a fresh shell, so export PATH above is
# only enough for this script. Persist the verified Rust binary directories
# for later steps that invoke rustup or cargo directly.
if [[ -n "${GITHUB_PATH:-}" ]]; then
    echo "$cargo_bin_dir" >> "$GITHUB_PATH"
    if [[ "$rustup_bin_dir" != "$cargo_bin_dir" ]]; then
        echo "$rustup_bin_dir" >> "$GITHUB_PATH"
    fi
fi

# Print where `rustup` is located for debugging purposes
echo "Rustup binary location: $rustup_bin"
# Print where `cargo` is located and verify the pinned toolchain is available.
# Use the rustup cargo proxy explicitly so we verify the binary persisted for
# later workflow steps.
echo "Cargo binary location: $cargo_bin"
cargo +"$PINNED_VERSION" -vV

# Cargo subcommands should be installed into Cargo home, not necessarily next
# to the `cargo` binary. `cargo` may come from /usr/bin or another
# system-managed directory that a non-root bootstrap cannot write to.
cargo_home_bin_dir="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$cargo_home_bin_dir"
export PATH="$cargo_home_bin_dir:$PATH"
hash -r
if [[ -n "${GITHUB_PATH:-}" &&
      "$cargo_home_bin_dir" != "$cargo_bin_dir" &&
      "$cargo_home_bin_dir" != "$rustup_bin_dir" ]]; then
    echo "$cargo_home_bin_dir" >> "$GITHUB_PATH"
fi

# cargo-nextest is the runner `make test` / `make rust-tests` invoke
# (build.sh runs `cargo nextest run`), so bootstrap must provision it or
# the documented `make bootstrap` -> `make test` flow dies with
# "no such command: `nextest`". Pinned via .nextest-version, which
# .install/test_deps/install_rust_deps.sh shares. On Linux the
# statically-linked musl prebuilt is used so the same artifact works on
# any glibc (and on musl systems) — the same preference
# test_deps/install_rust_deps.sh documents for its binstall fallbacks.
NEXTEST_VERSION=$(cat "$REPO_ROOT/.nextest-version")
have_nextest="$(cargo nextest --version 2>/dev/null | head -1 | awk '{print $2}' || true)"
if [[ "$have_nextest" == "$NEXTEST_VERSION" ]]; then
    echo "cargo-nextest $have_nextest already installed - skipping"
else
    if [[ -n "$have_nextest" ]]; then
        echo "cargo-nextest $have_nextest does not match required $NEXTEST_VERSION - installing"
    else
        echo "Installing cargo-nextest $NEXTEST_VERSION"
    fi
    if [[ "$OS_TYPE" = 'Darwin' ]]; then
        nextest_artifact="mac"
    elif [[ "$processor" =~ ^(aarch64|arm64)$ ]]; then
        nextest_artifact="linux-arm-musl"
    else
        nextest_artifact="linux-musl"
    fi
    curl -L --proto '=https' --tlsv1.2 -sSf \
        "https://get.nexte.st/${NEXTEST_VERSION}/${nextest_artifact}" \
        | tar zxf - -C "$cargo_home_bin_dir"
    hash -r
fi

# cheadergen is required when REDISEARCH_GENERATE_HEADERS=ON, the default
# for a top-level RediSearch build. It uses rustdoc JSON from the pinned
# nightly toolchain to regenerate the Rust C headers.
if should_generate_headers; then
    rustup toolchain install "$NIGHTLY_VERSION" \
        --profile=minimal \
        --allow-downgrade \
        --component rust-docs-json

    have_cheadergen="$(cheadergen --version 2>/dev/null | awk '{print $NF}' || true)"
    if [[ "$have_cheadergen" == "$REQUIRED_CHEADERGEN_VERSION" ]]; then
        echo "cheadergen $have_cheadergen already installed - skipping"
    else
        if [[ -n "$have_cheadergen" ]]; then
            echo "cheadergen $have_cheadergen does not match required $REQUIRED_CHEADERGEN_VERSION - installing"
        else
            echo "Installing cheadergen $REQUIRED_CHEADERGEN_VERSION"
        fi
        rustup run "$PINNED_VERSION" cargo install --force --locked "cheadergen_cli@${REQUIRED_CHEADERGEN_VERSION}"
    fi
else
    echo "Skipping cheadergen setup because REDISEARCH_GENERATE_HEADERS is disabled"
fi
