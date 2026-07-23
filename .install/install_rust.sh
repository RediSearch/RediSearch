#!/usr/bin/env bash
set -eo pipefail
processor=$(uname -m)
OS_TYPE=$(uname -s)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
NIGHTLY_VERSION=$(cat "$REPO_ROOT/.rust-nightly")
REQUIRED_CHEADERGEN_VERSION=$(cat "$REPO_ROOT/.cheadergen-version")
NEXTEST_VERSION=$(cat "$REPO_ROOT/.nextest-version")
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

# dry-run prints these, real runs them (see deps_lib.sh _sh/_env). No summary:
# the script flows through the same path in every mode, so dry-run can't drift
# from what a real bootstrap does — including the shell/env setup below.
install_rustup() {
    if [ "${DRY_RUN:-0}" != 1 ]; then echo "${1:-Installing Rust toolchain via rustup-init...}"; fi
    # --default-toolchain none: don't let rustup-init install `stable`; the
    # repo's pinned toolchain is installed explicitly below.
    _sh "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none"
    # Source the freshly-installed env for the rest of THIS process (real mode).
    # Not printed: the dry-run env prefix below already puts ~/.cargo/bin on PATH
    # for a paste, so printing here would just duplicate it.
    [ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env" || true
    export PATH="$HOME/.cargo/bin:$PATH"
    hash -r
}

# Make an already-installed rust (from a parent shell / prior run) visible so the
# presence checks below are accurate. Detection only: runs in every mode but is
# NEVER printed — it's not a step the user pastes. The env setup that IS pasted
# comes from install_rustup, which only runs when rust is actually being installed.
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env" || true
export PATH="$HOME/.cargo/bin:$PATH"
hash -r

# list: record the build-relevant deps (nothing installed). rust = rustup AND
# cargo (mirrors the real install condition); cheadergen when header-gen is on.
if [ "${CHECK_DEPS:-0}" = 1 ]; then
    if command -v rustup >/dev/null 2>&1 && command -v cargo >/dev/null 2>&1; then DEPS_OK="$DEPS_OK rust"; else DEPS_MISSING="$DEPS_MISSING rust"; fi
    if should_generate_headers; then
        if [ "$(cheadergen --version 2>/dev/null | awk '{print $NF}' || true)" = "$REQUIRED_CHEADERGEN_VERSION" ]; then DEPS_OK="$DEPS_OK cheadergen"; else DEPS_MISSING="$DEPS_MISSING cheadergen"; fi
    fi
    return 0 2>/dev/null || exit 0
fi

# ── DRY_RUN prints each command; real executes. Same flow in both. ───────────

# The rustup/cargo commands below live in ~/.cargo/bin, which a freshly-opened
# shell won't have on PATH. So in dry-run, if ANY rust step is pending, emit the
# env setup FIRST — otherwise a pasted "rustup …" line dies with "command not
# found" (the real bootstrap has it on PATH in-process; a fresh paste does not).
# When every step is already satisfied nothing is pending and no env is emitted.
if [ "${DRY_RUN:-0}" = 1 ]; then
    _need=0
    command -v rustup >/dev/null 2>&1 && command -v cargo >/dev/null 2>&1 || _need=1
    rustup toolchain list 2>/dev/null | grep -q "$PINNED_VERSION" || _need=1
    [ "$(cargo nextest --version 2>/dev/null | head -1 | awk '{print $2}' || true)" = "$NEXTEST_VERSION" ] || _need=1
    if should_generate_headers; then
        rustup toolchain list 2>/dev/null | grep -q "$NIGHTLY_VERSION" || _need=1
        [ "$(cheadergen --version 2>/dev/null | awk '{print $NF}' || true)" = "$REQUIRED_CHEADERGEN_VERSION" ] || _need=1
    fi
    if [ "$_need" = 1 ]; then
        _dry_line '[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env" || true'
        _dry_line 'export PATH="$HOME/.cargo/bin:$PATH"'
    fi
fi

if ! command -v rustup >/dev/null 2>&1 || ! command -v cargo >/dev/null 2>&1; then
    install_rustup "Installing Rust toolchain via rustup-init..."
elif [ "${DRY_RUN:-0}" != 1 ]; then
    echo "rustup and cargo already installed - skipping rustup-init"
fi

# Install the repo's pinned toolchain explicitly — skip when it's already present
# so a provisioned host emits nothing (same end state either way: the install is
# a no-op when the exact version exists). --profile=minimal + explicit
# clippy/rustfmt (which `make lint`/`make fmt` need); rustdoc HTML is omitted.
if ! rustup toolchain list 2>/dev/null | grep -q "$PINNED_VERSION"; then
    _sh "rustup toolchain install --profile=minimal \"$PINNED_VERSION\" -c clippy -c rustfmt"
fi

# If `cargo` is a distro binary instead of a rustup proxy, `cargo +<pinned>`
# won't work; install rustup-init to create the ~/.cargo/bin proxies. The probe
# assumes an installed state, so it's real-only (skipped in dry-run, where the
# install above hasn't actually run — avoids a false re-trigger).
if [ "${DRY_RUN:-0}" != 1 ] && ! cargo +"$PINNED_VERSION" -vV >/dev/null 2>&1; then
    install_rustup "Installing rustup-init to create a rustup-proxied cargo..."
    _sh "rustup toolchain install --profile=minimal \"$PINNED_VERSION\" -c clippy -c rustfmt"
fi

# Binary-location bookkeeping, GITHUB_PATH persistence and the pinned-toolchain
# verification all assume an installed state → real-only (not part of the paste).
if [ "${DRY_RUN:-0}" != 1 ]; then
    rustup_bin="$(command -v rustup)"
    cargo_bin="$(command -v cargo)"
    rustup_bin_dir="$(dirname "$rustup_bin")"
    cargo_bin_dir="$(dirname "$cargo_bin")"
    if [[ -n "${GITHUB_PATH:-}" ]]; then
        echo "$cargo_bin_dir" >> "$GITHUB_PATH"
        if [[ "$rustup_bin_dir" != "$cargo_bin_dir" ]]; then
            echo "$rustup_bin_dir" >> "$GITHUB_PATH"
        fi
    fi
    echo "Rustup binary location: $rustup_bin"
    echo "Cargo binary location: $cargo_bin"
    cargo +"$PINNED_VERSION" -vV
fi

# Cargo subcommands install into Cargo home (cargo may be a system binary in a
# dir a non-root bootstrap can't write to). Put it on PATH — functional/detection
# only, never printed (install_rustup already puts ~/.cargo/bin on PATH in a paste).
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
cargo_home_bin_dir="${CARGO_HOME:-$HOME/.cargo}/bin"
if [ "${DRY_RUN:-0}" != 1 ]; then
    mkdir -p "$cargo_home_bin_dir"
    hash -r
    if [[ -n "${GITHUB_PATH:-}" &&
          "$cargo_home_bin_dir" != "$cargo_bin_dir" &&
          "$cargo_home_bin_dir" != "$rustup_bin_dir" ]]; then
        echo "$cargo_home_bin_dir" >> "$GITHUB_PATH"
    fi
fi

# cargo-nextest — the runner `make test` / `make rust-tests` invoke. Pinned via
# .nextest-version; Linux uses the static musl prebuilt (works on glibc & musl).
if [ "$(cargo nextest --version 2>/dev/null | head -1 | awk '{print $2}' || true)" != "$NEXTEST_VERSION" ]; then
    if [[ "$OS_TYPE" = 'Darwin' ]]; then
        nextest_artifact="mac"
    elif [[ "$processor" =~ ^(aarch64|arm64)$ ]]; then
        nextest_artifact="linux-arm-musl"
    else
        nextest_artifact="linux-musl"
    fi
    _sh "curl -L --proto '=https' --tlsv1.2 -sSf \"https://get.nexte.st/${NEXTEST_VERSION}/${nextest_artifact}\" | tar zxf - -C \"$cargo_home_bin_dir\""
    hash -r
fi

# cheadergen — required when REDISEARCH_GENERATE_HEADERS=ON (default). Uses
# rustdoc JSON from the pinned nightly toolchain to regenerate the Rust C headers.
if should_generate_headers; then
    if ! rustup toolchain list 2>/dev/null | grep -q "$NIGHTLY_VERSION"; then
        _sh "rustup toolchain install \"$NIGHTLY_VERSION\" --profile=minimal --allow-downgrade --component rust-docs-json"
    fi
    if [ "$(cheadergen --version 2>/dev/null | awk '{print $NF}' || true)" != "$REQUIRED_CHEADERGEN_VERSION" ]; then
        _sh "rustup run \"$PINNED_VERSION\" cargo install --force --locked \"cheadergen_cli@${REQUIRED_CHEADERGEN_VERSION}\""
    fi
elif [ "${DRY_RUN:-0}" != 1 ]; then
    echo "Skipping cheadergen setup because REDISEARCH_GENERATE_HEADERS is disabled"
fi
