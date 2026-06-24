#!/usr/bin/env bash
set -eo pipefail
processor=$(uname -m)
OS_TYPE=$(uname -s)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
NIGHTLY_VERSION=$(cat "$REPO_ROOT/.rust-nightly")
REQUIRED_CHEADERGEN_VERSION=$(cat "$REPO_ROOT/.cheadergen-version")

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
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
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

# Install/update the stable toolchain explicitly (idempotent - rustup reports
# "unchanged" if already current). This also covers pre-existing rustup setups
# that do not yet have stable installed.
rustup update stable

# If `cargo` is a distro binary instead of a rustup proxy, `cargo +stable` will
# not work. Install rustup-init to create the standard ~/.cargo/bin proxies so
# later CI steps that run bare `cargo` respect rust-toolchain.toml.
if ! cargo +stable -vV >/dev/null 2>&1; then
    install_rustup "Installing rustup-init to create a rustup-proxied cargo..."
    rustup update stable
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
# Print where `cargo` is located and verify the stable toolchain is available.
# Use the rustup cargo proxy explicitly so we verify the binary persisted for
# later workflow steps.
echo "Cargo binary location: $cargo_bin"
cargo +stable -vV
# Ensure we have both clippy and rustfmt installed for the stable toolchain
# (rustup is also idempotent here - reports "up to date" if installed).
rustup component add --toolchain stable clippy rustfmt

# cheadergen is required when REDISEARCH_GENERATE_HEADERS=ON, the default
# for a top-level RediSearch build. It uses rustdoc JSON from the pinned
# nightly toolchain to regenerate the Rust C headers.
if should_generate_headers; then
    rustup toolchain install "$NIGHTLY_VERSION" \
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
        rustup run stable cargo install --force --locked "cheadergen_cli@${REQUIRED_CHEADERGEN_VERSION}"
    fi
else
    echo "Skipping cheadergen setup because REDISEARCH_GENERATE_HEADERS is disabled"
fi
