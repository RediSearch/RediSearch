#!/usr/bin/env bash
set -eo pipefail
processor=$(uname -m)
OS_TYPE=$(uname -s)

# Skip the rustup-init download/run if rustup and cargo are already on PATH,
# or reachable from the standard ~/.cargo install location. A previous run
# of this script may have installed them in a parent shell whose PATH we
# did not inherit.
if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi

if ! command -v rustup >/dev/null 2>&1 || ! command -v cargo >/dev/null 2>&1; then
    echo "Installing Rust toolchain via rustup-init..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    if [[ -f "$HOME/.cargo/env" ]]; then
        # shellcheck disable=SC1091
        source "$HOME/.cargo/env"
    fi
else
    echo "rustup ($(rustup --version 2>/dev/null | head -1)) and cargo already installed - skipping rustup-init"
fi

# Print where `rustup` is located for debugging purposes
echo "Rustup binary location: $(which rustup)"
# Verify Cargo is in path
cargo -vV
# Print where `cargo` is located for debugging purposes
echo "Cargo binary location: $(which cargo)"

# Install/update the stable toolchain explicitly (idempotent - rustup reports
# "unchanged" if already current). This also covers pre-existing rustup setups
# that do not yet have stable installed.
rustup update stable
# Ensure we have both clippy and rustfmt installed for the stable toolchain
# (rustup is also idempotent here - reports "up to date" if installed).
rustup component add --toolchain stable clippy rustfmt
