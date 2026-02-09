#!/usr/bin/env bash
set -e

# Set HOME if not set (needed for EC2 runners)
export HOME="${HOME:-/home/ubuntu}"
export DEBIAN_FRONTEND=noninteractive

# Install system dependencies
sudo apt-get update -qq
sudo apt-get install -yqq git wget build-essential lcov openssl libssl-dev \
    python3 python3-pip python3-venv python3-dev unzip rsync curl \
    liburing-dev cmake pipx

# Install Rust (required for RediSearch build)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
export PATH="$HOME/.cargo/bin:$PATH"

# Verify cargo is available
echo "Cargo binary location: $(which cargo)"
cargo --version

# Install Python tools
pipx install redisbench-admin
pipx ensurepath

