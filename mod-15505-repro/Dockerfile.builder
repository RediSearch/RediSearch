# Pre-built builder image with all RediSearch build deps.
# Built once, reused across all tag builds to skip ~90s setup per build.
#
# Build:   docker build -f Dockerfile.builder -t mod15505-builder:latest .
# Use:     build-tag.sh handles invocation.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC \
    CARGO_HOME=/root/.cargo \
    RUSTUP_HOME=/root/.rustup \
    PATH=/root/.local/bin:/root/.cargo/bin:/usr/local/bin:/usr/bin:/bin

# Apt deps that cover all tag setup scripts (ubuntu_22.04.sh + cmake/boost/llvm).
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
    sudo lsb-release ca-certificates curl wget gnupg software-properties-common \
    git build-essential gcc-12 g++-12 make cmake ninja-build pkg-config \
    autoconf automake libtool \
    python3 python3-pip python3-dev python3-venv \
    libssl-dev zlib1g-dev libbz2-dev liblzma-dev libffi-dev libclang-dev \
    libstemmer0d libstemmer-dev \
    lcov unzip rsync gdb file && \
    rm -rf /var/lib/apt/lists/* && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 60 \
      --slave /usr/bin/g++ g++ /usr/bin/g++-12 && \
    update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-12 60

# LLVM 17 (tags read it via setup script; satisfy ahead of time).
RUN wget -qO- https://apt.llvm.org/llvm.sh | bash -s -- 17 && \
    update-alternatives --install /usr/bin/clang clang /usr/bin/clang-17 60 \
      --slave /usr/bin/clang++ clang++ /usr/bin/clang++-17

# Boost 1.88 headers (1.83+ satisfies install_boost.sh checks).
RUN curl -sSLo /tmp/boost.tar.gz \
      https://archives.boost.io/release/1.88.0/source/boost_1_88_0.tar.gz && \
    mkdir -p /usr/include && tar -xzf /tmp/boost.tar.gz -C /tmp && \
    cp -r /tmp/boost_1_88_0/boost /usr/include/ && \
    rm -rf /tmp/boost*

# Rust (stable + nightly). Newer tags read .rust-nightly file at build time;
# build-tag.sh installs the requested nightly on demand.
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable \
      --profile default && \
    rustup toolchain install nightly --profile default && \
    rustup component add clippy rustfmt --toolchain stable && \
    rustup default stable

# uv for python tooling that some newer tags use.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Trust any worktree mounted in, since macOS/Linux UIDs differ.
RUN git config --global --add safe.directory '*' && \
    git config --global user.email 'builder@local' && \
    git config --global user.name 'Builder'
