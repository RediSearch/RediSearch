# ---------------------------------------------------------
# Build argument to select the base image dynamically.
# Examples:
#   docker build --build-arg BASE_IMAGE=ubuntu:24.04 .
#   docker build --build-arg BASE_IMAGE=rockylinux:9 .
#   docker build --build-arg BASE_IMAGE=alpine:3 .
# ---------------------------------------------------------
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ENV GITHUB_ACTIONS=true
WORKDIR /project

# Ensure bash is present. Not all images come with it.
RUN if ! command -v bash >/dev/null 2>&1; then \
        (apt-get update && apt-get install -y --no-install-recommends bash) || \
        (yum install -y bash) || \
        (apk add --no-cache bash); \
    fi

COPY .install .install
COPY .rust-nightly .
COPY .python-version .
COPY rust-toolchain.toml .
COPY uv.lock .

WORKDIR /project/.install
# Install base dependencies, Rust toolchain, and optionally LLVM for sanitizer builds.
RUN bash retry.sh bash -l -eo pipefail install_script.sh && \
    bash retry.sh bash -l -eo pipefail test_deps/install_rust_deps.sh && \
    if [ "$SAN" = "address" ]; then bash retry.sh bash -l -eo pipefail install_llvm.sh; fi
WORKDIR /project
# Expose newly-installed Rust and Python tools via PATH
ENV PATH="/usr/local/llvm/bin:/root/.cargo/bin:/root/.local/bin:${PATH}"

COPY . .
RUN git init

RUN bash -l -eo pipefail -c '\
    ./build.sh LTO=1 TESTS=1 BUILD_INTEL_SVS_OPT=yes || true; \
    echo "=== Scanning for problematic symbols ==="; \
    BUILD_DIR=$(find bin -maxdepth 2 -name search-community -type d 2>/dev/null | head -1); \
    if [ -z "$BUILD_DIR" ]; then echo "ERROR: build dir not found"; exit 1; fi; \
    cd "$BUILD_DIR"; \
    for f in $(find . -name "*.a"); do \
        nm -C --print-file-name "$f" 2>/dev/null \
            | grep -E "ios_base_library_init|_M_replace_cold" || true; \
    done; \
    echo "=== Undefined symbols in redisearch.so ==="; \
    nm -uCD redisearch.so 2>/dev/null \
        | grep -E "ios_base_library_init|_M_replace_cold" || echo "(none)"; \
    '
