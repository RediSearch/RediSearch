# ---------------------------------------------------------
# Build argument to select the base image dynamically.
# Examples:
#   docker build --build-arg BASE_IMAGE=ubuntu:24.04 .
#   docker build --build-arg BASE_IMAGE=rockylinux:9 .
#   docker build --build-arg BASE_IMAGE=alpine:3 .
# ---------------------------------------------------------
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG SAN=none

ENV GITHUB_ACTIONS=true
WORKDIR /project

# Ensure bash is present. Not all images come with it.
RUN if ! command -v bash >/dev/null 2>&1; then \
        (apt-get update && apt-get install -y --no-install-recommends bash) || \
        (yum install -y bash) || \
        (apk add --no-cache bash); \
    fi

COPY . .

WORKDIR /project/.install
# Install base dependencies, Rust toolchain, and optionally LLVM for sanitizer builds.
RUN bash retry.sh bash -l -eo pipefail install_script.sh && \
    bash retry.sh bash -l -eo pipefail test_deps/install_rust_deps.sh && \
    if [ "$SAN" = "address" ]; then bash retry.sh bash -l -eo pipefail install_llvm.sh; fi
WORKDIR /project
# Expose newly-installed Rust and Python tools via PATH
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

WORKDIR /project
