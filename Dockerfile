# ---------------------------------------------------------
# Build argument to select the base image dynamically.
# Examples:
#   docker build --build-arg BASE_IMAGE=ubuntu:24.04 .
#   docker build --build-arg BASE_IMAGE=rockylinux:9 .
#   docker build --build-arg BASE_IMAGE=alpine:3 .
# ---------------------------------------------------------
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ENV HOME=/root
WORKDIR /project

# Ensure bash is present. Not all images come with it.
RUN if ! command -v bash >/dev/null 2>&1; then \
        (apt-get update && apt-get install -y bash) || \
        (yum install -y bash) || \
        (apk add --no-cache bash); \
    fi

COPY . .

WORKDIR /project/.install
RUN bash -l -eo pipefail install_script.sh
WORKDIR /project
RUN bash -l -eo pipefail .install/test_deps/install_rust_deps.sh
# Expose newly-installed Rust and Python tools via PATH
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

WORKDIR /project
