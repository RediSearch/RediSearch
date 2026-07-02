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
# install_llvm.sh unpacks tarballs into a versioned dir (/usr/local/llvm-<ver>);
# the unversioned symlink keeps the static ENV PATH below working. Image-scoped:
# this image builds only RediSearch, so the unversioned name can't mislead
# anything else.
RUN SKIP_BOOST=1 bash retry.sh bash -l -eo pipefail install_script.sh && \
    if [ "$SAN" = "address" ]; then \
        bash retry.sh bash -l -eo pipefail install_llvm.sh && \
        . ./LLVM_VERSION.sh && \
        if [ -d "/usr/local/llvm-${LLVM_FULL_VERSION}" ]; then ln -sfn "/usr/local/llvm-${LLVM_FULL_VERSION}" /usr/local/llvm; fi; \
    fi
# Mount the GitHub token as a build secret so cargo-binstall benefits from
# higher GitHub API rate limits when fetching prebuilt release artifacts.
RUN --mount=type=secret,id=GITHUB_TOKEN \
    if [ -f /run/secrets/GITHUB_TOKEN ]; then export GITHUB_TOKEN=$(cat /run/secrets/GITHUB_TOKEN); fi && \
    bash retry.sh bash -l -eo pipefail test_deps/install_rust_deps.sh
WORKDIR /project
# Expose newly-installed Rust and Python tools via PATH
ENV PATH="/usr/local/llvm/bin:/root/.cargo/bin:/root/.local/bin:${PATH}"

WORKDIR /project
