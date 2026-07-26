#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

# apk index refresh — real-only (not a dep; `apk add --no-cache` works without
# it, so list/dry-run stay clean). Runs on a real bootstrap.
if [ "${CHECK_DEPS:-0}" != 1 ] && [ "${DRY_RUN:-0}" != 1 ]; then $MODE apk update; fi

apk_install build-base gcc g++ make linux-headers openblas-dev \
    xsimd curl wget git openssl openssl-dev \
    tar xz which rsync bsd-compat-headers clang curl \
    clang-static ncurses-dev llvm-dev compiler-rt bash

# We must install Python via the package manager until
# `uv` starts providing aarch64-musl builds.
# See https://github.com/astral-sh/python-build-standalone/pull/569
if [ "$(uname -m)" = "aarch64" ]; then
    apk_install python3 python3-dev py3-pip
    # Needed before checkout
    apk_install gcompat libstdc++ libgcc
else
    # On x86_64, we need Python headers to build psutil@5.x.y from
    # source, since it only started providing wheels for musl
    # in version 6.w.z.
    apk_install python3-dev
fi

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE

# Static LLVM/clang libraries for bindgen-static mode (redis-module musl target
# in document_metadata uses bindgen-static, which links clang-sys statically).
# compiler-rt: the python test venv builds its sdist-only deps with clang
# (see test_deps/install_python_deps.sh), whose baked-in --rtlib=compiler-rt
# needs the runtime present.
# LLVM_VER is derived from what install_llvm.sh landed; in list/dry-run nothing
# was installed so it's empty — skip the version-specific static libs then.
LLVM_VER=$(ls /usr/lib/ 2>/dev/null | grep -oE 'llvm[0-9]+' | sort -V | tail -1 | tr -d 'llvm' || true)
if [ -n "$LLVM_VER" ]; then
    apk_install llvm${LLVM_VER}-static ncurses-static zlib-static zstd-static compiler-rt

    # Alpine ships component .a files but no combined libLLVM-<ver>.a.
    # clang-sys emits cargo:rustc-link-lib=LLVM-<ver> which the linker resolves to
    # libLLVM-<ver>.a. GNU ar's thin-archive mode (`ar rcT`) flattens the nested
    # component archives into member headers ld.lld cannot parse ("could not get
    # the buffer for a child of the archive"), so provide a GROUP() linker script
    # instead — both ld.lld and GNU ld accept a text script in place of an
    # archive, and it costs no disk space.
    if [ ! -e /usr/lib/llvm${LLVM_VER}/lib/libLLVM-${LLVM_VER}.a ]; then
        # Expand the glob before tee creates the output file, so the combined
        # archive never lists itself.
        # shellcheck disable=SC2086
        LLVM_COMPONENT_LIBS=$(echo /usr/lib/llvm${LLVM_VER}/lib/libLLVM*.a /usr/lib/libzstd.a)
        _sh "echo \"GROUP( ${LLVM_COMPONENT_LIBS} )\" | $MODE tee /usr/lib/llvm${LLVM_VER}/lib/libLLVM-${LLVM_VER}.a > /dev/null"
    fi
fi
