#!/usr/bin/env bash

# LLVM version used for building RediSearch
# This must match the LLVM version used by Rust for LTO to work
# Check with: rustc --version --verbose | grep "LLVM version"
LLVM_VERSION=21
