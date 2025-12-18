#!/usr/bin/env bash

# This script registers a specific version of Clang and LLVM tools using the `update-alternatives` command.
# It creates symlinks for the given version, allowing users to switch between multiple versions of Clang and LLVM.


# Function: register_clang_version
# Arguments:
#   1. version  - The version of Clang/LLVM to be registered (e.g., 18).
#   2. priority - The priority of the version. A higher priority number indicates a preferred version.
#
# Sets up a primary symlink and several slave symlinks that correspond to related tools for Clang and LLVM.
#
# `update-alternatives --install` creates or updates the alternative for the given tool.
# The first argument after `--install` is the path to the main symlink (e.g., /usr/bin/clang).
# The second argument is the name of the alternative (e.g., clang), and the third argument is the
# path to the actual binary for the specified version (e.g., /usr/bin/clang-18).
# The `--slave` options are used to link other related binaries (like clang-format, llvm-nm, etc.)
# so that all tools are switched consistently when the main tool (e.g., clang) is switched.

# Function to register alternatives as slave first and fall back to master if it fails
register_alternative() {
    local tool=$1
    local tool_with_version=$2
    local version=$3
    local priority=$4

    # Try registering as slave first
    update-alternatives --remove-all "${tool}"
    update-alternatives --verbose --install "/usr/bin/${tool}" "${tool}" "/usr/bin/${tool_with_version}" "${priority}"

    # Check if the previous command resulted in an error indicating that the tool is a master alternative
    #if [ $? -ne 0 ]; then
    #    echo "Error: Failed to set up ${tool} as an alternative."
#
    #    # Force reinstallation in case of broken symlink group
    #    echo "Forcing reinstallation of ${tool}."
    #    update-alternatives --remove "${tool}" "/usr/bin/${tool}-${version}"
    #    update-alternatives --install "/usr/bin/${tool}" "${tool}" "/usr/bin/${tool_with_version}" ${priority}
    #fi
}

# Function to register Clang tools
register_clang_version() {
    local version=$1
    local priority=$2

    # List of all Clang and LLVM tools and their binary names
    declare -a tools=(
        # Clang tools
        "clang" "clang-${version}"
        "clang-cpp" "clang-cpp-${version}"
        "clang-cl" "clang-cl-${version}"
        "clangd" "clangd-${version}"
        "clang-check" "clang-check-${version}"
        "clang-query" "clang-query-${version}"
        "asan_symbolize" "asan_symbolize-${version}"
        "bugpoint" "bugpoint-${version}"
        "dsymutil" "dsymutil-${version}"
        "lld" "lld-${version}"
        "ld.lld" "ld.lld-${version}"
        "lld-link" "lld-link-${version}"
        "llc" "llc-${version}"
        "lli" "lli-${version}"
        "opt" "opt-${version}"
        "sanstats" "sanstats-${version}"
        "verify-uselistorder" "verify-uselistorder-${version}"
        "wasm-ld" "wasm-ld-${version}"
        "yaml2obj" "yaml2obj-${version}"
        "clang++" "clang++-${version}"
        "clang-tidy" "clang-tidy-${version}"
        "clang-format" "clang-format-${version}"

        # LLVM tools
        "llvm-config" "llvm-config-${version}"
        "llvm-ar" "llvm-ar-${version}"
        "llvm-as" "llvm-as-${version}"
        "llvm-bcanalyzer" "llvm-bcanalyzer-${version}"
        "llvm-c-test" "llvm-c-test-${version}"
        "llvm-cat" "llvm-cat-${version}"
        "llvm-cfi-verify" "llvm-cfi-verify-${version}"
        "llvm-cov" "llvm-cov-${version}"
        "llvm-cvtres" "llvm-cvtres-${version}"
        "llvm-cxxdump" "llvm-cxxdump-${version}"
        "llvm-cxxfilt" "llvm-cxxfilt-${version}"
        "llvm-diff" "llvm-diff-${version}"
        "llvm-dis" "llvm-dis-${version}"
        "llvm-dlltool" "llvm-dlltool-${version}"
        "llvm-dwarfdump" "llvm-dwarfdump-${version}"
        "llvm-dwp" "llvm-dwp-${version}"
        "llvm-exegesis" "llvm-exegesis-${version}"
        "llvm-extract" "llvm-extract-${version}"
        "llvm-lib" "llvm-lib-${version}"
        "llvm-link" "llvm-link-${version}"
        "llvm-lto" "llvm-lto-${version}"
        "llvm-lto2" "llvm-lto2-${version}"
        "llvm-mc" "llvm-mc-${version}"
        "llvm-mca" "llvm-mca-${version}"
        "llvm-modextract" "llvm-modextract-${version}"
        "llvm-mt" "llvm-mt-${version}"
        "llvm-nm" "llvm-nm-${version}"
        "llvm-objcopy" "llvm-objcopy-${version}"
        "llvm-objdump" "llvm-objdump-${version}"
        "llvm-opt-report" "llvm-opt-report-${version}"
        "llvm-pdbutil" "llvm-pdbutil-${version}"
        "llvm-PerfectShuffle" "llvm-PerfectShuffle-${version}"
        "llvm-profdata" "llvm-profdata-${version}"
        "llvm-ranlib" "llvm-ranlib-${version}"
        "llvm-rc" "llvm-rc-${version}"
        "llvm-readelf" "llvm-readelf-${version}"
        "llvm-readobj" "llvm-readobj-${version}"
        "llvm-rtdyld" "llvm-rtdyld-${version}"
        "llvm-size" "llvm-size-${version}"
        "llvm-split" "llvm-split-${version}"
        "llvm-stress" "llvm-stress-${version}"
        "llvm-strings" "llvm-strings-${version}"
        "llvm-strip" "llvm-strip-${version}"
        "llvm-symbolizer" "llvm-symbolizer-${version}"
        "llvm-tblgen" "llvm-tblgen-${version}"
        "llvm-undname" "llvm-undname-${version}"
        "llvm-xray" "llvm-xray-${version}"
    )

    # Loop through the tools list and register them
    for ((i=0; i<${#tools[@]}; i+=2)); do
        tool="${tools[$i]}"
        tool_bin="${tools[$i+1]}"
        register_alternative "$tool" "$tool_bin" "$version" "$priority"
    done
}

# Call the function to register clang version (replace with actual version and priority)
register_clang_version $1 $2
