#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y gcc-toolset-14-gcc gcc-toolset-14-gcc-c++ make wget git --nobest --skip-broken --allowerasing

cp /opt/rh/gcc-toolset-14/enable /etc/profile.d/gcc-toolset-14.sh
# install other stuff after installing gcc-toolset-14 to avoid dependencies conflicts
$MODE dnf install -y openssl openssl-devel which rsync unzip curl clang  clang-devel gdb --nobest --skip-broken --allowerasing

# Need xz for extracting LLVM tarball, epel for patchelf (used by GLIBCXX compat fix)
$MODE dnf install -y xz epel-release --nobest --skip-broken --allowerasing
$MODE dnf install -y patchelf --nobest --skip-broken --allowerasing

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE

# ---------------------------------------------------------------------------
# Fix GLIBCXX compatibility for LLVM tarball binaries on Rocky 9.
#
# The official LLVM tarballs are built with GCC 12+ (GLIBCXX_3.4.30).
# Rocky 9's system libstdc++.so.6 only has up to GLIBCXX_3.4.29 (GCC 11).
# clang and lld need exactly one symbol from GLIBCXX_3.4.30:
#   std::condition_variable::wait(std::unique_lock<std::mutex>&)
# which moved from inline to exported in GCC 12.
#
# Fix: build a small compat shim providing the symbol, binary-patch the
# VERNEED entries to reference GLIBCXX_3.4.29, and use patchelf to wire
# the shim into the binaries' NEEDED list.
# ---------------------------------------------------------------------------
fix_glibcxx_compat() {
    local install_dir="${LLVM_INSTALL_DIR:-/usr/local/llvm}"
    local llvm_ver="${LLVM_VERSION}"
    local clang_bin="${install_dir}/bin/clang-${llvm_ver}"
    local lld_bin="${install_dir}/bin/lld"

    # Quick check: does clang already work?
    local clang_err
    if clang_err=$("${clang_bin}" --version 2>&1); then
        echo ">>> clang-${llvm_ver} runs fine, no GLIBCXX compat fix needed"
        return 0
    fi

    # Check if this is the GLIBCXX issue
    if ! echo "$clang_err" | grep -q "GLIBCXX_3.4.30"; then
        echo ">>> clang-${llvm_ver} fails for a reason other than GLIBCXX_3.4.30:"
        echo "$clang_err"
        return 1
    fi

    echo ">>> System libstdc++ lacks GLIBCXX_3.4.30 — applying compat fix"

    # 1. Build the compat shim
    local shim_src
    shim_src=$(mktemp /tmp/glibcxx_compat_XXXXXX.cpp)
    cat > "$shim_src" << 'SHIMEOF'
#include <mutex>
#include <pthread.h>
// Provide std::condition_variable::wait which moved from inline (GCC < 12)
// to exported (GCC 12+, GLIBCXX_3.4.30). The first member of
// std::condition_variable is a pthread_cond_t.
extern "C" {
    void _ZNSt18condition_variable4waitERSt11unique_lockISt5mutexE(
        void* cv, std::unique_lock<std::mutex>& lock) {
        pthread_cond_t* cond = static_cast<pthread_cond_t*>(cv);
        pthread_cond_wait(cond, lock.mutex()->native_handle());
    }
}
SHIMEOF
    g++ -shared -fPIC -o "${install_dir}/lib/libglibcxx_compat.so" "$shim_src" -lpthread
    rm -f "$shim_src"

    # 2. Binary-patch VERNEED: GLIBCXX_3.4.30 -> GLIBCXX_3.4.29
    #    Must patch both the version string and the ELF hash.
    python3 -c "
import struct, sys

def elf_hash(name):
    h = 0
    for c in name.encode():
        h = (h << 4) + c
        g = h & 0xf0000000
        if g:
            h ^= g >> 24
        h &= 0x0fffffff
    return h

old_str = b'GLIBCXX_3.4.30\x00'
new_str = b'GLIBCXX_3.4.29\x00'
old_hash = struct.pack('<I', elf_hash('GLIBCXX_3.4.30'))
new_hash = struct.pack('<I', elf_hash('GLIBCXX_3.4.29'))

for path in sys.argv[1:]:
    with open(path, 'rb') as f:
        data = f.read()
    if old_str not in data:
        continue
    data = data.replace(old_str, new_str)
    data = data.replace(old_hash, new_hash)
    with open(path, 'wb') as f:
        f.write(data)
    print(f'Patched VERNEED in {path}')
" "${clang_bin}" "${lld_bin}"

    # 3. Add the compat shim to each binary's NEEDED list and set RPATH
    patchelf --add-needed libglibcxx_compat.so --set-rpath "${install_dir}/lib" "${clang_bin}"
    patchelf --add-needed libglibcxx_compat.so --set-rpath "${install_dir}/lib" "${lld_bin}"

    # Verify
    if "${clang_bin}" --version &>/dev/null 2>&1; then
        echo ">>> GLIBCXX compat fix applied successfully"
    else
        echo "ERROR: clang-${llvm_ver} still fails after compat fix:"
        "${clang_bin}" --version 2>&1 || true
        return 1
    fi
}

fix_glibcxx_compat
