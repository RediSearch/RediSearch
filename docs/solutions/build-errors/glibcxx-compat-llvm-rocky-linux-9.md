---
title: "GLIBCXX_3.4.30 compatibility fix for LLVM 21 on Rocky Linux 9"
category: build-errors
date: 2026-03-22
severity: medium
component: CI build infrastructure
tags:
  - LTO
  - LLVM
  - Rocky Linux
  - GLIBCXX
  - ELF
  - binary-patching
  - CI
  - cross-language-optimization
symptoms:
  - "clang-21 --version fails with: /lib64/libstdc++.so.6: version 'GLIBCXX_3.4.30' not found"
  - "lld fails with same GLIBCXX_3.4.30 error"
  - "Docker image build succeeds but clang/lld are unusable"
root_cause: "LLVM 21 tarballs built with GCC 12+ require GLIBCXX_3.4.30, but Rocky 9 ships GCC 11 (max GLIBCXX_3.4.29)"
resolution_type: workaround
files_changed:
  - .install/rocky_linux_9.sh
  - .install/install_llvm.sh
  - .github/workflows/task-get-config.yml
---

## Problem

Enabling cross-language LTO (C/C++ ↔ Rust) on Rocky Linux 9 CI requires LLVM 21 (to match rustc's LLVM version). The official LLVM 21 tarballs are built with GCC 12+, which exports `std::condition_variable::wait(std::unique_lock<std::mutex>&)` under `GLIBCXX_3.4.30`. Rocky 9's system `libstdc++.so.6` (from GCC 11) only provides up to `GLIBCXX_3.4.29`, causing clang and lld to fail at startup.

## Root Cause Analysis

In GCC 12, `std::condition_variable::wait` moved from an inline function to an exported symbol under a new version tag (`GLIBCXX_3.4.30`). LLVM binaries built against GCC 12+ record a VERNEED entry demanding `GLIBCXX_3.4.30` from `libstdc++.so.6`. The ELF dynamic linker checks VERNEED entries **per-library** — it specifically looks for the version node in `libstdc++.so.6`, not in any loaded library.

Key technical detail: ELF version checking uses both a string match AND a hash comparison (`vna_hash` field computed by the ELF hash function). Both must match for version resolution to succeed.

Rocky Linux 8 was also evaluated but is **completely blocked** — its glibc 2.28 is too old for LLVM 21 tarballs (which require glibc 2.34).

## Investigation Steps (What Didn't Work)

### 1. LD_PRELOAD with shim library
Built a `.so` providing the missing symbol and used `LD_PRELOAD`.
**Why it failed**: ELF VERNEED checks are per-library. The binary says "I need `GLIBCXX_3.4.30` from `libstdc++.so.6`" — a preloaded library doesn't satisfy that check.

### 2. Fedora 41 libstdc++.so.6
Extracted from RPM as a drop-in replacement.
**Why it failed**: Built against glibc 2.38, but Rocky 9 only has glibc 2.36.

### 3. Fake libstdc++.so.6
Created a minimal `.so` with just GLIBCXX version nodes.
**Why it failed**: The binaries also need `CXXABI_1.3.*` versions from the same library.

### 4. Binary patching (string only, no hash)
Hex-patched the version string `GLIBCXX_3.4.30` → `GLIBCXX_3.4.29`.
**Why it failed**: The ELF `vna_hash` field still contained the hash for `"GLIBCXX_3.4.30"`, causing a hash mismatch during version lookup. Error changed to `version 'GLIBCXX_3.4.29' not found` because the hash didn't match any actual 3.4.29 entry.

### 5. set -eo pipefail interaction
`clang --version 2>&1 | grep -q "GLIBCXX_3.4.30"` was killed by `pipefail` before grep could process.
**Fix**: Capture output to a variable first: `clang_err=$("${clang_bin}" --version 2>&1)`.

## Working Solution

Three-part fix implemented in `.install/rocky_linux_9.sh` as `fix_glibcxx_compat()`:

### Part 1: Build compat shim
```cpp
#include <mutex>
#include <pthread.h>
extern "C" {
    void _ZNSt18condition_variable4waitERSt11unique_lockISt5mutexE(
        void* cv, std::unique_lock<std::mutex>& lock) {
        pthread_cond_t* cond = static_cast<pthread_cond_t*>(cv);
        pthread_cond_wait(cond, lock.mutex()->native_handle());
    }
}
```
```bash
g++ -shared -fPIC -o ${install_dir}/lib/libglibcxx_compat.so shim.cpp -lpthread
```

### Part 2: Binary-patch VERNEED entries
Patch **both** the version string AND the ELF hash in clang and lld binaries:
- `GLIBCXX_3.4.30\0` → `GLIBCXX_3.4.29\0`
- `elf_hash("GLIBCXX_3.4.30")` → `elf_hash("GLIBCXX_3.4.29")`

### Part 3: Wire shim into binaries
```bash
patchelf --add-needed libglibcxx_compat.so --set-rpath "${install_dir}/lib" "${clang_bin}"
patchelf --add-needed libglibcxx_compat.so --set-rpath "${install_dir}/lib" "${lld_bin}"
```

### Supporting changes
- `.install/install_llvm.sh`: Made verification step non-fatal (`|| echo "WARNING: ..."`) so the calling script can apply the fix after LLVM install returns.
- `.github/workflows/task-get-config.yml`: Added `'enable_lto': '1'` for Rocky 9 x86_64 and aarch64.
- Prerequisites: `xz` (for tarball extraction), `epel-release` + `patchelf` (for ELF manipulation).

## Key Insights

1. **ELF VERNEED is per-library**: You cannot satisfy a version requirement from one `.so` by loading a different `.so`. The check ties the version to a specific `NEEDED` library by name.
2. **ELF hash must match**: Patching version strings alone is insufficient — the `vna_hash` field must also be updated to match the new string.
3. **Only one symbol was needed**: The entire GLIBCXX_3.4.30 dependency was for a single symbol (`condition_variable::wait`), making a shim feasible.
4. **The fix is self-detecting**: `fix_glibcxx_compat()` checks if clang already works before applying any patches, making it safe to run on platforms that don't need it.

## Verification

- Docker image builds successfully
- `clang-21 --version` runs correctly
- LTO build completes (`./build.sh LTO=1`)
- Final binary (`redisearch.so`) only requires GLIBCXX_3.4.29 (within platform max)
- All 1371 unit tests pass

## Prevention & Maintenance

### When upgrading LLVM
- Check `readelf -V clang-XX | grep GLIBCXX` for new version requirements
- If a new GLIBCXX version appears, identify which symbols are needed (`nm -D --undefined-only`)
- The shim may need additional symbols if LLVM starts using more GCC 12+ exports

### When Rocky Linux updates GCC
- If Rocky 9 ships GCC 12+ (GLIBCXX_3.4.30), the fix becomes unnecessary — the self-detection will skip it automatically

### Useful diagnostic commands
```bash
# Check what GLIBCXX versions a binary needs
readelf -V binary | grep GLIBCXX

# Check what GLIBCXX versions a library provides
strings /usr/lib64/libstdc++.so.6 | grep GLIBCXX

# Check undefined symbols requiring a specific version
nm -D --undefined-only binary | grep GLIBCXX_3.4.30

# Verify binary compatibility
nm -D binary | grep GLIBCXX | sort -t@ -k2 -V | tail -5
```

## Related Files
- `.install/rocky_linux_9.sh` — Contains the fix
- `.install/install_llvm.sh` — Generic LLVM installer
- `.install/LLVM_VERSION.sh` — Pinned LLVM version (21.1.8)
- `.github/workflows/task-get-config.yml` — CI platform config
