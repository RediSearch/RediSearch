# LTO CI Investigation — Summary

## Background: Key Concepts

### The C/C++ Toolchain on Linux

**GCC** (GNU Compiler Collection) — the default C/C++ compiler on most Linux distros. Each GCC version ships with two runtime libraries:

- **libstdc++** — the C++ standard library. Provides high-level C++ functionality: containers (`std::vector`, `std::string`), streams (`std::cout`), exceptions, etc. This is what your C++ code links against when it uses `#include <string>` or `#include <iostream>`. Each GCC major version adds new symbols to libstdc++, and code compiled against newer headers may reference symbols that don't exist in older versions.

- **libgcc** — low-level compiler runtime support. Provides helper functions that the compiler itself needs: 64-bit arithmetic on 32-bit platforms, stack unwinding for exceptions, thread-local storage support, etc. Your code doesn't call these directly — the compiler inserts calls to them automatically. Much more stable across versions than libstdc++.

The critical difference: **libstdc++ is a C++ API that changes with each GCC release** (new classes, new methods on existing classes), while **libgcc is a compiler-internal ABI that rarely changes**.

**glibc** — the GNU C Library. The foundational C runtime on Linux. Provides core functions like `malloc`, `printf`, `pthread_create`, file I/O, etc. Every program on the system links against it. Each distro ships a specific version, and **newer glibc versions add symbols that don't exist on older distros**.

### Runtime Library Map

```
┌─────────────────────────────────────────────────────────┐
│                    YOUR C/C++/RUST CODE                  │
│                                                         │
│  C code ──────► compiled by clang (LLVM)                │
│  C++ code ────► compiled by clang (LLVM)                │
│  Rust code ───► compiled by rustc (LLVM)                │
└──────────┬──────────────┬───────────────┬───────────────┘
           │              │               │
           ▼              ▼               ▼
┌──────────────┐ ┌────────────────┐ ┌──────────────┐
│   libstdc++  │ │    libgcc(s)   │ │    glibc     │
│              │ │                │ │              │
│ C++ standard │ │ Low-level      │ │ C standard   │
│ library      │ │ compiler       │ │ library      │
│              │ │ runtime        │ │              │
│ std::string  │ │ Stack unwind   │ │ malloc/free  │
│ std::vector  │ │ Exception      │ │ printf       │
│ std::cout    │ │ handling       │ │ pthreads     │
│ exceptions   │ │ 64-bit math    │ │ file I/O     │
│              │ │ helpers        │ │ sockets      │
├──────────────┤ ├────────────────┤ ├──────────────┤
│ From: GCC    │ │ From: GCC      │ │ From: GNU    │
│ Versioned:   │ │ Versioned:     │ │   (separate) │
│   per GCC    │ │   per GCC      │ │ Versioned:   │
│   release    │ │   (but stable) │ │   per distro │
│ ABI: changes │ │ ABI: stable    │ │ ABI: changes │
│   often      │ │                │ │   sometimes  │
└──────┬───────┘ └────────────────┘ └──────────────┘
       │                                    ▲
       │         libstdc++ depends on       │
       └────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ LLVM provides alternatives (not used by us on Linux):   │
│   libc++      — alternative to libstdc++                │
│   compiler-rt — alternative to libgcc                   │
│   libunwind   — alternative to libgcc_s stack unwinding │
└─────────────────────────────────────────────────────────┘
```

### The LLVM Toolchain

**Clang** — an alternative C/C++ compiler built on the LLVM framework. On Linux, clang does **not** ship its own C++ standard library — it uses the system's **libstdc++** from GCC. This means clang-compiled code must be compatible with the GCC version whose libstdc++ it links against.

**lld** — LLVM's linker. Faster than GNU ld and required for cross-language LTO. Stricter about undefined symbols by default.

**Rust/rustc** — the Rust compiler, also built on LLVM. This is what makes cross-language LTO between C/C++ and Rust possible — both emit LLVM bitcode.

### LTO (Link-Time Optimization)

Normally, the compiler optimizes each source file independently, then the linker just glues the resulting machine code together. With **LTO**, the compiler emits intermediate code (LLVM bitcode) instead, and the **linker** performs whole-program optimization across all compilation units — inlining across files, eliminating dead code globally, etc.

**Cross-language LTO** extends this across C/C++ and Rust. Since both clang and rustc use LLVM, their bitcode is compatible (when LLVM versions match), allowing the linker to optimize across language boundaries.

Requirements: **clang**, **lld**, and **rustc** must all use the **same LLVM major version**.

## Our LTO Setup

The `build.sh` script configures cross-language LTO by:

1. Detecting rustc's LLVM version (currently 21)
2. Finding matching `clang-21` and `lld-21`
3. Pinning clang to the system GCC's headers and libraries via `--gcc-install-dir`
4. Setting `RUSTFLAGS` for Rust to emit LLVM bitcode and use clang as the linker

## The Prebuilt SVS Problem (x86_64 only)

On x86_64, when `BUILD_INTEL_SVS_OPT=1` (the default), the build downloads a **prebuilt** Intel SVS (Scalable Vector Search) library. This prebuilt binary was compiled with clang 20+ against a newer toolchain, so it references symbols from **GCC 12+ libstdc++** and **glibc 2.32+**. When this library gets linked into `redisearch.so`, those symbol references propagate.

On older platforms, those symbols simply don't exist:

| Platform | glibc | libstdc++ (GCC) | Has required symbols? |
|---|---|---|---|
| ubuntu:focal | 2.31 | 9/10 | No |
| debian:bullseye | 2.31 | 10/11 | No |
| debian:bookworm | 2.36 | 12 | Possibly |
| ubuntu:jammy | 2.35 | 11/12 | Possibly |
| ubuntu:noble | 2.39 | 13/14 | Yes |

**This problem only affects x86_64** — on aarch64 there is no prebuilt SVS library, so it's built from source with our toolchain and everything is compatible.

Setting `BUILD_INTEL_SVS_OPT=0` forces SVS to be built from source, respecting our compiler flags and producing compatible binaries.

## Current LTO Status

| Platform | aarch64 | x86_64 |
|---|---|---|
| ubuntu:noble | LTO works | LTO works |
| ubuntu:jammy | LTO works | LTO works (with `BUILD_INTEL_SVS_OPT=0`) |
| ubuntu:focal | LTO works | LTO works (with `BUILD_INTEL_SVS_OPT=0`) |
| debian:bookworm | LTO works | LTO works (with `BUILD_INTEL_SVS_OPT=0`) |
| debian:bullseye | LTO works | LTO works (with `BUILD_INTEL_SVS_OPT=0`) |
