---
name: enable-lto
description: Enable cross-language LTO (C/Rust) on a new CI platform. Use when adding LTO support to a platform that doesn't have it yet.
---

# Enable LTO for a Platform

Enable cross-language LTO (Link-Time Optimization) between C/C++ and Rust on a new CI platform.
All verification is done locally in Docker — no CI runs needed.

## Arguments

The target platform (matching CI naming), e.g. `/enable-lto rockylinux:9`.

Platform to enable LTO on: `$ARGUMENTS`

## Background

Cross-language LTO requires clang, lld, and rustc to share the same LLVM major version.
The pinned version is in `.install/LLVM_VERSION.sh`. Currently LLVM 21.

Key files:
- `build.sh` — LTO setup logic (clang/lld detection, GCC header pinning, RUSTFLAGS)
- `.install/install_llvm.sh` — cross-distro LLVM installer
- `.install/LLVM_VERSION.sh` — pinned LLVM version
- `.github/workflows/task-get-config.yml` — per-platform CI config (contains `enable_lto` flag)
- `docs/lto-ci-investigation.md` — detailed investigation notes

## Instructions

### 1. Identify the Container Image

Look up `$ARGUMENTS` in `.github/workflows/task-get-config.yml` to find:
- The Docker `container` image (e.g. `gcc:12-bookworm`, `rockylinux:9`)

Use this value as `<CONTAINER_IMAGE>` in the commands below.

Derive `<PLATFORM_SLUG>` from the platform name by replacing colons with dashes
(e.g. `rockylinux:9` → `rockylinux-9`). This is used for unique Docker image/container names
so multiple platforms can be tested in parallel.

### 2. Build the Docker Image

The repo has a `Dockerfile` that takes a `BASE_IMAGE` build arg. It copies the repo into the
image and runs the platform's install script, Rust toolchain setup, and all other dependencies.

```bash
docker build --build-arg BASE_IMAGE=<CONTAINER_IMAGE> -t lto-<PLATFORM_SLUG> .
```

This runs:
- `.install/install_script.sh` — detects the distro and sources the correct per-platform script
  (e.g. `.install/rocky_linux_9.sh`), then installs cmake, boost, Rust, and Python
- `.install/test_deps/install_rust_deps.sh` — Rust test tooling (cargo-nextest, etc.)

The Dockerfile does **not** run `install_llvm.sh` by default (only for sanitizer builds).
If the platform's install script already sources `install_llvm.sh`, clang will be available.
If not, you'll add it in step 4.

**If the build fails**, the platform's install script likely needs fixes before you can proceed.
Debug interactively:

```bash
docker run --rm -it <CONTAINER_IMAGE> bash
```

### 3. Start a Persistent Container

Create a named container from the built image with the repo mounted (so host edits are
reflected immediately). All subsequent steps run inside it via `docker exec`.

```bash
docker run -d --name lto-<PLATFORM_SLUG> \
  -v $(pwd):/redisearch -w /redisearch \
  lto-<PLATFORM_SLUG> sleep infinity
```

For aarch64 testing on an x86_64 host, add `--platform linux/arm64` to both the `docker build`
and `docker run` commands.

### 4. Recon — Probe the Platform

Gather toolchain info:

```bash
# System GCC version (clang will link against its libstdc++)
docker exec lto-<PLATFORM_SLUG> gcc --version

# glibc version (affects SVS symbol compatibility on x86_64)
docker exec lto-<PLATFORM_SLUG> ldd --version

# Rust's LLVM version (must match clang)
docker exec lto-<PLATFORM_SLUG> bash -c 'rustc --version --verbose | grep LLVM'

# Verify clang was installed
docker exec lto-<PLATFORM_SLUG> bash -c 'clang-21 --version 2>/dev/null || clang --version'

# Check GCC C++ header search paths (what clang will use)
docker exec lto-<PLATFORM_SLUG> bash -c "g++ -E -x c++ -v /dev/null 2>&1 | sed -n '/#include <\.\.\.>/,/^End/{ /^ /p }'"
```

Record:
- **glibc version** — affects symbol compatibility of prebuilt SVS on x86_64
- **GCC version** — clang will use this GCC's libstdc++ headers
- **Whether clang is present** — if not, the install script doesn't source `install_llvm.sh` yet

### 5. Update the Install Script

This step is done on the **host** (editing files in the repo).

If the platform's `.install/<script>.sh` doesn't already source `install_llvm.sh`, add:

```bash
# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
```

For non-Debian/Ubuntu distros, `install_llvm.sh` falls back to the official LLVM tarball.
If this doesn't work for the target platform, add a case to `install_llvm.sh`.

After editing, rebuild the image and recreate the container to pick up the changes:

```bash
docker rm -f lto-<PLATFORM_SLUG>
docker build --build-arg BASE_IMAGE=<CONTAINER_IMAGE> -t lto-<PLATFORM_SLUG> .
docker run -d --name lto-<PLATFORM_SLUG> \
  -v $(pwd):/redisearch -w /redisearch \
  lto-<PLATFORM_SLUG> sleep infinity
```

Verify clang is now available:

```bash
docker exec lto-<PLATFORM_SLUG> bash -c 'clang-21 --version 2>/dev/null || clang --version'
```

### 6. Build with LTO

```bash
docker exec lto-<PLATFORM_SLUG> bash -c './build.sh LTO=1'
```

On x86_64, if the build fails with GLIBCXX or glibc symbol errors from SVS, retry with:

```bash
docker exec lto-<PLATFORM_SLUG> bash -c './build.sh LTO=1 BUILD_INTEL_SVS_OPT=0'
```

Watch for:
- **GLIBCXX symbol mismatch** — clang picking up wrong GCC headers. `build.sh` has a diagnostic
  that catches this. Fix by ensuring `--gcc-install-dir` and `-nostdinc++`/`-isystem` point to
  system GCC.
- **Linker errors from lld** — undefined symbols, missing libraries. Often caused by incompatible
  prebuilt dependencies.
- **`install_llvm.sh` failures** — the distro's package manager may not have the required LLVM
  version.

### 7. Validate the Binary

Verify `redisearch.so` doesn't reference symbols unavailable on the platform:

```bash
# GLIBCXX symbols in the binary
docker exec lto-<PLATFORM_SLUG> bash -c 'nm -D bin/linux-*/search/redisearch.so | grep GLIBCXX | sort -t@ -k2 -V'

# GLIBCXX symbols available on the platform
docker exec lto-<PLATFORM_SLUG> bash -c 'strings /usr/lib/*/libstdc++.so.6 | grep GLIBCXX | sort -V'

# glibc symbols in the binary (x86_64 SVS concern)
docker exec lto-<PLATFORM_SLUG> bash -c 'nm -D bin/linux-*/search/redisearch.so | grep GLIBC_ | sort -t@ -k2 -V'

# glibc symbols available on the platform
docker exec lto-<PLATFORM_SLUG> bash -c 'strings /lib/*/libc.so.6 | grep GLIBC_ | sort -V'
```

Every GLIBCXX/GLIBC version referenced by the binary must exist in the platform's runtime
libraries. If not, the binary will fail to load on that platform.

### 8. Run Tests

```bash
# Unit tests
docker exec lto-<PLATFORM_SLUG> bash -c './build.sh LTO=1 RUN_UNIT_TESTS'

# Python integration tests
docker exec lto-<PLATFORM_SLUG> bash -c './build.sh LTO=1 RUN_PYTEST'
```

If x86_64 needed `BUILD_INTEL_SVS_OPT=0` for the build, add it to the test commands too.

### 9. Clean Up the Container

**Important:** Always stop and remove running Docker containers when finished. Do not leave them running.

```bash
docker rm -f lto-<PLATFORM_SLUG>
```

### 10. Test Both Architectures

Repeat steps 2-9 for both x86_64 and aarch64 if the platform supports both.
On aarch64 there is no prebuilt SVS library, so SVS-related issues don't apply.

### 11. Update CI Config

This step is done on the **host** (editing files in the repo).

Once local Docker testing passes for all architectures, flip the flag in
`.github/workflows/task-get-config.yml`.

Find the `$ARGUMENTS` entry in `platform_configs` and add or update:

```python
'enable_lto': '1'
```

If the platform requires `BUILD_INTEL_SVS_OPT=0` on x86_64, note this as a comment or handle it
in `build.sh` (check the existing pattern for how other platforms handle this).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `GLIBCXX_3.4.3x not found` | Clang using newer GCC headers than system GCC | Fix GCC header pinning in `build.sh` — check `-nostdinc++` and `-isystem` flags |
| `undefined symbol: __memcmpeq@GLIBC_2.35` | Prebuilt SVS linked against newer glibc | Use `BUILD_INTEL_SVS_OPT=0` to build SVS from source |
| `clang-21: command not found` | `install_llvm.sh` failed or didn't add to PATH | Check `install_llvm.sh` output, verify PATH includes `/usr/local/llvm/bin` |
| `LLVM version mismatch` | Clang and rustc using different LLVM versions | Ensure `.install/LLVM_VERSION.sh` matches `rustc --version --verbose` |
| `lld: error: undefined symbol` | Missing runtime library at link time | Check that lld can find libstdc++, libgcc, and glibc |
