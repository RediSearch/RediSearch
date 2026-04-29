---
name: add-ci-platform
description: Add a new OS platform to RediSearch CI. Use when adding a new distro version, OS, or container target to the build/test matrix.
---

# Add a New CI Platform

Add a new OS platform (distro version) to RediSearch's CI pipeline. Validate locally with Docker on the host architecture, then enable the other architecture in CI.

## Arguments

The target platform (matching CI naming), e.g. `/add-ci-platform alpine:3.23`.

Platform to add: `$ARGUMENTS`

## Key Files

- `.install/install_script.sh` — OS detection and script dispatch
- `.install/<platform>.sh` — per-platform package installation
- `.install/install_llvm.sh` — cross-distro LLVM/Clang installer (needed for LTO)
- `.install/LLVM_VERSION.sh` — pinned LLVM version
- `.github/workflows/task-get-config.yml` — platform-to-container mapping, LTO/setup config
- `.github/workflows/generate-matrix.yml` — all platform/arch combinations for tests
- `.github/workflows/flow-test.yml` — platform list for test workflow
- `.github/workflows/flow-build-artifacts.yml` — platform list for artifact builds
- `.github/workflows/event-merge-to-queue.yml` — merge-queue platform list
- `Dockerfile` — parameterized build, accepts `BASE_IMAGE` arg

## Instructions

### 1. Find a Template Platform

Find the closest existing platform in `task-get-config.yml` to use as a template.
For version bumps (e.g. `alpine:3.23` from `alpine:3.22`), copy the previous version's config verbatim as a starting point.

### 2. Determine the Install Script Name

The install script name is derived from `/etc/os-release` in the base container image.
Check what the target image reports:

```bash
docker run --rm <BASE_IMAGE> cat /etc/os-release
```

`install_script.sh` builds the filename: lowercase NAME + `_` + VERSION_ID, with spaces/slashes replaced by underscores. For example, `Alpine Linux` + `3.23` → `alpine_linux_3.sh` (Alpine uses major-only VERSION_ID).

If the existing install script already covers the new version (e.g. `alpine_linux_3.sh` covers all Alpine 3.x), no new script is needed — just verify it works.

### 3. Create or Update the Install Script

If a new `.install/<platform>.sh` is needed:
1. Copy the closest existing script
2. Adjust package names for the new version (check with the distro's package manager)
3. Add the license header

### 4. Detect Host Architecture

Run `uname -m` to determine the host architecture. This sets which arch is tested locally
(native Docker, fast) vs in CI only (emulated Docker is too slow).

| `uname -m`  | `<LOCAL_ARCH>` | `<LOCAL_PLATFORM>` | `<OTHER_ARCH>` |
|---|---|---|---|
| `arm64` / `aarch64` | `aarch64` | `linux/arm64` | `x86_64` |
| `x86_64` | `x86_64` | `linux/amd64` | `aarch64` |

Use `<LOCAL_ARCH>`, `<LOCAL_PLATFORM>`, and `<OTHER_ARCH>` in the steps below.

### 5. Validation Progression

Work through these 5 stages in order. Each stage must pass before moving to the next.

#### Stage 1: Local arch, no LTO — local Docker

```bash
docker build --platform <LOCAL_PLATFORM> --build-arg BASE_IMAGE=<BASE_IMAGE> -t add-<SLUG> .
docker run --rm --platform <LOCAL_PLATFORM> -v $(pwd):/redisearch -w /redisearch add-<SLUG> bash -l -c "./build.sh"
```

If either step fails, see [Troubleshooting](#troubleshooting) below.

#### Stage 2: Local arch, LTO — local Docker

Cross-language LTO requires clang, lld, and rustc to share the same LLVM major version
(pinned in `.install/LLVM_VERSION.sh`).

**Ensure the install script sources `install_llvm.sh`.** If the platform's `.install/<script>.sh`
doesn't already source it, add:

```bash
# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
```

Rebuild and test with LTO:

```bash
docker build --platform <LOCAL_PLATFORM> --build-arg BASE_IMAGE=<BASE_IMAGE> -t add-<SLUG> .
docker run --rm --platform <LOCAL_PLATFORM> -v $(pwd):/redisearch -w /redisearch add-<SLUG> bash -l -c "./build.sh LTO=1"
```

Watch for:
- **`clang-21: command not found`** — `install_llvm.sh` failed or didn't add to PATH
- **GLIBCXX symbol mismatch** — clang picking up wrong GCC headers (`build.sh` has a diagnostic)
- **Linker errors from lld** — undefined symbols, missing libraries

**Validate the binary** — every GLIBCXX/GLIBC version referenced must exist on the platform:

```bash
docker run --rm --platform <LOCAL_PLATFORM> -v $(pwd):/redisearch -w /redisearch add-<SLUG> bash -c '
  echo "=== Binary GLIBCXX ===" && nm -D bin/linux-*/search/redisearch.so | grep GLIBCXX | sort -t@ -k2 -V
  echo "=== Platform GLIBCXX ===" && strings /usr/lib/*/libstdc++.so.6 | grep GLIBCXX | sort -V
'
```

#### Stage 3: Update CI Config Files

Update these 4 files (use the template platform's entries as a guide):

- [ ] **`task-get-config.yml`** — Add platform entry under `platform_configs` with `container`, `setup_script`, `post_setup_script`, and `name` for both `x86_64` and `aarch64`. Set `enable_lto: '1'` for `<LOCAL_ARCH>` only (`<OTHER_ARCH>` LTO comes later in stage 5).
- [ ] **`generate-matrix.yml`** — Add `('<platform>', '<arch>')` tuples to the test matrix.
- [ ] **`flow-test.yml`** — Add platform to the `platform` list.
- [ ] **`flow-build-artifacts.yml`** — Add platform to the `platform` list.

Optionally update `event-merge-to-queue.yml` if this platform should run on merge-queue builds.

#### Stage 4: Local arch LTO + remote arch no-LTO — CI

Push and trigger CI. This validates `<LOCAL_ARCH>` with LTO (already proven locally) and `<OTHER_ARCH>` without LTO:

```bash
git push -u origin HEAD
gh workflow run flow-test.yml -r <BRANCH> -f platform=<PLATFORM> -f fail-fast=false
```

Then find the run ID and watch it in the background so you're notified when it completes:

```bash
gh run list --workflow=flow-test.yml --branch=<BRANCH> --limit 1 --json databaseId --jq '.[0].databaseId'
```

Use `gh run watch <RUN_ID> -i 60` with the Bash tool's `run_in_background: true` parameter. Continue with other work while CI runs — you'll be notified when it finishes.

If CI fails, see [Troubleshooting](#troubleshooting) below.

#### Stage 5: Remote arch LTO — CI

Once stage 4 passes, enable LTO for `<OTHER_ARCH>` in `task-get-config.yml` and push again:

```bash
git push
gh workflow run flow-test.yml -r <BRANCH> -f platform=<PLATFORM> -f fail-fast=false
```

Find the run ID and watch in the background as in stage 4:

```bash
gh run list --workflow=flow-test.yml --branch=<BRANCH> --limit 1 --json databaseId --jq '.[0].databaseId'
```

Use `gh run watch <RUN_ID> -i 60` with `run_in_background: true`.

#### Stage 6: Full regression — CI

Run the workflow for **all** platforms to verify nothing was broken by the new platform's config changes:

```bash
gh workflow run flow-test.yml -r <BRANCH> -f platform=all -f fail-fast=false
```

Find the run ID and watch in the background as in stage 4:

```bash
gh run list --workflow=flow-test.yml --branch=<BRANCH> --limit 1 --json databaseId --jq '.[0].databaseId'
```

Use `gh run watch <RUN_ID> -i 60` with `run_in_background: true`.

### 5. Troubleshooting

When any stage fails, debug using the steps below. For local Docker failures, fix and rebuild
immediately. For CI failures, reproduce locally on `<LOCAL_ARCH>` first, then fix and re-push.

#### Debug Commands

```bash
# Check how the OS identifies itself (this is what install_script.sh uses)
docker run --rm --platform <LOCAL_PLATFORM> <BASE_IMAGE> cat /etc/os-release

# Check package availability (Debian/Ubuntu)
docker run --rm --platform <LOCAL_PLATFORM> <BASE_IMAGE> bash -c "apt-get update -qq && apt-cache show <suspect-pkg>"
# RHEL-family
docker run --rm --platform <LOCAL_PLATFORM> <BASE_IMAGE> bash -c "dnf info <suspect-pkg>"
# Alpine
docker run --rm --platform <LOCAL_PLATFORM> <BASE_IMAGE> bash -c "apk info <suspect-pkg>"

# Interactive shell for deeper investigation
docker run --rm --platform <LOCAL_PLATFORM> -it <BASE_IMAGE> bash
```

#### Common Failure Points

**OS detection mismatch**: `install_script.sh` builds a filename from `/etc/os-release` NAME + VERSION_ID (lowercased, spaces/slashes replaced with underscores). If the base image reports differently than expected, it won't find the install script.

**Package renamed or removed**: Packages differ across distro versions. The `gcc:*` images only include `Components: main`. Verify with `apt-cache show <pkg>` or `dnf info <pkg>`.

**LLVM installation** (`.install/install_llvm.sh`): Uses `apt.llvm.org/llvm.sh` for Debian/Ubuntu, official tarballs for RHEL-family. The apt script needs `software-properties-common` on older Debian/Ubuntu but this package was removed in Debian 13+.

**Node20 compatibility**: Some older distros don't support node20 (GHA runner requirement). Check `node20_unsupported_platforms` in `task-get-config.yml`.

#### Fix and Rebuild

After fixing, rebuild with `--no-cache` to ensure a clean image:

```bash
docker build --no-cache --platform <LOCAL_PLATFORM> --build-arg BASE_IMAGE=<BASE_IMAGE> -t add-<SLUG> .
```

Verify no regressions by also rebuilding the previous version of the same distro:

```bash
docker build --no-cache --platform <LOCAL_PLATFORM> --build-arg BASE_IMAGE=<PREVIOUS_BASE_IMAGE> -t add-<PREVIOUS_SLUG> .
```

### 6. Clean Up Docker Images

```bash
docker rmi add-<SLUG> add-<PREVIOUS_SLUG> 2>/dev/null
```
