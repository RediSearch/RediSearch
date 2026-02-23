# MOD-13766: Release Flow for RediSearchDisk

## Overview

- **RediSearchDisk:** Private Enterprise releases (Flex) → `s3://redismodules/redisearch/`
- As of 8.6, RediSearchDisk has its own release flow independent of RediSearch OSS

## Required Platforms (Enterprise 8.6)

| Platform | x86_64 | aarch64 |
|----------|--------|---------|
| ubuntu:noble | ✅ | ✅ |
| ubuntu:jammy | ✅ | ✅ |
| ubuntu:focal | ✅ | ✅ |
| rockylinux:8 | ✅ | ✅ |
| rockylinux:9 | ✅ | ✅ |
| amazonlinux:2 | ✅ | ❌ |
| mariner:2 | ✅ | ❌ |
| azurelinux:3 | ✅ | ✅ |

**Total: 14 build configurations**

## Implementation

### Task 1: Update `generate-matrix.yml` ✅ DONE
Enable all 14 platform configurations listed above.

### Task 2: Modify `task-test.yml` to accept platform/architecture inputs ✅ DONE
Current `task-test.yml` is hardcoded for Ubuntu. Modified to:
- Add `platform` and `architecture` inputs with **default values** (`ubuntu:noble`, `x86_64`)
  - This ensures backward compatibility with `event-pull-request.yml` which calls `task-test.yml` without inputs
- Use `task-get-config.yml` to get container/runner configuration
- Handle container vs non-container execution (like RediSearch's `task-test.yml`)
- Handle different package managers per platform:
  - Ubuntu/Debian: `apt`
  - Rocky/Amazon 2023/Azure: `dnf`
  - Amazon 2: `yum`
  - Mariner: `tdnf`
  - Alpine: `apk`
- Keep Redis with SpeedB build logic (required for integration tests)
- Handle `liburing-dev` equivalent per platform (for SpeedB async API)
- Updated cache key to include platform/architecture for binary compatibility
- Updated artifact name to include platform/architecture

### Task 3: Create `flow-test.yml` ✅ DONE
Created a new workflow similar to RediSearch's `flow-test.yml`:
- Accept `platform` and `architecture` inputs (default: `all`)
- Call `generate-matrix.yml` to get platform matrix
- Call `task-test.yml` for each platform in the matrix
- Add `workflow_dispatch` trigger for manual testing
- Simpler than RediSearch (no coverage, sanitize, coordinator options needed)

### Task 4: Update `event-merge-to-queue.yml` ✅ DONE
Replace single `task-test.yml` call with `flow-test.yml`:
```yaml
jobs:
  test-all-platforms:
    uses: ./.github/workflows/flow-test.yml
    secrets: inherit
    with:
      platform: all
      architecture: all
```

**CI strategy (like RediSearch):**
- **PR (feature branch):** Quick tests on single platform (fast feedback)
- **Merge queue (before merging to main/version):** Full tests on all 14 platforms

The merge queue is a GitHub feature that runs checks *after* PR approval but *before* actual merge.
This ensures main/version branches always have passing tests on all platforms.

### Task 5: Create `event-release.yml` ✅ DONE
Port from RediSearch (`deps/RediSearch/.github/workflows/event-release.yml`) with:

| Aspect | RediSearch | RediSearchDisk |
|--------|------------|----------------|
| Version file | `src/version.h` | `src/version.h` |
| OSS build | Yes | No |
| Version bump PR | Yes | No |

**Release flow:**
1. Trigger on version tag (`v1.2.3`) or `workflow_dispatch`
2. Validate tag matches `src/version.h`
3. Find snapshots matching git SHA
4. Copy from `snapshots/` to release directory

### Task 6: Fix Amazon Linux 2 Support ✅ DONE

**Problem:** `actions/create-github-app-token@v1` requires Node.js 20, unavailable on amazonlinux:2.

**Solution:** Created `.github/actions/checkout-with-token/action.yml` composite action that:
- Detects node20 support based on container name
- Uses standard actions when node20 available
- Falls back to bash+openssl for token generation on amazonlinux:2
- Outputs the token for downstream steps

**Files modified:**
- `.github/actions/checkout-with-token/action.yml` - NEW composite action
- `task-get-config.yml` - added `jq openssl curl` to amazonlinux:2 setup_script
- `task-test.yml` - uses composite action
- `task-build-artifacts.yml` - uses composite action

## Validation

### Test Setup

Since workflows only trigger on `main` or version branches, we test using a fake `4.8` branch:
- Created `test-4.8` branch in RediSearch with version 4.8.0
- Created `4.8` branch in RediSearchDisk pointing to that submodule commit

### Validation Plan

| Workflow | How to Trigger |
|----------|----------------|
| `flow-test.yml` | Push to `4.8` branch |
| `flow-build-artifacts.yml` | Called by `event-deploy-snapshots.yml` |
| `event-deploy-snapshots.yml` | Push to `4.8` branch |
| `event-release.yml` | Manual `workflow_dispatch` or tag push |
| Beta upload to `main` | After merge to `main`, verify artifacts in both `snapshots/` and `beta/` directories |

### Cleanup (after validation complete)

```bash
# RediSearchDisk
git push origin --delete v4.8.0
git push origin --delete 4.8

# RediSearch
cd deps/RediSearch && git push origin --delete test-4.8
```

**Important:** Remove the temporary `push` trigger from `flow-test.yml` before merging to `main`.

## Key Decisions

### Independent Versioning

RediSearchDisk has its own `src/version.h` as single source of truth.

**Why independent:**
- As of 8.6, RediSearch Enterprise (including disk-related content) releases independently from RediSearch OSS
- RediSearch OSS development continues on `master`; we don't backport every Enterprise change to OSS version branches
- When ready for Enterprise 8.6 release, we branch from OSS `master` (similar to the private repo used for CVE fixes)
- Allows different release cadence and versioning without coordinating with OSS releases

**Release process:**
1. Update `src/version.h` with release version (e.g., `8.6.0`)
2. Push tag `v8.6.0` → release workflow validates and promotes artifacts

### Other Decisions

- **S3 collision:** Prevented by `VERSION_SUFFIX` (timestamp + git SHA, e.g., `.20260211.140538.4eed64d`)
- **Redis Enterprise integration:** Not our concern - just upload to S3
