# Contributing to RediSearch

Thank you for your interest in contributing to RediSearch. This guide explains how to set up the project, follow the repository conventions, submit a pull request, and work through review.

By contributing code to this Redis project in any form, you agree to the Redis Software Grant and Contributor License Agreement in the [Legal](#legal) section below. Contributions are subject to the Redis tri-license under RSALv2, SSPLv1, or AGPLv3 as described in `LICENSE.txt`.

## Before You Start

- Use GitHub issues for bugs and detailed feature requests.
- Ask general usage questions on the [Redis Discord](https://discord.com/invite/redis) or [Stack Overflow](https://stackoverflow.com/questions/tagged/redis).
- Report security bugs and vulnerabilities through `SECURITY.md`.
- For major features or semantic changes, open an issue and discuss the design before investing heavily in code.
- Documentation-only changes for redis.io documentation usually belong in the [redis-doc](https://github.com/redis/redis-doc) repository.

## Set Up Locally

1. Fork the repository on GitHub.
2. Clone your fork with submodules:

   ```bash
   git clone --recursive git@github.com:<your-user>/RediSearch.git
   cd RediSearch
   ```

   If you cloned without submodules, initialize them later:

   ```bash
   git submodule update --init --recursive
   ```

3. Install platform dependencies. Use the scripts in `.install/`; see `.install/README.md` for platform-specific details. For example:

   ```bash
   cd .install
   ./install_script.sh sudo
   cd ..
   ```

4. Build RediSearch:

   ```bash
   ./build.sh
   ./build.sh DEBUG=1
   ```

   `DEBUG=1` is recommended while developing. Use `./build.sh FORCE` to rebuild from a clean build state.

5. Rust code lives in `src/redisearch_rs/`. Run Rust workspace commands from that directory unless a command says otherwise.

## Coding Standards

### C and C++

- `.clang-format` at the repository root is the formatting source of truth.
- Use 2-space indentation, no tabs, and no trailing whitespace.
- Use `rm_malloc`, `rm_free`, `rm_calloc`, and `rm_realloc` in module code. Do not use raw `malloc` or `free`.
- Return `REDISMODULE_OK` or `REDISMODULE_ERR` for status-code APIs.
- Use a `goto cleanup` pattern when a function needs shared cleanup on error paths.
- Add the required Redis license header to new source files.

### Rust

- Rust code uses edition 2024.
- Document every `unsafe` block with a `// SAFETY:` comment.
- Prefer `#[expect(...)]` over `#[allow(...)]` for lint suppressions.
- Use `tracing` macros such as `debug!`, `info!`, `warn!`, and `error!` for logging.
- If Rust/C FFI headers or generated files change, commit the generated outputs with the source change.

## Branch and Pull Request Workflow

1. Create a topic branch in your fork:

   ```bash
   git checkout -b fix-short-description
   ```

2. Keep pull requests focused on one bug fix, feature, or cleanup.
3. Push your branch to your fork and open a pull request against `RediSearch/RediSearch:master`.
4. Fill out the pull request template. Include:
   - the current behavior or problem,
   - the change you made,
   - the expected outcome,
   - the main files or subsystems changed.
5. Exactly one release-notes checkbox must be checked. CI enforces this.
6. After review begins, address comments with follow-up commits. Do not force-push or rewrite history unless a maintainer asks you to.

## Testing Requirements

Run the tests that match the area you changed. Useful commands include:

```bash
./build.sh RUN_UNIT_TESTS
./build.sh RUN_UNIT_TESTS TEST=unit_test_name
./build.sh RUN_UNIT_TESTS SAN=address
./build.sh RUN_PYTEST
./build.sh RUN_PYTEST TEST=<file>
./build.sh RUN_PYTEST TEST=<file>:<function>
```

For Rust tests:

```bash
cd src/redisearch_rs
cargo nextest run
cargo nextest run -p <crate_name>
```

Coverage runs in CI when selected by the pull request matrix. Contributors usually do not need to upload coverage locally.

## CI for Fork Pull Requests

Fork pull requests run on the regular `pull_request` event. They do not use Redis-owned sccache credentials or cache storage. Codecov upload is non-blocking for fork pull requests, but build, test, sanitizer, and coverage job failures still matter and may need to be fixed before merge.

## Review Process

Maintainers review pull requests for correctness, tests, documentation, compatibility, and release-note needs. CI must pass before merge. Reviews and merges may take time because maintainers prioritize work across bugs, releases, and community contributions.

If a maintainer asks for changes, push follow-up commits to the same branch. GitHub will update the pull request automatically.

## Legal

By contributing code to the Redis project in any form you agree to the Redis Software Grant and Contributor License Agreement attached below. Only contributions made under the Redis Software Grant and Contributor License Agreement may be accepted by Redis, and any contribution is subject to the terms of the Redis tri-license under RSALv2/SSPLv1/AGPLv3 as described in the LICENSE.txt file included in the Redis source distribution.

REDIS SOFTWARE GRANT AND CONTRIBUTOR LICENSE AGREEMENT

To specify the intellectual property license granted in any Contribution, Redis Ltd., ("Redis") requires a Software Grant and Contributor License Agreement ("Agreement"). This Agreement is for your protection as a contributor as well as the protection of Redis and its users; it does not change your rights to use your own Contribution for any other purpose permitted by this Agreement.

By making any Contribution, You accept and agree to the following terms and conditions for the Contribution. Except for the license granted in this Agreement to Redis and the recipients of the software distributed by Redis, You reserve all right, title, and interest in and to Your Contribution.

Definitions

1.1. "You" (or "Your") means the copyright owner or legal entity authorized by the copyright owner that is entering into this Agreement with Redis. For legal entities, the entity making a Contribution and all other entities that Control, are Controlled by, or are under common Control with that entity are considered to be a single contributor. For the purposes of this definition, "Control" means (i) the power, direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii) ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity.

1.2. "Contribution" means the code, documentation, or any original work of authorship, including any modifications or additions to an existing work described above.

"Work" means any software project stewarded by Redis.

Grant of Copyright License. Subject to the terms and conditions of this Agreement, You grant to Redis and to the recipients of the software distributed by Redis a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce, prepare derivative works of, publicly display, publicly perform, sublicense, and distribute Your Contribution and such derivative works.

Grant of Patent License. Subject to the terms and conditions of this Agreement, You grant to Redis and to the recipients of the software distributed by Redis a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable (except as stated in this section) patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Work, where such license applies only to those patent claims licensable by You that are necessarily infringed by Your Contribution alone or by a combination of Your Contribution with the Work to which such Contribution was submitted. If any entity institutes patent litigation against You or any other entity (including a cross-claim or counterclaim in a lawsuit) alleging that your Contribution, or the Work to which you have contributed, constitutes a direct or contributory patent infringement, then any patent licenses granted to the claimant entity under this Agreement for that Contribution or Work terminate as of the date such litigation is filed.

Representations and Warranties. You represent and warrant that: (i) You are legally entitled to grant the above licenses; and (ii) if You are an entity, each employee or agent designated by You is authorized to submit the Contribution on behalf of You; and (iii) your Contribution is Your original work, and that it will not infringe on any third party's intellectual property right(s).

Disclaimer. You are not expected to provide support for Your Contribution, except to the extent You desire to provide support. You may provide support for free, for a fee, or not at all. Unless required by applicable law or agreed to in writing, You provide Your Contribution on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.

Enforceability. Nothing in this Agreement will be construed as creating any joint venture, employment relationship, or partnership between You and Redis. If any provision of this Agreement is held to be unenforceable, the remaining provisions of this Agreement will not be affected. This represents the entire agreement between You and Redis relating to the Contribution.

Additional information on the RSALv2, SSPLv1, and AGPLv3 tri-license is in `LICENSE.txt`.
