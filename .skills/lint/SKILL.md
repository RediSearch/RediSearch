---
name: lint
description: Check code quality and formatting before committing changes. Use this to verify your changes meet our coding standards.
---

# Lint Skill

Check code quality and formatting before committing changes.

## Usage
Run this skill to check for lint errors and formatting issues.

## Instructions

### C Code

1. Check C/C++ formatting against `.clang-format`:
   ```bash
   clang-format --dry-run -Werror <modified .c and .h files>
   ```

2. If formatting check fails, apply formatting:
   ```bash
   clang-format -i <modified .c and .h files>
   ```

3. Verify the project compiles without warnings-as-errors:
   ```bash
   ./build.sh
   ```
   The CMake build promotes key warnings to errors (`-Werror=incompatible-pointer-types`,
   `-Werror=implicit-function-declaration`). A successful build confirms these pass.

### VecSim formatting

Preserve the component's scoped `.clang-format` files (four-space indentation),
including those under the relocated unit and benchmark directories. Use
`make vecsim-format` to check with clang-format 18, or `make vecsim-format FIX=1`
to apply it. `CLANG_FORMAT` may point to a version-18 executable. Do not apply
Search's root formatting rules to VecSim or reformat it with a newer formatter.

### Rust Code

1. Run the lint check:
   ```bash
   make lint
   ```

2. If clippy reports warnings or errors, fix them before proceeding

3. Check formatting:
   ```bash
   make fmt CHECK=1
   ```

4. If formatting check fails, apply formatting:
   ```bash
   make fmt
   ```

5. If license headers are missing, add them:
   ```bash
   (cd src/redisearch_rs && cargo license-fix)   # subshell: custom subcommand, no --manifest-path
   ```

### Common Clippy Fixes

- **Document unsafe blocks**: Add `// SAFETY:` comment explaining why the unsafe code is sound
- **Use `#[expect(...)]`**: Prefer over `#[allow(...)]` for lint suppressions

### Rust-Only Quick Check

```bash
cargo clippy --manifest-path src/redisearch_rs/Cargo.toml --all-targets --all-features
```
