---
name: lint_and_format
description: Check code quality and formatting before committing changes
---

# Lint and Format Skill

Check code quality and fix formatting before committing changes.

## Quick Reference

| Command | What it checks | When to use |
|---------|----------------|-------------|
| `./build.sh lint` | Rust code (clippy + fmt check) | Before committing Rust changes |
| `./build.sh lint-vecsim` | C++ code (clang-format) | Before committing C++ changes |
| `./build.sh format` | Fix Rust formatting | When lint reports formatting issues |
| `./build.sh format-vecsim` | Fix C++ formatting | When lint-vecsim reports formatting issues |

## Rust Code (redisearch_disk/)

### Check for Issues

```bash
./build.sh lint
```

This runs:
1. `cargo clippy -- -D warnings` - Static analysis with all warnings as errors
2. `cargo fmt -- --check` - Formatting check (does not modify files)

### Fix Formatting

If the formatting check fails:

```bash
./build.sh format
```

This runs `cargo fmt` to automatically fix formatting issues.

### Manual Clippy/Fmt Commands

For more control, you can run the tools directly:

```bash
cd redisearch_disk

# Run clippy with specific lints
cargo clippy -- -D warnings

# Check formatting
cargo fmt -- --check

# Apply formatting
cargo fmt
```

## C++ Code (vecsim_disk/)

### Check for Issues

```bash
./build.sh lint-vecsim
```

This runs `clang-format --dry-run -Werror` on all `.cpp`, `.h`, and `.hpp` files.

### Fix Formatting

If the formatting check fails:

```bash
./build.sh format-vecsim
```

This runs `clang-format -i` to automatically fix formatting issues.

## CI Integration

The lint checks are run automatically in CI via `.github/workflows/task-lint.yml`:

1. Rust lint (`./build.sh lint`)
2. C++ lint (`./build.sh lint-vecsim`)

Both must pass for PRs to be merged.

## Troubleshooting

### Clippy Warnings

Fix all clippy warnings before committing. Common issues:
- Unused imports or variables
- Redundant clones
- Missing documentation on public items

If a clippy warning is a false positive, use `#[expect(...)]` (preferred over `#[allow(...)]`) with a comment explaining why:

```rust
#[expect(clippy::some_lint, reason = "Explanation of why this is acceptable")]
```

### Formatting Conflicts

If you have merge conflicts in formatting, resolve them and run:

```bash
./build.sh format        # For Rust
./build.sh format-vecsim # For C++
```

### SpeedB Linking Errors During Lint

The lint command sets `SPEEDB_SKIP_LINK=1` to allow linting without having SpeedB built. If you still encounter linking errors, ensure you're using the build.sh wrapper:

```bash
./build.sh lint  # Correct
# NOT: cd redisearch_disk && cargo clippy  # May fail without SpeedB
```
