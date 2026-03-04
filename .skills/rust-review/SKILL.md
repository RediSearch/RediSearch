---
name: rust-review
description: Review Rust code changes for unsafe correctness, documentation quality, and C-to-Rust porting fidelity. Use this when you want to review Rust changes before merging.
---

# Rust Review

Review Rust code changes for unsafe correctness, documentation quality, and (when applicable) C-to-Rust porting fidelity.

## Arguments

The input specifies what to review. Exactly one of the following forms:

**Changesets:**
- `<revset>`: A jj revset (when `.jj/` is present) — uses `jj diff -r <revset>`.
  Examples: `slrzwyul`, `slrzwyul::vlrzmzvm`, `@-`.
- `<commit>` or `<commit1>..<commit2>`: Git commit(s) (when `.jj/` is absent) — uses `git diff` / `git show`.
- `pr:<number>`: GitHub pull request — fetches the PR branch and reviews locally.

**Source files or directories:**
- `<path>`: Path to a Rust file or directory.
- `<path1> <path2>`: Multiple files or directories.

If a path doesn't include `src/`, assume it to be in the `src/redisearch_rs` directory.
E.g. `trie_rs` becomes `src/redisearch_rs/trie_rs`.
If a path points to a directory, review all `.rs` files in that directory (recursively).

**No argument:** default to reviewing the uncommitted working-tree changes
(`jj diff` if `.jj/` is present, `git diff` otherwise).

## Instructions

### 1. Collect the code to review

**When reviewing a changeset** (revset, commits, or PR), obtain the full diff of the
Rust files (`.rs`):

```bash
# Jujutsu changes (when .jj/ is present)
jj diff -r <revset> --git -- glob:'**/*.rs'

# Git commits (when .jj/ is absent)
git diff <commit1>..<commit2> -- '*.rs'
# or for a single commit
git show <commit> -- '*.rs'
```

**For a GitHub PR** (`pr:<number>`), fetch the PR head ref and diff against master:

```bash
# When .jj/ is absent:
git fetch origin refs/pull/<number>/head
git diff origin/master...FETCH_HEAD -- '*.rs'

# When .jj/ is present:
git fetch origin refs/pull/<number>/head
jj git import
# Use the fetched SHA directly in the revset
jj diff -r 'master@origin..<sha>' --git -- glob:'**/*.rs'
```

Read the full source of every Rust file that was added or modified so that you have
complete context (not just the diff hunks).

**When reviewing source files or directories**, there is no diff — read the full source
of every `.rs` file at the given path(s) and review them in their entirety.

### 2. Determine if this is a C-to-Rust port

Scan the diff and commit messages for signals that the change re-implements existing C code:
- New files under `c_entrypoint/*_ffi/`
- Removal or reduction of C files with corresponding new Rust files
- Commit messages mentioning "port", "migrate", "rewrite", "reimplement", or "replace"

If detected, set **porting mode = true** and identify the original C module(s) by reading
them with [`/read-unmodified-c-module`](../read-unmodified-c-module/SKILL.md).

### 3. Review checklist

Run every check below on the changed Rust code. For each violation found, record:
- **File and line** (or line range)
- **Rule** that is violated
- **Explanation** of the issue
- **Suggested fix**

#### 3a. Unsafe — method pre-conditions

Every `unsafe fn` must have a `# Safety` section in its doc comment that documents
**all** pre-conditions the caller must uphold.

Violations:
- `unsafe fn` with no doc comment at all.
- `unsafe fn` with a doc comment but no `# Safety` section.
- `# Safety` section that omits a pre-condition required for soundness
  (e.g. pointer validity, alignment, aliasing, lifetime, initialized memory).

#### 3b. Unsafe — call-site safety comments

Every `unsafe` block (or `unsafe` call inside an `unsafe fn`) must have a
`// SAFETY:` comment **immediately preceding** the unsafe block or call that
explains why every pre-condition of the called function / accessed operation
is satisfied at that call site.

Violations:
- Missing `// SAFETY:` comment.
- Comment that does not address every pre-condition listed in the callee's `# Safety`
  section (or the standard library's documented safety requirements).
- Generic or vacuous comments (e.g. `// SAFETY: safe to call`) that do not reference
  the specific pre-conditions.

#### 3c. Rustdoc — intra-doc links

When a rustdoc comment mentions a Rust symbol (type, function, constant, trait, module, etc.),
it must use an [intra-doc link](https://doc.rust-lang.org/rustdoc/write-documentation/linking-to-items-by-name.html)
(`[`Symbol`]` or `[`Symbol::method`]`).

Violations:
- A symbol name appears in backticks (`` `Foo` ``) inside a doc comment but is not an intra-doc link.
- A symbol name appears as plain text inside a doc comment without backticks or link.

Exceptions: symbols that are not Rust items (e.g. C function names, Redis command names,
field names used in prose) do not need intra-doc links.

### 4. Porting-mode checks (only when porting mode = true)

#### 4a. Semantic equivalence

Compare the new Rust implementation against the original C code and verify:
- All branches / code paths in the C code have a corresponding path in Rust.
- Edge cases (NULL checks, overflow, empty inputs, error returns) are preserved or
  replaced with idiomatic Rust equivalents (e.g. `Option`, `Result`).
- Numeric types and casts preserve the original semantics (watch for sign / width changes).
- Side effects (global state mutations, logging, metric updates) are preserved.

Violations: any semantic divergence that could change observable behavior.

#### 4b. Test coverage

Identify all C/C++ tests that exercise the ported module (look under `tests/` for files
that reference the module's functions or types).

For each C/C++ test, verify that an equivalent Rust test exists that covers the same
scenario. Use [`/check-rust-coverage`](../check-rust-coverage/SKILL.md) to confirm
line-level coverage of the new Rust code.

Violations:
- A C/C++ test scenario that has no corresponding Rust test.
- Rust code paths that are uncovered by any test.

### 5. Emit the report

Present findings grouped by check (3a, 3b, 3c, 4a, 4b). For each group, list the
violations or state "No issues found."

At the end, provide a summary:
- Total number of violations by severity (blocking vs. suggestion).
- Whether the change is **ready to merge** or **needs revision**.

Blocking violations: any issue in 3a, 3b, 4a, or 4b.
Suggestions: issues in 3c (intra-doc links).
