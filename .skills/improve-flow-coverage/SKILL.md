---
name: improve-flow-coverage
description: Improve Python flow test coverage for C source files. Runs /check-flow-coverage to find gaps, then writes or extends tests following /write-flow-tests guidelines.
---

# Improve Flow Coverage

Improve Python flow test coverage for specified C source files by finding coverage gaps and
writing tests to close them.

This skill orchestrates `/check-flow-coverage` (analysis) and `/write-flow-tests` (test
authoring) into a single workflow.

## Arguments
- `<path>`: Path to a C source file.
- `<path 1> <path 2>`: Multiple C source file paths.

Paths can be relative to the repository root. E.g. `src/module.c` or `src/query.c`.

Arguments provided: `$ARGUMENTS`

## Instructions

If no arguments were provided (`$ARGUMENTS` is empty), stop and ask the user to provide one or
more source file paths. Example usage: `/improve-flow-coverage src/module.c src/query.c`

If the user provides a Rust file path (e.g., under `src/redisearch_rs/`), stop and redirect
them to use `/check-rust-coverage` instead — this workflow only covers C source files.

Follow these steps in order.

### Step 1: Get the coverage report

Run `/check-flow-coverage $ARGUMENTS` to get the coverage analysis. This will build with
coverage instrumentation, run the full test suite, and produce a report of testable coverage
gaps grouped by feature.

### Step 2: Triage the gaps

Review the coverage report from Step 1. For each testable gap, determine:

1. **Which gaps to address with tests** — code paths reachable via standard Redis commands
   (`FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE`, `FT.EXPLAIN`, etc.) that should be exercised
   by flow tests. Group related gaps by feature.

2. **Which gaps to address with assertions** — unreachable defensive code paths that should
   be replaced with assertions rather than tested. See "Replacing defensive code with
   assertions" below.

**Present the testable coverage gaps to the user** as a structured list, grouped by feature:
- Feature/area name (e.g., "Geometry queries", "Tag prefix edge cases")
- All related line ranges under that feature, with brief descriptions
- Suggested test approach (extend existing test or write new one)
- Which test file to modify

Ask the user which gaps they'd like to address before writing any code.

### Step 3: Replace unreachable defensive code with assertions

After the user selects which testable gaps to address, also handle **unreachable defensive
code paths** — code that exists only as a safety net but can never actually be reached at
runtime. Instead of silently swallowing impossible conditions, replace them with assertions
so that violations are caught during development.

**When to use assertions vs. lcov exclusions:**

- **Replace with `RS_ASSERT`** when the defensive code checks a condition that should
  *never* be false. If the condition were ever violated, it would indicate a bug that should
  be caught immediately — not silently handled. This is the **preferred approach** for most
  unreachable defensive code.

- **Keep lcov exclusions** (`LCOV_EXCL_LINE` / `LCOV_EXCL_START`..`LCOV_EXCL_STOP`) only
  when the defensive code performs **cleanup or resource management** that cannot be safely
  replaced by an assertion (e.g., freeing memory to avoid leaks before returning).

**Eligible for replacement with `RS_ASSERT`:**
- Exhaustive switch default/fallthrough arms that exist only for compiler completeness
  (e.g., `default: return NULL` in switches over enum types where all valid cases are
  already handled) — replace the body with `RS_ABORT("reason")`
- Defensive type checks that guard against impossible node types in a specific context
  (e.g., a tag eval switch that only receives TOKEN/PREFIX/WILDCARD_QUERY/PHRASE nodes)
- Null checks after internal functions that are guaranteed to return non-NULL in practice
  (but NOT allocation-failure guards — those should remain as-is or use lcov exclusions)

**Still eligible for lcov exclusion only:**
- Allocation-failure guards for small fixed-size allocations (`if (!ptr) return NULL` after
  `malloc`/`calloc`/iterator creation) — these perform cleanup that must remain
- Multi-line defensive blocks that free resources before returning

**NOT eligible for either** — do not mark or replace these even if currently uncovered:
- Disk-only paths (guarded by `diskSpec`/`SearchDisk`) — these are tested separately
- API-only code (reachable via the C module API) — tested by LLAPI unit tests
- Code that is testable but just hasn't been tested yet
- Error handling for user-reachable error conditions

**Assertion macros** (defined in `deps/rmutil/rm_assert.h`):
- `RS_ASSERT(condition)` — debug-only assertion (NOP without `ENABLE_ASSERT`). Use for
  conditions that should always hold but where crashing in production is undesirable.
- `RS_ASSERT_ALWAYS(condition)` — always active, even in release builds. Use sparingly,
  only for invariants so critical that violating them would cause worse damage than crashing.
- `RS_ABORT("reason")` — unconditional abort with a message. Use for switch default arms
  and other paths that should be truly impossible to reach.

**Examples:**

```c
// BEFORE: defensive default arm silently returns NULL
default:
    return NULL;

// AFTER: assert that we never reach this — all valid types are handled above
default:
    RS_ABORT("unexpected node type in tag eval");
    return NULL;  // unreachable, keeps compiler happy
```

```c
// BEFORE: defensive null check with fallback behavior
if (!node) return;

// AFTER: assert the invariant — node must always be non-NULL here
RS_ASSERT(node);
```

```c
// Lcov exclusion is still appropriate for allocation-failure guards with cleanup:
if (!it) { // LCOV_EXCL_START — defensive: allocation failure in iterator creation
    rm_free(its);
    return NULL;
} // LCOV_EXCL_STOP
```

### Step 4: Write or extend tests

After the user selects which coverage gaps to address, write or extend the Python tests.

Follow the guidelines in [/write-flow-tests](../write-flow-tests/SKILL.md) when writing new
tests. Also look at nearby tests in the same file for additional style and patterns specific
to that file.

### Step 5: Verify coverage improvement

After the tests have been written/updated, re-run the coverage pipeline to confirm
the previously uncovered lines are now covered.

**IMPORTANT**: Run the tests exactly once. Do NOT run the same tests twice (e.g., once to
check pass/fail and again for coverage). A single `RUN_PYTEST COV=1` run both executes the
tests and records gcov data. After that single run, check results.

**Note:** Shell variables do not persist between tool calls. When creating marker files with
`mktemp`, capture the printed path from the command output and substitute it literally in all
subsequent commands.

1. Rebuild if any C source was changed:
   ```bash
   ./build.sh COV=1
   ```

2. Record a start marker so we can check freshness afterward:
   ```bash
   COV_MARKER=$(mktemp /tmp/cov_start_marker.XXXXXX)
   echo "Using marker: $COV_MARKER"
   ```

3. Run the specific tests that were added/modified (once). `COV=1` resets gcov counters,
   captures the baseline, and runs `capture_coverage` automatically after the run completes.
   Use `tail` to avoid truncation. **Set a timeout of at least 300000ms (5 min) for
   targeted runs — they are faster than the full suite but can still take a few minutes.**

   **TEST parameter format**: The `TEST` parameter accepts `module.function` format
   (e.g., `TEST="test_parser.test_my_func"`) or just a module name (e.g.,
   `TEST="test_parser"`). Using only a bare function name like `TEST="test_my_func"` will
   fail because the runner interprets it as a file path (`test_my_func.py`).

   ```bash
   # IMPORTANT: Use timeout of 300000ms for this command
   ./build.sh RUN_PYTEST COV=1 TEST="<test_file>" 2>&1 | tail -80
   ```

4. Check if `flow_standalone.info` was updated (newer than start marker). Substitute the
   literal marker path from step 2:

   ```bash
   if [ -f bin/flow_standalone.info ] && [ bin/flow_standalone.info -nt /tmp/cov_start_marker.XXXXXX ]; then
       echo "FRESH — coverage file updated"
   else
       echo "STALE or MISSING — coverage capture failed"
   fi
   rm -f /tmp/cov_start_marker.XXXXXX
   ```

   If stale or missing, the coverage build may not be instrumented correctly —
   rebuild with `COV=1 FORCE` and retry.

5. Re-extract coverage for the target files using the same Python script from
   `/check-flow-coverage` Step 3, but **only check the specific lines you are trying to
   cover**, not overall coverage. Running a subset of tests will show artificially low
   overall coverage since most code paths are not exercised.

6. **Compare the results**: Report which lines are now covered and which gaps remain.
   If gaps remain, go back to Step 2 for the remaining uncovered paths. If only a few
   scattered lines remain (e.g., 2-3 lines in error branches or edge cases), note them
   but do not iterate further — the effort to cover them likely outweighs the benefit.

## Notes

- This skill orchestrates `/check-flow-coverage` and `/write-flow-tests` — refer to those
  skills for detailed guidelines on coverage analysis and test authoring respectively.
- The coverage build uses `debug-cov` flavor with gcov instrumentation for C code.
- Coverage data only tracks C source code under `src/` and `deps/thpool/`.
- This skill does **not** cover Rust code. For Rust coverage, use `/check-rust-coverage`.
