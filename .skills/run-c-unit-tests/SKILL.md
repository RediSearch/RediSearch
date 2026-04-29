---
name: run-c-unit-tests
description: Run C/C++ unit tests to verify correctness. Use this after making changes to C code to verify nothing is broken.
---

# Run C/C++ Unit Tests

Build and run the C/C++ unit test suite.

## Arguments
- No arguments: Run all C/C++ unit tests
- `<test_name>`: Run a specific unit test (matched by name or gtest filter)

Arguments provided: `$ARGUMENTS`

## Instructions

### All Unit Tests

```bash
./build.sh RUN_UNIT_TESTS ENABLE_ASSERT=1
```

This builds the project (if needed) and runs all C and C++ unit test binaries.

### Specific C++ Tests (Google Test)

C++ tests use Google Test and compile into a single binary:
```bash
bin/<target>/search-community/tests/cpptests/rstest --gtest_filter=<pattern>
```

The `<target>` is your architecture (e.g., `linux-x64`). Use `ls bin/` to find it.

Filter examples:
```bash
# Run all tests in a test suite
rstest --gtest_filter='InvertedIndexTest.*'

# Run a specific test
rstest --gtest_filter='InvertedIndexTest.TestBasic'

# Run tests matching a pattern
rstest --gtest_filter='*NumericRange*'
```

To list available tests without running them:
```bash
rstest --gtest_list_tests
```

### Specific C Tests

C test binaries are individual executables under:
```
bin/<target>/search-community/tests/ctests/
```

Run a specific C test by executing the binary directly:
```bash
bin/<target>/search-community/tests/ctests/test_<name>
```

### With AddressSanitizer

To detect memory errors (use-after-free, buffer overflow, leaks):
```bash
./build.sh RUN_UNIT_TESTS SAN=address
```

### Debug Build

For more detailed assertion failures and stack traces:
```bash
./build.sh RUN_UNIT_TESTS DEBUG=1 ENABLE_ASSERT=1
```

### Debugging Failed Tests

To debug a failing test under gdb/lldb:
```bash
# Build test binaries
./build.sh TESTS DEBUG=1

# Run under debugger
gdb bin/<target>/search-community/tests/cpptests/rstest
(gdb) run --gtest_filter='<failing_test>'
```

For C tests:
```bash
gdb bin/<target>/search-community/tests/ctests/test_<name>
(gdb) run
```

## Interpreting Output

Google Test output shows:
- `[  PASSED  ]` — test succeeded
- `[  FAILED  ]` — test failed, with assertion details (file, line, expected vs. actual)
- `[ DISABLED ]` — test is explicitly disabled

C test binaries typically print assertions to stderr and exit with non-zero on failure.

## Report

After running the tests, provide:

- Number of tests passed / failed / skipped
- For each failing test:
  - Test name and suite
  - The assertion that failed (file:line, expected vs. actual values)
  - Relevant context from the test output
- If AddressSanitizer was used, include any sanitizer findings (leak summary, error details)
