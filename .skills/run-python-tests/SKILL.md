---
name: run-python-tests
description: Run end-to-end Python tests after making changes to verify correctness. Use this when you want to verify your changes from an end-to-end perspective, after ensuring the build and Rust tests pass.
---

# Run Python Tests Skill

Run end-to-end Python tests after making changes to verify correctness.

## Arguments
- No arguments: Run all Python tests
- `<filename>`: Run all Python tests in the specified file
- `<filename>:<test_name>`: Run a specific Python test in the specified file
- `<filename 1> <filename 2>`: Run all Python tests in the specified files

Arguments provided: `$ARGUMENTS`

## Instructions

### Test Timeout

Some tests take longer to run than others. The `TEST_TIMEOUT` parameter controls how long each test is allowed to run before being terminated.

- **Quick verification (preferred)**: Pass `TEST_TIMEOUT=20` to get fast feedback. Tests that exceed this timeout will be terminated.
- **Full test run**: Omit `TEST_TIMEOUT` to let tests run with the default timeout (300 seconds).

**Always start with a quick verification, using `TEST_TIMEOUT=20`**. Faster feedback loops lead to faster iteration.

If there are timeouts during a quick verification run, check if the timed-out tests are relevant to the current task:
- If you can determine relevance autonomously (e.g., the test name clearly relates to the code you changed), re-run those specific tests without a timeout.
- If you cannot determine relevance, ask the user whether the timed-out tests should be re-run without a timeout.

### All Tests

```bash
./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20
```

### All Tests From A Specific File

```bash
source .venv/bin/activate && ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="<filename without extension>"
```

For example:

```bash
source .venv/bin/activate && ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="test_crash"
```

for running tests from `tests/pytests/test_crash.py`.

### All Tests From Multiple Files

```bash
source .venv/bin/activate && ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="<filename 1> <filename 2>"
```

For example:

```bash
source .venv/bin/activate && ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="test_crash test_gc"
```

for running tests from `tests/pytests/test_crash.py` and `tests/pytests/test_gc.py`.

### Specific Test From A Specific File

```bash
source .venv/bin/activate && ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="<filename without extension>:<test_name>"
```

For example:

```bash
source .venv/bin/activate && ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST_TIMEOUT=20 TEST="test_crash:test_query_thread_crash"
```

for running the `test_query_thread_crash` test from `tests/pytests/test_crash.py`.

## Interpreting The Test Output

For each failed test, you'll see an error message with details about the failure, as seen _from the Python test runner_.
Each failed test will also have an associated log file, located under `tests/pytests/logs`. The name of the log file
changes with every test run, but it's included in the output of the test runner.

## Report

After running the tests, put together a report:

- Number of tests passed
- Number of tests failed
- Number of tests skipped
- For each failing test:
  - The error message reported in the test output, on the Python side
  - The stack trace from the server logs for the Redis server:
    - If the panic is in Rust code, include the Rust panic message and the Rust backtrace (from the `# search_rust_backtrace` section)
    - If the crash is in C code, include just the C backtrace.
  - Path to the log file, relative to the root of the repository
