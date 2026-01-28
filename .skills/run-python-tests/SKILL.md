---
name: run-python-tests
description: Run end-to-end Python tests after making changes to verify correctness
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

### All Tests

```bash
TEST_TIMEOUT=20 ./build.sh RUN_PYTEST ENABLE_ASSERT=1
```

### All Tests From A Specific File

```bash
source .venv/bin/activate && TEST_TIMEOUT=20 ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST="<filename without extension>"
```

For example:

```bash
source .venv/bin/activate && TEST_TIMEOUT=20 ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST="test_crash"
``` 

for running tests from `tests/pytests/test_crash.py`.

### All Tests From Multiple Files

```bash
source .venv/bin/activate && TEST_TIMEOUT=20 ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST="<filename 1> <filename 2>"
```

For example:

```bash
source .venv/bin/activate && TEST_TIMEOUT=20 ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST="test_crash test_gc"
``` 

for running tests from `tests/pytests/test_crash.py` and `tests/pytests/test_gc.py`.

### Specific Test From A Specific File

```bash
source .venv/bin/activate && TEST_TIMEOUT=20 ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST="<filename without extension>:<test_name>"
```

For example:

```bash
source .venv/bin/activate && TEST_TIMEOUT=20 ./build.sh RUN_PYTEST ENABLE_ASSERT=1 TEST="test_crash:test_query_thread_crash"
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
