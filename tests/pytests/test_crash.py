from common import *
import re


# EnterpriseStandaloneEnvBase (not RLTest.Env) so this still gets enterprise
# topology seeding (see common.py) when it defines its own Env subclass.
class CrashingEnv(EnterpriseStandaloneEnvBase):
    def getEnvByName(self):
        env = super().getEnvByName()
        env._stopProcess = self.passStopProcess
        return env

    # stopping the process checks if the process crashed and output the crash message
    # since we are testing the crash itself, the process already crashed and we want to avoid the crash error message
    def passStopProcess(self, *args, **kwargs):
        pass


def prepare_index(env, terms=["hello"], doc_count=10):
    env.cmd("FT.CREATE", "idx", "SCHEMA", "text", "TEXT")
    for i in range(doc_count):
        env.cmd("HSET", f"doc{i}", "text", " ".join(terms))
    waitForIndex(env, "idx")

# Redis brackets the crash report with these markers; only the lines between
# them are copied when users paste a bug report.
BUG_REPORT_START_MARKER = "=== REDIS BUG REPORT START"
BUG_REPORT_END_MARKER = "=== REDIS BUG REPORT END"


def log_file_path(env):
    """Path to the server's log file. Requires a live server, so resolve it
    before triggering a crash."""
    logDir = env.cmd("config", "get", "dir")[1]
    logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
    return os.path.join(logDir, logFileName)


def read_log_lines(path):
    # A crash report can carry raw memory bytes, so the log is not necessarily
    # valid UTF-8. The fragments asserted on are ASCII, so decode lossily.
    with open(path, encoding="utf-8", errors="replace") as logFile:
        return logFile.readlines()


def bug_report_span(lines):
    """The lines strictly between the bug-report START and END markers.

    Returns [] when either marker is missing, so span-restricted assertions
    fail rather than silently passing against the wrong lines.
    """
    start = next((i for i, line in enumerate(lines) if BUG_REPORT_START_MARKER in line), None)
    if start is None:
        return []
    end = next(
        (i for i, line in enumerate(lines) if i > start and BUG_REPORT_END_MARKER in line),
        None,
    )
    if end is None:
        return []
    return lines[start + 1:end]


def extract_query_crash_output(env, expected_fragments, doc_count=10, crash_in_rust=False,
                               crash_report_only=False):
    """
    Extract values for each fragment from the crash log, checking they appear in order.

    Args:
        env: Test environment
        expected_fragments: List of field names/fragments to extract in order (e.g., "search_num_docs:")
        crash_in_rust: Whether to crash in Rust code
        crash_report_only: Restrict the scan to the bug-report span (between the
            START and END markers) — the part of the log users are asked to copy

    Returns:
        Dictionary mapping fragment to extracted value (or None if not found)
        Fragments must appear in the order specified in expected_fragments
    """
    logFilePath = log_file_path(env)
    runDebugQueryCommandAndCrash(
        env, ["FT.SEARCH", "idx", "*"], crash_in_rust=crash_in_rust
    )

    lines = read_log_lines(logFilePath)
    if crash_report_only:
        lines = bug_report_span(lines)

    # Initialize result dictionary with None for all fragments
    results = {fragment: None for fragment in expected_fragments}
    pos = 0  # Track position in expected_fragments to enforce ordering

    for line in lines:
        # Only look for the next expected fragment (enforces ordering)
        if pos < len(expected_fragments):
            fragment = expected_fragments[pos]
            if fragment in line:
                # Extract the value after the fragment
                idx = line.find(fragment)
                if idx != -1:
                    # Get the part after the fragment
                    value_part = line[idx + len(fragment):].strip()
                    # If there's a value after the colon, extract it
                    if value_part:
                        # Remove trailing comments or newlines
                        value_part = value_part.split('#')[0].strip()
                        results[fragment] = value_part if value_part else line.strip()
                    else:
                        # Just mark that we found the fragment (e.g., section headers)
                        results[fragment] = line.strip()
                # Move to next fragment
                pos += 1

    return results


# we expect to see out index information about the crash in the log file
@skip(cluster=True)
def test_query_thread_crash():
    env = CrashingEnv(testName="test_query_thread_crash", freshEnv=True)

    doc_count = 10
    terms = ['hello', 'world']
    prepare_index(env, terms=terms, doc_count=doc_count)

    # Resolve the log path before the crash — the server is unreachable afterwards.
    log_path = log_file_path(env)

    results = extract_query_crash_output(env, doc_count=doc_count, expected_fragments=[
        "search_current_thread",
        "search_run_time_ns:",
        # Index name is now a section header, not a field
        "search_idx",
        # Fields are now in nested dictionaries
        "search_number_of_docs:",
        "search_index_properties:",
        "search_index_properties_in_mb:",
        "search_total_inverted_index_blocks:",
        "search_index_failures:",
    ])

    # Verify all fragments were found
    for fragment, value in results.items():
        env.assertIsNotNone(value, message=f"Fragment '{fragment}' not found in crash log")

    # Verify specific values
    # Empty index should have 0 documents
    env.assertEqual(results["search_number_of_docs:"], f"{doc_count}")

    # Verify index_properties contains expected fields
    env.assertIn(f"max_doc_id={doc_count}", results["search_index_properties:"])
    env.assertIn(f"num_terms={len(terms)}", results["search_index_properties:"])

    # Verify index_properties_in_mb contains inverted_size and it's > 0
    env.assertIn("inverted_size=", results["search_index_properties_in_mb:"])
    inverted_size_str = results["search_index_properties_in_mb:"].split("inverted_size=")[1].split(",")[0]
    inverted_size = float(inverted_size_str)
    env.assertGreater(inverted_size, 0)

    # Total inverted index blocks should be >= 0
    blocks = int(results["search_total_inverted_index_blocks:"])
    env.assertGreater(blocks, 0)

    # Verify index_failures contains indexing field
    env.assertIn("indexing=0", results["search_index_failures:"])

    # Run time should be > 0 (some time elapsed)
    run_time = int(results["search_run_time_ns:"])
    env.assertGreater(run_time, 0)

    # A C crash stashes no Rust panic, so the bug report must not carry
    # Rust-panic fields.
    report_lines = bug_report_span(read_log_lines(log_path))
    env.assertFalse(
        any("search_panic_payload" in line for line in report_lines),
        message="search_panic_payload found in the bug report of a C crash",
    )


# The Rust panic hook logs before Redis prints the bug report, so scanning the
# whole log would find the panic payload even when it is missing from the
# report itself. Assert against the START..END span — the part users are asked
# to paste into a bug report.
@skip(cluster=True)
def test_query_thread_crash_with_rust_panic():
    env = CrashingEnv(testName="test_query_thread_crash_with_rust_panic", freshEnv=True)

    doc_count = 10
    terms = ['hello', 'world']
    prepare_index(env, terms, doc_count=doc_count)

    # Resolve the log path before the crash — the server is unreachable afterwards.
    log_path = log_file_path(env)

    results = extract_query_crash_output(
        env,
        doc_count=doc_count,
        expected_fragments=[
            "search_current_thread",
            "search_run_time_ns:",
            # Index name is now a section header
            "search_idx",
            "search_number_of_docs:",
            "search_index_properties_in_mb:",
            # The backtrace
            "# search_rust_backtrace",
        ],
        crash_in_rust=True,
        crash_report_only=True,
    )

    # Verify all fragments were found
    for fragment, value in results.items():
        env.assertIsNotNone(value, message=f"Fragment '{fragment}' not found in crash log")

    # The panic details must be emitted as INFO fields inside the bug-report
    # span — the hook's tracing line is not enough, since it lands above the
    # START marker.
    log_lines = read_log_lines(log_path)
    report_lines = bug_report_span(log_lines)

    payload_line = next((l for l in report_lines if "search_panic_payload:" in l), None)
    env.assertIsNotNone(
        payload_line, message="search_panic_payload missing from the bug report"
    )
    env.assertIn("Crash in Rust code", payload_line)

    location_line = next((l for l in report_lines if "search_panic_location:" in l), None)
    env.assertIsNotNone(
        location_line, message="search_panic_location missing from the bug report"
    )
    env.assertIn("crash.rs", location_line)

    recorded_at_line = next(
        (l for l in report_lines if "search_panic_recorded_at:" in l), None
    )
    env.assertIsNotNone(
        recorded_at_line, message="search_panic_recorded_at missing from the bug report"
    )
    env.assertTrue(
        re.search(
            r"search_panic_recorded_at:\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC",
            recorded_at_line,
        ),
        message=f"unexpected search_panic_recorded_at format: {recorded_at_line}",
    )

    # The panic hook's tracing line predates the report and stays in the log;
    # it is not a substitute for the in-report fields asserted above.
    env.assertTrue(
        any(
            'A panic occurred in the Rust code panic.payload="Crash in Rust code"' in line
            for line in log_lines
        ),
        message="panic hook tracing line missing from the log",
    )

    # Verify index stats for empty index
    env.assertEqual(results["search_number_of_docs:"], f"{doc_count}")

    # Verify index_properties_in_mb contains inverted_size and it's > 0
    env.assertIn("inverted_size=", results["search_index_properties_in_mb:"])
    inverted_size_str = results["search_index_properties_in_mb:"].split("inverted_size=")[1].split(",")[0]
    inverted_size = float(inverted_size_str)
    env.assertGreater(inverted_size, 0)

    # Run time should be > 0
    run_time = int(results["search_run_time_ns:"])
    env.assertGreater(run_time, 0)

    # Verify Rust backtrace section is present
    env.assertIn("search_rust_backtrace", results["# search_rust_backtrace"])
