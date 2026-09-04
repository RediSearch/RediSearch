# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
import hashlib
import re
import subprocess
import threading
from redis.backoff import NoBackoff
from redis.retry import Retry

from common import *
from test_blocked_client_timeout import wait_for_blocked_query_client


# EnterpriseStandaloneEnvBase (not RLTest.Env) so this still gets enterprise
# topology seeding (see common.py) when it defines its own Env subclass.
class CrashingEnv(EnterpriseStandaloneEnvBase):
    def getEnvByName(self):
        env = super().getEnvByName()
        # In cluster mode only the first shard is crashed (the tests use it as
        # the coordinator); the surviving shards must still stop normally.
        crashing = env.shards[0] if getattr(env, 'shards', None) else env
        crashing._stopProcess = self.passStopProcess
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

    Returns [] when the START marker is missing, so span-restricted assertions
    fail rather than silently passing against the wrong lines. A missing END
    marker means the crash handler died mid-report (sanitizer builds truncate
    the report during its slow memory-test phase), so the span then runs to the
    end of the log.
    """
    start = next((i for i, line in enumerate(lines) if BUG_REPORT_START_MARKER in line), None)
    if start is None:
        return []
    end = next(
        (i for i, line in enumerate(lines) if i > start and BUG_REPORT_END_MARKER in line),
        None,
    )
    if end is None:
        return lines[start + 1:]
    return lines[start + 1:end]


def scan_log_fragments(logFilePath, expected_fragments, crash_report_only=False):
    """
    Extract values for each fragment from an existing crash log, checking they
    appear in order.

    Args:
        logFilePath: Path to the server log file
        expected_fragments: List of field names/fragments to extract in order (e.g., "search_num_docs:")
        crash_report_only: Restrict the scan to `bug_report_span`, the part of
            the log users are asked to copy

    Returns:
        Dictionary mapping fragment to extracted value (or None if not found)
        Fragments must appear in the order specified in expected_fragments
    """
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


def extract_query_crash_output(env, expected_fragments, doc_count=10, crash_in_rust=False,
                               crash_report_only=False):
    """Crash a query thread mid-execution and scan the crash log."""
    logFilePath = log_file_path(env)
    runDebugQueryCommandAndCrash(
        env, ["FT.SEARCH", "idx", "*"], crash_in_rust=crash_in_rust
    )
    return scan_log_fragments(logFilePath, expected_fragments, crash_report_only)


# we expect to see out index information about the crash in the log file
@skip(cluster=True)
def test_query_thread_crash():
    env = CrashingEnv(testName="test_query_thread_crash", freshEnv=True)

    doc_count = 10
    terms = ['hello', 'world']
    prepare_index(env, terms=terms, doc_count=doc_count)

    # Resolve the log path before the crash: the server is unreachable afterwards.
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
    for field in ("search_rust_backtrace", "search_panic_payload",
                  "search_panic_location", "search_panic_recorded_at"):
        env.assertFalse(
            any(field in line for line in report_lines),
            message=f"{field} found in the bug report of a C crash",
        )


@skip(cluster=True, macos=True, musl=True, asan=True, msan=True)
@require_enable_assert
def test_query_thread_crash_with_pending_spec_writer(_probe_env):
    """MOD-17347: crash reporting must not reacquire the crashed query's spec lock."""
    env = CrashingEnv(
        testName="test_query_thread_crash_with_pending_spec_writer",
        moduleArgs="WORKERS 1 TIMEOUT 0",
        freshEnv=True,
    )
    prepare_index(env)

    sync_point = "BeforeAggregateResultsClaim"
    env.expect(debug_cmd(), "SYNC_POINT", "CLEAR").ok()
    env.expect(debug_cmd(), "SYNC_POINT", "ARM", sync_point).ok()

    query_conn = env.getConnection()
    writer_conn = env.getConnection()
    query_conn.set_retry(Retry(NoBackoff(), 0))
    writer_conn.set_retry(Retry(NoBackoff(), 0))
    process = env.envRunner.masterProcess
    writer_thread = None
    unexpected_errors = []

    def run_ignoring_connection_error(conn, *cmd):
        try:
            conn.execute_command(*cmd)
        except redis.exceptions.ConnectionError:
            pass  # both commands lose their connection when Redis aborts
        except Exception as error:
            unexpected_errors.append(error)

    query_thread = threading.Thread(
        target=run_ignoring_connection_error,
        args=(query_conn, debug_cmd(), "FT.SEARCH", "idx", "*", "CRASH",
              "DEBUG_PARAMS_COUNT", 1),
        daemon=True,
    )
    query_thread.start()

    exited_promptly = False
    try:
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), "SYNC_POINT", "IS_WAITING", sync_point) == 1, {}),
            f"Timeout waiting for {sync_point}",
            timeout=15,
        )

        # The worker holds the spec read lock at the sync point. This write
        # blocks the main thread on the same spec and releases the worker via
        # the sync point's pending-writer predicate. The worker then aborts.
        writer_thread = threading.Thread(
            target=run_ignoring_connection_error,
            args=(writer_conn, "HSET", "doc:pending-writer", "text", "hello"),
            daemon=True,
        )
        writer_thread.start()

        try:
            process.wait(timeout=15)
            exited_promptly = True
        except subprocess.TimeoutExpired:
            pass
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        query_thread.join(timeout=5)
        if writer_thread is not None:
            writer_thread.join(timeout=5)
        query_conn.close()
        writer_conn.close()

    env.assertEqual(
        unexpected_errors,
        [],
        message=f"Unexpected command errors: {unexpected_errors}",
    )
    env.assertFalse(query_thread.is_alive(), message="Crash query thread did not exit")
    env.assertFalse(writer_thread.is_alive(), message="Pending writer thread did not exit")
    env.assertTrue(
        exited_promptly,
        message="Redis hung while producing a crash report with a pending spec writer",
    )


# The Rust panic hook logs before Redis prints the bug report, so scanning the
# whole log would find the panic payload even when it is missing from the
# report itself. Assert against the START..END span, the part users are asked
# to paste into a bug report.
@skip(cluster=True)
def test_query_thread_crash_with_rust_panic():
    env = CrashingEnv(testName="test_query_thread_crash_with_rust_panic", freshEnv=True)

    doc_count = 10
    terms = ['hello', 'world']
    prepare_index(env, terms, doc_count=doc_count)

    # Resolve the log path before the crash: the server is unreachable afterwards.
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
            "search_backtrace:",
        ],
        crash_in_rust=True,
        crash_report_only=True,
    )

    # Verify all fragments were found
    for fragment, value in results.items():
        env.assertIsNotNone(value, message=f"Fragment '{fragment}' not found in crash log")

    # The panic details must be emitted as INFO fields inside the bug-report
    # span: the hook's tracing line is not enough, since it lands above the
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
    env.assertTrue(
        re.search(r"\d+:", results["search_backtrace:"]),
        message="Rust panic backtrace is empty",
    )


def crash_main_thread_with_blocked_queries(env, hide_user_data=False):
    """Block an FT.SEARCH and an FT.CURSOR READ on a paused worker pool, then
    crash the main thread. Returns (logFilePath, cursor_id) for scanning the
    crash report."""
    prepare_index(env)

    # The cursor to read from; its initial cycle must complete before the pool
    # is paused.
    _, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@text',
                           'WITHCURSOR', 'COUNT', '2')
    env.assertNotEqual(cursor_id, 0)

    if hide_user_data:
        env.expect('CONFIG', 'SET', 'hide-user-data-from-log', 'yes').ok()

    logFilePath = log_file_path(env)

    env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()

    def run_ignoring_errors(*cmd):
        try:
            env.getConnection().execute_command(*cmd)
        except Exception:
            pass  # the server crashes while the command is blocked, by design

    threading.Thread(target=run_ignoring_errors,
                     args=('FT.SEARCH', 'idx', '*'), daemon=True).start()
    threading.Thread(target=run_ignoring_errors,
                     args=('FT.CURSOR', 'READ', 'idx', cursor_id),
                     daemon=True).start()

    # A client observed blocked proves its cycle is registered: registration
    # runs inside the command handler, before the handler returns.
    wait_for_blocked_query_client(env, 'FT.SEARCH')
    wait_for_blocked_query_client(env, 'FT.CURSOR|READ')

    try:
        env.cmd('DEBUG', 'SEGFAULT')  # crash the main thread
    except Exception:
        pass

    return logFilePath, cursor_id


# a main-thread crash must report the in-flight blocked queries and cursors,
# read through each request wrapper's registry entry
@skip(cluster=True)
def test_main_thread_crash_reports_blocked_queries():
    # WORKERS 1 forces the blocked-client path; the CI runner disables workers
    # by default, which would run the queries inline, unregistered.
    env = CrashingEnv(testName="test_main_thread_crash_reports_blocked_queries",
                      moduleArgs='WORKERS 1', freshEnv=True)
    logFilePath, cursor_id = crash_main_thread_with_blocked_queries(env)

    results = scan_log_fragments(logFilePath, [
        '# search_blocked_queries',
        'search_idx:started_at=',
        '# search_blocked_cursors',
        f'search_{cursor_id}:index=idx,started_at=',
    ])
    for fragment, value in results.items():
        env.assertIsNotNone(value, message=f"Fragment '{fragment}' not found in crash log")
    env.assertGreater(int(results['search_idx:started_at=']), 0)


# under hideUserDataFromLog the blocked-queries walkers must report the
# obfuscated index name (the spec's own sha1 derivation) and never the raw one.
# Enterprise skip: 'hide-user-data-from-log' is a server-level config whose
# presence depends on the enterprise server build (see test_hideUserDataFromLogs).
@skip(cluster=True, enterprise=True)
def test_main_thread_crash_reports_blocked_queries_obfuscated():
    # WORKERS 1 forces the blocked-client path; the CI runner disables workers
    # by default, which would run the queries inline, unregistered.
    env = CrashingEnv(testName="test_main_thread_crash_reports_blocked_queries_obfuscated",
                      moduleArgs='WORKERS 1', freshEnv=True)
    logFilePath, cursor_id = crash_main_thread_with_blocked_queries(env, hide_user_data=True)

    # Same derivation as the spec's own obfuscated name: sha1 of the name.
    obfuscated = 'Index@' + hashlib.sha1(b'idx').hexdigest()
    results = scan_log_fragments(logFilePath, [
        '# search_blocked_queries',
        f'search_{obfuscated}:started_at=',
        '# search_blocked_cursors',
        f'search_{cursor_id}:index={obfuscated},started_at=',
    ])
    for fragment, value in results.items():
        env.assertIsNotNone(value, message=f"Fragment '{fragment}' not found in crash log")

    # The raw index name must not leak into the blocked-query entries.
    with open(logFilePath) as logFile:
        log = logFile.read()
    env.assertNotIn('search_idx:started_at=', log)
    env.assertNotIn('index=idx,', log)


# the coordinator's blocked cycles register too: a coordinator main-thread
# crash must report its in-flight distributed queries and cursor reads
# (FT.HYBRID shares the aggregate path's registration site)
@skip(cluster=False)
def test_main_thread_crash_reports_blocked_coordinator_queries():
    env = CrashingEnv(testName="test_main_thread_crash_reports_blocked_coordinator_queries",
                      freshEnv=True)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 'text', 'hello')

    # Shard 1 coordinates every command this test sends directly to it.
    coordinator = env.getConnection(1)

    # The cursor to read from; its initial cycle needs the coordinator pool
    # still running.
    res = coordinator.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@text',
                                      'WITHCURSOR', 'COUNT', '2')
    cursor_id = res[1]
    env.assertNotEqual(cursor_id, 0)

    logFilePath = log_file_path(env)

    # Dispatched coordinator cycles stay queued — and registered — on the
    # paused pool.
    coordinator.execute_command(debug_cmd(), 'COORD_THREADS', 'PAUSE')
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
        'Timeout while waiting for coordinator threads to pause')

    def run_ignoring_errors(*cmd):
        try:
            env.getConnection(1).execute_command(*cmd)
        except Exception:
            pass  # the coordinator crashes while the command is blocked, by design

    threading.Thread(target=run_ignoring_errors,
                     args=('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@text'),
                     daemon=True).start()
    threading.Thread(target=run_ignoring_errors,
                     args=('FT.CURSOR', 'READ', 'idx', cursor_id),
                     daemon=True).start()

    # A client observed blocked proves its cycle is registered: registration
    # runs inside the command handler, before the handler returns.
    wait_for_blocked_query_client(env, 'FT.AGGREGATE')
    wait_for_blocked_query_client(env, 'FT.CURSOR|READ')

    try:
        coordinator.execute_command('DEBUG', 'SEGFAULT')  # crash the coordinator's main thread
    except Exception:
        pass

    results = scan_log_fragments(logFilePath, [
        '# search_blocked_queries',
        'search_idx:started_at=',
        '# search_blocked_cursors',
        f'search_{cursor_id}:index=idx,started_at=',
    ])
    for fragment, value in results.items():
        env.assertIsNotNone(value, message=f"Fragment '{fragment}' not found in crash log")
    env.assertGreater(int(results['search_idx:started_at=']), 0)
