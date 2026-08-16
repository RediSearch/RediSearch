import hashlib
import threading

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

def get_log_file_path(env):
    """The server log path, captured while the server can still answer."""
    logDir = env.cmd("config", "get", "dir")[1]
    logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
    return os.path.join(logDir, logFileName)


def scan_log_fragments(logFilePath, expected_fragments):
    """
    Extract values for each fragment from the crash log, checking they appear in order.

    Args:
        logFilePath: Path to the server log file
        expected_fragments: List of field names/fragments to extract in order (e.g., "search_num_docs:")

    Returns:
        Dictionary mapping fragment to extracted value (or None if not found)
        Fragments must appear in the order specified in expected_fragments
    """
    # Initialize result dictionary with None for all fragments
    results = {fragment: None for fragment in expected_fragments}
    pos = 0  # Track position in expected_fragments to enforce ordering

    # A crash report can carry raw memory bytes, so the log is not necessarily
    # valid UTF-8. The fragments below are ASCII, so decode lossily.
    with open(logFilePath, encoding="utf-8", errors="replace") as logFile:
        for line in logFile:
            # Only look for the next expected fragment (enforces ordering)
            if pos < len(expected_fragments):
                fragment = expected_fragments[pos]
                if fragment in line:
                    # Get the part after the fragment
                    value_part = line[line.find(fragment) + len(fragment):].strip()
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


def extract_query_crash_output(env, expected_fragments, doc_count=10, crash_in_rust=False):
    """Crash a query thread mid-execution and scan the crash log; see
    scan_log_fragments for the return value."""
    logFilePath = get_log_file_path(env)
    runDebugQueryCommandAndCrash(
        env, ["FT.SEARCH", "idx", "*"], crash_in_rust=crash_in_rust
    )
    return scan_log_fragments(logFilePath, expected_fragments)


# we expect to see out index information about the crash in the log file
@skip(cluster=True)
def test_query_thread_crash():
    env = CrashingEnv(testName="test_query_thread_crash", freshEnv=True)

    doc_count = 10
    terms = ['hello', 'world']
    prepare_index(env, terms=terms, doc_count=doc_count)
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


# we expect to see the Rust panic information in the crash report,
# alongside the index information
@skip(cluster=True)
def test_query_thread_crash_with_rust_panic():
    env = CrashingEnv(testName="test_query_thread_crash_with_rust_panic", freshEnv=True)

    doc_count = 10
    terms = ['hello', 'world']
    prepare_index(env, terms, doc_count=doc_count)
    results = extract_query_crash_output(
        env,
        doc_count=doc_count,
        expected_fragments=[
            # The panic message
            'A panic occurred in the Rust code panic.payload="Crash in Rust code"',
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
    )

    # Verify all fragments were found
    for fragment, value in results.items():
        env.assertIsNotNone(value, message=f"Fragment '{fragment}' not found in crash log")

    # Verify the panic location is present (the value after the panic.payload fragment)
    env.assertIn("panic.location=", results['A panic occurred in the Rust code panic.payload="Crash in Rust code"'])
    env.assertIn("crash.rs", results['A panic occurred in the Rust code panic.payload="Crash in Rust code"'])

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

    logFilePath = get_log_file_path(env)

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

    logFilePath = get_log_file_path(env)

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
