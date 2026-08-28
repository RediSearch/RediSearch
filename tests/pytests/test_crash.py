import RLTest
import re

from common import *


class CrashingEnv(RLTest.Env):
    def getEnvByName(self):
        env = super().getEnvByName()
        env._stopProcess = self.passStopProcess
        return env

    # stopping the process checks if the process crashed and output the crash message
    # since we are testing the crash itself, the process already crashed and we want to avoid the crash error message
    def passStopProcess(self, *args, **kwargs):
        pass


BUG_REPORT_START_MARKER = "=== REDIS BUG REPORT START"
BUG_REPORT_END_MARKER = "=== REDIS BUG REPORT END"


def log_file_path(env):
    logDir = env.cmd("config", "get", "dir")[1]
    logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
    return os.path.join(logDir, logFileName)


def read_log_lines(path):
    with open(path, encoding="utf-8", errors="replace") as logFile:
        return logFile.readlines()


def bug_report_span(lines):
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


def expect_query_crash_output(env, expected_fragments, crash_in_rust=False,
                              crash_report_only=False):
    logFilePath = log_file_path(env)
    env.cmd("FT.CREATE", "idx", "SCHEMA", "text", "TEXT")
    runDebugQueryCommandAndCrash(
        env, ["FT.SEARCH", "idx", "*"], crash_in_rust=crash_in_rust
    )
    lines = read_log_lines(logFilePath)
    if crash_report_only:
        lines = bug_report_span(lines)
    pos = 0
    for line in lines:
        if pos == len(expected_fragments):
            break
        if line.find(expected_fragments[pos]) != -1:
            pos += 1
    if pos < len(expected_fragments):
        print(f"Expected fragment {expected_fragments[pos]} not found")
        return False
    else:
        return True


# we expect to see out index information about the crash in the log file
@skip(cluster=True)
def test_query_thread_crash():
    env = CrashingEnv(testName="test_query_thread_crash", freshEnv=True)
    log_path = log_file_path(env)
    env.assertTrue(
        expect_query_crash_output(
            env,
            [
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
            ],
        )
    )
    report_lines = bug_report_span(read_log_lines(log_path))
    for field in ("search_panic_payload", "search_panic_location",
                  "search_panic_recorded_at"):
        env.assertFalse(any(field in line for line in report_lines))


# we expect to see the Rust panic information in the crash report,
# alongside the index information
@skip(cluster=True)
def test_query_thread_crash_with_rust_panic():
    env = CrashingEnv(testName="test_query_thread_crash_with_rust_panic", freshEnv=True)
    log_path = log_file_path(env)
    env.assertTrue(
        expect_query_crash_output(
            env,
            [
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
                # The backtrace
                "# search_rust_backtrace",
            ],
            crash_in_rust=True,
            crash_report_only=True,
        )
    )
    log_lines = read_log_lines(log_path)
    report_lines = bug_report_span(log_lines)
    payload_line = next((l for l in report_lines if "search_panic_payload:" in l), None)
    env.assertIsNotNone(payload_line)
    env.assertIn("Crash in Rust code", payload_line)
    location_line = next((l for l in report_lines if "search_panic_location:" in l), None)
    env.assertIsNotNone(location_line)
    env.assertIn("crash.rs", location_line)
    recorded_at_line = next(
        (l for l in report_lines if "search_panic_recorded_at:" in l), None
    )
    env.assertIsNotNone(recorded_at_line)
    env.assertTrue(re.search(
        r"search_panic_recorded_at:\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC",
        recorded_at_line,
    ))
    env.assertTrue(any(
        'A panic occurred in the Rust code panic.payload="Crash in Rust code"' in line
        for line in log_lines
    ))
