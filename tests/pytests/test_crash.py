import RLTest

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


def expect_query_crash_output(env, expected_fragments, crash_in_rust=False):
    logDir = env.cmd("config", "get", "dir")[1]
    logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
    logFilePath = os.path.join(logDir, logFileName)
    env.cmd("FT.CREATE", "idx", "SCHEMA", "text", "TEXT")
    runDebugQueryCommandAndCrash(
        env, ["FT.SEARCH", "idx", "*"], crash_in_rust=crash_in_rust
    )
    pos = 0
    with open(logFilePath) as logFile:
        for line in logFile:
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
    env.assertTrue(
        expect_query_crash_output(env, [
            "search_current_thread",
            "search_index_name:idx",
            "search_num_docs:",
            "search_max_doc_id:",
            "search_num_terms:",
            "search_inverted_sz_mb:",
            "search_indexing:",
            "search_num_fields:",
        ])
    )


# we expect to see the Rust panic information in the crash report,
# alongside the index information
@skip(cluster=True)
def test_query_thread_crash_with_rust_panic():
    env = CrashingEnv(testName="test_query_thread_crash_with_rust_panic", freshEnv=True)
    env.assertTrue(
        expect_query_crash_output(
            env,
            [
                # The panic message
                'A panic occurred in the Rust code panic.payload="Crash in Rust code"',
                "search_current_thread",
                "search_index_name:idx",
                "search_num_docs:",
                "search_inverted_sz_mb:",
                # The backtrace
                "# search_rust_backtrace",
            ],
            crash_in_rust=True,
        )
    )
