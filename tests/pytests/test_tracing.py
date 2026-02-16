from RLTest import Env

from common import *
from includes import *


# Assert that by default "init message" is hidden
def test_default_level(env):
    logDir = env.cmd("config", "get", "dir")[1]
    logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
    logFilePath = os.path.join(logDir, logFileName)
    matchCount = _grep_file_count(logFilePath, "Tracing Subscriber Initialized!")
    env.assertEqual(
        matchCount, 0, message="tracing subscriber message present for default level"
    )


# Assert that we can set the `RUST_LOG` env var to enable the "init message"s level
def test_trace_level():
    with EnvContextManager(RUST_LOG="trace"):
        env = Env()
        logDir = env.cmd("config", "get", "dir")[1]
        logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
        logFilePath = os.path.join(logDir, logFileName)
        matchCount = _grep_file_count(logFilePath, "Tracing Subscriber Initialized!")
        env.assertEqual(matchCount, 1, message="missing tracing subscriber message")


# Assert that we can disable log message sources (in this case the subscriber itseflf)
def test_ignore_crate():
    with EnvContextManager(RUST_LOG="trace,tracing_redismodule=off"):
        env = Env()
        logDir = env.cmd("config", "get", "dir")[1]
        logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
        logFilePath = os.path.join(logDir, logFileName)
        matchCount = _grep_file_count(logFilePath, "Tracing Subscriber Initialized!")
        env.assertEqual(
            matchCount,
            0,
            message="tracing subscriber message even though source was disabled",
        )


class EnvContextManager:
    def __init__(self, **kwargs):
        self.env_vars = kwargs

    def __enter__(self):
        self.old_env = dict(os.environ)
        os.environ.update(self.env_vars)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        os.environ.clear()
        os.environ.update(self.old_env)


def _grep_file_count(filename, pattern):
    """
    Grep a file for a given pattern using python.

    Args:
        filename (str): The path to the file to grep.
        pattern (str): The pattern to search for.

    Returns:
        int: The number of lines that match the pattern.
    """
    try:
        with open(filename, "r") as f:
            count = 0
            for line in f:
                if pattern in line:
                    count += 1
            return count
    except FileNotFoundError:
        print(f"Error: File not found: {filename}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 0
