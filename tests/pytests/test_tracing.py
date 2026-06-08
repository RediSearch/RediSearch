from common import *
from includes import *
from RLTest import Env
from RLTest.env import Defaults

INIT_MESSAGE = "Tracing Subscriber Initialized!"


def _log_has_init_message(env):
    logDir = env.cmd("config", "get", "dir")[1]
    logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
    logFilePath = os.path.join(logDir, logFileName)
    try:
        with open(logFilePath) as f:
            return any(INIT_MESSAGE in line for line in f)
    except FileNotFoundError:
        return False


# creates an RLTest Env configured with the given loglevel.
def _env_with_loglevel(level):
    Defaults.loglevel = level
    logDir = os.path.join(Defaults.logdir, f"test_tracing_loglevel_{level}")
    # force a brand-new env bc RLTests default env comparison doesn't include the loglevel
    # also force logs into prefixed logdirs so they don't trample on one another
    return Env(freshEnv=True, logDir=logDir)


# The redis `loglevel` is read at module load and drives the tracing filter.
# The init message is a DEBUG-level log, so it should only show for "debug" or "verbose"
@skip(cluster=True)
def test_initial_level_filtering():
    saved_loglevel = Defaults.loglevel
    try:
        env = _env_with_loglevel("debug")
        env.assertTrue(_log_has_init_message(env), message="debug")
        env.stop()

        env = _env_with_loglevel("verbose")
        env.assertTrue(_log_has_init_message(env), message="verbose")
        env.stop()

        env = _env_with_loglevel("notice")
        env.assertFalse(_log_has_init_message(env), message="notice")
        env.stop()

        env = _env_with_loglevel("warning")
        env.assertFalse(_log_has_init_message(env), message="warning")
        env.stop()
    finally:
        Defaults.loglevel = saved_loglevel


# Each accepted `loglevel` must reload the filter without crashing the module.
def test_loglevel_reload(env):
    for level in ("debug", "verbose", "notice", "warning"):
        env.expect("CONFIG", "SET", "loglevel", level).ok()
    env.assertTrue(env.cmd("PING"))
