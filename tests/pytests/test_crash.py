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

def expect_query_crash_output(env, lines):
    logDir = env.cmd('config', 'get', 'dir')[1]
    logFileName = env.cmd('CONFIG', 'GET', 'logfile')[1]
    logFilePath = os.path.join(logDir, logFileName)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT')
    runDebugQueryCommandAndCrash(env, ['FT.SEARCH', 'idx', '*'])
    pos = 0
    with open(logFilePath) as logFile:
        for line in logFile:
            if pos == len(lines):
                break
            if line.find(lines[pos]) != -1:
                pos += 1
            elif pos != 0 and pos < len(lines):
                return False
    return pos == len(lines)

# we expect to see out index information about the crash in the log file
@skip(cluster=True)
def test_query_thread_crash():
    env = CrashingEnv(testName="test_query_thread_crash", freshEnv=True)
    env.assertTrue(expect_query_crash_output(env, ['search_current_thread', 'search_index:idx']))
