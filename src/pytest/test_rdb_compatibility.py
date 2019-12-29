import os
import subprocess
from includes import *


REDISEARCH_CACHE_DIR = '/tmp/'
BASE_RDBS_URL = 'https://s3.amazonaws.com/redismodules/redisearch-enterprise/rdbs/'

RDBS = [
    'redisearch_1.2.0.rdb',
    'redisearch_1.4.0.rdb',
    'redisearch_1.4.6.rdb',
    'redisearch_1.4.11.rdb'
]

def downloadFiles():
    if not os.path.exists(REDISEARCH_CACHE_DIR):
        os.makedirs(REDISEARCH_CACHE_DIR)
    for f in RDBS:
        path = os.path.join(REDISEARCH_CACHE_DIR, f)
        if not os.path.exists(path):
            subprocess.call(['wget', BASE_RDBS_URL + f, '-O', path])
        if not os.path.exists(path):
            return False
    return True

def testRDBCompatibility(env):
    env.skipOnCluster()
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    if not downloadFiles():
        if os.environ.get('CI'):
            env.assertTrue(False)  ## we could not download rdbs and we are running on CI, let fail the test
        else:
            env.skip()
            return

    for fileName in RDBS:
        env.stop()
        filePath = os.path.join(REDISEARCH_CACHE_DIR, fileName)
        try:
            os.unlink(rdbFilePath)
        except OSError:
            pass
        os.symlink(filePath, rdbFilePath)
        env.start()
        env.cmd('flushall')
        env.assertTrue(env.checkExitCode())

if __name__ == "__main__":
    if not downloadFiles():
        raise Exception("Couldn't download RDB files")
    print("RDB Files ready for testing!")