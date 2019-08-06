import os
import subprocess
from shutil import copyfile

REDISEARCH_CACHE_DIR = '/tmp/'
BASE_RDBS_URL = 'https://s3.amazonaws.com/redismodules/redisearch-enterprise/rdbs/'

def testRDBCompatibility(env):
    env.skipOnCluster()
    filesNames = [
            'redisearch_1.2.0.rdb',
            'redisearch_1.4.0.rdb',
            'redisearch_1.4.6.rdb',
            'redisearch_1.4.11.rdb'
        ]
    
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    
    rdbFilePath = os.path.join(dbDir, dbFileName) 
    
    if not os.path.exists(REDISEARCH_CACHE_DIR):
        os.makedirs(REDISEARCH_CACHE_DIR)
    
    for fileName in filesNames:
        env.stop()
        filePath = os.path.join(REDISEARCH_CACHE_DIR, fileName)
        if not os.path.exists(filePath):
            subprocess.call(['wget', BASE_RDBS_URL + fileName, '-O', filePath])
        if not os.path.exists(filePath):
            if os.environ.get('CI') is None:
                env.skip()
            env.assertTrue(False) ## we could not download rdbs and we are running on CI, let fail the test
        copyfile(filePath, rdbFilePath)  
        env.start()
        env.cmd('flushall')
        env.assertTrue(env.checkExitCode())
    