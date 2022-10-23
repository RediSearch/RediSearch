import os
import subprocess
from includes import *
from common import *
from RLTest import Env


REDISEARCH_CACHE_DIR = '/tmp/rdbcompat'

RDBS = [
    'redisearch_1.2.0.rdb',
    'redisearch_1.4.0.rdb',
    'redisearch_1.4.6.rdb',
    'redisearch_1.4.11.rdb',
    'redisearch_1.6.13.rdb',
    'redisearch_1.6.13_with_synonyms.rdb',
    'redisearch_1.8.1.rdb',
    'redisearch_2.0.9.rdb'
]

def downloadFiles(rdbs = None):
    rdbs = RDBS if rdbs is None else rdbs
    if not os.path.exists(REDISEARCH_CACHE_DIR):
        os.makedirs(REDISEARCH_CACHE_DIR)
    for f in rdbs:
        path = os.path.join(REDISEARCH_CACHE_DIR, f)
        if not os.path.exists(path):
            dpath = paella.wget(BASE_RDBS_URL + f, dest=path)
        if not os.path.exists(path):
            return False
    return True

@unstable
def testRDBCompatibility(env):
    # temp skip for out-of-index

    env = Env(moduleArgs='UPGRADE_INDEX idx; PREFIX 1 tt; LANGUAGE french; LANGUAGE_FIELD MyLang; SCORE 0.5; SCORE_FIELD MyScore; PAYLOAD_FIELD MyPayload; UPGRADE_INDEX idx1')
    # env = Env(moduleArgs=['UPGRADE_INDEX idx', 'PREFIX 1 tt', 'LANGUAGE french', 'LANGUAGE_FIELD MyLang', 'SCORE 0.5', 'SCORE_FIELD MyScore', 'PAYLOAD_FIELD MyPayload', 'UPGRADE_INDEX idx1'])
    # env = Env(moduleArgs=['UPGRADE_INDEX idx; PREFIX 1 tt; LANGUAGE french', 'LANGUAGE_FIELD MyLang', 'SCORE 0.5', 'SCORE_FIELD MyScore', 'PAYLOAD_FIELD MyPayload', 'UPGRADE_INDEX idx1'])

    env.skipOnCluster()
    skipOnExistingEnv(env)
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    if not downloadFiles():
        if CI:
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
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH idx * LIMIT 0 0').equal([1000])
        env.expect('DBSIZE').equal(1000)
        res = env.cmd('FT.INFO idx')
        res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        env.assertEqual(res['index_definition'], ['key_type', 'HASH', 'prefixes', ['tt'], 'default_language', 'french', 'language_field', 'MyLang', 'default_score', '0.5', 'score_field', 'MyScore', 'payload_field', 'MyPayload'])
        env.assertEqual(res['num_docs'], '1000')
        env.expect('FT.SEARCH', 'idx', 'Short', 'LIMIT', '0', '0').equal([943])
        if fileName == 'redisearch_1.6.13_with_synonyms.rdb':
            res = env.cmd('FT.SYNDUMP idx')
            res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
            env.assertEqual(res, {'term2': ['0'], 'term1': ['0']})
        env.cmd('flushall')
        env.assertTrue(env.checkExitCode())

def testRDBCompatibility_vecsim():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    env.skipOnCluster()
    skipOnExistingEnv(env)
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)

    rdbs = ['redisearch_2.4.14_with_vecsim.rdb']
    algorithms = ['FLAT', 'HNSW']
    if not downloadFiles(rdbs):
        if CI:
            env.assertTrue(False)  ## we could not download rdbs and we are running on CI, let fail the test
        else:
            env.skip()
            return

    for fileName in rdbs:
        env.stop()
        filePath = os.path.join(REDISEARCH_CACHE_DIR, fileName)
        try:
            os.unlink(rdbFilePath)
        except OSError:
            pass
        os.symlink(filePath, rdbFilePath)
        env.start()
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH idx * LIMIT 0 0').equal([100])
        for vec_field in [alg.lower() + '_vec' for alg in algorithms]:
            env.expect('FT.SEARCH', 'idx', f'*=>[KNN 1000 @{vec_field} $b]', 'PARAMS', '2', 'b', '<<????>>', 'LIMIT', '0', '0').equal([100])
        env.expect('DBSIZE').equal(100)
        res = to_dict(env.cmd('FT.INFO idx'))
        env.assertEqual(res['num_docs'], '100')
        env.assertEqual(res['hash_indexing_failures'], '0')

        env.cmd('flushall')
        env.assertTrue(env.checkExitCode())

if __name__ == "__main__":
    if not downloadFiles():
        raise Exception("Couldn't download RDB files")
    print("RDB Files ready for testing!")
