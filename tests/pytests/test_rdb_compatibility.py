import os
import subprocess
import redis
from includes import *
from common import *
from RLTest import Env

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

@skip(cluster=True)
def testRDBCompatibility(env):
    # temp skip for out-of-index

    env = Env(moduleArgs='UPGRADE_INDEX idx; PREFIX 1 tt; LANGUAGE french; LANGUAGE_FIELD MyLang; SCORE 0.5; SCORE_FIELD MyScore; PAYLOAD_FIELD MyPayload; UPGRADE_INDEX idx1')
    # env = Env(moduleArgs=['UPGRADE_INDEX idx', 'PREFIX 1 tt', 'LANGUAGE french', 'LANGUAGE_FIELD MyLang', 'SCORE 0.5', 'SCORE_FIELD MyScore', 'PAYLOAD_FIELD MyPayload', 'UPGRADE_INDEX idx1'])
    # env = Env(moduleArgs=['UPGRADE_INDEX idx; PREFIX 1 tt; LANGUAGE french', 'LANGUAGE_FIELD MyLang', 'SCORE 0.5', 'SCORE_FIELD MyScore', 'PAYLOAD_FIELD MyPayload', 'UPGRADE_INDEX idx1'])
    skipOnExistingEnv(env)
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    if not getRDBFiles(env, RDBS):
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
        env.assertEqual(res['index_definition'], ['key_type', 'HASH', 'prefixes', ['tt'], 'default_language', 'french', 'language_field', 'MyLang', 'default_score', '0.5', 'score_field', 'MyScore', 'payload_field', 'MyPayload', 'indexes_all', 'false'])
        env.assertEqual(res['num_docs'], 1000)
        env.expect('FT.SEARCH', 'idx', 'Short', 'LIMIT', '0', '0').equal([943])
        if fileName == 'redisearch_1.6.13_with_synonyms.rdb':
            res = env.cmd('FT.SYNDUMP idx')
            res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
            env.assertEqual(res, {'term2': ['0'], 'term1': ['0']})
        env.cmd('flushall')
        env.assertTrue(env.checkExitCode(), message=fileName)

@skip(cluster=True)
def testRDBCompatibility_vecsim():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 MIN_OPERATION_WORKERS 0')
    skipOnExistingEnv(env)
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)

    rdbs = ['redisearch_2.4.14_with_vecsim.rdb',
            'redisearch_2.6.9_with_vecsim.rdb',
            'redisearch_8.0_with_vecsim.rdb']

    algorithms = ['FLAT', 'HNSW']
    if not getRDBFiles(env, rdbs):
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

        vec_fields = [alg.lower() + '_vec' for alg in algorithms]
        for vec_field in vec_fields:
            env.expect('FT.SEARCH', 'idx', f'*=>[KNN 1000 @{vec_field} $b]', 'PARAMS', '2', 'b', '<<????>>', 'LIMIT', '0', '0').equal([100])
        env.expect('DBSIZE').equal(100)
        res = to_dict(env.cmd('FT.INFO idx'))
        env.assertEqual(res['num_docs'], 100)
        env.assertEqual(res['hash_indexing_failures'], 0)

        expected_attr_info = [[
          'identifier', 'hnsw_vec',
          'attribute', 'hnsw_vec',
          'type', 'VECTOR',
          'algorithm', 'HNSW',
          'data_type', 'FLOAT32',
          'dim', 2,
          'distance_metric', 'L2',
          'M', 16,
          'ef_construction', 200,
          'ef_runtime', 10
        ], [
          'identifier', 'flat_vec',
          'attribute', 'flat_vec',
          'type', 'VECTOR',
          'algorithm', 'FLAT',
          'data_type', 'FLOAT32',
          'dim', 2,
          'distance_metric', 'L2',
        ]]
        assertInfoField(env, 'idx', 'attributes', expected_attr_info)

        env.cmd('flushall')
        env.assertTrue(env.checkExitCode())


@skip(cluster=True)
def testRDBOldVersionIndexScopedToDb0(env):
    """Indexes from an RDB written before INDEX_DB_ID_VERSION (28) carry no dbid,
    so they must load bound to DB 0. This loads such an RDB (normal, non-legacy
    IndexSpec_RdbLoad path) and asserts the index is queryable on DB 0 and, per
    per-DB scoping, NOT visible from a non-zero DB - i.e. it loaded as DB 0, not
    as a global/DB-agnostic index."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2 MIN_OPERATION_WORKERS 0')
    skipOnExistingEnv(env)
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)

    # Most recent pre-v28 RDB; loads via the normal (event-based) RdbLoad path.
    rdbs = ['redisearch_8.0_with_vecsim.rdb']
    if not getRDBFiles(env, rdbs):
        return

    env.stop()
    filePath = os.path.join(REDISEARCH_CACHE_DIR, rdbs[0])
    try:
        os.unlink(rdbFilePath)
    except OSError:
        pass
    os.symlink(filePath, rdbFilePath)
    env.start()
    waitForIndex(env, 'idx')

    # Loaded on DB 0 (the env's default connection) and queryable there.
    list0 = env.cmd('FT._LIST')
    env.assertTrue('idx' in list0, message=f'DB0 FT._LIST={list0}')
    env.expect('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0').equal([100])

    # Bound to DB 0 only: invisible/unqueryable from a non-zero DB.
    port = env.getConnection().connection_pool.connection_kwargs['port']
    db14 = redis.Redis(host='localhost', port=port, db=14, decode_responses=True)
    try:
        list14 = db14.execute_command('FT._LIST')
        env.assertTrue('idx' not in list14, message=f'DB14 FT._LIST={list14}')
        try:
            r = db14.execute_command('FT.SEARCH', 'idx', '*')
            env.assertTrue(False, message=f'DB14 FT.SEARCH idx unexpectedly returned {r}')
        except redis.ResponseError:
            pass
    finally:
        db14.close()

    env.cmd('flushall')
    env.assertTrue(env.checkExitCode())
