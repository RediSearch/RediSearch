from common import *
from RLTest import Env

@skip(cluster=True, no_json=True, asan=True)
def test_loading_rdb_without_aux_data(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.DROPINDEX', 'idx').equal('OK')
    # Save state to RDB
    env.stop()
    # Restart without modules. 
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')
    # Attempt to load RDB should work because the RDB
    # does not contains module aux data
    env.start()
    env.expect('HGET', 'doc1', 't').equal('hello')

@skip(cluster=True, no_json=True, asan=True)
def test_loading_rdb_with_aux_data(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])
    # Save state to RDB
    env.stop()
    # Restart without modules. 
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')
    # Attempt to load RDB should fail because the RDB contains module aux data
    try:
        env.start()
    except Exception as e:
        expected_msg = 'Redis server is dead'
        env.assertContains(expected_msg, str(e))
        if expected_msg not in str(e):
            raise e
    finally:
        env.assertFalse(env.isUp()) 



