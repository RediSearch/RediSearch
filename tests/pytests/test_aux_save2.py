import subprocess
from common import *
from RLTest import Env

RDBS = [
    'redisearch_2.8.18_empty_dict.rdb',
    'redisearch_2.10.7_empty_dict.rdb',
]

def _removeModuleArgs(env: Env):
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithoutIndexAuxData(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.DROPINDEX', 'idx').equal('OK')
    # Save state to RDB
    env.stop()
    _removeModuleArgs(env)
    # Attempt to load RDB should work because the RDB
    # does not contains module aux data
    env.start()
    env.expect('HGET', 'doc1', 't').equal('hello')

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithIndexAuxData(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])
    # Save state to RDB
    env.stop()
    # Restart without modules
    _removeModuleArgs(env)
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

@skip(cluster=True, asan=True)
def testLoadRdbWithIndexAuxDataUsingModules(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])
    # Save state to RDB
    env.stop()
    # Restart with modules
    env.start()
    # doc1 should exist
    env.expect('HGET', 'doc1', 't').equal('hello')
    # idx should exist
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithoutSpellcheckDictAuxData(env: Env):
    env.expect('FT.DICTADD', 'dict', 'bar', 'baz', 'hakuna matata').equal(3)
    env.expect('HSET', 'doc1', 't', 'lion').equal(1)
    res = env.cmd('FT.DICTDUMP', 'dict')
    env.assertEqual(res, ['bar', 'baz', 'hakuna matata'])
    env.expect('FT.DICTDEL', 'dict', 'bar', 'baz', 'hakuna matata').equal(3)
    # Save state to RDB
    env.stop()
    # Restart without modules
    _removeModuleArgs(env)
    # Attempt to load RDB should work because the RDB
    # does not contains module aux data
    env.start()
    env.expect('HGET', 'doc1', 't').equal('lion')

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithSpellcheckDictAuxData(env: Env):
    # Create dict and add items
    env.expect('FT.DICTADD', 'dict', 'bar', 'baz', 'hakuna matata').equal(3)
    env.expect('HSET', 'doc1', 't', 'lion').equal(1)
    res = env.cmd('FT.DICTDUMP', 'dict')
    env.assertEqual(res, ['bar', 'baz', 'hakuna matata'])
    # Save state to RDB
    env.stop()
    # Restart without modules
    _removeModuleArgs(env)
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

@skip(cluster=True, asan=True)
def testLoadRdbWithSpellcheckDictAuxDataUsingModules(env: Env):
    # Create dict1 and add items
    env.expect('FT.DICTADD', 'dict1', 'bar', 'baz', 'hakuna matata').equal(3)
    env.expect('HSET', 'doc1', 't', 'lion').equal(1)
    res = env.cmd('FT.DICTDUMP', 'dict1')
    env.assertEqual(res, ['bar', 'baz', 'hakuna matata'])
    # Create dict2, add an item, and delete it
    env.expect('FT.DICTADD', 'dict2', 'foo').equal(1)
    res = env.cmd('FT.DICTDUMP', 'dict2')
    env.assertEqual(res, ['foo'])
    env.expect('FT.DICTDEL', 'dict2', 'foo').equal(1)
    env.expect('FT.DICTDUMP', 'dict2').equal([])
    # Save state to RDB
    env.stop()
    # Restart with modules
    env.start()
    # doc1 should exist
    env.expect('HGET', 'doc1', 't').equal('lion')
    # dict1 should exist
    res = env.cmd('FT.DICTDUMP', 'dict1')
    env.assertEqual(res, ['bar', 'baz', 'hakuna matata'])
    # dict2 does not exist, but FT.DICTDUMP returns an empty list
    env.expect('FT.DICTDUMP', 'dict2').equal([])

@skip(cluster=True)
def testLoadRdbWithEmptySpellcheckDict(env):
    # Test loading an RDB with 3 dictionaries:
    # empty_dict1 and empty_dict2 are empty dictionaries
    # dict is a non-empty dictionary, containing two items: ['hello', 'hola']

    env = Env()
    skipOnExistingEnv(env)
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    if not downloadFiles(RDBS):
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

        # Check that the non-empty dictionary is loaded
        res = env.cmd('FT.DICTDUMP', 'dict')
        env.assertEqual(res, ['hello', 'hola'])

        # File size after saving the RDB is smaller than the original file size
        # because the empty dictionaries are not saved
        filesize = os.path.getsize(rdbFilePath)
        env.assertEqual(filesize, 205)
        env.cmd('SAVE')
        filesize_after_save = os.path.getsize(rdbFilePath)
        env.assertGreater(filesize, filesize_after_save)

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithoutSuggestionData(env: Env):
    env.expect('FT.SUGADD', 'sug', 'hakuna matata', '1').equal(1)
    env.expect('FT.SUGADD', 'sug', 'hakuna', '1').equal(2)
    env.expect('HSET', 'doc1', 't', 'lion').equal(1)
    res = env.cmd('FT.SUGGET', 'sug', 'hakuna')
    env.assertEqual(res, ['hakuna', 'hakuna matata'])
    env.expect('FT.SUGDEL', 'sug', 'hakuna matata').equal(1)
    env.expect('FT.SUGLEN', 'sug').equal(1)
    env.expect('FT.SUGDEL', 'sug', 'hakuna').equal(1)
    env.expect('FT.SUGLEN', 'sug').equal(0)
    # sug should not exist, the key is deleted when the last item is removed
    env.expect('EXISTS', 'sug').equal(0)
    # Save state to RDB
    env.stop()
    # Restart without modules
    _removeModuleArgs(env)
    # Attempt to load RDB should work because the RDB does not contain
    # empty suggestion data
    env.start()
    env.expect('HGET', 'doc1', 't').equal('lion')

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithSuggestionData(env: Env):
    # Create suggestion dict and add items
    env.expect('FT.SUGADD', 'sug', 'hakuna matata', '1').equal(1)
    env.expect('FT.SUGADD', 'sug', 'hakuna', '1').equal(2)
    env.expect('HSET', 'doc1', 't', 'lion').equal(1)
    res = env.cmd('FT.SUGGET', 'sug', 'hakuna')
    env.assertEqual(res, ['hakuna', 'hakuna matata'])
    # Save state to RDB
    env.stop()
    _removeModuleArgs(env)
    # Attempt to load RDB should fail because the RDB contains module data
    try:
        env.start()
    except Exception as e:
        expected_msg = 'Redis server is dead'
        env.assertContains(expected_msg, str(e))
        if expected_msg not in str(e):
            raise e
    finally:
        env.assertFalse(env.isUp())

@skip(cluster=True, asan=True)
def testLoadRdbWithSuggestionDataUsingModules(env: Env):
    # Create dict1 and add items
    env.expect('FT.SUGADD', 'sug1', 'hakuna matata', '1').equal(1)
    env.expect('FT.SUGADD', 'sug1', 'hakuna', '1').equal(2)
    env.expect('HSET', 'doc1', 't', 'lion').equal(1)
    res = env.cmd('FT.SUGGET', 'sug1', 'hakuna')
    env.assertEqual(res, ['hakuna', 'hakuna matata'])
    # Create sug2, add an item, and delete it
    env.expect('FT.SUGADD', 'sug2', 'hello world', '1').equal(1)
    res = env.cmd('FT.SUGGET', 'sug2', 'hello')
    env.assertEqual(res, ['hello world'])
    env.expect('FT.SUGLEN', 'sug2').equal(1)
    env.expect('FT.SUGDEL', 'sug2', 'hello world').equal(1)
    env.expect('FT.SUGGET', 'sug2', 'hello').equal([])
    env.expect('FT.SUGLEN', 'sug2').equal(0)
    # sug2 should not exist, the key is deleted when the last item is removed
    env.expect('EXISTS', 'sug2').equal(0)
    # Save state to RDB
    env.stop()
    # Restart with modules
    env.start()
    # doc1 should exist
    env.expect('HGET', 'doc1', 't').equal('lion')
    # dict1 should exist
    res = env.cmd('FT.SUGGET', 'sug1', 'hakuna')
    env.assertEqual(res, ['hakuna', 'hakuna matata'])
    # dict2 does not exist, FT.SUGGET returns an empty list
    env.expect('EXISTS', 'sug2').equal(0)
    env.expect('FT.SUGGET', 'sug2', 'hello').equal([])
    env.expect('FT.SUGLEN', 'sug2').equal(0)
