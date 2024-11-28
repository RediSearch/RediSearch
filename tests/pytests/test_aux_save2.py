from common import *
from RLTest import Env

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithoutIndexAuxData(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.DROPINDEX', 'idx').equal('OK')
    # Save state to RDB
    env.stop()
    # Restart without modules
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
def testLoadRdbWithIndexAuxData(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    env.expect('HSET', 'doc1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc1'])
    # Save state to RDB
    env.stop()
    # Restart without modules
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

@skip(cluster=True, no_json=True, asan=True)
def testLoadRdbWithoutSuggestionData(env: Env):
    env.expect('FT.SUGADD', 'sug', 'hakuna matata', '1').equal(1)
    env.expect('FT.SUGADD', 'sug', 'hakuna', '1').equal(2)
    env.expect('HSET', 'doc1', 't', 'lion').equal(1)
    res = env.cmd('FT.SUGGET', 'sug', 'hakuna')
    env.assertEqual(res, ['hakuna', 'hakuna matata'])
    env.expect('FT.SUGDEL', 'sug', 'hakuna matata').equal(1)
    env.expect('FT.SUGDEL', 'sug', 'hakuna').equal(1)
    env.expect('FT.SUGLEN', 'sug').equal(0)
    env.expect('EXISTS', 'sug').equal(0)
    # Save state to RDB
    env.stop()
    # Restart without modules
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')
    # Attempt to load RDB should work because the RDB
    # does not contains module specific data
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
    # Restart without modules
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')
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
    env.expect('FT.SUGDEL', 'sug2', 'hello world').equal(1)
    env.expect('FT.SUGGET', 'sug2', 'hello').equal([])
    env.expect('FT.SUGLEN', 'sug2').equal(0)
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
    # dict2 does not exist, but FT.SUGGET returns an empty list
    env.expect('FT.SUGGET', 'sug2', 'hello').equal([])
    env.expect('FT.SUGLEN', 'sug2').equal(0)