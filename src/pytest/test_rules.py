from RLTest import Env
import pprint
import time
import utils

def testCreateRules(env):
    env.cmd('ft.create', 'idx', 'EXPRESSION',
            'hasprefix("user:")||@year>2015',
            'SCHEMA', 'f1', 'text')

    env.cmd('hset', 'user:mnunberg', 'foo', 'bar')
    env.cmd('hset', 'user:mnunberg', 'f1', 'hello world')

    print env.cmd('ft.search', 'idx', 'hello')
    env.cmd('del', 'user:mnunberg')
    print env.cmd('ft.search', 'idx', 'hello')

    env.cmd('hset', 'someDoc', 'year', '2019')
    env.cmd('hset', 'someDoc', 'name', 'mark')
    env.cmd('hset', 'someDoc', 'f1', 'goodbye')

    print env.cmd('ft.search', 'idx', 'goodbye')

def testScanRules(env):
    for x in range(10000):
        env.cmd('hset', 'doc{}'.format(x), 'f1', 'hello')
    
    # One document that should not match..
    env.cmd('hset', 'dummy', 'f1', 'hello')

    env.cmd('ft.create', 'idx', 'EXPRESSION', 'hasprefix("doc")', 'schema', 'f1', 'text')

    # print("SCANSTART")
    # env.cmd('ft.scanstart')
    while True:
        if utils.is_synced(env):
            break
        time.sleep(0.01)
    rv = env.cmd('ft.search', 'idx', 'hello')
    env.assertEqual(10000, rv[0])  # Order can be different!

def testPersistence(env):
    env.cmd('ft.create', 'idx', 'EXPRESSION', "hasprefix('doc')", 'SCHEMA', 'f1', 'TEXT')
    utils.dump_and_reload(env)

    env.cmd('hset', 'doc1', 'f1', 'hello world')
    rv = env.cmd('ft.search', 'idx', 'hello')
    env.assertEqual([1L, 'doc1', ['f1', 'hello world']], rv)