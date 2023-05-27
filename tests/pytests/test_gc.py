
from common import *
import platform
from time import sleep


def testBasicGC(env):
    if env.isCluster():
        env.skip()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH',
                         'schema', 'title', 'text', 'id', 'numeric', 't', 'tag'))
    waitForIndex(env, 'idx')
    for i in range(101):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                             'title', 'hello world',
                             'id', '5',
                             't', 'tag1'))

    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [int(i) for i in range(1, 102)])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[int(i) for i in range(1, 102)]])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [int(i) for i in range(1, 102)]]])

    env.assertEqual(env.cmd('ft.del', 'idx', 'doc0'), 1)

    forceInvokeGC(env, 'idx')

    # check that the gc collected the deleted docs
    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [int(i) for i in range(2, 102)])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[int(i) for i in range(2, 102)]])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [int(i) for i in range(2, 102)]]])

def testBasicGCWithEmptyInvIdx(env):
    if env.isCluster():
        env.skip()
    if env.moduleArgs is not None and 'GC_POLICY LEGACY' in env.moduleArgs:
        # this test is not relevent for legacy gc cause its not squeshing inverted index
        env.skip()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text'))
    waitForIndex(env, 'idx')
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                         'title', 'hello world'))

    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1])

    env.assertEqual(env.cmd('ft.del', 'idx', 'doc1'), 1)

    forceInvokeGC(env, 'idx')

    # check that the gc collected the deleted docs
    env.expect('ft.debug', 'DUMP_INVIDX', 'idx', 'world').error().contains('Can not find the inverted index')

def testNumericGCIntensive(env):
    if env.isCluster():
        env.skip()
    NumberOfDocs = 1000
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id', 'numeric'))
    waitForIndex(env, 'idx')

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'id', '1'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        forceInvokeGC(env, 'idx')

    res = env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id')
    for r1 in res:
        for r2 in r1:
            # if r2 is greater then 900 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 900)

def testGeoGCIntensive(env):
    if env.isCluster():
        env.skip()
    NumberOfDocs = 1000
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'g', 'geo'))
    waitForIndex(env, 'idx')

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'g', '12.34,56.78'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        forceInvokeGC(env, 'idx')

    res = env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'g')
    for r1 in res:
        for r2 in r1:
            # if r2 is greater then 900 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 900)

def testTagGC(env):
    if env.isCluster():
        env.skip()
    NumberOfDocs = 101
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'tag'))
    waitForIndex(env, 'idx')

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 't', '1'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        forceInvokeGC(env, 'idx')

    res = env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't')
    for r1 in res:
        for r2 in r1[1]:
            # if r2 is greater then 100 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 100)

def testDeleteEntireBlock(env):
    if env.isCluster():
        env.skip()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'test', 'TEXT', 'SORTABLE', 'test2', 'TEXT', 'SORTABLE', ).ok()
    waitForIndex(env, 'idx')
    # creating 5 blocks on 'checking' inverted index
    for i in range(700):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'test', 'checking', 'test2', 'checking%d' % i).ok()

    # delete docs in the midle of the inverted index, make sure the binary search are not braken
    for i in range(400, 501):
        env.expect('FT.DEL', 'idx', 'doc%d' % i).equal(1)
    res = env.cmd('FT.SEARCH', 'idx', '@test:checking @test2:checking250')
    env.assertEqual(res[0:2],[1, 'doc250'])
    env.assertEqual(set(res[2]), set(['test', 'checking', 'test2', 'checking250']))

    # actually clean the inverted index, make sure the binary search are not braken, check also after rdb reload
    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        forceInvokeGC(env, 'idx')
    for _ in env.reloading_iterator():
        waitForIndex(env, 'idx')
        res = env.cmd('FT.SEARCH', 'idx', '@test:checking @test2:checking250')
        env.assertEqual(res[0:2],[1, 'doc250'])
        env.assertEqual(set(res[2]), set(['test', 'checking', 'test2', 'checking250']))

def testGCIntegrationWithRedisFork(env):
    if env.env == 'existing-env':
        env.skip()
    if env.env == 'enterprise':
        env.skip()
    if env.isCluster():
        env.skip()
    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.expect('FT.CONFIG', 'SET', 'FORKGC_SLEEP_BEFORE_EXIT', '4').ok()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')
    env.expect('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'title', 'hello world').ok()
    env.expect('bgsave').true()
    forceInvokeGC(env, 'idx')
    env.expect('bgsave').true()
    env.cmd('FT.CONFIG', 'SET', 'FORKGC_SLEEP_BEFORE_EXIT', '0')

def testGCThreshold(env):
    if env.env == 'existing-env' or env.isCluster():
        env.skip()

    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 1000')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'title', 'foo').ok()

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    for i in range(999):
        env.expect('FT.DEL', 'idx', 'doc%d' % i).equal(1)

    forceInvokeGC(env, 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo').equal(debug_rep)

    env.expect('FT.DEL', 'idx', 'doc999').equal(1)

    forceInvokeGC(env, 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo').error().contains('Can not find the inverted index')

    # retry with replace
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'title', 'foo').ok()

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    for i in range(999):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'FIELDS', 'title', 'foo1').ok()

    forceInvokeGC(env, 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo').equal(debug_rep)

    env.expect('FT.ADD', 'idx', 'doc999', '1.0', 'REPLACE', 'FIELDS', 'title', 'foo1').ok()

    forceInvokeGC(env, 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo').error().contains('Can not find the inverted index')

    # retry with replace partial

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo1')

    for i in range(999):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'foo2').ok()

    forceInvokeGC(env, 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo1').equal(debug_rep)

    env.expect('FT.ADD', 'idx', 'doc999', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'foo2').ok()

    forceInvokeGC(env, 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo1').error().contains('Can not find the inverted index')

def testGCShutDownOnExit(env):
    if env.env == 'existing-env' or env.env == 'enterprise' or env.isCluster() or platform.system() == 'Darwin':
        env.skip()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env = Env(moduleArgs='GC_POLICY FORK FORKGC_SLEEP_BEFORE_EXIT 20')
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')
    env.expect('FT.DEBUG', 'GC_FORCEBGINVOKE', 'idx').ok()
    env.stop()
    env.start()

    # make sure server started successfully
    env.cmd('flushall')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')

def testGFreeEmpryTerms(env):
    if env.env == 'existing-env' or env.env == 'enterprise' or env.isCluster():
        env.skip()

    env = Env(moduleArgs='GC_POLICY FORK')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()

    for i in range(200):
        env.expect('hset', 'doc%d'%i, 't', 'foo')

    for i in range(200):
        env.expect('del', 'doc%d'%i)

    env.expect('FT.DEBUG', 'DUMP_TERMS', 'idx').equal(['foo'])
    forceInvokeGC(env, 'idx')
    env.expect('FT.DEBUG', 'DUMP_TERMS', 'idx').equal([])

def testAutoMemory_MOD_3951():
    env = Env(moduleArgs='FORK_GC_CLEAN_THRESHOLD 0')
    env.skipOnCluster()
    conn = getConnectionByEnv(env)

    # create index with filter
    conn.execute_command('FT.CREATE', 'idx', 'FILTER', '@t == "5"', 'SCHEMA', 't', 'TEXT')
    # add docs
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', i, 't', i % 10)
    # delete 1 doc and trigger GC
    conn.execute_command('DEL', 0)
    forceInvokeGC(env, 'idx')
    # call alter to trigger rescan
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '2nd', 'TEXT').equal('OK')

    # This test should catch some leaks on the sanitizer
