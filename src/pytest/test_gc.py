
import unittest
from RLTest import Env
import platform
from includes import *


def testBasicGC(env):
    if env.isCluster():
        raise unittest.SkipTest()
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 'title', 'text', 'id', 'numeric', 't', 'tag'))
    for i in range(101):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                             'title', 'hello world',
                             'id', '5',
                             't', 'tag1'))

    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [long(i) for i in range(1, 102)])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[long(i) for i in range(1, 102)]])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [long(i) for i in range(1, 102)]]])

    env.assertEqual(env.cmd('ft.del', 'idx', 'doc0'), 1)

    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    # check that the gc collected the deleted docs
    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [long(i) for i in range(2, 102)])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[long(i) for i in range(2, 102)]])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [long(i) for i in range(2, 102)]]])

def testBasicGCWithEmptyInvIdx(env):
    if env.isCluster():
        raise unittest.SkipTest()
    if env.moduleArgs is not None and 'GC_POLICY LEGACY' in env.moduleArgs:
        # this test is not relevent for legacy gc cause its not squeshing inverted index
        raise unittest.SkipTest()
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 'title', 'text'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                         'title', 'hello world'))

    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1])

    env.assertEqual(env.cmd('ft.del', 'idx', 'doc1'), 1)

    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    # check that the gc collected the deleted docs
    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [])


def testNumerciGCIntensive(env):
    if env.isCluster():
        raise unittest.SkipTest()
    NumberOfDocs = 1000
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 'id', 'numeric'))

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'id', '1'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    res = env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id')
    for r1 in res:
        for r2 in r1:
            # if r2 is greater then 900 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 900)


def testTagGC(env):
    if env.isCluster():
        raise unittest.SkipTest()
    NumberOfDocs = 101
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 't', 'tag'))

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 't', '1'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    res = env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't')
    for r1 in res:
        for r2 in r1[1]:
            # if r2 is greater then 100 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 100)


def testDeleteEntireBlock(env):
    if env.isCluster():
        raise unittest.SkipTest()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE', 'test2', 'TEXT', 'SORTABLE', ).ok()
    # creating 5 blocks on 'checking' inverted index
    for i in range(700):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'test', 'checking', 'test2', 'checking%d' % i).ok()

    # delete docs in the midle of the inverted index, make sure the binary search are not braken
    for i in range(400, 501):
        env.expect('FT.DEL', 'idx', 'doc%d' % i).equal(1)
    env.expect('FT.SEARCH', 'idx', '@test:checking @test2:checking250').equal([1L, 'doc250', ['test', 'checking', 'test2', 'checking250']])

    # actually clean the inverted index, make sure the binary search are not braken, check also after rdb reload
    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')
    for _ in env.reloading_iterator():
        env.expect('FT.SEARCH', 'idx', '@test:checking @test2:checking250').equal([1L, 'doc250', ['test', 'checking', 'test2', 'checking250']])


def testDeleteDocWithGoeField(env):
    if env.isCluster():
        raise unittest.SkipTest()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE', 'test2', 'GEO').ok()
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'checking', 'test2', '1,1').ok()
    env.expect('zrange', 'geo:idx/test2', '0', '-1').equal(['1'])
    env.expect('FT.DEL', 'idx', 'doc1').equal(1)
    rv = env.cmd('zrange', 'geo:idx/test2', '0', '-1')
    # On newer redis versions, this is a NULL instead of an empty array
    env.assertFalse(bool(rv))

def testGCIntegrationWithRedisFork(env):
    if env.env == 'existing-env':
        env.skip()
    if env.env == 'enterprise':
        env.skip()
    if env.isCluster():
        raise unittest.SkipTest()
    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        raise unittest.SkipTest()
    env.expect('FT.CONFIG', 'SET', 'FORKGC_SLEEP_BEFORE_EXIT', '4').ok()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    env.expect('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'title', 'hello world').ok()
    env.expect('bgsave').equal('Background saving started')
    env.cmd('FT.DEBUG', 'GC_FORCEINVOKE', 'idx')
    env.expect('bgsave').equal('Background saving started')
    env.cmd('FT.CONFIG', 'SET', 'FORKGC_SLEEP_BEFORE_EXIT', '0')

def testGCThreshold(env):
    if env.env == 'existing-env':
        env.skip()
    if env.isCluster():
        raise unittest.SkipTest()

    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 1000')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'title', 'foo').ok()

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    for i in range(999):
        env.expect('FT.DEL', 'idx', 'doc%d' % i).equal(1)

    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo').equal(debug_rep)

    env.expect('FT.DEL', 'idx', 'doc999').equal(1)

    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    env.assertEqual(len(debug_rep), 0)

    # retry with replace
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'title', 'foo').ok()

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    for i in range(999):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'FIELDS', 'title', 'foo1').ok()

    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo').equal(debug_rep)

    env.expect('FT.ADD', 'idx', 'doc999', '1.0', 'REPLACE', 'FIELDS', 'title', 'foo1').ok()

    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    env.assertEqual(len(debug_rep), 0)

    # retry with replace partial

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    for i in range(999):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'foo2').ok()

    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo').equal(debug_rep)

    env.expect('FT.ADD', 'idx', 'doc999', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'foo1').ok()

    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    debug_rep = env.cmd('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'foo')

    env.assertEqual(len(debug_rep), 0)

def testGCShutDownOnExit(env):
    if env.env == 'existing-env' or env.env == 'enterprise' or env.isCluster() or platform.system() == 'Darwin':
        env.skip()
    env = Env(moduleArgs='GC_POLICY FORK FORKGC_SLEEP_BEFORE_EXIT 20')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    env.expect('FT.DEBUG', 'GC_FORCEBGINVOKE', 'idx').ok()
    env.stop()
    env.start()

    # make sure server started successfully
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
