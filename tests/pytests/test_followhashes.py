# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env

def testSyntax1(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx',
               'ONfoo*',
               'SCHEMA', 'foo', 'text').error().contains('Unknown argument `ONfoo*`')

    env.expect('ft.create', 'idx2',
               'LANGUAGE', 'eng'
               'SCHEMA', 'foo', 'text').equal('SEARCH_ADD_ARGS Invalid language')

    env.expect('ft.create', 'idx2',
               'SCORE', '1.0'
               'SCHEMA', 'foo', 'text').error().contains('Unknown argument `foo`')

    env.expect('ft.create', 'idx2',
               'PAYLOAD_FIELD', 'awfw'
               'SCHEMA', 'foo', 'text').error().contains('Unknown argument `foo`')

    env.expect('ft.create', 'idx2',
               'FILTER', 'a'
               'SCHEMA', 'foo', 'text').equal("SEARCH_EXPR Unknown symbol 'aSCHEMA'")

def testPrefix0a(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', '',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1, 'thing:bar', ['name', 'foo']])

def testPrefix0b(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH', 'SCHEMA', 'name', 'text')
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1, 'thing:bar', ['name', 'foo']])

def testPrefix1(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['name', 'foo']])

def testPrefix2(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '2', 'this:', 'that:',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'this:foo', 'name', 'foo')
    conn.execute_command('hset', 'that:foo', 'name', 'foo')

    res = env.cmd('ft.search', 'things', 'foo')
    env.assertContains('that:foo', res)
    env.assertContains('this:foo', res)

def testFlushallManyPrefixes(env):
    conn = getConnectionByEnv(env)

    # This test purpose it to validate the cleanup of the spec:prefixes dictionary upon
    # server 'flushall'
    num_indices = 100
    for i in range(num_indices):
        env.cmd('ft.create', i, 'ON', 'HASH',
                            'PREFIX', '1', i,
                            'SCHEMA', 'name', 'text')

    # Sanity check
    dump_trie = to_dict(env.cmd(debug_cmd(), "DUMP_PREFIX_TRIE"))
    env.assertEqual(dump_trie['prefixes_count'], num_indices)

    conn.execute_command('FLUSHALL')
    # Verify the global prefixes trie is empty
    dump_trie = to_dict(env.cmd(debug_cmd(), "DUMP_PREFIX_TRIE"))
    env.assertEqual(dump_trie['prefixes_count'], 0)
    env.assertEqual(dump_trie['prefixes_trie_nodes'], 0)

def testPrefix3(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'stuff',
            'ON', 'HASH',
            'PREFIX', '1', 'stuff:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    conn.execute_command('hset', 'object:jojo', 'name', 'vivi')
    conn.execute_command('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['name', 'foo', 'age', '42']])

def testDel(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.expect('ft.search', 'things', 'foo').equal([0])
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1, 'thing:bar', ['name', 'foo']])
    conn.execute_command('del', 'thing:bar')
    env.expect('ft.search', 'things', 'foo').equal([0])

def testSet(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.expect('ft.search', 'things', 'foo').equal([0])
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1, 'thing:bar', ['name', 'foo']])
    env.assertEqual(index_info(env, 'things')['num_docs'], 1)
    env.expect('set', 'thing:bar', "bye bye")
    env.expect('ft.search', 'things', 'foo').equal([0])
    env.assertEqual(index_info(env, 'things')['num_docs'], 0)

@skip(cluster=True)
def testRename(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create things PREFIX 1 thing: SCHEMA name text')
    env.expect('ft.search things foo').equal([0])

    conn.execute_command('hset thing:bar name foo')
    env.expect('ft.search things foo').equal([1, 'thing:bar', ['name', 'foo']])

    env.expect('RENAME thing:bar thing:foo').ok()
    env.expect('ft.search things foo').equal([1, 'thing:foo', ['name', 'foo']])

    env.cmd('ft.create otherthings PREFIX 1 otherthing: SCHEMA name text')
    env.expect('RENAME thing:foo otherthing:foo').ok()
    env.expect('ft.search things foo').equal([0])
    env.expect('ft.search otherthings foo').equal([1, 'otherthing:foo', ['name', 'foo']])

    # Test that renaming a String key (unrelated type) does not crash
    env.cmd('SET foo bar')
    env.cmd('RENAME foo fubu')

@skip(cluster=True)
def testCopy(env):
    if not server_version_at_least(env, "6.2.0"):
        env.skip()
    conn = getConnectionByEnv(env)

    conn.execute_command('ft.create', 'things', 'SCHEMA', 'name', 'text')
    env.expect('FT.SEARCH', 'things', 'foo').equal([0])

    conn.execute_command('hset', '1', 'name', 'foo')
    env.expect('FT.SEARCH', 'things', 'foo').equal([1, '1', ['name', 'foo']])

    # copy key to a non existing key
    env.expect('COPY', '1', '2').equal(1)
    env.expect('FT.SEARCH', 'things', 'foo').equal([2, '1', ['name', 'foo'], '2', ['name', 'foo']])

    conn.execute_command('hset', '2', 'name', 'bar')
    env.expect('FT.SEARCH', 'things', 'foo').equal([1, '1', ['name', 'foo']])

    # copy key to an existing key
    env.expect('COPY', '1', '2').equal(0)
    env.expect('FT.SEARCH', 'things', 'foo').equal([1, '1', ['name', 'foo']])

    # copy key to an existing key with replace
    env.expect('COPY', '1', '2', 'REPLACE').equal(1)
    env.expect('FT.SEARCH', 'things', 'foo').equal([2, '1', ['name', 'foo'], '2', ['name', 'foo']])

    # replace with non hash key
    conn.execute_command('set', '3', 'foo')
    env.expect('COPY', '3', '1', 'REPLACE').equal(1)
    env.expect('FT.SEARCH', 'things', 'foo').equal([1, '2', ['name', 'foo']])

def testFlush(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    conn.execute_command('FLUSHALL')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo').error().contains('Index not found: things')

def testNotExist(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'txt', 'text')

    conn.execute_command('hset', 'thing:bar', 'not_text', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([0])

def testPayload(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'things', 'ON', 'HASH',
                'PREFIX', '1', 'thing:',
                'PAYLOAD_FIELD', 'payload',
                'SCHEMA', 'name', 'text').ok()
    conn.execute_command('hset', 'thing:foo', 'name', 'foo', 'payload', 'stuff')

    for _ in env.reloadingIterator():
        waitForIndex(env, 'things')
        res = env.cmd('ft.search', 'things', 'foo')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'thing:foo', ['name', 'foo']]))

        res = env.cmd('ft.search', 'things', 'foo', 'withpayloads')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'thing:foo', 'stuff', ['name', 'foo']]))

def testBinaryPayload(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'things', 'ON', 'HASH',
                'PREFIX', '1', 'thing:',
                'PAYLOAD_FIELD', 'payload',
                'SCHEMA', 'name', 'text').ok()
    conn.execute_command('hset', 'thing:foo', 'name', 'foo', 'payload', b'\x00\xAB\x20')

    for _ in env.reloadingIterator():
        waitForIndex(env, 'things')
        res = env.cmd('ft.search', 'things', 'foo')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'thing:foo', ['name', 'foo']]))

        res = env.cmd('ft.search', 'things', 'foo', 'withpayloads', **{NEVER_DECODE: []})
        env.assertEqual(res, [1, b'thing:foo', b'\x00\xAB\x20', [b'name', b'foo']])

def testDuplicateFields(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC', 'SORTABLE').ok()
    con = env.getClusterConnectionIfNeeded()
    con.execute_command('FT.ADD', 'idx', 'doc', 1.0,
            'FIELDS', 'txt', 'foo', 'txt', 'bar', 'txt', 'baz')
    env.expect('ft.search', 'idx', 'baz').equal([1, 'doc', ['txt', 'baz']])
    env.expect('ft.search', 'idx', 'foo').equal([0])

def testReplace(env):
    conn = getConnectionByEnv(env)

    env.expect('ft.create idx schema f text').ok()

    res = conn.execute_command('HSET', 'doc1', 'f', 'hello world')
    env.assertEqual(res, 1)
    res = conn.execute_command('HSET', 'doc2', 'f', 'hello world')
    env.assertEqual(res, 1)
    res = env.cmd('ft.search', 'idx', 'hello world')
    env.assertEqual(2, res[0])

    # now replace doc1 with a different content
    res = conn.execute_command('HSET', 'doc1', 'f', 'goodbye universe')
    env.assertEqual(res, 0)

    for _ in env.reloadingIterator():
        waitForRdbSaveToFinish(env)
        waitForIndex(env, 'idx')
        # make sure the query for hello world does not return the replaced document
        env.expect('ft.search', 'idx', 'hello world', 'nocontent').equal([1, 'doc2'])

        # search for the doc's new content
        env.expect('ft.search', 'idx', 'goodbye universe', 'nocontent').equal([1, 'doc1'])

def testSortable(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'FILTER', 'startswith(@__key, "")',
                'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.assertOk(env.getClusterConnectionIfNeeded().execute_command('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1'))

def testMissingArgs(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'FILTER', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error()

def testWrongArgs(env):
    env.expect('FT.CREATE', 'idx', 'SCORE', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid score')
    env.expect('FT.CREATE', 'idx', 'SCORE', 10, 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid score')
    env.expect('FT.CREATE', 'idx', 'LANGUAGE', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid language')
    env.expect('FT.CREATE', 'idx', 'LANGUAGE', 'none', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid language')

def testLanguageDefaultAndField(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idxTest1', 'LANGUAGE_FIELD', 'lang', 'SCHEMA', 'body', 'TEXT')
    env.cmd('FT.CREATE', 'idxTest2', 'LANGUAGE', 'hindi', 'SCHEMA', 'body', 'TEXT')
    conn.execute_command('HSET', 'doc1', 'lang', 'hindi', 'body', u'अँगरेजी अँगरेजों अँगरेज़')

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idxTest1')
        waitForIndex(env, 'idxTest2')
        #test for language field
        res = env.cmd('FT.SEARCH', 'idxTest1', u'अँगरेज़')
        res1 = {res[2][i]:res[2][i + 1] for i in range(0, len(res[2]), 2)}
        env.assertEqual(u'अँगरेजी अँगरेजों अँगरेज़', res1['body'])
        # test for default language
        res = env.cmd('FT.SEARCH', 'idxTest2', u'अँगरेज़')
        res1 = {res[2][i]:res[2][i + 1] for i in range(0, len(res[2]), 2)}
        env.assertEqual(u'अँगरेजी अँगरेजों अँगरेज़', res1['body'])

def testScoreDecimal(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx1', 'SCORE', '0.5', 'schema', 'title', 'text').ok()
    env.expect('FT.CREATE', 'idx2', 'SCORE_FIELD', 'score', 'schema', 'title', 'text').ok()
    res = conn.execute_command('HSET', 'doc1', 'title', 'hello', 'score', '0.25')
    env.assertEqual(res, 2)

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx1')
        waitForIndex(env, 'idx2')
        res = env.cmd('ft.search', 'idx1', 'hello', 'scorer', 'TFIDF', 'withscores', 'nocontent')
        env.assertEqual(float(res[2]), 0.5)
        res = env.cmd('ft.search', 'idx2', 'hello', 'scorer', 'TFIDF', 'withscores', 'nocontent')
        env.assertEqual(float(res[2]), 0.25)

@skip(cluster=True)
def testInfo(env):

    env.expect('FT.CREATE', 'test', 'ON', 'HASH',
               'PREFIX', '2', 'student:', 'pupil:',
               'FILTER', '@age > 16',
               'language', 'hindi',
               'language_field', 'lang',
               'score', '0.5',
               'score_field', 'score',
               'payload_field', 'pl',
               'SCHEMA', 't', 'TEXT').ok()
    res_actual = env.cmd('FT.INFO test')
    res_expected = ['key_type', 'HASH',
                    'prefixes', ['student:', 'pupil:'],
                    'filter', '@age > 16',
                    'default_language', 'hindi',
                    'language_field', 'lang',
                    'default_score', '0.5',
                    'score_field', 'score',
                    'payload_field', 'pl',
                    'indexes_all', 'false']
    env.assertEqual(res_actual[5], res_expected)

    env.expect('ft.drop test').ok()

    env.expect('FT.CREATE', 'test', 'SCHEMA', 't', 'TEXT').ok()
    res_actual = env.cmd('FT.INFO test')
    res_expected = ['key_type', 'HASH',
                    'prefixes', [''],
                    'default_score', '1',
                    'indexes_all', 'false']
    env.assertEqual(res_actual[5], res_expected)

def testCreateDropCreate(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.create', 'things', 'ON', 'HASH',
               'PREFIX', '1', 'thing:', 'SCHEMA', 'name', 'text').ok()
    waitForIndex(env, 'things')
    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['name', 'foo']])
    env.expect('ft.dropindex things').ok()
    env.expect('ft.create', 'things', 'ON', 'HASH',
               'PREFIX', '1', 'thing:', 'SCHEMA', 'name', 'text').ok()
    waitForIndex(env, 'things')
    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['name', 'foo']])

@skip(cluster=True)
def testPartial(env):
    if env.env == 'existing-env':
        env.skip()
    env = Env(moduleArgs='PARTIAL_INDEXED_DOCS 1')

    # HSET
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    env.expect('HSET doc1 test foo').equal(1)
    env.expect(debug_cmd() + ' docidtoid idx doc1').equal(1)
    env.expect('HSET doc1 testtest foo').equal(1)
    env.expect(debug_cmd() + ' docidtoid idx doc1').equal(1)
    env.expect('HSET doc1 test bar').equal(0)
    env.expect(debug_cmd() + ' docidtoid idx doc1').equal(2)
    env.expect('FT.SEARCH idx bar').equal([1, 'doc1', ['test', 'bar', 'testtest', 'foo']])

    # HMSET
    env.expect('HMSET doc2 test foo').ok()
    env.expect(debug_cmd() + ' docidtoid idx doc2').equal(3)
    env.expect('HMSET doc2 testtest foo').ok()
    env.expect(debug_cmd() + ' docidtoid idx doc2').equal(3)
    env.expect('HMSET doc2 test baz').ok()
    env.expect(debug_cmd() + ' docidtoid idx doc2').equal(4)
    env.expect('FT.SEARCH idx baz').equal([1, 'doc2', ['test', 'baz', 'testtest', 'foo']])

    # HSETNX
    env.expect('HSETNX doc3 test foo').equal(1)
    env.expect(debug_cmd() + ' docidtoid idx doc3').equal(5)
    env.expect('HSETNX doc3 testtest foo').equal(1)
    env.expect(debug_cmd() + ' docidtoid idx doc3').equal(5)
    env.expect('HSETNX doc3 test bad').equal(0)
    env.expect(debug_cmd() + ' docidtoid idx doc3').equal(5)
    env.expect('FT.SEARCH idx foo').equal([1, 'doc3', ['test', 'foo', 'testtest', 'foo']])

    # HINCRBY
    env.expect('HINCRBY doc4 test 5').equal(5)
    env.expect(debug_cmd() + ' docidtoid idx doc4').equal(6)
    env.expect('HINCRBY doc4 testtest 5').equal(5)
    env.expect(debug_cmd() + ' docidtoid idx doc4').equal(6)
    env.expect('HINCRBY doc4 test 6').equal(11)
    env.expect(debug_cmd() + ' docidtoid idx doc4').equal(7)
    env.expect('HINCRBY doc4 test 5.5').error(). contains('value is not an integer or out of range')
    env.expect(debug_cmd() + ' docidtoid idx doc4').equal(7)
    env.expect('FT.SEARCH idx 11').equal([1, 'doc4', ['test', '11', 'testtest', '5']])

    # HINCRBYFLOAT
    env.expect('HINCRBYFLOAT doc5 test 5.5').equal('5.5')
    env.expect(debug_cmd() + ' docidtoid idx doc5').equal(8)
    env.expect('HINCRBYFLOAT doc5 testtest 5.5').equal('5.5')
    env.expect(debug_cmd() + ' docidtoid idx doc5').equal(8)
    res = env.cmd('HINCRBYFLOAT doc5 test 6.6')
    env.assertEqual(float(res), 12.1)
    env.expect(debug_cmd() + ' docidtoid idx doc5').equal(9)
    res = env.cmd('HINCRBYFLOAT doc5 test 5')
    env.assertEqual(float(res), 17.1)
    env.expect(debug_cmd() + ' docidtoid idx doc5').equal(10)
    res = env.cmd('FT.SEARCH idx *')
    res[8][1] = float(res[8][1])
    res[10][1] = float(res[10][1])
    env.assertEqual(res, [5, 'doc1', ['test', 'bar', 'testtest', 'foo'],
                             'doc2', ['test', 'baz', 'testtest', 'foo'],
                             'doc3', ['test', 'foo', 'testtest', 'foo'],
                             'doc4', ['test', 11, 'testtest', '5'],
                             'doc5', ['test', 17.1, 'testtest', '5.5']])

@skip(cluster=True)
def testHDel(env):
    if env.env == 'existing-env':
        env.skip()
    env = Env(moduleArgs='PARTIAL_INDEXED_DOCS 1')

    env.expect('FT.CREATE idx SCHEMA test1 TEXT test2 TEXT').equal('OK')
    env.expect('FT.CREATE idx2 SCHEMA test1 TEXT test2 TEXT').equal('OK')
    env.expect('HSET doc1 test1 foo test2 bar test3 baz').equal(3)
    env.expect(debug_cmd() + ' docidtoid idx doc1').equal(1)
    env.expect('HDEL doc1 test1').equal(1)
    env.expect(debug_cmd() + ' docidtoid idx doc1').equal(2)
    env.expect('HDEL doc1 test3').equal(1)
    env.expect(debug_cmd() + ' docidtoid idx doc1').equal(2)
    env.expect('FT.SEARCH idx bar').equal([1, 'doc1', ['test2', 'bar']])
    env.expect('HDEL doc1 test2').equal(1)
    env.expect('FT.SEARCH idx bar').equal([0])

@skip(cluster=True)
def testPartialIndexedDocsIsANoOp(env):
    """`PARTIAL_INDEXED_DOCS` is accepted and changes nothing.

    It used to switch on the command filter that reported which fields a write touched. That
    now comes from subkey notifications and needs no configuration, so the config survives
    only because removing a registered one stops the server from starting.

    Both halves have to be run to claim it is inert. Asserting only the enabled case would
    pass just as well against an implementation that still gated the optimization on the
    flag -- and that is the case `testPartial` already covers. The one that matters here is
    the flag *absent*, which is the new default.

    Standalone only, for the same reason `testPartial` and `testHDel` are: `DOCIDTOID` takes
    no key, so it answers from whichever shard receives it, while `HSET doc1` is routed by
    hash slot. In a cluster the two would not be talking about the same shard.
    """
    if env.env == 'existing-env':
        env.skip()

    def docid_progression(module_args):
        """(first, after a non-schema write, after a schema write) for one server."""
        server = Env(moduleArgs=module_args) if module_args else Env()
        conn = getConnectionByEnv(server)
        server.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
        conn.execute_command('HSET', 'doc1', 'test', 'foo')
        first = server.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1')
        # Outside the schema: nothing indexed can have changed.
        conn.execute_command('HSET', 'doc1', 'testtest', 'foo')
        unindexed = server.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1')
        # In the schema: has to be reindexed.
        conn.execute_command('HSET', 'doc1', 'test', 'bar')
        indexed = server.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1')
        server.stop()
        return (first, unindexed, indexed)

    default = docid_progression(None)
    enabled = docid_progression('PARTIAL_INDEXED_DOCS 1')

    env.assertEqual(default, enabled,
                    message=f'the deprecated argument changed behaviour: {default} vs {enabled}')

    first, unindexed, indexed = default
    env.assertEqual(unindexed, first,
                    message='a write touching no indexed field should not reindex by default')
    env.assertGreater(indexed, first,
                      message='a write to a schema field must still reindex')

@skip(cluster=True)
def testAliasedFieldWriteIsQueryable(env):
    """A write to an aliased field's hash path must reach the index, not just the doc table.

    A change set names the hash field the command wrote -- the field's path. The schema knows
    the field by its `AS` alias, so matching the alias instead finds nothing, the write looks
    like it touched nothing indexed, and the reindex is skipped. The document then answers
    queries with its previous value.

    The doc-id is checked because that is what the skip decision moves, and the search result
    because that is what a user sees. Neither alone says enough: a new doc-id with stale terms
    would pass the first, and the second could be satisfied by a reindex that happened for an
    unrelated reason.

    Standalone only, as with the other `DOCIDTOID` assertions in this file.
    """
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'AS', 'renamed', 'TEXT').ok()

    conn.execute_command('HSET', 'doc1', 'title', 'hello')
    first = env.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1')
    env.expect('FT.SEARCH', 'idx', '@renamed:hello', 'NOCONTENT').equal([1, 'doc1'])

    # The command writes `title`; the schema calls the field `renamed`.
    conn.execute_command('HSET', 'doc1', 'title', 'goodbye')

    env.assertGreater(env.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1'), first,
                      message='writing an aliased field must reindex the document')
    env.expect('FT.SEARCH', 'idx', '@renamed:goodbye', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.SEARCH', 'idx', '@renamed:hello', 'NOCONTENT').equal([0])

@skip(cluster=True)
def testPlainHashNotificationsFallback(env):
    """The degraded path still indexes, and the probe admits to being on it.

    A server without subkey notifications takes hash events over the plain channel with no
    change set, so every write reindexes the whole document. That is what an older Redis does
    in production, and no CI lane can reach it -- every one of them runs a Redis that has the
    API. `_FORCE_PLAIN_HASH_NOTIFICATIONS` selects that channel anyway so it can be tested.

    Two claims. The probe reports the path actually taken rather than the server's capability,
    which is what lets a test tell the two apart at all. And on the plain channel a write
    touching no indexed field still reindexes -- the skip needs a change set, and correctly
    declines to skip without one.
    """
    if env.env == 'existing-env':
        env.skip()

    # (A) On the subkey channel: the probe says so, and a write to no indexed field is skipped.
    subkey_env = Env()
    subkey_env.assertEqual(subkey_env.cmd(debug_cmd(), 'HASH_SUBKEY_NOTIFICATIONS'), 1,
                           message='CI runs a Redis with the API, so the subkey channel must be in use')
    conn = getConnectionByEnv(subkey_env)
    subkey_env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    conn.execute_command('HSET', 'doc1', 'test', 'foo')
    first = subkey_env.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1')
    conn.execute_command('HSET', 'doc1', 'untouched_by_the_index', 'x')
    subkey_env.assertEqual(subkey_env.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1'), first,
                           message='with a change set, a write to no indexed field is skipped')
    subkey_env.stop()

    # (B) Forced onto the plain channel: no change set, so the same write reindexes.
    plain_env = Env(moduleArgs='_FORCE_PLAIN_HASH_NOTIFICATIONS true')
    plain_env.assertEqual(plain_env.cmd(debug_cmd(), 'HASH_SUBKEY_NOTIFICATIONS'), 0,
                          message='forced onto the plain channel, the probe must not claim otherwise')
    conn = getConnectionByEnv(plain_env)
    plain_env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    conn.execute_command('HSET', 'doc1', 'test', 'foo')
    first = plain_env.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1')

    conn.execute_command('HSET', 'doc1', 'untouched_by_the_index', 'x')
    plain_env.assertGreater(plain_env.cmd(debug_cmd(), 'docidtoid', 'idx', 'doc1'), first,
                            message='without a change set there is nothing to skip on')
    # And the document is still correctly indexed afterwards, which is the point of the fallback.
    plain_env.expect('FT.SEARCH', 'idx', 'foo', 'NOCONTENT').equal([1, 'doc1'])

    conn.execute_command('HSET', 'doc1', 'test', 'bar')
    plain_env.expect('FT.SEARCH', 'idx', 'bar', 'NOCONTENT').equal([1, 'doc1'])
    plain_env.expect('FT.SEARCH', 'idx', 'foo', 'NOCONTENT').equal([0])
    plain_env.stop()

@skip(cluster=True)
def testRestore(env):
    if env.env == 'existing-env':
        env.skip()
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    env.expect('HSET doc1 test foo').equal(1)
    env.expect('FT.SEARCH idx foo').equal([1, 'doc1', ['test', 'foo']])
    dump = env.cmd('dump doc1', **{NEVER_DECODE: []})
    env.expect('DEL doc1').equal(1)
    env.expect('FT.SEARCH idx foo').equal([0])
    env.expect('RESTORE', 'doc1', 0, dump)
    env.expect('FT.SEARCH idx foo').equal([1, 'doc1', ['test', 'foo']])

@skip(cluster=True)
def testEvicted(env):
    skipOnCrdtEnv(env)

    # Ignore OOM so this test won't be effected by the OOM
    env.expect('FT.CONFIG', 'SET', 'ON_OOM', 'IGNORE').ok()

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')

    memory = 0
    info = conn.execute_command('INFO MEMORY')
    for line in info.splitlines():
        if 'used_memory:' in line:
            sub = line.split(':')
            memory = int(sub[1])

    conn.execute_command('CONFIG', 'SET', 'MAXMEMORY-POLICY', 'ALLKEYS-RANDOM')
    conn.execute_command('CONFIG', 'SET', 'MAXMEMORY', memory + 150000)
    for i in range(1000):
        env.expect('HSET', f'doc{i}', 'test', 'foo').equal(1)
    res = env.cmd('FT.SEARCH idx foo limit 0 0')
    env.assertLess(res[0], 1000)
    env.assertGreater(res[0], 0)
    conn.execute_command('CONFIG', 'SET', 'MAXMEMORY', 0)

def testSkipInitialScan(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'a', 'test', 'hello', 'text', 'world')

    # Regular
    env.expect('FT.CREATE idx SCHEMA test TEXT').ok()
    waitForIndex(env, 'idx')
    env.assertEqual(toSortedFlatList(env.cmd('FT.SEARCH idx hello')), toSortedFlatList([1, 'a', ['test', 'hello', 'text', 'world']]))
    # SkipInitialIndex
    env.expect('FT.CREATE idx_no_scan SKIPINITIALSCAN SCHEMA test TEXT').ok()
    waitForIndex(env, 'idx_no_scan')
    env.expect('FT.SEARCH idx_no_scan hello').equal([0])
    # Temporary
    env.expect('FT.CREATE temp_idx TEMPORARY 10 SCHEMA test TEXT').ok()
    waitForIndex(env, 'temp_idx')
    env.assertEqual(toSortedFlatList(env.cmd('FT.SEARCH temp_idx hello')), toSortedFlatList([1, 'a', ['test', 'hello', 'text', 'world']]))
    # Temporary & NoInitialIndex
    env.expect('FT.CREATE temp_idx_no_scan SKIPINITIALSCAN TEMPORARY 10 SCHEMA test TEXT').equal('OK')
    waitForIndex(env, 'temp_idx_no_scan')
    env.expect('FT.SEARCH temp_idx_no_scan hello').equal([0])

def testWrongFieldType(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA t TEXT n NUMERIC').ok()
    conn.execute_command('HSET', 'a', 't', 'hello', 'n', '42')
    conn.execute_command('HSET', 'b', 't', 'hello', 'n', 'world')

    env.assertEqual(toSortedFlatList(env.cmd('FT.SEARCH idx hello')), toSortedFlatList([1, 'a', ['t', 'hello', 'n', '42']]))

    res_actual = env.cmd('FT.INFO idx')
    res_actual = {res_actual[i]: res_actual[i + 1] for i in range(0, len(res_actual), 2)}
    env.assertEqual(str(res_actual['hash_indexing_failures']), '1')

@skip(cluster=True)
def testDocIndexedInTwoIndexes():
    env = Env(moduleArgs='MAXDOCTABLESIZE 50')
    env.expect('FT.CREATE idx1 SCHEMA t TEXT').ok()
    env.expect('FT.CREATE idx2 SCHEMA t TEXT').ok()

    for i in range(1000):
        env.expect('HSET', 'doc%d' % i, 't', 'foo').equal(1)

    env.expect('FT.DROPINDEX idx2 DD').ok()
    env.expect('FT.SEARCH idx1 foo').equal([0])

    env.expect('FT.DROPINDEX idx1 DD').ok()
