# -*- coding: utf-8 -*-

from common import *
import redis
from hotels import hotels
import random
import time
import unittest

# this tests is not longer relevant
# def testAdd(env):
#     if env.isCluster():
#         env.skip()

#     r = env
#     env.expect('ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text').ok()
#     env.assertTrue(r.exists('idx:idx'))
#     env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields', 'title', 'hello world', 'body', 'lorem ist ipsum').ok()

#     for _ in env.reloadingIterator():
#         prefix = 'ft'
#         env.assertExists(prefix + ':idx/hello')
#         env.assertExists(prefix + ':idx/world')
#         env.assertExists(prefix + ':idx/lorem')

def testAddErrors(env):
    env.expect('ft.create idx ON HASH schema foo text bar numeric sortable').equal('OK')
    env.expect('ft.add idx doc1 1 redis 4').error().contains('Unknown keyword')
    env.expect('ft.add idx doc1').error().contains("wrong number of arguments")
    env.expect('ft.add idx doc1 42').error().contains("Score must be between 0 and 1")
    env.expect('ft.add idx doc1 1.0').error().contains("No field list found")
    env.expect('ft.add fake_idx doc1 1.0 fields foo bar').error().contains("Unknown index name")

def assertEqualIgnoreCluster(env, val1, val2):
    # todo: each test that uses this function should be switch back to env.assertEqual once fix
    # issues on coordinator
    if env.isCluster():
        return
    env.assertEqual(val1, val2)

def testConditionalUpdate(env):
    env.assertOk(env.cmd(
        'ft.create', 'idx','ON', 'HASH',
        'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
    env.assertOk(env.cmd('ft.add', 'idx', '1', '1',
                           'fields', 'foo', 'hello', 'bar', '123'))
    env.assertOk(env.cmd('ft.add', 'idx', '1', '1', 'replace', 'if',
                           '@foo == "hello"', 'fields', 'foo', 'world', 'bar', '123'))
    env.assertEqual('NOADD', env.cmd('ft.add', 'idx', '1', '1', 'replace',
                                       'if', '@foo == "hello"', 'fields', 'foo', 'world', 'bar', '123'))
    env.assertEqual('NOADD', env.cmd('ft.add', 'idx', '1', '1', 'replace',
                                       'if', '1 == 2', 'fields', 'foo', 'world', 'bar', '123'))
    env.assertOk(env.cmd('ft.add', 'idx', '1', '1', 'replace', 'partial', 'if',
                           '@foo == "world"', 'fields', 'bar', '234'))
    env.assertOk(env.cmd('ft.add', 'idx', '1', '1', 'replace', 'if',
                           '@bar == 234', 'fields', 'foo', 'hello', 'bar', '123'))

    # Ensure that conditionals are ignored if the document doesn't exist
    env.assertOk(env.cmd('FT.ADD', 'idx', '666', '1',
                           'IF', '@bar > 42', 'FIELDS', 'bar', '15'))
    # Ensure that it fails if we try again, because it already exists
    env.assertEqual('NOADD', env.cmd('FT.ADD', 'idx', '666', '1',
                                       'REPLACE', 'IF', '@bar > 42', 'FIELDS', 'bar', '15'))
    # Ensure that it fails because we're not using 'REPLACE'
    with env.assertResponseError():
        env.assertOk(env.cmd('FT.ADD', 'idx', '666', '1',
                               'IF', '@bar > 42', 'FIELDS', 'bar', '15'))

def testUnionIdList(env):
    # Regression test for https://github.com/RediSearch/RediSearch/issues/306
    N = 100
    env.expect(
        "ft.create", "test", 'ON', 'HASH',
        "SCHEMA",  "tags", "TAG", "waypoint", "GEO").ok()
    env.expect("ft.add", "test", "1", "1", "FIELDS", "tags", "alberta", "waypoint", "-113.524,53.5244").ok()
    env.expect("ft.add", "test", "2", "1", "FIELDS", "tags", "ontario", "waypoint", "-79.395,43.661667").ok()

    env.cmd('ft.search', 'test', '@tags:{ontario}')

    res = env.cmd(
        'ft.search', 'test', "@waypoint:[-113.52 53.52 20 mi]|@tags:{ontario}", 'nocontent')
    env.assertEqual(res, [2, '1', '2'])

def testAttributes(env):
    env.assertOk(env.cmd('ft.create', 'idx','ON', 'HASH',
                         'schema', 'title', 'text', 'body', 'text'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 't1 t2', 'body', 't3 t4 t5'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                           'body', 't1 t2', 'title', 't3 t5'))

    res = env.cmd(
        'ft.search', 'idx', '(@title:(t1 t2) => {$weight: 0.2}) |(@body:(t1 t2) => {$weight: 0.5})', 'nocontent')
    env.assertEqual([2, 'doc2', 'doc1'], res)
    res = env.cmd(
        'ft.search', 'idx', '(@title:(t1 t2) => {$weight: 2.5}) |(@body:(t1 t2) => {$weight: 0.5})', 'nocontent')
    env.assertEqual([2, 'doc1', 'doc2'], res)

    res = env.cmd(
        'ft.search', 'idx', '(t3 t5) => {$slop: 4}', 'nocontent')
    env.assertEqual([2, 'doc2', 'doc1'], res)
    res = env.cmd(
        'ft.search', 'idx', '(t5 t3) => {$slop: 0}', 'nocontent')
    env.assertEqual([1, 'doc2'], res)
    res = env.cmd(
        'ft.search', 'idx', '(t5 t3) => {$slop: 0; $inorder:true}', 'nocontent')
    env.assertEqual([0], res)

def testUnion(env):
    N = 100
    env.expect('ft.create', 'idx','ON', 'HASH', 'schema', 'f', 'text').ok()
    for i in range(N):

        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world' if i % 2 == 0 else 'hallo werld').ok()

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd(
            'ft.search', 'idx', 'hello|hallo', 'nocontent', 'limit', '0', '100')
        env.assertEqual(N + 1, len(res))
        env.assertEqual(N, res[0])

        res = env.cmd(
            'ft.search', 'idx', 'hello|world', 'nocontent', 'limit', '0', '100')
        env.assertEqual(51, len(res))
        env.assertEqual(50, res[0])

        res = env.cmd('ft.search', 'idx', '(hello|hello)(world|world)',
                                'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(51, len(res))
        env.assertEqual(50, res[0])

        res = env.cmd(
            'ft.search', 'idx', '(hello|hallo)(werld|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

        res = env.cmd(
            'ft.search', 'idx', '(hallo|hello)(world|werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

        res = env.cmd(
            'ft.search', 'idx', '(hello|werld)(hallo|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

        res = env.cmd(
            'ft.search', 'idx', '(hello|hallo) world', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(51, len(res))
        env.assertEqual(50, res[0])

        res = env.cmd(
            'ft.search', 'idx', '(hello world)|((hello world)|(hallo world|werld) | hello world werld)',
            'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

def testSearch(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH',
             'schema', 'title', 'text', 'weight', 10.0, 'body', 'text').ok()
    env.expect('ft.add', 'idx', 'doc1', 0.5,
             'fields','title', 'hello world', 'body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1.0,
             'fields', 'title', 'hello another world', 'body', 'lorem ist ipsum lorem lorem').ok()
    # order of documents might change after reload
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('ft.search', 'idx', 'hello')
        expected = [2, 'doc2', ['title', 'hello another world', 'body', 'lorem ist ipsum lorem lorem'],
                    'doc1', ['title', 'hello world', 'body', 'lorem ist ipsum']]
        env.assertEqual(toSortedFlatList(res), toSortedFlatList(expected))

        # Test empty query
        res = env.cmd('ft.search', 'idx', '')
        env.assertEqual([0], res)

        # Test searching with no content
        res = env.cmd(
            'ft.search', 'idx', 'hello', 'nocontent')
        env.assertTrue(len(res) == 3)
        expected = ['doc2', 'doc1']
        env.assertEqual(res[0], 2)
        for item in expected:
            env.assertContains(item, res)

        # Test searching WITHSCORES
        res = env.cmd('ft.search', 'idx', 'hello', 'WITHSCORES')
        env.assertEqual(len(res), 7)
        env.assertEqual(res[0], 2)
        for item in expected:
            env.assertContains(item, res)
        env.assertTrue(float(res[2]) > 0)
        env.assertTrue(float(res[5]) > 0)

        # Test searching WITHSCORES NOCONTENT
        res = env.cmd('ft.search', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
        env.assertEqual(len(res), 5)
        env.assertEqual(res[0], 2)
        for item in expected:
            env.assertContains(item, res)
        env.assertTrue(float(res[2]) > 0)
        env.assertTrue(float(res[4]) > 0)

def testGet(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text', 'bar', 'text').ok()

    env.expect('ft.get').error().contains("wrong number of arguments")
    env.expect('ft.get', 'idx').error().contains("wrong number of arguments")
    env.expect('ft.get', 'idx', 'foo', 'bar').error().contains("wrong number of arguments")
    env.expect('ft.mget').error().contains("wrong number of arguments")
    env.expect('ft.mget', 'idx').error().contains("wrong number of arguments")
    env.expect('ft.mget', 'fake_idx').error().contains("wrong number of arguments")

    env.expect('ft.get fake_idx foo').error().contains("Unknown Index name")
    env.expect('ft.mget fake_idx foo').error().contains("Unknown Index name")

    for i in range(100):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                   'foo', 'hello world', 'bar', 'wat wat').ok()

    for i in range(100):
        res = env.cmd('ft.get', 'idx', 'doc%d' % i)
        env.assertIsNotNone(res)
        env.assertEqual(set(['foo', 'hello world', 'bar', 'wat wat']), set(res))
        env.assertIsNone(env.cmd(
            'ft.get', 'idx', 'doc%dsdfsd' % i))
    env.expect('ft.get', 'no_idx', 'doc0').error().contains("Unknown Index name")

    rr = env.cmd(
        'ft.mget', 'idx', *('doc%d' % i for i in range(100)))
    env.assertEqual(len(rr), 100)
    for res in rr:
        env.assertIsNotNone(res)
        env.assertEqual(set(['foo', 'hello world', 'bar', 'wat wat']), set(res))
    rr = env.cmd(
        'ft.mget', 'idx', *('doc-%d' % i for i in range(100)))
    env.assertEqual(len(rr), 100)
    for res in rr:
        env.assertIsNone(res)

    # Verify that when a document is deleted, GET returns NULL
    env.cmd('ft.del', 'idx', 'doc10') # But we still keep the document
    env.cmd('ft.del', 'idx', 'doc11')
    assert env.cmd('ft.del', 'idx', 'coverage') == 0
    res = env.cmd('ft.get', 'idx', 'doc10')
    env.assertEqual(None, res)
    res = env.cmd('ft.mget', 'idx', 'doc10')
    env.assertEqual([None], res)
    res = env.cmd('ft.mget', 'idx', 'doc10', 'doc11', 'doc12')
    env.assertIsNone(res[0])
    env.assertIsNone(res[1])
    env.assertTrue(not not res[2])

    env.expect('ft.add idx doc 0.1 language arabic payload redislabs fields foo foo').ok()
    env.expect('ft.get idx doc').equal(['foo', 'foo'])
    res = env.cmd('hgetall doc')
    env.assertEqual(set(res), set(['foo', 'foo', '__score', '0.1', '__language', 'arabic', '__payload', 'redislabs']))


def testDelete(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text').ok()

    for i in range(100):
        env.assertEqual(1, env.cmd('hset', 'doc%d' % i, 'f', 'hello world'))

    env.expect('ft.del', 'fake_idx', 'doc1').error()

    for i in range(100):
        # the doc hash should exist now
        env.expect('ft.get', 'idx', 'doc%d' % i).noError()
        # Delete the actual docs only half of the time
        env.assertEqual(1, env.cmd(
           'ft.del', 'idx', 'doc%d' % i, 'DD' if i % 2 == 0 else ''))
        # second delete should return 0
        env.assertEqual(0, env.cmd('ft.del', 'idx', 'doc%d' % i))
        # second delete should return 0

        # TODO: return 0 if doc wasn't found
        #env.assertEqual(0, env.cmd(
        #    'ft.del', 'idx', 'doc%d' % i))

        # After del with DD the doc hash should not exist
        if i % 2 == 0:
            env.assertFalse(r.exists('doc%d' % i))
        else:
            env.expect('ft.get', 'idx', 'doc%d' % i).noError()
        res = env.cmd('ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
        env.assertNotContains('doc%d' % i, res)
        env.assertEqual(res[0], 100 - i - 1)
        env.assertEqual(len(res), 100 - i)

        # test reinsertion
        env.assertEqual(1, env.cmd('hset', 'doc%d' % i, 'f', 'hello world'))
        res = env.cmd('ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
        env.assertContains('doc%d' % i, res)
        env.assertEqual(1, env.cmd('ft.del', 'idx', 'doc%d' % i))

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        did = 'rrrr'
        env.assertEqual(1, env.cmd('hset', did, 'f', 'hello world'))
        env.assertEqual(1, env.cmd('ft.del', 'idx', did))
        env.assertEqual(0, env.cmd('ft.del', 'idx', did))
        env.assertEqual(1, env.cmd('hset', 'idx', did, 'f', 'hello world'))
        env.assertEqual(1, env.cmd('ft.del', 'idx', did))
        env.assertEqual(0, env.cmd('ft.del', 'idx', did))

def testReplace(env):

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text').ok()

    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields', 'f', 'hello world').ok()
    env.expect('ft.add', 'idx', 'doc2', 1.0, 'fields', 'f', 'hello world').ok()
    res = env.cmd(
        'ft.search', 'idx', 'hello world')
    env.assertEqual(2, res[0])

    with env.assertResponseError():
        # make sure we can't insert a doc twice
        res = env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                'f', 'hello world')

    # now replace doc1 with a different content
    env.expect('ft.add', 'idx', 'doc1', 1.0, 'replace', 'fields', 'f', 'goodbye universe').ok()

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        # make sure the query for hello world does not return the replaced
        # document
        res = env.cmd(
            'ft.search', 'idx', 'hello world', 'nocontent')
        env.assertEqual(1, res[0])
        env.assertEqual('doc2', res[1])

        # search for the doc's new content
        res = env.cmd(
            'ft.search', 'idx', 'goodbye universe', 'nocontent')
        env.assertEqual(1, res[0])
        env.assertEqual('doc1', res[1])

def testDrop(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo').ok()

    for i in range(100):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                   'f', 'hello world', 'n', 666, 't', 'foo bar', 'g', '19.04,47.497').ok()
    env.assertGreaterEqual(countKeys(env), 100)

    env.expect('ft.drop', 'idx').ok()

    env.assertEqual(0, countKeys(env))
    env.flush()

    # Now do the same with KEEPDOCS
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo').ok()

    for i in range(100):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                   'f', 'hello world', 'n', 666, 't', 'foo bar', 'g', '19.04,47.497').ok()
    env.assertGreaterEqual(countKeys(env), 100)

    if not env.isCluster():
        env.expect('ft.drop', 'idx', 'KEEPDOCS').ok()
        keys = env.keys('*')
        env.assertEqual(['doc0', 'doc1', 'doc10', 'doc11', 'doc12', 'doc13', 'doc14', 'doc15',
                             'doc16', 'doc17', 'doc18', 'doc19', 'doc2', 'doc20', 'doc21', 'doc22',
                             'doc23', 'doc24', 'doc25', 'doc26', 'doc27', 'doc28', 'doc29', 'doc3',
                             'doc30', 'doc31', 'doc32', 'doc33', 'doc34', 'doc35', 'doc36', 'doc37',
                             'doc38', 'doc39', 'doc4', 'doc40', 'doc41', 'doc42', 'doc43', 'doc44',
                             'doc45', 'doc46', 'doc47', 'doc48', 'doc49', 'doc5', 'doc50', 'doc51',
                             'doc52', 'doc53', 'doc54', 'doc55', 'doc56', 'doc57', 'doc58', 'doc59',
                             'doc6', 'doc60', 'doc61', 'doc62', 'doc63', 'doc64', 'doc65', 'doc66',
                             'doc67', 'doc68', 'doc69', 'doc7', 'doc70', 'doc71', 'doc72', 'doc73',
                             'doc74', 'doc75', 'doc76', 'doc77', 'doc78', 'doc79', 'doc8', 'doc80',
                             'doc81', 'doc82', 'doc83', 'doc84', 'doc85', 'doc86', 'doc87', 'doc88',
                             'doc89', 'doc9', 'doc90', 'doc91', 'doc92', 'doc93', 'doc94', 'doc95',
                             'doc96', 'doc97', 'doc98', 'doc99'],
                             py2sorted(keys))

    env.expect('FT.DROP', 'idx', 'KEEPDOCS', '666').error().contains("wrong number of arguments")

def testDelete(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo').ok()

    for i in range(100):
        res = conn.execute_command('hset', 'doc%d' % i,
                                   'f', 'hello world', 'n', 666, 't', 'foo bar', 'g', '19.04,47.497')
        env.assertEqual(4, res)
    env.assertGreaterEqual(countKeys(env), 100)

    env.expect('FT.DROPINDEX', 'idx', 'dd').ok()
    env.assertEqual(0, countKeys(env))
    env.flush()

    # Now do the same with KEEPDOCS
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo').ok()

    for i in range(100):
        res = conn.execute_command('hset', 'doc%d' % i,
                                   'f', 'hello world', 'n', 666, 't', 'foo bar', 'g', '19.04,47.497')
        env.assertEqual(4, res)
    env.assertGreaterEqual(countKeys(env), 100)

    if not env.isCluster():
        env.expect('FT.DROPINDEX', 'idx').ok()
        keys = env.keys('*')
        env.assertEqual(py2sorted("doc%d" %k for k in range(100)), py2sorted(keys))

    env.expect('FT.DROPINDEX', 'idx', 'dd', '666').error().contains("wrong number of arguments")

def testCustomStopwords(env):
    # Index with default stopwords
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text').ok()

    # Index with custom stopwords
    env.expect('ft.create', 'idx2', 'ON', 'HASH', 'stopwords', 2, 'hello', 'world',
                                    'schema', 'foo', 'text').ok()
    assertInfoField(env, 'idx2', 'stopwords_list', ['hello', 'world'])

    # Index with NO stopwords
    env.expect('ft.create', 'idx3', 'ON', 'HASH', 'stopwords', 0,
                                    'schema', 'foo', 'text').ok()
    assertInfoField(env, 'idx3', 'stopwords_list', [])

    # 2nd Index with NO stopwords - check global is used and freed
    env.expect('ft.create', 'idx4', 'ON', 'HASH', 'stopwords', 0,
                                    'schema', 'foo', 'text').ok()

    # Index with keyword as stopword - not supported in dialect1
    env.expect('ft.create', 'idx5', 'ON', 'HASH', 'stopwords', 1, 'true',
               'schema', 'foo', 'text').ok()
    env.expect('ft.search', 'idx5', '@foo:title=>{$inorder:true}', 'DIALECT', '2').equal([0])

    #for idx in ('idx', 'idx2', 'idx3'):
    env.expect('ft.add', 'idx', 'doc1', 1.0, 'fields', 'foo', 'hello world').ok()
    env.expect('ft.add', 'idx', 'doc2', 1.0, 'fields', 'foo', 'to be or not to be').ok()

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        # Normal index should return results just for 'hello world'
        env.assertEqual([1, 'doc1'],  env.cmd(
            'ft.search', 'idx', 'hello world', 'nocontent'))
        env.assertEqual([0],  env.cmd(
            'ft.search', 'idx', 'to be or not', 'nocontent'))

        # Custom SW index should return results just for 'to be or not'
        env.assertEqual([0],  env.cmd(
            'ft.search', 'idx2', 'hello world', 'nocontent'))
        env.assertEqual([1, 'doc2'],  env.cmd(
            'ft.search', 'idx2', 'to be or not', 'nocontent'))

        # No SW index should return results for both
        env.assertEqual([1, 'doc1'],  env.cmd(
            'ft.search', 'idx3', 'hello world', 'nocontent'))
        env.assertEqual([1, 'doc2'],  env.cmd(
            'ft.search', 'idx3', 'to be or not', 'nocontent'))

def testStopwords(env):
    # This test was taken from Python's tests, and failed due to some changes
    # made earlier
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'stopwords', 3, 'foo',
             'bar', 'baz', 'schema', 'txt', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'txt', 'foo bar')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields', 'txt', 'hello world')

    r1 = env.cmd('ft.search', 'idx', 'foo bar', 'nocontent')
    r2 = env.cmd('ft.search', 'idx', 'foo bar hello world', 'nocontent')
    env.assertEqual(0, r1[0])
    env.assertEqual(1, r2[0])

def testNoStopwords(env):
    # This test taken from Java's test suite
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text')
    for i in range(100):
        env.cmd('ft.add', 'idx', 'doc{}'.format(i), 1.0, 'fields',
                 'title', 'hello world' if i % 2 == 0 else 'hello worlds')

    res = env.cmd('ft.search', 'idx', 'hello a world', 'NOCONTENT')
    env.assertEqual(100, res[0])

    res = env.cmd('ft.search', 'idx', 'hello a world',
                   'VERBATIM', 'NOCONTENT')
    env.assertEqual(50, res[0])

    res = env.cmd('ft.search', 'idx', 'hello a world', 'NOSTOPWORDS')
    env.assertEqual(0, res[0])

def testOptional(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text').ok()
    env.expect('ft.add', 'idx',
                                    'doc1', 1.0, 'fields', 'foo', 'hello wat woot').ok()
    env.expect('ft.add', 'idx', 'doc2',
                                    1.0, 'fields', 'foo', 'hello world woot').ok()
    env.expect('ft.add', 'idx', 'doc3',
                                    1.0, 'fields', 'foo', 'hello world werld').ok()

    expected = [3, 'doc1', 'doc2', 'doc3']
    res = env.cmd('ft.search', 'idx', 'hello', 'nocontent')
    env.assertEqual(res, expected)
    res = env.cmd(
        'ft.search', 'idx', 'hello world', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual([2, 'doc2', 'doc3'], res)
    res = env.cmd(
        'ft.search', 'idx', 'hello ~world', 'nocontent', 'scorer', 'DISMAX')
    # Docs that contains the optional term would rank higher.
    env.assertEqual(res, [3, 'doc2', 'doc3', 'doc1'])
    res = env.cmd(
        'ft.search', 'idx', 'hello ~world ~werld', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual(res, [3, 'doc3', 'doc2', 'doc1'])
    res = env.cmd(
        'ft.search', 'idx', '~world ~(werld hello)', 'withscores', 'nocontent', 'scorer', 'DISMAX')
    # Note that doc1 gets 0 score since neither 'world' appears in the doc nor the phrase 'werld hello'.
    env.assertEqual(res, [3, 'doc3', '3', 'doc2', '1', 'doc1', '0'])

def testExplain(env):

    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'foo', 'text', 'bar', 'numeric', 'sortable',
        'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2', 't', 'TEXT').ok()
    q = '(hello world) "what what" (hello|world) (@bar:[10 100]|@bar:[200 300])'
    res = env.cmd('ft.explain', 'idx', q)
    # print res.replace('\n', '\\n')
    # expected = """INTERSECT {\n  UNION {\n    hello\n    +hello(expanded)\n  }\n  UNION {\n    world\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    # expected = """INTERSECT {\n  UNION {\n    hello\n    <HL(expanded)\n    +hello(expanded)\n  }\n  UNION {\n    world\n    <ARLT(expanded)\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      <HL(expanded)\n      +hello(expanded)\n    }\n    UNION {\n      world\n      <ARLT(expanded)\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    expected = """INTERSECT {\n  UNION {\n    hello\n    +hello(expanded)\n  }\n  UNION {\n    world\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    env.assertEqual(res, expected)


    # expected = ['INTERSECT {', '  UNION {', '    hello', '    <HL(expanded)', '    +hello(expanded)', '  }', '  UNION {', '    world', '    <ARLT(expanded)', '    +world(expanded)', '  }', '  EXACT {', '    what', '    what', '  }', '  UNION {', '    UNION {', '      hello', '      <HL(expanded)', '      +hello(expanded)', '    }', '    UNION {', '      world', '      <ARLT(expanded)', '      +world(expanded)', '    }', '  }', '  UNION {', '    NUMERIC {10.000000 <= @bar <= 100.000000}', '    NUMERIC {200.000000 <= @bar <= 300.000000}', '  }', '}', '']
    if env.isCluster():
        env.skip()
    res = env.cmd('ft.explainCli', 'idx', q)
    expected = ['INTERSECT {', '  UNION {', '    hello', '    +hello(expanded)', '  }', '  UNION {', '    world', '    +world(expanded)', '  }', '  EXACT {', '    what', '    what', '  }', '  UNION {', '    UNION {', '      hello', '      +hello(expanded)', '    }', '    UNION {', '      world', '      +world(expanded)', '    }', '  }', '  UNION {', '    NUMERIC {10.000000 <= @bar <= 100.000000}', '    NUMERIC {200.000000 <= @bar <= 300.000000}', '  }', '}', '']
    env.assertEqual(expected, res)

    q = '(hello world) "what what" hello|world @bar:[10 100]|@bar:[200 300]'
    res = env.cmd('ft.explain', 'idx', q, 'DIALECT', 1)
    expected = """INTERSECT {\n  UNION {\n    hello\n    +hello(expanded)\n  }\n  UNION {\n    world\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    env.assertEqual(res, expected)
    res = env.cmd('ft.explain', 'idx', q, 'DIALECT', 2)
    expected = """UNION {\n  INTERSECT {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n    EXACT {\n      what\n      what\n    }\n    UNION {\n      hello\n      +hello(expanded)\n    }\n  }\n  INTERSECT {\n    UNION {\n      world\n      +world(expanded)\n    }\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n  }\n  NUMERIC {200.000000 <= @bar <= 300.000000}\n}\n"""
    env.assertEqual(res, expected)

    res = env.cmd('ft.explaincli', 'idx', q, 'DIALECT', 1)
    expected = ['INTERSECT {', '  UNION {', '    hello', '    +hello(expanded)', '  }', '  UNION {', '    world', '    +world(expanded)', '  }', '  EXACT {', '    what', '    what', '  }', '  UNION {', '    UNION {', '      hello', '      +hello(expanded)', '    }', '    UNION {', '      world', '      +world(expanded)', '    }', '  }', '  UNION {', '    NUMERIC {10.000000 <= @bar <= 100.000000}', '    NUMERIC {200.000000 <= @bar <= 300.000000}', '  }', '}', '']
    env.assertEqual(res, expected)
    res = env.cmd('ft.explainCli', 'idx', q, 'DIALECT', 2)
    expected = ['UNION {', '  INTERSECT {', '    UNION {', '      hello', '      +hello(expanded)', '    }', '    UNION {', '      world', '      +world(expanded)', '    }', '    EXACT {', '      what', '      what', '    }', '    UNION {', '      hello', '      +hello(expanded)', '    }', '  }', '  INTERSECT {', '    UNION {', '      world', '      +world(expanded)', '    }', '    NUMERIC {10.000000 <= @bar <= 100.000000}', '  }', '  NUMERIC {200.000000 <= @bar <= 300.000000}', '}', '']
    env.assertEqual(expected, res)


    q = ['* => [KNN $k @v $B EF_RUNTIME 100]', 'DIALECT', 2, 'PARAMS', '4', 'k', '10', 'B', b'\xa4\x21\xf5\x42\x18\x07\x00\xc7']
    res = env.cmd('ft.explain', 'idx', *q)
    expected = """VECTOR {K=10 nearest vectors to `$B` in vector index associated with field @v, EF_RUNTIME = 100, yields distance as `__v_score`}\n"""
    env.assertEqual(expected, res)

    # range query
    q = ['@v:[VECTOR_RANGE $r $B]=>{$epsilon: 1.2; $yield_distance_as:dist}', 'DIALECT', 2, 'PARAMS', '4', 'r', 0.1, 'B', b'\xa4\x21\xf5\x42\x18\x07\x00\xc7']
    res = env.cmd('ft.explain', 'idx', *q)
    expected = """VECTOR {Vectors that are within 0.1 distance radius from `$B` in vector index associated with field @v, epsilon = 1.2, yields distance as `dist`}\n"""
    env.assertEqual(expected, res)

    # test with hybrid query
    q = ['(@t:hello world) => [KNN $k @v $B EF_RUNTIME 100]', 'DIALECT', 2, 'PARAMS', '4', 'k', '10', 'B', b'\xa4\x21\xf5\x42\x18\x07\x00\xc7']
    res = env.cmd('ft.explain', 'idx', *q)
    expected = """VECTOR {\n  INTERSECT {\n    @t:hello\n    world\n  }\n} => {K=10 nearest vectors to `$B` in vector index associated with field @v, EF_RUNTIME = 100, yields distance as `__v_score`}\n"""
    env.assertEqual(expected, res)

    # retest when index is not empty
    env.expect('hset', '1', 'v', 'abababab', 't', "hello").equal(2)
    res = env.cmd('ft.explain', 'idx', *q)
    env.assertEqual(expected, res)


def testNoIndex(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH', 'schema',
        'foo', 'text',
        'num', 'numeric', 'sortable', 'noindex',
        'extra', 'text', 'noindex', 'sortable').ok()

    if not env.isCluster():
        # to specific check on cluster, todo : change it to be generic enough
        res = env.cmd('ft.info', 'idx')
        env.assertEqual(res[7][1][8], 'NOINDEX')
        env.assertEqual(res[7][2][9], 'NOINDEX')

    env.expect('ft.add', 'idx', 'doc1', '0.1', 'fields',
                                    'foo', 'hello world', 'num', 1, 'extra', 'hello lorem ipsum').ok()
    res = env.cmd(
        'ft.search', 'idx', 'hello world', 'nocontent')
    env.assertEqual([1, 'doc1'], res)
    res = env.cmd(
        'ft.search', 'idx', 'lorem ipsum', 'nocontent')
    env.assertEqual([0], res)
    res = env.cmd(
        'ft.search', 'idx', '@extra:hello', 'nocontent')
    env.assertEqual([0], res)
    res = env.cmd(
        'ft.search', 'idx', '@num:[1 1]', 'nocontent')
    env.assertEqual([0], res)

def testPartial(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',  'SCORE_FIELD', '__score',
        'schema',
        'foo', 'text',
        'num', 'numeric', 'sortable', 'noindex',
        'extra', 'text', 'noindex').ok()
    # print env.cmd('ft.info', 'idx')

    env.expect('ft.add', 'idx', 'doc1', '0.1', 'fields',
               'foo', 'hello world', 'num', 1, 'extra', 'lorem ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', '0.1', 'fields',
               'foo', 'hello world', 'num', 2, 'extra', 'abba').ok()
    res = env.cmd('ft.search', 'idx', 'hello world',
                            'sortby', 'num', 'asc', 'nocontent', 'withsortkeys')
    env.assertEqual([2, 'doc1', '#1', 'doc2', '#2'], res)
    res = env.cmd('ft.search', 'idx', 'hello world',
                            'sortby', 'num', 'desc', 'nocontent', 'withsortkeys')
    env.assertEqual([2, 'doc2', '#2', 'doc1', '#1'], res)

    # Updating non indexed fields doesn't affect search results
    env.expect('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial', 'fields', 'num', 3, 'extra', 'jorem gipsum').ok()
    env.expect('ft.add', 'idx', 'doc12', '0.1', 'replace', 'partial',
                                    'fields', 'num1', 'redis').equal('OK')

    res = env.cmd(
        'ft.search', 'idx', 'hello world', 'sortby', 'num', 'desc',)
    assertResultsEqual(env, [2, 'doc1', ['foo', 'hello world', 'num', '3','extra', 'jorem gipsum'],
        'doc2', ['foo', 'hello world', 'num', '2', 'extra', 'abba']], res)
    res = env.cmd(
        'ft.search', 'idx', 'hello', 'nocontent', 'withscores')
    # Updating only indexed field affects search results
    env.expect('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial', 'fields', 'foo', 'wat wet').ok()
    res = env.cmd(
        'ft.search', 'idx', 'hello world', 'nocontent')
    env.assertEqual([1, 'doc2'], res)
    res = env.cmd('ft.search', 'idx', 'wat', 'nocontent')
    env.assertEqual([1, 'doc1'], res)

    # Test updating of score and no fields
    res = env.cmd(
        'ft.search', 'idx', 'wat', 'nocontent', 'withscores')
    env.assertLess(float(res[2]), 1)
    # env.assertEqual([1, 'doc1'], res)
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'replace', 'partial', 'fields').ok()
    res = env.cmd(
        'ft.search', 'idx', 'wat', 'nocontent', 'withscores')
    # We reindex though no new fields, just score is updated. this effects score
    env.assertEqual(float(res[2]), 1)

    # Test updating payloads
    res = env.cmd(
        'ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
    env.assertIsNone(res[2])
    env.expect('ft.add', 'idx', 'doc1', '1.0',
                                    'replace', 'partial', 'payload', 'foobar', 'fields').ok()
    res = env.cmd(
        'ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
    env.assertEqual('foobar', res[2])

def testPaging(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable').ok()
    N = 100
    for i in range(N):
        env.expect('ft.add', 'idx', '%d' % i, 1, 'fields',
                                        'foo', 'hello', 'bar', i).ok()

    chunk = 7
    offset = 0
    while True:

        res = env.cmd(
            'ft.search', 'idx', 'hello', 'nocontent', 'sortby', 'bar', 'desc', 'limit', offset, chunk)
        env.assertEqual(res[0], N)

        if offset + chunk > N:
            env.assertTrue(len(res) - 1 <= chunk)
            break
        env.assertEqual(len(res), chunk + 1)
        for n, id in enumerate(res[1:]):
            env.assertEqual(int(id), N - 1 - (offset + n))
        offset += chunk
        chunk = random.randrange(1, 10)
    res = env.cmd(
        'ft.search', 'idx', 'hello', 'nocontent', 'sortby', 'bar', 'asc', 'limit', N, 10)
    env.assertEqual(res[0], N)
    env.assertEqual(len(res), 1)

    with env.assertResponseError():
        env.cmd(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, -1)
    with env.assertResponseError():
        env.cmd(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', -1, 10)
    with env.assertResponseError():
        env.cmd(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 2000000)

def testPrefix(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text').ok()
    N = 100
    for i in range(N):
        env.assertEqual(1, conn.execute_command('hset', 'doc%d' % i, 'foo', 'constant term%d' % (random.randrange(0, 5))))
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        env.expect('ft.search', 'idx', 'constant term', 'nocontent').equal([0])
        res = env.cmd('ft.search', 'idx', 'constant term*', 'nocontent')
        env.assertEqual(N, res[0])
        res = env.cmd('ft.search', 'idx', 'const* term*', 'nocontent')
        env.assertEqual(N, res[0])
        res = env.cmd('ft.search', 'idx', 'constant term1*', 'nocontent')
        env.assertGreater(res[0], 2)
        env.expect('ft.search', 'idx', 'const* -term*', 'nocontent').equal([0])
        env.expect('ft.search', 'idx', 'constant term9*', 'nocontent').equal([0])

def testPrefixNodeCaseSensitive(env):

    conn = getConnectionByEnv(env)
    modes = ["TEXT", "TAG", "TAG_CASESENSITIVE"]
    create_functions = {
        "TEXT": ['FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT'],
        "TAG": ['FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG'],
        "TAG_CASESENSITIVE": ['FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'CASESENSITIVE']
    }

    # For each mode, we test both lowercase and uppercase queries with CONTAINS, so we
    # can check both prefix and suffix modes.
    queries_expectations = {
        "TEXT": {
            "lowercase":
            {  "query": ["@t:(*el*)"],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            },
            "lowercase_params":
            {  "query": ["@t:(*$p*)", "PARAMS", "2", "p", "el", "DIALECT", "2" ],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            },
            "uppercase":
            {  "query": ["@t:(*EL*)"],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            },
            "uppercase_params":
            {  "query": ["@t:(*$p*)", "PARAMS", "2", "p", "EL", "DIALECT", "2" ],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            },
        },
        "TAG": {
            "lowercase":
            {  "query": ["@t:{*el*}"],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            },
            "lowercase_params":
            {  "query": ["@t:{*$p*}", "PARAMS", "2", "p", "el", "DIALECT", "2"],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            },
            "uppercase": {
                "query": ["@t:{*EL*}"],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            },
            "uppercase_params": {
                "query": ["@t:{*$p*}", "PARAMS", "2", "p", "EL", "DIALECT", "2"],
                "expectation": [4, 'doc1', 'doc2', 'doc3', 'doc4']
            }
        },
        "TAG_CASESENSITIVE": {
            "lowercase":
            {  "query": ["@t:{*el*}"],
                "expectation": [2, 'doc1', 'doc3']
            },
            "lowercase_params":
            {  "query": ["@t:{*$p*}", "PARAMS", "2", "p", "el", "DIALECT", "2"],
                "expectation": [2, 'doc1', 'doc3']
            },
            "uppercase": {
                "query": ["@t:{*EL*}"],
                "expectation": [2, 'doc2', 'doc4']
            },
            "uppercase_params": {
                "query": ["@t:{*$p*}", "PARAMS", "2", "p", "EL", "DIALECT", "2"],
                "expectation": [2, 'doc2', 'doc4']
            }
        }
    }

    for mode in modes:
        env.expect(*create_functions[mode]).ok()
        conn.execute_command('HSET', 'doc1', 't', 'hello')
        conn.execute_command('HSET', 'doc2', 't', 'HELLO')
        conn.execute_command('HSET', 'doc3', 't', 'help')
        conn.execute_command('HSET', 'doc4', 't', 'HELP')
        for case in queries_expectations[mode]:
            query = queries_expectations[mode][case]["query"]
            expectation = queries_expectations[mode][case]["expectation"]
            res = env.cmd('ft.search', 'idx', *query, 'NOCONTENT')
            # Sort to avoid coordinator reorder.
            docs = res[1:]
            docs.sort()
            env.assertEqual(res[0], expectation[0])
            env.assertEqual(docs, expectation[1:])
        env.expect('FT.DROP', 'idx').ok()


def testSortBy(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text', 'sortable', 'bar', 'numeric', 'sortable').ok()
    N = 100
    for i in range(N):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello%03d world' % i, 'bar', 100 - i).ok()
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo')
        env.assertEqual([100, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo', 'desc')
        env.assertEqual([100, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'desc')
        env.assertEqual([100, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'asc')
        env.assertEqual([100, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)

        res = env.cmd('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withscores', 'limit', '2', '5')
        env.assertEqual(
            [100, 'doc2', '1', 'doc3', '1', 'doc4', '1', 'doc5', '1', 'doc6', '1'], res)

        res = env.cmd('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertEqual(
            [100, 'doc0', '#100', 'doc1', '#99', 'doc2', '#98', 'doc3', '#97', 'doc4', '#96'], res)
        res = env.cmd('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'foo', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertEqual([100, 'doc99', '$hello099 world', 'doc98', '$hello098 world', 'doc97', '$hello097 world', 'doc96',
                              '$hello096 world', 'doc95', '$hello095 world'], res)

def testSortByWithoutSortable(env):
    env.expect('ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'baz', 'text', 'sortable').ok()
    N = 100
    for i in range(N):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                   'foo', 'hello%03d world' % i, 'bar', 100 - i).ok()
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')

        # test text
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo')
        env.assertEqual([100, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo', 'desc')
        env.assertEqual([100, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
        res = env.cmd('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'foo', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertEqual([100, 'doc99', '$hello099 world', 'doc98', '$hello098 world', 'doc97', '$hello097 world', 'doc96',
                              '$hello096 world', 'doc95', '$hello095 world'], res)

        # test numeric
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'desc')
        env.assertEqual([100, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = env.cmd(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'asc')
        env.assertEqual([100, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)

        res = env.cmd('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withscores', 'limit', '2', '5')
        env.assertEqual(
            [100, 'doc2', '1', 'doc3', '1', 'doc4', '1', 'doc5', '1', 'doc6', '1'], res)

        res = env.cmd('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertEqual(
            [100, 'doc0', '#100', 'doc1', '#99', 'doc2', '#98', 'doc3', '#97', 'doc4', '#96'], res)


def testSortByWithTie(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'schema', 't', 'text').ok()
    for i in range(10):
        conn.execute_command('hset', i, 't', 'hello')

    # Assert that the order of results is the same in both configurations (by ascending id).
    res1 = env.cmd('ft.search', 'idx', 'hello', 'nocontent')
    res2 = env.cmd('ft.search', 'idx', 'hello', 'nocontent', 'sortby', 't')
    env.assertEqual(res1, res2)


def testNot(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text').ok()
    N = 10
    for i in range(N):
        env.assertEqual(1, conn.execute_command('hset', 'doc%d' % i, 'foo', 'constant term%d' % (random.randrange(0, 5))))

    for i in range(5):
        inclusive = env.cmd(
            'ft.search', 'idx', 'constant term%d' % i, 'nocontent', 'limit', 0, N)

        exclusive = env.cmd(
            'ft.search', 'idx', 'constant -term%d' % i, 'nocontent', 'limit', 0, N)
        exclusive2 = env.cmd(
            'ft.search', 'idx', '-(term%d)' % i, 'nocontent', 'limit', 0, N)
        exclusive3 = env.cmd(
            'ft.search', 'idx', '(-term%d) (constant)' % i, 'nocontent', 'limit', 0, N)

        env.assertNotEqual(inclusive[0], N)
        env.assertEqual(inclusive[0] + exclusive[0], N)
        env.assertEqual(exclusive3[0], exclusive2[0])
        env.assertEqual(exclusive3[0], exclusive[0])

        s1, s2, s3, s4 = set(inclusive[1:]), set(
            exclusive[1:]), set(exclusive2[1:]), set(exclusive3[1:])
        env.assertTrue(s1.difference(s2) == s1)
        env.assertTrue(s1.difference(s3) == s1)
        env.assertTrue(s1.difference(s4) == s1)
        env.assertTrue(s2 == s3)
        env.assertTrue(s2 == s4)
        env.assertTrue(s2.intersection(s1) == set())
        env.assertTrue(s3.intersection(s1) == set())
        env.assertTrue(s4.intersection(s1) == set())

    # NOT on a non existing term
    env.assertEqual(env.cmd(
        'ft.search', 'idx', 'constant -dasdfasdf', 'nocontent')[0], N)
    # not on env term
    env.expect('ft.search', 'idx', 'constant -constant', 'nocontent').equal([0])

    env.expect('ft.search', 'idx', 'constant -(term0|term1|term2|term3|term4|nothing)', 'nocontent').equal([0])
    # env.assertEqual(env.cmd('ft.search', 'idx', 'constant -(term1 term2)', 'nocontent')[0], N)

def testNestedIntersection(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'a', 'text', 'b', 'text', 'c', 'text', 'd', 'text').ok()
    for i in range(20):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'a', 'foo', 'b', 'bar', 'c', 'baz', 'd', 'gaz').ok()
    res = [
        env.cmd('ft.search', 'idx',
                          'foo bar baz gaz', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          '@a:foo @b:bar @c:baz @d:gaz', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          '@b:bar @a:foo @c:baz @d:gaz', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          '@c:baz @b:bar @a:foo @d:gaz', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          '@d:gaz @c:baz @b:bar @a:foo', 'nocontent'),
        env.cmd(
            'ft.search', 'idx', '@a:foo (@b:bar (@c:baz @d:gaz))', 'nocontent'),
        env.cmd(
            'ft.search', 'idx', '@c:baz (@a:foo (@b:bar (@c:baz @d:gaz)))', 'nocontent'),
        env.cmd(
            'ft.search', 'idx', '@b:bar (@a:foo (@c:baz @d:gaz))', 'nocontent'),
        env.cmd(
            'ft.search', 'idx', '@d:gaz (@a:foo (@c:baz @b:bar))', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          'foo (bar baz gaz)', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          'foo (bar (baz gaz))', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          'foo (bar (foo bar) (foo bar))', 'nocontent'),
        env.cmd('ft.search', 'idx',
                          'foo (foo (bar baz (gaz)))', 'nocontent'),
        env.cmd('ft.search', 'idx', 'foo (foo (bar (baz (gaz (foo bar (gaz))))))', 'nocontent')]

    for i, r in enumerate(res):
        # print i, res[0], r
        env.assertEqual(res[0], r)

def testInKeys(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text').ok()

    for i in range(200):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello world').ok()

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for keys in (
            ['doc%d' % i for i in range(10)], ['doc%d' % i for i in range(0, 30, 2)], [
                'doc%d' % i for i in range(99, 0, -5)]
        ):
            res = env.cmd(
                'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', len(keys), *keys)
            env.assertEqual(len(keys), res[0])
            env.assertTrue(all((k in res for k in keys)))

        env.assertEqual(0, env.cmd(
            'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', 3, 'foo', 'bar', 'baz')[0])

    with env.assertResponseError():
        env.cmd('ft.search', 'idx', 'hello', 'INKEYS', 99)
    with env.assertResponseError():
        env.cmd('ft.search', 'idx', 'hello', 'INKEYS', -1)
    with env.assertResponseError():
        env.cmd('ft.search', 'idx', 'hello', 'inkeys', 4, 'foo')

def testSlopInOrder(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text').ok()
    env.expect('ft.add', 'idx', 'doc1', 1, 'fields', 'title', 't1 t2').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 't1 t3 t2').ok()
    env.expect('ft.add', 'idx', 'doc3', 1, 'fields', 'title', 't1 t3 t4 t2').ok()
    env.expect('ft.add', 'idx', 'doc4', 1, 'fields', 'title', 't1 t3 t4 t5 t2').ok()

    res = env.cmd(
        'ft.search', 'idx', 't1|t4 t3|t2', 'slop', '0', 'inorder', 'nocontent')
    env.assertEqual({'doc3', 'doc4', 'doc2', 'doc1'}, set(res[1:]))
    res = env.cmd(
        'ft.search', 'idx', 't2 t1', 'slop', '0', 'nocontent')
    env.assertEqual(1, res[0])
    env.assertEqual('doc1', res[1])
    env.assertEqual(0, env.cmd(
        'ft.search', 'idx', 't2 t1', 'slop', '0', 'inorder')[0])
    env.assertEqual(1, env.cmd(
        'ft.search', 'idx', 't1 t2', 'slop', '0', 'inorder')[0])

    env.assertEqual(2, env.cmd(
        'ft.search', 'idx', 't1 t2', 'slop', '1', 'inorder')[0])
    env.assertEqual(3, env.cmd(
        'ft.search', 'idx', 't1 t2', 'slop', '2', 'inorder')[0])
    env.assertEqual(4, env.cmd(
        'ft.search', 'idx', 't1 t2', 'slop', '3', 'inorder')[0])
    env.assertEqual(4, env.cmd(
        'ft.search', 'idx', 't1 t2', 'inorder')[0])
    env.assertEqual(0, env.cmd(
        'ft.search', 'idx', 't t1', 'inorder')[0])
    env.assertEqual(2, env.cmd(
        'ft.search', 'idx', 't1 t2 t3 t4')[0])
    env.assertEqual(0, env.cmd(
        'ft.search', 'idx', 't1 t2 t3 t4', 'inorder')[0])


def testSlopInOrderIssue1986(env):
    # test with qsort optimization on intersect iterator
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text').ok()

    env.expect('ft.add', 'idx', 'doc1', 1, 'fields', 'title', 't1 t2').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 't2 t1').ok()
    env.expect('ft.add', 'idx', 'doc3', 1, 'fields', 'title', 't1').ok()

    # before fix, both queries returned `doc2`
    env.assertEqual([1, 'doc2', ['title', 't2 t1']], env.cmd(
        'ft.search', 'idx', 't2 t1', 'slop', '0', 'inorder'))
    env.assertEqual([1, 'doc1', ['title', 't1 t2']], env.cmd(
        'ft.search', 'idx', 't1 t2', 'slop', '0', 'inorder'))

def testExact(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'title', 'text', 'weight', 10.0, 'body', 'text').ok()
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields',
               'title', 'hello world', 'body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1.0, 'fields',
               'title', 'hello another world', 'body', 'lorem ist ipsum lorem lorem').ok()

    res = env.cmd('ft.search', 'idx', '"hello world"', 'verbatim')
    env.assertEqual(3, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc1", res[1])

    res = env.cmd('ft.search', 'idx', "hello \"another world\"", 'verbatim')
    env.assertEqual(3, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])


def testGeoErrors(env):
    env.expect('flushall')
    env.expect('ft.create idx ON HASH schema name text location geo').equal('OK')
    env.expect('ft.add idx hotel 1.0 fields name hill location -0.1757,51.5156').equal('OK')
    env.expect('ft.search idx hilton geofilter location -0.1757 51.5156 1 km').equal([0])

    # Insert error - works fine with out of keyspace implementation
    # env.expect('ft.add', 'idx', 'hotel1', 1, 'fields', 'name', '_hotel1', 'location', '1, 1').error()   \
    #        .contains('Could not index geo value')

    # Query errors
    env.expect('ft.search idx hilton geofilter location lon 51.5156 1 km').error()   \
            .contains('Bad arguments for <lon>: Could not convert argument to expected type')
    env.expect('ft.search idx hilton geofilter location 51.5156 lat 1 km').error()   \
            .contains('Bad arguments for <lat>: Could not convert argument to expected type')
    env.expect('ft.search idx hilton geofilter location -0.1757 51.5156 radius km').error()   \
            .contains('Bad arguments for <radius>: Could not convert argument to expected type')
    env.expect('ft.search idx hilton geofilter location -0.1757 51.5156 1 fake').error()   \
            .contains('Unknown distance unit fake')
    env.expect('ft.search idx hilton geofilter location -0.1757 51.5156 1').error()   \
            .contains('GEOFILTER requires 5 arguments')

def testGeo(env):
    gsearch = lambda query, lon, lat, dist, unit='km': env.cmd(
        'ft.search', 'idx', query, 'geofilter', 'location', lon, lat, dist, unit, 'LIMIT', 0, 20)

    gsearch_inline = lambda query, lon, lat, dist, unit='km': env.cmd(
        'ft.search', 'idx', '{} @location:[{} {} {} {}]'.format(query,  lon, lat, dist, unit), 'LIMIT', 0, 20)

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'name', 'text', 'location', 'geo').ok()

    for i, hotel in enumerate(hotels):
        env.assertOk(env.cmd('ft.add', 'idx', 'hotel{}'.format(i), 1.0, 'fields', 'name',
                                        hotel[0], 'location', '{},{}'.format(hotel[2], hotel[1])))

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('ft.search', 'idx', 'hilton')
        env.assertEqual(len(hotels), res[0])

        res = gsearch('hilton', "-0.1757", "51.5156", '1')
        env.assertEqual(3, res[0])
        env.assertContains('hotel2', res)
        env.assertContains('hotel21', res)
        env.assertContains('hotel79', res)
        res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '1')
        env.assertEqual(py2sorted(res), py2sorted(res2))

        res = gsearch('hilton', "-0.1757", "51.5156", '10')
        env.assertEqual(14, res[0])

        res2 = gsearch('hilton', "-0.1757", "51.5156", '10000', 'm')
        env.assertEqual(py2sorted(res), py2sorted(res2))
        res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '10')
        env.assertEqual(py2sorted(res), py2sorted(res2))

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'm')
        env.assertEqual(1, res[0])
        env.assertEqual('hotel94', res[1])
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '10', 'm')
        env.assertEqual(py2sorted(res), py2sorted(res2))

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'km')
        env.assertEqual(5, res[0])
        env.assertContains('hotel94', res)
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '10', 'km')
        env.assertEqual(5, res2[0])
        env.assertEqual(py2sorted(res), py2sorted(res2))

        res = gsearch('heathrow', -0.44155, 51.45865, '5', 'km')
        env.assertEqual(3, res[0])
        env.assertContains('hotel94', res)
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '5', 'km')
        env.assertEqual(py2sorted(res), py2sorted(res2))

def testTagErrors(env):
    env.expect("ft.create", "test", 'ON', 'HASH',
                "SCHEMA",  "tags", "TAG").equal('OK')
    env.expect("ft.add", "test", "1", "1", "FIELDS", "tags", "alberta").equal('OK')
    env.expect("ft.add", "test", "2", "1", "FIELDS", "tags", "ontario. alberta").equal('OK')

@skip(cluster=True)
def testGeoDeletion(env):
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema',
            'g1', 'geo', 'g2', 'geo', 't1', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
            'g1', "-0.1757,51.5156",
            'g2', "-0.1757,51.5156",
            't1', "hello")
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
            'g1', "-0.1757,51.5156",
            'g2', "-0.1757,51.5156",
            't1', "hello")
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'fields',
            'g1', "-0.1757,51.5156",
            't1', "hello")

    # keys are: "geo:idx/g1" and "geo:idx/g2"
    env.assertEqual(3, len(env.cmd('FT.DEBUG DUMP_NUMIDX idx g1')[0]))
    env.assertEqual(2, len(env.cmd('FT.DEBUG DUMP_NUMIDX idx g2')[0]))

    # Remove the first doc
    env.cmd('ft.del', 'idx', 'doc1')
    for _ in range(100):
        forceInvokeGC(env, 'idx')
    env.assertEqual(2, len(env.cmd('FT.DEBUG DUMP_NUMIDX idx g1')[0]))
    env.assertEqual(1, len(env.cmd('FT.DEBUG DUMP_NUMIDX idx g2')[0]))

    # Replace the other one:
    env.cmd('ft.add', 'idx', 'doc2', 1.0,
            'replace', 'fields',
            't1', 'just text here')
    for _ in range(100):
        forceInvokeGC(env, 'idx')
    env.assertEqual(1, len(env.cmd('FT.DEBUG DUMP_NUMIDX idx g1')[0]))
    env.assertEqual(0, len(env.cmd('FT.DEBUG DUMP_NUMIDX idx g2')[0]))

def testInfields(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'title', 'text', 'weight', 10.0, 'body', 'text', 'weight', 1.0).ok()
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields',
               'title', 'hello world', 'body', 'lorem ipsum').ok()

    env.expect('ft.add', 'idx', 'doc2', 1.0, 'fields',
               'title', 'hello world lorem ipsum', 'body', 'hello world').ok()

    res = env.cmd(
        'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "title", "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])
    env.assertEqual("doc2", res[1])
    env.assertEqual("doc1", res[2])

    res = env.cmd(
        'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "body", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

    res = env.cmd(
        'ft.search', 'idx', 'hello', 'verbatim', "infields", 1, "body", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

    res = env.cmd(
        'ft.search', 'idx',  '\"hello world\"', 'verbatim', "infields", 1, "body", "nocontent")

    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

    res = env.cmd(
        'ft.search', 'idx', '\"lorem ipsum\"', 'verbatim', "infields", 1, "body", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc1", res[1])

    res = env.cmd(
        'ft.search', 'idx', 'lorem ipsum', "infields", 2, "body", "title", "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])
    env.assertEqual("doc2", res[1])
    env.assertEqual("doc1", res[2])

def testScorerSelection(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()

    # this is the default scorer
    res = env.cmd(
        'ft.search', 'idx', 'foo', 'scorer', 'TFIDF')
    env.assertEqual(res, [0])
    with env.assertResponseError():
        res = env.cmd(
            'ft.search', 'idx', 'foo', 'scorer', 'NOSUCHSCORER')

def testFieldSelectors(env):
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc',
        'schema', 'TiTle', 'text', 'BoDy', 'text', "יוניקוד", 'text', 'field.with,punct', 'text').ok()
    #todo: document as breaking change, ft.add fields name are not case insentive
    env.expect('ft.add', 'idx', 'doc1', 1, 'fields',
               'TiTle', 'hello world', 'BoDy', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt').ok()
    env.expect('ft.add', 'idx', 'doc2', 0.5, 'fields',
               'BoDy', 'hello world', 'TiTle', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt').ok()

    res = env.cmd(
        'ft.search', 'idx', '@TiTle:hello world', 'nocontent')
    env.assertEqual(res, [1, 'doc1'])
    res = env.cmd(
        'ft.search', 'idx', '@BoDy:hello world', 'nocontent')
    env.assertEqual(res, [1, 'doc2'])

    res = env.cmd(
        'ft.search', 'idx', '@BoDy:hello @TiTle:world', 'nocontent')
    env.assertEqual(res, [0])

    res = env.cmd(
        'ft.search', 'idx', '@BoDy:hello world @TiTle:world', 'nocontent')
    env.assertEqual(res, [0])
    res = env.cmd(
        'ft.search', 'idx', '@BoDy:(hello|foo) @TiTle:(world|bar)', 'nocontent')
    env.assertEqual(py2sorted(res), py2sorted([2, 'doc1', 'doc2']))

    res = env.cmd(
        'ft.search', 'idx', '@BoDy:(hello|foo world|bar)', 'nocontent')
    env.assertEqual(py2sorted(res), py2sorted([2, 'doc1', 'doc2']))

    res = env.cmd(
        'ft.search', 'idx', '@BoDy|TiTle:(hello world)', 'nocontent')
    env.assertEqual(py2sorted(res), py2sorted([2, 'doc1', 'doc2']))

    res = env.cmd(
        'ft.search', 'idx', '@יוניקוד:(unicode)', 'nocontent')
    env.assertEqual(py2sorted(res), py2sorted([2, 'doc1', 'doc2']))

    res = env.cmd(
        'ft.search', 'idx', '@field\\.with\\,punct:(punt)', 'nocontent')
    env.assertEqual(py2sorted(res), py2sorted([2, 'doc1', 'doc2']))

def testStemming(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text').ok()
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                    'title', 'hello kitty').ok()
    env.expect('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'hello kitties').ok()

    res = env.cmd(
        'ft.search', 'idx', 'hello kitty', "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])

    res = env.cmd(
        'ft.search', 'idx', 'hello kitty', "nocontent", "verbatim")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

    # test for unknown language
    with env.assertResponseError():
        res = env.cmd(
            'ft.search', 'idx', 'hello kitty', "nocontent", "language", "foofoofian")

def testExpander(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text').ok()
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                    'title', 'hello kitty').ok()
    res = env.cmd(
        'ft.search', 'idx', 'kitties',
        "nocontent",
        "expander", "SBSTEM"
        )
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

    res = env.cmd(
        'ft.search', 'idx', 'kitties', "nocontent", "expander", "noexpander")
    env.assertEqual(1, len(res))
    env.assertEqual(0, res[0])

    res = env.cmd(
        'ft.search', 'idx', 'kitti', "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

    res = env.cmd(
        'ft.search', 'idx', 'kitti', "nocontent", 'verbatim')
    env.assertEqual(1, len(res))
    env.assertEqual(0, res[0])

    # Calling a stem directly works even with VERBATIM.
    # You need to use the + prefix escaped
    res = env.cmd(
        'ft.search', 'idx', '\\+kitti', "nocontent", 'verbatim')
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

def testNumericRange(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'score', 'numeric', 'price', 'numeric').ok()

    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 5).error().contains("FILTER requires 3 arguments")
    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 5, 'inf').error().contains("Bad upper range: inf")
    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 'inf', 5).error().contains("Bad lower range: inf")

    for i in range(100):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1, 'fields',
                   'title', 'hello kitty', 'score', i, 'price', 100 + 10 * i).ok()

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('ft.search', 'idx', 'hello kitty', "nocontent",
                                "filter", "score", 0, 100)

        env.assertEqual(11, len(res))
        env.assertEqual(100, res[0])

        res = env.cmd('ft.search', 'idx', 'hello kitty', "nocontent",
                                "filter", "score", 0, 50)
        env.assertEqual(51, res[0])

        res = env.cmd('ft.search', 'idx', 'hello kitty', 'verbatim', "nocontent", "limit", 0, 100,
                                "filter", "score", "(0", "(50")

        env.assertEqual(49, res[0])
        res = env.cmd('ft.search', 'idx', 'hello kitty', "nocontent",
                                "filter", "score", "-inf", "+inf")
        env.assertEqual(100, res[0])

        # test multi filters
        scrange = (19, 90)
        prrange = (290, 385)
        res = env.cmd('ft.search', 'idx', 'hello kitty',
                                "filter", "score", scrange[
                                    0], scrange[1],
                                "filter", "price", prrange[0], prrange[1])

        # print res
        for doc in res[2::2]:

            sc = int(doc[doc.index('score') + 1])
            pr = int(doc[doc.index('price') + 1])

            env.assertTrue(sc >= scrange[0] and sc <= scrange[1])
            env.assertGreaterEqual(pr, prrange[0])
            env.assertLessEqual(pr, prrange[1])

        env.assertEqual(10, res[0])

        res = env.cmd('ft.search', 'idx', 'hello kitty',
                                "filter", "score", "19", "90",
                                "filter", "price", "90", "185")

        env.assertEqual(0, res[0])

        # Test numeric ranges as part of query syntax
        res = env.cmd(
            'ft.search', 'idx', 'hello kitty @score:[0 100]', "nocontent")

        env.assertEqual(11, len(res))
        env.assertEqual(100, res[0])

        res = env.cmd(
            'ft.search', 'idx', 'hello kitty @score:[0 50]', "nocontent")
        env.assertEqual(51, res[0])
        res = env.cmd(
            'ft.search', 'idx', 'hello kitty @score:[(0 (50]', 'verbatim', "nocontent")
        env.assertEqual(49, res[0])
        res = env.cmd(
            'ft.search', 'idx', '@score:[(0 (50]', 'verbatim', "nocontent")
        env.assertEqual(49, res[0])
        res = env.cmd(
            'ft.search', 'idx', 'hello kitty -@score:[(0 (50]', 'verbatim', "nocontent", 'limit', 0, 51)
        env.assertEqual(51, res[0])
        env.debugPrint(', '.join(toSortedFlatList(res[2:])), force=TEST_DEBUG)
        env.cmd(
            'ft.profile', 'idx', 'search', 'query', 'hello kitty -@score:[(0 (50]', 'verbatim', "nocontent", 'limit', 0, 51)
        res = env.cmd(
            'ft.search', 'idx', 'hello kitty @score:[-inf +inf]', "nocontent")
        env.assertEqual(100, res[0])

def testNotIter(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'score', 'numeric', 'price', 'numeric').ok()

    for i in range(8):
        conn.execute_command('HSET', 'doc%d' % i, 'title', 'hello kitty', 'score', i, 'price', 100 + 10 * i)

    # middle shunk
    res = env.cmd(
        'ft.search', 'idx', '-@score:[2 4]', 'verbatim', "nocontent")
    env.assertEqual(5, res[0])
    env.debugPrint(', '.join(toSortedFlatList(res[1:])), force=TEST_DEBUG)

    res = env.cmd(
        'ft.search', 'idx', 'hello kitty -@score:[2 4]', 'verbatim', "nocontent")
    env.assertEqual(5, res[0])
    env.debugPrint(', '.join(toSortedFlatList(res[1:])), force=TEST_DEBUG)

    # start chunk
    res = env.cmd(
        'ft.search', 'idx', '-@score:[0 2]', 'verbatim', "nocontent")
    env.assertEqual(5, res[0])
    env.debugPrint(', '.join(toSortedFlatList(res[1:])), force=TEST_DEBUG)

    res = env.cmd(
        'ft.search', 'idx', 'hello kitty -@score:[0 2]', 'verbatim', "nocontent")
    env.assertEqual(5, res[0])
    env.debugPrint(', '.join(toSortedFlatList(res[1:])), force=TEST_DEBUG)

    # end chunk
    res = env.cmd(
        'ft.search', 'idx', '-@score:[5 7]', 'verbatim', "nocontent")
    env.assertEqual(5, res[0])
    env.debugPrint(', '.join(toSortedFlatList(res[1:])), force=TEST_DEBUG)

    res = env.cmd(
        'ft.search', 'idx', 'hello kitty -@score:[5 7]', 'verbatim', "nocontent")
    env.assertEqual(5, res[0])
    env.debugPrint(', '.join(toSortedFlatList(res[1:])), force=TEST_DEBUG)

    # whole chunk
    res = env.cmd(
        'ft.search', 'idx', '-@score:[0 7]', 'verbatim', "nocontent")
    env.assertEqual(0, res[0])
    env.debugPrint(str(len(res)), force=TEST_DEBUG)

    res = env.cmd(
        'ft.search', 'idx', 'hello kitty -@score:[0 7]', 'verbatim', "nocontent")
    env.assertEqual(0, res[0])
    env.debugPrint(str(len(res)), force=TEST_DEBUG)

def testPayload(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'PAYLOAD_FIELD', '__payload', 'schema', 'f', 'text').ok()
    for i in range(10):
        env.expect('ft.add', 'idx', '%d' % i, 1.0,
                 'payload', 'payload %d' % i,
                 'fields', 'f', 'hello world').ok()

    for x in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('ft.search', 'idx', 'hello world')
        env.assertEqual(21, len(res))

        res = env.cmd('ft.search', 'idx', 'hello world', 'withpayloads')
        env.assertEqual(31, len(res))
        env.assertEqual(10, res[0])
        for i in range(1, 30, 3):
            env.assertEqual(res[i + 1], 'payload %s' % res[i])

@skip(cluster=True)
def testGarbageCollector(env):
    if env.moduleArgs is not None and 'GC_POLICY FORK' in env.moduleArgs:
        # this test is not relevent for fork gc cause its not cleaning the last block
        env.skip()

    N = 100
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text').ok()
    waitForIndex(env, 'idx')
    for i in range(N):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0,
                 'fields', 'foo', ' '.join(('term%d' % random.randrange(0, 10) for i in range(10)))).ok()

    def get_stats(r):
        res = r.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        gc_stats = {d['gc_stats'][x]: float(
            d['gc_stats'][x + 1]) for x in range(0, len(d['gc_stats']), 2)}
        d['gc_stats'] = gc_stats
        return d

    stats = get_stats(env)
    if 'current_hz' in stats['gc_stats']:
        env.assertGreater(stats['gc_stats']['current_hz'], 8)
    env.assertEqual(0, stats['gc_stats']['bytes_collected'])
    env.assertGreater(int(stats['num_records']), 0)

    initialIndexSize = float(stats['inverted_sz_mb']) * 1024 * 1024
    for i in range(N):
        env.expect('ft.del', 'idx', 'doc%d' % i).equal(1)

    for _ in range(100):
        # gc is random so we need to do it long enough times for it to work
        forceInvokeGC(env, 'idx')

    stats = get_stats(env)

    env.assertEqual(0, int(stats['num_docs']))
    env.assertEqual(0, int(stats['num_records']))
    if not env.isCluster():
        env.assertEqual(100, int(stats['max_doc_id']))
        if 'current_hz' in stats['gc_stats']:
            env.assertGreater(stats['gc_stats']['current_hz'], 30)
        currentIndexSize = float(stats['inverted_sz_mb']) * 1024 * 1024
        # print initialIndexSize, currentIndexSize,
        # stats['gc_stats']['bytes_collected']
        env.assertGreater(initialIndexSize, currentIndexSize)
        env.assertGreater(stats['gc_stats'][
            'bytes_collected'], currentIndexSize)

    for i in range(10):

        res = env.cmd('ft.search', 'idx', 'term%d' % i)
        env.assertEqual([0], res)

def testReturning(env):
    env.assertCmdOk('ft.create', 'idx', 'ON', 'HASH', 'schema',
                     'f1', 'text',
                     'f2', 'text',
                     'n1', 'numeric', 'sortable',
                     'f3', 'text')
    for i in range(10):
        env.assertCmdOk('ft.add', 'idx', 'DOC_{0}'.format(i), 1.0, 'fields',
                         'f2', 'val2', 'f1', 'val1', 'f3', 'val3',
                         'n1', i)

    # RETURN 0. Simplest case
    for x in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('ft.search', 'idx', 'val*', 'return', '0')
        env.assertEqual(11, len(res))
        env.assertEqual(10, res[0])
        for r in res[1:]:
            env.assertTrue(r.startswith('DOC_'))

    for field in ('f1', 'f2', 'f3', 'n1'):
        res = env.cmd('ft.search', 'idx', 'val*', 'return', 1, field)
        env.assertEqual(21, len(res))
        env.assertEqual(10, res[0])
        for pair in grouper(res[1:], 2):
            docname, fields = pair
            env.assertEqual(2, len(fields))
            env.assertEqual(field, fields[0])
            env.assertTrue(docname.startswith('DOC_'))

    # Test that we don't return SORTBY fields if they weren't specified
    # also in RETURN
    res = env.cmd('ft.search', 'idx', 'val*', 'return', 1, 'f1',
        'sortby', 'n1', 'ASC')
    row = res[2]
    # get the first result
    env.assertEqual(['f1', 'val1'], row)

    # Test when field is not found
    res = env.cmd('ft.search', 'idx', 'val*', 'return', 1, 'nonexist')
    env.assertEqual(21, len(res))
    env.assertEqual(10, res[0])

    # # Test that we don't crash if we're given the wrong number of fields
    with env.assertResponseError():
        res = env.cmd('ft.search', 'idx', 'val*', 'return', 700, 'nonexist')

def _test_create_options_real(env, *options):
    options = [x for x in options if x]
    has_offsets = 'NOOFFSETS' not in options
    has_fields = 'NOFIELDS' not in options
    has_freqs = 'NOFREQS' not in options

    try:
        env.cmd('ft.drop', 'idx')
        # RS 2.0 ft.drop does not remove documents
        env.flush()
    except Exception as e:
        pass

    options = ['idx'] + options + ['ON', 'HASH', 'schema', 'f1', 'text', 'f2', 'text']
    env.assertCmdOk('ft.create', *options)
    for i in range(10):
        env.assertCmdOk('ft.add', 'idx', 'doc{}'.format(
            i), 0.5, 'fields', 'f1', 'value for {}'.format(i))

    # Query
#     res = env.cmd('ft.search', 'idx', "value for 3")
#     if not has_offsets:
#         env.assertIsNone(res)
#     else:
#         env.assertIsNotNone(res)

    # Frequencies:
    env.assertCmdOk('ft.add', 'idx', 'doc100',
                     1.0, 'fields', 'f1', 'foo bar')
    env.assertCmdOk('ft.add', 'idx', 'doc200', 1.0,
                     'fields', 'f1', ('foo ' * 10) + ' bar')
    res = env.cmd('ft.search', 'idx', 'foo')
    env.assertEqual(2, res[0])
    if has_offsets:
        docname = res[1]
        if has_freqs:
            # changed in minminheap PR. TODO: remove
            env.assertEqual('doc100', docname)
        else:
            env.assertEqual('doc100', docname)

    env.assertCmdOk('ft.add', 'idx', 'doc300',
                     1.0, 'fields', 'f1', 'Hello')
    res = env.cmd('ft.search', 'idx', '@f2:Hello')
    if has_fields:
        env.assertEqual(1, len(res))
    else:
        env.assertEqual(3, len(res))

def testCreationOptions(env):
    from itertools import combinations
    for x in range(1, 5):
        for combo in combinations(('NOOFFSETS', 'NOFREQS', 'NOFIELDS', ''), x):
            _test_create_options_real(env, *combo)

    env.expect('ft.create', 'idx').error()

def testInfoCommand(env):
    from itertools import combinations
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'NOFIELDS', 'schema', 'title', 'text').ok()
    N = 50
    for i in range(N):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1, 'replace', 'fields',
                   'title', 'hello term%d' % i).ok()
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')

        res = env.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}

        env.assertEqual(d['index_name'], 'idx')
        env.assertEqual(d['index_options'], ['NOFIELDS'])
        env.assertEqual(
            d['attributes'], [['identifier', 'title', 'attribute', 'title', 'type', 'TEXT', 'WEIGHT', '1']])

        if not env.isCluster():
            env.assertEqual(int(d['num_docs']), N)
            env.assertEqual(int(d['num_terms']), N + 1)
            env.assertEqual(int(d['max_doc_id']), N)
            env.assertEqual(int(d['records_per_doc_avg']), 2)
            env.assertEqual(int(d['num_records']), N * 2)

            env.assertGreater(float(d['offset_vectors_sz_mb']), 0)
            env.assertGreater(float(d['key_table_size_mb']), 0)
            env.assertGreater(float(d['inverted_sz_mb']), 0)
            env.assertGreater(float(d['bytes_per_record_avg']), 0)
            env.assertGreater(float(d['doc_table_size_mb']), 0)

    for x in range(1, 6):
        for combo in combinations(('NOOFFSETS', 'NOFREQS', 'NOFIELDS', 'NOHL', ''), x):
            combo = list(filter(None, combo))
            options = combo + ['schema', 'f1', 'text']
            try:
                env.cmd('ft.drop', 'idx')
            except:
                pass
            env.assertCmdOk('ft.create', 'idx', 'ON', 'HASH', *options)
            info = env.cmd('ft.info', 'idx')
            ix = info.index('index_options')
            env.assertFalse(ix == -1)

            opts = info[ix + 1]
            # make sure that an empty opts string returns no options in
            # info
            if not combo:
                env.assertEqual([], opts)

            for option in filter(None, combo):
                env.assertTrue(option in opts)

def testInfoCommandImplied(env):
    ''' Test that NOHL is implied by NOOFFSETS '''
    env.assertCmdOk('ft.create', 'idx', 'ON', 'HASH', 'NOOFFSETS', 'schema', 'f1', 'text')
    res = env.cmd('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertNotEqual(-1, d['index_options'].index('NOOFFSETS'))
    env.assertNotEqual(-1, d['index_options'].index('NOHL'))

def testNoStem(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'schema', 'body', 'text', 'name', 'text', 'nostem')
    if not env.isCluster():
        # todo: change it to be more generic to pass on isCluster
        res = env.cmd('ft.info', 'idx')
        env.assertEqual(res[7][1][8], 'NOSTEM')
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        try:
            env.cmd('ft.del', 'idx', 'doc')
        except redis.ResponseError:
            pass

        # Insert a document
        env.assertCmdOk('ft.add', 'idx', 'doc', 1.0, 'fields',
                         'body', "located",
                         'name', "located")

        # Now search for the fields
        res_body = env.cmd('ft.search', 'idx', '@body:location')
        res_name = env.cmd('ft.search', 'idx', '@name:location')
        env.assertEqual(0, res_name[0])
        env.assertEqual(1, res_body[0])

def testSortbyMissingField(env):
    # GH Issue 131
    #
    env.cmd('ft.create', 'ix', 'ON', 'HASH', 'schema', 'txt',
             'text', 'num', 'numeric', 'sortable')
    env.cmd('ft.add', 'ix', 'doc1', 1.0, 'fields', 'txt', 'foo', 'noexist', 3.14)

    env.expect('ft.search', 'ix', 'foo', 'sortby', 'num')                       \
        .equal([1, 'doc1', ['txt', 'foo', 'noexist', '3.14']])
    env.expect('ft.search', 'ix', 'foo', 'sortby', 'noexist').error()           \
        .contains('Property `noexist` not loaded nor in schema')

    env.expect('ft.aggregate', 'ix', 'foo', 'load', 2, '@__key', '@num', 'sortby', 2, '@num', 'asc')            \
        .equal([1, ['__key', 'doc1']])
    env.expect('ft.aggregate', 'ix', 'foo', 'load', 2, '@__key', '@noexist', 'sortby', 2, '@noexist', 'asc')    \
        .equal([1, ['__key', 'doc1', 'noexist', '3.14']])

def testParallelIndexing(env):
    # GH Issue 207
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'txt', 'text')
    from threading import Thread
    env.getConnection()
    ndocs = 100

    def runner(tid):
        cli = env.getConnection()
        for num in range(ndocs):
            cli.execute_command('ft.add', 'idx', 'doc{}_{}'.format(tid, num), 1.0,
                                'fields', 'txt', 'hello world' * 20)
    ths = []
    for tid in range(10):
        ths.append(Thread(target=runner, args=(tid,)))

    [th.start() for th in ths]
    [th.join() for th in ths]
    res = env.cmd('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertEqual(1000, int(d['num_docs']))

def testDoubleAdd(env):
    # Tests issue #210
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'txt', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'txt', 'hello world')
    with env.assertResponseError():
        env.cmd('ft.add', 'idx', 'doc1', 1.0,
                 'fields', 'txt', 'goodbye world')

    env.assertEqual('hello world', env.cmd('ft.get', 'idx', 'doc1')[1])
    env.assertEqual(0, env.cmd('ft.search', 'idx', 'goodbye')[0])
    env.assertEqual(1, env.cmd('ft.search', 'idx', 'hello')[0])

    # Now with replace
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'replace',
             'fields', 'txt', 'goodbye world')
    env.assertEqual(1, env.cmd('ft.search', 'idx', 'goodbye')[0])
    env.assertEqual(0, env.cmd('ft.search', 'idx', 'hello')[0])
    env.assertEqual('goodbye world', env.cmd('ft.get', 'idx', 'doc1')[1])

def testConcurrentErrors(env):
    # Workaround for: Can't pickle local object 'testConcurrentErrors.<locals>.thrfn'
    if sys.version_info >= (3, 8):
        env.skip()

    from multiprocessing import Process
    import random

    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'txt', 'text')
    docs_per_thread = 100
    num_threads = 50

    docIds = ['doc{}'.format(x) for x in range(docs_per_thread)]

    def thrfn():
        myIds = docIds[::]
        random.shuffle(myIds)
        cli = env.getConnection()
        with cli.pipeline(transaction=False) as pl:
            for x in myIds:
                pl.execute_command('ft.add', 'idx', x, 1.0,
                                   'fields', 'txt', ' hello world ' * 50)
            try:
                pl.execute()
            except Exception as e:
                pass
                # print e

    thrs = [Process(target=thrfn) for x in range(num_threads)]
    [th.start() for th in thrs]
    [th.join() for th in thrs]
    res = env.cmd('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertEqual(100, int(d['num_docs']))

def testBinaryKeys(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'txt', 'text')
    # Insert a document
    env.cmd('ft.add', 'idx', 'Hello', 1.0, 'fields', 'txt', 'NoBin match')
    env.cmd('ft.add', 'idx', 'Hello\x00World', 1.0, 'fields', 'txt', 'Bin match')
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        exp = [2, 'Hello\x00World', ['txt', 'Bin match'], 'Hello', ['txt', 'NoBin match']]
        res = env.cmd('ft.search', 'idx', 'match')
        for r in res:
            env.assertContains(r, exp)

@skip(cluster=True)
def testNonDefaultDb(env):
    # Should be ok
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH', 'schema', 'txt', 'text')
    try:
        env.cmd('SELECT 1')
    except redis.ResponseError:
        return

    # Should fail
    with env.assertResponseError():
        env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH', 'schema', 'txt', 'text')

def testDuplicateNonspecFields(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'schema', 'txt', 'text').ok()
    env.expect('FT.ADD', 'idx', 'doc', 1.0, 'fields',
                'txt', 'foo', 'f1', 'f1val', 'f1', 'f1val2', 'F1', 'f1Val3').ok()
    res = env.cmd('ft.get', 'idx', 'doc')
    res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertTrue(res['f1'] in ('f1val', 'f1val2'))
    env.assertEqual('f1Val3', res['F1'])

def testDuplicateFields(env):
    # As of RS 2.0 it is allowed. only latest field will be saved and indexed
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC', 'SORTABLE')

    env.expect('FT.ADD', 'idx', 'doc', 1.0, 'FIELDS',
        'txt', 'foo', 'txt', 'bar', 'txt', 'baz').ok()
    env.expect('FT.SEARCH idx *').equal([1, 'doc', ['txt', 'baz']])

def testDuplicateSpec(env):
    with env.assertResponseError():
        env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
                'SCHEMA', 'f1', 'text', 'n1', 'numeric', 'f1', 'text')

def testSortbyMissingFieldSparse(env):
    # Note, the document needs to have one present sortable field in
    # order for the indexer to give it a sort vector
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'SCHEMA', 'lastName', 'text', 'SORTABLE', 'firstName', 'text', 'SORTABLE')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'lastName', 'mark')
    waitForIndex(env, 'idx')
    res = env.cmd('ft.search', 'idx', 'mark', 'WITHSORTKEYS', "SORTBY",
                   "firstName", "ASC", "limit", 0, 100)
    # commented because we don't filter out exclusive sortby fields
    # env.assertEqual([1, 'doc1', None, ['lastName', 'mark']], res)

def testLanguageField(env):
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'language', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc1', 1.0,
             'FIELDS', 'language', 'gibberish')
    res = env.cmd('FT.SEARCH', 'idx', 'gibberish')
    env.assertEqual([1, 'doc1', ['language', 'gibberish']], res)
    # The only way I can verify that LANGUAGE is parsed twice is ensuring we
    # provide a wrong language. This is much easier to test than trying to
    # figure out how a given word is stemmed
    with env.assertResponseError():
        env.cmd('FT.ADD', 'idx', 'doc1', 1.0, 'LANGUAGE',
                 'blah', 'FIELDS', 'language', 'gibber')

def testUninitSortvector(env):
    # This would previously crash
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'f1', 'TEXT')
    for x in range(2000):
        env.cmd('FT.ADD', 'idx', 'doc{}'.format(
            x), 1.0, 'FIELDS', 'f1', 'HELLO')

    env.broadcast('SAVE')
    for x in range(10):
        env.broadcast('DEBUG RELOAD')


def normalize_row(row):
    return to_dict(row)

def assertResultsEqual(env, exp, got, inorder=True):
    from pprint import pprint
    # pprint(exp)
    # pprint(got)
    env.assertEqual(exp[0], got[0])
    env.assertEqual(len(exp), len(got))

    exp = list(grouper(exp[1:], 2))
    got = list(grouper(got[1:], 2))

    for x in range(len(exp)):
        exp_did, exp_fields = exp[x]
        got_did, got_fields = got[x]
        env.assertEqual(exp_did, got_did, message="at position {}".format(x))
        got_fields = to_dict(got_fields)
        exp_fields = to_dict(exp_fields)
        env.assertEqual(exp_fields, got_fields, message="at position {}".format(x))

def testAlterIndex(env):
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'f1', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'f1', 'hello', 'f2', 'world')
    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f2', 'TEXT')
    waitForIndex(env, 'idx')
    env.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'FIELDS', 'f1', 'hello', 'f2', 'world')

    # RS 2.0 reindex and after reload both documents are found
    # for _ in env.reloadingIterator():
    res = env.cmd('FT.SEARCH', 'idx', 'world')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc2', ['f1', 'hello', 'f2', 'world'], 'doc1', ['f1', 'hello', 'f2', 'world']]))
    # env.assertEqual([1, 'doc2', ['f1', 'hello', 'f2', 'world']], ret)

    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f3', 'TEXT', 'SORTABLE')
    for x in range(10):
        env.cmd('FT.ADD', 'idx', 'doc{}'.format(x + 3), 1.0,
                 'FIELDS', 'f1', 'hello', 'f3', 'val{}'.format(x))

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        # Test that sortable works
        res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SORTBY', 'f3', 'DESC')
        exp = [12, 'doc12', ['f1', 'hello', 'f3', 'val9'], 'doc11', ['f1', 'hello', 'f3', 'val8'],
                   'doc10', ['f1', 'hello', 'f3', 'val7'], 'doc9',  ['f1', 'hello', 'f3', 'val6'],
                   'doc8',  ['f1', 'hello', 'f3', 'val5'], 'doc7',  ['f1', 'hello', 'f3', 'val4'],
                   'doc6',  ['f1', 'hello', 'f3', 'val3'], 'doc5',  ['f1', 'hello', 'f3', 'val2'],
                   'doc4',  ['f1', 'hello', 'f3', 'val1'], 'doc3',  ['f1', 'hello', 'f3', 'val0']]
        assertResultsEqual(env, exp, res)

    # Test that we can add a numeric field
    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n1', 'NUMERIC')
    env.cmd('FT.ADD', 'idx', 'docN1', 1.0, 'FIELDS', 'n1', 50)
    env.cmd('FT.ADD', 'idx', 'docN2', 1.0, 'FIELDS', 'n1', 250)
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('FT.SEARCH', 'idx', '@n1:[0 100]')
        env.assertEqual([1, 'docN1', ['n1', '50']], res)

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'NOT_ADD', 'f2', 'TEXT').error()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD').error()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f2').error()
    env.expect('FT.ALTER', 'idx', 'ADD', 'SCHEMA', 'f2', 'TEXT').error()
    env.expect('FT.ALTER', 'idx', 'f2', 'TEXT').error()

def testAlterValidation(env):
    # Test that constraints for ALTER comand
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH', 'SCHEMA', 'f0', 'TEXT')
    for x in range(1, 32):
        env.cmd('FT.ALTER', 'idx1', 'SCHEMA', 'ADD', 'f{}'.format(x), 'TEXT')
    # OK for now.

    # Should be too many indexes
    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER',
                      'idx1', 'SCHEMA', 'ADD', 'tooBig', 'TEXT')

    env.cmd('FT.CREATE', 'idx2', 'MAXTEXTFIELDS', 'ON', 'HASH', 'SCHEMA', 'f0', 'TEXT')
    # print env.cmd('FT.INFO', 'idx2')
    for x in range(1, 50):
        env.cmd('FT.ALTER', 'idx2', 'SCHEMA', 'ADD', 'f{}'.format(x + 1), 'TEXT')

    env.cmd('FT.ADD', 'idx2', 'doc1', 1.0, 'FIELDS', 'f50', 'hello')
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx2')
        ret = env.cmd('FT.SEARCH', 'idx2', '@f50:hello')
        env.assertEqual([1, 'doc1', ['f50', 'hello']], ret)

    env.cmd('FT.CREATE', 'idx3', 'ON', 'HASH', 'SCHEMA', 'f0', 'text')
    # Try to alter the index with garbage
    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER', 'idx3',
                      'SCHEMA', 'ADD', 'f1', 'TEXT', 'f2', 'garbage')
    ret = to_dict(env.cmd('ft.info', 'idx3'))
    env.assertEqual(1, len(ret['attributes']))

    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER',
                      'nonExist', 'SCHEMA', 'ADD', 'f1', 'TEXT')

    # test with no fields!
    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER', 'idx2', 'SCHEMA', 'ADD')

    # test with no fields!
    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER', 'idx2', 'SCHEMA', 'ADD')

def testIssue366_2(env):
    # FT.CREATE atest SCHEMA textfield TEXT numfield NUMERIC
    # FT.ADD atest anId 1 PAYLOAD '{"hello":"world"}' FIELDS textfield sometext numfield 1234
    # FT.ADD atest anId 1 PAYLOAD '{"hello":"world2"}' REPLACE PARTIAL FIELDS numfield 1111
    # shutdown
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH',
            'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
    env.cmd('FT.ADD', 'idx1', 'doc1', 1, 'PAYLOAD', '{"hello":"world"}',
             'FIELDS', 'textfield', 'sometext', 'numfield', 1234)
    env.cmd('ft.add', 'idx1', 'doc1', 1,
             'PAYLOAD', '{"hello":"world2"}',
             'REPLACE', 'PARTIAL',
             'FIELDS', 'textfield', 'sometext', 'numfield', 1111)
    for _ in env.reloadingIterator():
        pass  #

def testIssue654(env):
    # Crashes during FILTER
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id', 'numeric')
    env.cmd('ft.add', 'idx', 1, 1, 'fields', 'id', 1)
    env.cmd('ft.add', 'idx', 2, 1, 'fields', 'id', 2)
    res = env.cmd('ft.search', 'idx', '*', 'filter', '@version', 0, 2)

def testReplaceReload(env):
    env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH',
            'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
    # Create a document and then replace it.
    env.cmd('FT.ADD', 'idx2', 'doc2', 1.0, 'FIELDS', 'textfield', 's1', 'numfield', 99)
    env.cmd('FT.ADD', 'idx2', 'doc2', 1.0, 'REPLACE', 'PARTIAL',
             'FIELDS', 'textfield', 's100', 'numfield', 990)
    env.dumpAndReload()
    # RDB Should still be fine

    env.cmd('FT.ADD', 'idx2', 'doc2', 1.0, 'REPLACE', 'PARTIAL',
             'FIELDS', 'textfield', 's200', 'numfield', 1090)
    doc = to_dict(env.cmd('FT.GET', 'idx2', 'doc2'))
    env.assertEqual('s200', doc['textfield'])
    env.assertEqual('1090', doc['numfield'])


# command = 'FT.CREATE idx SCHEMA '
# for i in range(255):
#     command += 't%d NUMERIC SORTABLE ' % i
# command = command[:-1]
# env.cmd(command)
# env.cmd('save')
# // reload from ...
# env.cmd('FT.ADD idx doc1 1.0 FIELDS t0 1')
def testIssue417(env):
    command = ['ft.create', 'idx', 'ON', 'HASH', 'schema']
    for x in range(255):
        command += ['t{}'.format(x), 'numeric', 'sortable']
    command = command[:-1]
    env.cmd(*command)
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        try:
            env.cmd('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 't0', '1')
        except redis.ResponseError as e:
            env.assertTrue('already' in str(e))

# >FT.CREATE myIdx SCHEMA title TEXT WEIGHT 5.0 body TEXT url TEXT
# >FT.ADD myIdx doc1 1.0 FIELDS title "hello world" body "lorem ipsum" url "www.google.com"
# >FT.SEARCH myIdx "no-as"
# Could not connect to Redis at 127.0.0.1:6379: Connection refused
# >FT.SEARCH myIdx "no-as"
# (error) Unknown Index name
def testIssue422(env):
    env.cmd('ft.create', 'myIdx', 'ON', 'HASH', 'schema',
             'title', 'TEXT', 'WEIGHT', '5.0',
             'body', 'TEXT',
             'url', 'TEXT')
    env.cmd('ft.add', 'myIdx', 'doc1', '1.0', 'FIELDS', 'title', 'hello world', 'bod', 'lorem ipsum', 'url', 'www.google.com')
    rv = env.cmd('ft.search', 'myIdx', 'no-as')
    env.assertEqual([0], rv)

def testIssue446(env):
    env.cmd('ft.create', 'myIdx', 'ON', 'HASH', 'schema',
             'title', 'TEXT', 'SORTABLE')
    env.cmd('ft.add', 'myIdx', 'doc1', '1.0', 'fields', 'title', 'hello world', 'body', 'lorem ipsum', 'url', '"www.google.com')
    rv = env.cmd('ft.search', 'myIdx', 'hello', 'limit', '0', '0')
    env.assertEqual([1], rv)

    # Related - issue 635
    env.cmd('ft.add', 'myIdx', 'doc2', '1.0', 'fields', 'title', 'hello')
    rv = env.cmd('ft.search', 'myIdx', 'hello', 'limit', '0', '0')
    env.assertEqual([2], rv)

@skip(cluster=True)
def testTimeout(env):
    if VALGRIND:
        env.skip()

    num_range = 1000
    env.cmd('ft.config', 'set', 'timeout', '1')
    # TODO: Remove `TIMEOUT 1` arguments (see commands) once MOD-6286 is merged.
    env.cmd('ft.config', 'set', 'maxprefixexpansions', num_range)

    env.cmd('ft.create', 'myIdx', 'schema', 't', 'TEXT', 'geo', 'GEO')
    for i in range(num_range):
        env.expect('HSET', 'doc%d'%i, 't', 'aa' + str(i), 'geo', str(i/10000) + ',' + str(i/1000))

    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', '0').noEqual([num_range])

    env.expect('ft.config', 'set', 'on_timeout', 'fail').ok()
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', '0') \
       .contains('Timeout limit was reached')

    # test `TIMEOUT` param in query
    res = env.cmd('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'TIMEOUT', 10000)
    env.assertEqual(res[0], num_range)
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'TIMEOUT', '1')    \
        .error().contains('Timeout limit was reached')

    env.expect('ft.aggregate', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'TIMEOUT').error()
    env.expect('ft.aggregate', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'TIMEOUT', -1).error()
    env.expect('ft.aggregate', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'TIMEOUT', 'STR').error()

    # check no time w/o sorter/grouper
    env.expect(
        'FT.AGGREGATE', 'myIdx', '*',
        'LOAD', 1, 'geo',
        'APPLY', 'geodistance(@geo, "0.1,-0.1")', 'AS', 'geodistance1',
        'APPLY', 'geodistance(@geo, "0.11,-0.11")', 'AS', 'geodistance2',
        'APPLY', 'geodistance(@geo, "0.1,-0.1")', 'AS', 'geodistance3',
        'APPLY', 'geodistance(@geo, "0.11,-0.11")', 'AS', 'geodistance4',
        'APPLY', 'geodistance(@geo, "0.1,-0.1")', 'AS', 'geodistance5',
        'TIMEOUT', '1'
    ).error().contains('Timeout limit was reached')

    # test grouper
    env.expect('FT.AGGREGATE', 'myIdx', 'aa*|aa*',
               'LOAD', 1, 't',
               'GROUPBY', 1, '@t',
               'APPLY', 'contains(@t, "a1")', 'AS', 'contain1',
               'APPLY', 'contains(@t, "a1")', 'AS', 'contain2',
               'APPLY', 'contains(@t, "a1")', 'AS', 'contain3',
               'TIMEOUT', '1') \
       .contains('Timeout limit was reached')

    # test sorter
    env.expect('FT.AGGREGATE', 'myIdx', 'aa*|aa*',
               'LOAD', 1, 't',
               'SORTBY', 1, '@t',
               'APPLY', 'contains(@t, "a1")', 'AS', 'contain1',
               'APPLY', 'contains(@t, "a1")', 'AS', 'contain2',
               'APPLY', 'contains(@t, "a1")', 'AS', 'contain3',
               'TIMEOUT', '1') \
       .contains('Timeout limit was reached')

    # test cursor
    res = env.cmd('FT.AGGREGATE', 'myIdx', 'aa*', 'WITHCURSOR', 'count', 50, 'timeout', 500)
    l = len(res[0]) - 1 # do not count the number of results (the first element in the results)
    cursor = res[1]

    time.sleep(0.01)
    while cursor != 0:
        r, cursor = env.cmd('FT.CURSOR', 'READ', 'myIdx', str(cursor))
        l += (len(r) - 1)
    env.assertEqual(l, num_range)

@skip(cluster=True)
def testTimeoutOnSorter(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.config', 'set', 'timeout', '1')
    pl = conn.pipeline()

    env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric', 'SORTABLE')

    elements = 1024 * 64
    for i in range(elements):
        pl.execute_command('hset', i, 'n', i)
        if i % 10000 == 0:
            pl.execute()
    pl.execute()

    res = env.cmd('ft.search', 'idx', '*', 'SORTBY', 'n', 'DESC')
    env.assertGreater(elements, res[0])
    env.assertGreater(len(res), 2)

def testAlias(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc1', 'schema', 't1', 'text')
    env.cmd('ft.create', 'idx2', 'ON', 'HASH', 'PREFIX', 1, 'doc2', 'schema', 't1', 'text')

    env.expect('ft.aliasAdd', 'myIndex').error()
    env.expect('ft.aliasupdate', 'fake_alias', 'imaginary_alias', 'Too_many_args').error()
    env.cmd('ft.aliasAdd', 'myIndex', 'idx')
    env.cmd('ft.add', 'myIndex', 'doc1', 1.0, 'fields', 't1', 'hello')
    r = env.cmd('ft.search', 'idx', 'hello')
    env.assertEqual([1, 'doc1', ['t1', 'hello']], r)
    r2 = env.cmd('ft.search', 'myIndex', 'hello')
    env.assertEqual(r, r2)

    # try to add the same alias again; should be an error
    env.expect('ft.aliasAdd', 'myIndex', 'idx2').error()
    env.expect('ft.aliasAdd', 'alias2', 'idx').noError()
    # now delete the index
    env.cmd('ft.drop', 'myIndex')
    # RS2 does not delete doc on ft.drop
    conn.execute_command('DEL', 'doc1')

    # index list should be cleared now. This can be tested by trying to alias
    # the old alias to different index
    env.cmd('ft.aliasAdd', 'myIndex', 'idx2')
    env.cmd('ft.aliasAdd', 'alias2', 'idx2')
    env.cmd('ft.add', 'myIndex', 'doc2', 1.0, 'fields', 't1', 'hello')
    r = env.cmd('ft.search', 'alias2', 'hello')
    env.assertEqual([1, 'doc2', ['t1', 'hello']], r)

    # check that aliasing one alias to another returns an error. This will
    # end up being confusing
    env.expect('ft.aliasAdd', 'alias3', 'myIndex').error()

    # check that deleting the alias works as expected
    env.expect('ft.aliasDel', 'myIndex').noError()
    env.expect('ft.search', 'myIndex', 'foo').error()

    # create a new index and see if we can use the old name
    env.cmd('ft.create', 'idx3', 'ON', 'HASH', 'PREFIX', 1, 'doc3', 'schema', 't1', 'text')
    env.cmd('ft.add', 'idx3', 'doc3', 1.0, 'fields', 't1', 'foo')
    env.cmd('ft.aliasAdd', 'myIndex', 'idx3')
    # also, check that this works in rdb save
    for _ in env.reloadingIterator():
        waitForIndex(env, 'myIndex')
        r = env.cmd('ft.search', 'myIndex', 'foo')
        env.assertEqual([1, 'doc3', ['t1', 'foo']], r)

    # Check that we can move an alias from one index to another
    env.cmd('ft.aliasUpdate', 'myIndex', 'idx2')
    r = env.cmd('ft.search', 'myIndex', "hello")
    env.assertEqual([1, 'doc2', ['t1', 'hello']], r)

    # Test that things like ft.get, ft.aggregate, etc. work
    r = env.cmd('ft.get', 'myIndex', 'doc2')
    env.assertEqual(['t1', 'hello'], r)

    r = env.cmd('ft.aggregate', 'myIndex', 'hello', 'LOAD', '1', '@t1')
    env.assertEqual([1, ['t1', 'hello']], r)

    # Test update
    env.expect('ft.aliasAdd', 'updateIndex', 'idx3')
    env.expect('ft.aliasUpdate', 'updateIndex', 'fake_idx')

    r = env.cmd('ft.del', 'idx2', 'doc2')
    env.assertEqual(1, r)
    env.expect('ft.aliasdel').error()
    env.expect('ft.aliasdel', 'myIndex', 'yourIndex').error()
    env.expect('ft.aliasdel', 'non_existing_alias').error()

    # Test index alias with the same length as the original (MOD 5945)
    env.expect('FT.ALIASADD', 'temp', 'idx3').ok()
    r = env.cmd('ft.search', 'temp', 'foo')
    env.assertEqual([1, 'doc3', ['t1', 'foo']], r)

def testNoCreate(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f1', 'text')
    env.expect('ft.add', 'idx', 'schema', 'f1').error()
    env.expect('ft.add', 'idx', 'doc1', 1, 'nocreate', 'fields', 'f1', 'hello').error()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'nocreate', 'fields', 'f1', 'hello').error()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'fields', 'f1', 'hello').noError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'nocreate', 'fields', 'f1', 'world').noError()

def testSpellCheck(env):
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'report', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'report', 'report content')
    rv = env.cmd('FT.SPELLCHECK', 'idx', '111111')
    env.assertEqual([['TERM', '111111', []]], rv)
    if not env.isCluster():
        rv = env.cmd('FT.SPELLCHECK', 'idx', '111111', 'FULLSCOREINFO')
        env.assertEqual([1, ['TERM', '111111', []]], rv)

# Standalone functionality
def testIssue484(env):
# Issue with split
# 127.0.0.1:6379> ft.drop productSearch1
# OK
# 127.0.0.1:6379> "FT.CREATE" "productSearch1" "NOSCOREIDX" "SCHEMA" "productid" "TEXT" "categoryid" "TEXT"  "color" "TEXT" "timestamp" "NUMERIC"
# OK
# 127.0.0.1:6379> "FT.ADD" "productSearch1" "GUID1" "1.0" "REPLACE" "FIELDS" "productid" "1" "categoryid" "cars" "color" "blue" "categoryType" 0
# OK
# 127.0.0.1:6379> "FT.ADD" "productSearch1" "GUID2" "1.0" "REPLACE" "FIELDS" "productid" "1" "categoryid" "small cars" "color" "white" "categoryType" 0
# OK
# 127.0.0.1:6379> "FT.ADD" "productSearch1" "GUID3" "1.0" "REPLACE" "FIELDS" "productid" "2" "categoryid" "Big cars" "color" "white" "categoryType" 0
# OK
# 127.0.0.1:6379> "FT.ADD" "productSearch1" "GUID4" "1.0" "REPLACE" "FIELDS" "productid" "2" "categoryid" "Big cars" "color" "green" "categoryType" 0
# OK
# 127.0.0.1:6379> "FT.ADD" "productSearch1" "GUID5" "1.0" "REPLACE" "FIELDS" "productid" "3" "categoryid" "cars" "color" "blue" "categoryType" 0
# OK
# 127.0.0.1:6379>  FT.AGGREGATE productSearch1 * load 2 @color @categoryid APPLY "split(format(\"%s-%s\",@color,@categoryid),\"-\")" as value GROUPBY 1 @value REDUCE COUNT 0 as value_count
    env.cmd('ft.create', 'productSearch1', 'noscoreidx', 'ON', 'HASH', 'schema', 'productid',
            'text', 'categoryid', 'text', 'color', 'text', 'timestamp', 'numeric')
    env.cmd('ft.add', 'productSearch1', 'GUID1', '1.0', 'REPLACE', 'FIELDS', 'productid', '1', 'categoryid', 'cars', 'color', 'blue', 'categoryType', 0)
    env.cmd('ft.add', 'productSearch1', 'GUID2', '1.0', 'REPLACE', 'FIELDS', 'productid', '1', 'categoryid', 'small cars', 'color', 'white', 'categoryType', 0)
    env.cmd('ft.add', 'productSearch1', 'GUID3', '1.0', 'REPLACE', 'FIELDS', 'productid', '2', 'categoryid', 'Big cars', 'color', 'white', 'categoryType', 0)
    env.cmd('ft.add', 'productSearch1', 'GUID4', '1.0', 'REPLACE', 'FIELDS', 'productid', '2', 'categoryid', 'Big cars', 'color', 'green', 'categoryType', 0)
    env.cmd('ft.add', 'productSearch1', 'GUID5', '1.0', 'REPLACE', 'FIELDS', 'productid', '3', 'categoryid', 'cars', 'color', 'blue', 'categoryType', 0)
    res = env.cmd('FT.AGGREGATE', 'productSearch1', '*',
        'load', '2', '@color', '@categoryid',
        'APPLY', 'split(format("%s-%s",@color,@categoryid),"-")', 'as', 'value',
        'GROUPBY', '1', '@value',
        'REDUCE', 'COUNT', '0', 'as', 'value_count',
        'SORTBY', '4', '@value_count', 'DESC', '@value', 'ASC')
    expected = [6, ['value', 'white', 'value_count', '2'], ['value', 'cars', 'value_count', '2'], ['value', 'small cars', 'value_count', '1'], ['value', 'blue', 'value_count', '2'], ['value', 'Big cars', 'value_count', '2'], ['value', 'green', 'value_count', '1']]
    env.assertEqual(toSortedFlatList(expected), toSortedFlatList(res))

    for var in expected:
        env.assertContains(var, res)

def testIssue501(env):
    env.cmd('FT.CREATE', 'incidents', 'ON', 'HASH', 'SCHEMA', 'report', 'TEXT')
    env.cmd('FT.ADD', 'incidents', 'doc1', 1.0, 'FIELDS', 'report', 'report content')
    env.cmd('FT.DICTADD', 'slang', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')
    rv = env.cmd('FT.SPELLCHECK', 'incidents', 'qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq',
        'TERMS', 'INCLUDE', 'slang', 'TERMS', 'EXCLUDE', 'slang')
    env.assertEqual("qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq", rv[0][1])
    env.assertEqual([], rv[0][2])

    env.expect('FT.SPELLCHECK', 'incidents', 'qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq',
        'TERMS', 'FAKE_COMMAND', 'slang').error()

def testIssue589(env):
    env.cmd('FT.CREATE', 'incidents', 'ON', 'HASH', 'SCHEMA', 'report', 'TEXT')
    env.cmd('FT.ADD', 'incidents', 'doc1', 1.0, 'FIELDS', 'report', 'report content')
    env.expect('FT.SPELLCHECK', 'incidents', 'report :').error().contains("Syntax error at offset")

def testIssue621(env):
    env.expect('ft.create', 'test', 'ON', 'HASH', 'SCHEMA', 'uuid', 'TAG', 'title', 'TEXT').equal('OK')
    env.expect('ft.add', 'test', 'a', '1', 'REPLACE', 'PARTIAL', 'FIELDS', 'uuid', 'foo', 'title', 'bar').equal('OK')
    env.expect('ft.add', 'test', 'a', '1', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'bar').equal('OK')
    res = env.cmd('ft.search', 'test', '@uuid:{foo}')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'a', ['uuid', 'foo', 'title', 'bar']]))

# Server crash on doc names that conflict with index keys #666
# again this test is not relevant cause index is out of key space
# def testIssue666(env):
#     # We cannot reliably determine that any error will occur in cluster mode
#     # because of the key name
#     env.skipOnCluster()

#     env.cmd('ft.create', 'foo', 'schema', 'bar', 'text')
#     env.cmd('ft.add', 'foo', 'mydoc', 1, 'fields', 'bar', 'one two three')

#     # crashes here
#     with env.assertResponseError():
#         env.cmd('ft.add', 'foo', 'ft:foo/two', '1', 'fields', 'bar', 'four five six')
#     # try with replace:
#     with env.assertResponseError():
#         env.cmd('ft.add', 'foo', 'ft:foo/two', '1', 'REPLACE',
#             'FIELDS', 'bar', 'four five six')
#     with env.assertResponseError():
#         env.cmd('ft.add', 'foo', 'idx:foo', '1', 'REPLACE',
#             'FIELDS', 'bar', 'four five six')

#     env.cmd('ft.add', 'foo', 'mydoc1', 1, 'fields', 'bar', 'four five six')

# 127.0.0.1:6379> flushdb
# OK
# 127.0.0.1:6379> ft.create foo SCHEMA bar text
# OK
# 127.0.0.1:6379> ft.add foo mydoc 1 FIELDS bar "one two three"
# OK
# 127.0.0.1:6379> keys *
# 1) "mydoc"
# 2) "ft:foo/one"
# 3) "idx:foo"
# 4) "ft:foo/two"
# 5) "ft:foo/three"
# 127.0.0.1:6379> ft.add foo "ft:foo/two" 1 FIELDS bar "four five six"
# Could not connect to Redis at 127.0.0.1:6379: Connection refused

@skip(cluster=True)
def testPrefixDeletedExpansions(env):

    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'txt1', 'text', 'tag1', 'tag')
    # get the number of maximum expansions
    maxexpansions = int(env.cmd('ft.config', 'get', 'MAXEXPANSIONS')[0][1])

    for x in range(maxexpansions):
        env.cmd('ft.add', 'idx', 'doc{}'.format(x), 1, 'fields',
                'txt1', 'term{}'.format(x), 'tag1', 'tag{}'.format(x))

    for x in range(maxexpansions):
        env.cmd('ft.del', 'idx', 'doc{}'.format(x))

    env.cmd('ft.add', 'idx', 'doc_XXX', 1, 'fields', 'txt1', 'termZZZ', 'tag1', 'tagZZZ')

    # r = env.cmd('ft.search', 'idx', 'term*')
    # print(r)
    # r = env.cmd('ft.search', 'idx', '@tag1:{tag*}')
    # print(r)

    tmax = time.time() + 0.5  # 250ms max
    iters = 0
    while time.time() < tmax:
        iters += 1
        forceInvokeGC(env, 'idx')
        r = env.cmd('ft.search', 'idx', '@txt1:term* @tag1:{tag*}')
        if r[0]:
            break

    # print 'did {} iterations'.format(iters)
    r = env.cmd('ft.search', 'idx', '@txt1:term* @tag1:{tag*}')
    env.assertEqual(toSortedFlatList([1, 'doc_XXX', ['txt1', 'termZZZ', 'tag1', 'tagZZZ']]), toSortedFlatList(r))


def testOptionalFilter(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 't1', 'text')
    for x in range(100):
        env.cmd('ft.add', 'idx', 'doc_{}'.format(x), 1, 'fields', 't1', 'hello world word{}'.format(x))

    env.cmd('ft.explain', 'idx', '(~@t1:word20)')
    # print(r)

    r = env.cmd('ft.search', 'idx', '~(word20 => {$weight: 2.0})')

def testIssue828(env):
    env.cmd('ft.create', 'beers', 'ON', 'HASH', 'SCHEMA',
        'name', 'TEXT', 'PHONETIC', 'dm:en',
        'style', 'TAG', 'SORTABLE',
        'abv', 'NUMERIC', 'SORTABLE')
    rv = env.cmd("FT.ADD", "beers", "802", "1.0",
        "FIELDS", "index", "25", "abv", "0.049",
        "name", "Hell or High Watermelon Wheat (2009)",
        "style", "Fruit / Vegetable Beer")
    env.assertEqual('OK', rv)

def testIssue862(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    rv = env.cmd("FT.ADD", "idx", "doc1", "1.0", "FIELDS", "test", "foo")
    env.assertEqual('OK', rv)
    env.cmd("FT.SEARCH", "idx", "foo", 'WITHSORTKEYS')
    env.assertTrue(env.isUp())

def testIssue_884(env):
    env.expect('FT.create', 'idx', 'ON', 'HASH', 'STOPWORDS', '0', 'SCHEMA', 'title', 'text', 'weight',
               '50', 'subtitle', 'text', 'weight', '10', 'author', 'text', 'weight',
               '10', 'description', 'text', 'weight', '20').equal('OK')

    env.expect('FT.ADD', 'idx', 'doc4', '1.0', 'FIELDS', 'title', 'mohsin conversation the conversation tahir').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc3', '1.0', 'FIELDS', 'title', 'Fareham Civilization Church - Sermons and conversations mohsin conversation the').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc2', '1.0', 'FIELDS', 'title', 'conversation the conversation - a drama about conversation, the science of conversation.').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'title', 'mohsin conversation with the mohsin').equal('OK')

    expected = [2, 'doc2', ['title', 'conversation the conversation - a drama about conversation, the science of conversation.'], 'doc4', ['title', 'mohsin conversation the conversation tahir']]
    res = env.cmd('FT.SEARCH', 'idx', '@title:(conversation) (@title:(conversation the conversation))=>{$inorder: true;$slop: 0}')
    env.assertEqual(len(expected), len(res))
    for v in expected:
        env.assertContains(v, res)

def testIssue_848(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test1', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test1', 'foo').equal('OK')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'test2', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc2', '1.0', 'FIELDS', 'test1', 'foo', 'test2', 'bar').equal('OK')
    env.expect('FT.SEARCH', 'idx', 'foo', 'SORTBY', 'test2', 'ASC').equal([2, 'doc2', ['test2', 'bar', 'test1', 'foo'], 'doc1', ['test1', 'foo']])

def testMod_309(env):
    n = 10000 if VALGRIND else 100000
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(n):
        conn.execute_command('HSET', f'doc{i}', 'test', 'foo')
    waitForIndex(env, 'idx')
    info = index_info(env, 'idx')
    env.assertEqual(int(info['num_docs']), n)
    res = env.cmd('FT.AGGREGATE', 'idx', 'foo', 'TIMEOUT', 300000)
    env.assertEqual(len(res), n + 1)

    # test with cursor
    if env.isCluster():
        return

    res, cursor = env.cmd('FT.AGGREGATE', 'idx', 'foo', 'WITHCURSOR', 'TIMEOUT', 300000)
    l = len(res) - 1  # do not count the number of results (the first element in the results)
    while cursor != 0:
        r, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', str(cursor))
        l += len(r) - 1
    env.assertEqual(l, n)

def testIssue_865(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', '1', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', '1', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', '1', 'foo2').equal('OK')
    env.expect('ft.search', 'idx', 'foo*', 'SORTBY', '1', 'ASC').equal([2, 'doc1', ['1', 'foo1'], 'doc2', ['1', 'foo2']])
    env.expect('ft.search', 'idx', 'foo*', 'SORTBY', '1', 'DESC').equal([2, 'doc2', ['1', 'foo2'], 'doc1', ['1', 'foo1']])
    env.expect('ft.search', 'idx', 'foo*', 'SORTBY', '1', 'bad').error()
    env.expect('ft.search', 'idx', 'foo*', 'SORTBY', 'bad', 'bad').error()
    env.expect('ft.search', 'idx', 'foo*', 'SORTBY', 'bad').error()
    env.expect('ft.search', 'idx', 'foo*', 'SORTBY').error()

def testIssue_779(env):
    # FT.ADD should return NOADD and not change the doc if value < same_value, but it returns OK and makes the change.
    # Note that "greater than" ">" does not have the same bug.

    env.cmd('FT.CREATE idx2 ON HASH SCHEMA ot1 TAG')
    env.cmd('FT.ADD idx2 doc2 1.0 FIELDS newf CAT ot1 4001')
    res = env.cmd('FT.GET idx2 doc2')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(["newf", "CAT", "ot1", "4001"]))

    # NOADD is expected since 4001 is not < 4000, and no updates to the doc2 is expected as a result
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4000 FIELDS newf DOG ot1 4000', 'NOADD')
    res = env.cmd('FT.GET idx2 doc2')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(["newf", "CAT", "ot1", "4001"]))

    # OK is expected since 4001 < 4002 and the doc2 is updated
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4002 FIELDS newf DOG ot1 4002').equal('OK')
    res = env.cmd('FT.GET idx2 doc2')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(["newf", "DOG", "ot1", "4002"]))

    # OK is NOT expected since 4002 is not < 4002
    # We expect NOADD and doc2 update; however, we get OK and doc2 updated
    # After fix, @ot1 implicitly converted to a number, thus we expect NOADD
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4002 FIELDS newf FISH ot1 4002').equal('NOADD')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if to_number(@ot1)<4002 FIELDS newf FISH ot1 4002').equal('NOADD')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<to_str(4002) FIELDS newf FISH ot1 4002').equal('NOADD')
    res = env.cmd('FT.GET idx2 doc2')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(["newf", "DOG", "ot1", "4002"]))

    # OK and doc2 update is expected since 4002 < 4003
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4003 FIELDS newf HORSE ot1 4003').equal('OK')
    res = env.cmd('FT.GET idx2 doc2')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(["newf", "HORSE", "ot1", "4003"]))

    # Expect NOADD since 4003 is not > 4003
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1>4003 FIELDS newf COW ot1 4003').equal('NOADD')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if 4003<@ot1 FIELDS newf COW ot1 4003').equal('NOADD')

    # Expect OK and doc2 updated since 4003 > 4002
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1>4002 FIELDS newf PIG ot1 4002').equal('OK')
    res = env.cmd('FT.GET idx2 doc2')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(["newf", "PIG", "ot1", "4002"]))

    # Syntax errors
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4-002 FIELDS newf DOG ot1 4002').contains('Syntax error')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<to_number(4-002) FIELDS newf DOG ot1 4002').contains('Syntax error')

def testUnknownSymbolErrorOnConditionalAdd(env):
    env.expect('FT.CREATE idx ON HASH SCHEMA f1 TAG f2 NUMERIC NOINDEX f3 TAG NOINDEX').ok()
    env.expect('ft.add idx doc1 1.0 REPLACE PARTIAL IF @f1<awfwaf FIELDS f1 foo f2 1 f3 boo').ok()
    env.expect('ft.add idx doc1 1.0 REPLACE PARTIAL IF @f1<awfwaf FIELDS f1 foo f2 1 f3 boo').error()

def testWrongResultsReturnedBySkipOptimization(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'f1', 'TEXT', 'f2', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'f1', 'foo', 'f2', 'bar').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'f1', 'moo', 'f2', 'foo').equal('OK')
    env.expect('ft.search', 'idx', 'foo @f2:moo').equal([0])

def testErrorWithApply(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo bar').equal('OK')
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'split()'
    ).error().contains('Invalid number of arguments for split')

def testSummerizeWithAggregateRaiseError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', 'foo2', 'SUMMARIZE', 'FIELDS', '1', 'test',
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0').error().contains("SUMMARIZE is not supported on FT.AGGREGATE")

def testSummerizeHighlightParseError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.search', 'idx', 'foo2', 'SUMMARIZE', 'FIELDS', 'WITHSCORES').error()
    env.expect('ft.search', 'idx', 'foo2', 'HIGHLIGHT', 'FIELDS', 'WITHSCORES').error()

def testCursorBadArgument(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', '*',
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0',
               'WITHCURSOR', 'COUNT', 'BAD').error()

def testLimitBadArgument(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.search', 'idx', '*', 'LIMIT', '1').error()

def testOnTimeoutBadArgument(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'bad').error()

def testAggregateSortByWrongArgument(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', 'bad').error()

def testAggregateSortByMaxNumberOfFields(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'test1', 'TEXT', 'SORTABLE',
               'test2', 'TEXT', 'SORTABLE',
               'test3', 'TEXT', 'SORTABLE',
               'test4', 'TEXT', 'SORTABLE',
               'test5', 'TEXT', 'SORTABLE',
               'test6', 'TEXT', 'SORTABLE',
               'test7', 'TEXT', 'SORTABLE',
               'test8', 'TEXT', 'SORTABLE',
               'test9', 'TEXT', 'SORTABLE'
               ).equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', '9', *['@test%d' % (i + 1) for i in range(9)]).error()
    args = ['@test%d' % (i + 1) for i in range(8)] + ['bad']
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', '9', *args).error()
    args = ['@test%d' % (i + 1) for i in range(8)] + ['ASC', 'MAX', 'bad']
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', '9', *args).error()
    args = ['@test%d' % (i + 1) for i in range(8)] + ['ASC', 'MAX']
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', '9', *args).error()

def testNumericFilterError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', 'bad', '2').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', '0', 'bad').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', '0').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', 'bad').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', '0', '2', 'FILTER', 'test', '0', 'bla').error()

def testGeoFilterError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', 'bad' , '2', '3', 'km').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1' , 'bad', '3', 'km').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1' , '2', 'bad', 'km').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1' , '2', '3', 'bad').error()

def testReducerError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', 'bad').error()
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'as').error()

def testGroupbyError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE').error()
    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test1').error()
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'bad', '0').error()
    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'SUM', '1', '@test1').error()

def testGroupbyWithSort(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.add', 'idx', 'doc3', '1.0', 'FIELDS', 'test', '2').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', '2', '@test', 'ASC',
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'as', 'count').equal([2, ['test', '2', 'count', '1'], ['test', '1', 'count', '2']])

def testApplyError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'APPLY', 'split(@test)', 'as').error()

def testLoadError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD').error()
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', 'bad').error()
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', 'bad', 'test').error()
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '2', 'test').error()
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '2', '@test').error()

def testMissingArgsError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx').error()

def testUnexistsScorer(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'SCORER', 'bad').error()

def testHighlightWithUnknowsProperty(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'HIGHLIGHT', 'FIELDS', '1', 'test1').error()

def testHighlightOnAggregate(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'HIGHLIGHT', 'FIELDS', '1', 'test').error().contains("HIGHLIGHT is not supported on FT.AGGREGATE")

def testBadFilterExpression(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'FILTER', 'blabla').error()
        env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'FILTER', '@test1 > 1').error()

def testWithSortKeysOnNoneSortableValue(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'WITHSORTKEYS', 'SORTBY', 'test').equal([1, 'doc1', '$foo', ['test', 'foo']])

@skip(cluster=True)
def testWithWithRawIds(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'WITHRAWIDS').equal([1, 'doc1', 1, ['test', 'foo']])

# todo: unskip once fix on coordinator
#       the coordinator do not return erro on unexisting index.
@skip(cluster=True)
def testUnkownIndex(env):
    env.expect('ft.aggregate').error()
    env.expect('ft.aggregate', 'idx', '*').error()
    env.expect('ft.aggregate', 'idx', '*', 'WITHCURSOR').error()

def testExplainError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('FT.EXPLAIN', 'idx', '(').error()

def testBadCursor(env):
    env.expect('FT.CURSOR', 'READ', 'idx').error()
    env.expect('FT.CURSOR', 'READ', 'idx', '1111').error()
    env.expect('FT.CURSOR', 'READ', 'idx', 'bad').error()
    env.expect('FT.CURSOR', 'DROP', 'idx', '1111').error()
    env.expect('FT.CURSOR', 'bad', 'idx', '1111').error()

def testGroupByWithApplyError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').ok()
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'APPLY', 'split()', 'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'AS', 'count'
    ).error().contains('Invalid number of arguments for split')

def testSubStrErrors(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')

    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a', 'APPLY', 'substr(@a,0,4)'
    ).error()

    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test",3,-2)', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test",3,1000)', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test",-1,2)', 'as', 'a')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test")', 'as', 'a').error().contains("Invalid arguments for function 'substr'")
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr(1)', 'as', 'a').error().contains("Invalid arguments for function 'substr'")
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test", "test")', 'as', 'a').error().contains("Invalid arguments for function 'substr'")
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test", "test", "test")', 'as', 'a').error().contains("Invalid type (3) for argument 1 in function 'substr'")
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test", "-1", "-1")', 'as', 'a').error().contains("Invalid type (3) for argument 1 in function 'substr'")
    env.assertTrue(env.isUp())

def testToUpperLower(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower(@test)', 'as', 'a').equal([1, ['test', 'foo', 'a', 'foo']])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower("FOO")', 'as', 'a').equal([1, ['test', 'foo', 'a', 'foo']])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper(@test)', 'as', 'a').equal([1, ['test', 'foo', 'a', 'FOO']])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper("foo")', 'as', 'a').equal([1, ['test', 'foo', 'a', 'FOO']])

    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper()', 'as', 'a'
    ).error()
    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower()', 'as', 'a'
    ).error()

    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper(1)', 'as', 'a').equal([1, ['test', 'foo', 'a', None]])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower(1)', 'as', 'a').equal([1, ['test', 'foo', 'a', None]])

    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper(1,2)', 'as', 'a'
    ).error()
    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower(1,2)', 'as', 'a'
    ).error()

def testMatchedTerms(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a').equal([1, ['test', 'foo', 'a', None]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a').equal([1, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms(100)', 'as', 'a').equal([1, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms(-100)', 'as', 'a').equal([1, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms("test")', 'as', 'a').equal([1, ['test', 'foo', 'a', ['foo']]])

def testStrFormatError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect(
        'ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format()', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format("%s")', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format("%", "test")', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format("%b", "test")', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format(5)', 'as', 'a'
    ).error()

    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'upper(1)', 'as', 'b', 'APPLY', 'format("%s", @b)', 'as', 'a').equal([1, ['test', 'foo', 'b', None, 'a', '(null)']])

    # working example
    env.expect('ft.aggregate', 'idx', 'foo', 'APPLY', 'format("%%s-test", "test")', 'as', 'a').equal([1, ['a', '%s-test']])
    env.expect('ft.aggregate', 'idx', 'foo', 'APPLY', 'format("%s-test", "test")', 'as', 'a').equal([1, ['a', 'test-test']])

def testTimeFormatError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt()', 'as', 'a'
    ).error()

    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test1)', 'as', 'a').error()

    env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test)', 'as', 'a')

    env.assertTrue(env.isUp())

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test, 4)', 'as', 'a'
    ).error()

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt("awfawf")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(235325153152356426246246246254)', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test, "%s")' % ('d' * 2048), 'as', 'a').equal([1, ['test', '12234556', 'a', None]])

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'hour("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'minute("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'day("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'month("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'dayofweek("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'dayofmonth("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'dayofyear("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'year("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear("not_number")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])

def testMonthOfYear(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear(@test)', 'as', 'a').equal([1, ['test', '12234556', 'a', '4']])

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear(@test, 112)', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear()', 'as', 'a'
    ).error()

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear("bad")', 'as', 'a').equal([1, ['test', '12234556', 'a', None]])

def testParseTime(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TAG')
    conn.execute_command('HSET', 'doc1', 'test', '20210401')

    # check for errors
    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime()', 'as', 'a'
    ).error().contains("Invalid arguments for function 'parsetime'")

    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime(11)', 'as', 'a'
    ).error().contains("Invalid arguments for function 'parsetime'")

    env.expect(
        'ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime(11,22)', 'as', 'a'
    ).error().contains("Invalid type (1) for argument 0 in function 'parsetime'. VALIDATE_ARG__STRING(v, 0) was false.")

    # valid test
    res = env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime(@test, "%Y%m%d")', 'as', 'a')
    env.assertEqual(res, [1, ['test', '20210401', 'a', '1617235200']])

def testMathFunctions(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'exp(@test)', 'as', 'a').equal([1, ['test', '12234556', 'a', 'inf']])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'ceil(@test)', 'as', 'a').equal([1, ['test', '12234556', 'a', '12234556']])

def testErrorOnOpperation(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', '1 + split()', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'split() + 1', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', '"bad" + "bad"', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'split("bad" + "bad")', 'as', 'a'
    ).error()

    env.expect(
        'ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', '!(split("bad" + "bad"))', 'as', 'a'
    ).error()

    if not env.isCluster():
        env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'APPLY', '!@test', 'as', 'a').error().contains('not loaded nor in pipeline')


def testSortkeyUnsortable(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'test', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 'test', 'foo')
    rv = env.cmd('ft.aggregate', 'idx', 'foo', 'withsortkeys',
        'load', '1', '@test',
        'sortby', '1', '@test')
    env.assertEqual([1, '$foo', ['test', 'foo']], rv)


def testIssue919(env):
    # This only works if the missing field has a lower sortable index
    # than the present field..
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'schema', 't1', 'text', 'sortable', 'n1', 'numeric', 'sortable')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 'n1', 42)
    rv = env.cmd('ft.search', 'idx', '*', 'sortby', 't1', 'desc')
    env.assertEqual([1, 'doc1', ['n1', '42']], rv)


def testIssue1074(env):
    # Ensure that sortable fields are returned in their string form from the
    # document
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'schema', 't1', 'text', 'n1', 'numeric', 'sortable')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 't1', 'hello', 'n1', 1581011976800)
    rv = env.cmd('ft.search', 'idx', '*', 'sortby', 'n1')
    env.assertEqual([1, 'doc1', ['n1', '1581011976800', 't1', 'hello']], rv)

@skip(cluster=True)
def testIssue1085(env):
    env.cmd('FT.CREATE issue1085 ON HASH SCHEMA foo TEXT SORTABLE bar NUMERIC SORTABLE')
    for i in range(1, 10):
        env.cmd('FT.ADD issue1085 document_%d 1 REPLACE FIELDS foo foo%d bar %d' % (i, i, i))
    res = env.cmd('FT.SEARCH', 'issue1085', '@bar:[8 8]')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'document_8', ['foo', 'foo8', 'bar', '8']]))

    for i in range(1, 10):
        env.cmd('FT.ADD issue1085 document_8 1 REPLACE FIELDS foo foo8 bar 8')

    forceInvokeGC(env, 'issue1085')

    res = env.cmd('FT.SEARCH', 'issue1085', '@bar:[8 8]')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'document_8', ['foo', 'foo8', 'bar', '8']]))


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import zip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}

def testInfoError(env):
    env.expect('ft.info', 'no_idx').error()

def testIndexNotRemovedFromCursorListAfterRecreated(env):
    env.expect('FT.CREATE idx ON HASH SCHEMA f1 TEXT').ok()
    env.expect('FT.AGGREGATE idx * WITHCURSOR').equal([[0], 0])
    env.expect('FT.CREATE idx ON HASH SCHEMA f1 TEXT').error()
    env.expect('FT.AGGREGATE idx * WITHCURSOR').equal([[0], 0])

def testHindiStemmer(env):
    env.cmd('FT.CREATE', 'idxTest', 'LANGUAGE_FIELD', '__language', 'SCHEMA', 'body', 'TEXT')
    env.cmd('FT.ADD', 'idxTest', 'doc1', 1.0, 'LANGUAGE', 'hindi', 'FIELDS', 'body', u'अँगरेजी अँगरेजों अँगरेज़')
    res = env.cmd('FT.SEARCH', 'idxTest', u'अँगरेज़')
    res1 = {res[2][i]:res[2][i + 1] for i in range(0, len(res[2]), 2)}
    env.assertEqual(u'अँगरेजी अँगरेजों अँगरेज़', res1['body'])

@skip(cluster=True)
def testMOD507(env):
    env.expect('ft.create idx ON HASH SCHEMA t1 TEXT').ok()

    for i in range(50):
        env.expect('ft.add idx doc-%d 1.0 FIELDS t1 foo' % i).ok()

    for i in range(50):
        env.expect('del doc-%d' % i).equal(1)

    res = env.cmd('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'SUMMARIZE', 'FRAGS', '1', 'LEN', '25', 'HIGHLIGHT', 'TAGS', "<span style='background-color:yellow'>", "</span>")

    # from redisearch 2.0, docs are removed from index when `DEL` is called
    env.assertEqual(len(res), 1)

@skip(cluster=True)
def testUnseportedSortableTypeErrorOnTags(env):
    env.expect('FT.CREATE idx ON HASH SCHEMA f1 TEXT SORTABLE f2 NUMERIC SORTABLE NOINDEX f3 TAG SORTABLE NOINDEX f4 TEXT SORTABLE NOINDEX').ok()
    env.expect('FT.ADD idx doc1 1.0 FIELDS f1 foo1 f2 1 f3 foo1 f4 foo1').ok()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL FIELDS f2 2 f3 foo2 f4 foo2').ok()
    res = env.cmd('HGETALL doc1')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(['f1', 'foo1', 'f2', '2', 'f3', 'foo2', 'f4', 'foo2', '__score', '1.0']))
    res = env.cmd('FT.SEARCH idx *')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'doc1', ['f1', 'foo1', 'f2', '2', 'f3', 'foo2', 'f4', 'foo2']]))


def testIssue1158(env):
    env.cmd('FT.CREATE idx ON HASH SCHEMA txt1 TEXT txt2 TEXT txt3 TEXT')

    env.cmd('FT.ADD idx doc1 1.0 FIELDS txt1 10 txt2 num1')
    res = env.cmd('FT.GET idx doc1')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(['txt1', '10', 'txt2', 'num1']))

    # only 1st checked (2nd returns an error)
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt1||to_number(@txt2)<5 FIELDS txt1 5').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt3&&to_number(@txt2)<5 FIELDS txt1 5').equal('NOADD')

    # both are checked
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11||to_number(@txt1)>42 FIELDS txt2 num2').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11||to_number(@txt1)<42 FIELDS txt2 num2').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11&&to_number(@txt1)>42 FIELDS txt2 num2').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11&&to_number(@txt1)<42 FIELDS txt2 num2').equal('NOADD')
    res = env.cmd('FT.GET idx doc1')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(['txt1', '5', 'txt2', 'num2']))

def testIssue1159(env):
    env.cmd('FT.CREATE idx ON HASH SCHEMA f1 TAG')
    for i in range(1000):
        env.cmd('FT.add idx doc%d 1.0 FIELDS f1 foo' % i)

def testIssue1169(env):
    env.cmd('FT.CREATE idx ON HASH SCHEMA txt1 TEXT txt2 TEXT')
    env.cmd('FT.ADD idx doc1 1.0 FIELDS txt1 foo')

    env.expect('FT.AGGREGATE idx foo GROUPBY 1 @txt1 REDUCE FIRST_VALUE 1 @txt2 as test').equal([1, ['txt1', 'foo', 'test', None]])

@skip(cluster=True)
def testIssue1184(env):

    field_types = ['TEXT', 'NUMERIC', 'TAG']
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    for ft in field_types:
        env.expect('FT.CREATE idx ON HASH SCHEMA  field ' + ft).ok()

        res = env.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        env.assertEqual(d['inverted_sz_mb'], '0')
        env.assertEqual(d['num_records'], 0)


        value = '42'
        env.expect('FT.ADD idx doc0 1 FIELD field ' + value).ok()
        doc = env.cmd('FT.SEARCH idx *')
        env.assertEqual(doc, [1, 'doc0', ['field', value]])

        res = env.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        env.assertGreater(d['inverted_sz_mb'], '0')
        env.assertEqual(d['num_records'], 1)

        env.assertEqual(env.cmd('FT.DEL idx doc0'), 1)

        forceInvokeGC(env, 'idx')

        res = env.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}

        # When inverted index is empty we expect the size to be 102 bytes
        env.assertGreaterEqual(102 / ( 1024 * 1024 ), float(d['inverted_sz_mb']))
        env.assertEqual(int(d['num_records']), 0)

        env.cmd('FT.DROP idx')
        env.cmd('DEL doc0')

def testIndexListCommand(env):
    env.expect('FT.CREATE idx1 ON HASH SCHEMA n NUMERIC').ok()
    env.expect('FT.CREATE idx2 ON HASH SCHEMA n NUMERIC').ok()
    res = env.cmd('FT._LIST')
    env.assertEqual(set(res), set(['idx1', 'idx2']))
    env.expect('FT.DROP idx1').ok()
    env.expect('FT._LIST').equal(['idx2'])
    env.expect('FT.CREATE idx3 ON HASH SCHEMA n NUMERIC').ok()
    res = env.cmd('FT._LIST')
    env.assertEqual(set(res), set(['idx2', 'idx3']))


def testIssue1208(env):
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC')
    env.cmd('FT.ADD idx doc1 1 FIELDS n 1.0321e5')
    env.cmd('FT.ADD idx doc2 1 FIELDS n 101.11')
    env.cmd('FT.ADD idx doc3 1 FIELDS n 0.0011')
    env.expect('FT.SEARCH', 'idx', '@n:[1.1432E3 inf]').equal([1, 'doc1', ['n', '1.0321e5']])
    env.expect('FT.SEARCH', 'idx', '@n:[-1.12E-3 1.12E-1]').equal([1, 'doc3', ['n', '0.0011']])
    res = [3, 'doc1', ['n', '1.0321e5'], 'doc2', ['n', '101.11'], 'doc3', ['n', '0.0011']]
    env.expect('FT.SEARCH', 'idx', '@n:[-inf inf]').equal(res)

    env.expect('FT.ADD idx doc3 1 REPLACE PARTIAL IF @n>42e3 FIELDS n 100').equal('NOADD')
    env.expect('FT.ADD idx doc3 1 REPLACE PARTIAL IF @n<42e3 FIELDS n 100').ok()
    # print env.cmd('FT.SEARCH', 'idx', '@n:[-inf inf]')

@skip(cluster=True)
def testFieldsCaseSensetive(env):
    # this test will not pass on coordinator coorently as if one shard return empty results coordinator
    # will not reflect the errors
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC f TEXT t TAG g GEO')

    # make sure text fields are case sesitive
    conn.execute_command('hset', 'doc1', 'F', 'test')
    conn.execute_command('hset', 'doc2', 'f', 'test')
    env.expect('ft.search idx @f:test').equal([1, 'doc2', ['f', 'test']])
    env.expect('ft.search idx @F:test').equal([0])

    # make sure numeric fields are case sesitive
    conn.execute_command('hset', 'doc3', 'N', '1.0')
    conn.execute_command('hset', 'doc4', 'n', '1.0')
    env.expect('ft.search', 'idx', '@n:[0 2]').equal([1, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@N:[0 2]').equal([0])

    # make sure tag fields are case sesitive
    conn.execute_command('hset', 'doc5', 'T', 'tag')
    conn.execute_command('hset', 'doc6', 't', 'tag')
    env.expect('ft.search', 'idx', '@t:{tag}').equal([1, 'doc6', ['t', 'tag']])
    env.expect('ft.search', 'idx', '@T:{tag}').equal([0])

    # make sure geo fields are case sesitive
    conn.execute_command('hset', 'doc8', 'G', '-113.524,53.5244')
    conn.execute_command('hset', 'doc9', 'g', '-113.524,53.5244')
    env.expect('ft.search', 'idx', '@g:[-113.52 53.52 20 mi]').equal([1, 'doc9', ['g', '-113.524,53.5244']])
    env.expect('ft.search', 'idx', '@G:[-113.52 53.52 20 mi]').equal([0])

    # make sure search filter are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'n', 0, 2).equal([1, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'N', 0, 2).equal([0])

    # make sure RETURN are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'n').equal([1, 'doc4', ['n', '1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'N').equal([1, 'doc4', []])

    # make sure SORTBY are case sensitive
    conn.execute_command('hset', 'doc7', 'n', '1.1')
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'n').equal([2, 'doc4', ['n', '1.0'], 'doc7', ['n', '1.1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'N').error().contains('not loaded nor in schema')

    # make sure aggregation load are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n').equal([1, ['n', '1'], ['n', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@N').equal([1, [], []])

    # make sure aggregation apply are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'apply', '@n', 'as', 'r').equal([1, ['n', '1', 'r', '1'], ['n', '1.1', 'r', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'apply', '@N', 'as', 'r').error().contains('not loaded nor in pipeline')

    # make sure aggregation filter are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'filter', '@n==1.0').equal([1, ['n', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'filter', '@N==1.0').error().contains('not loaded nor in pipeline')

    # make sure aggregation groupby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'groupby', '1', '@n', 'reduce', 'count', 0, 'as', 'count').equal([2, ['n', '1', 'count', '1'], ['n', '1.1', 'count', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'groupby', '1', '@N', 'reduce', 'count', 0, 'as', 'count').error().contains('No such property')

    # make sure aggregation sortby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'sortby', '1', '@n').equal([2, ['n', '1'], ['n', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'sortby', '1', '@N').error().contains('not loaded')

@skip(cluster=True)
def testSortedFieldsCaseSensetive(env):
    # this test will not pass on coordinator coorently as if one shard return empty results coordinator
    # will not reflect the errors
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC SORTABLE f TEXT SORTABLE t TAG SORTABLE g GEO SORTABLE')

    # make sure text fields are case sesitive
    conn.execute_command('hset', 'doc1', 'F', 'test')
    conn.execute_command('hset', 'doc2', 'f', 'test')
    env.expect('ft.search idx @f:test').equal([1, 'doc2', ['f', 'test']])
    env.expect('ft.search idx @F:test').equal([0])

    # make sure numeric fields are case sesitive
    conn.execute_command('hset', 'doc3', 'N', '1.0')
    conn.execute_command('hset', 'doc4', 'n', '1.0')
    env.expect('ft.search', 'idx', '@n:[0 2]').equal([1, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@N:[0 2]').equal([0])

    # make sure tag fields are case sesitive
    conn.execute_command('hset', 'doc5', 'T', 'tag')
    conn.execute_command('hset', 'doc6', 't', 'tag')
    env.expect('ft.search', 'idx', '@t:{tag}').equal([1, 'doc6', ['t', 'tag']])
    env.expect('ft.search', 'idx', '@T:{tag}').equal([0])

    # make sure geo fields are case sesitive
    conn.execute_command('hset', 'doc8', 'G', '-113.524,53.5244')
    conn.execute_command('hset', 'doc9', 'g', '-113.524,53.5244')
    env.expect('ft.search', 'idx', '@g:[-113.52 53.52 20 mi]').equal([1, 'doc9', ['g', '-113.524,53.5244']])
    env.expect('ft.search', 'idx', '@G:[-113.52 53.52 20 mi]').equal([0])

    # make sure search filter are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'n', 0, 2).equal([1, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'N', 0, 2).equal([0])

    # make sure RETURN are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'n').equal([1, 'doc4', ['n', '1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'N').equal([1, 'doc4', []])

    # make sure SORTBY are case sensitive
    conn.execute_command('hset', 'doc7', 'n', '1.1')
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'n').equal([2, 'doc4', ['n', '1.0'], 'doc7', ['n', '1.1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'N').error().contains('not loaded nor in schema')

    # make sure aggregation apply are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'apply', '@n', 'as', 'r').equal([1, ['n', '1', 'r', '1'], ['n', '1.1', 'r', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'apply', '@N', 'as', 'r').error().contains('not loaded nor in pipeline')

    # make sure aggregation filter are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'filter', '@n==1.0').equal([1, ['n', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'filter', '@N==1.0').error().contains('not loaded nor in pipeline')

    # make sure aggregation groupby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'groupby', '1', '@n', 'reduce', 'count', 0, 'as', 'count').equal([2, ['n', '1', 'count', '1'], ['n', '1.1', 'count', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'groupby', '1', '@N', 'reduce', 'count', 0, 'as', 'count').error().contains('No such property')

    # make sure aggregation sortby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'sortby', '1', '@n').equal([2, ['n', '1'], ['n', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'sortby', '1', '@N').error().contains('not loaded')

def testScoreLangPayloadAreReturnedIfCaseNotMatchToSpecialFields(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC SORTABLE')
    conn.execute_command('hset', 'doc1', 'n', '1.0', '__Language', 'eng', '__Score', '1', '__Payload', '10')
    res = env.cmd('ft.search', 'idx', '@n:[0 2]')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'doc1', ['n', '1.0', '__Language', 'eng', '__Score', '1', '__Payload', '10']]))

def testReturnSameFieldDifferentCase(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC SORTABLE N NUMERIC SORTABLE')
    conn.execute_command('hset', 'doc1', 'n', '1.0', 'N', '2.0')
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '2', 'n', 'N').equal([1, 'doc1', ['n', '1', 'N', '2']])

def testCreateIfNX(env):
    env.expect('FT._CREATEIFNX idx ON HASH SCHEMA n NUMERIC SORTABLE N NUMERIC SORTABLE').ok()
    env.expect('FT._CREATEIFNX idx ON HASH SCHEMA n NUMERIC SORTABLE N NUMERIC SORTABLE').ok()

def testDropIfX(env):
    env.expect('FT._DROPIFX idx').ok()

def testDeleteIfX(env):
    env.expect('FT._DROPINDEXIFX idx').ok()

def testAlterIfNX(env):
    env.expect('FT.CREATE idx ON HASH SCHEMA n NUMERIC').ok()
    env.expect('FT._ALTERIFNX idx SCHEMA ADD n1 NUMERIC').ok()
    env.expect('FT._ALTERIFNX idx SCHEMA ADD n1 NUMERIC').ok()
    res = env.cmd('ft.info idx')
    res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}['attributes']
    env.assertEqual(res, [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC'],
                          ['identifier', 'n1', 'attribute', 'n1', 'type', 'NUMERIC']])

def testAliasAddIfNX(env):
    env.expect('FT.CREATE idx ON HASH SCHEMA n NUMERIC').ok()
    env.expect('FT._ALIASADDIFNX a1 idx').ok()
    env.expect('FT._ALIASADDIFNX a1 idx').ok()

def testAliasDelIfX(env):
    env.expect('FT._ALIASDELIFX a1').ok()

def testEmptyDoc(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA t TEXT').ok()
    env.expect('FT.ADD idx doc1 1 FIELDS t foo').ok()
    env.expect('FT.ADD idx doc2 1 FIELDS t foo').ok()
    env.expect('FT.ADD idx doc3 1 FIELDS t foo').ok()
    env.expect('FT.ADD idx doc4 1 FIELDS t foo').ok()
    env.expect('FT.SEARCH idx * limit 0 0').equal([4])
    conn.execute_command('DEL', 'doc1')
    conn.execute_command('DEL', 'doc3')
    env.expect('FT.SEARCH idx *').equal([2, 'doc2', ['t', 'foo'], 'doc4', ['t', 'foo']])

def testRED47209(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA t TEXT').ok()
    conn.execute_command('hset', 'doc1', 't', 'foo')
    if env.isCluster():
        # on cluster we have WITHSCORES set unconditionally for FT.SEARCH
        res = [1, 'doc1', ['t', 'foo']]
    else:
        res = [1, 'doc1', None, ['t', 'foo']]
    env.expect('FT.SEARCH idx foo WITHSORTKEYS LIMIT 0 1').equal(res)

@skip(cluster=True)
def testInvertedIndexWasEntirelyDeletedDuringCursor():
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 1')

    env.expect('FT.CREATE idx SCHEMA t TEXT').ok()
    env.expect('HSET doc1 t foo').equal(1)
    env.expect('HSET doc2 t foo').equal(1)

    res, cursor = env.cmd('FT.AGGREGATE idx foo WITHCURSOR COUNT 1')
    env.assertEqual(res, [1, []])

    # delete both documents and run the GC to clean 'foo' inverted index
    env.expect('DEL doc1').equal(1)
    env.expect('DEL doc2').equal(1)

    forceInvokeGC(env, 'idx')

    # make sure the inverted index was cleaned
    env.expect('FT.DEBUG DUMP_INVIDX idx foo').error().contains('not find the inverted index')

    # read from the cursor
    res, cursor = env.cmd('FT.CURSOR READ idx %d' % cursor)

    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)

def testNegativeOnly(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 'not', 'foo')

    env.expect('FT.SEARCH idx *').equal([1, 'doc1', ['not', 'foo']])
    env.expect('FT.SEARCH', 'idx', '-bar').equal([1, 'doc1', ['not', 'foo']])

def testNotOnly(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt1', 'TEXT').ok()
  conn.execute_command('HSET', 'a', 'txt1', 'hello', 'txt2', 'world')
  conn.execute_command('HSET', 'b', 'txt1', 'world', 'txt2', 'hello')
  env.assertEqual(toSortedFlatList(env.cmd('ft.search idx !world')), toSortedFlatList([1, 'b', ['txt1', 'world', 'txt2', 'hello']]))

def testServerVersion(env):
    env.assertTrue(server_version_at_least(env, "6.0.0"))

def testSchemaWithAs(env):
  conn = getConnectionByEnv(env)
  # sanity
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'AS', 'foo', 'TEXT')
  conn.execute_command('HSET', 'a', 'txt', 'hello')
  conn.execute_command('HSET', 'b', 'foo', 'world')

  for _ in env.reloadingIterator():
    env.expect('ft.search idx @txt:hello').equal([0])
    env.expect('ft.search idx @txt:world').equal([0])
    env.expect('ft.search idx @foo:hello').equal([1, 'a', ['txt', 'hello']])
    env.expect('ft.search idx @foo:world').equal([0])

    # RETURN from schema
    env.expect('ft.search idx hello RETURN 1 txt').equal([1, 'a', ['txt', 'hello']])
    env.expect('ft.search idx hello RETURN 1 foo').equal([1, 'a', ['foo', 'hello']])
    env.expect('ft.search idx hello RETURN 3 txt AS baz').equal([1, 'a', ['baz', 'hello']])
    env.expect('ft.search idx hello RETURN 3 foo AS baz').equal([1, 'a', ['baz', 'hello']])
    env.expect('ft.search idx hello RETURN 6 txt AS baz txt AS bar').equal([1, 'a', ['baz', 'hello', 'bar', 'hello']])
    env.expect('ft.search idx hello RETURN 6 txt AS baz txt AS baz').equal([1, 'a', ['baz', 'hello']])

    # RETURN outside of schema
    conn.execute_command('HSET', 'a', 'not_in_schema', '42')
    res = conn.execute_command('HGETALL', 'a')
    env.assertEqual(res, {'txt': 'hello', 'not_in_schema': '42'})
    env.expect('ft.search idx hello RETURN 3 not_in_schema AS txt2').equal([1, 'a', ['txt2', '42']])
    env.expect('ft.search idx hello RETURN 1 not_in_schema').equal([1, 'a', ['not_in_schema', '42']])
    env.expect('ft.search idx hello').equal([1, 'a', ['txt', 'hello', 'not_in_schema', '42']])

    env.expect('ft.search idx hello RETURN 3 not_exist as txt2').equal([1, 'a', []])
    env.expect('ft.search idx hello RETURN 1 not_exist').equal([1, 'a', []])

    env.expect('ft.search idx hello RETURN 3 txt as as').error().contains('Alias for RETURN cannot be `AS`')

    # LOAD for FT.AGGREGATE
    # for path - can rename
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '1', '@txt').equal([1, ['txt', 'hello']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '3', '@txt', 'AS', 'txt1').equal([1, ['txt1', 'hello']])

    # for name - cannot rename
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '1', '@foo').equal([1, ['foo', 'hello']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '3', '@foo', 'AS', 'foo1').equal([1, ['foo1', 'hello']])

    # for for not in schema - can rename
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '1', '@not_in_schema').equal([1, ['not_in_schema', '42']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '3', '@not_in_schema', 'AS', 'NIS').equal([1, ['NIS', '42']])

    conn.execute_command('HDEL', 'a', 'not_in_schema')

def testSchemaWithAs_Alter(env):
  conn = getConnectionByEnv(env)
  # sanity
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'AS', 'foo', 'TEXT')
  conn.execute_command('HSET', 'a', 'txt', 'hello')
  conn.execute_command('HSET', 'b', 'foo', 'world')

  # FT.ALTER
  env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'foo', 'AS', 'bar', 'TEXT')
  waitForIndex(env, 'idx')
  env.expect('ft.search idx @bar:hello').equal([0])
  env.expect('ft.search idx @bar:world').equal([1, 'b', ['foo', 'world']])
  env.expect('ft.search idx @foo:world').equal([0])

def testSchemaWithAs_Duplicates(env):
    conn = getConnectionByEnv(env)

    conn.execute_command('HSET', 'a', 'txt', 'hello')

    # Error if field name is duplicated
    res = env.expect('FT.CREATE', 'conflict1', 'SCHEMA', 'txt1', 'AS', 'foo', 'TEXT', 'txt2', 'AS', 'foo', 'TAG') \
                                                                .error().contains('Duplicate field in schema - foo')
    # Success if field path is duplicated
    res = env.expect('FT.CREATE', 'conflict2', 'SCHEMA', 'txt', 'AS', 'foo1', 'TEXT',
                                                        'txt', 'AS', 'foo2', 'TEXT').ok()
    waitForIndex(env, 'conflict2')
    env.expect('ft.search conflict2 @foo1:hello').equal([1, 'a', ['txt', 'hello']])
    env.expect('ft.search conflict2 @foo2:hello').equal([1, 'a', ['txt', 'hello']])
    env.expect('ft.search conflict2 @foo1:world').equal([0])
    env.expect('ft.search conflict2 @foo2:world').equal([0])

def testMod1407(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'limit', 'TEXT', 'LimitationTypeID', 'TAG', 'LimitationTypeDesc', 'TEXT').ok()

    conn.execute_command('HSET', 'doc1', 'limit', 'foo1', 'LimitationTypeID', 'boo1', 'LimitationTypeDesc', 'doo1')
    conn.execute_command('HSET', 'doc2', 'limit', 'foo2', 'LimitationTypeID', 'boo2', 'LimitationTypeDesc', 'doo2')

    env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '3', '@limit', '@LimitationTypeID', 'ASC').equal([2, ['limit', 'foo1', 'LimitationTypeID', 'boo1'], ['limit', 'foo2', 'LimitationTypeID', 'boo2']])

    # make sure the crashed query is not crashing anymore
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '2', 'LLimitationTypeID', 'LLimitationTypeDesc', 'REDUCE', 'COUNT', '0')

    # make sure correct query not crashing and return the right results
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '2', '@LimitationTypeID', '@LimitationTypeDesc', 'REDUCE', 'COUNT', '0').equal([2, ['LimitationTypeID', 'boo2', 'LimitationTypeDesc', 'doo2', '__generated_aliascount', '1'], ['LimitationTypeID', 'boo1', 'LimitationTypeDesc', 'doo1', '__generated_aliascount', '1']])

def testMod1452(env):
    if not env.isCluster():
        # this test is only relevant on cluster
        env.skip()

    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    conn.execute_command('HSET', 'doc1', 't', 'foo')

    # here we only check that its not crashing
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', 'foo', 'REDUCE', 'FIRST_VALUE', 3, '@not_exists', 'BY', '@foo')

@no_msan
def test_mod1548(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$["prod:id"]', 'AS', 'prod:id_bracketnotation', 'TEXT',
               '$.prod:id', 'AS', 'prod:id_dotnotation', 'TEXT',
               '$.name', 'AS', 'name', 'TEXT',
               '$.categories', 'AS', 'categories', 'TAG', 'SEPARATOR' ,',').ok()
    waitForIndex(env, 'idx')

    res = conn.execute_command('JSON.SET', 'prod:1', '$', '{"prod:id": "35114964", "SKU": "35114964", "name":"foo", "categories":"abcat0200000"}')
    env.assertOk(res)
    res = conn.execute_command('JSON.SET', 'prod:2', '$', '{"prod:id": "35114965", "SKU": "35114965", "name":"bar", "categories":"abcat0200000"}')
    env.assertOk(res)

    # Supported jsonpath
    res = env.cmd('FT.SEARCH', 'idx', '@categories:{abcat0200000}', 'RETURN', '1', 'name')
    env.assertEqual(res,  [2, 'prod:1', ['name', 'foo'], 'prod:2', ['name', 'bar']])

    # Supported jsonpath (actual path contains a colon using the bracket notation)
    res = env.cmd('FT.SEARCH', 'idx', '@categories:{abcat0200000}', 'RETURN', '1', 'prod:id_bracketnotation')
    env.assertEqual(res,  [2, 'prod:1', ['prod:id_bracketnotation', '35114964'], 'prod:2', ['prod:id_bracketnotation', '35114965']])

    # Supported jsonpath (actual path contains a colon using the dot notation)
    res = env.cmd('FT.SEARCH', 'idx', '@categories:{abcat0200000}', 'RETURN', '1', 'prod:id_dotnotation')
    env.assertEqual(res,  [2, 'prod:1', ['prod:id_dotnotation', '35114964'], 'prod:2', ['prod:id_dotnotation', '35114965']])

def test_empty_field_name(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', '', 'TEXT').ok()
    conn.execute_command('hset', 'doc1', '', 'foo')
    env.expect('FT.SEARCH', 'idx', 'foo').equal([1, 'doc1', ['', 'foo']])

@skip(cluster=True)
def test_free_resources_on_thread(env):
    conn = getConnectionByEnv(env)
    pl = conn.pipeline()
    results = []

    for _ in range(2):
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TAG', 'SORTABLE',
                                                 't2', 'TAG', 'SORTABLE',
                                                 't3', 'TAG', 'SORTABLE',
                                                 't4', 'TAG', 'SORTABLE',
                                                 't5', 'TAG', 'SORTABLE',
                                                 't6', 'TAG', 'SORTABLE',
                                                 't7', 'TAG', 'SORTABLE',
                                                 't8', 'TAG', 'SORTABLE',
                                                 't9', 'TAG', 'SORTABLE',
                                                 't10', 'TAG', 'SORTABLE',
                                                 't11', 'TAG', 'SORTABLE',
                                                 't12', 'TAG', 'SORTABLE',
                                                 't13', 'TAG', 'SORTABLE',
                                                 't14', 'TAG', 'SORTABLE',
                                                 't15', 'TAG', 'SORTABLE',
                                                 't16', 'TAG', 'SORTABLE',
                                                 't17', 'TAG', 'SORTABLE',
                                                 't18', 'TAG', 'SORTABLE',
                                                 't19', 'TAG', 'SORTABLE',
                                                 't20', 'TAG', 'SORTABLE').ok()
        for i in range(1024 * 32):
            pl.execute_command('HSET', i, 't1', i, 't2', i, 't3', i, 't4', i, 't5', i,
                                          't6', i, 't7', i, 't8', i, 't9', i, 't10', i,
                                          't11', i, 't12', i, 't13', i, 't14', i, 't15', i,
                                          't16', i, 't17', i, 't18', i, 't19', i, 't20', i)
            if i % 1000 == 0:
                pl.execute()
        pl.execute()

        start_time = time.time()
        conn.execute_command('FLUSHALL')
        end_time = time.time()

        results.append(end_time - start_time)

        conn.execute_command('FT.CONFIG', 'SET', '_FREE_RESOURCE_ON_THREAD', 'false')

    # ensure freeing resources on a 2nd thread is quicker
    # than freeing it on the main thread
    # (skip this check point on CI since it is not guaranteed)
    if not CI:
        env.assertLess(results[0], results[1])

    conn.execute_command('FT.CONFIG', 'SET', '_FREE_RESOURCE_ON_THREAD', 'true')

def testUsesCounter(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'NOFIELDS', 'schema', 'title', 'text').ok()
    env.cmd('ft.info', 'idx')
    env.cmd('ft.search', 'idx', '*')

    assertInfoField(env, 'idx', 'number_of_uses', 3)

def test_aggregate_return_fail(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'RETURN', '1', 'test').error().contains("RETURN is not supported on FT.AGGREGATE")

def test_emoji(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('FT.CREATE', 'idx_tag', 'ON', 'HASH', 'SCHEMA', 'test', 'TAG').equal('OK')

    conn.execute_command('HSET', 'doc1', 'test', 'a📌')
    env.expect('ft.search', 'idx', 'a📌').equal([1, 'doc1', ['test', 'a📌']])
    env.expect('ft.search', 'idx_tag', '@test:{a📌}').equal([1, 'doc1', ['test', 'a📌']])
    conn.execute_command('HSET', 'doc2', 'test', '💮a')
    env.expect('ft.search', 'idx', '💮a').equal([1, 'doc2', ['test', '💮a']])
    env.expect('ft.search', 'idx_tag', '@test:{💮a}').equal([1, 'doc2', ['test', '💮a']])
    conn.execute_command('HSET', 'doc3', 'test', '💩')
    env.expect('ft.search', 'idx', '💩').equal([1, 'doc3', ['test', '💩']])
    env.expect('ft.search', 'idx_tag', '@test:{💩}').equal([1, 'doc3', ['test', '💩']])
    '''
    conn.execute_command('HSET', 'doc4', 'test', '😀😁🙂')
    env.expect('ft.search', 'idx', '😀😁*').equal([1, 'doc4', ['test', '😀😁🙂']])
    env.expect('ft.search', 'idx', '%😀😁%').equal([1, 'doc4', ['test', '😀😁🙂']])
    conn.execute_command('HSET', 'doc4', 'test', '')
    '''

def test_mod_4200(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    for i in range(1001):
        env.expect('ft.add', 'idx', 'doc%i' % i, '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '((~foo) foo) | ((~foo) foo)', 'LIMIT', '0', '0').equal([1001])

@skip(cluster=True)
def test_RED_86036(env):
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    for i in range(1000):
        env.cmd('hset', 'doc%d' % i, 't', 'foo')
    res = env.cmd('FT.PROFILE', 'idx', 'search', 'query', '*', 'INKEYS', '2', 'doc0', 'doc999')
    res = res[1][4][1][7] # get the list iterator profile
    env.assertEqual(res[1], 'ID-LIST')
    env.assertLess(res[5], 3)

def test_MOD_4290(env):
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('hset', 'doc%d' % i, 't', 'foo')
    env.cmd('FT.PROFILE', 'idx', 'aggregate', 'query', '*', 'LIMIT', '0', '1')
    env.expect('ping').equal(True) # make sure environment is still up */

@skip(cluster=True)
def test_missing_schema(env):
    # MOD-4388: assert on sp->indexer

    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'foo', 'TEXT').equal('OK')
    env.expect('FT.CREATE', 'idx2', 'TEMPORARY', 1000, 'foo', 'bar').error().contains('Unknown argument `foo`')
    # make sure the index succeecfully index new docs
    conn.execute_command('HSET', 'doc1', 'foo', 'bar')
    env.expect('FT.SEARCH', 'idx1', '*').equal([1, 'doc1', ['foo', 'bar']] )
    env.expect('FT.SEARCH', 'idx2', '*').error().equal('idx2: no such index')


@skip(cluster=False) # this test is only relevant on cluster
def test_cluster_set(env):
    cluster_set_test(env)

@skip(cluster=False) # this test is only relevant on cluster
def test_cluster_set_with_password():
    mypass = '42MySecretPassword'
    args = 'OSS_GLOBAL_PASSWORD ' + mypass
    env = Env(moduleArgs=args, password=mypass)
    cluster_set_test(env)

def cluster_set_test(env: Env):
    def verify_address(addr):
        try:
            with TimeLimit(10):
                res = None
                while res is None or res[9][2][1] != addr:
                    res = env.cmd('SEARCH.CLUSTERINFO')
        except Exception:
            env.assertTrue(False, message='Failed waiting cluster set command to be updated with the new IP address %s' % addr)

    password = env.password + "@" if env.password else ""
    # test ipv4
    env.expect('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '16383',
               'ADDR',
               f'{password}127.0.0.1:{env.port}',
               'MASTER'
            ).ok()
    verify_address('127.0.0.1')

    env.stop()
    env.start()

    # test ipv6 test
    env.expect('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '16383',
               'ADDR',
               f'{password}[::1]:{env.port}',
               'MASTER'
            ).ok()
    verify_address('::1')

    env.stop()
    env.start()

    # test unix socket
    env.expect('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '16383',
               'ADDR',
               f'{password}localhost:{env.port}',
               'UNIXADDR',
               '/tmp/redis.sock',
               'MASTER'
            ).ok()
    verify_address('localhost')
    # check that multiple unix sockets are not allowed
    env.expect('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '16383',
               'ADDR',
               f'{password}localhost:{env.port}',
               'UNIXADDR',
               '/tmp/redis.sock',
               'UNIXADDR',
               '/tmp/another.sock',
               'MASTER'
            ).error().contains('Syntax error at offset').contains("near 'UNIXADDR'")

    invalid_addresses = [
        'invalid',
        'invalid:',
        'localhost:invalid',
        '[::1:234'
    ]
    for addr in invalid_addresses:
        # Test withouth unix socket
        env.expect('SEARCH.CLUSTERSET',
                   'MYID',
                   '1',
                   'RANGES',
                   '1',
                   'SHARD',
                   '1',
                   'SLOTRANGE',
                   '0',
                   '16383',
                   'ADDR',
                    addr,
                   'MASTER'
            ).error().contains('Invalid tcp address').contains(addr)
        # Test with unix socket
        env.expect('SEARCH.CLUSTERSET',
                   'MYID',
                   '1',
                   'RANGES',
                   '1',
                   'SHARD',
                   '1',
                   'SLOTRANGE',
                   '0',
                   '16383',
                   'ADDR',
                    addr,
                   'UNIXADDR',
                   '/tmp/redis.sock',
                   'MASTER'
            ).error().contains('Invalid tcp address').contains(addr)

def test_cluster_set_server_memory_tracking(env):
    if not env.isCluster():
        # this test is only relevant on cluster
        env.skip()

    def get_memory(env):
        res = env.cmd('INFO', "MEMORY")
        return res['used_memory']

    def cluster_set_ipv4():
        env.cmd('SEARCH.CLUSTERSET',
               'MYID',
               '1',
               'RANGES',
               '1',
               'SHARD',
               '1',
               'SLOTRANGE',
               '0',
               '16383',
               'ADDR',
               'password@127.0.0.1:22000',
               'MASTER'
            )
    for _ in range(200):
        cluster_set_ipv4()
    initial = get_memory(env) - 1024 * 1024 # to account for some variance in memory
    for _ in range(1_000): # hangs at 1932 iterations. need to determine the cause
        cluster_set_ipv4()
        mem = get_memory(env)
        env.assertLessEqual(initial, mem)



def test_internal_commands(env):
    ''' Test that internal cluster commands cannot run from a script '''
    if not env.isCluster():
        env.skip()

    def fail_eval_call(r, env, cmd):
        cmd = str(cmd)[1:-1]
        try:
            r.eval(f'redis.call({cmd})', 0)
            env.assertTrue(False, message=f'Failed to raise error during call to {cmd}')
        except redis.ResponseError as e:
            env.assertTrue(str(e).index("not allowed from script") != -1)

    with env.getClusterConnectionIfNeeded() as r:
        fail_eval_call(r, env, ['SEARCH.CLUSTERSET', 'MYID', '1', 'RANGES', '1', 'SHARD', '1', 'SLOTRANGE', '0', '16383', 'ADDR', 'password@127.0.0.1:22000', 'MASTER'])
        fail_eval_call(r, env, ['SEARCH.CLUSTERREFRESH'])
        fail_eval_call(r, env, ['SEARCH.CLUSTERINFO'])

def test_timeout_non_strict_policy(env):
    """Tests that we get the wanted behavior for the non-strict timeout policy.
    `ON_TIMEOUT RETURN` - return partial results.
    """

    conn = getConnectionByEnv(env)

    # Create an index, and populate it
    n = 25000
    populate_db(env, text=True, n_per_shard=n)

    # Query the index with a small timeout, and verify that we get partial results
    num_docs = n * env.shardsCount
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
        )
    env.assertTrue(len(res) < num_docs * 2 + 1)

    # Same for `FT.AGGREGATE`
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@text1', 'TIMEOUT', '1'
        )
    env.assertTrue(len(res) < num_docs + 1)

def test_timeout_strict_policy():
    """Tests that we get the wanted behavior for the strict timeout policy.
    `ON_TIMEOUT FAIL` - return an error upon experiencing a timeout, without the
    partial results.
    """

    env = Env(moduleArgs='ON_TIMEOUT FAIL')

    # Create an index, and populate it
    n = 25000
    populate_db(env, text=True, n_per_shard=n)

    # Query the index with a small timeout, and verify that we get an error
    num_docs = n * env.shardsCount
    env.expect(
        'FT.SEARCH', 'idx', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
        ).error().contains('Timeout limit was reached')

    # Same for `FT.AGGREGATE`
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@text1', 'TIMEOUT', '1'
        ).error().contains('Timeout limit was reached')

def common_with_auth(env: Env):
    conn = getConnectionByEnv(env)
    n_docs = 100

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc{i}', 'n', i)

    if env.isCluster():
        # Mimic periodic cluster refresh
        env.expect('SEARCH.CLUSTERREFRESH').ok()

    expected_res = [n_docs]
    for i in range(10):
        expected_res.extend([f'doc{i}', ['n', str(i)]])
    env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 'n').equal(expected_res)

def test_with_password():
    mypass = '42MySecretPassword$'
    args = f'OSS_GLOBAL_PASSWORD {mypass}' if COORD else None
    env = Env(moduleArgs=args, password=mypass)
    common_with_auth(env)

def test_with_tls():
    cert_file, key_file, ca_cert_file, passphrase = get_TLS_args()
    env = Env(useTLS=True,
              tlsCertFile=cert_file,
              tlsKeyFile=key_file,
              tlsCaCertFile=ca_cert_file,
              tlsPassphrase=passphrase)

    common_with_auth(env)

@skip(asan=True)
def test_timeoutCoordSearch_NonStrict(env):
    """Tests edge-cases for the `TIMEOUT` parameter for the coordinator's
    `FT.SEARCH` path"""

    if VALGRIND:
        unittest.SkipTest()

    SkipOnNonCluster(env)

    # Create and populate an index
    n_docs_pershard = 1100
    n_docs = n_docs_pershard * env.shardsCount
    populate_db(env, text=True, n_per_shard=n_docs_pershard)

    # test erroneous params
    env.expect('ft.search', 'idx', '* aa*', 'timeout').error()
    env.expect('ft.search', 'idx', '* aa*', 'timeout', -1).error()
    env.expect('ft.search', 'idx', '* aa*', 'timeout', 'STR').error()

    res = env.cmd('ft.search', 'idx', '*', 'TIMEOUT', '0')
    env.assertEqual(res[0], n_docs)

    res = env.cmd('ft.search', 'idx', '*', 'TIMEOUT', '1')
    env.assertLessEqual(res[0], n_docs)

@skip(asan=True)
def test_timeoutCoordSearch_Strict():
    """Tests edge-cases for the `TIMEOUT` parameter for the coordinator's
    `FT.SEARCH` path, when the timeout policy is strict"""

    if VALGRIND:
        unittest.SkipTest()

    env = Env(moduleArgs='ON_TIMEOUT FAIL')

    SkipOnNonCluster(env)

    # Create and populate an index
    n_docs_pershard = 50000
    n_docs = n_docs_pershard * env.shardsCount
    populate_db(env, text=True, n_per_shard=n_docs_pershard)

    # test erroneous params
    env.expect('ft.search', 'idx', '* aa*', 'timeout').error()
    env.expect('ft.search', 'idx', '* aa*', 'timeout', -1).error()
    env.expect('ft.search', 'idx', '* aa*', 'timeout', 'STR').error()

    res = env.cmd('ft.search', 'idx', '*', 'TIMEOUT', '0')
    env.assertEqual(res[0], n_docs)

    res = env.cmd('ft.search', 'idx', '*', 'TIMEOUT', '100000')
    env.assertEqual(res[0], n_docs)

    env.expect('ft.search', 'idx', '*', 'TIMEOUT', '1').error().contains('Timeout limit was reached')

@skip(cluster=True)
def test_notIterTimeout(env):
    """Tests that we fail fast from the NOT iterator in the edge case similar to
    MOD-5512
    * Skipped on cluster since the it would only test error propagation from the
    shard to the coordinator, which is tested elsewhere.
    """

    if VALGRIND:
        env.skip()

    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CONFIG', 'SET', 'ON_TIMEOUT', 'FAIL')

    # Create an index
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'tag1', 'TAG', 'title', 'TEXT', 'n', 'NUMERIC')

    # Populate the index
    num_docs = 15000
    for i in range(int(num_docs / 2)):
        env.cmd('HSET', f'doc:{i}', 'tag1', 'fantasy', 'title', f'title:{i}', 'n', i)

    # Populate with other tag value in a separate loop so doc-ids will be incremental.
    for i in range(int(num_docs / 2), num_docs):
        env.cmd('HSET', f'doc:{i}', 'tag1', 'drama', 'title', f'title:{i}', 'n', i)

    # Send a query that will skip all the docs with the first tag value (fantasy),
    # such that the timeout will be checked in the NOT iterator loop (coverage).
    env.expect(
        'FT.AGGREGATE', 'idx', '-@tag1:{fantasy}', 'LOAD', '2', '@title', '@n',
        'APPLY', '@n^2 / 2', 'AS', 'new_n', 'GROUPBY', '1', '@title', 'TIMEOUT', '1'
    ).error().contains('Timeout limit was reached')
