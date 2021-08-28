# -*- coding: utf-8 -*-

import redis
import unittest
from hotels import hotels
import random
import time
from RLTest import Env
from includes import *
from common import *

# this tests is not longer relevant
# def testAdd(env):
#     if env.is_cluster():
#         raise unittest.SkipTest()

#     r = env
#     env.assertOk(r.execute_command(
#         'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
#     env.assertTrue(r.exists('idx:idx'))
#     env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
#                                     'title', 'hello world',
#                                     'body', 'lorem ist ipsum'))

#     for _ in r.retry_with_rdb_reload():
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
    r = env
    N = 100
    env.assertOk(r.execute_command(
        "ft.create", "test", 'ON', 'HASH',
        "SCHEMA",  "tags", "TAG", "waypoint", "GEO"))
    env.assertOk(r.execute_command(
        "ft.add", "test", "1", "1", "FIELDS", "tags", "alberta", "waypoint", "-113.524,53.5244"))
    env.assertOk(r.execute_command(
        "ft.add", "test", "2", "1", "FIELDS", "tags", "ontario", "waypoint", "-79.395,43.661667"))

    r.cmd('ft.search', 'test', '@tags:{ontario}')

    res = r.execute_command(
        'ft.search', 'test', "@waypoint:[-113.52 53.52 20 mi]|@tags:{ontario}", 'nocontent')
    env.assertEqual(res, [2L, '1', '2'])

def testAttributes(env):
    env.assertOk(env.cmd('ft.create', 'idx','ON', 'HASH',
                         'schema', 'title', 'text', 'body', 'text'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 't1 t2', 'body', 't3 t4 t5'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                           'body', 't1 t2', 'title', 't3 t5'))

    res = env.cmd(
        'ft.search', 'idx', '(@title:(t1 t2) => {$weight: 0.2}) |(@body:(t1 t2) => {$weight: 0.5})', 'nocontent')
    env.assertListEqual([2L, 'doc2', 'doc1'], res)
    res = env.cmd(
        'ft.search', 'idx', '(@title:(t1 t2) => {$weight: 2.5}) |(@body:(t1 t2) => {$weight: 0.5})', 'nocontent')
    env.assertListEqual([2L, 'doc1', 'doc2'], res)

    res = env.cmd(
        'ft.search', 'idx', '(t3 t5) => {$slop: 4}', 'nocontent')
    env.assertListEqual([2L, 'doc2', 'doc1'], res)
    res = env.cmd(
        'ft.search', 'idx', '(t5 t3) => {$slop: 0}', 'nocontent')
    env.assertListEqual([1L, 'doc2'], res)
    res = env.cmd(
        'ft.search', 'idx', '(t5 t3) => {$slop: 0; $inorder:true}', 'nocontent')
    env.assertListEqual([0], res)

def testUnion(env):
    N = 100
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx','ON', 'HASH', 'schema', 'f', 'text'))
    for i in range(N):

        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world' if i % 2 == 0 else 'hallo werld'))

    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        res = r.execute_command(
            'ft.search', 'idx', 'hello|hallo', 'nocontent', 'limit', '0', '100')
        env.assertEqual(N + 1, len(res))
        env.assertEqual(N, res[0])

        res = r.execute_command(
            'ft.search', 'idx', 'hello|world', 'nocontent', 'limit', '0', '100')
        env.assertEqual(51, len(res))
        env.assertEqual(50, res[0])

        res = r.execute_command('ft.search', 'idx', '(hello|hello)(world|world)',
                                'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(51, len(res))
        env.assertEqual(50, res[0])

        res = r.execute_command(
            'ft.search', 'idx', '(hello|hallo)(werld|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

        res = r.execute_command(
            'ft.search', 'idx', '(hallo|hello)(world|werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

        res = r.execute_command(
            'ft.search', 'idx', '(hello|werld)(hallo|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

        res = r.execute_command(
            'ft.search', 'idx', '(hello|hallo) world', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(51, len(res))
        env.assertEqual(50, res[0])

        res = r.execute_command(
            'ft.search', 'idx', '(hello world)|((hello world)|(hallo world|werld) | hello world werld)',
            'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

def testSearch(env):
    r = env
    r.expect('ft.create', 'idx', 'ON', 'HASH',
             'schema', 'title', 'text', 'weight', 10.0, 'body', 'text').ok()
    r.expect('ft.add', 'idx', 'doc1', 0.5,
             'fields','title', 'hello world', 'body', 'lorem ist ipsum').ok()
    r.expect('ft.add', 'idx', 'doc2', 1.0,
             'fields', 'title', 'hello another world', 'body', 'lorem ist ipsum lorem lorem').ok()
    # order of documents might change after reload
    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        res = r.execute_command('ft.search', 'idx', 'hello')
        expected = [2L, 'doc2', ['title', 'hello another world', 'body', 'lorem ist ipsum lorem lorem'],
                    'doc1', ['title', 'hello world', 'body', 'lorem ist ipsum']]
        env.assertEqual(toSortedFlatList(res), toSortedFlatList(expected))

        # Test empty query
        res = r.execute_command('ft.search', 'idx', '')
        env.assertListEqual([0], res)

        # Test searching with no content
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent')
        env.assertTrue(len(res) == 3)
        expected = ['doc2', 'doc1']
        env.assertEqual(res[0], 2L)
        for item in expected:
            env.assertIn(item, res)

        # Test searching WITHSCORES
        res = r.execute_command('ft.search', 'idx', 'hello', 'WITHSCORES')
        env.assertEqual(len(res), 7)
        env.assertEqual(res[0], 2L)
        for item in expected:
            env.assertIn(item, res)
        env.assertTrue(float(res[2]) > 0)
        env.assertTrue(float(res[5]) > 0)

        # Test searching WITHSCORES NOCONTENT
        res = r.execute_command('ft.search', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
        env.assertEqual(len(res), 5)
        env.assertEqual(res[0], 2L)
        for item in expected:
            env.assertIn(item, res)
        env.assertTrue(float(res[2]) > 0)
        env.assertTrue(float(res[4]) > 0)

def testGet(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'foo', 'text', 'bar', 'text'))

    env.expect('ft.get').error().contains("wrong number of arguments")
    env.expect('ft.get', 'idx').error().contains("wrong number of arguments")
    env.expect('ft.get', 'idx', 'foo', 'bar').error().contains("wrong number of arguments")
    env.expect('ft.mget').error().contains("wrong number of arguments")
    env.expect('ft.mget', 'idx').error().contains("wrong number of arguments")
    env.expect('ft.mget', 'fake_idx').error().contains("wrong number of arguments")

    env.expect('ft.get fake_idx foo').error().contains("Unknown Index name")
    env.expect('ft.mget fake_idx foo').error().contains("Unknown Index name")

    for i in range(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello world', 'bar', 'wat wat'))

    for i in range(100):
        res = r.execute_command('ft.get', 'idx', 'doc%d' % i)
        env.assertIsNotNone(res)
        env.assertEqual(set(['foo', 'hello world', 'bar', 'wat wat']), set(res))
        env.assertIsNone(r.execute_command(
            'ft.get', 'idx', 'doc%dsdfsd' % i))
    env.expect('ft.get', 'no_idx', 'doc0').error().contains("Unknown Index name")

    rr = r.execute_command(
        'ft.mget', 'idx', *('doc%d' % i for i in range(100)))
    env.assertEqual(len(rr), 100)
    for res in rr:
        env.assertIsNotNone(res)
        env.assertEqual(set(['foo', 'hello world', 'bar', 'wat wat']), set(res))
    rr = r.execute_command(
        'ft.mget', 'idx', *('doc-%d' % i for i in range(100)))
    env.assertEqual(len(rr), 100)
    for res in rr:
        env.assertIsNone(res)

    # Verify that when a document is deleted, GET returns NULL
    r.cmd('ft.del', 'idx', 'doc10') # But we still keep the document
    r.cmd('ft.del', 'idx', 'doc11')
    assert r.cmd('ft.del', 'idx', 'coverage') == 0
    res = r.cmd('ft.get', 'idx', 'doc10')
    r.assertEqual(None, res)
    res = r.cmd('ft.mget', 'idx', 'doc10')
    r.assertEqual([None], res)
    res = r.cmd('ft.mget', 'idx', 'doc10', 'doc11', 'doc12')
    r.assertIsNone(res[0])
    r.assertIsNone(res[1])
    r.assertTrue(not not res[2])

    env.expect('ft.add idx doc 0.1 language arabic payload redislabs fields foo foo').ok()
    env.expect('ft.get idx doc').equal(['foo', 'foo'])
    res = env.cmd('hgetall doc')
    env.assertEqual(set(res), set(['foo', 'foo', '__score', '0.1', '__language', 'arabic', '__payload', 'redislabs']))


def testDelete(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text'))

    for i in range(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world'))

    env.expect('ft.del', 'fake_idx', 'doc1').error()

    for i in range(100):
        # the doc hash should exist now
        r.expect('ft.get', 'idx', 'doc%d' % i).notRaiseError()
        # Delete the actual docs only half of the time
        env.assertEqual(1, r.execute_command(
           'ft.del', 'idx', 'doc%d' % i, 'DD' if i % 2 == 0 else ''))
        # second delete should return 0
        env.assertEqual(0, r.execute_command(
            'ft.del', 'idx', 'doc%d' % i))
        # second delete should return 0

        # TODO: return 0 if doc wasn't found
        #env.assertEqual(0, r.execute_command(
        #    'ft.del', 'idx', 'doc%d' % i))

        # After del with DD the doc hash should not exist
        if i % 2 == 0:
            env.assertFalse(r.exists('doc%d' % i))
        else:
            r.expect('ft.get', 'idx', 'doc%d' % i).notRaiseError()
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
        env.assertNotIn('doc%d' % i, res)
        env.assertEqual(res[0], 100 - i - 1)
        env.assertEqual(len(res), 100 - i)

        # test reinsertion
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world'))
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
        env.assertIn('doc%d' % i, res)
        env.assertEqual(1, r.execute_command(
            'ft.del', 'idx', 'doc%d' % i))
    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        did = 'rrrr'
        env.assertOk(r.execute_command('ft.add', 'idx', did, 1, 'fields',
                                        'f', 'hello world'))
        env.assertEqual(1, r.execute_command('ft.del', 'idx', did))
        env.assertEqual(0, r.execute_command('ft.del', 'idx', did))
        env.assertOk(r.execute_command('ft.add', 'idx', did, 1, 'fields',
                                        'f', 'hello world'))
        env.assertEqual(1, r.execute_command('ft.del', 'idx', did))
        env.assertEqual(0, r.execute_command('ft.del', 'idx', did))

def testReplace(env):
    r = env

    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'f', 'hello world'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'f', 'hello world'))
    res = r.execute_command(
        'ft.search', 'idx', 'hello world')
    env.assertEqual(2, res[0])

    with env.assertResponseError():
        # make sure we can't insert a doc twice
        res = r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                'f', 'hello world')

    # now replace doc1 with a different content
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'replace', 'fields',
                                    'f', 'goodbye universe'))

    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        # make sure the query for hello world does not return the replaced
        # document
        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'nocontent')
        env.assertEqual(1, res[0])
        env.assertEqual('doc2', res[1])

        # search for the doc's new content
        res = r.execute_command(
            'ft.search', 'idx', 'goodbye universe', 'nocontent')
        env.assertEqual(1, res[0])
        env.assertEqual('doc1', res[1])

def testDrop(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo'))

    for i in range(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world', 'n', 666, 't', 'foo bar',
                                        'g', '19.04,47.497'))
    keys = r.keys('*')
    env.assertGreaterEqual(len(keys), 100)

    env.assertOk(r.execute_command('ft.drop', 'idx'))
    keys = r.keys('*')

    env.assertEqual(0, len(keys))
    env.flush()

    # Now do the same with KEEPDOCS
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo'))

    for i in range(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world', 'n', 666, 't', 'foo bar',
                                        'g', '19.04,47.497'))
    keys = r.keys('*')
    env.assertGreaterEqual(len(keys), 100)

    if not env.is_cluster():
        env.assertOk(r.execute_command('ft.drop', 'idx', 'KEEPDOCS'))
        keys = r.keys('*')
        env.assertListEqual(['doc0', 'doc1', 'doc10', 'doc11', 'doc12', 'doc13', 'doc14', 'doc15', 'doc16', 'doc17', 'doc18', 'doc19', 'doc2', 'doc20', 'doc21', 'doc22', 'doc23', 'doc24', 'doc25', 'doc26', 'doc27', 'doc28', 'doc29', 'doc3', 'doc30', 'doc31', 'doc32', 'doc33', 'doc34', 'doc35', 'doc36', 'doc37', 'doc38', 'doc39', 'doc4', 'doc40', 'doc41', 'doc42', 'doc43', 'doc44', 'doc45', 'doc46', 'doc47', 'doc48', 'doc49', 'doc5', 'doc50', 'doc51', 'doc52', 'doc53',
                              'doc54', 'doc55', 'doc56', 'doc57', 'doc58', 'doc59', 'doc6', 'doc60', 'doc61', 'doc62', 'doc63', 'doc64', 'doc65', 'doc66', 'doc67', 'doc68', 'doc69', 'doc7', 'doc70', 'doc71', 'doc72', 'doc73', 'doc74', 'doc75', 'doc76', 'doc77', 'doc78', 'doc79', 'doc8', 'doc80', 'doc81', 'doc82', 'doc83', 'doc84', 'doc85', 'doc86', 'doc87', 'doc88', 'doc89', 'doc9', 'doc90', 'doc91', 'doc92', 'doc93', 'doc94', 'doc95', 'doc96', 'doc97', 'doc98', 'doc99'], sorted(keys))

    env.expect('FT.DROP', 'idx', 'KEEPDOCS', '666').error().contains("wrong number of arguments")

def testDelete(env):
    r = env
    r.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo').ok()

    for i in range(100):
        r.expect('ft.add', 'idx', 'doc%d' % i, 1.0,
                 'fields', 'f', 'hello world', 'n', 666, 't', 'foo bar',
                 'g', '19.04,47.497').ok()
    keys = r.keys('*')
    env.assertGreaterEqual(len(keys), 100)

    r.expect('FT.DROPINDEX', 'idx', 'dd').ok()
    keys = r.keys('*')

    env.assertEqual(0, len(keys))
    env.flush()

    # Now do the same with KEEPDOCS
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo').ok()

    for i in range(100):
        r.expect('ft.add', 'idx', 'doc%d' % i, 1.0,
                 'fields', 'f', 'hello world', 'n', 666, 't', 'foo bar',
                 'g', '19.04,47.497').ok()
    keys = r.keys('*')
    env.assertGreaterEqual(len(keys), 100)

    if not env.is_cluster():
        r.expect('FT.DROPINDEX', 'idx').ok()
        keys = r.keys('*')
        env.assertListEqual(sorted("doc%d" %k for k in range(100)), sorted(keys))

    env.expect('FT.DROPINDEX', 'idx', 'dd', '666').error().contains("wrong number of arguments")

def testCustomStopwords(env):
    r = env
    # Index with default stopwords
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text'))

    # Index with custom stopwords
    env.assertOk(r.execute_command('ft.create', 'idx2', 'ON', 'HASH', 'stopwords', 2, 'hello', 'world',
                                    'schema', 'foo', 'text'))
    assertInfoField(env, 'idx2', 'stopwords_list', ['hello', 'world'])

    # Index with NO stopwords
    env.assertOk(r.execute_command('ft.create', 'idx3', 'ON', 'HASH', 'stopwords', 0,
                                    'schema', 'foo', 'text'))
    assertInfoField(env, 'idx3', 'stopwords_list', [])

    # 2nd Index with NO stopwords - check global is used and freed
    env.assertOk(r.execute_command('ft.create', 'idx4', 'ON', 'HASH', 'stopwords', 0,
                                    'schema', 'foo', 'text'))

    #for idx in ('idx', 'idx2', 'idx3'):
    env.assertOk(r.execute_command(
        'ft.add', 'idx', 'doc1', 1.0, 'fields', 'foo', 'hello world'))
    env.assertOk(r.execute_command(
        'ft.add', 'idx', 'doc2', 1.0, 'fields', 'foo', 'to be or not to be'))

    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        # Normal index should return results just for 'hello world'
        env.assertEqual([1, 'doc1'],  r.execute_command(
            'ft.search', 'idx', 'hello world', 'nocontent'))
        env.assertEqual([0],  r.execute_command(
            'ft.search', 'idx', 'to be or not', 'nocontent'))

        # Custom SW index should return results just for 'to be or not'
        env.assertEqual([0],  r.execute_command(
            'ft.search', 'idx2', 'hello world', 'nocontent'))
        env.assertEqual([1, 'doc2'],  r.execute_command(
            'ft.search', 'idx2', 'to be or not', 'nocontent'))

        # No SW index should return results for both
        env.assertEqual([1, 'doc1'],  r.execute_command(
            'ft.search', 'idx3', 'hello world', 'nocontent'))
        env.assertEqual([1, 'doc2'],  r.execute_command(
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
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx',
                                    'doc1', 1.0, 'fields', 'foo', 'hello wat woot'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2',
                                    1.0, 'fields', 'foo', 'hello world woot'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc3',
                                    1.0, 'fields', 'foo', 'hello world werld'))

    expected = [3L, 'doc1', 'doc2', 'doc3']
    res = r.execute_command('ft.search', 'idx', 'hello', 'nocontent')
    env.assertEqual(res, expected)
    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual([2L, 'doc2', 'doc3'], res)
    res = r.execute_command(
        'ft.search', 'idx', 'hello ~world', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual(res, expected)
    res = r.execute_command(
        'ft.search', 'idx', 'hello ~world ~werld', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual(res, expected)
    res = r.execute_command(
        'ft.search', 'idx', '~world ~werld hello', 'withscores', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual(res, [3L, 'doc3', '3', 'doc2', '2', 'doc1', '1'])

def testExplain(env):

    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
    q = '(hello world) "what what" hello|world @bar:[10 100]|@bar:[200 300]'
    res = r.execute_command('ft.explain', 'idx', q)
    # print res.replace('\n', '\\n')
    # expected = """INTERSECT {\n  UNION {\n    hello\n    +hello(expanded)\n  }\n  UNION {\n    world\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    # expected = """INTERSECT {\n  UNION {\n    hello\n    <HL(expanded)\n    +hello(expanded)\n  }\n  UNION {\n    world\n    <ARLT(expanded)\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      <HL(expanded)\n      +hello(expanded)\n    }\n    UNION {\n      world\n      <ARLT(expanded)\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    expected = """INTERSECT {\n  UNION {\n    hello\n    +hello(expanded)\n  }\n  UNION {\n    world\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    env.assertEqual(res, expected)


    # expected = ['INTERSECT {', '  UNION {', '    hello', '    <HL(expanded)', '    +hello(expanded)', '  }', '  UNION {', '    world', '    <ARLT(expanded)', '    +world(expanded)', '  }', '  EXACT {', '    what', '    what', '  }', '  UNION {', '    UNION {', '      hello', '      <HL(expanded)', '      +hello(expanded)', '    }', '    UNION {', '      world', '      <ARLT(expanded)', '      +world(expanded)', '    }', '  }', '  UNION {', '    NUMERIC {10.000000 <= @bar <= 100.000000}', '    NUMERIC {200.000000 <= @bar <= 300.000000}', '  }', '}', '']
    if env.is_cluster():
        raise unittest.SkipTest()
    res = env.cmd('ft.explainCli', 'idx', q)
    expected = ['INTERSECT {', '  UNION {', '    hello', '    +hello(expanded)', '  }', '  UNION {', '    world', '    +world(expanded)', '  }', '  EXACT {', '    what', '    what', '  }', '  UNION {', '    UNION {', '      hello', '      +hello(expanded)', '    }', '    UNION {', '      world', '      +world(expanded)', '    }', '  }', '  UNION {', '    NUMERIC {10.000000 <= @bar <= 100.000000}', '    NUMERIC {200.000000 <= @bar <= 300.000000}', '  }', '}', '']
    env.assertEqual(expected, res)

def testNoIndex(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema',
        'foo', 'text',
        'num', 'numeric', 'sortable', 'noindex',
        'extra', 'text', 'noindex', 'sortable'))

    if not env.isCluster():
        # to specific check on cluster, todo : change it to be generic enough
        res = env.cmd('ft.info', 'idx')
        env.assertEqual(res[7][1][7], 'NOINDEX')
        env.assertEqual(res[7][2][9], 'NOINDEX')

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'fields',
                                    'foo', 'hello world', 'num', 1, 'extra', 'hello lorem ipsum'))
    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'nocontent')
    env.assertListEqual([1, 'doc1'], res)
    res = r.execute_command(
        'ft.search', 'idx', 'lorem ipsum', 'nocontent')
    env.assertListEqual([0], res)
    res = r.execute_command(
        'ft.search', 'idx', '@extra:hello', 'nocontent')
    env.assertListEqual([0], res)
    res = r.execute_command(
        'ft.search', 'idx', '@num:[1 1]', 'nocontent')
    env.assertListEqual([0], res)

def testPartial(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',  'SCORE_FIELD', '__score',
        'schema',
        'foo', 'text',
        'num', 'numeric', 'sortable', 'noindex',
        'extra', 'text', 'noindex'))
    # print r.execute_command('ft.info', 'idx')

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'fields',
                                    'foo', 'hello world', 'num', 1, 'extra', 'lorem ipsum'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', '0.1', 'fields',
                                    'foo', 'hello world', 'num', 2, 'extra', 'abba'))
    res = r.execute_command('ft.search', 'idx', 'hello world',
                            'sortby', 'num', 'asc', 'nocontent', 'withsortkeys')
    env.assertListEqual([2L, 'doc1', '#1', 'doc2', '#2'], res)
    res = r.execute_command('ft.search', 'idx', 'hello world',
                            'sortby', 'num', 'desc', 'nocontent', 'withsortkeys')
    env.assertListEqual([2L, 'doc2', '#2', 'doc1', '#1'], res)

    # Updating non indexed fields doesn't affect search results
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                                    'fields', 'num', 3, 'extra', 'jorem gipsum'))
    env.expect('ft.add', 'idx', 'doc12', '0.1', 'replace', 'partial',
                                    'fields', 'num1', 'redis').equal('OK')

    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'sortby', 'num', 'desc',)
    assertResultsEqual(env, [2L, 'doc1', ['foo', 'hello world', 'num', '3','extra', 'jorem gipsum'],
        'doc2', ['foo', 'hello world', 'num', '2', 'extra', 'abba']], res)
    res = r.execute_command(
        'ft.search', 'idx', 'hello', 'nocontent', 'withscores')
    # Updating only indexed field affects search results
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                                    'fields', 'foo', 'wat wet'))
    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'nocontent')
    env.assertListEqual([1L, 'doc2'], res)
    res = r.execute_command('ft.search', 'idx', 'wat', 'nocontent')
    env.assertListEqual([1L, 'doc1'], res)

    # Test updating of score and no fields
    res = r.execute_command(
        'ft.search', 'idx', 'wat', 'nocontent', 'withscores')
    env.assertLess(float(res[2]), 1)
    # env.assertListEqual([1L, 'doc1'], res)
    env.assertOk(r.execute_command('ft.add', 'idx',
                                    'doc1', '1.0', 'replace', 'partial', 'fields'))
    res = r.execute_command(
        'ft.search', 'idx', 'wat', 'nocontent', 'withscores')
    # We reindex though no new fields, just score is updated. this effects score
    env.assertEqual(float(res[2]), 1)

    # Test updating payloads
    res = r.execute_command(
        'ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
    env.assertIsNone(res[2])
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '1.0',
                                    'replace', 'partial', 'payload', 'foobar', 'fields'))
    res = r.execute_command(
        'ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
    env.assertEqual('foobar', res[2])

def testPaging(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
    N = 100
    for i in range(N):
        env.assertOk(r.execute_command('ft.add', 'idx', '%d' % i, 1, 'fields',
                                        'foo', 'hello', 'bar', i))

    chunk = 7
    offset = 0
    while True:

        res = r.execute_command(
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
    res = r.execute_command(
        'ft.search', 'idx', 'hello', 'nocontent', 'sortby', 'bar', 'asc', 'limit', N, 10)
    env.assertEqual(res[0], N)
    env.assertEqual(len(res), 1)

    with env.assertResponseError():
        r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, -1)
    with env.assertResponseError():
        r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', -1, 10)
    with env.assertResponseError():
        r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 2000000)

def testPrefix(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text'))
    N = 100
    for i in range(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'constant term%d' % (random.randrange(0, 5))))
    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        res = r.execute_command(
            'ft.search', 'idx', 'constant term', 'nocontent')
        env.assertEqual([0], res)
        res = r.execute_command(
            'ft.search', 'idx', 'constant term*', 'nocontent')
        env.assertEqual(N, res[0])
        res = r.execute_command(
            'ft.search', 'idx', 'const* term*', 'nocontent')
        env.assertEqual(N, res[0])
        res = r.execute_command(
            'ft.search', 'idx', 'constant term1*', 'nocontent')
        env.assertGreater(res[0], 2)
        res = r.execute_command(
            'ft.search', 'idx', 'const* -term*', 'nocontent')
        env.assertEqual([0], res)
        res = r.execute_command(
            'ft.search', 'idx', 'constant term9*', 'nocontent')
        env.assertEqual([0], res)

def testSortBy(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text', 'sortable', 'bar', 'numeric', 'sortable'))
    N = 100
    for i in range(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello%03d world' % i, 'bar', 100 - i))
    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo')
        env.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo', 'desc')
        env.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'desc')
        env.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'asc')
        env.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)

        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withscores', 'limit', '2', '5')
        env.assertEqual(
            [100L, 'doc2', '1', 'doc3', '1', 'doc4', '1', 'doc5', '1', 'doc6', '1'], res)

        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertListEqual(
            [100L, 'doc0', '#100', 'doc1', '#99', 'doc2', '#98', 'doc3', '#97', 'doc4', '#96'], res)
        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'foo', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertListEqual([100L, 'doc99', '$hello099 world', 'doc98', '$hello098 world', 'doc97', '$hello097 world', 'doc96',
                              '$hello096 world', 'doc95', '$hello095 world'], res)

def testSortByWithoutSortable(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'baz', 'text', 'sortable'))
    N = 100
    for i in range(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello%03d world' % i, 'bar', 100 - i))
    for _ in r.retry_with_rdb_reload():
        waitForIndex(r, 'idx')

        # test text
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo')
        env.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo', 'desc')
        env.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'foo', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertListEqual([100L, 'doc99', '$hello099 world', 'doc98', '$hello098 world', 'doc97', '$hello097 world', 'doc96',
                              '$hello096 world', 'doc95', '$hello095 world'], res)

        # test numeric
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'desc')
        env.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                          'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'asc')
        env.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                          'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)

        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withscores', 'limit', '2', '5')
        env.assertEqual(
            [100L, 'doc2', '1', 'doc3', '1', 'doc4', '1', 'doc5', '1', 'doc6', '1'], res)

        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertListEqual(
            [100L, 'doc0', '#100', 'doc1', '#99', 'doc2', '#98', 'doc3', '#97', 'doc4', '#96'], res)

def testNot(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text'))
    N = 10
    for i in range(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'constant term%d' % (random.randrange(0, 5))))

    for i in range(5):
        inclusive = r.execute_command(
            'ft.search', 'idx', 'constant term%d' % i, 'nocontent', 'limit', 0, N)

        exclusive = r.execute_command(
            'ft.search', 'idx', 'constant -term%d' % i, 'nocontent', 'limit', 0, N)
        exclusive2 = r.execute_command(
            'ft.search', 'idx', '-(term%d)' % i, 'nocontent', 'limit', 0, N)
        exclusive3 = r.execute_command(
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
    env.assertEqual(r.execute_command(
        'ft.search', 'idx', 'constant -dasdfasdf', 'nocontent')[0], N)
    # not on env term
    env.assertEqual(r.execute_command(
        'ft.search', 'idx', 'constant -constant', 'nocontent'), [0])

    env.assertEqual(r.execute_command(
        'ft.search', 'idx', 'constant -(term0|term1|term2|term3|term4|nothing)', 'nocontent'), [0])
    # env.assertEqual(r.execute_command('ft.search', 'idx', 'constant -(term1 term2)', 'nocontent')[0], N)

def testNestedIntersection(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'a', 'text', 'b', 'text', 'c', 'text', 'd', 'text'))
    for i in range(20):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'a', 'foo', 'b', 'bar', 'c', 'baz', 'd', 'gaz'))
    res = [
        r.execute_command('ft.search', 'idx',
                          'foo bar baz gaz', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          '@a:foo @b:bar @c:baz @d:gaz', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          '@b:bar @a:foo @c:baz @d:gaz', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          '@c:baz @b:bar @a:foo @d:gaz', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          '@d:gaz @c:baz @b:bar @a:foo', 'nocontent'),
        r.execute_command(
            'ft.search', 'idx', '@a:foo (@b:bar (@c:baz @d:gaz))', 'nocontent'),
        r.execute_command(
            'ft.search', 'idx', '@c:baz (@a:foo (@b:bar (@c:baz @d:gaz)))', 'nocontent'),
        r.execute_command(
            'ft.search', 'idx', '@b:bar (@a:foo (@c:baz @d:gaz))', 'nocontent'),
        r.execute_command(
            'ft.search', 'idx', '@d:gaz (@a:foo (@c:baz @b:bar))', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          'foo (bar baz gaz)', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          'foo (bar (baz gaz))', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          'foo (bar (foo bar) (foo bar))', 'nocontent'),
        r.execute_command('ft.search', 'idx',
                          'foo (foo (bar baz (gaz)))', 'nocontent'),
        r.execute_command('ft.search', 'idx', 'foo (foo (bar (baz (gaz (foo bar (gaz))))))', 'nocontent')]

    for i, r in enumerate(res):
        # print i, res[0], r
        env.assertListEqual(res[0], r)

def testInKeys(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text'))

    for i in range(200):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello world'))

    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        for keys in (
            ['doc%d' % i for i in range(10)], ['doc%d' % i for i in range(0, 30, 2)], [
                'doc%d' % i for i in range(99, 0, -5)]
        ):
            res = r.execute_command(
                'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', len(keys), *keys)
            env.assertEqual(len(keys), res[0])
            env.assertTrue(all((k in res for k in keys)))

        env.assertEqual(0, r.execute_command(
            'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', 3, 'foo', 'bar', 'baz')[0])

    with env.assertResponseError():
        env.cmd('ft.search', 'idx', 'hello', 'INKEYS', 99)
    with env.assertResponseError():
        env.cmd('ft.search', 'idx', 'hello', 'INKEYS', -1)
    with env.assertResponseError():
        env.cmd('ft.search', 'idx', 'hello', 'inkeys', 4, 'foo')

def testSlopInOrder(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                    'title', 't1 t2'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1, 'fields',
                                    'title', 't1 t3 t2'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc3', 1, 'fields',
                                    'title', 't1 t3 t4 t2'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc4', 1, 'fields',
                                    'title', 't1 t3 t4 t5 t2'))

    res = r.execute_command(
        'ft.search', 'idx', 't1|t4 t3|t2', 'slop', '0', 'inorder', 'nocontent')
    env.assertEqual({'doc3', 'doc4', 'doc2', 'doc1'}, set(res[1:]))
    res = r.execute_command(
        'ft.search', 'idx', 't2 t1', 'slop', '0', 'nocontent')
    env.assertEqual(1, res[0])
    env.assertEqual('doc1', res[1])
    env.assertEqual(0, r.execute_command(
        'ft.search', 'idx', 't2 t1', 'slop', '0', 'inorder')[0])
    env.assertEqual(1, r.execute_command(
        'ft.search', 'idx', 't1 t2', 'slop', '0', 'inorder')[0])

    env.assertEqual(2, r.execute_command(
        'ft.search', 'idx', 't1 t2', 'slop', '1', 'inorder')[0])
    env.assertEqual(3, r.execute_command(
        'ft.search', 'idx', 't1 t2', 'slop', '2', 'inorder')[0])
    env.assertEqual(4, r.execute_command(
        'ft.search', 'idx', 't1 t2', 'slop', '3', 'inorder')[0])
    env.assertEqual(4, r.execute_command(
        'ft.search', 'idx', 't1 t2', 'inorder')[0])
    env.assertEqual(0, r.execute_command(
        'ft.search', 'idx', 't t1', 'inorder')[0])
    env.assertEqual(2, r.execute_command(
        'ft.search', 'idx', 't1 t2 t3 t4')[0])
    env.assertEqual(0, r.execute_command(
        'ft.search', 'idx', 't1 t2 t3 t4', 'inorder')[0])


def testSlopInOrderIssue1986(env):
    r = env
    # test with qsort optimization on intersect iterator
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                    'title', 't1 t2'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1, 'fields',
                                    'title', 't2 t1'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc3', 1, 'fields',
                                    'title', 't1'))

    # before fix, both queries returned `doc2`
    env.assertEqual([1L, 'doc2', ['title', 't2 t1']], r.execute_command(
        'ft.search', 'idx', 't2 t1', 'slop', '0', 'inorder'))
    env.assertEqual([1L, 'doc1', ['title', 't1 t2']], r.execute_command(
        'ft.search', 'idx', 't1 t2', 'slop', '0', 'inorder'))

def testExact(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'weight', 10.0, 'body', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                    'title', 'hello world',
                                    'body', 'lorem ist ipsum'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'hello another world',
                                    'body', 'lorem ist ipsum lorem lorem'))

    res = r.execute_command(
        'ft.search', 'idx', '"hello world"', 'verbatim')
    env.assertEqual(3, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc1", res[1])

    res = r.execute_command(
        'ft.search', 'idx', "hello \"another world\"", 'verbatim')
    env.assertEqual(3, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])


def testGeoErrors(env):
    env.expect('flushall')
    env.expect('ft.create idx ON HASH schema name text location geo').equal('OK')
    env.expect('ft.add idx hotel 1.0 fields name hill location -0.1757,51.5156').equal('OK')
    env.expect('ft.search idx hilton geofilter location -0.1757 51.5156 1 km').equal([0L])

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
    r = env
    gsearch = lambda query, lon, lat, dist, unit='km': r.execute_command(
        'ft.search', 'idx', query, 'geofilter', 'location', lon, lat, dist, unit, 'LIMIT', 0, 20)

    gsearch_inline = lambda query, lon, lat, dist, unit='km': r.execute_command(
        'ft.search', 'idx', '{} @location:[{} {} {} {}]'.format(query,  lon, lat, dist, unit), 'LIMIT', 0, 20)

    env.assertOk(r.execute_command('ft.create', 'idx', 'ON', 'HASH',
                                    'schema', 'name', 'text', 'location', 'geo'))

    for i, hotel in enumerate(hotels):
        env.assertOk(r.execute_command('ft.add', 'idx', 'hotel{}'.format(i), 1.0, 'fields', 'name',
                                        hotel[0], 'location', '{},{}'.format(hotel[2], hotel[1])))

    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        res = r.execute_command('ft.search', 'idx', 'hilton')
        env.assertEqual(len(hotels), res[0])

        res = gsearch('hilton', "-0.1757", "51.5156", '1')
        env.assertEqual(3, res[0])
        env.assertIn('hotel2', res)
        env.assertIn('hotel21', res)
        env.assertIn('hotel79', res)
        res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '1')
        env.assertListEqual(sorted(res), sorted(res2))

        res = gsearch('hilton', "-0.1757", "51.5156", '10')
        env.assertEqual(14, res[0])

        res2 = gsearch('hilton', "-0.1757", "51.5156", '10000', 'm')
        env.assertListEqual(sorted(res), sorted(res2))
        res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '10')
        env.assertListEqual(sorted(res), sorted(res2))

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'm')
        env.assertEqual(1, res[0])
        env.assertEqual('hotel94', res[1])
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '10', 'm')
        env.assertListEqual(sorted(res), sorted(res2))

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'km')
        env.assertEqual(5, res[0])
        env.assertIn('hotel94', res)
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '10', 'km')
        env.assertEqual(5, res2[0])
        env.assertListEqual(sorted(res), sorted(res2))

        res = gsearch('heathrow', -0.44155, 51.45865, '5', 'km')
        env.assertEqual(3, res[0])
        env.assertIn('hotel94', res)
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '5', 'km')
        env.assertListEqual(sorted(res), sorted(res2))

def testTagErrors(env):
    env.expect("ft.create", "test", 'ON', 'HASH',
                "SCHEMA",  "tags", "TAG").equal('OK')
    env.expect("ft.add", "test", "1", "1", "FIELDS", "tags", "alberta").equal('OK')
    env.expect("ft.add", "test", "2", "1", "FIELDS", "tags", "ontario. alberta").equal('OK')

def testGeoDeletion(env):
    if env.is_cluster():
        raise unittest.SkipTest()
        # Can't properly test if deleted on cluster

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
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'weight', 10.0, 'body', 'text', 'weight', 1.0))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                    'title', 'hello world',
                                    'body', 'lorem ipsum'))

    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'hello world lorem ipsum',
                                    'body', 'hello world'))

    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "title", "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])
    env.assertEqual("doc2", res[1])
    env.assertEqual("doc1", res[2])

    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "body", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

    res = r.execute_command(
        'ft.search', 'idx', 'hello', 'verbatim', "infields", 1, "body", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

    res = r.execute_command(
        'ft.search', 'idx',  '\"hello world\"', 'verbatim', "infields", 1, "body", "nocontent")

    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

    res = r.execute_command(
        'ft.search', 'idx', '\"lorem ipsum\"', 'verbatim', "infields", 1, "body", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc1", res[1])

    res = r.execute_command(
        'ft.search', 'idx', 'lorem ipsum', "infields", 2, "body", "title", "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])
    env.assertEqual("doc2", res[1])
    env.assertEqual("doc1", res[2])

def testScorerSelection(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text'))

    # this is the default scorer
    res = r.execute_command(
        'ft.search', 'idx', 'foo', 'scorer', 'TFIDF')
    env.assertEqual(res, [0])
    with env.assertResponseError():
        res = r.execute_command(
            'ft.search', 'idx', 'foo', 'scorer', 'NOSUCHSCORER')

def testFieldSelectors(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc',
        'schema', 'TiTle', 'text', 'BoDy', 'text', "יוניקוד", 'text', 'field.with,punct', 'text'))
    #todo: document as breaking change, ft.add fields name are not case insentive
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                    'TiTle', 'hello world', 'BoDy', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 0.5, 'fields',
                                    'BoDy', 'hello world', 'TiTle', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt'))

    res = r.execute_command(
        'ft.search', 'idx', '@TiTle:hello world', 'nocontent')
    env.assertEqual(res, [1, 'doc1'])
    res = r.execute_command(
        'ft.search', 'idx', '@BoDy:hello world', 'nocontent')
    env.assertEqual(res, [1, 'doc2'])

    res = r.execute_command(
        'ft.search', 'idx', '@BoDy:hello @TiTle:world', 'nocontent')
    env.assertEqual(res, [0])

    res = r.execute_command(
        'ft.search', 'idx', '@BoDy:hello world @TiTle:world', 'nocontent')
    env.assertEqual(res, [0])
    res = r.execute_command(
        'ft.search', 'idx', '@BoDy:(hello|foo) @TiTle:(world|bar)', 'nocontent')
    env.assertEqual(sorted(res), sorted([2, 'doc1', 'doc2']))

    res = r.execute_command(
        'ft.search', 'idx', '@BoDy:(hello|foo world|bar)', 'nocontent')
    env.assertEqual(sorted(res), sorted([2, 'doc1', 'doc2']))

    res = r.execute_command(
        'ft.search', 'idx', '@BoDy|TiTle:(hello world)', 'nocontent')
    env.assertEqual(sorted(res), sorted([2, 'doc1', 'doc2']))

    res = r.execute_command(
        'ft.search', 'idx', '@יוניקוד:(unicode)', 'nocontent')
    env.assertEqual(sorted(res), sorted([2, 'doc1', 'doc2']))

    res = r.execute_command(
        'ft.search', 'idx', '@field\\.with\\,punct:(punt)', 'nocontent')
    env.assertEqual(sorted(res), sorted([2, 'doc1', 'doc2']))

def testStemming(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                    'title', 'hello kitty'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'hello kitties'))

    res = r.execute_command(
        'ft.search', 'idx', 'hello kitty', "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])

    res = r.execute_command(
        'ft.search', 'idx', 'hello kitty', "nocontent", "verbatim")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

    # test for unknown language
    with env.assertResponseError():
        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty', "nocontent", "language", "foofoofian")

def testExpander(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                    'title', 'hello kitty'))
    res = r.execute_command(
        'ft.search', 'idx', 'kitties',
        "nocontent",
        "expander", "SBSTEM"
        )
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

    res = r.execute_command(
        'ft.search', 'idx', 'kitties', "nocontent", "expander", "noexpander")
    env.assertEqual(1, len(res))
    env.assertEqual(0, res[0])

    res = r.execute_command(
        'ft.search', 'idx', 'kitti', "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

    res = r.execute_command(
        'ft.search', 'idx', 'kitti', "nocontent", 'verbatim')
    env.assertEqual(1, len(res))
    env.assertEqual(0, res[0])

    # Calling a stem directly works even with VERBATIM.
    # You need to use the + prefix escaped
    res = r.execute_command(
        'ft.search', 'idx', '\\+kitti', "nocontent", 'verbatim')
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])

def testNumericRange(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'score', 'numeric', 'price', 'numeric'))

    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 5).error().contains("FILTER requires 3 arguments")
    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 5, 'inf').error().contains("Bad upper range: inf")
    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 'inf', 5).error().contains("Bad lower range: inf")

    for i in xrange(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'fields',
                                        'title', 'hello kitty', 'score', i, 'price', 100 + 10 * i))

    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent",
                                "filter", "score", 0, 100)

        env.assertEqual(11, len(res))
        env.assertEqual(100, res[0])

        res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent",
                                "filter", "score", 0, 50)
        env.assertEqual(51, res[0])

        res = r.execute_command('ft.search', 'idx', 'hello kitty', 'verbatim', "nocontent", "limit", 0, 100,
                                "filter", "score", "(0", "(50")

        env.assertEqual(49, res[0])
        res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent",
                                "filter", "score", "-inf", "+inf")
        env.assertEqual(100, res[0])

        # test multi filters
        scrange = (19, 90)
        prrange = (290, 385)
        res = r.execute_command('ft.search', 'idx', 'hello kitty',
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

        res = r.execute_command('ft.search', 'idx', 'hello kitty',
                                "filter", "score", "19", "90",
                                "filter", "price", "90", "185")

        env.assertEqual(0, res[0])

        # Test numeric ranges as part of query syntax
        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty @score:[0 100]', "nocontent")

        env.assertEqual(11, len(res))
        env.assertEqual(100, res[0])

        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty  @score:[0 50]', "nocontent")
        env.assertEqual(51, res[0])
        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty @score:[(0 (50]', 'verbatim', "nocontent")
        env.assertEqual(49, res[0])
        res = r.execute_command(
            'ft.search', 'idx', '@score:[(0 (50]', 'verbatim', "nocontent")
        env.assertEqual(49, res[0])
        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty -@score:[(0 (50]', 'verbatim', "nocontent")
        env.assertEqual(51, res[0])
        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty @score:[-inf +inf]', "nocontent")
        env.assertEqual(100, res[0])

def testPayload(env):
    r = env
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'PAYLOAD_FIELD', '__payload', 'schema', 'f', 'text').ok()
    for i in range(10):
        r.expect('ft.add', 'idx', '%d' % i, 1.0,
                 'payload', 'payload %d' % i,
                 'fields', 'f', 'hello world').ok()

    for x in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        res = r.execute_command('ft.search', 'idx', 'hello world')
        r.assertEqual(21, len(res))

        res = r.execute_command('ft.search', 'idx', 'hello world', 'withpayloads')
        r.assertEqual(31, len(res))
        r.assertEqual(10, res[0])
        for i in range(1, 30, 3):
            r.assertEqual(res[i + 1], 'payload %s' % res[i])

def testGarbageCollector(env):
    env.skipOnCluster()
    if env.moduleArgs is not None and 'GC_POLICY FORK' in env.moduleArgs:
        # this test is not relevent for fork gc cause its not cleaning the last block
        raise unittest.SkipTest()
    N = 100
    r = env
    r.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'foo', 'text').ok()
    waitForIndex(r, 'idx')
    for i in range(N):
        r.expect('ft.add', 'idx', 'doc%d' % i, 1.0,
                 'fields', 'foo', ' '.join(('term%d' % random.randrange(0, 10) for i in range(10)))).ok()

    def get_stats(r):
        res = r.execute_command('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        gc_stats = {d['gc_stats'][x]: float(
            d['gc_stats'][x + 1]) for x in range(0, len(d['gc_stats']), 2)}
        d['gc_stats'] = gc_stats
        return d

    stats = get_stats(r)
    if 'current_hz' in stats['gc_stats']:
        env.assertGreater(stats['gc_stats']['current_hz'], 8)
    env.assertEqual(0, stats['gc_stats']['bytes_collected'])
    env.assertGreater(int(stats['num_records']), 0)

    initialIndexSize = float(stats['inverted_sz_mb']) * 1024 * 1024
    for i in range(N):
        r.expect('ft.del', 'idx', 'doc%d' % i).equal(1)

    for _ in range(100):
        # gc is random so we need to do it long enough times for it to work
        forceInvokeGC(env, 'idx')

    stats = get_stats(r)

    env.assertEqual(0, int(stats['num_docs']))
    env.assertEqual(0, int(stats['num_records']))
    if not env.is_cluster():
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

        res = r.execute_command('ft.search', 'idx', 'term%d' % i)
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
    for x in env.retry_with_reload():
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
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'ON', 'HASH', 'NOFIELDS', 'schema', 'title', 'text'))
    N = 50
    for i in xrange(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'replace', 'fields',
                                        'title', 'hello term%d' % i))
    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')

        res = r.execute_command('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}

        env.assertEqual(d['index_name'], 'idx')
        env.assertEqual(d['index_options'], ['NOFIELDS'])
        env.assertListEqual(
            d['attributes'], [['identifier', 'title', 'attribute', 'title', 'type', 'TEXT', 'WEIGHT', '1']])

        if not env.is_cluster():
            env.assertEquals(int(d['num_docs']), N)
            env.assertEquals(int(d['num_terms']), N + 1)
            env.assertEquals(int(d['max_doc_id']), N)
            env.assertEquals(int(d['records_per_doc_avg']), 2)
            env.assertEquals(int(d['num_records']), N * 2)

            env.assertGreater(float(d['offset_vectors_sz_mb']), 0)
            env.assertGreater(float(d['key_table_size_mb']), 0)
            env.assertGreater(float(d['inverted_sz_mb']), 0)
            env.assertGreater(float(d['bytes_per_record_avg']), 0)
            env.assertGreater(float(d['doc_table_size_mb']), 0)

    for x in range(1, 5):
        for combo in combinations(('NOOFFSETS', 'NOFREQS', 'NOFIELDS', ''), x):
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
                env.assertListEqual([], opts)

            for option in filter(None, combo):
                env.assertTrue(option in opts)

def testNoStem(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'schema', 'body', 'text', 'name', 'text', 'nostem')
    if not env.isCluster():
        # todo: change it to be more generic to pass on is_cluster
        res = env.cmd('ft.info', 'idx')
        env.assertEqual(res[7][1][8], 'NOSTEM')
    for _ in env.retry_with_reload():
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
    env.cmd('ft.create', 'ix', 'ON', 'HASH', 'schema', 'txt',
             'text', 'num', 'numeric', 'sortable')
    env.cmd('ft.add', 'ix', 'doc1', 1.0, 'fields', 'txt', 'foo')
    env.cmd('ft.search', 'ix', 'foo', 'sortby', 'num')

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
    for _ in env.reloading_iterator():
        waitForIndex(env, 'idx')
        exp = [2L, 'Hello\x00World', ['txt', 'Bin match'], 'Hello', ['txt', 'NoBin match']]
        res = env.cmd('ft.search', 'idx', 'match')
        for r in res:
            env.assertIn(r, exp)

def testNonDefaultDb(env):
    if env.is_cluster():
        raise unittest.SkipTest()

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
    env.expect('FT.SEARCH idx *').equal([1L, 'doc', ['txt', 'baz']])

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
    res = env.cmd('ft.search', 'idx', 'mark', 'WITHSORTKEYS', "SORTBY",
                   "firstName", "ASC", "limit", 0, 100)
    # commented because we don't filter out exclusive sortby fields
    # env.assertEqual([1L, 'doc1', None, ['lastName', 'mark']], res)

def testLuaAndMulti(env):
    env.skip() # addhash isn't supported
    if env.is_cluster():
        raise unittest.SkipTest()
    # Ensure we can work in Lua and Multi environments without crashing
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'f1', 'text', 'n1', 'numeric')
    env.cmd('HMSET', 'hashDoc', 'f1', 'v1', 'n1', 4)
    env.cmd('HMSET', 'hashDoc2', 'f1', 'v1', 'n1', 5)

    r = env.getConnection()

    r.eval("return redis.call('ft.add', 'idx', 'doc1', 1.0, 'fields', 'f1', 'bar')", "0")
    r.eval("return redis.call('ft.addhash', 'idx', 'hashDoc', 1.0)", 0)

    # Try in a pipeline:
    with r.pipeline(transaction=True) as pl:
        pl.execute_command('ft.add', 'idx', 'doc2',
                           1.0, 'fields', 'f1', 'v3')
        pl.execute_command('ft.add', 'idx', 'doc3',
                           1.0, 'fields', 'f1', 'v4')
        pl.execute_command('ft.addhash', 'idx', 'hashdoc2', 1.0)
    pl.execute()

def testLanguageField(env):
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'language', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc1', 1.0,
             'FIELDS', 'language', 'gibberish')
    res = env.cmd('FT.SEARCH', 'idx', 'gibberish')
    env.assertEqual([1L, 'doc1', ['language', 'gibberish']], res)
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


def assertAggrowsEqual(env, exp, got):
    env.assertEqual(exp[0], got[0])
    env.assertEqual(len(exp), len(got))

    # and now, it's just free form:
    exp = sorted(to_dict(x) for x in exp[1:])
    got = sorted(to_dict(x) for x in got[1:])
    env.assertEqual(exp, got)

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
    # for _ in env.retry_with_reload():
    res = env.cmd('FT.SEARCH', 'idx', 'world')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2L, 'doc2', ['f1', 'hello', 'f2', 'world'], 'doc1', ['f1', 'hello', 'f2', 'world']]))
    # env.assertEqual([1, 'doc2', ['f1', 'hello', 'f2', 'world']], ret)

    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f3', 'TEXT', 'SORTABLE')
    for x in range(10):
        env.cmd('FT.ADD', 'idx', 'doc{}'.format(x + 3), 1.0,
                 'FIELDS', 'f1', 'hello', 'f3', 'val{}'.format(x))

    for _ in env.retry_with_reload():
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
    for _ in env.retry_with_reload():
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
    for _ in env.retry_with_reload():
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
    for _ in env.retry_with_reload():
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
    env.dump_and_reload()
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
# r.execute_command(command)
# r.execute_command('save')
# // reload from ...
# r.execute_command('FT.ADD idx doc1 1.0 FIELDS t0 1')
def testIssue417(env):
    command = ['ft.create', 'idx', 'ON', 'HASH', 'schema']
    for x in range(255):
        command += ['t{}'.format(x), 'numeric', 'sortable']
    command = command[:-1]
    env.cmd(*command)
    for _ in env.reloading_iterator():
        waitForIndex(env, 'idx')
        try:
            env.execute_command('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 't0', '1')
        except redis.ResponseError as e:
            env.assertTrue('already' in e.message.lower())

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

def testTimeout(env):
    env.skipOnCluster()
    num_range = 1000
    env.cmd('ft.config', 'set', 'timeout', '1')
    env.cmd('ft.config', 'set', 'maxprefixexpansions', num_range)
    env.cmd('ft.create', 'myIdx', 'schema', 't', 'TEXT')
    for i in range(num_range):
        env.expect('HSET', 'doc%d'%i, 't', 'aa' + str(i))

    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', '0').noEqual([num_range])

    env.expect('ft.config', 'set', 'on_timeout', 'fail').ok()
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', '0') \
                .contains('Timeout limit was reached')

    res = env.cmd('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'timeout', 1000)
    env.assertEqual(res[0], num_range)

    # test erroneous params
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'timeout').error()
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'timeout', -1).error()
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'timeout', 'STR').error()

    # test cursor
    res = env.cmd('FT.AGGREGATE', 'myIdx', 'aa*', 'WITHCURSOR', 'count', 50, 'timeout', 500)
    l = len(res[0]) - 1 # do not count the number of results (the first element in the results)
    cursor = res[1]

    time.sleep(0.01)
    while cursor != 0:
        r, cursor = env.cmd('FT.CURSOR', 'READ', 'myIdx', str(cursor))
        l += (len(r) - 1)
    env.assertEqual(l, 1000)

    # restore old configuration
    env.cmd('ft.config', 'set', 'timeout', '500')
    env.cmd('ft.config', 'set', 'maxprefixexpansions', 200)

def testAlias(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc1', 'schema', 't1', 'text')
    env.cmd('ft.create', 'idx2', 'ON', 'HASH', 'PREFIX', 1, 'doc2', 'schema', 't1', 'text')

    env.expect('ft.aliasAdd', 'myIndex').raiseError()
    env.expect('ft.aliasupdate', 'fake_alias', 'imaginary_alias', 'Too_many_args').raiseError()
    env.cmd('ft.aliasAdd', 'myIndex', 'idx')
    env.cmd('ft.add', 'myIndex', 'doc1', 1.0, 'fields', 't1', 'hello')
    r = env.cmd('ft.search', 'idx', 'hello')
    env.assertEqual([1, 'doc1', ['t1', 'hello']], r)
    r2 = env.cmd('ft.search', 'myIndex', 'hello')
    env.assertEqual(r, r2)

    # try to add the same alias again; should be an error
    env.expect('ft.aliasAdd', 'myIndex', 'idx2').raiseError()
    env.expect('ft.aliasAdd', 'alias2', 'idx').notRaiseError()
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
    env.assertEqual([1L, 'doc2', ['t1', 'hello']], r)

    # check that aliasing one alias to another returns an error. This will
    # end up being confusing
    env.expect('ft.aliasAdd', 'alias3', 'myIndex').raiseError()

    # check that deleting the alias works as expected
    env.expect('ft.aliasDel', 'myIndex').notRaiseError()
    env.expect('ft.search', 'myIndex', 'foo').raiseError()

    # create a new index and see if we can use the old name
    env.cmd('ft.create', 'idx3', 'ON', 'HASH', 'PREFIX', 1, 'doc3', 'schema', 't1', 'text')
    env.cmd('ft.add', 'idx3', 'doc3', 1.0, 'fields', 't1', 'foo')
    env.cmd('ft.aliasAdd', 'myIndex', 'idx3')
    # also, check that this works in rdb save
    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'myIndex')
        r = env.cmd('ft.search', 'myIndex', 'foo')
        env.assertEqual([1L, 'doc3', ['t1', 'foo']], r)

    # Check that we can move an alias from one index to another
    env.cmd('ft.aliasUpdate', 'myIndex', 'idx2')
    r = env.cmd('ft.search', 'myIndex', "hello")
    env.assertEqual([1L, 'doc2', ['t1', 'hello']], r)

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
    env.expect('ft.aliasdel').raiseError()
    env.expect('ft.aliasdel', 'myIndex', 'yourIndex').raiseError()
    env.expect('ft.aliasdel', 'non_existing_alias').raiseError()


def testNoCreate(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f1', 'text')
    env.expect('ft.add', 'idx', 'schema', 'f1').raiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'nocreate', 'fields', 'f1', 'hello').raiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'nocreate', 'fields', 'f1', 'hello').raiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'fields', 'f1', 'hello').notRaiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'nocreate', 'fields', 'f1', 'world').notRaiseError()

def testSpellCheck(env):
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'report', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'report', 'report content')
    rv = env.cmd('FT.SPELLCHECK', 'idx', '111111')
    env.assertEqual([['TERM', '111111', []]], rv)
    if not env.isCluster():
        rv = env.cmd('FT.SPELLCHECK', 'idx', '111111', 'FULLSCOREINFO')
        env.assertEqual([1L, ['TERM', '111111', []]], rv)

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
    assertAggrowsEqual(env, expected, res)
    for var in expected:
        env.assertIn(var, res)

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
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'a', ['uuid', 'foo', 'title', 'bar']]))

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

def testPrefixDeletedExpansions(env):
    env.skipOnCluster()

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


def testIssue736(env):
    #for new RS 2.0 ft.add does not return certian errors
    env.skip()
    # 1. create the schema, we need a tag field
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'schema', 't1', 'text', 'n2', 'numeric', 't2', 'tag')
    # 2. create a single document to initialize at least one RSAddDocumentCtx
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 't1', 'hello', 't2', 'foo, bar')
    # 3. create a second document with many filler fields to force a realloc:
    extra_fields = []
    for x in range(20):
        extra_fields += ['nidx_fld{}'.format(x), 'val{}'.format(x)]
    extra_fields += ['n2', 'not-a-number', 't2', 'random, junk']
    with env.assertResponseError():
        env.cmd('ft.add', 'idx', 'doc2', 1, 'fields', *extra_fields)

def testCriteriaTesterDeactivated():
    env = Env(moduleArgs='_MAX_RESULTS_TO_UNSORTED_MODE 1')
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 't1', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 't1', 'hello1 hey hello2')
    env.cmd('ft.add', 'idx', 'doc2', 1, 'fields', 't1', 'hello2 hey')
    env.cmd('ft.add', 'idx', 'doc3', 1, 'fields', 't1', 'hey')
    
    expected_res = sorted([2L, 'doc1', ['t1', 'hello1 hey hello2'], 'doc2', ['t1', 'hello2 hey']])
    actual_res = sorted(env.cmd('ft.search', 'idx', '(hey hello1)|(hello2 hey)'))
    env.assertEqual(expected_res, actual_res)

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

    expected = [2L, 'doc2', ['title', 'conversation the conversation - a drama about conversation, the science of conversation.'], 'doc4', ['title', 'mohsin conversation the conversation tahir']]
    res = env.cmd('FT.SEARCH', 'idx', '@title:(conversation) (@title:(conversation the conversation))=>{$inorder: true;$slop: 0}')
    env.assertEquals(len(expected), len(res))
    for v in expected:
        env.assertContains(v, res)

def testIssue_848(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test1', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test1', 'foo').equal('OK')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'test2', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc2', '1.0', 'FIELDS', 'test1', 'foo', 'test2', 'bar').equal('OK')
    env.expect('FT.SEARCH', 'idx', 'foo', 'SORTBY', 'test2', 'ASC').equal([2L, 'doc1', ['test1', 'foo'], 'doc2', ['test2', 'bar', 'test1', 'foo']])

def testMod_309(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    conn = getConnectionByEnv(env)
    for i in range(100000):
        conn.execute_command('HSET', 'doc%d'%i, 'test', 'foo')
    res = env.cmd('FT.AGGREGATE', 'idx', 'foo')
    env.assertEqual(len(res), 100001)

    # test with cursor
    env.skipOnCluster()
    res = env.cmd('FT.AGGREGATE', 'idx', 'foo', 'WITHCURSOR')
    l = len(res[0]) - 1 # do not count the number of results (the first element in the results)
    cursor = res[1]
    while cursor != 0:
        r, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', str(cursor))
        l += len(r) - 1
    env.assertEqual(l, 100000)

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
    env.expect('ft.search', 'idx', 'foo @f2:moo').equal([0L])

def testErrorWithApply(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo bar').equal('OK')
    err = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'split()')[1]
    env.assertEqual(str(err[0]), 'Invalid number of arguments for split')

def testSummerizeWithAggregateRaiseError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', 'foo2', 'SUMMARIZE', 'FIELDS', '1', 'test',
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0').error()

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
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'as', 'count').equal([2L, ['test', '2', 'count', '1'], ['test', '1', 'count', '2']])

def testApplyError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'APPLY', 'split(@test)', 'as').error()

def testLoadError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
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

def testBadFilterExpression(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'FILTER', 'blabla').error()
    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'FILTER', '@test1 > 1').error()

def testWithSortKeysOnNoneSortableValue(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'WITHSORTKEYS', 'SORTBY', 'test').equal([1L, 'doc1', '$foo', ['test', 'foo']])

def testWithWithRawIds(env):
    env.skipOnCluster() # todo: remove once fix on coordinator
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'WITHRAWIDS').equal([1L, 'doc1', 1L, ['test', 'foo']])

def testUnkownIndex(env):
    env.skipOnCluster() # todo: remove once fix on coordinator
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
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    err = env.cmd('FT.AGGREGATE', 'idx', '*', 'APPLY', 'split()', 'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'AS', 'count')[1]
    assertEqualIgnoreCluster(env, str(err[0]), 'Invalid number of arguments for split')

def testSubStrErrors(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')

    err = env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a', 'APPLY', 'substr(@a,0,4)')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test",3,-2)', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test",3,1000)', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test",-1,2)', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test")', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr(1)', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test", "test")', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test", "test", "test")', 'as', 'a')
    env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test2', 'APPLY', 'substr("test", "-1", "-1")', 'as', 'a')
    env.assertTrue(env.isUp())

def testToUpperLower(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower(@test)', 'as', 'a').equal([1L, ['test', 'foo', 'a', 'foo']])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower("FOO")', 'as', 'a').equal([1L, ['test', 'foo', 'a', 'foo']])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper(@test)', 'as', 'a').equal([1L, ['test', 'foo', 'a', 'FOO']])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper("foo")', 'as', 'a').equal([1L, ['test', 'foo', 'a', 'FOO']])

    err = env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)
    err = env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper(1)', 'as', 'a').equal([1L, ['test', 'foo', 'a', None]])
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower(1)', 'as', 'a').equal([1L, ['test', 'foo', 'a', None]])

    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)
    err = env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'upper(1,2)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)
    err = env.cmd('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'lower(1,2)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

def testMatchedTerms(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a').equal([1L, ['test', 'foo', 'a', None]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms(100)', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms(-100)', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms("test")', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])

def testStrFormatError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    err = env.cmd('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format("%s")', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format("%", "test")', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format("%b", "test")', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'format(5)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'upper(1)', 'as', 'b', 'APPLY', 'format("%s", @b)', 'as', 'a').equal([1L, ['test', 'foo', 'b', None, 'a', '(null)']])

    # working example
    env.expect('ft.aggregate', 'idx', 'foo', 'APPLY', 'format("%%s-test", "test")', 'as', 'a').equal([1L, ['a', '%s-test']])
    env.expect('ft.aggregate', 'idx', 'foo', 'APPLY', 'format("%s-test", "test")', 'as', 'a').equal([1L, ['a', 'test-test']])

def testTimeFormatError(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test1)', 'as', 'a').error()

    env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test)', 'as', 'a')

    env.assertTrue(env.isUp())

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test, 4)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt("awfawf")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(235325153152356426246246246254)', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'timefmt(@test, "%s")' % ('d' * 2048), 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'hour("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'minute("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'day("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'month("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'dayofweek("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'dayofmonth("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'dayofyear("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'year("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear("not_number")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

def testMonthOfYear(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear(@test)', 'as', 'a').equal([1L, ['test', '12234556', 'a', '4']])

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear(@test, 112)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear("bad")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

def testParseTime(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TAG')
    conn.execute_command('HSET', 'doc1', 'test', '20210401')

    # check for errors
    err = conn.execute_command('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = conn.execute_command('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime(11)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = conn.execute_command('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime(11,22)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    # valid test
    res = conn.execute_command('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'parsetime(@test, "%Y%m%d")', 'as', 'a')
    assertEqualIgnoreCluster(env, res, [1L, ['test', '20210401', 'a', '1617235200']])

def testMathFunctions(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'exp(@test)', 'as', 'a').equal([1L, ['test', '12234556', 'a', 'inf']])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'ceil(@test)', 'as', 'a').equal([1L, ['test', '12234556', 'a', '12234556']])

def testErrorOnOpperation(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', '1 + split()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'split() + 1', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', '"bad" + "bad"', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'split("bad" + "bad")', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', '!(split("bad" + "bad"))', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'APPLY', '!@test', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)


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
    env.assertEqual([1L, 'doc1', ['n1', '42']], rv)


def testIssue1074(env):
    # Ensure that sortable fields are returned in their string form from the
    # document
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'schema', 't1', 'text', 'n1', 'numeric', 'sortable')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 't1', 'hello', 'n1', 1581011976800)
    rv = env.cmd('ft.search', 'idx', '*', 'sortby', 'n1')
    env.assertEqual([1L, 'doc1', ['n1', '1581011976800', 't1', 'hello']], rv)

def testIssue1085(env):
    env.skipOnCluster()
    env.cmd('FT.CREATE issue1085 ON HASH SCHEMA foo TEXT SORTABLE bar NUMERIC SORTABLE')
    for i in range(1, 10):
        env.cmd('FT.ADD issue1085 document_%d 1 REPLACE FIELDS foo foo%d bar %d' % (i, i, i))
    res = env.cmd('FT.SEARCH', 'issue1085', '@bar:[8 8]')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'document_8', ['foo', 'foo8', 'bar', '8']]))

    for i in range(1, 10):
        env.cmd('FT.ADD issue1085 document_8 1 REPLACE FIELDS foo foo8 bar 8')

    forceInvokeGC(env, 'issue1085')

    res = env.cmd('FT.SEARCH', 'issue1085', '@bar:[8 8]')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'document_8', ['foo', 'foo8', 'bar', '8']]))


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


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
    env.assertEqual(u'अँगरेजी अँगरेजों अँगरेज़', unicode(res1['body'], 'utf-8'))

def testMOD507(env):
    env.skipOnCluster()
    env.expect('ft.create idx ON HASH SCHEMA t1 TEXT').ok()

    for i in range(50):
        env.expect('ft.add idx doc-%d 1.0 FIELDS t1 foo' % i).ok()

    for i in range(50):
        env.expect('del doc-%d' % i).equal(1)

    res = env.cmd('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'SUMMARIZE', 'FRAGS', '1', 'LEN', '25', 'HIGHLIGHT', 'TAGS', "<span style='background-color:yellow'>", "</span>")

    # from redisearch 2.0, docs are removed from index when `DEL` is called
    env.assertEqual(len(res), 1)

def testUnseportedSortableTypeErrorOnTags(env):
    env.skipOnCluster()
    env.expect('FT.CREATE idx ON HASH SCHEMA f1 TEXT SORTABLE f2 NUMERIC SORTABLE NOINDEX f3 TAG SORTABLE NOINDEX f4 TEXT SORTABLE NOINDEX').ok()
    env.expect('FT.ADD idx doc1 1.0 FIELDS f1 foo1 f2 1 f3 foo1 f4 foo1').ok()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL FIELDS f2 2 f3 foo2 f4 foo2').ok()
    res = env.cmd('HGETALL doc1')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(['f1', 'foo1', 'f2', '2', 'f3', 'foo2', 'f4', 'foo2', '__score', '1.0']))
    res = env.cmd('FT.SEARCH idx *')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'doc1', ['f1', 'foo1', 'f2', '2', 'f3', 'foo2', 'f4', 'foo2']]))


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

    env.expect('FT.AGGREGATE idx foo GROUPBY 1 @txt1 REDUCE FIRST_VALUE 1 @txt2 as test').equal([1L, ['txt1', 'foo', 'test', None]])

def testIssue1184(env):
    env.skipOnCluster()

    field_types = ['TEXT', 'NUMERIC', 'TAG']
    env.assertOk(env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0))
    for ft in field_types:
        env.assertOk(env.execute_command('FT.CREATE idx ON HASH SCHEMA  field ' + ft))

        res = env.execute_command('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        env.assertEqual(d['inverted_sz_mb'], '0')
        env.assertEqual(d['num_records'], '0')


        value = '42'
        env.assertOk(env.execute_command('FT.ADD idx doc0 1 FIELD field ' + value))
        doc = env.cmd('FT.SEARCH idx *')
        env.assertEqual(doc, [1L, 'doc0', ['field', value]])

        res = env.execute_command('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        env.assertGreater(d['inverted_sz_mb'], '0')
        env.assertEqual(d['num_records'], '1')

        env.assertEqual(env.execute_command('FT.DEL idx doc0'), 1)

        forceInvokeGC(env, 'idx')

        res = env.execute_command('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        env.assertEqual(d['inverted_sz_mb'], '0')
        env.assertEqual(d['num_records'], '0')

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
    env.expect('FT.SEARCH', 'idx', '@n:[1.1432E3 inf]').equal([1L, 'doc1', ['n', '1.0321e5']])
    env.expect('FT.SEARCH', 'idx', '@n:[-1.12E-3 1.12E-1]').equal([1L, 'doc3', ['n', '0.0011']])
    res = [3L, 'doc1', ['n', '1.0321e5'], 'doc2', ['n', '101.11'], 'doc3', ['n', '0.0011']]
    env.expect('FT.SEARCH', 'idx', '@n:[-inf inf]').equal(res)

    env.expect('FT.ADD idx doc3 1 REPLACE PARTIAL IF @n>42e3 FIELDS n 100').equal('NOADD')
    env.expect('FT.ADD idx doc3 1 REPLACE PARTIAL IF @n<42e3 FIELDS n 100').ok()
    # print env.cmd('FT.SEARCH', 'idx', '@n:[-inf inf]')

def testFieldsCaseSensetive(env):
    # this test will not pass on coordinator coorently as if one shard return empty results coordinator
    # will not reflect the errors
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC f TEXT t TAG g GEO')

    # make sure text fields are case sesitive
    conn.execute_command('hset', 'doc1', 'F', 'test')
    conn.execute_command('hset', 'doc2', 'f', 'test')
    env.expect('ft.search idx @f:test').equal([1L, 'doc2', ['f', 'test']])
    env.expect('ft.search idx @F:test').equal([0])

    # make sure numeric fields are case sesitive
    conn.execute_command('hset', 'doc3', 'N', '1.0')
    conn.execute_command('hset', 'doc4', 'n', '1.0')
    env.expect('ft.search', 'idx', '@n:[0 2]').equal([1L, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@N:[0 2]').equal([0])

    # make sure tag fields are case sesitive
    conn.execute_command('hset', 'doc5', 'T', 'tag')
    conn.execute_command('hset', 'doc6', 't', 'tag')
    env.expect('ft.search', 'idx', '@t:{tag}').equal([1L, 'doc6', ['t', 'tag']])
    env.expect('ft.search', 'idx', '@T:{tag}').equal([0])

    # make sure geo fields are case sesitive
    conn.execute_command('hset', 'doc8', 'G', '-113.524,53.5244')
    conn.execute_command('hset', 'doc9', 'g', '-113.524,53.5244')
    env.expect('ft.search', 'idx', '@g:[-113.52 53.52 20 mi]').equal([1L, 'doc9', ['g', '-113.524,53.5244']])
    env.expect('ft.search', 'idx', '@G:[-113.52 53.52 20 mi]').equal([0])

    # make sure search filter are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'n', 0, 2).equal([1L, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'N', 0, 2).equal([0])

    # make sure RETURN are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'n').equal([1L, 'doc4', ['n', '1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'N').equal([1L, 'doc4', []])

    # make sure SORTBY are case sensitive
    conn.execute_command('hset', 'doc7', 'n', '1.1')
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'n').equal([2L, 'doc4', ['n', '1.0'], 'doc7', ['n', '1.1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'N').error().contains('not loaded nor in schema')

    # make sure aggregation load are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n').equal([1L, ['n', '1'], ['n', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@N').equal([1L, [], []])

    # make sure aggregation apply are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'apply', '@n', 'as', 'r').equal([1L, ['n', '1', 'r', '1'], ['n', '1.1', 'r', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'apply', '@N', 'as', 'r').error().contains('not loaded in pipeline')

    # make sure aggregation filter are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'filter', '@n==1.0').equal([1L, ['n', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'filter', '@N==1.0').error().contains('not loaded in pipeline')

    # make sure aggregation groupby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'groupby', '1', '@n', 'reduce', 'count', 0, 'as', 'count').equal([2L, ['n', '1', 'count', '1'], ['n', '1.1', 'count', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'groupby', '1', '@N', 'reduce', 'count', 0, 'as', 'count').error().contains('No such property')

    # make sure aggregation sortby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'sortby', '1', '@n').equal([2L, ['n', '1'], ['n', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'LOAD', '1', '@n', 'sortby', '1', '@N').error().contains('not loaded')

def testSortedFieldsCaseSensetive(env):
    # this test will not pass on coordinator coorently as if one shard return empty results coordinator
    # will not reflect the errors
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC SORTABLE f TEXT SORTABLE t TAG SORTABLE g GEO SORTABLE')

    # make sure text fields are case sesitive
    conn.execute_command('hset', 'doc1', 'F', 'test')
    conn.execute_command('hset', 'doc2', 'f', 'test')
    env.expect('ft.search idx @f:test').equal([1L, 'doc2', ['f', 'test']])
    env.expect('ft.search idx @F:test').equal([0])

    # make sure numeric fields are case sesitive
    conn.execute_command('hset', 'doc3', 'N', '1.0')
    conn.execute_command('hset', 'doc4', 'n', '1.0')
    env.expect('ft.search', 'idx', '@n:[0 2]').equal([1L, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@N:[0 2]').equal([0])

    # make sure tag fields are case sesitive
    conn.execute_command('hset', 'doc5', 'T', 'tag')
    conn.execute_command('hset', 'doc6', 't', 'tag')
    env.expect('ft.search', 'idx', '@t:{tag}').equal([1L, 'doc6', ['t', 'tag']])
    env.expect('ft.search', 'idx', '@T:{tag}').equal([0])

    # make sure geo fields are case sesitive
    conn.execute_command('hset', 'doc8', 'G', '-113.524,53.5244')
    conn.execute_command('hset', 'doc9', 'g', '-113.524,53.5244')
    env.expect('ft.search', 'idx', '@g:[-113.52 53.52 20 mi]').equal([1L, 'doc9', ['g', '-113.524,53.5244']])
    env.expect('ft.search', 'idx', '@G:[-113.52 53.52 20 mi]').equal([0])

    # make sure search filter are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'n', 0, 2).equal([1L, 'doc4', ['n', '1.0']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'FILTER', 'N', 0, 2).equal([0])

    # make sure RETURN are case sensitive
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'n').equal([1L, 'doc4', ['n', '1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '1', 'N').equal([1L, 'doc4', []])

    # make sure SORTBY are case sensitive
    conn.execute_command('hset', 'doc7', 'n', '1.1')
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'n').equal([2L, 'doc4', ['n', '1.0'], 'doc7', ['n', '1.1']])
    env.expect('ft.search', 'idx', '@n:[0 2]', 'SORTBY', 'N').error().contains('not loaded nor in schema')

    # make sure aggregation apply are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'apply', '@n', 'as', 'r').equal([1L, ['n', '1', 'r', '1'], ['n', '1.1', 'r', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'apply', '@N', 'as', 'r').error().contains('not loaded in pipeline')

    # make sure aggregation filter are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'filter', '@n==1.0').equal([1L, ['n', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'filter', '@N==1.0').error().contains('not loaded in pipeline')

    # make sure aggregation groupby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'groupby', '1', '@n', 'reduce', 'count', 0, 'as', 'count').equal([2L, ['n', '1', 'count', '1'], ['n', '1.1', 'count', '1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'groupby', '1', '@N', 'reduce', 'count', 0, 'as', 'count').error().contains('No such property')

    # make sure aggregation sortby are case sensitive
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'sortby', '1', '@n').equal([2L, ['n', '1'], ['n', '1.1']])
    env.expect('ft.aggregate', 'idx', '@n:[0 2]', 'sortby', '1', '@N').error().contains('not loaded')

def testScoreLangPayloadAreReturnedIfCaseNotMatchToSpecialFields(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC SORTABLE')
    conn.execute_command('hset', 'doc1', 'n', '1.0', '__Language', 'eng', '__Score', '1', '__Payload', '10')
    res = env.cmd('ft.search', 'idx', '@n:[0 2]')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'doc1', ['n', '1.0', '__Language', 'eng', '__Score', '1', '__Payload', '10']]))

def testReturnSameFieldDifferentCase(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE idx ON HASH SCHEMA n NUMERIC SORTABLE N NUMERIC SORTABLE')
    conn.execute_command('hset', 'doc1', 'n', '1.0', 'N', '2.0')
    env.expect('ft.search', 'idx', '@n:[0 2]', 'RETURN', '2', 'n', 'N').equal([1L, 'doc1', ['n', '1', 'N', '2']])

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
    env.expect('FT.SEARCH idx *').equal([2L, 'doc2', ['t', 'foo'], 'doc4', ['t', 'foo']])

def testRED47209(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA t TEXT').ok()
    conn.execute_command('hset', 'doc1', 't', 'foo')
    if env.isCluster():
        # on cluster we have WITHSCORES set unconditionally for FT.SEARCH
        res = [1L, 'doc1', ['t', 'foo']]
    else:
        res = [1L, 'doc1', None, ['t', 'foo']]
    env.expect('FT.SEARCH idx foo WITHSORTKEYS LIMIT 0 1').equal(res)

def testInvertedIndexWasEntirelyDeletedDuringCursor():
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 1')

    env.skipOnCluster()

    env.expect('FT.CREATE idx SCHEMA t TEXT').ok()
    env.expect('HSET doc1 t foo').equal(1)
    env.expect('HSET doc2 t foo').equal(1)

    res, cursor = env.cmd('FT.AGGREGATE idx foo WITHCURSOR COUNT 1')
    env.assertEqual(res, [1L, []])

    # delete both documents and run the GC to clean 'foo' inverted index
    env.expect('DEL doc1').equal(1)
    env.expect('DEL doc2').equal(1)

    forceInvokeGC(env, 'idx')

    # make sure the inverted index was cleaned
    env.expect('FT.DEBUG DUMP_INVIDX idx foo').error().contains('not find the inverted index')

    # read from the cursor
    res, cursor = env.cmd('FT.CURSOR READ idx %d' % cursor)

    env.assertEqual(res, [0L])
    env.assertEqual(cursor, 0)

def testNegativeOnly(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA t TEXT').ok()
    conn.execute_command('HSET', 'doc1', 'not', 'foo')

    env.expect('FT.SEARCH idx *').equal([1L, 'doc1', ['not', 'foo']])
    env.expect('FT.SEARCH', 'idx', '-bar').equal([1L, 'doc1', ['not', 'foo']])

def testNotOnly(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'txt1', 'TEXT')
  conn.execute_command('HSET', 'a', 'txt1', 'hello', 'txt2', 'world')
  conn.execute_command('HSET', 'b', 'txt1', 'world', 'txt2', 'hello')
  env.assertEqual(toSortedFlatList(env.cmd('ft.search idx !world')), toSortedFlatList([1L, 'b', ['txt1', 'world', 'txt2', 'hello']]))

def testServerVersion(env):
    env.assertTrue(server_version_at_least(env, "6.0.0"))

def testSchemaWithAs(env):
  conn = getConnectionByEnv(env)
  # sanity
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'AS', 'foo', 'TEXT')
  conn.execute_command('HSET', 'a', 'txt', 'hello')
  conn.execute_command('HSET', 'b', 'foo', 'world')
  
  for _ in env.retry_with_rdb_reload():
    env.expect('ft.search idx @txt:hello').equal([0L])
    env.expect('ft.search idx @txt:world').equal([0L])
    env.expect('ft.search idx @foo:hello').equal([1L, 'a', ['txt', 'hello']])
    env.expect('ft.search idx @foo:world').equal([0L])

    # RETURN from schema
    env.expect('ft.search idx hello RETURN 1 txt').equal([1L, 'a', ['txt', 'hello']])
    env.expect('ft.search idx hello RETURN 1 foo').equal([1L, 'a', ['foo', 'hello']])
    env.expect('ft.search idx hello RETURN 3 txt AS baz').equal([1L, 'a', ['baz', 'hello']])
    env.expect('ft.search idx hello RETURN 3 foo AS baz').equal([1L, 'a', ['baz', 'hello']])
    env.expect('ft.search idx hello RETURN 6 txt AS baz txt AS bar').equal([1L, 'a', ['baz', 'hello', 'bar', 'hello']])
    env.expect('ft.search idx hello RETURN 6 txt AS baz txt AS baz').equal([1L, 'a', ['baz', 'hello']])

    # RETURN outside of schema
    conn.execute_command('HSET', 'a', 'not_in_schema', '42')
    res = conn.execute_command('HGETALL', 'a')
    env.assertEqual(res, {'txt': 'hello', 'not_in_schema': '42'})
    env.expect('ft.search idx hello RETURN 3 not_in_schema AS txt2').equal([1L, 'a', ['txt2', '42']])
    env.expect('ft.search idx hello RETURN 1 not_in_schema').equal([1L, 'a', ['not_in_schema', '42']])
    env.expect('ft.search idx hello').equal([1L, 'a', ['txt', 'hello', 'not_in_schema', '42']])

    env.expect('ft.search idx hello RETURN 3 not_exist as txt2').equal([1L, 'a', []])
    env.expect('ft.search idx hello RETURN 1 not_exist').equal([1L, 'a', []])

    env.expect('ft.search idx hello RETURN 3 txt as as').error().contains('Alias for RETURN cannot be `AS`')
    
    # LOAD for FT.AGGREGATE
    # for path - can rename
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '1', '@txt').equal([1L, ['txt', 'hello']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '3', '@txt', 'AS', 'txt1').equal([1L, ['txt1', 'hello']])

    # for name - cannot rename
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '1', '@foo').equal([1L, ['foo', 'hello']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '3', '@foo', 'AS', 'foo1').equal([1L, ['foo1', 'hello']])
    
    # for for not in schema - can rename
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '1', '@not_in_schema').equal([1L, ['not_in_schema', '42']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', '3', '@not_in_schema', 'AS', 'NIS').equal([1L, ['NIS', '42']])

    conn.execute_command('HDEL', 'a', 'not_in_schema')

def testSchemaWithAs_Alter(env):
  conn = getConnectionByEnv(env)
  # sanity
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'AS', 'foo', 'TEXT')
  conn.execute_command('HSET', 'a', 'txt', 'hello')
  conn.execute_command('HSET', 'b', 'foo', 'world')
  
  # FT.ALTER
  conn.execute_command('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'foo', 'AS', 'bar', 'TEXT')
  waitForIndex(env, 'idx')
  env.expect('ft.search idx @bar:hello').equal([0L])
  env.expect('ft.search idx @bar:world').equal([1L, 'b', ['foo', 'world']])
  env.expect('ft.search idx @foo:world').equal([0L])

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
    env.expect('ft.search conflict2 @foo1:hello').equal([1L, 'a', ['txt', 'hello']])
    env.expect('ft.search conflict2 @foo2:hello').equal([1L, 'a', ['txt', 'hello']])
    env.expect('ft.search conflict2 @foo1:world').equal([0L])
    env.expect('ft.search conflict2 @foo2:world').equal([0L])

def testMod1407(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'limit', 'TEXT', 'LimitationTypeID', 'TAG', 'LimitationTypeDesc', 'TEXT').ok()
    
    conn.execute_command('HSET', 'doc1', 'limit', 'foo1', 'LimitationTypeID', 'boo1', 'LimitationTypeDesc', 'doo1')
    conn.execute_command('HSET', 'doc2', 'limit', 'foo2', 'LimitationTypeID', 'boo2', 'LimitationTypeDesc', 'doo2')

    env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '3', '@limit', '@LimitationTypeID', 'ASC').equal([2L, ['limit', 'foo1', 'LimitationTypeID', 'boo1'], ['limit', 'foo2', 'LimitationTypeID', 'boo2']])

    # make sure the crashed query is not crashing anymore
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '2', 'LLimitationTypeID', 'LLimitationTypeDesc', 'REDUCE', 'COUNT', '0')

    # make sure correct query not crashing and return the right results
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '2', '@LimitationTypeID', '@LimitationTypeDesc', 'REDUCE', 'COUNT', '0').equal([2L, ['LimitationTypeID', 'boo2', 'LimitationTypeDesc', 'doo2', '__generated_aliascount', '1'], ['LimitationTypeID', 'boo1', 'LimitationTypeDesc', 'doo1', '__generated_aliascount', '1']])

def testMod1452(env):
    if not env.isCluster():
        # this test is only relevant on cluster
        env.skip()

    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    
    conn.execute_command('HSET', 'doc1', 't', 'foo')

    # here we only check that its not crashing
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', 'foo', 'REDUCE', 'FIRST_VALUE', 3, '@not_exists', 'BY', '@foo')


def test_mod1548(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$["prod:id"]', 'AS', 'prod:id', 'TEXT', '$.prod:id', 'AS', 'prod:id_unsupported', 'TEXT', '$.name', 'AS', 'name', 'TEXT', '$.categories', 'AS', 'categories', 'TAG', 'SEPARATOR' ,',').ok()
    waitForIndex(env, 'idx')

    res = conn.execute_command('JSON.SET', 'prod:1', '$', '{"prod:id": "35114964", "SKU": "35114964", "name":"foo", "categories":"abcat0200000"}')
    env.assertOk(res)
    res = conn.execute_command('JSON.SET', 'prod:2', '$', '{"prod:id": "35114965", "SKU": "35114965", "name":"bar", "categories":"abcat0200000"}')
    env.assertOk(res)

    # Supported jsonpath
    res = conn.execute_command('FT.SEARCH', 'idx', '@categories:{abcat0200000}', 'RETURN', '1', 'name')
    env.assertEqual(res,  [2L, 'prod:1', ['name', 'foo'], 'prod:2', ['name', 'bar']])

    # Supported jsonpath (actual path contains a colon using the bracket notation)
    res = conn.execute_command('FT.SEARCH', 'idx', '@categories:{abcat0200000}', 'RETURN', '1', 'prod:id')
    env.assertEqual(res,  [2L, 'prod:1', ['prod:id', '35114964'], 'prod:2', ['prod:id', '35114965']])

    # Currently unsupported jsonpath (actual path contains a colon using the dot notation)
    res = conn.execute_command('FT.SEARCH', 'idx', '@categories:{abcat0200000}', 'RETURN', '1', 'prod:id_unsupported')
    env.assertEqual(res, [2L, 'prod:1', [], 'prod:2', []])

