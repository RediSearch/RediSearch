# -*- coding: utf-8 -*-

import redis
import unittest
from hotels import hotels
import random
import time
from RLTest import Env
from includes import *

def testAdd(env):
    if env.is_cluster():
        raise unittest.SkipTest()

    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
    env.assertTrue(r.exists('idx:idx'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'hello world',
                                    'body', 'lorem ist ipsum'))

    for _ in r.retry_with_rdb_reload():
        prefix = 'ft'
        env.assertExists(prefix + ':idx/hello')
        env.assertExists(prefix + ':idx/world')
        env.assertExists(prefix + ':idx/lorem')

def testAddErrors(env):
    env.expect('ft.create idx schema foo text bar numeric sortable').equal('OK')
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
        'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
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
        "ft.create", "test", "SCHEMA",  "tags", "TAG", "waypoint", "GEO"))
    env.assertOk(r.execute_command(
        "ft.add", "test", "1", "1", "FIELDS", "tags", "alberta", "waypoint", "-113.524,53.5244"))
    env.assertOk(r.execute_command(
        "ft.add", "test", "2", "1", "FIELDS", "tags", "ontario", "waypoint", "-79.395,43.661667"))

    r.cmd('ft.search', 'test', '@tags:{ontario}')

    res = r.execute_command(
        'ft.search', 'test', "@waypoint:[-113.52 53.52 20 mi]|@tags:{ontario}", 'nocontent')
    env.assertEqual(res, [2, '2', '1'])

def testAttributes(env):
    env.assertOk(env.cmd('ft.create', 'idx', 'schema',
                           'title', 'text', 'body', 'text'))
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
        'ft.create', 'idx', 'schema', 'f', 'text'))
    for i in range(N):

        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world' if i % 2 == 0 else 'hallo werld'))

    for _ in r.retry_with_rdb_reload():
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
            'ft.search', 'idx', '(hello world)|((hello world)|(hallo world|werld) | hello world werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
        env.assertEqual(101, len(res))
        env.assertEqual(100, res[0])

def testSearch(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                    'title', 'hello world',
                                    'body', 'lorem ist ipsum'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'hello another world',
                                    'body', 'lorem ist ipsum lorem lorem'))
    for _ in r.retry_with_rdb_reload():

        res = r.execute_command('ft.search', 'idx', 'hello')

        env.assertTrue(len(res) == 5)
        env.assertEqual(res[0], 2L)
        env.assertEqual(res[1], "doc2")
        env.assertTrue(isinstance(res[2], list))
        env.assertTrue('title' in res[2])
        env.assertTrue('hello another world' in res[2])
        env.assertEqual(res[3], "doc1")
        env.assertTrue('hello world' in res[4])

        # Test empty query
        res = r.execute_command('ft.search', 'idx', '')
        env.assertListEqual([0], res)

        # Test searching with no content
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent')
        env.assertTrue(len(res) == 3)
        env.assertEqual(res[0], 2L)
        env.assertEqual(res[1], "doc2")
        env.assertEqual(res[2], "doc1")

        # Test searching WITHSCORES
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'WITHSCORES')
        env.assertEqual(len(res), 7)
        env.assertEqual(res[0], 2L)
        env.assertEqual(res[1], "doc2")
        env.assertTrue(float(res[2]) > 0)
        env.assertEqual(res[4], "doc1")
        env.assertTrue(float(res[5]) > 0)

        # Test searching WITHSCORES NOCONTENT
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
        env.assertEqual(len(res), 5)
        env.assertEqual(res[0], 2L)
        env.assertEqual(res[1], "doc2")
        env.assertTrue(float(res[2]) > 0)
        env.assertEqual(res[3], "doc1")
        env.assertTrue(float(res[4]) > 0)

def testSearchNosave(env):
    # Check to see what happens when we try to return unsaved documents
    env.cmd('ft.create', 'idx', 'SCHEMA', 'f1', 'text')
    # Add 3 documents
    for x in range(3):
        env.cmd('ft.add', 'idx', 'doc{}'.format(x),
                 1.0, 'NOSAVE', 'FIELDS', 'f1', 'value')

    # Now query the results
    res = env.cmd('ft.search', 'idx', 'value')
    env.assertEqual(3, res[0])
    for content in res[2::2]:
        env.assertEqual([], content)

def testGet(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'text'))

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
        env.assertListEqual(
            ['foo', 'hello world', 'bar', 'wat wat'], res)
        env.assertIsNone(r.execute_command(
            'ft.get', 'idx', 'doc%dsdfsd' % i))
    env.expect('ft.get', 'no_idx', 'doc0').error().contains("Unknown Index name")

    rr = r.execute_command(
        'ft.mget', 'idx', *('doc%d' % i for i in range(100)))
    env.assertEqual(len(rr), 100)
    for res in rr:
        env.assertIsNotNone(res)
        env.assertListEqual(
            ['foo', 'hello world', 'bar', 'wat wat'], res)
    rr = r.execute_command(
        'ft.mget', 'idx', *('doc-%d' % i for i in range(100)))
    env.assertEqual(len(rr), 100)
    for res in rr:
        env.assertIsNone(res)

    # Verify that when a document is deleted, GET returns NULL
    r.cmd('ft.del', 'idx', 'doc10') # But we still keep the document
    r.cmd('ft.del', 'idx', 'doc11')
    r.cmd('ft.del', 'idx', 'coverage')
    res = r.cmd('ft.get', 'idx', 'doc10')
    r.assertEqual(None, res)
    res = r.cmd('ft.mget', 'idx', 'doc10')
    r.assertEqual([None], res)
    res = r.cmd('ft.mget', 'idx', 'doc10', 'doc11', 'doc12')
    r.assertIsNone(res[0])
    r.assertIsNone(res[1])
    r.assertTrue(not not res[2])

def testDelete(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'f', 'text'))

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
        'ft.create', 'idx', 'schema', 'f', 'text'))

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
        'ft.create', 'idx', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo'))

    for i in range(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world', 'n', 666, 't', 'foo bar',
                                        'g', '19.04,47.497'))
    keys = r.keys('*')
    env.assertGreaterEqual(len(keys), 100)

    env.assertOk(r.execute_command('ft.drop', 'idx'))
    keys = r.keys('*')
    env.assertEqual(0, len(keys))

    # Now do the same with KEEPDOCS
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo'))

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

def testCustomStopwords(env):
    r = env
    # Index with default stopwords
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text'))

    # Index with custom stopwords
    env.assertOk(r.execute_command('ft.create', 'idx2', 'stopwords', 2, 'hello', 'world',
                                    'schema', 'foo', 'text'))
    if not env.isCluster:
        res = env.cmd('ft.info', 'idx2')
        env.assertEqual(res[39], ['hello', 'world'])

    # Index with NO stopwords
    env.assertOk(r.execute_command('ft.create', 'idx3', 'stopwords', 0,
                                    'schema', 'foo', 'text'))
    
    if not env.isCluster:
        res = env.cmd('ft.info', 'idx3')
        env.assertEqual(res[39], [])

    for idx in ('idx', 'idx2', 'idx3'):
        env.assertOk(r.execute_command(
            'ft.add', idx, 'doc1', 1.0, 'fields', 'foo', 'hello world'))
        env.assertOk(r.execute_command(
            'ft.add', idx, 'doc2', 1.0, 'fields', 'foo', 'to be or not to be'))

    for _ in r.retry_with_rdb_reload():
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
    env.cmd('ft.create', 'idx', 'stopwords', 3, 'foo',
             'bar', 'baz', 'schema', 'txt', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'txt', 'foo bar')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields', 'txt', 'hello world')

    r1 = env.cmd('ft.search', 'idx', 'foo bar', 'nocontent')
    r2 = env.cmd('ft.search', 'idx', 'foo bar hello world', 'nocontent')
    env.assertEqual(0, r1[0])
    env.assertEqual(1, r2[0])

def testNoStopwords(env):
    # This test taken from Java's test suite
    env.cmd('ft.create', 'idx', 'schema', 'title', 'text')
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
        'ft.create', 'idx', 'schema', 'foo', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx',
                                    'doc1', 1.0, 'fields', 'foo', 'hello wat woot'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2',
                                    1.0, 'fields', 'foo', 'hello world woot'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc3',
                                    1.0, 'fields', 'foo', 'hello world werld'))

    res = r.execute_command('ft.search', 'idx', 'hello', 'nocontent')
    env.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)
    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual([2L, 'doc3', 'doc2'], res)
    res = r.execute_command(
        'ft.search', 'idx', 'hello ~world', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)
    res = r.execute_command(
        'ft.search', 'idx', 'hello ~world ~werld', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)
    res = r.execute_command(
        'ft.search', 'idx', '~world ~werld hello', 'nocontent', 'scorer', 'DISMAX')
    env.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)

def testExplain(env):

    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
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
        'ft.create', 'idx', 'schema',
        'foo', 'text',
        'num', 'numeric', 'sortable', 'noindex',
        'extra', 'text', 'noindex', 'sortable'))

    if not env.isCluster():
        # to specific check on cluster, todo : change it to be generic enough
        res = env.cmd('ft.info', 'idx')
        env.assertEqual(res[5][1][4], 'NOINDEX')
        env.assertEqual(res[5][2][6], 'NOINDEX')

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
        'ft.create', 'idx', 'schema',
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
    assertResultsEqual(env, [2L, 'doc1', ['foo', 'hello world', 'num', '3', 'extra', 'jorem gipsum'],
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
    env.assertGreater(float(res[2]), 1)

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
        'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
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
        'ft.create', 'idx', 'schema', 'foo', 'text'))
    N = 100
    for i in range(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'constant term%d' % (random.randrange(0, 5))))
    for _ in r.retry_with_rdb_reload():
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
        'ft.create', 'idx', 'schema', 'foo', 'text', 'sortable', 'bar', 'numeric', 'sortable'))
    N = 100
    for i in range(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello%03d world' % i, 'bar', 100 - i))
    for _ in r.retry_with_rdb_reload():

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
            [100L, 'doc2', '0', 'doc3', '0', 'doc4', '0', 'doc5', '0', 'doc6', '0'], res)

        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'bar', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertListEqual(
            [100L, 'doc0', '#100', 'doc1', '#99', 'doc2', '#98', 'doc3', '#97', 'doc4', '#96'], res)
        res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                'sortby', 'foo', 'desc', 'withsortkeys', 'limit', 0, 5)
        env.assertListEqual([100L, 'doc99', '$hello099 world', 'doc98', '$hello098 world', 'doc97', '$hello097 world', 'doc96',
                              '$hello096 world', 'doc95', '$hello095 world'], res)

def testNot(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text'))
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
        'ft.create', 'idx', 'schema', 'a', 'text', 'b', 'text', 'c', 'text', 'd', 'text'))
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
        'ft.create', 'idx', 'schema', 'foo', 'text'))

    for i in range(200):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'foo', 'hello world'))

    for _ in r.retry_with_rdb_reload():

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
        'ft.create', 'idx', 'schema', 'title', 'text'))
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

def testExact(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text'))
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
    env.expect('ft.create idx schema name text location geo').equal('OK')
    env.expect('ft.add idx hotel 1.0 fields name hill location -0.1757,51.5156').equal('OK')
    env.expect('ft.search idx hilton geofilter location -0.1757 51.5156 1 km').equal([0L])

    # Insert error
    env.expect('ft.add', 'idx', 'hotel1', 1, 'fields', 'name', '_hotel1', 'location', '1, 1').error()   \
            .contains('Could not index geo value')

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

    if not env.isCluster():
        env.expect('flushall')
        env.expect('set geo:idx/location foo').equal('OK')
        env.expect('ft.create idx schema name text location geo').equal('OK')
        env.expect('ft.add idx hotel 1.0 fields name hill location -0.1757,51.5156').error() \
                .contains('Could not index geo value')

def testGeo(env):
    r = env
    gsearch = lambda query, lon, lat, dist, unit='km': r.execute_command(
        'ft.search', 'idx', query, 'geofilter', 'location', lon, lat, dist, unit)

    gsearch_inline = lambda query, lon, lat, dist, unit='km': r.execute_command(
        'ft.search', 'idx', '{} @location:[{} {} {} {}]'.format(query,  lon, lat, dist, unit))

    env.assertOk(r.execute_command('ft.create', 'idx',
                                    'schema', 'name', 'text', 'location', 'geo'))

    for i, hotel in enumerate(hotels):
        env.assertOk(r.execute_command('ft.add', 'idx', 'hotel{}'.format(i), 1.0, 'fields', 'name',
                                        hotel[0], 'location', '{},{}'.format(hotel[2], hotel[1])))

    for _ in r.retry_with_rdb_reload():
        res = r.execute_command('ft.search', 'idx', 'hilton')
        env.assertEqual(len(hotels), res[0])

        res = gsearch('hilton', "-0.1757", "51.5156", '1')
        print res
        env.assertEqual(3, res[0])
        env.assertEqual('hotel2', res[5])
        env.assertEqual('hotel21', res[3])
        env.assertEqual('hotel79', res[1])
        res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '1')
        env.assertListEqual(res, res2)

        res = gsearch('hilton', "-0.1757", "51.5156", '10')
        env.assertEqual(14, res[0])
        env.assertEqual('hotel93', res[1])
        env.assertEqual('hotel92', res[3])
        env.assertEqual('hotel79', res[5])

        res2 = gsearch('hilton', "-0.1757", "51.5156", '10000', 'm')
        env.assertListEqual(res, res2)
        res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '10')
        env.assertListEqual(res, res2)

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'm')
        env.assertEqual(1, res[0])
        env.assertEqual('hotel94', res[1])
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '10', 'm')
        env.assertListEqual(res, res2)

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'km')
        env.assertEqual(5, res[0])
        env.assertIn('hotel94', res)
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '10', 'km')
        env.assertListEqual(res, res2)

        res = gsearch('heathrow', -0.44155, 51.45865, '5', 'km')
        env.assertEqual(3, res[0])
        env.assertIn('hotel94', res)
        res2 = gsearch_inline(
            'heathrow', -0.44155, 51.45865, '5', 'km')
        env.assertListEqual(res, res2)

def testTagErrors(env):
    env.expect("ft.create", "test", "SCHEMA",  "tags", "TAG").equal('OK')
    env.expect("ft.add", "test", "1", "1", "FIELDS", "tags", "alberta").equal('OK')
    env.expect("ft.add", "test", "2", "1", "FIELDS", "tags", "ontario. alberta").equal('OK')

def testGeoDeletion(env):
    if env.is_cluster():
        raise unittest.SkipTest()
        # Can't properly test if deleted on cluster

    env.cmd('ft.create', 'idx', 'schema',
            'g1', 'geo', 'g2', 'geo', 't1', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
            'g1', "-0.1757,51.5156",
            'g2', "-0.1757,51.5156",
            't1', "hello")
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
            'g1', "-0.1757,51.5156",
            'g2', "-0.1757,51.5156",
            't1', "hello")

    # keys are: "geo:idx/g1" and "geo:idx/g2"
    env.assertEqual(2, env.cmd('zcard', 'geo:idx/g1'))
    env.assertEqual(2, env.cmd('zcard', 'geo:idx/g2'))

    # Remove the first doc
    env.cmd('ft.del', 'idx', 'doc1')
    env.assertEqual(1, env.cmd('zcard', 'geo:idx/g1'))
    env.assertEqual(1, env.cmd('zcard', 'geo:idx/g2'))

    # Replace the other one:
    env.cmd('ft.add', 'idx', 'doc2', 1.0,
            'replace', 'fields',
            't1', 'just text here')
    env.assertEqual(0, env.cmd('zcard', 'geo:idx/g1'))
    env.assertEqual(0, env.cmd('zcard', 'geo:idx/g2'))

def testAddHash(env):
    if env.is_cluster():
        raise unittest.SkipTest()

    r = env
    env.assertOk(r.execute_command('ft.create', 'idx', 'schema',
                                    'title', 'text', 'weight', 10.0, 'body', 'text', 'price', 'numeric'))

    env.assertTrue(
        r.hmset('doc1', {"title": "hello world", "body": "lorem ipsum", "price": 2}))
    env.assertTrue(
        r.hmset('doc2', {"title": "hello werld", "body": "lorem ipsum", "price": 5}))

    env.assertOk(r.execute_command('ft.addhash', 'idx', 'doc1', 1.0))
    env.assertOk(r.execute_command('ft.addhash', 'idx', 'doc2', 1.0))
    env.expect('ft.addhash', 'idx', 'doc3', 1.0, 1.0).error().contains('Unknown keyword: `1.0`')

    res = r.execute_command('ft.search', 'idx', "hello", "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])
    env.assertEqual("doc1", res[2])
    env.assertEqual("doc2", res[1])

    res = r.execute_command(
        'ft.search', 'idx',
        "hello",
        "filter", "price", "0", "3"
        )
    env.assertEqual(3, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc1", res[1])
    env.assertListEqual(
        ['body', 'lorem ipsum', 'price', '2', 'title', 'hello world'], res[2])

    res = r.execute_command(
        'ft.search', 'idx', "hello werld", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

def testSafeAddHash(env):
    if env.is_cluster():
        raise unittest.SkipTest()

    r = env
    env.assertOk(r.execute_command('ft.create', 'idx', 'schema',
                                    'title', 'text', 'weight', 10.0, 'body', 'text', 'price', 'numeric'))

    env.assertTrue(
        r.hmset('doc1', {"title": "hello world", "body": "lorem ipsum", "price": 2}))
    env.assertTrue(
        r.hmset('doc2', {"title": "hello werld", "body": "lorem ipsum", "price": 5}))

    env.expect('ft.safeaddhash idx doc1 1.0').equal('OK')
    env.expect('ft.safeaddhash idx doc2 1.0').equal('OK')
    env.expect('ft.safeaddhash idx').error().contains("wrong number of arguments for 'ft.safeaddhash' command")
    env.expect('ft.safeaddhash idx doc3 2.0').error().contains('Score must be between 0 and 1')
    env.expect('ft.safeaddhash idx doc3 -2.0').error().contains('Score must be between 0 and 1')
    env.expect('ft.safeaddhash idx doc3 1.0 1.0').error().contains('Unknown keyword: `1.0`')
    env.expect('ft.safeaddhash idx doc3 not_a_number').error().contains('Could not parse document score')
    env.expect('ft.safeaddhash idx doc3 1.0 LANGUAGE RediSearch').error().contains('Unknown language: `RediSearch`')
    env.expect('ft.safeaddhash idx doc3 1.0 LANGUAGE RediSearch not_an_arg').error().contains("Unknown keyword: `not_an_arg`")
    #env.expect('ft.safeaddhash', 'idx', 'doc3', '1.0', 'LANGUAGE', 'RediSearch, ""').error().contains("Error parsing arguments for `%s`: %s")
    env.expect('ft.safeaddhash not_idx doc3 1.0').error().contains('Unknown Index name')
    res = r.execute_command('ft.search', 'idx', "hello", "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])
    env.assertEqual("doc1", res[2])
    env.assertEqual("doc2", res[1])

    res = r.execute_command(
        'ft.search', 'idx',
        "hello",
        "filter", "price", "0", "3"
        )
    env.assertEqual(3, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc1", res[1])
    env.assertListEqual(
        ['body', 'lorem ipsum', 'price', '2', 'title', 'hello world'], res[2])

    res = r.execute_command(
        'ft.search', 'idx', "hello werld", "nocontent")
    env.assertEqual(2, len(res))
    env.assertEqual(1, res[0])
    env.assertEqual("doc2", res[1])

def testInfields(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text', 'weight', 1.0))
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
        'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))

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
        'ft.create', 'idx', 'schema', 'TiTle', 'text', 'BoDy', 'text', "יוניקוד", 'text', 'field.with,punct', 'text'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                    'title', 'hello world', 'body', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt'))
    env.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 0.5, 'fields',
                                    'body', 'hello world', 'title', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt'))

    res = r.execute_command(
        'ft.search', 'idx', '@title:hello world', 'nocontent')
    env.assertEqual(res, [1, 'doc1'])
    res = r.execute_command(
        'ft.search', 'idx', '@body:hello world', 'nocontent')
    env.assertEqual(res, [1, 'doc2'])

    res = r.execute_command(
        'ft.search', 'idx', '@body:hello @title:world', 'nocontent')
    env.assertEqual(res, [0])

    res = r.execute_command(
        'ft.search', 'idx', '@body:hello world @title:world', 'nocontent')
    env.assertEqual(res, [0])
    res = r.execute_command(
        'ft.search', 'idx', '@BoDy:(hello|foo) @Title:(world|bar)', 'nocontent')
    env.assertEqual(res, [2, 'doc1', 'doc2'])

    res = r.execute_command(
        'ft.search', 'idx', '@body:(hello|foo world|bar)', 'nocontent')
    env.assertEqual(res, [2, 'doc1', 'doc2'])

    res = r.execute_command(
        'ft.search', 'idx', '@body|title:(hello world)', 'nocontent')
    env.assertEqual(res, [2, 'doc1', 'doc2'])

    res = r.execute_command(
        'ft.search', 'idx', '@יוניקוד:(unicode)', 'nocontent')
    env.assertEqual(res, [2, 'doc1', 'doc2'])

    res = r.execute_command(
        'ft.search', 'idx', '@field\\.with\\,punct:(punt)', 'nocontent')
    env.assertEqual(res, [2, 'doc1', 'doc2'])

def testStemming(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'title', 'text'))
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
        'ft.create', 'idx', 'schema', 'title', 'text'))
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
        'ft.create', 'idx', 'schema', 'title', 'text', 'score', 'numeric', 'price', 'numeric'))

    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 5).error().contains("FILTER requires 3 arguments")
    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 5, 'inf').error().contains("Bad upper range: inf")
    env.expect('ft.search', 'idx', 'hello kitty', 'filter', 'score', 'inf', 5).error().contains("Bad lower range: inf")

    for i in xrange(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'fields',
                                        'title', 'hello kitty', 'score', i, 'price', 100 + 10 * i))

    for _ in r.retry_with_rdb_reload():
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

def testSuggestions(env):
    r = env
    env.assertEqual(1, r.execute_command(
        'ft.SUGADD', 'ac', 'hello world', 1))
    env.assertEqual(1, r.execute_command(
        'ft.SUGADD', 'ac', 'hello world', 1, 'INCR'))

    res = r.execute_command("FT.SUGGET", "ac", "hello")
    env.assertEqual(1, len(res))
    env.assertEqual("hello world", res[0])

    terms = ["hello werld", "hallo world",
             "yellow world", "wazzup", "herp", "derp"]
    sz = 2
    for term in terms:
        env.assertEqual(sz, r.execute_command(
            'ft.SUGADD', 'ac', term, sz - 1))
        sz += 1

    for _ in r.retry_with_rdb_reload():

        env.assertEqual(7, r.execute_command('ft.SUGLEN', 'ac'))

        # search not fuzzy
        env.assertEqual(["hello world", "hello werld"],
                         r.execute_command("ft.SUGGET", "ac", "hello"))

        # print  r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1", "WITHSCORES")
        # search fuzzy - shuold yield more results
        env.assertEqual(['hello world', 'hello werld', 'yellow world', 'hallo world'],
                         r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY"))

        # search fuzzy with limit of 1
        env.assertEqual(['hello world'],
                         r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1"))

        # scores should return on WITHSCORES
        rc = r.execute_command(
            "ft.SUGGET", "ac", "hello", "WITHSCORES")
        env.assertEqual(4, len(rc))
        env.assertTrue(float(rc[1]) > 0)
        env.assertTrue(float(rc[3]) > 0)

    rc = r.execute_command("ft.SUGDEL", "ac", "hello world")
    env.assertEqual(1L, rc)
    rc = r.execute_command("ft.SUGDEL", "ac", "world")
    env.assertEqual(0L, rc)

    rc = r.execute_command("ft.SUGGET", "ac", "hello")
    env.assertEqual(['hello werld'], rc)

def testSuggestErrors(env):
    env.expect('ft.SUGADD ac olah 1').equal(1)
    env.expect('ft.SUGADD ac olah 1 INCR').equal(1)
    env.expect('ft.SUGADD ac missing').error().contains("wrong number of arguments")
    env.expect('ft.SUGADD ac olah not_a_number').error().contains("invalid score")
    env.expect('ft.SUGADD ac olah 1 PAYLOAD').error().contains('Invalid payload: Expected an argument, but none provided')
    env.expect('ft.SUGADD ac olah 1 REDIS PAYLOAD payload').error().contains('Unknown argument `REDIS`')
    env.expect('ft.SUGGET ac olah FUZZ').error().contains("Unrecognized argument: FUZZ")
    query = 'verylongquery'
    for _ in range(3):
        query += query
    env.expect('ft.SUGGET ac', query).error().contains("Invalid query")
    env.expect('ft.SUGGET ac', query + query).error().contains("Invalid query length")

def testSuggestPayload(env):
    r = env
    env.assertEqual(1, r.execute_command(
        'ft.SUGADD', 'ac', 'hello world', 1, 'PAYLOAD', 'foo'))
    env.assertEqual(2, r.execute_command(
        'ft.SUGADD', 'ac', 'hello werld', 1, 'PAYLOAD', 'bar'))
    env.assertEqual(3, r.execute_command(
        'ft.SUGADD', 'ac', 'hello nopayload', 1, 'PAYLOAD', ''))
    env.assertEqual(4, r.execute_command(
        'ft.SUGADD', 'ac', 'hello nopayload2', 1))

    res = r.execute_command("FT.SUGGET", "ac", "hello", 'WITHPAYLOADS')
    env.assertListEqual(['hello world', 'foo', 'hello werld', 'bar', 'hello nopayload', None, 'hello nopayload2', None],
                         res)
    res = r.execute_command("FT.SUGGET", "ac", "hello")
    env.assertListEqual(['hello world',  'hello werld', 'hello nopayload', 'hello nopayload2'],
                         res)
    res = r.execute_command(
        "FT.SUGGET", "ac", "hello", 'WITHPAYLOADS', 'WITHSCORES')
    # we don't compare the scores beause they may change
    env.assertEqual(12, len(res))

def testPayload(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'f', 'text'))
    for i in range(10):

        env.assertOk(r.execute_command('ft.add', 'idx', '%d' % i, 1.0,
                                        'payload', 'payload %d' % i,
                                        'fields', 'f', 'hello world'))

    for x in r.retry_with_rdb_reload():

        res = r.execute_command(
            'ft.search', 'idx', 'hello world')
        env.assertEqual(21, len(res))

        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'withpayloads')

        env.assertEqual(31, len(res))
        env.assertEqual(10, res[0])
        for i in range(1, 30, 3):
            env.assertEqual(res[i + 1], 'payload %s' % res[i])

def testGarbageCollector(env):
    env.skipOnCluster()
    if env.moduleArgs is not None and 'GC_POLICY FORK' in env.moduleArgs:
        # this test is not relevent for fork gc cause its not cleaning the last block
        raise unittest.SkipTest()
    N = 100
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text'))
    for i in range(N):

        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0,
                                        'fields', 'foo', ' '.join(('term%d' % random.randrange(0, 10) for i in range(10)))))

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
        env.assertEqual(1, r.execute_command(
            'ft.del', 'idx', 'doc%d' % i))

    for _ in range(100):
        # gc is random so we need to do it long enough times for it to work
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

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
    env.assertCmdOk('ft.create', 'idx', 'schema',
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
    except:
        pass

    options = ['idx'] + options + ['schema', 'f1', 'text', 'f2', 'text']
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
            env.assertEqual('doc200', docname)
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
        'ft.create', 'idx', 'NOFIELDS', 'schema', 'title', 'text'))
    N = 50
    for i in xrange(N):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'replace', 'fields',
                                        'title', 'hello term%d' % i))
    for _ in r.retry_with_rdb_reload():

        res = r.execute_command('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}

        env.assertEqual(d['index_name'], 'idx')
        env.assertEqual(d['index_options'], ['NOFIELDS'])
        env.assertListEqual(
            d['fields'], [['title', 'type', 'TEXT', 'WEIGHT', '1']])

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
            env.assertCmdOk('ft.create', 'idx', *options)
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
    env.cmd('ft.create', 'idx', 'schema', 'body',
             'text', 'name', 'text', 'nostem')
    if not env.isCluster():
        # todo: change it to be more generic to pass on is_cluster
        res = env.cmd('ft.info', 'idx')
        env.assertEqual(res[5][1][5], 'NOSTEM')
    for _ in env.retry_with_reload():
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

def testSearchNonexistField(env):
    # GH Issue 133
    env.cmd('ft.create', 'idx', 'schema', 'title', 'text',
             'weight', 5.0, 'body', 'text', 'url', 'text')
    env.cmd('ft.add', 'idx', 'd1', 1.0, 'nosave', 'fields', 'title',
             'hello world', 'body', 'lorem dipsum', 'place', '-77.0366 38.8977')
    env.cmd('ft.search', 'idx', 'Foo', 'GEOFILTER',
             'place', '-77.0366', '38.8977', '1', 'km')

def testSortbyMissingField(env):
    # GH Issue 131
    env.cmd('ft.create', 'ix', 'schema', 'txt',
             'text', 'num', 'numeric', 'sortable')
    env.cmd('ft.add', 'ix', 'doc1', 1.0, 'fields', 'txt', 'foo')
    env.cmd('ft.search', 'ix', 'foo', 'sortby', 'num')

def testParallelIndexing(env):
    # GH Issue 207
    env.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
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
    env.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
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

    env.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
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
    env.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
    # Insert a document
    env.cmd('ft.add', 'idx', 'Hello', 1.0, 'fields', 'txt', 'NoBin match')
    env.cmd('ft.add', 'idx', 'Hello\x00World', 1.0, 'fields', 'txt', 'Bin match')
    for _ in env.reloading_iterator():
        exp = [2L, 'Hello\x00World', ['txt', 'Bin match'], 'Hello', ['txt', 'NoBin match']]
        res = env.cmd('ft.search', 'idx', 'match')
        env.assertEqual(exp, res)

def testNonDefaultDb(env):
    if env.is_cluster():
        raise unittest.SkipTest()

    # Should be ok
    env.cmd('FT.CREATE', 'idx1', 'schema', 'txt', 'text')
    try:
        env.cmd('SELECT 1')
    except redis.ResponseError:
        return

    # Should fail
    with env.assertResponseError():
        env.cmd('FT.CREATE', 'idx2', 'schema', 'txt', 'text')

def testDuplicateNonspecFields(env):
    env.cmd('FT.CREATE', 'idx', 'schema', 'txt', 'text')
    env.cmd('FT.ADD', 'idx', 'doc', 1.0, 'fields',
             'f1', 'f1val', 'f1', 'f1val2', 'F1', 'f1Val3')

    res = env.cmd('ft.get', 'idx', 'doc')
    res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertTrue(res['f1'] in ('f1val', 'f1val2'))
    env.assertEqual('f1Val3', res['F1'])

def testDuplicateFields(env):
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt',
             'TEXT', 'num', 'NUMERIC', 'SORTABLE')
    for _ in env.retry_with_reload():
        # Ensure the index assignment is correct after an rdb load
        with env.assertResponseError():
            env.cmd('FT.ADD', 'idx', 'doc', 1.0, 'FIELDS',
                     'txt', 'foo', 'txt', 'bar', 'txt', 'baz')

        # Try add hash
        env.hmset('newDoc', {'txt': 'foo', 'Txt': 'bar', 'txT': 'baz'})
        # Get the actual value:

        from redis import ResponseError
        if not env.is_cluster():
            with env.assertResponseError(contained='twice'):
                env.cmd('FT.ADDHASH', 'idx', 'newDoc', 1.0)

        # Try with REPLACE
        with env.assertResponseError():
            env.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'REPLACE', 'FIELDS',
                     'txt', 'foo', 'txt', 'bar')

        # With replace partial
        env.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'REPLACE',
                 'PARTIAL', 'FIELDS', 'num', 42)
        with env.assertResponseError():
            env.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'REPLACE',
                     'PARTIAL', 'FIELDS', 'num', 42, 'num', 32)

def testDuplicateSpec(env):
    with env.assertResponseError():
        env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1',
                 'text', 'n1', 'numeric', 'f1', 'text')

def testSortbyMissingFieldSparse(env):
    # Note, the document needs to have one present sortable field in
    # order for the indexer to give it a sort vector
    env.cmd('ft.create', 'idx', 'SCHEMA', 'lastName', 'text',
             'SORTABLE', 'firstName', 'text', 'SORTABLE')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'lastName', 'mark')
    res = env.cmd('ft.search', 'idx', 'mark', 'WITHSORTKEYS', "SORTBY",
                   "firstName", "ASC", "limit", 0, 100)
    # commented because we don't filter out exclusive sortby fields
    # env.assertEqual([1L, 'doc1', None, ['lastName', 'mark']], res)

def testLuaAndMulti(env):
    if env.is_cluster():
        raise unittest.SkipTest()
    # Ensure we can work in Lua and Multi environments without crashing
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1', 'text', 'n1', 'numeric')
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
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'language', 'TEXT')
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
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1', 'TEXT')
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
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'f1', 'hello', 'f2', 'world')
    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f2', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'FIELDS', 'f1', 'hello', 'f2', 'world')
    for _ in env.retry_with_reload():
        ret = env.cmd('FT.SEARCH', 'idx', 'world')
        env.assertEqual([1, 'doc2', ['f1', 'hello', 'f2', 'world']], ret)

    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f3', 'TEXT', 'SORTABLE')
    for x in range(10):
        env.cmd('FT.ADD', 'idx', 'doc{}'.format(x + 3), 1.0,
                 'FIELDS', 'f1', 'hello', 'f3', 'val{}'.format(x))

    for _ in env.retry_with_reload():
        # Test that sortable works
        res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SORTBY', 'f3', 'DESC')
        exp = [12, 'doc12', ['f1', 'hello', 'f3', 'val9'], 'doc11', ['f1', 'hello', 'f3', 'val8'], 'doc10', ['f1', 'hello', 'f3', 'val7'], 'doc9', ['f1', 'hello', 'f3', 'val6'], 'doc8', ['f1', 'hello', 'f3', 'val5'], 'doc7', [
                'f1', 'hello', 'f3', 'val4'], 'doc6', ['f1', 'hello', 'f3', 'val3'], 'doc5', ['f1', 'hello', 'f3', 'val2'], 'doc4', ['f1', 'hello', 'f3', 'val1'], 'doc3', ['f1', 'hello', 'f3', 'val0']]

        assertResultsEqual(env, exp, res)

    # Test that we can add a numeric field
    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n1', 'NUMERIC')
    env.cmd('FT.ADD', 'idx', 'docN1', 1.0, 'FIELDS', 'n1', 50)
    env.cmd('FT.ADD', 'idx', 'docN2', 1.0, 'FIELDS', 'n1', 250)
    for _ in env.retry_with_reload():
        res = env.cmd('FT.SEARCH', 'idx', '@n1:[0 100]')
        env.assertEqual([1, 'docN1', ['n1', '50']], res)

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'NOT_ADD', 'f2', 'TEXT').error()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD').error()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f2').error()

def testAlterValidation(env):
    # Test that constraints for ALTER comand
    env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'f0', 'TEXT')
    for x in range(1, 32):
        env.cmd('FT.ALTER', 'idx1', 'SCHEMA', 'ADD', 'f{}'.format(x), 'TEXT')
    # OK for now.

    # Should be too many indexes
    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER',
                      'idx1', 'SCHEMA', 'ADD', 'tooBig', 'TEXT')

    env.cmd('FT.CREATE', 'idx2', 'MAXTEXTFIELDS', 'SCHEMA', 'f0', 'TEXT')
    # print env.cmd('FT.INFO', 'idx2')
    for x in range(1, 50):
        env.cmd('FT.ALTER', 'idx2', 'SCHEMA', 'ADD', 'f{}'.format(x + 1), 'TEXT')

    env.cmd('FT.ADD', 'idx2', 'doc1', 1.0, 'FIELDS', 'f50', 'hello')
    for _ in env.retry_with_reload():
        ret = env.cmd('FT.SEARCH', 'idx2', '@f50:hello')
        env.assertEqual([1, 'doc1', ['f50', 'hello']], ret)

    env.cmd('FT.CREATE', 'idx3', 'SCHEMA', 'f0', 'text')
    # Try to alter the index with garbage
    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER', 'idx3',
                      'SCHEMA', 'ADD', 'f1', 'TEXT', 'f2', 'garbage')
    ret = to_dict(env.cmd('ft.info', 'idx3'))
    env.assertEqual(1, len(ret['fields']))

    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER',
                      'nonExist', 'SCHEMA', 'ADD', 'f1', 'TEXT')

    # test with no fields!
    env.assertRaises(redis.ResponseError, env.cmd, 'FT.ALTER', 'idx2', 'SCHEMA', 'ADD')

def testIssue366_1(env):
    if env.is_cluster():
        raise unittest.SkipTest('ADDHASH unsupported!')
    # Test random RDB regressions, see GH 366
    env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
    env.hmset('foo', {'textfield': 'blah', 'numfield': 1})
    env.cmd('FT.ADDHASH', 'idx1', 'foo', 1, 'replace')
    env.cmd('FT.DEL', 'idx1', 'foo')
    for _ in env.retry_with_reload():
        pass  # --just ensure it doesn't crash

def testIssue366_2(env):
    # FT.CREATE atest SCHEMA textfield TEXT numfield NUMERIC
    # FT.ADD atest anId 1 PAYLOAD '{"hello":"world"}' FIELDS textfield sometext numfield 1234
    # FT.ADD atest anId 1 PAYLOAD '{"hello":"world2"}' REPLACE PARTIAL FIELDS numfield 1111
    # shutdown
    env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
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
    env.cmd('ft.create', 'idx', 'schema', 'id', 'numeric')
    env.cmd('ft.add', 'idx', 1, 1, 'fields', 'id', 1)
    env.cmd('ft.add', 'idx', 2, 1, 'fields', 'id', 2)
    res = env.cmd('ft.search', 'idx', '*', 'filter', '@version', 0, 2)

def testReplaceReload(env):
    env.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
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
    command = ['ft.create', 'idx', 'schema']
    for x in range(255):
        command += ['t{}'.format(x), 'numeric', 'sortable']
    command = command[:-1]
    env.cmd(*command)
    for _ in env.reloading_iterator():
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
    env.cmd('ft.create', 'myIdx', 'schema',
             'title', 'TEXT', 'WEIGHT', '5.0',
             'body', 'TEXT',
             'url', 'TEXT')
    env.cmd('ft.add', 'myIdx', 'doc1', '1.0', 'FIELDS', 'title', 'hello world', 'bod', 'lorem ipsum', 'url', 'www.google.com')
    rv = env.cmd('ft.search', 'myIdx', 'no-as')
    env.assertEqual([0], rv)

def testIssue446(env):
    env.cmd('ft.create', 'myIdx', 'schema',
             'title', 'TEXT', 'SORTABLE')
    env.cmd('ft.add', 'myIdx', 'doc1', '1.0', 'fields', 'title', 'hello world', 'body', 'lorem ipsum', 'url', '"www.google.com')
    rv = env.cmd('ft.search', 'myIdx', 'hello', 'limit', '0', '0')
    env.assertEqual([1], rv)

    # Related - issue 635
    env.cmd('ft.add', 'myIdx', 'doc2', '1.0', 'fields', 'title', 'hello')
    rv = env.cmd('ft.search', 'myIdx', 'hello', 'limit', '0', '0')
    env.assertEqual([2], rv)


def testTimeoutSettings(env):
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text')
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'BLAHBLAH').raiseError()
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'RETURN').notRaiseError()
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'FAIL').notRaiseError()

def testAlias(env):
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text')
    env.cmd('ft.create', 'idx2', 'schema', 't1', 'text')

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
    env.cmd('ft.create', 'idx3', 'schema', 't1', 'text')
    env.cmd('ft.add', 'idx3', 'doc3', 1.0, 'fields', 't1', 'foo')
    env.cmd('ft.aliasAdd', 'myIndex', 'idx3')
    # also, check that this works in rdb save
    for _ in env.retry_with_rdb_reload():
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
    env.cmd('ft.create', 'idx', 'schema', 'f1', 'text')
    env.expect('ft.add', 'idx', 'schema', 'f1').raiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'nocreate', 'fields', 'f1', 'hello').raiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'nocreate', 'fields', 'f1', 'hello').raiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'fields', 'f1', 'hello').notRaiseError()
    env.expect('ft.add', 'idx', 'doc1', 1, 'replace', 'nocreate', 'fields', 'f1', 'world').notRaiseError()

def testSpellCheck(env):
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'report', 'TEXT')
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
    env.cmd('ft.create', 'productSearch1', 'noscoreidx', 'schema', 'productid',
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
    env.cmd('FT.CREATE', 'incidents', 'SCHEMA', 'report', 'TEXT')
    env.cmd('FT.ADD', 'incidents', 'doc1', 1.0, 'FIELDS', 'report', 'report content')
    env.cmd('FT.DICTADD', 'slang', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')
    rv = env.cmd('FT.SPELLCHECK', 'incidents', 'qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq',
        'TERMS', 'INCLUDE', 'slang', 'TERMS', 'EXCLUDE', 'slang')
    env.assertEqual("qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq", rv[0][1])
    env.assertEqual([], rv[0][2])

    env.expect('FT.SPELLCHECK', 'incidents', 'qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq',
        'TERMS', 'FAKE_COMMAND', 'slang').error()

def testIssue589(env):
    env.cmd('FT.CREATE', 'incidents', 'SCHEMA', 'report', 'TEXT')
    env.cmd('FT.ADD', 'incidents', 'doc1', 1.0, 'FIELDS', 'report', 'report content')
    env.expect('FT.SPELLCHECK', 'incidents', 'report :').error().contains("Syntax error at offset")

def testIssue621(env):
    env.expect('ft.create', 'test', 'SCHEMA', 'uuid', 'TAG', 'title', 'TEXT').equal('OK')
    env.expect('ft.add', 'test', 'a', '1', 'REPLACE', 'PARTIAL', 'FIELDS', 'uuid', 'foo', 'title', 'bar').equal('OK')
    env.expect('ft.add', 'test', 'a', '1', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'bar').equal('OK')
    env.expect('ft.search', 'test', '@uuid:{foo}').equal([1L, 'a', ['uuid', 'foo', 'title', 'bar']])

# Server crash on doc names that conflict with index keys #666
def testIssue666(env):
    # We cannot reliably determine that any error will occur in cluster mode
    # because of the key name
    env.skipOnCluster()

    env.cmd('ft.create', 'foo', 'schema', 'bar', 'text')
    env.cmd('ft.add', 'foo', 'mydoc', 1, 'fields', 'bar', 'one two three')

    # crashes here
    with env.assertResponseError():
        env.cmd('ft.add', 'foo', 'ft:foo/two', '1', 'fields', 'bar', 'four five six')
    # try with replace:
    with env.assertResponseError():
        env.cmd('ft.add', 'foo', 'ft:foo/two', '1', 'REPLACE',
            'FIELDS', 'bar', 'four five six')
    with env.assertResponseError():
        env.cmd('ft.add', 'foo', 'idx:foo', '1', 'REPLACE',
            'FIELDS', 'bar', 'four five six')

    env.cmd('ft.add', 'foo', 'mydoc1', 1, 'fields', 'bar', 'four five six')

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

    env.cmd('ft.create', 'idx', 'schema', 'txt1', 'text', 'tag1', 'tag')
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
        env.cmd('ft.debug', 'gc_forceinvoke', 'idx')
        r = env.cmd('ft.search', 'idx', '@txt1:term* @tag1:{tag*}')
        if r[0]:
            break

    print 'did {} iterations'.format(iters)
    r = env.cmd('ft.search', 'idx', '@txt1:term* @tag1:{tag*}')
    env.assertEqual([1, 'doc_XXX', ['txt1', 'termZZZ', 'tag1', 'tagZZZ']], r)


def testOptionalFilter(env):
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text')
    for x in range(100):
        env.cmd('ft.add', 'idx', 'doc_{}'.format(x), 1, 'fields', 't1', 'hello world word{}'.format(x))

    env.cmd('ft.explain', 'idx', '(~@t1:word20)')
    # print(r)

    r = env.cmd('ft.search', 'idx', '~(word20 => {$weight: 2.0})')


def testIssue736(env):
    # 1. create the schema, we need a tag field
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text', 'n2', 'numeric', 't2', 'tag')
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
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 't1', 'hello1 hey hello2')
    env.cmd('ft.add', 'idx', 'doc2', 1, 'fields', 't1', 'hello2 hey')
    env.cmd('ft.add', 'idx', 'doc3', 1, 'fields', 't1', 'hey')
    env.expect('ft.search', 'idx', '(hey hello1)|(hello2 hey)').equal([2L, 'doc1', ['t1', 'hello1 hey hello2'], 'doc2', ['t1', 'hello2 hey']])

def testIssue828(env):
    env.cmd('ft.create', 'beers', 'SCHEMA',
        'name', 'TEXT', 'PHONETIC', 'dm:en',
        'style', 'TAG', 'SORTABLE',
        'abv', 'NUMERIC', 'SORTABLE')
    rv = env.cmd("FT.ADD", "beers", "802", "1.0",
        "FIELDS", "index", "25", "abv", "0.049",
        "name", "Hell or High Watermelon Wheat (2009)",
        "style", "Fruit / Vegetable Beer")
    env.assertEqual('OK', rv)

def testIssue862(env):
    env.cmd('ft.create', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    rv = env.cmd("FT.ADD", "idx", "doc1", "1.0", "FIELDS", "test", "foo")
    env.assertEqual('OK', rv)
    env.cmd("FT.SEARCH", "idx", "foo", 'WITHSORTKEYS')
    env.assertTrue(env.isUp())

def testIssue_884(env):
    env.expect('FT.create', 'idx', 'STOPWORDS', '0', 'SCHEMA', 'title', 'text', 'weight',
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

def testIssue_866(env):
    env.expect('ft.sugadd', 'sug', 'test123', '1').equal(1)
    env.expect('ft.sugadd', 'sug', 'test456', '1').equal(2)
    env.expect('ft.sugdel', 'sug', 'test').equal(0)
    env.expect('ft.sugget', 'sug', '').equal(['test123', 'test456'])

def testIssue_848(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test1', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test1', 'foo').equal('OK')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'test2', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc2', '1.0', 'FIELDS', 'test1', 'foo', 'test2', 'bar').equal('OK')
    env.expect('FT.SEARCH', 'idx', 'foo', 'SORTBY', 'test2', 'ASC').equal([2L, 'doc1', ['test1', 'foo'], 'doc2', ['test2', 'bar', 'test1', 'foo']])

def testMod_309(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    for i in range(100000):
        env.expect('FT.ADD', 'idx', 'doc%d'%i, '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    res = env.cmd('FT.AGGREGATE', 'idx', 'foo')
    env.assertEqual(len(res), 100001)

def testIssue_865(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', '1', 'TEXT', 'SORTABLE').equal('OK')
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

    env.cmd('FT.CREATE idx2 SCHEMA ot1 TAG')
    env.cmd('FT.ADD idx2 doc2 1.0 FIELDS newf CAT ot1 4001')
    env.expect('FT.GET idx2 doc2').equal(["newf", "CAT", "ot1", "4001"])

    # NOADD is expected since 4001 is not < 4000, and no updates to the doc2 is expected as a result
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4000 FIELDS newf DOG ot1 4000', 'NOADD')
    env.expect('FT.GET idx2 doc2').equal(["newf", "CAT", "ot1", "4001"])

    # OK is expected since 4001 < 4002 and the doc2 is updated
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4002 FIELDS newf DOG ot1 4002').equal('OK')
    env.expect('FT.GET idx2 doc2').equal(["newf", "DOG", "ot1", "4002"])

    # OK is NOT expected since 4002 is not < 4002
    # We expect NOADD and doc2 update; however, we get OK and doc2 updated
    # After fix, @ot1 implicitly converted to a number, thus we expect NOADD
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4002 FIELDS newf FISH ot1 4002').equal('NOADD')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if to_number(@ot1)<4002 FIELDS newf FISH ot1 4002').equal('NOADD')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<to_str(4002) FIELDS newf FISH ot1 4002').equal('NOADD')
    env.expect('FT.GET idx2 doc2').equal(["newf", "DOG", "ot1", "4002"])

    # OK and doc2 update is expected since 4002 < 4003
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4003 FIELDS newf HORSE ot1 4003').equal('OK')
    env.expect('FT.GET idx2 doc2').equal(["newf", "HORSE", "ot1", "4003"])

    # Expect NOADD since 4003 is not > 4003
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1>4003 FIELDS newf COW ot1 4003').equal('NOADD')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if 4003<@ot1 FIELDS newf COW ot1 4003').equal('NOADD')

    # Expect OK and doc2 updated since 4003 > 4002
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1>4002 FIELDS newf PIG ot1 4002').equal('OK')
    env.expect('FT.GET idx2 doc2').equal(["newf", "PIG", "ot1", "4002"])

    # Syntax errors
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<4-002 FIELDS newf DOG ot1 4002').contains('Syntax error')
    env.expect('FT.ADD idx2 doc2 1.0 REPLACE PARTIAL if @ot1<to_number(4-002) FIELDS newf DOG ot1 4002').contains('Syntax error')

def testUnknownSymbolErrorOnConditionalAdd(env):
    env.expect('FT.CREATE idx SCHEMA f1 TAG f2 NUMERIC NOINDEX f3 TAG NOINDEX').ok()
    env.expect('ft.add idx doc1 1.0 REPLACE PARTIAL IF @f1<awfwaf FIELDS f1 foo f2 1 f3 boo').ok()
    env.expect('ft.add idx doc1 1.0 REPLACE PARTIAL IF @f1<awfwaf FIELDS f1 foo f2 1 f3 boo').error()

def testDelIndexExternally(env):
    env.skipOnCluster() # todo: remove once fix on coordinator
    env.expect('FT.CREATE idx SCHEMA num NUMERIC t TAG g GEO').equal('OK')
    env.expect('ft.add idx doc1 1.0 FIELDS num 3 t my_tag g', "1,1").equal('OK')
    
    env.expect('set nm:idx/num 1').equal('OK')
    env.expect('ft.add idx doc2 1.0 FIELDS num 3').equal('Could not open numeric index for indexing')

    env.expect('set tag:idx/t 1').equal('OK')
    env.expect('ft.add idx doc3 1.0 FIELDS t 3').equal('Could not open tag index for indexing')

    env.expect('set geo:idx/g 1').equal('OK')
    env.expect('ft.add idx doc4 1.0 FIELDS g "1,1"').equal('Could not index geo value')

def testWrongResultsReturnedBySkipOptimization(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'f1', 'TEXT', 'f2', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'f1', 'foo', 'f2', 'bar').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'f1', 'moo', 'f2', 'foo').equal('OK')
    env.expect('ft.search', 'idx', 'foo @f2:moo').debugPrint().equal([0L])

def testErrorWithApply(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo bar').equal('OK')
    err = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'split()')[1]
    env.assertEqual(str(err[0]), 'Invalid number of arguments for split')

def testSummerizeWithAggregateRaiseError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', 'foo2', 'SUMMARIZE', 'FIELDS', '1', 'test',
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0').error()

def testSummerizeHighlightParseError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.search', 'idx', 'foo2', 'SUMMARIZE', 'FIELDS', 'WITHSCORES').error()
    env.expect('ft.search', 'idx', 'foo2', 'HIGHLIGHT', 'FIELDS', 'WITHSCORES').error()

def testCursorBadArgument(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', '*',
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0',
               'WITHCURSOR', 'COUNT', 'BAD').error()

def testLimitBadArgument(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.search', 'idx', '*', 'LIMIT', '1').error()

def testOnTimeoutBadArgument(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'bad').error()

def testAggregateSortByWrongArgument(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', 'foo2').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', 'bad').error()

def testAggregateSortByMaxNumberOfFields(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
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
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', 'bad', '2').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', '0', 'bad').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', '0').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', 'bad').error()
    env.expect('ft.search', 'idx', '*', 'FILTER', 'test', '0', '2', 'FILTER', 'test', '0', 'bla').error()

def testGeoFilterError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', 'bad' , '2', '3', 'km').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1' , 'bad', '3', 'km').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1' , '2', 'bad', 'km').error()
    env.expect('ft.search', 'idx', '*', 'GEOFILTER', 'test', '1' , '2', '3', 'bad').error()

def testReducerError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', 'bad').error()
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'as').error()

def testGroupbyError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE').error()
    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test1').error()
    env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'bad', '0').error()
    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '*', 'GROUPBY', '1', '@test', 'REDUCE', 'SUM', '1', '@test1').error()

def testGroupbyWithSort(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'test', '1').equal('OK')
    env.expect('ft.add', 'idx', 'doc3', '1.0', 'FIELDS', 'test', '2').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', '2', '@test', 'ASC', 
               'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'as', 'count').equal([2L, ['test', '2', 'count', '1'], ['test', '1', 'count', '2']])

def testApplyError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'APPLY', 'split(@test)', 'as').error()

def testLoadError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', 'bad').error()
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', 'bad', 'test').error()
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '2', 'test').error()
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '2', '@test').error()

def testMissingArgsError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx').error()

def testUnexistsScorer(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'SCORER', 'bad').error()

def testHighlightWithUnknowsProperty(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'HIGHLIGHT', 'FIELDS', '1', 'test1').error()

def testBadFilterExpression(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'FILTER', 'blabla').error()
    if not env.isCluster(): # todo: remove once fix on coordinator
        env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'FILTER', '@test1 > 1').error()

def testWithSortKeysOnNoneSortableValue(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'WITHSORTKEYS', 'SORTBY', 'test').equal([1L, 'doc1', '$foo', ['test', 'foo']])

def testWithWithRawIds(env):
    env.skipOnCluster() # todo: remove once fix on coordinator
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.search', 'idx', '*', 'WITHRAWIDS').equal([1L, 'doc1', 1L, ['test', 'foo']])

def testUnkownIndex(env):
    env.skipOnCluster() # todo: remove once fix on coordinator
    env.expect('ft.aggregate').error()
    env.expect('ft.aggregate', 'idx', '*').error()
    env.expect('ft.aggregate', 'idx', '*', 'WITHCURSOR').error()

def testExplainError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('FT.EXPLAIN', 'idx', '(').error()

def testBadCursor(env):
    env.expect('FT.CURSOR', 'READ', 'idx').error()
    env.expect('FT.CURSOR', 'READ', 'idx', '1111').error()
    env.expect('FT.CURSOR', 'READ', 'idx', 'bad').error()
    env.expect('FT.CURSOR', 'DROP', 'idx', '1111').error()
    env.expect('FT.CURSOR', 'bad', 'idx', '1111').error()

def testGroupByWithApplyError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    err = env.cmd('FT.AGGREGATE', 'idx', '*', 'APPLY', 'split()', 'GROUPBY', '1', '@test', 'REDUCE', 'COUNT', '0', 'AS', 'count')[1]
    assertEqualIgnoreCluster(env, str(err[0]), 'Invalid number of arguments for split')

def testSubStrErrors(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
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
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
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
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo').equal('OK')
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a').equal([1L, ['test', 'foo', 'a', None]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms()', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms(100)', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms(-100)', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])
    env.expect('ft.aggregate', 'idx', 'foo', 'LOAD', '1', '@test', 'APPLY', 'matched_terms("test")', 'as', 'a').equal([1L, ['test', 'foo', 'a', ['foo']]])

def testStrFormatError(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'TEXT').equal('OK')
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
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
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
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear(@test)', 'as', 'a').equal([1L, ['test', '12234556', 'a', '4']])

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear(@test, 112)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'monthofyear("bad")', 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

def testParseTimeErrors(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'parse_time()', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'parse_time(11)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    err = env.cmd('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'parse_time(11,22)', 'as', 'a')[1]
    assertEqualIgnoreCluster(env, type(err[0]), redis.exceptions.ResponseError)

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'parse_time("%s", "%s")' % ('d' * 2048, 'd' * 2048), 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'parse_time("test", "%s")' % ('d' * 2048), 'as', 'a').equal([1L, ['test', '12234556', 'a', None]])

def testMathFunctions(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12234556').equal('OK')

    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'exp(@test)', 'as', 'a').equal([1L, ['test', '12234556', 'a', 'inf']])
    env.expect('ft.aggregate', 'idx', '@test:[0..inf]', 'LOAD', '1', '@test', 'APPLY', 'ceil(@test)', 'as', 'a').equal([1L, ['test', '12234556', 'a', '12234556']])

def testErrorOnOpperation(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'test', 'NUMERIC').equal('OK')
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
    env.cmd('ft.create', 'idx', 'schema', 'test', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 'test', 'foo')
    rv = env.cmd('ft.aggregate', 'idx', 'foo', 'withsortkeys',
        'load', '1', '@test',
        'sortby', '1', '@test')
    env.assertEqual([1, '$foo', ['test', 'foo']], rv)


def testIssue919(env):
    # This only works if the missing field has a lower sortable index
    # than the present field..
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text', 'sortable', 'n1', 'numeric', 'sortable')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 'n1', 42)
    rv = env.cmd('ft.search', 'idx', '*', 'sortby', 't1', 'desc')
    env.assertEqual([1L, 'doc1', ['n1', '42']], rv)


def testIssue1074(env):
    # Ensure that sortable fields are returned in their string form from the
    # document
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text', 'n1', 'numeric', 'sortable')
    env.cmd('ft.add', 'idx', 'doc1', 1, 'fields', 't1', 'hello', 'n1', 1581011976800)
    rv = env.cmd('ft.search', 'idx', '*', 'sortby', 'n1')
    env.assertEqual([1L, 'doc1', ['n1', '1581011976800', 't1', 'hello']], rv)

def testIssue1085(env):
    env.skipOnCluster()
    env.cmd('FT.CREATE issue1085 SCHEMA foo TEXT SORTABLE bar NUMERIC SORTABLE')
    for i in range(1, 10):
        env.cmd('FT.ADD issue1085 document_%d 1 REPLACE FIELDS foo foo%d bar %d' % (i, i, i))
    env.expect('FT.SEARCH', 'issue1085', '@bar:[8 8]').equal([1L, 'document_8', ['foo', 'foo8', 'bar', '8']])

    for i in range(1, 10):
        env.cmd('FT.ADD issue1085 document_8 1 REPLACE FIELDS foo foo8 bar 8')

    env.expect('ft.debug GC_FORCEINVOKE issue1085').equal('DONE')

    env.expect('FT.SEARCH', 'issue1085', '@bar:[8 8]').equal([1, 'document_8', ['foo', 'foo8', 'bar', '8']])


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}

def testOptimize(env):
    env.skipOnCluster()
    env.cmd('ft.create', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.cmd('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo')   
    env.assertEqual(0, env.cmd('FT.OPTIMIZE', 'idx'))   
    with env.assertResponseError():
        env.assertOk(env.cmd('FT.OPTIMIZE', 'idx', '666'))   
    env.expect('FT.OPTIMIZE', 'fake_idx').error()

def testInfoError(env):
    env.expect('ft.info', 'no_idx').error()

def testSetPayload(env):
    env.skipOnCluster()
    env.expect('flushall')
    env.expect('ft.create idx schema name text').equal('OK')
    env.expect('ft.add idx hotel 1.0 fields name hilton').equal('OK')
    env.expect('FT.SETPAYLOAD idx hotel payload').equal('OK')
    env.expect('FT.SETPAYLOAD idx hotel payload').equal('OK')
    env.expect('FT.SETPAYLOAD idx fake_hotel').error()          \
            .contains("wrong number of arguments for 'FT.SETPAYLOAD' command")
    env.expect('FT.SETPAYLOAD fake_idx hotel payload').error().contains('Unknown Index name')    
    env.expect('FT.SETPAYLOAD idx fake_hotel payload').error().contains('Document not in index')    

def testIndexNotRemovedFromCursorListAfterRecreated(env):
    env.expect('FT.CREATE idx SCHEMA f1 TEXT').ok()
    env.expect('FT.AGGREGATE idx * WITHCURSOR').equal([[0], 0])
    env.expect('FT.CREATE idx SCHEMA f1 TEXT').error()
    env.expect('FT.AGGREGATE idx * WITHCURSOR').equal([[0], 0])

def testSearchNotExistsTagValue(env):
    # this test basically make sure we are not leaking
    env.expect('FT.CREATE idx SCHEMA t TAG SORTABLE').ok()
    env.expect('FT.SEARCH idx @t:{val}').equal([0])

def testUnseportedSortableTypeErrorOnTags(env):
    env.expect('FT.CREATE idx SCHEMA f1 TEXT SORTABLE f2 NUMERIC SORTABLE NOINDEX f3 TAG SORTABLE NOINDEX f4 TEXT SORTABLE NOINDEX').ok()
    env.expect('FT.ADD idx doc1 1.0 FIELDS f1 foo1 f2 1 f3 foo1 f4 foo1').ok()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL FIELDS f2 2 f3 foo2 f4 foo2').ok()
    env.expect('HGETALL doc1').equal(['f1', 'foo1', 'f2', '2', 'f3', 'foo2', 'f4', 'foo2'])
    env.expect('FT.SEARCH idx *').equal([1L, 'doc1', ['f1', 'foo1', 'f2', '2', 'f3', 'foo2', 'f4', 'foo2']])


def testIssue1158(env):
    env.cmd('FT.CREATE idx SCHEMA txt1 TEXT txt2 TEXT txt3 TEXT')

    env.cmd('FT.ADD idx doc1 1.0 FIELDS txt1 10 txt2 num1')
    env.expect('FT.GET idx doc1').equal(['txt1', '10', 'txt2', 'num1'])

    # only 1st checked (2nd returns an error)
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt1||to_number(@txt2)<5 FIELDS txt1 5').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt3&&to_number(@txt2)<5 FIELDS txt1 5').equal('NOADD')
    
    # both are checked
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11||to_number(@txt1)>42 FIELDS txt2 num2').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11||to_number(@txt1)<42 FIELDS txt2 num2').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11&&to_number(@txt1)>42 FIELDS txt2 num2').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt1)>11&&to_number(@txt1)<42 FIELDS txt2 num2').equal('NOADD')
    env.expect('FT.GET idx doc1').equal(['txt1', '5', 'txt2', 'num2'])