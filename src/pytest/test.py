# -*- coding: utf-8 -*-

import redis
import unittest
from hotels import hotels
import random
import time


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
    # Regression test for https://github.com/RedisLabsModules/RediSearch/issues/306
    r = env
    N = 100
    env.assertOk(r.execute_command(
        "ft.create", "test", "SCHEMA",  "tags", "TAG", "waypoint", "GEO"))
    env.assertOk(r.execute_command(
        "ft.add", "test", "1", "1", "FIELDS", "tags", "alberta", "waypoint", "-113.524,53.5244"))
    env.assertOk(r.execute_command(
        "ft.add", "test", "2", "1", "FIELDS", "tags", "ontario", "waypoint", "-79.395,43.661667"))
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

def testDelete(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'f', 'text'))

    for i in range(100):
        env.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                        'f', 'hello world'))

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

def testCustomStopwords(env):
    r = env
    # Index with default stopwords
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text'))

    # Index with custom stopwords
    env.assertOk(r.execute_command('ft.create', 'idx2', 'stopwords', 2, 'hello', 'world',
                                    'schema', 'foo', 'text'))
    # Index with NO stopwords
    env.assertOk(r.execute_command('ft.create', 'idx3', 'stopwords', 0,
                                    'schema', 'foo', 'text'))

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

def testExplain(env):

    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
    q = '(hello world) "what what" hello|world @bar:[10 100]|@bar:[200 300]'
    res = r.execute_command('ft.explain', 'idx', q)
    # print res.replace('\n', '\\n')
    # expected = """INTERSECT {\n  UNION {\n    hello\n    +hello(expanded)\n  }\n  UNION {\n    world\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    expected = """INTERSECT {\n  UNION {\n    hello\n    <HL(expanded)\n    +hello(expanded)\n  }\n  UNION {\n    world\n    <ARLT(expanded)\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      <HL(expanded)\n      +hello(expanded)\n    }\n    UNION {\n      world\n      <ARLT(expanded)\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
    env.assertEqual(res, expected)

    expected = ['INTERSECT {', '  UNION {', '    hello', '    <HL(expanded)', '    +hello(expanded)', '  }', '  UNION {', '    world', '    <ARLT(expanded)', '    +world(expanded)', '  }', '  EXACT {', '    what', '    what', '  }', '  UNION {', '    UNION {', '      hello', '      <HL(expanded)', '      +hello(expanded)', '    }', '    UNION {', '      world', '      <ARLT(expanded)', '      +world(expanded)', '    }', '  }', '  UNION {', '    NUMERIC {10.000000 <= @bar <= 100.000000}', '    NUMERIC {200.000000 <= @bar <= 300.000000}', '  }', '}', '']
    if env.is_cluster():
        raise unittest.SkipTest()
    res = env.cmd('ft.explainCli', 'idx', q)
    env.assertEqual(expected, res)

def testNoIndex(env):
    r = env
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema',
        'foo', 'text',
        'num', 'numeric', 'sortable', 'noindex',
        'extra', 'text', 'noindex', 'sortable'))
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
    res = r.execute_command(
        'ft.search', 'idx', 'hello world', 'sortby', 'num', 'desc',)
    env.assertListEqual([2L, 'doc1', ['foo', 'hello world', 'num', '3', 'extra', 'jorem gipsum'],
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

    res = r.execute_command('ft.search', 'idx', "hello", "nocontent")
    env.assertEqual(3, len(res))
    env.assertEqual(2, res[0])
    env.assertEqual("doc1", res[2])
    env.assertEqual("doc2", res[1])

    res = r.execute_command(
        'ft.search', 'idx', "hello", "filter", "price", "0", "3")
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
        'ft.search', 'idx', 'kitties', "nocontent", "expander", "SBSTEM")
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
    env.assertCmdOk('ft.create', 'idx', 'schema', 'f1',
                     'text', 'f2', 'text', 'n1', 'numeric', 'f3', 'text')
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

    # Test when field is not found
    res = env.cmd('ft.search', 'idx', 'val*', 'return', 1, 'nonexist')
    env.assertEqual(21, len(res))
    env.assertEqual(10, res[0])
    for pair in grouper(res[1:], 2):
        _, pair = pair
        env.assertEqual(None, pair[1])

    # Test that we don't crash if we're given the wrong number of fields
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
    res = env.cmd('ft.search', 'idx', "value for 3")
    if not has_offsets:
        env.assertIsNone(res)
    else:
        env.assertIsNotNone(res)

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
        for combo in combinations(('NOOFSETS', 'NOFREQS', 'NOFIELDS', ''), x):
            _test_create_options_real(env, *combo)

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
    env.cmd('ft.add', 'idx', 'Hello\x00World',
             1.0, 'fields', 'txt', 'Bin match')
    for _ in env.reloading_iterator():
        res = env.cmd('ft.search', 'idx', 'match')
        env.assertEqual(res, [2L, 'Hello\x00World', [
                         'txt', 'Bin match'], 'Hello', ['txt', 'NoBin match']])

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
    env.assertEqual([1L, 'doc1', None, ['lastName', 'mark']], res)

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
        env.assertEqual(
            [12, 'doc12', ['f1', 'hello', 'f3', 'val9'], 'doc11', ['f1', 'hello', 'f3', 'val8'], 'doc10', ['f1', 'hello', 'f3', 'val7'], 'doc9', ['f1', 'hello', 'f3', 'val6'], 'doc8', ['f1', 'hello', 'f3', 'val5'], 'doc7', [
                'f1', 'hello', 'f3', 'val4'], 'doc6', ['f1', 'hello', 'f3', 'val3'], 'doc5', ['f1', 'hello', 'f3', 'val2'], 'doc4', ['f1', 'hello', 'f3', 'val1'], 'doc3', ['f1', 'hello', 'f3', 'val0']],
            res)

    # Test that we can add a numeric field
    env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n1', 'NUMERIC')
    env.cmd('FT.ADD', 'idx', 'docN1', 1.0, 'FIELDS', 'n1', 50)
    env.cmd('FT.ADD', 'idx', 'docN2', 1.0, 'FIELDS', 'n1', 250)
    for _ in env.retry_with_reload():
        res = env.cmd('FT.SEARCH', 'idx', '@n1:[0 100]')
        env.assertEqual([1, 'docN1', ['n1', '50']], res)

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
            env.assertTrue('already in index' in e.message.lower())

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

def testTimeoutSettings(env):
    env.cmd('ft.create', 'idx', 'schema', 't1', 'text')
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'BLAHBLAH').raiseError()
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'RETURN').notRaiseError()
    env.expect('ft.search', 'idx', '*', 'ON_TIMEOUT', 'FAIL').notRaiseError()

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
        'REDUCE', 'COUNT', '0', 'as', 'value_count')
    expected = [6, ['value', 'white', 'value_count', '2'], ['value', 'cars', 'value_count', '2'], ['value', 'small cars', 'value_count', '1'], ['value', 'blue', 'value_count', '2'], ['value', 'Big cars', 'value_count', '2'], ['value', 'green', 'value_count', '1']]
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

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}
