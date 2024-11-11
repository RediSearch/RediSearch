# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import *

def testSuggestions(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    res = conn.execute_command('ft.SUGADD', 'ac', 'hello world', 1)
    env.assertEqual(res, 1)

    res = conn.execute_command('ft.SUGADD', 'ac', 'hello world', 1, 'INCR')
    env.assertEqual(res, 1)

    res = conn.execute_command('FT.SUGGET', 'ac', 'hello')
    env.assertEqual(1, len(res))
    env.assertEqual('hello world', res[0])

    terms = ['hello werld', 'hallo world',
             'yellow world', 'wazzup', 'herp', 'derp']
    sz = 2
    for term in terms:
        res = conn.execute_command('ft.SUGADD', 'ac', term, sz - 1)
        env.assertEqual(res, sz)
        sz += 1

    for _ in env.reloadingIterator():
        res = conn.execute_command('ft.SUGLEN', 'ac')
        env.assertEqual(res, 7)

        # search not fuzzy
        res = conn.execute_command('ft.SUGGET', 'ac', 'hello')
        env.assertEqual(res, ['hello world', 'hello werld'])

        # print  env.cmd('ft.SUGGET', 'ac', 'hello', 'FUZZY', 'MAX', '1', 'WITHSCORES')
        # search fuzzy - shuold yield more results
        res = conn.execute_command('ft.SUGGET', 'ac', 'hello', 'FUZZY')
        env.assertEqual(res, ['hello world', 'hello werld', 'yellow world', 'hallo world'])

        # search fuzzy with limit of 1
        res = conn.execute_command('ft.SUGGET', 'ac', 'hello', 'FUZZY', 'MAX', '1')
        env.assertEqual(res, ['hello world'])

        # scores should return on WITHSCORES
        res = conn.execute_command('ft.SUGGET', 'ac', 'hello', 'WITHSCORES')
        env.assertEqual(4, len(res))
        env.assertTrue(float(res[1]) > 0)
        env.assertTrue(float(res[3]) > 0)

    res = conn.execute_command('ft.SUGDEL', 'ac', 'hello world')
    env.assertEqual(res, 1)

    res = conn.execute_command('ft.SUGDEL', 'ac', 'world')
    env.assertEqual(res, 0)

    res = conn.execute_command('ft.SUGGET', 'ac', 'hello')
    env.assertEqual(res, ['hello werld'])

def testSuggestErrors(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    res = conn.execute_command('ft.SUGADD', 'ac', 'olah', '1')
    env.assertEqual(res, 1)

    res = conn.execute_command('ft.SUGADD', 'ac', 'olah', '1', 'INCR')
    env.assertEqual(res, 1)

    try:
        conn.execute_command('ft.SUGADD', 'ac', 'missing')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains('wrong number of arguments', str(e))
    try:
        conn.execute_command('ft.SUGADD', 'ac', 'olah', 'not_a_number')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains('invalid score', str(e))
    try:
        conn.execute_command('ft.SUGADD', 'ac', 'olah', '1', 'PAYLOAD')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains('Invalid payload: Expected an argument, but none provided', str(e))
    try:
        conn.execute_command('ft.SUGADD', 'ac', 'olah', '1', 'REDIS', 'PAYLOAD', 'payload')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains('Unknown argument `REDIS`', str(e))
    try:
        conn.execute_command('ft.SUGGET', 'ac', 'olah', 'FUZZ')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains('Unrecognized argument: FUZZ', str(e))

    query = 'verylongquery'
    for _ in range(3):
        query += query
    try:
        conn.execute_command('ft.SUGGET', 'ac', query)
        env.assertTrue(False)
    except Exception as e:
        env.assertContains(str(e), 'Invalid query')
    try:
        conn.execute_command('ft.SUGGET', 'ac', query + query)
        env.assertTrue(False)
    except Exception as e:
        env.assertContains(str(e), 'Invalid query length')

def testSuggestPayload(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    env.assertEqual(1, conn.execute_command(
        'ft.SUGADD', 'ac', 'hello world', 1, 'PAYLOAD', 'foo'))
    env.assertEqual(2, conn.execute_command(
        'ft.SUGADD', 'ac', 'hello werld', 1, 'PAYLOAD', 'bar'))
    env.assertEqual(3, conn.execute_command(
        'ft.SUGADD', 'ac', 'hello nopayload', 1, 'PAYLOAD', ''))
    env.assertEqual(4, conn.execute_command(
        'ft.SUGADD', 'ac', 'hello nopayload2', 1))

    res = conn.execute_command('FT.SUGGET', 'ac', 'hello', 'WITHPAYLOADS')
    env.assertEqual(['hello world', 'foo', 'hello werld', 'bar', 'hello nopayload', None, 'hello nopayload2', None],
                         res)
    res = conn.execute_command('FT.SUGGET', 'ac', 'hello')
    env.assertEqual(['hello world',  'hello werld', 'hello nopayload', 'hello nopayload2'],
                         res)
    res = conn.execute_command(
        'FT.SUGGET', 'ac', 'hello', 'WITHPAYLOADS', 'WITHSCORES')
    # we don't compare the scores beause they may change
    env.assertEqual(12, len(res))

def testIssue_866(env):
    skipOnCrdtEnv(env)
    with env.getClusterConnectionIfNeeded() as conn:
        res = conn.execute_command('ft.sugadd', 'sug', 'test123', '1')
        env.assertEqual(res, 1)
        res = conn.execute_command('ft.sugadd', 'sug', 'test456', '1')
        env.assertEqual(res, 2)
        res = conn.execute_command('ft.sugdel', 'sug', 'test')
        env.assertEqual(res, 0)
        res = conn.execute_command('ft.sugget', 'sug', '')
        env.assertEqual(res, ['test123', 'test456'])

def testSuggestMax(env):
    #skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()
    for i in range(10):
        res = conn.execute_command('ft.sugadd', 'sug', 'test%d' % i, i + 1)
        env.assertEqual(res, i + 1)
        #  for j in range(i + 1):
        #env.expect('ft.sugadd', 'sug', 'test10', '1', 'INCR').equal(i + 1)

    expected_res = ['test9', '7.0710678100585938', 'test8', '6.3639612197875977', 'test7', '5.6568541526794434',
                  'test6', '4.9497475624084473', 'test5', '4.242640495300293', 'test4', '3.5355339050292969',
                  'test3', '2.8284270763397217', 'test2', '2.1213202476501465', 'test1', '1.4142135381698608',
                  'test0', '0.70710676908493042']
    for i in range(1,11):
        res = conn.execute_command('FT.SUGGET', 'sug', 'test', 'MAX', i, 'WITHSCORES')
        compare_lists(env, res, expected_res[0:i*2], delta=0.0001)
    res = conn.execute_command('FT.SUGGET', 'sug', 'test', 'MAX', 10, 'WITHSCORES')
    compare_lists(env, res, expected_res, delta=0.0001)

def testSuggestMax2(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    for i in range(10):
        res = conn.execute_command('ft.sugadd', 'sug', 'test %d' % i, 10 - i)
        env.assertEqual(res, i + 1)

    expected_res = ['test 0', 'test 1', 'test 2', 'test 3', 'test 4', 'test 5']
    for i in range(1,7):
        res = conn.execute_command('FT.SUGGET', 'sug', 'test ', 'MAX', i)
        for item in res:
            env.assertContains(item, expected_res[0:i])

def testIssue_490(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    res = conn.execute_command('ft.sugadd', 'sug', 'RediSearch', '1', 'PAYLOAD', 'RediSearch, an awesome search engine')
    env.assertEqual(res, 1)
    res = conn.execute_command('ft.sugget', 'sug', 'Redis', 'WITHPAYLOADS')
    env.assertEqual(res, ['RediSearch', 'RediSearch, an awesome search engine'])
    res = conn.execute_command('ft.sugadd', 'sug', 'RediSearch', '1', 'INCR')
    env.assertEqual(res, 1)
    res = conn.execute_command('ft.sugget', 'sug', 'Redis', 'WITHPAYLOADS')
    env.assertEqual(res, ['RediSearch', 'RediSearch, an awesome search engine'])
    res = conn.execute_command('ft.sugadd', 'sug', 'RediSearch', '1', 'INCR', 'PAYLOAD', 'RediSearch 2.0, next gen search engine')
    env.assertEqual(res, 1)
    res = conn.execute_command('ft.sugget', 'sug', 'Redis', 'WITHPAYLOADS')
    env.assertEqual(res, ['RediSearch', 'RediSearch 2.0, next gen search engine'])
