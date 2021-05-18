# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep
from RLTest import Env



def testSuggestions(env):
    r = env
    r.expect('ft.SUGADD', 'ac', 'hello world', 1).equal(1)
    r.expect('ft.SUGADD', 'ac', 'hello world', 1, 'INCR').equal(1)

    res = r.execute_command('FT.SUGGET', 'ac', 'hello')
    env.assertEqual(1, len(res))
    env.assertEqual('hello world', res[0])

    terms = ['hello werld', 'hallo world',
             'yellow world', 'wazzup', 'herp', 'derp']
    sz = 2
    for term in terms:
        r.expect('ft.SUGADD', 'ac', term, sz - 1).equal(sz)
        sz += 1

    for _ in r.retry_with_rdb_reload():
        r.expect('ft.SUGLEN', 'ac').equal(7)

        # search not fuzzy
        r.expect('ft.SUGGET', 'ac', 'hello').equal(['hello world', 'hello werld'])

        # print  r.execute_command('ft.SUGGET', 'ac', 'hello', 'FUZZY', 'MAX', '1', 'WITHSCORES')
        # search fuzzy - shuold yield more results
        r.expect('ft.SUGGET', 'ac', 'hello', 'FUZZY')\
         .equal(['hello world', 'hello werld', 'yellow world', 'hallo world'])

        # search fuzzy with limit of 1
        r.expect('ft.SUGGET', 'ac', 'hello', 'FUZZY', 'MAX', '1').equal(['hello world'])

        # scores should return on WITHSCORES
        res = r.execute_command('ft.SUGGET', 'ac', 'hello', 'WITHSCORES')
        env.assertEqual(4, len(res))
        env.assertTrue(float(res[1]) > 0)
        env.assertTrue(float(res[3]) > 0)

    r.expect('ft.SUGDEL', 'ac', 'hello world').equal(1L)
    r.expect('ft.SUGDEL', 'ac', 'world').equal(0L)

    r.expect('ft.SUGGET', 'ac', 'hello').equal(['hello werld'])

def testSuggestErrors(env):
    env.expect('ft.SUGADD ac olah 1').equal(1)
    env.expect('ft.SUGADD ac olah 1 INCR').equal(1)
    env.expect('ft.SUGADD ac missing').error().contains('wrong number of arguments')
    env.expect('ft.SUGADD ac olah not_a_number').error().contains('invalid score')
    env.expect('ft.SUGADD ac olah 1 PAYLOAD').error().contains('Invalid payload: Expected an argument, but none provided')
    env.expect('ft.SUGADD ac olah 1 REDIS PAYLOAD payload').error().contains('Unknown argument `REDIS`')
    env.expect('ft.SUGGET ac olah FUZZ').error().contains('Unrecognized argument: FUZZ')
    query = 'verylongquery'
    for _ in range(3):
        query += query
    env.expect('ft.SUGGET ac', query).error().contains('Invalid query')
    env.expect('ft.SUGGET ac', query + query).error().contains('Invalid query length')

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

    res = r.execute_command('FT.SUGGET', 'ac', 'hello', 'WITHPAYLOADS')
    env.assertListEqual(['hello world', 'foo', 'hello werld', 'bar', 'hello nopayload', None, 'hello nopayload2', None],
                         res)
    res = r.execute_command('FT.SUGGET', 'ac', 'hello')
    env.assertListEqual(['hello world',  'hello werld', 'hello nopayload', 'hello nopayload2'],
                         res)
    res = r.execute_command(
        'FT.SUGGET', 'ac', 'hello', 'WITHPAYLOADS', 'WITHSCORES')
    # we don't compare the scores beause they may change
    env.assertEqual(12, len(res))

def testIssue_866(env):
    env.expect('ft.sugadd', 'sug', 'test123', '1').equal(1)
    env.expect('ft.sugadd', 'sug', 'test456', '1').equal(2)
    env.expect('ft.sugdel', 'sug', 'test').equal(0)
    env.expect('ft.sugget', 'sug', '').equal(['test123', 'test456'])

def testSuggestMax(env):
  for i in range(10):
    env.expect('ft.sugadd', 'sug', 'test%d' % i, i + 1).equal(i + 1)
  #  for j in range(i + 1):
  #env.expect('ft.sugadd', 'sug', 'test10', '1', 'INCR').equal(i + 1)

  expected_res = ['test9', '7.0710678100585938', 'test8', '6.3639612197875977', 'test7', '5.6568541526794434',
                  'test6', '4.9497475624084473', 'test5', '4.242640495300293', 'test4', '3.5355339050292969',
                  'test3', '2.8284270763397217', 'test2', '2.1213202476501465', 'test1', '1.4142135381698608',
                  'test0', '0.70710676908493042']
  for i in range(1,11):
    env.expect('FT.SUGGET', 'sug', 'test', 'MAX', i, 'WITHSCORES').equal(expected_res[0:i*2])
  env.expect('FT.SUGGET', 'sug', 'test', 'MAX', 10, 'WITHSCORES').equal(expected_res)

def testSuggestMax2(env):
  for i in range(10):
    env.expect('ft.sugadd', 'sug', 'test %d' % i, 1).equal(i + 1)

  expected_res = ['test 0', 'test 1', 'test 2', 'test 3', 'test 4', 'test 5']
  for i in range(1,7):
    res = env.cmd('FT.SUGGET', 'sug', 'test ', 'MAX', i)
    for item in res:
        env.assertIn(item, expected_res[0:i])

def testIssue_490(env):
    env.expect('ft.sugadd', 'sug', 'RediSearch', '1', 'PAYLOAD', 'RediSearch, an awesome search engine').equal(1)
    env.expect('ft.sugget', 'sug', 'Redis', 'WITHPAYLOADS').equal(['RediSearch', 'RediSearch, an awesome search engine'])
    env.expect('ft.sugadd', 'sug', 'RediSearch', '1', 'INCR').equal(1)
    env.expect('ft.sugget', 'sug', 'Redis', 'WITHPAYLOADS').equal(['RediSearch', 'RediSearch, an awesome search engine'])
    env.expect('ft.sugadd', 'sug', 'RediSearch', '1', 'INCR', 'PAYLOAD', 'RediSearch 2.0, next gen search engine').equal(1)
    env.expect('ft.sugget', 'sug', 'Redis', 'WITHPAYLOADS').equal(['RediSearch', 'RediSearch 2.0, next gen search engine'])
