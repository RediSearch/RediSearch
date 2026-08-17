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
        # search fuzzy - should yield more results
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
        env.assertContains(str(e), 'SEARCH_SUGGEST_INVALID_QUERY Invalid query')
    try:
        conn.execute_command('ft.SUGGET', 'ac', query + query)
        env.assertTrue(False)
    except Exception as e:
        env.assertContains(str(e), 'SEARCH_SUGGEST_QUERY_TOO_LONG Invalid query length')

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
    # we don't compare the scores because they may change
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

def testUnexistentSuggestionDict(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    # Test on unexistent suggestion dictionary
    res = conn.execute_command('exists', 'unexistent_sug')
    env.assertEqual(res, 0)
    res = conn.execute_command('ft.suglen', 'unexistent_sug')
    env.assertEqual(res, 0)
    res = conn.execute_command('ft.sugget', 'unexistent_sug', 'hello')
    env.assertEqual(res, [])
    res = conn.execute_command('ft.sugdel', 'unexistent_sug', 'hello')
    env.assertEqual(res, 0)

def testEmptySuggestionDict(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    res = conn.execute_command('ft.sugadd', 'sug', 'hello world', '1')
    env.assertEqual(res, 1)
    res = conn.execute_command('ft.sugdel', 'sug', 'hello world')
    env.assertEqual(res, 1)
    # The key is deleted when the suggestion dict is emptied
    res = conn.execute_command('exists', 'sug')
    env.assertEqual(res, 0)
    res = conn.execute_command('ft.suglen', 'sug')
    env.assertEqual(res, 0)
    res = conn.execute_command('ft.sugget', 'sug', 'hello')
    env.assertEqual(res, [])
    res = conn.execute_command('ft.sugdel', 'sug', 'hello world')
    env.assertEqual(res, 0)

def testSuggestTrimDropsTail(env):
    # Regression for MOD-15932: FT.SUGGET ... TRIM used to double-free when the
    # trim post-pass dropped tail entries. The first loop left `h` pointing at
    # the last surviving entry; the second loop then re-freed it once per
    # dropped entry. Reliable crash with one score-dominant entry and several
    # entries below the SCORE_TRIM_FACTOR (10x) cutoff.
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    env.assertEqual(1, conn.execute_command('FT.SUGADD', 'sug', 'ha', '100'))
    env.assertEqual(2, conn.execute_command('FT.SUGADD', 'sug', 'hb', '1'))
    env.assertEqual(3, conn.execute_command('FT.SUGADD', 'sug', 'hc', '1'))
    env.assertEqual(4, conn.execute_command('FT.SUGADD', 'sug', 'hd', '1'))
    env.assertEqual(5, conn.execute_command('FT.SUGADD', 'sug', 'he', '1'))

    res = conn.execute_command('FT.SUGGET', 'sug', 'h', 'MAX', '10', 'TRIM')
    env.assertEqual(res, ['ha'])

    # Server must still be alive and the trie intact: a follow-up SUGGET
    # without TRIM still returns the full set.
    res = conn.execute_command('FT.SUGGET', 'sug', 'h', 'MAX', '10')
    env.assertEqual(sorted(res), ['ha', 'hb', 'hc', 'hd', 'he'])

def testWrongType(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    errMsg = 'WRONGTYPE Operation against a key holding the wrong kind of value'

    # Test on wrong type
    conn.execute_command('set', 'sug', 'hello')
    try:
        conn.execute_command('ft.sugadd', 'sug', 'hello world', '1')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains(errMsg, str(e))

    try:
        conn.execute_command('ft.suglen', 'sug')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains(errMsg, str(e))

    try:
        conn.execute_command('ft.sugget', 'sug', 'hello')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains(errMsg, str(e))

    try:
        conn.execute_command('ft.sugdel', 'sug', 'hello world')
        env.assertTrue(False)
    except Exception as e:
        env.assertContains(errMsg, str(e))

def testSuggestZeroScoreAdd(env):
    # FT.SUGADD key string 0 — direct insert with score 0.
    # SUGGET queries use a strict prefix to avoid the (float)INT_MAX exact-match
    # boost applied to whole-term matches in Trie_Search.
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    env.assertEqual(1, conn.execute_command('ft.sugadd', 'ac', 'foo', 0))
    env.assertEqual(1, conn.execute_command('ft.suglen', 'ac'))
    env.assertEqual(['foo'], conn.execute_command('ft.sugget', 'ac', 'fo'))
    res = conn.execute_command('ft.sugget', 'ac', 'fo', 'WITHSCORES')
    env.assertEqual('foo', res[0])
    env.assertEqual(0.0, float(res[1]))

def testSuggestZeroScoreIncrOnMissing(env):
    # FT.SUGADD key string 0 INCR on an absent term — terminal at 0.
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    env.assertEqual(1, conn.execute_command('ft.sugadd', 'ac', 'foo', 0, 'INCR'))
    env.assertEqual(1, conn.execute_command('ft.suglen', 'ac'))
    res = conn.execute_command('ft.sugget', 'ac', 'fo', 'WITHSCORES')
    env.assertEqual('foo', res[0])
    env.assertEqual(0.0, float(res[1]))

def testSuggestIncrToZero(env):
    # FT.SUGADD key string N, then FT.SUGADD key string -N INCR — net score 0
    # on a pre-existing node; entry must remain terminal and visible to SUGGET.
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    env.assertEqual(1, conn.execute_command('ft.sugadd', 'ac', 'foo', 2))
    env.assertEqual(1, conn.execute_command('ft.sugadd', 'ac', 'foo', -2, 'INCR'))
    env.assertEqual(1, conn.execute_command('ft.suglen', 'ac'))
    res = conn.execute_command('ft.sugget', 'ac', 'fo', 'WITHSCORES')
    env.assertEqual('foo', res[0])
    env.assertEqual(0.0, float(res[1]))

    # Still deletable after collapsing to score 0.
    env.assertEqual(1, conn.execute_command('ft.sugdel', 'ac', 'foo'))
    env.assertEqual(0, conn.execute_command('ft.suglen', 'ac'))

def testSuggestZeroScoreSortsLast(env):
    # Score-0 entries must rank below positively-scored peers in SUGGET.
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    conn.execute_command('ft.sugadd', 'ac', 'foo zero', 0)
    conn.execute_command('ft.sugadd', 'ac', 'foo one', 1)
    conn.execute_command('ft.sugadd', 'ac', 'foo two', 2)
    res = conn.execute_command('ft.sugget', 'ac', 'foo')
    env.assertEqual(['foo two', 'foo one', 'foo zero'], res)

def testSuggestIncrKeepsEntryReachable(env):
    # SUGGET's top-k walk prunes a subtree whose subtree max score falls below
    # the heap threshold, so INCR must fold the entry's post-update score —
    # not just the delta — into every ancestor's subtree max score; a
    # delta-only fold leaves a non-terminal ancestor with a stale subtree max
    # score and the walk drops the highest-scoring suggestion.
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    # Two entries force a shared non-terminal node ("abcde", subtree max
    # score 1).
    conn.execute_command('ft.sugadd', 'ac', 'abcdef', 1)
    conn.execute_command('ft.sugadd', 'ac', 'abcdeg', 1)
    # Raise abcdef to 9 in deltas of 1: a delta-only fold keeps every subtree
    # max score on its path at max(initial score, deltas) = 1 while the
    # entry's score is 9.
    for _ in range(8):
        conn.execute_command('ft.sugadd', 'ac', 'abcdef', 1, 'INCR')
    # Split "abc" out of "abcde"; the split sibling scores too low to matter.
    conn.execute_command('ft.sugadd', 'ac', 'abcx', 0.5)
    # Two higher-scored siblings placed ahead of the stale "de" child in the
    # descending-score child order. With MAX 2 they fill the heap first and
    # raise the prune threshold to 3/sqrt(2) ~ 2.12, above the stale subtree
    # max score.
    conn.execute_command('ft.sugadd', 'ac', 'abcy', 3)
    conn.execute_command('ft.sugadd', 'ac', 'abcz', 3)

    # abcdef normalizes to 9/sqrt(4) = 4.5 — the best match by far.
    res = conn.execute_command('ft.sugget', 'ac', 'abc', 'MAX', '2')
    env.assertEqual(2, len(res))
    env.assertEqual('abcdef', res[0])

# TRIE_MAX_PREFIX (src/trie/trie_node.h) is the maximum rune length accepted by
# the trie search path behind FT.SUGGET. The boundary is inclusive: a query of
# exactly TRIE_MAX_PREFIX runes is accepted; one rune more is rejected.
TRIE_MAX_PREFIX = 100

def testSuggestGetAtMaxPrefixLength(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    term = 'a' * TRIE_MAX_PREFIX
    env.assertEqual(conn.execute_command('FT.SUGADD', 'ac', term, 1), 1)

    res = conn.execute_command('FT.SUGGET', 'ac', term)
    env.assertEqual(res, [term])

def testSuggestGetOverMaxPrefixLength(env):
    skipOnCrdtEnv(env)
    conn = env.getClusterConnectionIfNeeded()

    term = 'a' * (TRIE_MAX_PREFIX + 1)
    env.assertEqual(conn.execute_command('FT.SUGADD', 'ac', term, 1), 1)

    try:
        conn.execute_command('FT.SUGGET', 'ac', term)
        env.assertTrue(False)
    except Exception as e:
        env.assertContains('Invalid query', str(e))
