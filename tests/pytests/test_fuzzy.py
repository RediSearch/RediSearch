from includes import *
from common import *


def testBasicFuzzy(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.assertOk(env.getClusterConnectionIfNeeded().execute_command(
        'ft.add', 'idx', 'doc1', 1.0, 'fields',
        'title', 'hello world',
        'body', 'this is a test'))

    res = env.cmd('ft.search', 'idx', '%word%')
    env.assertEqual(res[0:2], [1, 'doc1'])
    env.assertEqual(set(res[2]), set(['title', 'hello world', 'body', 'this is a test']))

def testThreeFuzzy(env):
    env.cmd('FT.CREATE', 'idx', 'schema', 't', 'text')
    env.cmd('HSET', 'doc', 't', 'hello world')
    env.expect('FT.SEARCH', 'idx', '%%%wo%%%').equal([1, 'doc', ['t', 'hello world']])
    env.expect('FT.SEARCH', 'idx', '%%%wi%%%').equal([0])

    # check for upper case to lower case
    env.expect('FT.SEARCH', 'idx', '%%%WO%%%').equal([1, 'doc', ['t', 'hello world']])

def testLdLimit(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text')
    con = env.getClusterConnectionIfNeeded()
    con.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields', 'title', 'hello world')
    env.assertEqual([1, 'doc1', ['title', 'hello world']], env.cmd('ft.search', 'idx', '%word%'))  # should be ok
    env.assertEqual([0], env.cmd('ft.search', 'idx', r'%sword%'))  # should return nothing
    env.assertEqual([1, 'doc1', ['title', 'hello world']], env.cmd('ft.search', 'idx', r'%%sword%%'))

def testStopwords(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 't1', 'text')
    con = env.getClusterConnectionIfNeeded()
    for t in ('iwth', 'ta', 'foo', 'rof', 'whhch', 'witha'):
        con.execute_command('ft.add', 'idx', t, 1.0, 'fields', 't1', t)

    r = env.cmd('ft.search', 'idx', '%for%')
    env.assertEqual([1, 'foo', ['t1', 'foo']], r)

    r = env.cmd('ft.search', 'idx', '%%with%%')
    env.assertEqual([2, 'iwth', ['t1', 'iwth'], 'witha', ['t1', 'witha']], r)

    r = env.cmd('ft.search', 'idx', '%with%')
    env.assertEqual([1, 'witha', ['t1', 'witha']], r)

    r = env.cmd('ft.search', 'idx', '%at%')
    env.assertEqual([1, 'ta', ['t1', 'ta']], r)

def testFuzzyMultipleResults(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'title', 'hello world',
                                    'body', 'this is a test'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                    'title', 'hello word',
                                    'body', 'this is a test'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc3', 1.0, 'fields',
                                    'title', 'hello ward',
                                    'body', 'this is a test'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc4', 1.0, 'fields',
                                    'title', 'hello wakld',
                                    'body', 'this is a test'))

    res = env.cmd('ft.search', 'idx', '%word%')
    env.assertEqual(res[0], 3)
    for i in range(1,6,2):
        env.assertContains(res[i], ['doc1', 'doc2', 'doc3'])

def testFuzzySyntaxError(env):
    unallowChars = ('*', '$', '~', '&', '@', '!')
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    con = env.getClusterConnectionIfNeeded()
    con.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                        'title', 'hello world', 'body', 'this is a test')
    for ch in unallowChars:
        error = None
        try:
            env.cmd('ft.search', 'idx', f'%wor{ch}d%')
        except Exception as e:
            error = str(e)
        env.assertTrue('Syntax error' in error)

def testFuzzyWithNumbersOnly(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'test', 'TEXT', 'SORTABLE').equal('OK')
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', '12345'))

    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.expect('ft.search', 'idx', '%%21345%%', 'DIALECT', dialect)\
            .equal([1, 'doc1', ['test', '12345']])

@skip(cluster=True)
def testFuzzyManyExpansions(env):
    """Verify that a fuzzy query works correctly when matching more than 8
    terms, which triggers the internal iterator array capacity doubling in
    addTerm (initial capacity is 8)."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Create 10 distinct 3-letter terms that are all within Levenshtein
    # distance 1 of "bat": substitute the first character.
    terms = ['bat', 'cat', 'dat', 'eat', 'fat', 'gat', 'hat', 'mat', 'pat', 'rat']
    for i, term in enumerate(terms):
        conn.execute_command('HSET', f'doc{i}', 't', term)

    # Fuzzy search with distance 1: %bat% should match all of the above
    res = env.cmd('ft.search', 'idx', '%bat%', 'LIMIT', '0', '0')
    # We expect at least 9 results (>8 to trigger the capacity doubling).
    # The exact count may vary depending on what the trie iterator yields,
    # but all 10 terms are within distance 1 of "bat".
    env.assertGreaterEqual(res[0], 9, message=res)

@skip(cluster=True)
def testFuzzyMaxPrefixExpansionsWarning():
    """Verify that a fuzzy query triggers a max prefix expansions warning
    when the number of fuzzy matches exceeds MAXPREFIXEXPANSIONS."""
    env = Env(protocol=3)
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Create terms that are all within Levenshtein distance 1 of "ab":
    # aa, ab, ac, ..., az  (26 terms, all distance <= 1 from "ab")
    for c in 'abcdefghijklmnopqrstuvwxyz':
        conn.execute_command('HSET', f'doc_a{c}', 't', f'a{c}')

    # Set max prefix expansions to 1 so the fuzzy expansion is sure to exceed it
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Fuzzy query: %ab% should try to expand to all terms within distance 1
    res = env.cmd('FT.SEARCH', 'idx', '%ab%')
    env.assertContains('Max prefix expansions limit was reached', res['warning'])

    # Restore default
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '200')

@skip()
def testTagFuzzy(env):
    # TODO: fuzzy on tag is broken?

    env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 't', 'TAG')
    env.cmd('FT.CREATE', 'idx2', 'SCHEMA', 't', 'TAG', 'CASESENSITIVE')
    env.cmd('HSET', 'doc', 't', 'hello world')
    env.expect('FT.SEARCH', 'idx1', '@t:{(%worl%)}').equal([1, 'doc', ['t', 'hello world']]) # codespell:ignore worl
    env.expect('FT.SEARCH', 'idx1', '@t:{(%wor%)}').equal([0]) # codespell:ignore wor
    env.expect('FT.SEARCH', 'idx2', '@t:{(%worl%)}').equal([0]) # codespell:ignore worl
    env.expect('FT.SEARCH', 'idx2', '@t:{(%wir%)}').equal([0]) # codespell:ignore wir
