
from RLTest import Env
from includes import *
from common import *


def testTagLexRangeGT(env):
    """Test > operator on TAG fields returns tags lexicographically greater."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'name', 'tag').ok()
    conn.execute_command('hset', 'doc1', 'name', 'alice')
    conn.execute_command('hset', 'doc2', 'name', 'bob')
    conn.execute_command('hset', 'doc3', 'name', 'charlie')
    conn.execute_command('hset', 'doc4', 'name', 'dave')
    conn.execute_command('hset', 'doc5', 'name', 'eve')

    waitForIndex(env, 'idx')

    # @name > charlie -> dave, eve
    res = env.cmd('ft.search', 'idx', '@name > charlie', 'nocontent')
    env.assertEqual(res[0], 2)
    results = py2sorted(res[1:])
    env.assertContains('doc4', results)
    env.assertContains('doc5', results)


def testTagLexRangeLeadingDigitNotParsedAsNumber(env):
    """RHS like 5stars must be a full TERM, not a partial numeric parse (issue #14996)."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'id', 'tag').ok()
    conn.execute_command('hset', 'a', 'id', '1st')
    conn.execute_command('hset', 'b', 'id', '5stars')
    conn.execute_command('hset', 'c', 'id', '5zzz')

    waitForIndex(env, 'idx')

    # Strictly greater than "5stars" -> only "5zzz"
    res = env.cmd('ft.search', 'idx', '@id > 5stars', 'nocontent')
    env.assertEqual(res[0], 1)
    env.assertContains('c', res[1:])


def testTagLexRangeGE(env):
    """Test >= operator on TAG fields returns tags lexicographically greater or equal."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'name', 'tag').ok()
    conn.execute_command('hset', 'doc1', 'name', 'alice')
    conn.execute_command('hset', 'doc2', 'name', 'bob')
    conn.execute_command('hset', 'doc3', 'name', 'charlie')
    conn.execute_command('hset', 'doc4', 'name', 'dave')
    conn.execute_command('hset', 'doc5', 'name', 'eve')

    waitForIndex(env, 'idx')

    # @name >= charlie -> charlie, dave, eve
    res = env.cmd('ft.search', 'idx', '@name >= charlie', 'nocontent')
    env.assertEqual(res[0], 3)
    results = py2sorted(res[1:])
    env.assertContains('doc3', results)
    env.assertContains('doc4', results)
    env.assertContains('doc5', results)


def testTagLexRangeLT(env):
    """Test < operator on TAG fields returns tags lexicographically less."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'name', 'tag').ok()
    conn.execute_command('hset', 'doc1', 'name', 'alice')
    conn.execute_command('hset', 'doc2', 'name', 'bob')
    conn.execute_command('hset', 'doc3', 'name', 'charlie')
    conn.execute_command('hset', 'doc4', 'name', 'dave')
    conn.execute_command('hset', 'doc5', 'name', 'eve')

    waitForIndex(env, 'idx')

    # @name < charlie -> alice, bob
    res = env.cmd('ft.search', 'idx', '@name < charlie', 'nocontent')
    env.assertEqual(res[0], 2)
    results = py2sorted(res[1:])
    env.assertContains('doc1', results)
    env.assertContains('doc2', results)


def testTagLexRangeLE(env):
    """Test <= operator on TAG fields returns tags lexicographically less or equal."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'name', 'tag').ok()
    conn.execute_command('hset', 'doc1', 'name', 'alice')
    conn.execute_command('hset', 'doc2', 'name', 'bob')
    conn.execute_command('hset', 'doc3', 'name', 'charlie')
    conn.execute_command('hset', 'doc4', 'name', 'dave')
    conn.execute_command('hset', 'doc5', 'name', 'eve')

    waitForIndex(env, 'idx')

    # @name <= charlie -> alice, bob, charlie
    res = env.cmd('ft.search', 'idx', '@name <= charlie', 'nocontent')
    env.assertEqual(res[0], 3)
    results = py2sorted(res[1:])
    env.assertContains('doc1', results)
    env.assertContains('doc2', results)
    env.assertContains('doc3', results)


def testTagLexRangeCombined(env):
    """Test combining lex range operators for between-style queries on TAG fields."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'name', 'tag').ok()
    conn.execute_command('hset', 'doc1', 'name', 'alice')
    conn.execute_command('hset', 'doc2', 'name', 'bob')
    conn.execute_command('hset', 'doc3', 'name', 'charlie')
    conn.execute_command('hset', 'doc4', 'name', 'dave')
    conn.execute_command('hset', 'doc5', 'name', 'eve')

    waitForIndex(env, 'idx')

    # @name >= bob @name <= dave -> bob, charlie, dave
    res = env.cmd('ft.search', 'idx', '@name >= bob @name <= dave', 'nocontent')
    env.assertEqual(res[0], 3)
    results = py2sorted(res[1:])
    env.assertContains('doc2', results)
    env.assertContains('doc3', results)
    env.assertContains('doc4', results)


def testTagLexRangeNoResults(env):
    """Test lex range returns empty when no matches exist."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'name', 'tag').ok()
    conn.execute_command('hset', 'doc1', 'name', 'alice')
    conn.execute_command('hset', 'doc2', 'name', 'bob')

    waitForIndex(env, 'idx')

    # @name > zzz -> nothing
    res = env.cmd('ft.search', 'idx', '@name > zzz', 'nocontent')
    env.assertEqual(res[0], 0)

    # @name < aaa -> nothing
    res = env.cmd('ft.search', 'idx', '@name < aaa', 'nocontent')
    env.assertEqual(res[0], 0)


def testTextLexRangeGT(env):
    """Test > operator on TEXT NOSTEM fields returns terms lexicographically greater."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'city', 'text', 'NOSTEM').ok()
    conn.execute_command('hset', 'doc1', 'city', 'atlanta')
    conn.execute_command('hset', 'doc2', 'city', 'boston')
    conn.execute_command('hset', 'doc3', 'city', 'chicago')
    conn.execute_command('hset', 'doc4', 'city', 'denver')
    conn.execute_command('hset', 'doc5', 'city', 'erie')

    waitForIndex(env, 'idx')

    # @city > chicago -> denver, erie
    res = env.cmd('ft.search', 'idx', '@city > chicago', 'nocontent')
    env.assertEqual(res[0], 2)
    results = py2sorted(res[1:])
    env.assertContains('doc4', results)
    env.assertContains('doc5', results)


def testTextLexRangeLE(env):
    """Test <= operator on TEXT NOSTEM fields."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'city', 'text', 'NOSTEM').ok()
    conn.execute_command('hset', 'doc1', 'city', 'atlanta')
    conn.execute_command('hset', 'doc2', 'city', 'boston')
    conn.execute_command('hset', 'doc3', 'city', 'chicago')
    conn.execute_command('hset', 'doc4', 'city', 'denver')
    conn.execute_command('hset', 'doc5', 'city', 'erie')

    waitForIndex(env, 'idx')

    # @city <= chicago -> atlanta, boston, chicago
    res = env.cmd('ft.search', 'idx', '@city <= chicago', 'nocontent')
    env.assertEqual(res[0], 3)
    results = py2sorted(res[1:])
    env.assertContains('doc1', results)
    env.assertContains('doc2', results)
    env.assertContains('doc3', results)


def testNumericStillWorksWithOperators(env):
    """Verify that numeric comparison operators still work correctly."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'price', 'numeric').ok()
    conn.execute_command('hset', 'doc1', 'price', '10')
    conn.execute_command('hset', 'doc2', 'price', '20')
    conn.execute_command('hset', 'doc3', 'price', '30')

    waitForIndex(env, 'idx')

    res = env.cmd('ft.search', 'idx', '@price > 15', 'nocontent')
    env.assertEqual(res[0], 2)

    res = env.cmd('ft.search', 'idx', '@price >= 20', 'nocontent')
    env.assertEqual(res[0], 2)

    res = env.cmd('ft.search', 'idx', '@price < 25', 'nocontent')
    env.assertEqual(res[0], 2)

    res = env.cmd('ft.search', 'idx', '@price <= 20', 'nocontent')
    env.assertEqual(res[0], 2)


def testTagLexRangeKeysetPagination(env):
    """Demonstrate keyset-based pagination using lex range on TAG fields."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'username', 'tag', 'SORTABLE').ok()

    users = ['anna', 'beth', 'carl', 'dana', 'emma', 'finn', 'gina', 'hans']
    for i, u in enumerate(users):
        conn.execute_command('hset', 'user:%s' % u, 'username', u)

    waitForIndex(env, 'idx')

    # Page 1: first 3 (sorted by tag): anna, beth, carl
    res = env.cmd('ft.search', 'idx', '*', 'SORTBY', 'username', 'ASC',
                  'LIMIT', '0', '3', 'nocontent')
    env.assertEqual(res[0], 8)

    # Page 2: keyset pagination with @username > carl
    res = env.cmd('ft.search', 'idx', '@username > carl',
                  'SORTBY', 'username', 'ASC', 'LIMIT', '0', '3', 'nocontent')
    env.assertEqual(res[0], 5)


def testTagLexRangeWithIntersection(env):
    """Test lex range combined with other filter conditions."""
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'schema', 'name', 'tag', 'score', 'numeric').ok()
    conn.execute_command('hset', 'doc1', 'name', 'alice', 'score', '90')
    conn.execute_command('hset', 'doc2', 'name', 'bob', 'score', '80')
    conn.execute_command('hset', 'doc3', 'name', 'charlie', 'score', '70')
    conn.execute_command('hset', 'doc4', 'name', 'dave', 'score', '85')
    conn.execute_command('hset', 'doc5', 'name', 'eve', 'score', '95')

    waitForIndex(env, 'idx')

    # @name >= charlie AND @score > 75 -> dave(85), eve(95)
    res = env.cmd('ft.search', 'idx', '@name >= charlie @score:[75 +inf]', 'nocontent')
    env.assertEqual(res[0], 2)
    results = py2sorted(res[1:])
    env.assertContains('doc4', results)
    env.assertContains('doc5', results)
