from common import *

def test_not_optimized():
    """Tests the optimized version of the NOT iterator, which holds an optimized
    wildcard iterator which is its "reference" traversal iterator instead of the
    basic 'incremental iterator'"""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Create an index that optimizes the wildcard iterator
    env.expect('FT.CREATE', 'idx', 'INDEXALL', 'ENABLE', 'SCHEMA', 'n', 'NUMERIC').ok()

    n_docs = 1005       # 5 more than the amount of entries in an index block
    q = '(@n:[42 42])'  # A simple query that has one result

    # Insert documents with positive numeric values
    for i in range(1, 1 + n_docs):
        conn.execute_command('HSET', f'doc{i}', 'n', i)

    # Search for the query and its negation
    res = env.cmd('FT.SEARCH', 'idx', f'-{q} | {q}', 'LIMIT', '0', '0')
    env.assertEqual(res, [n_docs])

    # Search for the negation of the query with no results (as all documents have a positive value)
    res = env.cmd('FT.SEARCH', 'idx', '-@n:[-1 -1]', 'LIMIT', '0', '0')
    env.assertEqual(res, [n_docs])

    # Search for the negation of the query with one result
    res = env.cmd('FT.SEARCH', 'idx', f'-{q}', 'LIMIT', '0', '0')
    env.assertEqual(res, [n_docs-1])

def test_not_optimized_with_missing():
    """Tests the optimized version of the NOT iterator with the missing values
    indexing enabled"""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Create an index that optimizes the wildcard iterator
    env.expect('FT.CREATE', 'idx', 'INDEXALL', 'ENABLE', 'SCHEMA', 't', 'TEXT', 'INDEXMISSING').ok()

    conn.execute_command('HSET', 'doc1', 't', 'hello')
    conn.execute_command('HSET', 'doc2', 't2', 'world')

    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@t)', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc2'])

    res = env.cmd('FT.SEARCH', 'idx', '-ismissing(@t)', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc1'])

    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@t) -ismissing(@t)', 'NOCONTENT')
    env.assertEqual(res, [0])

    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@t) | -ismissing(@t)', 'NOCONTENT')
    env.assertEqual(res, [2, 'doc1', 'doc2'])
