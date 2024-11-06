from common import *
import faker
from random import shuffle
from faker.providers.person.en import Provider

def test_not_optimized():
    """Tests the optimized version of the NOT iterator, which holds an optimized
    wildcard iterator which is its "reference" traversal iterator instead of the
    basic 'incremental iterator'"""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Create an index that optimizes the wildcard iterator
    env.expect('FT.CREATE', 'idx', 'INDEXALL', 'ENABLE', 'SCHEMA', 't', 'TEXT').ok()

    n_docs = 1005       # 5 more than the amount of entries in an index block
    first_names = list(set(Provider.first_names))
    last_names = list(set(Provider.last_names))
    shuffle(first_names)
    shuffle(last_names)

    for i in range(n_docs):
        name = f'{first_names[i]} {last_names[i % len(last_names)]}'
        conn.execute_command('HSET', f'doc{i}', 't', name)

    res = env.cmd('FT.SEARCH', 'idx', '-t | t', 'LIMIT', '0', '0')
    env.assertEqual(res, [n_docs])

    res = env.cmd('FT.SEARCH', 'idx', '-@t:123', 'LIMIT', '0', '0')
    env.assertEqual(res, [n_docs])

    res = env.cmd('FT.SEARCH', 'idx', f'-(@t:{name})', 'LIMIT', '0', '0')
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
