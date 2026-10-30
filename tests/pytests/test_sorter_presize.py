# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

# Above the sorter's initial heap capacity, so the limits below make the heap grow.
NUM_DOCS = 3000
# A requested result count whose up-front heap (8 B per slot) would be about 16 GiB.
HUGE = 2_000_000_000
# Bound on the peak used_memory a query over NUM_DOCS docs may add; below the 8 MiB a sorter
# presized to the default MAXSEARCHRESULTS would take.
PEAK_MEMORY_BUDGET = 4 * 1024 * 1024


def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(NUM_DOCS):
        # Shuffle the insertion order so the sort does real work.
        conn.execute_command('HSET', f'doc:{i}', 'n', (i * 7919) % NUM_DOCS)


def _peak_memory_growth(env, *query):
    """Run `query` and return how much it raised the server's used_memory peak.

    The peak is only reset by a restart, so callers use a fresh server. Servers that sample the
    peak only between commands (before Redis 8.10) miss allocations freed inside the command, so
    there this check cannot fail.
    """
    before = env.cmd('INFO', 'memory')['used_memory_peak']
    res = env.cmd(*query)
    after = env.cmd('INFO', 'memory')['used_memory_peak']
    return res, after - before


def _aggregate_values(res):
    return [int(row[1]) for row in res[1:]]


@skip(cluster=True)
def test_aggregate_huge_max_does_not_presize():
    """SORTBY MAX skips the LIMIT check, so MAX sizes the sorter directly. A huge MAX must
    return every row in order without allocating a heap of that size."""
    for query in (
        ('SORTBY', 2, '@n', 'ASC', 'MAX', HUGE),
        ('SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, HUGE),
    ):
        env = Env(freshEnv=True)
        _create_index(env)
        res, growth = _peak_memory_growth(env, 'FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n', *query)
        env.assertEqual(_aggregate_values(res), list(range(NUM_DOCS)), message=query)
        env.assertLess(growth, PEAK_MEMORY_BUDGET, message=query)


def test_sorter_grows_past_initial_capacity(env):
    """Limits above the sorter's initial heap capacity keep the same top-N semantics: the full
    result when the limit covers every match, and the correct prefix or page when it does not."""
    _create_index(env)

    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                  'SORTBY', 2, '@n', 'DESC', 'MAX', NUM_DOCS + 1)
    env.assertEqual(_aggregate_values(res), list(range(NUM_DOCS - 1, -1, -1)))

    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                  'SORTBY', 2, '@n', 'ASC', 'MAX', 2000)
    env.assertEqual(_aggregate_values(res), list(range(2000)))

    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                  'SORTBY', 2, '@n', 'ASC', 'LIMIT', 1500, 10)
    env.assertEqual(_aggregate_values(res), list(range(1500, 1510)))

    res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'DESC', 'LIMIT', 1500, 10,
                  'RETURN', 1, 'n')
    env.assertEqual(res[0], NUM_DOCS)
    env.assertEqual([int(fields[1]) for fields in res[2::2]],
                    list(range(NUM_DOCS - 1501, NUM_DOCS - 1511, -1)))


@skip(cluster=True)
def test_search_large_limit_does_not_presize():
    """FT.SEARCH LIMIT up to MAXSEARCHRESULTS used to presize the sorter to the limit."""
    env = Env(freshEnv=True)
    _create_index(env)
    max_results = int(env.cmd(config_cmd(), 'GET', 'MAXSEARCHRESULTS')[0][1])
    res, growth = _peak_memory_growth(env, 'FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'ASC',
                                      'LIMIT', 0, max_results, 'RETURN', 1, 'n')
    env.assertEqual(res[0], NUM_DOCS)
    env.assertEqual([int(fields[1]) for fields in res[2::2]], list(range(NUM_DOCS)))
    env.assertLess(growth, PEAK_MEMORY_BUDGET)
