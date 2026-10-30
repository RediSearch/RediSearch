# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

# Above the optimizer's initial heap and result-pool capacity, so the limits below make them grow.
NUM_DOCS = 3000
# Above what a query over NUM_DOCS docs needs (including a sorter presized to the default
# MAXSEARCHRESULTS), far below an optimizer result pool presized to that limit.
PEAK_MEMORY_BUDGET = 32 * 1024 * 1024


def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TAG').ok()
    conn = getConnectionByEnv(env)
    for i in range(NUM_DOCS):
        # Every doc has x; every other doc also has half, so @t:{half} makes the optimizer rewind.
        tags = 'x,half' if i % 2 else 'x'
        conn.execute_command('HSET', f'doc:{i}', 'n', (i * 7919) % NUM_DOCS, 't', tags)


def _n_values(res):
    return [int(fields[1]) for fields in res[2::2]]


def _optimized(env, *args, filter='@t:{x}'):
    """Run a FT.SEARCH sorted by @n through the optimizer (WITHOUTCOUNT) and check it is used."""
    query = ('FT.SEARCH', 'idx', filter, 'SORTBY', 'n', *args, 'RETURN', 1, 'n')
    profile = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *query[2:], 'WITHOUTCOUNT')
    env.assertContains('Optimizer', str(profile))
    return env.cmd(*query, 'WITHOUTCOUNT')


@skip(cluster=True)
def test_optimizer_large_limit_does_not_presize():
    """The optimizer used to allocate its heap and result pool for the whole LIMIT up front."""
    # Fresh server: the used_memory peak is never reset.
    env = Env(freshEnv=True)
    _create_index(env)
    max_results = int(env.cmd(config_cmd(), 'GET', 'MAXSEARCHRESULTS')[0][1])
    # Profiles the measured query once, so the measurement below is known to take the optimizer.
    _optimized(env, 'ASC', 'LIMIT', 0, max_results)

    # Before Redis 8.10 the peak is sampled only between commands, so this check cannot fail there.
    before = env.cmd('INFO', 'memory')['used_memory_peak']
    res = env.cmd('FT.SEARCH', 'idx', '@t:{x}', 'SORTBY', 'n', 'ASC', 'LIMIT', 0, max_results,
                  'RETURN', 1, 'n', 'WITHOUTCOUNT')
    growth = env.cmd('INFO', 'memory')['used_memory_peak'] - before

    env.assertEqual(_n_values(res), list(range(NUM_DOCS)))
    env.assertLess(growth, PEAK_MEMORY_BUDGET)


@skip(cluster=True)
def test_optimizer_grows_past_initial_capacity(env):
    """Limits above the optimizer's initial capacity return the same rows as without it."""
    _create_index(env)
    for filter, order, args in (('@t:{x}', 'ASC', ('LIMIT', 0, 2000)),
                                ('@t:{x}', 'DESC', ('LIMIT', 1500, 10)),
                                ('@t:{x}', 'ASC', ('LIMIT', 0, NUM_DOCS + 1)),
                                ('@t:{half}', 'ASC', ('LIMIT', 0, 1200)),
                                ('@t:{half}', 'DESC', ('LIMIT', 0, NUM_DOCS))):
        res = _optimized(env, order, *args, filter=filter)
        expected = env.cmd('FT.SEARCH', 'idx', filter, 'SORTBY', 'n', order, *args,
                           'RETURN', 1, 'n')
        env.assertEqual(_n_values(res), _n_values(expected), message=(filter, order, args))
