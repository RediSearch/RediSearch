# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

NUM_SMALL_GROUPS = 500
# Large enough that the group's COLLECT storage grows past its initial capacity.
BIG_GROUP_SIZE = 300
# COLLECT storage is built once per group, so a per-group presize of thousands of slots
# would add hundreds of MiB here; the rows themselves are well under this.
PEAK_MEMORY_BUDGET = 16 * 1024 * 1024


def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'TAG', 'SORTABLE',
               'v', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(NUM_SMALL_GROUPS):
        conn.execute_command('HSET', f'small:{i}', 'g', f's{i}', 'v', i)
    for i in range(BIG_GROUP_SIZE):
        conn.execute_command('HSET', f'big:{i}', 'g', 'big', 'v', i)


def _collected(res, group):
    for row in res['results']:
        attrs = row['extra_attributes']
        if attrs['g'] == group:
            return sorted(int(entry['v']) for entry in attrs['vals'])
    return None


@skip(cluster=True)
def test_collect_many_groups_does_not_presize():
    """Unsorted COLLECT without LIMIT keeps up to MAXAGGREGATERESULTS rows per group. That bound
    must not be allocated up front for every group, and a group must still keep all its rows."""
    # RESP3 for the map reply; fresh server because the used_memory peak is never reset.
    env = Env(protocol=3, freshEnv=True)
    _create_index(env)

    # Before Redis 8.10 the peak is sampled only between commands, so this check cannot fail there.
    before = env.cmd('INFO', 'memory')['used_memory_peak']
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 1, '@g',
                  'REDUCE', 'COLLECT', 3, 'FIELDS', 1, '@v', 'AS', 'vals')
    growth = env.cmd('INFO', 'memory')['used_memory_peak'] - before

    env.assertEqual(len(res['results']), NUM_SMALL_GROUPS + 1)
    env.assertEqual(_collected(res, 'big'), list(range(BIG_GROUP_SIZE)))
    env.assertEqual(_collected(res, 's7'), [7])
    env.assertLess(growth, PEAK_MEMORY_BUDGET)


def test_collect_sorted_grows_past_initial_capacity():
    """A sorted COLLECT with a LIMIT above the initial capacity keeps the correct top rows. Runs in
    cluster mode too, where the coordinator merges the shards' rows in its own storage."""
    env = Env(protocol=3)
    _create_index(env)
    res = env.cmd('FT.AGGREGATE', 'idx', '@g:{big}', 'GROUPBY', 1, '@g',
                  'REDUCE', 'COLLECT', 10, 'FIELDS', 1, '@v', 'SORTBY', 2, '@v', 'DESC',
                  'LIMIT', 0, 200, 'AS', 'vals')
    env.assertEqual(len(res['results']), 1, message=res)
    vals = [int(entry['v']) for entry in res['results'][0]['extra_attributes']['vals']]
    env.assertEqual(vals, list(range(BIG_GROUP_SIZE - 1, BIG_GROUP_SIZE - 201, -1)))
