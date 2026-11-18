# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Query-wide options must reach every shard of a distributed FT.AGGREGATE.

The coordinator builds the shard command itself, so a query option it does not
forward is silently ignored by the shards. Each case compares FT.AGGREGATE with
FT.SEARCH, which forwards the client arguments unchanged, and with a literal
count so the test does not pass when both are wrong the same way.
"""

from common import *
from RLTest import Env

NUM_DOCS = 20
# Half the documents have `redis` before `database`, half after; in both,
# `redis` precedes `fast` with one word in between.
TEXTS = ('redis database fast search', 'database redis search fast engine')


def _setup(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'content', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    # One hash tag per shard spreads the documents over every shard; a test
    # whose data sits on a single shard cannot show a forwarding bug.
    tags = list(distinct_shard_tags(conn)) if env.isCluster() else ['']
    for i in range(NUM_DOCS):
        conn.execute_command('HSET', f'doc:{{{tags[i % len(tags)]}}}{i}',
                             'content', TEXTS[i % len(TEXTS)])
    if env.isCluster():
        sizes = [c.execute_command('DBSIZE') for c in shardsConnections(env)]
        env.assertTrue(all(s > 0 for s in sizes), message=sizes)


def _agg_count(res):
    # GROUPBY 0 over no rows yields no group, so an empty reply means zero.
    if len(res) == 1:
        return 0
    row = res[1]
    return int(dict(zip(row[::2], row[1::2]))['n'])


def _aggregate_count(env, query, *args):
    return _agg_count(env.cmd('FT.AGGREGATE', 'idx', query, *args,
                              'GROUPBY', '0', 'REDUCE', 'COUNT', '0', 'AS', 'n'))


def testSlopAndInorderMatchSearch(env):
    """FT.AGGREGATE applies SLOP and INORDER on every shard, like FT.SEARCH."""
    _setup(env)
    cases = [
        ('redis fast', (), 20),
        ('redis fast', ('SLOP', '0'), 0),
        ('redis database', ('INORDER',), 10),
        ('fast redis', ('INORDER',), 0),
        # Both options forwarded together.
        ('redis fast', ('SLOP', '1', 'INORDER'), 20),
        ('fast redis', ('SLOP', '5', 'INORDER'), 0),
    ]
    for query, args, expected in cases:
        search = env.cmd('FT.SEARCH', 'idx', query, *args, 'NOCONTENT', 'LIMIT', '0', '0')
        aggregate = _aggregate_count(env, query, *args)
        env.assertEqual((search[0], aggregate), (expected, expected),
                        message=(query, args))


def testProfileAggregateForwardsSlop(env):
    """FT.PROFILE ... AGGREGATE builds the shard command the same way."""
    _setup(env)
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', 'redis fast', 'SLOP', '0',
                  'GROUPBY', '0', 'REDUCE', 'COUNT', '0', 'AS', 'n')
    env.assertEqual(_agg_count(res[0]), 0, message=res[0])


@skip(no_json=True)
def testSlopAndInorderRejectedOnUndefinedOrderField(env):
    """FT.AGGREGATE rejects SLOP and INORDER on a multi-value TEXT field, like FT.SEARCH.

    The shards raise the error, so it only reaches a cluster client once the
    coordinator forwards the options.
    """
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.books[*].authors', 'AS', 'author', 'TEXT').ok()
    for args in (('SLOP', '200'), ('INORDER',)):
        for cmd in ('FT.SEARCH', 'FT.AGGREGATE'):
            env.expect(cmd, 'idx', '@author:(does not matter)', *args) \
                .error().contains('with undefined ordering')


def testApplyAliasNamedInorderIsNotQueryOption(env):
    """An APPLY alias spelled INORDER must not be forwarded as the INORDER option.

    INORDER would drop the count for this query to zero.
    """
    _setup(env)
    env.assertEqual(_aggregate_count(env, 'fast redis', 'APPLY', '1', 'AS', 'INORDER'), 20)


def testApplyAliasNamedSlopIsNotQueryOption(env):
    """An APPLY alias spelled SLOP must not be forwarded as the SLOP option.

    A forwarded `SLOP GROUPBY` would fail to parse on every shard.
    """
    _setup(env)
    env.assertEqual(_aggregate_count(env, 'redis fast', 'APPLY', '1', 'AS', 'SLOP'), 20)
