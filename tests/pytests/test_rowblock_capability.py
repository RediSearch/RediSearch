# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

# Rolling-upgrade capability negotiation for the internal coordinator<->shard
# row-block reply format (src/aggregate/row_block.h). All tests here run in
# cluster mode only: the format, and the negotiation that gates it, only fire on
# the internal coordinator<->shard path (useRowBlock in aggregate_exec.c requires
# IsInternal(req)), which a standalone server never takes.
#
# RLTest starts every shard in a cluster from the same modulePath, so a genuinely
# mixed-build fleet cannot be started by the harness. `search-_simulate-legacy-shard`
# (RSConfig.simulateLegacyShard) is the test-only knob that stands in for an older
# build - but only halfway: it makes a shard's *argument parser* reject `_ROW_BLOCK`
# exactly as a pre-row-block build would, but a module's version, as advertised over
# HELLO, is fixed at RedisModule_Init and cannot be faked by a runtime config. So
# every shard in this test fleet, simulated-legacy or not, still reports this build's
# real (capable) version, and the coordinator's HELLO-derived belief always resolves
# to Yes for it. That makes this knob, by construction, exercise
# MRConnManager_DemoteRowBlockCap's defence-in-depth path (a shard believed capable
# that rejects the token anyway - a hole in the version mapping) rather than the
# primary version-based mechanism (RediSearchCaps_HasRowBlock correctly saying "no"
# for a genuinely old version) - that primary mechanism has no way to be driven end
# to end without a real older binary.
#
# Concretely, every "legacy" shard in this file must be asked-and-rejected, and so
# every query's *first* encounter with a freshly-simulated shard fails (see
# aggregate_request.c's parseAggPlan terminal `else` - an unrecognized argument
# fails the whole query, not just that shard's share). _warm_up_demotion runs the
# query until every simulated shard has been individually demoted, then the actual
# test assertions run against demoted (i.e. RESP-using) shards. In production, a
# real older shard would skip this bootstrap failure entirely: its HELLO already
# says "no", so the coordinator never asks it in the first place. That gap between
# what this harness can drive and what production actually does is the reason
# test_demotion_after_unrecognized_arg exists as its own test: it is the one
# scenario where "the first query fails, then it stops happening" is the actual
# documented behavior, not a test-harness workaround.

from common import *

ROW_BLOCK_CONFIG = 'search-internal-row-block-format'
LEGACY_SHARD_CONFIG = 'search-_simulate-legacy-shard'


def _create_and_populate(env, n=30):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'cat', 'TAG', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(n):
        conn.execute_command('HSET', f'doc:{i}', 'cat', f'cat{i % 3}', 'n', i)
    waitForIndex(env, 'idx')


def _aggregate_grouped(env):
    """A GROUPBY aggregation, distributed across shards and merged on the
    coordinator - the path that carries row-block-encoded rows when the format is
    used. SORTBY makes the row order (and so the result) deterministic regardless
    of which shards replied in blocks vs. RESP."""
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                   'GROUPBY', '1', '@cat',
                   'REDUCE', 'COUNT', '0', 'AS', 'count',
                   'REDUCE', 'SUM', '1', '@n', 'AS', 'total',
                   'SORTBY', '2', '@cat', 'ASC')
    return [to_dict(row) for row in res[1:]]


def _set_all_shards(env, config, value):
    verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', config, value)


def _warm_up_demotion(env):
    """Run the query until it stops erroring, so every shard with
    search-_simulate-legacy-shard on has been asked-and-rejected at least once and
    demoted (MRConnManager_DemoteRowBlockCap) - see the module docstring for why
    this bootstrap round is unavoidable in this harness. Bounded by shardsCount:
    each round can newly demote at most the shards that replied and errored in it,
    so at most one round per shard should ever be needed. The final call is made
    outside the try/except so a real regression (e.g. demotion silently not taking
    effect) surfaces as a normal test failure instead of being swallowed here."""
    for _ in range(env.shardsCount):
        try:
            _aggregate_grouped(env)
            return
        except Exception:
            continue
    _aggregate_grouped(env)


# cat0/cat1/cat2, each holding every third doc out of 30 (n=0..29). Every test in
# this file that reaches a result asserts it equals this same ground truth -
# together they are scenario (6): the result is identical whether every shard
# replied in blocks, every shard fell back to RESP, or the fleet was mixed.
def _expected_rows():
    return [
        {'cat': 'cat0', 'count': '10', 'total': '135'},
        {'cat': 'cat1', 'count': '10', 'total': '145'},
        {'cat': 'cat2', 'count': '10', 'total': '155'},
    ]


@skip(cluster=False)
def test_all_shards_capable():
    """Scenario (1): every shard is on this build (the only one that can decode
    row-block today) and the format is on, so the coordinator's belief resolves to
    Yes for every shard and every shard is asked."""
    env = Env(shardsCount=3)
    _create_and_populate(env)
    _set_all_shards(env, ROW_BLOCK_CONFIG, 'yes')

    rows = _aggregate_grouped(env)
    env.assertEqual(rows, _expected_rows())

    # Every shard's connection pool should have resolved a Yes belief by now.
    # Uses env's own (non-cluster-routing) connection: SHARD_CONNECTION_STATES is
    # keyless, and the cluster-aware client can't route a keyless command on its own.
    state = str(env.cmd(debug_cmd(), 'SHARD_CONNECTION_STATES'))
    env.assertEqual(state.count('RowBlockCapability=Yes'), env.shardsCount,
                     message=f"debug state: {state}")


@skip(cluster=False)
def test_all_shards_legacy():
    """Scenario (2): every shard rejects `_ROW_BLOCK`. Once every shard has been
    demoted (see module docstring), the coordinator falls back to RESP for all of
    them and still returns correct results with no error - the fleet-wide
    pre-upgrade case."""
    env = Env(shardsCount=3)
    _create_and_populate(env)
    _set_all_shards(env, ROW_BLOCK_CONFIG, 'yes')
    _set_all_shards(env, LEGACY_SHARD_CONFIG, 'yes')

    _warm_up_demotion(env)
    rows = _aggregate_grouped(env)
    env.assertEqual(rows, _expected_rows())


@skip(cluster=False)
def test_one_legacy_shard_among_many():
    """Scenario (3): one shard out of N cannot decode the format; the rest can.
    Once that one shard has been demoted (see module docstring), the coordinator
    asks only the capable shards, the demoted one replies in RESP, and the merged
    result is unaffected by the mix - the case that, without per-shard negotiation,
    would fail the *entire* query on this one shard's unrecognized-argument error
    (see aggregate_request.c's parseAggPlan terminal `else`), not just its own
    share of it."""
    env = Env(shardsCount=3)
    _create_and_populate(env)
    _set_all_shards(env, ROW_BLOCK_CONFIG, 'yes')
    env.getConnection(1).execute_command('CONFIG', 'SET', LEGACY_SHARD_CONFIG, 'yes')

    _warm_up_demotion(env)
    rows = _aggregate_grouped(env)
    env.assertEqual(rows, _expected_rows())


@skip(cluster=False)
def test_first_query_on_fresh_connections_does_not_error():
    """Scenario (4): a shard's capability is Unknown until its connection's HELLO
    reply resolves it, which happens no earlier than the first command dispatched
    on that connection. maybeAskRowBlock (rmr.c) treats Unknown exactly like No -
    no token is sent - so this must succeed via RESP with no error, never wait for
    or depend on the belief resolving first.

    This cannot deterministically force the Unknown state from a black-box flow
    test: RLTest establishes the coordinator's connections to every shard as part
    of cluster setup, and some other internal command may already have resolved
    the belief before this test's own query runs. What it does verify is that the
    very first query issued against a fresh environment succeeds regardless of
    the coordinator's connections' internal capability state at that point.
    """
    env = Env(shardsCount=3)
    _create_and_populate(env)
    _set_all_shards(env, ROW_BLOCK_CONFIG, 'yes')

    rows = _aggregate_grouped(env)
    env.assertEqual(rows, _expected_rows())


@skip(cluster=False)
def test_demotion_after_unrecognized_arg():
    """Scenario (5): a shard that errors on `_ROW_BLOCK` despite being believed
    capable (LEGACY_SHARD_CONFIG simulates a hole in the version mapping: this
    shard's HELLO still advertises a capable version) is not asked again.

    Unlike the other mixed-fleet tests, this one asserts the failing bootstrap
    query directly instead of hiding it in _warm_up_demotion: "the first query
    fails, then it stops happening" is the actual documented behavior for this
    scenario (see MRConnManager_DemoteRowBlockCap's doc comment in conn.h), not a
    test-harness workaround for an unfakeable HELLO version.
    """
    env = Env(shardsCount=2)
    _create_and_populate(env)
    _set_all_shards(env, ROW_BLOCK_CONFIG, 'yes')
    env.getConnection(1).execute_command('CONFIG', 'SET', LEGACY_SHARD_CONFIG, 'yes')

    # First query: shard 1 is believed capable (its HELLO reports this build's real,
    # capable version) but rejects the token - this query fails.
    env.expect('FT.AGGREGATE', 'idx', '*',
               'GROUPBY', '1', '@cat',
               'REDUCE', 'COUNT', '0', 'AS', 'count').error()

    # Subsequent queries: shard 1 has been demoted to No and is not asked again,
    # so they succeed.
    for _ in range(2):
        rows = _aggregate_grouped(env)
        env.assertEqual(rows, _expected_rows())
