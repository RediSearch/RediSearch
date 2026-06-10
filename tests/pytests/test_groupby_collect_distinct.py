import json

from common import Env, enable_unstable_features, getConnectionByEnv, skip


# Shared dataset for the DISTINCT tests: one group `G` whose docs have duplicate
# `cat` values at different `score`s, so DISTINCT collapses by `cat` keeping the
# best (highest score) representative. `cat=a` appears at scores 1 and 5; `cat=b`
# at scores 3 and 2.
_DISTINCT_DOCS = [('a', 1), ('a', 5), ('b', 3), ('b', 2)]


# ---------------------------------------------------------------------------
# COLLECT ... DISTINCT
# ---------------------------------------------------------------------------
def _setup_distinct(env):
    """Populate the shared DISTINCT dataset on a HASH index."""
    env.expect('FT.CREATE', 'didx', 'ON', 'HASH',
               'SCHEMA', 'grp', 'TAG', 'SORTABLE',
               'cat', 'TAG', 'SORTABLE',
               'score', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, (cat, score) in enumerate(_DISTINCT_DOCS):
        conn.execute_command('HSET', f'd:{i}', 'grp', 'G', 'cat', cat, 'score', str(score))


def _setup_distinct_json(env):
    """Populate the shared DISTINCT dataset on a JSON index."""
    env.expect('FT.CREATE', 'didx', 'ON', 'JSON',
               'SCHEMA',
               '$.grp', 'AS', 'grp', 'TAG', 'SORTABLE',
               '$.cat', 'AS', 'cat', 'TAG', 'SORTABLE',
               '$.score', 'AS', 'score', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, (cat, score) in enumerate(_DISTINCT_DOCS):
        conn.execute_command('JSON.SET', f'd:{i}', '$',
                             json.dumps({'grp': 'G', 'cat': cat, 'score': score}))


def test_collect_distinct_dedups_by_field():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_distinct(env)

    res = env.cmd(
        'FT.AGGREGATE', 'didx', '*',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '8',
            'FIELDS', '1', '@cat',
            'SORTBY', '2', '@score', 'DESC',
            'DISTINCT',
        'AS', 'cats')

    env.assertEqual(len(res['results']), 1)
    cats = res['results'][0]['extra_attributes']['cats']
    # Deduped to two cats. Order is by best score DESC: a(best=5) before b(best=3).
    # The order proves the *best* representative of `a` (score 5, not 1) was kept;
    # had the worse rep survived, `a`(1) would rank after `b`(3).
    env.assertEqual(cats, [{'cat': 'a'}, {'cat': 'b'}])


@skip(no_json=True)
def test_collect_distinct_dedups_by_field_json():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_distinct_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'didx', '*',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '8',
            'FIELDS', '1', '@cat',
            'SORTBY', '2', '@score', 'DESC',
            'DISTINCT',
        'AS', 'cats')

    env.assertEqual(len(res['results']), 1)
    cats = res['results'][0]['extra_attributes']['cats']
    # Same dedup behavior as the HASH case: a(best=5) before b(best=3).
    env.assertEqual(cats, [{'cat': 'a'}, {'cat': 'b'}])


def _setup_distinct_json_object(env):
    """JSON index where the projected COLLECT field is an *object*, not a scalar.
    """
    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON',
               'SCHEMA',
               '$.grp', 'AS', 'grp', 'TAG', 'SORTABLE',
               '$.score', 'AS', 'score', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    # Same content {a:1, b:2}, different key order. Pass raw JSON text (not
    # json.dumps) so the on-disk key order is exactly what we specify here.
    conn.execute_command('JSON.SET', 'd:0', '$',
                         '{"grp":"G","score":1,"payload":{"a":1,"b":2}}')
    conn.execute_command('JSON.SET', 'd:1', '$',
                         '{"grp":"G","score":5,"payload":{"b":2,"a":1}}')


@skip(no_json=True)
def test_collect_distinct_json_object_not_structurally_deduped():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_distinct_json_object(env)

    res = env.cmd(
        'FT.AGGREGATE', 'jidx', '*',
        'LOAD', '3', '$.payload', 'AS', 'payload',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '8',
            'FIELDS', '1', '@payload',
            'SORTBY', '2', '@score', 'DESC',
            'DISTINCT',
        'AS', 'payloads')

    env.assertEqual(len(res['results']), 1)
    payloads = res['results'][0]['extra_attributes']['payloads']
    # DISTINCT is NOT deep structural equality: two same-content objects with a
    # different key order are treated as distinct and both survive. (DISTINCT
    # hashes a JSON field like every other reducer — by the value's first
    # element, matching GROUP BY — never by deep structure.)
    env.assertEqual(len(payloads), 2)


def test_collect_without_distinct_keeps_duplicates():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_distinct(env)

    res = env.cmd(
        'FT.AGGREGATE', 'didx', '*',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '7',
            'FIELDS', '1', '@cat',
            'SORTBY', '2', '@score', 'DESC',
        'AS', 'cats')

    cats = res['results'][0]['extra_attributes']['cats']
    # No DISTINCT: all four rows retained, sorted by score DESC (5, 3, 2, 1).
    env.assertEqual(cats, [{'cat': 'a'}, {'cat': 'b'}, {'cat': 'b'}, {'cat': 'a'}])


def test_collect_distinct_with_limit():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_distinct(env)

    res = env.cmd(
        'FT.AGGREGATE', 'didx', '*',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '11',
            'FIELDS', '1', '@cat',
            'SORTBY', '2', '@score', 'DESC',
            'LIMIT', '0', '1',
            'DISTINCT',
        'AS', 'cats')

    cats = res['results'][0]['extra_attributes']['cats']
    # Dedup then top-1 by score DESC → just the best cat (a, best score 5).
    env.assertEqual(cats, [{'cat': 'a'}])


def test_collect_distinct_key_field_no_op():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_distinct(env)

    # @__key is among the projected fields, so every tuple is unique and DISTINCT
    # collapses nothing — all four rows are retained.
    res = env.cmd(
        'FT.AGGREGATE', 'didx', '*',
        'LOAD', '1', '@__key',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '9',
            'FIELDS', '2', '@cat', '@__key',
            'SORTBY', '2', '@score', 'DESC',
            'DISTINCT',
        'AS', 'rows')

    rows = res['results'][0]['extra_attributes']['rows']
    env.assertEqual(len(rows), 4)


@skip(cluster=False)
def test_collect_distinct_cluster_cross_shard():
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'didx', 'ON', 'HASH',
               'SCHEMA', 'grp', 'TAG', 'SORTABLE',
               'cat', 'TAG', 'SORTABLE',
               'score', 'NUMERIC', 'SORTABLE').ok()

    conn = getConnectionByEnv(env)
    # `cat=a` lands on two different shards (distinct hash tags) at scores 1 and 5;
    # `cat=b` on a third at score 3. The coordinator must dedup `a` across shards,
    # keeping the best (5).
    docs = [('d:0{s0}', 'a', 1), ('d:1{s1}', 'a', 5), ('d:2{s2}', 'b', 3)]
    for key, cat, score in docs:
        conn.execute_command('HSET', key, 'grp', 'G', 'cat', cat, 'score', str(score))

    res = env.cmd(
        'FT.AGGREGATE', 'didx', '*',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '8',
            'FIELDS', '1', '@cat',
            'SORTBY', '2', '@score', 'DESC',
            'DISTINCT',
        'AS', 'cats')

    env.assertEqual(len(res['results']), 1)
    cats = res['results'][0]['extra_attributes']['cats']
    # Cross-shard dedup: a(best=5) before b(3).
    env.assertEqual(cats, [{'cat': 'a'}, {'cat': 'b'}])


def _setup_cluster_json_object(env):
    """JSON index on a cluster; the projected COLLECT field is an *object*.
    """
    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON',
               'SCHEMA',
               '$.grp', 'AS', 'grp', 'TAG', 'SORTABLE',
               '$.score', 'AS', 'score', 'NUMERIC', 'SORTABLE').ok()
    return getConnectionByEnv(env)


def _aggregate_collect_json_payload(env):
    return env.cmd(
        'FT.AGGREGATE', 'jidx', '*',
        'LOAD', '3', '$.payload', 'AS', 'payload',
        'GROUPBY', '1', '@grp',
        'REDUCE', 'COLLECT', '8',
            'FIELDS', '1', '@payload',
            'SORTBY', '2', '@score', 'DESC',
            'DISTINCT',
        'AS', 'payloads')


@skip(cluster=False, no_json=True)
def test_collect_distinct_cluster_cross_shard_json_dedups_identical():
    # Two docs on DIFFERENT shards carry a byte-identical JSON `payload` object.
    # The coordinator dedups them on the serialized string ⇒ collapse to one.
    # Proves the cross-shard JSON dedup path actually fires (the guard that the
    # key-order test below is not a false positive from dedup never running).
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)
    conn = _setup_cluster_json_object(env)
    # `{s0}` and `{s1}` route to different shards (same convention as the HASH
    # cross-shard test above). Same payload, different scores.
    conn.execute_command('JSON.SET', 'j:0{s0}', '$',
                         '{"grp":"G","score":1,"payload":{"a":1,"b":2}}')
    conn.execute_command('JSON.SET', 'j:1{s1}', '$',
                         '{"grp":"G","score":5,"payload":{"a":1,"b":2}}')

    res = _aggregate_collect_json_payload(env)
    env.assertEqual(len(res['results']), 1)
    payloads = res['results'][0]['extra_attributes']['payloads']
    env.assertEqual(len(payloads), 1)


@skip(cluster=False, no_json=True)
def test_collect_distinct_cluster_cross_shard_json_dedups_by_string_not_structure():
    # Two docs on DIFFERENT shards carry the SAME logical `payload` object but
    # with keys in a DIFFERENT order. Dedup is not deep structural equality: on
    # the coordinator the payload arrives wire-serialized, and the two serialize
    # to different strings, so the coordinator keeps BOTH — the same
    # not-structurally-deduped outcome as the single-shard
    # `test_collect_distinct_json_object_not_structurally_deduped`. This pins
    # that the cross-shard path does not diverge from the single-shard one.
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)
    conn = _setup_cluster_json_object(env)
    conn.execute_command('JSON.SET', 'j:0{s0}', '$',
                         '{"grp":"G","score":1,"payload":{"a":1,"b":2}}')
    conn.execute_command('JSON.SET', 'j:1{s1}', '$',
                         '{"grp":"G","score":5,"payload":{"b":2,"a":1}}')

    res = _aggregate_collect_json_payload(env)
    env.assertEqual(len(res['results']), 1)
    payloads = res['results'][0]['extra_attributes']['payloads']
    env.assertEqual(len(payloads), 2)
