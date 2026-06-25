from common import *

# Basic end-to-end coverage for `REDUCE COLLECT … DISTINCT` (MOD-14806).
#
# This is the setup stage: the dedup identity for a projected row is a temporary
# naive placeholder (see `ProjectedRow::identity_repr` on the Rust side), so these
# tests assert the flag parses in both COLLECT shapes (with and without SORTBY) and
# that duplicate projected values collapse on a single shard. Full semantic and
# cluster coverage lands with the final field-aware identity.

# group 'a': sweetness 10,10,20,20,30 -> distinct {10,20,30}
# group 'b': sweetness 5,5            -> distinct {5}
DOCS = [
    ('a', 10), ('a', 10), ('a', 20), ('a', 20), ('a', 30),
    ('b', 5), ('b', 5),
]


def _setup(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'g', 'TAG', 'SORTABLE',
               'v', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, (g, v) in enumerate(DOCS):
        conn.execute_command('HSET', f'doc:{i}', 'g', g, 'v', str(v))


def _collected(group, alias):
    """Sorted list of the scalar `v` values from a COLLECT result array."""
    return sorted(int(e['v']) for e in group['extra_attributes'][alias])


def _group(res, g):
    for r in res['results']:
        if r['extra_attributes']['g'] == g:
            return r
    raise AssertionError(f'group {g} not found')


# ---------------------------------------------------------------------------
# DISTINCT without SORTBY (hash-set storage): dedup, first arrival per identity.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_distinct_no_sortby():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@g',
        'REDUCE', 'COLLECT', '4', 'FIELDS', '1', '@v', 'DISTINCT',
        'AS', 'vals')

    env.assertEqual(_collected(_group(res, 'a'), 'vals'), [10, 20, 30])
    env.assertEqual(_collected(_group(res, 'b'), 'vals'), [5])


# ---------------------------------------------------------------------------
# DISTINCT with SORTBY (priority-queue storage): dedup + ranked top-K.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_distinct_with_sortby():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@g',
        'REDUCE', 'COLLECT', '7', 'FIELDS', '1', '@v', 'SORTBY', '1', '@v', 'DISTINCT',
        'AS', 'vals')

    # Distinct values, ranked: group 'a' -> {10,20,30}, group 'b' -> {5}.
    group_a = [int(e['v']) for e in _group(res, 'a')['extra_attributes']['vals']]
    env.assertEqual(sorted(group_a), [10, 20, 30])
    env.assertEqual(_collected(_group(res, 'b'), 'vals'), [5])


# ---------------------------------------------------------------------------
# DISTINCT + SORTBY + LIMIT: cap applies on the ranked, deduped set.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_distinct_with_sortby_limit():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@g',
        'REDUCE', 'COLLECT', '11',
        'FIELDS', '1', '@v', 'SORTBY', '2', '@v', 'ASC', 'LIMIT', '0', '2', 'DISTINCT',
        'AS', 'vals')

    # group 'a' distinct ascending = [10,20,30]; LIMIT 0 2 -> [10,20].
    group_a = [int(e['v']) for e in _group(res, 'a')['extra_attributes']['vals']]
    env.assertEqual(group_a, [10, 20])


# ---------------------------------------------------------------------------
# Non-distinct COLLECT is unaffected (regression guard for the new flag).
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_without_distinct_keeps_duplicates():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@g',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@v',
        'AS', 'vals')

    # Without DISTINCT, group 'a' keeps all five rows (duplicates included).
    env.assertEqual(_collected(_group(res, 'a'), 'vals'), [10, 10, 20, 20, 30])
