from common import *
import json

# End-to-end coverage for `REDUCE COLLECT … DISTINCT` (MOD-14806), exercising the
# full matrix: HASH/JSON × `FIELDS *`/specific × SORTBY/no-SORTBY, on a single
# node and across cluster shards.
#
# Dataset: within group 'a', `v` and `t` are 1:1 (1↔x, 2↔y, 3↔z) and rows repeat,
# so deduping on `@v` alone or on the whole projected row (`FIELDS *`) both yield
# the same three distinct rows. Group 'b' collapses to one. One dataset therefore
# drives every cell, asserting the deduped `v` values per group.
#   (g, v, t)
ROWS = [
    ('a', 1, 'x'), ('a', 1, 'x'),
    ('a', 2, 'y'), ('a', 2, 'y'),
    ('a', 3, 'z'),
    ('b', 5, 'q'), ('b', 5, 'q'),
]
# Distinct `v` per group, ascending.
EXPECT = {'a': [1, 2, 3], 'b': [5]}


def _items(cluster):
    """`(key, g, v, t)` rows. In cluster mode each row gets a `{shard:N}` hash tag,
    spreading duplicate values across shards so the coordinator merge — not just a
    single shard — must deduplicate."""
    out = []
    for i, (g, v, t) in enumerate(ROWS):
        key = f'doc:{i}' + (f'{{shard:{i % 3}}}' if cluster else '')
        out.append((key, g, v, t))
    return out


def _create(env, doc_type):
    if doc_type == 'hash':
        env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
                   'g', 'TAG', 'SORTABLE',
                   'v', 'NUMERIC', 'SORTABLE',
                   't', 'TEXT', 'SORTABLE').ok()
    else:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
                   '$.g', 'AS', 'g', 'TAG', 'SORTABLE',
                   '$.v', 'AS', 'v', 'NUMERIC', 'SORTABLE',
                   '$.t', 'AS', 't', 'TEXT', 'SORTABLE').ok()


def _populate(conn, doc_type, items):
    for key, g, v, t in items:
        if doc_type == 'hash':
            conn.execute_command('HSET', key, 'g', g, 'v', str(v), 't', t)
        else:
            conn.execute_command('JSON.SET', key, '$', json.dumps({'g': g, 'v': v, 't': t}))


def _query(load_all, sortby, limit=None):
    """Build an FT.AGGREGATE … REDUCE COLLECT … DISTINCT command for one matrix
    cell. `FIELDS *` needs `v`/`t` in the rlookup, so they are LOADed upfront."""
    fields = ['FIELDS', '*'] if load_all else ['FIELDS', '1', '@v']
    sort = ['SORTBY', '2', '@v', 'ASC'] if sortby else []
    lim = ['LIMIT', str(limit[0]), str(limit[1])] if limit else []
    inner = fields + sort + lim + ['DISTINCT']
    cmd = ['FT.AGGREGATE', 'idx', '*']
    if load_all:
        cmd += ['LOAD', '2', '@v', '@t']
    cmd += ['GROUPBY', '1', '@g', 'REDUCE', 'COLLECT', str(len(inner))] + inner + ['AS', 'vals']
    return cmd


def _group(res, g):
    for r in res['results']:
        if r['extra_attributes']['g'] == g:
            return r
    raise AssertionError(f'group {g} not found')


def _vals(group, ordered):
    """Collected `v` values. Returned in emission order when `ordered` (SORTBY makes
    it deterministic), otherwise sorted (no-SORTBY order is unspecified, more so
    across shards)."""
    vs = [int(float(e['v'])) for e in group['extra_attributes']['vals']]
    return vs if ordered else sorted(vs)


def _run_matrix(env, doc_type, cluster):
    conn = getConnectionByEnv(env)
    items = _items(cluster)
    for load_all in (False, True):
        for sortby in (False, True):
            label = (f'{doc_type}/{"star" if load_all else "specific"}/'
                     f'{"sorted" if sortby else "unsorted"}')
            _create(env, doc_type)
            _populate(conn, doc_type, items)
            res = env.cmd(*_query(load_all, sortby))
            for g, expected in EXPECT.items():
                env.assertEqual(_vals(_group(res, g), sortby), expected,
                                message=f'{label} group {g}')
            env.expect('FT.DROPINDEX', 'idx', 'DD').ok()


# ---------------------------------------------------------------------------
# Single-node matrix
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_distinct_matrix_hash():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _run_matrix(env, 'hash', cluster=False)


@skip(cluster=True, no_json=True)
def test_collect_distinct_matrix_json():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _run_matrix(env, 'json', cluster=False)


# ---------------------------------------------------------------------------
# Cluster: duplicates spread across shards must dedup at the coordinator merge.
# (Doc-type is irrelevant to the merge — shard payloads have the same shape — so
# HASH covers it; the */specific × sorted/unsorted matrix still varies the local
# reducer's distinct path.)
# ---------------------------------------------------------------------------
@skip(cluster=False)
def test_collect_distinct_matrix_cluster():
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)
    _run_matrix(env, 'hash', cluster=True)


# ---------------------------------------------------------------------------
# LIMIT applies to the ranked, deduped set.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_distinct_sortby_limit():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _create(env, 'hash')
    _populate(getConnectionByEnv(env), 'hash', _items(cluster=False))

    res = env.cmd(*_query(load_all=False, sortby=True, limit=(0, 2)))
    # 'a' distinct ascending = [1,2,3]; LIMIT 0 2 -> [1,2].
    env.assertEqual(_vals(_group(res, 'a'), ordered=True), [1, 2])


# ---------------------------------------------------------------------------
# Regression: without DISTINCT, duplicates are kept.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_without_distinct_keeps_duplicates():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _create(env, 'hash')
    _populate(getConnectionByEnv(env), 'hash', _items(cluster=False))

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@g',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@v',
        'AS', 'vals')
    env.assertEqual(_vals(_group(res, 'a'), ordered=False), [1, 1, 2, 2, 3])
