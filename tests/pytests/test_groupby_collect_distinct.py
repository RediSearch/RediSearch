from common import *
import json

# End-to-end tests for `REDUCE COLLECT … DISTINCT` (MOD-14806). Each test pins one
# behavior with inline data and a direct assertion.
#
# DISTINCT dedups on the *projected* fields only. With SORTBY, the sort key is the
# deferred ranking key (not part of the identity), so duplicate rows collapse to
# their best-ranked representative; without SORTBY, the first arrival is kept.


def _group(res, field, value):
    """The extra_attributes of the result group whose `field` equals `value`."""
    for r in res['results']:
        if r['extra_attributes'][field] == value:
            return r['extra_attributes']
    raise AssertionError(f'group {field}={value} not found')


# ---------------------------------------------------------------------------
# Basic dedup, HASH, no SORTBY.
# ---------------------------------------------------------------------------
def test_collect_distinct_hash():
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'g', 'TAG', 'SORTABLE', 'v', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    rows = [('a', 1), ('a', 1), ('a', 2), ('a', 2), ('a', 3), ('b', 5), ('b', 5)]
    for i, (g, v) in enumerate(rows):
        conn.execute_command('HSET', f'doc:{i}', 'g', g, 'v', v)

    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'GROUPBY', '1', '@g',
                  'REDUCE', 'COLLECT', '4', 'FIELDS', '1', '@v', 'DISTINCT', 'AS', 'vals')

    a = sorted(int(e['v']) for e in _group(res, 'g', 'a')['vals'])
    b = sorted(int(e['v']) for e in _group(res, 'g', 'b')['vals'])
    env.assertEqual(a, [1, 2, 3])
    env.assertEqual(b, [5])


# ---------------------------------------------------------------------------
# Dedup entire JSON documents loaded dynamically (non-indexed) via `LOAD $`.
# ---------------------------------------------------------------------------
@skip(no_json=True)
def test_collect_distinct_json_whole_doc():
    env = Env(protocol=3)
    # Only `cat` is indexed (for grouping); the deduped value is the whole doc.
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
               'SCHEMA', '$.cat', 'AS', 'cat', 'TAG', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    apple = {'cat': 'fruit', 'name': 'apple'}
    banana = {'cat': 'fruit', 'name': 'banana'}
    for i, doc in enumerate([apple, apple, banana]):  # apple appears twice
        conn.execute_command('JSON.SET', f'doc:{i}', '$', json.dumps(doc))

    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'LOAD', '3', '$', 'AS', 'doc',
                  'GROUPBY', '1', '@cat',
                  'REDUCE', 'COLLECT', '4', 'FIELDS', '1', '@doc', 'DISTINCT', 'AS', 'docs')

    # `$` comes back as a JSON string (RedisJSON wraps single paths in an array).
    docs = []
    for e in _group(res, 'cat', 'fruit')['docs']:
        parsed = json.loads(e['doc'])
        docs.append(parsed[0] if isinstance(parsed, list) else parsed)
    env.assertEqual(sorted(d['name'] for d in docs), ['apple', 'banana'])


# ---------------------------------------------------------------------------
# `FIELDS *` keys on the whole loaded row, including non-indexed loaded fields.
# ---------------------------------------------------------------------------
def test_collect_distinct_star_includes_loaded_fields():
    env = Env(protocol=3)
    # a/b/c are indexed; d is not — it only enters the row via LOAD. g groups.
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'g', 'TAG', 'a', 'TAG', 'b', 'TAG', 'c', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'g', 'G', 'a', '1', 'b', '1', 'c', '1')
    conn.execute_command('HSET', 'doc:2', 'g', 'G', 'a', '1', 'b', '1', 'c', '1', 'd', '1')

    # On the indexed fields a/b/c the two docs are identical → dedup to one.
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'GROUPBY', '1', '@g',
                  'REDUCE', 'COLLECT', '6',
                      'FIELDS', '3', '@a', '@b', '@c', 'DISTINCT', 'AS', 'rows')
    env.assertEqual(len(res['results'][0]['extra_attributes']['rows']), 1)

    # `FIELDS *` with d loaded distinguishes them (doc:2 has d, doc:1 doesn't).
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'LOAD', '1', '@d',
                  'GROUPBY', '1', '@g',
                  'REDUCE', 'COLLECT', '3', 'FIELDS', '*', 'DISTINCT', 'AS', 'rows')
    env.assertEqual(len(res['results'][0]['extra_attributes']['rows']), 2)


# ---------------------------------------------------------------------------
# SORTBY + LIMIT bound the ranked, deduped set.
# ---------------------------------------------------------------------------
def test_collect_distinct_sortby_limit():
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'g', 'TAG', 'SORTABLE', 'v', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, v in enumerate([1, 1, 2, 2, 3]):
        conn.execute_command('HSET', f'doc:{i}', 'g', 'G', 'v', v)

    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'GROUPBY', '1', '@g',
                  'REDUCE', 'COLLECT', '11',
                      'FIELDS', '1', '@v', 'SORTBY', '2', '@v', 'ASC', 'LIMIT', '0', '2',
                      'DISTINCT', 'AS', 'vals')

    # distinct ascending = [1,2,3]; LIMIT 0 2 -> [1,2]. (Single group.)
    vals = res['results'][0]['extra_attributes']['vals']
    env.assertEqual([int(e['v']) for e in vals], [1, 2])


# ---------------------------------------------------------------------------
# DISTINCT keeps the best (smallest-SORTBY) representative per identity,
# independent of arrival interleaving. Output order reflects that best key.
# ---------------------------------------------------------------------------
def test_collect_distinct_keeps_best_representative():
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'grp', 'TAG', 'SORTABLE',
               'cat', 'TAG', 'SORTABLE', 'p', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)

    def collected_cats(docs):
        conn.execute_command('FLUSHALL')
        env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
                   'SCHEMA', 'grp', 'TAG', 'SORTABLE',
                   'cat', 'TAG', 'SORTABLE', 'p', 'NUMERIC', 'SORTABLE').ok()
        for i, (cat, p) in enumerate(docs):
            conn.execute_command('HSET', f'doc:{i}', 'grp', 'G', 'cat', cat, 'p', p)
        res = env.cmd('FT.AGGREGATE', 'idx', '*',
                      'GROUPBY', '1', '@grp',
                      'REDUCE', 'COLLECT', '8',
                          'FIELDS', '1', '@cat', 'SORTBY', '2', '@p', 'ASC',
                          'DISTINCT', 'AS', 'cats')
        return [e['cat'] for e in res['results'][0]['extra_attributes']['cats']]

    # All cases expect ['a', 'b']: a's best p (1) < b's best p (2).
    env.assertEqual(collected_cats([('a', 1), ('b', 2), ('a', 3), ('b', 4)]), ['a', 'b'])
    env.assertEqual(collected_cats([('a', 1), ('b', 2), ('b', 3), ('a', 4)]), ['a', 'b'])
    env.assertEqual(collected_cats([('a', 1), ('b', 2), ('a', 3), ('a', 4)]), ['a', 'b'])
    # Best representative arrives late: a is first seen at p=3 but its best is p=1.
    # A keep-first bug would rank a at 3 and return ['b', 'a'].
    env.assertEqual(collected_cats([('b', 2), ('a', 3), ('a', 1)]), ['a', 'b'])


# ---------------------------------------------------------------------------
# Dedup is per-group, not global: a doc landing in two groups is not deduped
# across them, but duplicates within a group still collapse.
# ---------------------------------------------------------------------------
def test_collect_distinct_is_per_group_not_global():
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'groups', 'TEXT', 'v', 'TAG', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'd0', 'groups', 'g1,g2', 'v', 'x')  # lands in g1 and g2
    conn.execute_command('HSET', 'd1', 'groups', 'g1', 'v', 'x')     # duplicate of d0 in g1

    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'LOAD', '1', '@groups',
                  'APPLY', 'split(@groups, ",")', 'AS', 'grp',
                  'GROUPBY', '1', '@grp',
                  'REDUCE', 'COLLECT', '4', 'FIELDS', '1', '@v', 'DISTINCT', 'AS', 'vals')

    # g1: d0 and d1 dedup to one x. g2: x is present again — not removed by g1's copy.
    env.assertEqual([e['v'] for e in _group(res, 'grp', 'g1')['vals']], ['x'])
    env.assertEqual([e['v'] for e in _group(res, 'grp', 'g2')['vals']], ['x'])


# ---------------------------------------------------------------------------
# DISTINCT dedups at the outer stage of a stacked GROUPBY.
# ---------------------------------------------------------------------------
def test_collect_distinct_outer_groupby():
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'color', 'TAG', 'SORTABLE', 'name', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    # red -> 2 docs, yellow -> 2 docs, green -> 1 doc (counts 2, 2, 1).
    docs = [('red', 'apple'), ('red', 'cherry'),
            ('yellow', 'banana'), ('yellow', 'lemon'),
            ('green', 'lime')]
    for i, (color, name) in enumerate(docs):
        conn.execute_command('HSET', f'doc:{i}', 'color', color, 'name', name)

    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'GROUPBY', '1', '@color',
                      'REDUCE', 'COUNT', '0', 'AS', 'cnt',
                  'GROUPBY', '0',
                      'REDUCE', 'COLLECT', '4', 'FIELDS', '1', '@cnt', 'DISTINCT', 'AS', 'cnts')

    # Three inner groups produce counts {2, 2, 1}; the outer DISTINCT collapses to {1, 2}.
    cnts = sorted(int(e['cnt']) for e in res['results'][0]['extra_attributes']['cnts'])
    env.assertEqual(cnts, [1, 2])


# ---------------------------------------------------------------------------
# Cluster: duplicates spread across shards dedup at the coordinator merge,
# keeping the best representative.
# ---------------------------------------------------------------------------
@skip(cluster=False)
def test_collect_distinct_cluster_cross_shard():
    env = Env(shardsCount=3, protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'grp', 'TAG', 'SORTABLE',
               'cat', 'TAG', 'SORTABLE', 'p', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    # Each key lands on a different shard, so the two cat=a docs straddle a
    # boundary and the coordinator must dedup across shards and keep the best
    # (p=1).
    tags = distinct_shard_tags(conn)
    docs = [(f'd:0{{{next(tags)}}}', 'a', 5),
            (f'd:1{{{next(tags)}}}', 'a', 1),
            (f'd:2{{{next(tags)}}}', 'b', 3)]
    for key, cat, p in docs:
        conn.execute_command('HSET', key, 'grp', 'G', 'cat', cat, 'p', p)

    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'GROUPBY', '1', '@grp',
                  'REDUCE', 'COLLECT', '8',
                      'FIELDS', '1', '@cat', 'SORTBY', '2', '@p', 'ASC',
                      'DISTINCT', 'AS', 'cats')

    # a kept at its best p=1, so a (1) sorts before b (3). (Single group.)
    cats = res['results'][0]['extra_attributes']['cats']
    env.assertEqual([e['cat'] for e in cats], ['a', 'b'])
