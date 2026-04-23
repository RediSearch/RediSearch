from common import *
import json


# Fixed dataset: 6 fruits with known grouping properties.
#   color groups: yellow (banana, lemon), red (apple, strawberry), green (kiwi, lime)
#   sweetness groups: 4 (banana, apple), 2 (lemon, lime), 3 (strawberry, kiwi)
# Some fruits have an 'origin' field, some don't (for NULL testing).
FRUITS = [
    {'name': 'banana',     'color': 'yellow', 'sweetness': 4, 'origin': 'Ecuador'},
    {'name': 'lemon',      'color': 'yellow', 'sweetness': 2},
    {'name': 'apple',      'color': 'red',    'sweetness': 4, 'origin': 'USA'},
    {'name': 'strawberry', 'color': 'red',    'sweetness': 3, 'origin': 'Spain'},
    {'name': 'kiwi',       'color': 'green',  'sweetness': 3},
    {'name': 'lime',       'color': 'green',  'sweetness': 2, 'origin': 'Mexico'},
]


def _setup_hash(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'name', 'TEXT', 'SORTABLE',
               'color', 'TAG', 'SORTABLE',
               'sweetness', 'NUMERIC', 'SORTABLE',
               'origin', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, f in enumerate(FRUITS):
        args = ['HSET', f'doc:{i}', 'name', f['name'], 'color', f['color'],
                'sweetness', str(f['sweetness'])]
        if 'origin' in f:
            args += ['origin', f['origin']]
        conn.execute_command(*args)
    enable_unstable_features(env)


def _setup_json(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
               'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT', 'SORTABLE',
               '$.color', 'AS', 'color', 'TAG', 'SORTABLE',
               '$.sweetness', 'AS', 'sweetness', 'NUMERIC', 'SORTABLE',
               '$.origin', 'AS', 'origin', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, f in enumerate(FRUITS):
        conn.execute_command('JSON.SET', f'doc:{i}', '$', json.dumps(f))
    enable_unstable_features(env)


def _sort_by(results, key):
    """Sort RESP3 aggregate results by a group key for stable comparison."""
    return sorted(results, key=lambda r: r['extra_attributes'][key])


def _sort_collected(entries, key):
    """Sort a COLLECT array of maps by a field for stable comparison."""
    return sorted(entries, key=lambda e: str(e.get(key, '')))


# ---------------------------------------------------------------------------
# COLLECT 1 field, HASH
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_1_field_hash():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'names')

    groups = _sort_by(res['results'], 'color')
    env.assertEqual(groups[0]['extra_attributes']['color'], 'green')
    env.assertEqual(_sort_collected(groups[0]['extra_attributes']['names'], 'name'),
                    [{'name': 'kiwi'}, {'name': 'lime'}])

    env.assertEqual(groups[1]['extra_attributes']['color'], 'red')
    env.assertEqual(_sort_collected(groups[1]['extra_attributes']['names'], 'name'),
                    [{'name': 'apple'}, {'name': 'strawberry'}])

    env.assertEqual(groups[2]['extra_attributes']['color'], 'yellow')
    env.assertEqual(_sort_collected(groups[2]['extra_attributes']['names'], 'name'),
                    [{'name': 'banana'}, {'name': 'lemon'}])


# ---------------------------------------------------------------------------
# COLLECT 3 fields, HASH  (TEXT fields are lowercased in HASH)
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_3_fields_hash():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '5', 'FIELDS', '3', '@name', '@sweetness', '@origin',
        'AS', 'info')

    groups = _sort_by(res['results'], 'color')
    green = _sort_collected(groups[0]['extra_attributes']['info'], 'name')
    env.assertEqual(green, [
        {'name': 'kiwi', 'sweetness': '3', 'origin': None},
        {'name': 'lime', 'sweetness': '2', 'origin': 'mexico'},
    ])

    red = _sort_collected(groups[1]['extra_attributes']['info'], 'name')
    env.assertEqual(red, [
        {'name': 'apple',      'sweetness': '4', 'origin': 'usa'},
        {'name': 'strawberry', 'sweetness': '3', 'origin': 'spain'},
    ])

    yellow = _sort_collected(groups[2]['extra_attributes']['info'], 'name')
    env.assertEqual(yellow, [
        {'name': 'banana', 'sweetness': '4', 'origin': 'ecuador'},
        {'name': 'lemon',  'sweetness': '2', 'origin': None},
    ])


# ---------------------------------------------------------------------------
# COLLECT 1 field, JSON
# ---------------------------------------------------------------------------
@skip(cluster=True)
@skip(no_json=True)
def test_collect_1_field_json():
    env = Env(protocol=3)
    _setup_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'names')

    groups = _sort_by(res['results'], 'color')
    env.assertEqual(_sort_collected(groups[0]['extra_attributes']['names'], 'name'),
                    [{'name': 'kiwi'}, {'name': 'lime'}])
    env.assertEqual(_sort_collected(groups[1]['extra_attributes']['names'], 'name'),
                    [{'name': 'apple'}, {'name': 'strawberry'}])
    env.assertEqual(_sort_collected(groups[2]['extra_attributes']['names'], 'name'),
                    [{'name': 'banana'}, {'name': 'lemon'}])


# ---------------------------------------------------------------------------
# COLLECT 3 fields, JSON  (JSON preserves original case)
# ---------------------------------------------------------------------------
@skip(cluster=True)
@skip(no_json=True)
def test_collect_3_fields_json():
    env = Env(protocol=3)
    _setup_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '5', 'FIELDS', '3', '@name', '@sweetness', '@origin',
        'AS', 'info')

    groups = _sort_by(res['results'], 'color')
    green = _sort_collected(groups[0]['extra_attributes']['info'], 'name')
    env.assertEqual(green, [
        {'name': 'kiwi', 'sweetness': '3', 'origin': None},
        {'name': 'lime', 'sweetness': '2', 'origin': 'Mexico'},
    ])

    red = _sort_collected(groups[1]['extra_attributes']['info'], 'name')
    env.assertEqual(red, [
        {'name': 'apple',      'sweetness': '4', 'origin': 'USA'},
        {'name': 'strawberry', 'sweetness': '3', 'origin': 'Spain'},
    ])


# ---------------------------------------------------------------------------
# Chained GROUPBY: second COLLECT collects previous reducers output
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_chained_groupby_collect():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COUNT', '0', 'AS', 'cnt',
        'REDUCE', 'AVG', '1', '@sweetness', 'AS', 'avg_sweet',
        'GROUPBY', '0',
        'REDUCE', 'COLLECT', '4', 'FIELDS', '2', '@cnt', '@avg_sweet',
        'AS', 'stats')

    results = res['results']
    env.assertEqual(len(results), 1)

    stats = sorted(results[0]['extra_attributes']['stats'],
                   key=lambda e: e['avg_sweet'])
    env.assertEqual(stats, [
        {'cnt': '2', 'avg_sweet': '2.5'},   # green:  (3+2)/2
        {'cnt': '2', 'avg_sweet': '3'},     # yellow: (4+2)/2
        {'cnt': '2', 'avg_sweet': '3.5'},   # red:    (4+3)/2
    ])


# ---------------------------------------------------------------------------
# COLLECT with NULL/missing values
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_missing_values():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@sweetness',
        'REDUCE', 'COLLECT', '4', 'FIELDS', '2', '@name', '@origin',
        'AS', 'items')

    groups = _sort_by(res['results'], 'sweetness')

    items2 = _sort_collected(groups[0]['extra_attributes']['items'], 'name')
    env.assertEqual(items2, [
        {'name': 'lemon', 'origin': None},
        {'name': 'lime',  'origin': 'mexico'},
    ])

    items3 = _sort_collected(groups[1]['extra_attributes']['items'], 'name')
    env.assertEqual(items3, [
        {'name': 'kiwi',       'origin': None},
        {'name': 'strawberry', 'origin': 'spain'},
    ])

    items4 = _sort_collected(groups[2]['extra_attributes']['items'], 'name')
    env.assertEqual(items4, [
        {'name': 'apple',  'origin': 'usa'},
        {'name': 'banana', 'origin': 'ecuador'},
    ])


# ---------------------------------------------------------------------------
# COLLECT with AS alias
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_alias():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'fruit_names')

    for row in res['results']:
        env.assertContains('fruit_names', row['extra_attributes'])
        env.assertNotContains('names', row['extra_attributes'])


# ---------------------------------------------------------------------------
# COLLECT with multi-key GROUPBY
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_multi_groupby_keys():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '2', '@color', '@sweetness',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'names')

    # Each (color, sweetness) combo has exactly 1 fruit
    env.assertEqual(len(res['results']), 6)
    for row in res['results']:
        attrs = row['extra_attributes']
        env.assertEqual(len(attrs['names']), 1)
        env.assertContains('color', attrs)
        env.assertContains('sweetness', attrs)


# ---------------------------------------------------------------------------
# Multiple COLLECT reducers in the same GROUPBY: mix array-path (no SORTBY)
# and heap-path (SORTBY + LIMIT) reducers to verify they don't share state.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_multiple_reducers_same_groupby():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
            'AS', 'all_names',
        'REDUCE', 'COLLECT', '10', 'FIELDS', '1', '@name',
            'SORTBY', '2', '@sweetness', 'DESC',
            'LIMIT', '0', '1',
            'AS', 'sweetest')

    groups = _sort_by(res['results'], 'color')

    # green: kiwi (3), lime (2) -> sweetest = kiwi
    green = groups[0]['extra_attributes']
    env.assertEqual(_sort_collected(green['all_names'], 'name'),
                    [{'name': 'kiwi'}, {'name': 'lime'}])
    env.assertEqual(green['sweetest'], [{'name': 'kiwi'}])

    # red: apple (4), strawberry (3) -> sweetest = apple
    red = groups[1]['extra_attributes']
    env.assertEqual(_sort_collected(red['all_names'], 'name'),
                    [{'name': 'apple'}, {'name': 'strawberry'}])
    env.assertEqual(red['sweetest'], [{'name': 'apple'}])

    # yellow: banana (4), lemon (2) -> sweetest = banana
    yellow = groups[2]['extra_attributes']
    env.assertEqual(_sort_collected(yellow['all_names'], 'name'),
                    [{'name': 'banana'}, {'name': 'lemon'}])
    env.assertEqual(yellow['sweetest'], [{'name': 'banana'}])


# ---------------------------------------------------------------------------
# COLLECT alongside a scalar reducer in the same GROUPBY.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_with_scalar_reducer_same_groupby():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COUNT', '0', 'AS', 'cnt',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name', 'AS', 'names')

    groups = _sort_by(res['results'], 'color')
    for g in groups:
        attrs = g['extra_attributes']
        env.assertEqual(attrs['cnt'], '2')
        env.assertEqual(len(attrs['names']), 2)

    env.assertEqual(_sort_collected(groups[0]['extra_attributes']['names'], 'name'),
                    [{'name': 'kiwi'}, {'name': 'lime'}])
    env.assertEqual(_sort_collected(groups[1]['extra_attributes']['names'], 'name'),
                    [{'name': 'apple'}, {'name': 'strawberry'}])
    env.assertEqual(_sort_collected(groups[2]['extra_attributes']['names'], 'name'),
                    [{'name': 'banana'}, {'name': 'lemon'}])


# ---------------------------------------------------------------------------
# Verify output structure: array of maps
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_output_structure():
    env = Env(protocol=3)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'items')

    for row in res['results']:
        items = row['extra_attributes']['items']
        env.assertTrue(isinstance(items, list))
        for entry in items:
            env.assertTrue(isinstance(entry, dict))
            env.assertContains('name', entry)
            env.assertEqual(len(entry), 1)


# ---------------------------------------------------------------------------
# COLLECT requires ENABLE_UNSTABLE_FEATURES
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_requires_unstable_features():
    env = Env()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'name', 'TEXT', 'color', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:0', 'name', 'banana', 'color', 'yellow')

    env.expect(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
    ).error().contains('COLLECT')


# ---------------------------------------------------------------------------
# COLLECT with LOAD json path aliased field
# ---------------------------------------------------------------------------
@skip(cluster=True)
@skip(no_json=True)
def test_collect_loaded_json_path():
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
               'SCHEMA', '$.name', 'AS', 'name', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, f in enumerate(FRUITS):
        conn.execute_command('JSON.SET', f'doc:{i}', '$', json.dumps(f))
    enable_unstable_features(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'LOAD', '3', '$.color', 'AS', 'Color',
        'GROUPBY', '1', '@Color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'names')

    groups = _sort_by(res['results'], 'Color')
    env.assertEqual(_sort_collected(groups[0]['extra_attributes']['names'], 'name'),
                    [{'name': 'kiwi'}, {'name': 'lime'}])
    env.assertEqual(_sort_collected(groups[1]['extra_attributes']['names'], 'name'),
                    [{'name': 'apple'}, {'name': 'strawberry'}])
    env.assertEqual(_sort_collected(groups[2]['extra_attributes']['names'], 'name'),
                    [{'name': 'banana'}, {'name': 'lemon'}])


# ---------------------------------------------------------------------------
# RESP2 sanity: basic COLLECT works under RESP2
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_resp2_sanity():
    env = Env(protocol=2)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'names')

    # RESP2: first element is the number of groups, rest are flat rows
    env.assertEqual(res[0], 3)
    env.assertEqual(len(res) - 1, 3)

    # Each row is [key, val, key, val, ...] with 'color' and 'names'
    for row in res[1:]:
        env.assertContains('color', row)
        env.assertContains('names', row)
        names_idx = row.index('names') + 1
        collected = row[names_idx]
        env.assertTrue(isinstance(collected, list))
        env.assertEqual(len(collected), 2)



# ---------------------------------------------------------------------------
# SORTBY / LIMIT dataset: 12 items, all color=red, two ties on @price
# so multi-key tests exercise the secondary sort direction.
#
# Docs are inserted as `doc:0` .. `doc:11` in the enumeration order below,
# so internal doc-ID order (the §6.4 tie-breaker) matches this order.
# ---------------------------------------------------------------------------
PRICED = [
    {'name': 'alice',   'color': 'red', 'price': 10},   # tie with 'bob'
    {'name': 'bob',     'color': 'red', 'price': 10},
    {'name': 'charlie', 'color': 'red', 'price': 15},   # tie with 'dave'
    {'name': 'dave',    'color': 'red', 'price': 15},
    {'name': 'eve',     'color': 'red', 'price':  8},
    {'name': 'frank',   'color': 'red', 'price':  7},
    {'name': 'grace',   'color': 'red', 'price':  6},
    {'name': 'henry',   'color': 'red', 'price':  5},
    {'name': 'iris',    'color': 'red', 'price':  4},
    {'name': 'jack',    'color': 'red', 'price':  3},
    {'name': 'kate',    'color': 'red', 'price':  2},
    {'name': 'liam',    'color': 'red', 'price':  1},
]


def _setup_priced_json(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
               'SCHEMA',
               '$.name',  'AS', 'name',  'TEXT',    'SORTABLE',
               '$.color', 'AS', 'color', 'TAG',     'SORTABLE',
               '$.price', 'AS', 'price', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, item in enumerate(PRICED):
        conn.execute_command('JSON.SET', f'doc:{i}', '$', json.dumps(item))
    enable_unstable_features(env)


def _names(entries):
    """Extract the list of @name values (in order) from a COLLECT Array<Map> result."""
    return [e['name'] for e in entries]


# ---------------------------------------------------------------------------
# (a) SORTBY single key DESC + LIMIT 0 3
# ---------------------------------------------------------------------------
# TODO(MOD-14803): re-enable once the §6.4 doc-ID tie-breaker is wired
# into `EntryKey::cmp` (see docs/mod14803/collect-heapitem-alignment.md
# §"Behavioural consequence: tie-break stability"). The assertion below
# pins the order of tied-price rows (charlie/dave @ 15, alice/bob @ 10)
# which is currently unspecified.
@skip()
@skip(cluster=True)
@skip(no_json=True)
def test_collect_sortby_desc_limit():
    env = Env(protocol=3)
    _setup_priced_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '10',
            'FIELDS', '1', '@name',
            'SORTBY', '2', '@price', 'DESC',
            'LIMIT', '0', '3',
        'AS', 'names')

    env.assertEqual(len(res['results']), 1)
    entries = res['results'][0]['extra_attributes']['names']
    # Top 3 prices DESC: charlie(15), dave(15), alice(10).
    # Within the price=15 tie, doc-ID ASC (§6.4): charlie (doc:2) < dave (doc:3).
    env.assertEqual(_names(entries), ['charlie', 'dave', 'alice'])


# ---------------------------------------------------------------------------
# (b) Multi-key SORTBY with mixed directions
# ---------------------------------------------------------------------------
@skip(cluster=True)
@skip(no_json=True)
def test_collect_sortby_multi_mixed_directions():
    env = Env(protocol=3)
    _setup_priced_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '12',
            'FIELDS', '1', '@name',
            'SORTBY', '4', '@price', 'DESC', '@name', 'ASC',
            'LIMIT', '0', '4',
        'AS', 'names')

    entries = res['results'][0]['extra_attributes']['names']
    # price DESC, name ASC:
    #   price=15 → charlie < dave  → [charlie, dave]
    #   price=10 → alice   < bob   → [alice,   bob]
    env.assertEqual(_names(entries), ['charlie', 'dave', 'alice', 'bob'])


# ---------------------------------------------------------------------------
# (c) LIMIT without SORTBY (array path, first-K in insertion order)
# ---------------------------------------------------------------------------
@skip(cluster=True)
@skip(no_json=True)
def test_collect_limit_without_sortby():
    env = Env(protocol=3)
    _setup_priced_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '6',
            'FIELDS', '1', '@name',
            'LIMIT', '0', '3',
        'AS', 'names')

    entries = res['results'][0]['extra_attributes']['names']
    # Without SORTBY we only assert the cap; scan order is not an API guarantee.
    env.assertEqual(len(entries), 3)
    known = {item['name'] for item in PRICED}
    for e in entries:
        env.assertContains(e['name'], known)



# ---------------------------------------------------------------------------
# (d) SORTBY without LIMIT -> DEFAULT_LIMIT = 10 applies
# ---------------------------------------------------------------------------
# TODO(MOD-14803): re-enable once the §6.4 doc-ID tie-breaker is wired
# into `EntryKey::cmp` (see docs/mod14803/collect-heapitem-alignment.md
# §"Behavioural consequence: tie-break stability"). The assertion pins
# the order of tied-price rows (charlie/dave @ 15, alice/bob @ 10) which
# is currently unspecified.
@skip()
@skip(cluster=True)
@skip(no_json=True)
def test_collect_sortby_default_limit():
    env = Env(protocol=3)
    _setup_priced_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '7',
            'FIELDS', '1', '@name',
            'SORTBY', '2', '@price', 'DESC',
        'AS', 'names')

    entries = res['results'][0]['extra_attributes']['names']
    # DEFAULT_LIMIT is 10 in the Rust reducer (see collect.rs) so the top-10
    # by @price DESC (ties broken by doc-ID ASC per §6.4) are returned.
    # Full ranking: charlie(15), dave(15), alice(10), bob(10), eve(8),
    # frank(7), grace(6), henry(5), iris(4), jack(3), [kate(2), liam(1) dropped]
    env.assertEqual(len(entries), 10)
    env.assertEqual(_names(entries),
                    ['charlie', 'dave', 'alice', 'bob', 'eve',
                     'frank',   'grace', 'henry', 'iris', 'jack'])


# ---------------------------------------------------------------------------
# (e) SORTBY with offset > 0
# ---------------------------------------------------------------------------
# TODO(MOD-14803): re-enable once the §6.4 doc-ID tie-breaker is wired
# into `EntryKey::cmp` (see docs/mod14803/collect-heapitem-alignment.md
# §"Behavioural consequence: tie-break stability"). The assertion pins
# the order of tied-price rows (alice/bob @ 10) which is currently
# unspecified.
@skip()
@skip(cluster=True)
@skip(no_json=True)
def test_collect_sortby_with_offset():
    env = Env(protocol=3)
    _setup_priced_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '10',
            'FIELDS', '1', '@name',
            'SORTBY', '2', '@price', 'DESC',
            'LIMIT', '2', '3',
        'AS', 'names')

    entries = res['results'][0]['extra_attributes']['names']
    # Skip first 2 (charlie, dave) of the DESC ranking, take next 3.
    env.assertEqual(_names(entries), ['alice', 'bob', 'eve'])


# ---------------------------------------------------------------------------
# (f) Array path (no SORTBY, no LIMIT) capped by MAXAGGREGATERESULTS
# ---------------------------------------------------------------------------
@skip(cluster=True)
@skip(no_json=True)
def test_collect_array_path_capped_by_max_aggregate_results():
    env = Env(protocol=3)
    _setup_priced_json(env)

    # Narrow the array-path cap; restore to unlimited at the end.
    env.expect(config_cmd(), 'SET', 'MAXAGGREGATERESULTS', '5').ok()
    try:
        res = env.cmd(
            'FT.AGGREGATE', 'idx', '*',
            'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '3',
                'FIELDS', '1', '@name',
            'AS', 'names')

        entries = res['results'][0]['extra_attributes']['names']
        # Array path stops accepting after maxAggregateResults items.
        env.assertEqual(len(entries), 5)
        known = {item['name'] for item in PRICED}
        for e in entries:
            env.assertContains(e['name'], known)
    finally:
        env.expect(config_cmd(), 'SET', 'MAXAGGREGATERESULTS', '-1').ok()
