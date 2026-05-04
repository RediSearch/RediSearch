from common import *
from test_hybrid_internal import get_shard_slot_ranges
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
# COLLECT coordinator merge across shards, HASH
# ---------------------------------------------------------------------------
@skip(cluster=False)
def test_collect_cluster_merges_same_group_across_shards():
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'sweetness', 'NUMERIC', 'SORTABLE',
               'color', 'TAG', 'SORTABLE').ok()

    conn = getConnectionByEnv(env)
    docs = [
        ('doc:1{shard:0}', 'from_shard_0', '1'),
        ('doc:2{shard:1}', 'from_shard_1', '2'),
        ('doc:3{shard:3}', 'from_shard_3', '3'),
    ]
    for key, name, sweetness in docs:
        conn.execute_command('HSET', key, 'name', name, 'color', 'shared',
                             'sweetness', sweetness)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '6',
                'FIELDS', '1', '@name',
                'SORTBY', '1', '@sweetness',
            'AS', 'names')

    env.assertEqual(len(res['results']), 1)
    attrs = res['results'][0]['extra_attributes']
    env.assertEqual(attrs['color'], 'shared')
    env.assertEqual(_sort_collected(attrs['names'], 'name'), [
        {'name': 'from_shard_0'},
        {'name': 'from_shard_1'},
        {'name': 'from_shard_3'},
    ])


# ---------------------------------------------------------------------------
# Chained GROUPBY
# ---------------------------------------------------------------------------
def test_collect_cluster_chained_groupby_collect():
    env = Env(protocol=3)
    enable_unstable_features(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'sweetness', 'NUMERIC', 'SORTABLE',
               'color', 'TAG', 'SORTABLE').ok()

    conn = getConnectionByEnv(env)
    docs = [
        ('doc:1', 'apple', 'red', '4'),
        ('doc:2', 'strawberry', 'red', '3'),
        ('doc:3', 'banana', 'yellow', '4'),
        ('doc:4', 'lemon', 'yellow', '2'),
    ]
    for key, name, color, sweetness in docs:
        conn.execute_command('HSET', key, 'name', name, 'color', color,
                             'sweetness', sweetness)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COUNT', '0', 'AS', 'cnt',
            'REDUCE', 'COLLECT', '4',
                'FIELDS', '2', '@name', '@sweetness',
            'AS', 'names',
        'GROUPBY', '0',
            'REDUCE', 'COLLECT', '5', 'FIELDS', '3', '@color', '@cnt', '@names',
            'AS', 'groups')

    env.assertEqual(len(res['results']), 1)
    groups = _sort_collected(res['results'][0]['extra_attributes']['groups'], 'color')
    for group in groups:
        group['names'] = _sort_collected(group['names'], 'name')
    env.assertEqual(groups, [
        {'color': 'red', 'cnt': '2',
         'names': [{'name': 'apple', 'sweetness': '4'},
                   {'name': 'strawberry', 'sweetness': '3'}]},
        {'color': 'yellow', 'cnt': '2',
         'names': [{'name': 'banana', 'sweetness': '4'},
                   {'name': 'lemon', 'sweetness': '2'}]},
    ])


# ---------------------------------------------------------------------------
# COLLECT 1 field, HASH
# ---------------------------------------------------------------------------
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
# Verify output structure: array of maps
# ---------------------------------------------------------------------------
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
def test_collect_requires_unstable_features():
    env = Env()
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'no')
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
# ---------------------------------------------------------------------------
# Internal-path serialization: _FT.AGGREGATE sets QEXEC_F_INTERNAL and must
# cause the shard to include sort-key values alongside projected fields.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_internal_serializes_sort_fields():
    """In internal mode the shard includes SORTBY fields alongside FIELDS."""
    env = Env(protocol=3)
    _setup_hash(env)

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '7',
            'FIELDS', '1', '@name',
            'SORTBY', '2', '@sweetness', 'DESC',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    groups = _sort_by(internal['results'], 'color')
    red = [g for g in groups if g['extra_attributes']['color'] == 'red'][0]
    for row in red['extra_attributes']['info']:
        env.assertEqual(set(row.keys()), {'name', 'sweetness'},
                        message='internal should include both FIELDS and SORTBY keys')


@skip(cluster=True)
def test_collect_internal_without_sortby_equals_external_shape():
    """No spurious widening: _FT without SORTBY must match FT output."""
    env = Env(protocol=3)
    _setup_hash(env)

    _, slots_data = get_shard_slot_ranges(env)[0]

    common_args = [
        'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'names',
    ]

    ext = env.cmd('FT.AGGREGATE', *common_args)
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')
    internal = env.cmd('_FT.AGGREGATE', *common_args, '_SLOTS_INFO', slots_data)

    ext_groups = _sort_by(ext['results'], 'color')
    int_groups = _sort_by(internal['results'], 'color')

    for eg, ig in zip(ext_groups, int_groups):
        ext_names = _sort_collected(eg['extra_attributes']['names'], 'name')
        int_names = _sort_collected(ig['extra_attributes']['names'], 'name')
        env.assertEqual(ext_names, int_names)
        # Each row should have only the projected field key
        for row in int_names:
            env.assertEqual(set(row.keys()), {'name'})


@skip(cluster=True)
def test_collect_internal_duplicate_field_and_sort():
    """When a field is also the sort key, internal mode emits exactly one wire entry per row.

    Uses RESP2 because RESP3 maps are parsed into Python dicts that silently
    collapse duplicate keys, hiding any wire-level duplication. Under RESP2
    the map comes back as a flat ``[k, v, k, v, ...]`` list where dup keys
    survive and can be counted directly.
    """
    env = Env(protocol=2)
    _setup_hash(env)

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    # @sweetness is both the projected FIELD and the SORTBY key
    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '7',
            'FIELDS', '1', '@sweetness',
            'SORTBY', '2', '@sweetness', 'DESC',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    # RESP2 shape: [num_groups, group_row, group_row, ...] where each
    # group_row is a flat [key, val, key, val, ...] list. The 'info' value
    # is a list of rows, each itself a flat [key, val, ...] list.
    for group_row in internal[1:]:
        info_idx = group_row.index('info') + 1
        rows = group_row[info_idx]
        for row in rows:
            env.assertEqual(
                row.count('sweetness'), 1,
                message='overlapping field/sort key must appear exactly once on the wire')
            env.assertEqual(
                len(row), 2,
                message='internal row must contain exactly one (key, value) pair')


# RESP2 sanity: basic COLLECT works under RESP2
# ---------------------------------------------------------------------------
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
