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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '5', 'FIELDS', '3', '@name', '@sweetness', '@origin',
        'AS', 'info')

    groups = _sort_by(res['results'], 'color')
    green = _sort_collected(groups[0]['extra_attributes']['info'], 'name')
    env.assertEqual(green, [
        {'name': 'kiwi', 'sweetness': '3'},
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
        {'name': 'lemon',  'sweetness': '2'},
    ])


# ---------------------------------------------------------------------------
# COLLECT 1 field, JSON
# ---------------------------------------------------------------------------
@skip(no_json=True)
def test_collect_1_field_json():
    env = Env(protocol=3)
    enable_unstable_features(env)
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
    enable_unstable_features(env)
    _setup_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '5', 'FIELDS', '3', '@name', '@sweetness', '@origin',
        'AS', 'info')

    groups = _sort_by(res['results'], 'color')
    green = _sort_collected(groups[0]['extra_attributes']['info'], 'name')
    env.assertEqual(green, [
        {'name': 'kiwi', 'sweetness': '3'},
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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@sweetness',
        'REDUCE', 'COLLECT', '4', 'FIELDS', '2', '@name', '@origin',
        'AS', 'items')

    groups = _sort_by(res['results'], 'sweetness')

    items2 = _sort_collected(groups[0]['extra_attributes']['items'], 'name')
    env.assertEqual(items2, [
        {'name': 'lemon'},
        {'name': 'lime',  'origin': 'mexico'},
    ])

    items3 = _sort_collected(groups[1]['extra_attributes']['items'], 'name')
    env.assertEqual(items3, [
        {'name': 'kiwi'},
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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
               'SCHEMA', '$.name', 'AS', 'name', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, f in enumerate(FRUITS):
        conn.execute_command('JSON.SET', f'doc:{i}', '$', json.dumps(f))

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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
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


# ---------------------------------------------------------------------------
# `COLLECT FIELDS *` rule: emits exactly the keys present in the source
# rlookup at row time — neither the schema, nor whatever happens to be in
# the underlying Redis hash, drives the projection.
#
# The three tests below pin this rule by varying what `LOAD` puts into the
# lookup while keeping the schema and hash contents constant:
#   1. Partial LOAD             → only the loaded subset is emitted.
#   2. LOAD with `@__key`       → derived keys ride along like any field.
#   3. LOAD *                   → the full schema is emitted.
#
# All three use RESP2 so each emitted row arrives as a flat
# ``[k, v, k, v, ...]`` list whose keys can be inspected directly without
# RESP3's silent duplicate-key collapse.
# ---------------------------------------------------------------------------
def _collect_load_all_index_with_three_fields(env):
    """Create a 3-field schema and two docs in the same group.

    Both docs populate every schema field, so any field absent from a
    collected row's wire payload comes from the rlookup not having that
    key — never from the row missing a value.
    """
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'team', 'TAG', 'SORTABLE',
               'score', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:alpha', 'name', 'alice', 'team', 't', 'score', '7')
    conn.execute_command('HSET', 'doc:bravo', 'name', 'bob',   'team', 't', 'score', '5')


def _collect_load_all_extract_rows(internal_resp2):
    """Iterate every collected row across every group from a RESP2 reply.

    RESP2 shape: ``[num_groups, group_row, group_row, ...]`` where each
    ``group_row`` is a flat ``[k, v, k, v, ...]`` list and the ``info``
    entry is itself a list of flat rows.
    """
    for group_row in internal_resp2[1:]:
        info_idx = group_row.index('info') + 1
        for row in group_row[info_idx]:
            yield row


@skip(cluster=True)
def test_collect_internal_load_all_partial_load_emits_only_loaded_fields():
    """Loading a strict subset of the schema produces a strict subset on the
    wire — even though the underlying hash holds all three fields.

    The schema has ``name``, ``team``, ``score``; ``LOAD`` pulls in only
    ``name`` and ``team``. Every emitted row therefore carries exactly
    ``{name, team}`` and ``score`` is absent. This is the canonical
    counter-example to "FIELDS * mirrors the schema": the rlookup, not the
    schema, drives the load-all walk.
    """
    env = Env(protocol=2)
    _collect_load_all_index_with_three_fields(env)

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'LOAD', '2', '@name', '@team',
        'GROUPBY', '1', '@team',
        'REDUCE', 'COLLECT', '2',
            'FIELDS', '*',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    expected_keys = {'name', 'team'}
    for row in _collect_load_all_extract_rows(internal):
        row_keys = set(row[0::2])
        env.assertEqual(
            row_keys, expected_keys,
            message=f'partial-load row must emit only the loaded subset, got {row_keys}')


@skip(cluster=True)
def test_collect_internal_load_all_emits_dunder_key_when_loaded():
    """``@__key`` rides through `FIELDS *` like any other loaded field.

    The schema has ``name``, ``team``, ``score``; ``LOAD`` pulls in
    ``name``, ``team`` and the special derived ``@__key``, but not
    ``score``. Every emitted row therefore carries exactly
    ``{name, team, __key}``, and the ``__key`` value matches the doc's
    Redis key. This documents that the load-all walk does not
    discriminate against derived keys: anything sitting in the rlookup at
    row time (modulo hidden flags and tombstones, neither of which apply
    to ``@__key``) is emitted.
    """
    env = Env(protocol=2)
    _collect_load_all_index_with_three_fields(env)

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'LOAD', '3', '@name', '@team', '@__key',
        'GROUPBY', '1', '@team',
        'REDUCE', 'COLLECT', '2',
            'FIELDS', '*',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    expected_keys = {'name', 'team', '__key'}
    expected_dunder_values = {'doc:alpha', 'doc:bravo'}
    seen_dunder = set()
    for row in _collect_load_all_extract_rows(internal):
        row_dict = dict(zip(row[0::2], row[1::2]))
        env.assertEqual(
            set(row_dict.keys()), expected_keys,
            message=f'expected exactly {expected_keys}, got {set(row_dict.keys())}')
        seen_dunder.add(row_dict['__key'])

    env.assertEqual(
        seen_dunder, expected_dunder_values,
        message='each doc must emit its own @__key value through FIELDS *')


@skip(cluster=True)
def test_collect_internal_load_all_with_load_star_emits_full_schema():
    """``LOAD *`` populates the rlookup with every schema field, so
    ``COLLECT FIELDS *`` emits all of them per row.

    With ``LOAD *`` the rlookup at COLLECT-time mirrors the schema
    exactly. This is the case the previous single test conflated — it now
    sits as one of three points on the rule curve, alongside the partial
    and `@__key` cases above.
    """
    env = Env(protocol=2)
    _collect_load_all_index_with_three_fields(env)

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'LOAD', '*',
        'GROUPBY', '1', '@team',
        'REDUCE', 'COLLECT', '2',
            'FIELDS', '*',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    expected_keys = {'name', 'team', 'score'}
    for row in _collect_load_all_extract_rows(internal):
        row_keys = set(row[0::2])
        env.assertEqual(
            row_keys, expected_keys,
            message=f'LOAD * row must emit the full schema, got {row_keys}')


@skip(cluster=True)
def test_collect_internal_load_all_omits_missing_fields():
    """When a row has no value for a key, `FIELDS *` drops it from the map.

    Two docs share the same group: ``full`` has every schema field set,
    ``partial`` is missing ``extra``. The `LOAD` step pulls every schema
    field (including ``extra``) into the lookup so the load-all walk has a
    chance to project it — the omit-if-missing rule is what makes the
    ``partial`` row drop ``extra`` while ``full`` still emits it.

    Uses RESP2 to inspect each row as a flat ``[k, v, k, v, ...]`` list
    without RESP3's silent duplicate-key collapse.
    """
    env = Env(protocol=2)
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'team', 'TAG', 'SORTABLE',
               'extra', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:full',    'name', 'full',    'team', 'g', 'extra', 'set')
    conn.execute_command('HSET', 'doc:partial', 'name', 'partial', 'team', 'g')

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'LOAD', '3', '@name', '@team', '@extra',
        'GROUPBY', '1', '@team',
        'REDUCE', 'COLLECT', '2',
            'FIELDS', '*',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    expected_has_extra = {'full': True, 'partial': False}
    seen_names = set()
    for group_row in internal[1:]:
        info_idx = group_row.index('info') + 1
        rows = group_row[info_idx]
        for row in rows:
            row_dict = dict(zip(row[0::2], row[1::2]))

            name = row_dict.get('name')
            env.assertIsNotNone(name, message='every row must carry its `name`')
            seen_names.add(name)

            should_have_extra = expected_has_extra[name]
            has_extra = 'extra' in row_dict
            env.assertEqual(
                has_extra, should_have_extra,
                message=f'doc {name!r}: expected extra present={should_have_extra}, got {has_extra}')

    env.assertEqual(
        seen_names, set(expected_has_extra),
        message='every doc must appear in the collected group')


# ---------------------------------------------------------------------------
# `COLLECT FIELDS *` on JSON indexes
#
# JSON's `LOAD *` does not fan out into per-field rlookup keys (as `HGETALL`
# does for HASH). Instead `RLookup_JSON_GetAll` adds a single key named
# ``$`` (`JSON_ROOT`) carrying the whole serialized document. The wildcard
# walk picks this up unchanged: each emitted row is a singleton map keyed
# on ``$``. The "rlookup drives projection" rule holds without any
# JSON-specific code path on the COLLECT side.
#
# The two tests below pin the rule by varying whether `LOAD *` runs:
#   1. With LOAD *  -> rlookup contains `$`; wildcard emits {$: <doc>}.
#   2. Without LOAD -> rlookup contains only what the GROUPBY key
#                      registered (`color`); wildcard emits {color: ...}
#                      and `$` is absent.
# ---------------------------------------------------------------------------
@skip(cluster=True, no_json=True)
def test_collect_internal_load_all_emits_dollar_on_json():
    """`LOAD *` on JSON adds a single ``$`` key to the rlookup carrying
    the whole serialized doc; `COLLECT FIELDS *` emits it alongside
    whatever else the request stages registered (here: ``color`` from
    the GROUPBY).

    This is the JSON counterpart of
    ``test_collect_internal_load_all_with_load_star_emits_full_schema``:
    both lock in that the rlookup at row time is what drives the
    wildcard's projection. The HASH side fans out into many keys, the
    JSON side keeps a single ``$`` carrying the serialized doc — and
    that asymmetry is intentional, mirroring the runtime behaviour of
    `LOAD *` itself (HGETALL fans out fields, JSON_GetAll does not).
    """
    env = Env(protocol=2)
    enable_unstable_features(env)
    _setup_json(env)

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'LOAD', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '2',
            'FIELDS', '*',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    fixture_by_name = {f['name']: f for f in FRUITS}
    seen_names = set()
    for group_row in internal[1:]:
        info_idx = group_row.index('info') + 1
        rows = group_row[info_idx]
        for row in rows:
            row_dict = dict(zip(row[0::2], row[1::2]))
            env.assertEqual(
                set(row_dict.keys()), {'color', '$'},
                message=f'LOAD * + FIELDS * on JSON must emit exactly the lookup keys ({{color, $}}), got {set(row_dict.keys())}')

            doc_str = row_dict['$']
            doc = json.loads(doc_str)
            env.assertIn(
                doc.get('name'), fixture_by_name,
                message=f'`$` value must be a serialized fixture doc, got {doc_str!r}')
            seen_names.add(doc['name'])
            # The `$` doc must be self-consistent with the GROUPBY key
            # that the same row also carries.
            env.assertEqual(
                doc['color'], row_dict['color'],
                message=f"row's `color` and `$.color` must agree, got color={row_dict['color']!r}, $.color={doc['color']!r}")
            # And it must match the original fixture exactly (both for
            # set fields and for absent-`origin` rows).
            env.assertEqual(
                doc, fixture_by_name[doc['name']],
                message=f"`$` value for {doc['name']!r} must round-trip the fixture exactly, got {doc!r}")

    env.assertEqual(
        seen_names, set(fixture_by_name),
        message=f'every fruit must appear in some collected group, saw {seen_names}')


@skip(cluster=True, no_json=True)
def test_collect_internal_no_load_emits_only_groupby_key_on_json():
    """Without `LOAD *`, only fields the request itself registered in the
    source rlookup are emitted by `COLLECT FIELDS *`.

    With ``GROUPBY 1 @color REDUCE COLLECT FIELDS 1 *`` and no upstream
    `LOAD`, the only key registered in the source rlookup is ``color``
    (resolved by the GROUPBY against the schema cache). ``color`` is
    sortable, so its value is supplied via the per-row sorting vector
    even without an explicit loader. ``$`` is absent because no
    `LOAD *` ever ran; ``name``/``sweetness``/``origin`` are absent
    because nothing in the request resolved them.

    This documents that the wildcard is rlookup-driven, not
    schema-driven — calling `COLLECT FIELDS *` on JSON without an
    explicit `LOAD` is a footgun that emits less than users may expect.
    """
    env = Env(protocol=2)
    enable_unstable_features(env)
    _setup_json(env)

    _, slots_data = get_shard_slot_ranges(env)[0]
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    internal = env.cmd(
        '_FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '2',
            'FIELDS', '*',
        'AS', 'info',
        '_SLOTS_INFO', slots_data,
    )

    expected_colors = {f['color'] for f in FRUITS}
    seen_colors = set()
    seen_doc_count = 0
    for group_row in internal[1:]:
        info_idx = group_row.index('info') + 1
        rows = group_row[info_idx]
        for row in rows:
            seen_doc_count += 1
            row_keys = row[0::2]
            env.assertEqual(
                row_keys, ['color'],
                message=f'no-LOAD wildcard row must contain exactly [\'color\', <val>], got keys {row_keys}')
            env.assertEqual(
                len(row), 2,
                message=f'no-LOAD wildcard row must contain exactly one (key, value) pair, got {row}')
            seen_colors.add(row[1])

    env.assertEqual(
        seen_colors, expected_colors,
        message=f'every fixture color must appear, expected {expected_colors}, got {seen_colors}')
    env.assertEqual(
        seen_doc_count, len(FRUITS),
        message=f'every fruit must appear in some collected group, saw {seen_doc_count}/{len(FRUITS)}')


# ---------------------------------------------------------------------------
# Chained GROUPBY: outer COLLECT FIELDS * projects every key the inner
# reducers placed in the lookup
# ---------------------------------------------------------------------------
def test_chained_groupby_collect_load_all():
    """Outer ``COLLECT FIELDS *`` after a chained GROUPBY projects every
    key surfaced by the inner reducers (``@color``, ``@cnt``,
    ``@avg_sweet``), none of which are explicit fields on the outer
    reducer.
    """
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COUNT', '0', 'AS', 'cnt',
        'REDUCE', 'AVG', '1', '@sweetness', 'AS', 'avg_sweet',
        'GROUPBY', '0',
        'REDUCE', 'COLLECT', '2', 'FIELDS', '*',
        'AS', 'stats')

    results = res['results']
    env.assertEqual(len(results), 1)

    stats = sorted(results[0]['extra_attributes']['stats'],
                   key=lambda e: e['avg_sweet'])
    env.assertEqual(stats, [
        {'color': 'green',  'cnt': '2', 'avg_sweet': '2.5'},
        {'color': 'yellow', 'cnt': '2', 'avg_sweet': '3'},
        {'color': 'red',    'cnt': '2', 'avg_sweet': '3.5'},
    ])


# RESP2 sanity: basic COLLECT works under RESP2
# ---------------------------------------------------------------------------
def test_collect_resp2_sanity():
    env = Env(protocol=2)
    enable_unstable_features(env)
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
# APPLY interactions with COLLECT
#
# The four tests below pin how APPLY-derived aliases flow through COLLECT:
#   1. APPLY alias as the GROUPBY key             (upstream rlookup -> grouping)
#   2. APPLY alias projected by COLLECT FIELDS    (upstream rlookup -> reducer)
#   3. APPLY over a reducer alias, outer COLLECT  (reducer->APPLY->outer COLLECT)
#   4. APPLY + outer COLLECT FIELDS *             (wildcard walk picks up APPLY)
# ---------------------------------------------------------------------------
def test_collect_apply_alias_as_groupby_key():
    """APPLY before GROUPBY writes `upper(@color)` into the upstream
    rlookup; ``GROUPBY @COLOR_UP`` then partitions by the derived alias.
    COLLECT sees the APPLY alias as a regular group key and projects
    ``@name`` per row unaffected.

    This pins that APPLY-produced keys are first-class GROUPBY inputs:
    the grouping step reads them out of the rlookup like any loaded
    field.
    """
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'APPLY', 'upper(@color)', 'AS', 'COLOR_UP',
        'GROUPBY', '1', '@COLOR_UP',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@name',
        'AS', 'names')

    groups = _sort_by(res['results'], 'COLOR_UP')
    env.assertEqual([g['extra_attributes']['COLOR_UP'] for g in groups],
                    ['GREEN', 'RED', 'YELLOW'])
    env.assertEqual(_sort_collected(groups[0]['extra_attributes']['names'], 'name'),
                    [{'name': 'kiwi'}, {'name': 'lime'}])
    env.assertEqual(_sort_collected(groups[1]['extra_attributes']['names'], 'name'),
                    [{'name': 'apple'}, {'name': 'strawberry'}])
    env.assertEqual(_sort_collected(groups[2]['extra_attributes']['names'], 'name'),
                    [{'name': 'banana'}, {'name': 'lemon'}])


def test_collect_apply_alias_as_field():
    """APPLY-derived alias flows through the upstream rlookup into
    COLLECT's per-row projection. Each collected map carries exactly
    the derived key (``@NAME_UP``), not the raw source (``@name``).

    The negative assertion is the real pin here: a COLLECT with a
    specific ``FIELDS`` list is name-driven, so any hypothetical path
    that silently rode the raw source through to the wire would be
    caught.
    """
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'APPLY', 'upper(@name)', 'AS', 'NAME_UP',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@NAME_UP',
        'AS', 'shouted')

    groups = _sort_by(res['results'], 'color')
    env.assertEqual(_sort_collected(groups[0]['extra_attributes']['shouted'], 'NAME_UP'),
                    [{'NAME_UP': 'KIWI'}, {'NAME_UP': 'LIME'}])
    env.assertEqual(_sort_collected(groups[1]['extra_attributes']['shouted'], 'NAME_UP'),
                    [{'NAME_UP': 'APPLE'}, {'NAME_UP': 'STRAWBERRY'}])
    env.assertEqual(_sort_collected(groups[2]['extra_attributes']['shouted'], 'NAME_UP'),
                    [{'NAME_UP': 'BANANA'}, {'NAME_UP': 'LEMON'}])

    for g in groups:
        for entry in g['extra_attributes']['shouted']:
            env.assertNotContains('name', entry)
            env.assertEqual(set(entry.keys()), {'NAME_UP'})


def test_chained_groupby_collect_apply_on_reducer_alias():
    """APPLY between two GROUPBYs consumes two reducer aliases
    (``@cnt`` and ``@avg_sweet``) and writes ``@weighted`` into the
    pipeline rlookup. The outer GROUPBY's COLLECT then projects
    ``@color`` and ``@weighted`` per inner-group row.

    The multiplication is chosen so every color group produces a
    distinct post-APPLY value -- a passing test can only be explained
    by per-group APPLY evaluation against that group's reducer output.

    Fixture math:
        green:  cnt=2, avg_sweet=2.5 -> weighted = 5
        yellow: cnt=2, avg_sweet=3.0 -> weighted = 6
        red:    cnt=2, avg_sweet=3.5 -> weighted = 7
    """
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COUNT', '0', 'AS', 'cnt',
        'REDUCE', 'AVG', '1', '@sweetness', 'AS', 'avg_sweet',
        'APPLY', '@cnt * @avg_sweet', 'AS', 'weighted',
        'GROUPBY', '0',
        'REDUCE', 'COLLECT', '4', 'FIELDS', '2', '@color', '@weighted',
        'AS', 'per_color')

    results = res['results']
    env.assertEqual(len(results), 1)

    entries = sorted(results[0]['extra_attributes']['per_color'],
                     key=lambda e: float(e['weighted']))
    env.assertEqual(entries, [
        {'color': 'green',  'weighted': '5'},
        {'color': 'yellow', 'weighted': '6'},
        {'color': 'red',    'weighted': '7'},
    ])


def test_chained_groupby_collect_apply_load_all():
    """Outer ``COLLECT FIELDS *`` after a chained GROUPBY + APPLY
    projects every key the inner stages placed in the source rlookup:
    ``@color`` (inner group key), ``@cnt`` and ``@avg_sweet`` (inner
    reducers), and ``@weighted`` (APPLY).

    This is the APPLY-aware sibling of
    ``test_chained_groupby_collect_load_all``: the additional APPLY
    alias must appear in the wildcard projection alongside the reducer
    aliases, pinning that APPLY-derived keys participate in the
    rlookup-driven wildcard walk the same way reducer-produced keys
    do -- the wildcard does not discriminate by producer.

    Fixture math matches the sibling reducer-alias test:
        green:  cnt=2, avg_sweet=2.5 -> weighted = 5
        yellow: cnt=2, avg_sweet=3.0 -> weighted = 6
        red:    cnt=2, avg_sweet=3.5 -> weighted = 7
    """
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COUNT', '0', 'AS', 'cnt',
        'REDUCE', 'AVG', '1', '@sweetness', 'AS', 'avg_sweet',
        'APPLY', '@cnt * @avg_sweet', 'AS', 'weighted',
        'GROUPBY', '0',
        'REDUCE', 'COLLECT', '2', 'FIELDS', '*',
        'AS', 'stats')

    results = res['results']
    env.assertEqual(len(results), 1)

    stats = sorted(results[0]['extra_attributes']['stats'],
                   key=lambda e: float(e['weighted']))
    env.assertEqual(stats, [
        {'color': 'green',  'cnt': '2', 'avg_sweet': '2.5', 'weighted': '5'},
        {'color': 'yellow', 'cnt': '2', 'avg_sweet': '3',   'weighted': '6'},
        {'color': 'red',    'cnt': '2', 'avg_sweet': '3.5', 'weighted': '7'},
    ])


# ---------------------------------------------------------------------------
# COLLECT FIELDS * (LOADALL) in cluster mode: every key observed across all
# shard payloads is emitted; missing keys for a row are omitted (no padding).
# ---------------------------------------------------------------------------

@skip(cluster=False)
def test_collect_cluster_load_all_merges_per_row_keys_across_shards():
    """FIELDS * in cluster mode: coordinator emits all keys present per row."""
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name',      'TEXT',    'SORTABLE',
               'color',     'TAG',     'SORTABLE',
               'sweetness', 'NUMERIC', 'SORTABLE',
               'origin',    'TEXT',    'SORTABLE').ok()

    conn = getConnectionByEnv(env)
    docs = [
        # All three have different shard affinity via hash tags.
        # banana has origin; lemon/kiwi do not.
        ('doc:1{a}', 'banana', 'yellow', '4', 'Ecuador'),
        ('doc:2{b}', 'lemon',  'yellow', '2', None),
        ('doc:3{c}', 'kiwi',   'green',  '3', None),
    ]
    for key, name, color, sweetness, origin in docs:
        args = ['HSET', key, 'name', name, 'color', color, 'sweetness', sweetness]
        if origin is not None:
            args += ['origin', origin]
        conn.execute_command(*args)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'LOAD', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '2', 'FIELDS', '*',
        'AS', 'info')

    groups = _sort_by(res['results'], 'color')
    env.assertEqual(len(groups), 2)

    green = groups[0]['extra_attributes']['info']
    env.assertEqual(len(green), 1)
    # kiwi has no origin — LOADALL omits the key entirely (no null_static padding).
    env.assertEqual(green[0].get('name'), 'kiwi')
    env.assertFalse('origin' in green[0],
                    message="LOADALL must omit keys not present in a row, not pad with None")

    yellow = _sort_collected(groups[1]['extra_attributes']['info'], 'name')
    env.assertEqual(len(yellow), 2)
    banana = yellow[0]
    lemon  = yellow[1]

    env.assertEqual(banana.get('name'), 'banana')
    env.assertEqual(banana.get('origin', '').lower(), 'ecuador')
    env.assertEqual(lemon.get('name'), 'lemon')
    env.assertFalse('origin' in lemon,
                    message="LOADALL must omit keys not present in a row, not pad with None")


# ---------------------------------------------------------------------------
# LIMIT dataset: 12 items, all color=red. Shared by the COLLECT LIMIT and
# SORTBY+LIMIT tests.
# ---------------------------------------------------------------------------
PRICED = [
    {'name': 'alice',   'color': 'red', 'price': 10},
    {'name': 'bob',     'color': 'red', 'price': 10},
    {'name': 'charlie', 'color': 'red', 'price': 15},
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


def _setup_priced_hash(env, key_for_idx=None):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name',  'TEXT',    'SORTABLE',
               'color', 'TAG',     'SORTABLE',
               'price', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, item in enumerate(PRICED):
        key = key_for_idx(i) if key_for_idx else f'doc:{i}'
        conn.execute_command('HSET', key,
                             'name', item['name'],
                             'color', item['color'],
                             'price', str(item['price']))


def _names(entries):
    """Extract the list of @name values (in order) from a COLLECT Array<Map> result."""
    return [e['name'] for e in entries]


# ---------------------------------------------------------------------------
# LIMIT without SORTBY (array path, first-K in insertion order)
# ---------------------------------------------------------------------------
@skip(no_json=True)
def test_collect_limit_without_sortby():
    env = Env(protocol=3)
    enable_unstable_features(env)
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
# LIMIT with SORTBY (heap path): sort first, then apply LIMIT.
# ---------------------------------------------------------------------------
@skip(no_json=True)
def test_collect_sortby_limit_applies_offset_after_sort():
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_priced_json(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '12',
            'FIELDS', '1', '@name',
            'SORTBY', '4', '@price', 'DESC', '@name', 'ASC',
            'LIMIT', '1', '4',
        'AS', 'names')

    entries = res['results'][0]['extra_attributes']['names']
    # price DESC, name ASC gives:
    # charlie(15), dave(15), alice(10), bob(10), eve(8), ...
    env.assertEqual(_names(entries), ['dave', 'alice', 'bob', 'eve'])
    for entry in entries:
        env.assertEqual(set(entry.keys()), {'name'})


@skip(cluster=False)
def test_collect_sortby_limit_merges_global_topk_across_shards():
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)
    _setup_priced_hash(env, key_for_idx=lambda i: f'doc:{i}{{slot:{i % 3}}}')

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '10',
            'FIELDS', '1', '@name',
            'SORTBY', '2', '@price', 'ASC',
            'LIMIT', '2', '4',
        'AS', 'names')

    entries = res['results'][0]['extra_attributes']['names']
    # Global price ASC is liam(1), kate(2), jack(3), iris(4), henry(5), grace(6).
    env.assertEqual(_names(entries), ['jack', 'iris', 'henry', 'grace'])
    for entry in entries:
        env.assertEqual(set(entry.keys()), {'name'})


# ---------------------------------------------------------------------------
# Heap path (SORTBY without LIMIT) default-caps at 10 by design.
# ---------------------------------------------------------------------------
def test_collect_sortby_without_limit_caps_at_default_10():
    """COLLECT + SORTBY without an explicit LIMIT runs the heap path with its
    default capacity of 10, even when the group has more matching docs."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_priced_hash(env)

    # PRICED has 12 docs in the 'red' group; ASC top-10 by price drops the
    # two highest-priced names (charlie=15, dave=15).
    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
        'REDUCE', 'COLLECT', '7',
            'FIELDS', '1', '@name',
            'SORTBY', '2', '@price', 'ASC',
        'AS', 'names')

    entries = res['results'][0]['extra_attributes']['names']
    env.assertEqual(len(entries), 10)
    names = {e['name'] for e in entries}
    # The two highest-priced docs must be the ones dropped.
    env.assertNotContains('charlie', names)
    env.assertNotContains('dave', names)


# ---------------------------------------------------------------------------
# Array path (no SORTBY, no LIMIT) capped by MAXAGGREGATERESULTS
# ---------------------------------------------------------------------------
@skip(no_json=True)
def test_collect_array_path_capped_by_max_aggregate_results():
    env = Env(protocol=3)
    enable_unstable_features(env)
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


# ---------------------------------------------------------------------------
# Multiple COLLECT reducers in one GROUPBY: independent fields / LIMIT / SORTBY
# ---------------------------------------------------------------------------
def test_two_collect_reducers_different_fields():
    """Two COLLECTs in the same GROUPBY emit independent arrays with their own
    field sets."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'names',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@sweetness',
                'SORTBY', '2', '@sweetness', 'ASC',
            'AS', 'sweets')

    expected_names = {
        'green':  [{'name': 'kiwi'},   {'name': 'lime'}],
        'red':    [{'name': 'apple'},  {'name': 'strawberry'}],
        'yellow': [{'name': 'banana'}, {'name': 'lemon'}],
    }
    expected_sweets = {
        'green':  [{'sweetness': '2'}, {'sweetness': '3'}],
        'red':    [{'sweetness': '3'}, {'sweetness': '4'}],
        'yellow': [{'sweetness': '2'}, {'sweetness': '4'}],
    }
    for g in res['results']:
        attrs = g['extra_attributes']
        env.assertEqual(attrs['names'],  expected_names[attrs['color']])
        env.assertEqual(attrs['sweets'], expected_sweets[attrs['color']])


def test_two_collect_reducers_one_with_limit():
    """LIMIT on one COLLECT must not bound the sibling COLLECT in the same group."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'all_names',
            'REDUCE', 'COLLECT', '10',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
                'LIMIT', '0', '1',
            'AS', 'one_name')

    expected_all = {
        'green':  [{'name': 'kiwi'},   {'name': 'lime'}],
        'red':    [{'name': 'apple'},  {'name': 'strawberry'}],
        'yellow': [{'name': 'banana'}, {'name': 'lemon'}],
    }
    for g in res['results']:
        attrs = g['extra_attributes']
        env.assertEqual(attrs['all_names'], expected_all[attrs['color']])
        # LIMIT 0,1 after SORTBY @name ASC picks the first of the full set.
        env.assertEqual(attrs['one_name'], [expected_all[attrs['color']][0]])


def test_two_collect_reducers_sortby_different_keys():
    """Two COLLECTs with different SORTBY keys keep independent heap state."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '10',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
                'LIMIT', '0', '1',
            'AS', 'first_by_name',
            'REDUCE', 'COLLECT', '10',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'DESC',
                'LIMIT', '0', '1',
            'AS', 'top_sweet',
        'SORTBY', '2', '@color', 'ASC')

    # name ASC firsts: 'apple' < 'strawberry'; 'banana' < 'lemon'; 'kiwi' < 'lime'.
    expected_first_by_name = {
        'green':  [{'name': 'kiwi'}],
        'red':    [{'name': 'apple'}],
        'yellow': [{'name': 'banana'}],
    }
    # sweetness DESC: red -> apple(4); yellow -> banana(4); green tie -> kiwi(3).
    expected_top_sweet = {
        'green':  [{'name': 'kiwi'}],
        'red':    [{'name': 'apple'}],
        'yellow': [{'name': 'banana'}],
    }
    for g in res['results']:
        attrs = g['extra_attributes']
        env.assertEqual(attrs['first_by_name'], expected_first_by_name[attrs['color']])
        env.assertEqual(attrs['top_sweet'],     expected_top_sweet[attrs['color']])


@skip(cluster=False)
def test_two_collect_reducers_overlapping_fields_cluster():
    """Two COLLECTs over @name with divergent SORTBY/LIMIT keep independent
    state across shards."""
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'color', 'TAG', 'SORTABLE').ok()

    shard_tags = ['shard:0', 'shard:1', 'shard:3']
    conn = getConnectionByEnv(env)
    for i, f in enumerate(FRUITS):
        conn.execute_command('HSET', f'doc:{i}{{{shard_tags[i % 3]}}}',
                             'name', f['name'], 'color', f['color'])

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'all_asc',
            'REDUCE', 'COLLECT', '10',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'DESC',
                'LIMIT', '0', '1',
            'AS', 'last_desc')

    # Full ASC list — pins that both shards' rows are merged.
    expected_all = {
        'green':  [{'name': 'kiwi'},   {'name': 'lime'}],
        'red':    [{'name': 'apple'},  {'name': 'strawberry'}],
        'yellow': [{'name': 'banana'}, {'name': 'lemon'}],
    }
    # Top-1 DESC after global shard merge: must be the last name in the ASC list.
    expected_last = {
        'green':  [{'name': 'lime'}],
        'red':    [{'name': 'strawberry'}],
        'yellow': [{'name': 'lemon'}],
    }
    for g in res['results']:
        attrs = g['extra_attributes']
        env.assertEqual(attrs['all_asc'],   expected_all[attrs['color']])
        env.assertEqual(attrs['last_desc'], expected_last[attrs['color']])


# ---------------------------------------------------------------------------
# COLLECT across multiple GROUPBY stages
# ---------------------------------------------------------------------------
def test_collect_in_first_groupby_survives_second_groupby():
    """A COLLECT array produced in stage 1 must round-trip as a payload through
    a stage-2 GROUPBY."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COUNT', '0', 'AS', 'cnt',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'items',
        'APPLY', 'to_str(@cnt)', 'AS', 'cnt_str',
        'GROUPBY', '1', '@cnt_str',
            'REDUCE', 'COLLECT', '8',
                'FIELDS', '2', '@color', '@items',
                'SORTBY', '2', '@color', 'ASC',
            'AS', 'rolled')

    # All three colors have cnt=2, so they roll up under a single cnt_str group.
    env.assertEqual(len(res['results']), 1)
    rolled = res['results'][0]['extra_attributes']['rolled']
    env.assertEqual([e['color'] for e in rolled], ['green', 'red', 'yellow'])
    for entry in rolled:
        # The nested COLLECT array survived the regroup.
        env.assertEqual(len(entry['items']), 2)


def test_three_stage_groupby_with_collect_at_end():
    """Three-stage pipeline: GROUPBY @color → APPLY derives @is_sweet →
    GROUPBY @is_sweet with COLLECT."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'AVG', '1', '@sweetness', 'AS', 'avg_sweet',
        'APPLY', '@avg_sweet >= 3', 'AS', 'is_sweet',
        'GROUPBY', '1', '@is_sweet',
            'REDUCE', 'COLLECT', '8',
                'FIELDS', '2', '@color', '@avg_sweet',
                'SORTBY', '2', '@color', 'ASC',
            'AS', 'colors',
        'SORTBY', '2', '@is_sweet', 'ASC')

    # avg_sweet: green=2.5, red=3.5, yellow=3.0 → green is the only @is_sweet=0 group.
    env.assertEqual(res['results'][0]['extra_attributes']['colors'],
                    [{'color': 'green', 'avg_sweet': '2.5'}])
    env.assertEqual(res['results'][1]['extra_attributes']['colors'], [
        {'color': 'red',    'avg_sweet': '3.5'},
        {'color': 'yellow', 'avg_sweet': '3'},
    ])


def test_chained_groupby_collect_of_collect_alias():
    """A COLLECT alias from stage 1 can be re-collected in stage 2 as a nested
    payload."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'names',
        'GROUPBY', '0',
            'REDUCE', 'COLLECT', '8',
                'FIELDS', '2', '@color', '@names',
                'SORTBY', '2', '@color', 'ASC',
            'AS', 'all')

    env.assertEqual(len(res['results']), 1)
    env.assertEqual(res['results'][0]['extra_attributes']['all'], [
        {'color': 'green',  'names': [{'name': 'kiwi'},   {'name': 'lime'}]},
        {'color': 'red',    'names': [{'name': 'apple'},  {'name': 'strawberry'}]},
        {'color': 'yellow', 'names': [{'name': 'banana'}, {'name': 'lemon'}]},
    ])


# ---------------------------------------------------------------------------
# COLLECT combined with COUNT / AVG / MIN / MAX / TOLIST in one GROUPBY
# ---------------------------------------------------------------------------
def test_collect_with_count_and_avg():
    """COLLECT alongside COUNT and AVG: per-group counts/averages must match
    collected rows."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COUNT', '0', 'AS', 'cnt',
            'REDUCE', 'AVG', '1', '@sweetness', 'AS', 'avg_sweet',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'names',
        'SORTBY', '2', '@color', 'ASC')

    expected = {
        'green':  ('2', '2.5', [{'name': 'kiwi'},   {'name': 'lime'}]),
        'red':    ('2', '3.5', [{'name': 'apple'},  {'name': 'strawberry'}]),
        'yellow': ('2', '3',   [{'name': 'banana'}, {'name': 'lemon'}]),
    }
    for g in res['results']:
        attrs = g['extra_attributes']
        exp_cnt, exp_avg, exp_names = expected[attrs['color']]
        env.assertEqual(attrs['cnt'], exp_cnt)
        env.assertEqual(attrs['avg_sweet'], exp_avg)
        env.assertEqual(attrs['names'], exp_names)
        env.assertEqual(int(attrs['cnt']), len(attrs['names']))


def test_collect_with_tolist_same_field():
    """TOLIST (flat list) and COLLECT (Array<Map>) over the same field have
    different wire shapes."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'TOLIST', '1', '@name', 'AS', 'tolist_names',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'collect_names')

    for g in res['results']:
        attrs = g['extra_attributes']
        # COLLECT is server-sorted; TOLIST has no SORTBY, so sort Python-side.
        collect_flat = [e['name'] for e in attrs['collect_names']]
        env.assertEqual(sorted(attrs['tolist_names']), collect_flat)
        # TOLIST returns plain strings; COLLECT returns maps.
        for v in attrs['tolist_names']:
            env.assertEqual(type(v), str)
        for v in attrs['collect_names']:
            env.assertEqual(type(v), dict)


def test_collect_with_min_max_sortby():
    """SORTBY+LIMIT inside COLLECT must agree with MIN/MAX over the same key."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_priced_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'MIN', '1', '@price', 'AS', 'min_price',
            'REDUCE', 'MAX', '1', '@price', 'AS', 'max_price',
            'REDUCE', 'COLLECT', '11',
                'FIELDS', '2', '@name', '@price',
                'SORTBY', '2', '@price', 'DESC',
                'LIMIT', '0', '3',
            'AS', 'top3')

    attrs = res['results'][0]['extra_attributes']
    env.assertEqual(attrs['min_price'], '1')
    env.assertEqual(attrs['max_price'], '15')
    top3 = attrs['top3']
    env.assertEqual(len(top3), 3)
    # MAX must equal the first (DESC-sorted) collected price.
    env.assertEqual(top3[0]['price'], attrs['max_price'])
    # Collected prices are sorted DESC: 15, 15, 10 (charlie/dave tie, then alice).
    env.assertEqual([e['price'] for e in top3], ['15', '15', '10'])


def test_collect_with_quantile_and_stddev():
    """QUANTILE and STDDEV coexist with COLLECT without state interference."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_priced_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'QUANTILE', '2', '@price', '0.5', 'AS', 'q50',
            'REDUCE', 'STDDEV', '1', '@price',          'AS', 'stddev_price',
            'REDUCE', 'COLLECT', '3', 'FIELDS', '1', '@price', 'AS', 'prices')

    attrs = res['results'][0]['extra_attributes']
    # All 12 prices are collected; reducers see the same 12 inputs.
    env.assertEqual(len(attrs['prices']), 12)
    # Sorted prices (0-indexed): [1, 2, 3, 4, 5, 6, 7, 8, 10, 10, 15, 15].
    # QUANTILE picks rank = ceil(quantile * n) - 1 in the sorted input.
    # For the median (quantile=0.5) over n=12 → index 5 → '6'.
    env.assertEqual(attrs['q50'], '6')
    # Sample STDDEV (n-1 denominator) over the 12 prices ≈ 4.6478.
    env.assertAlmostEqual(float(attrs['stddev_price']), 4.6478, delta=1e-3)


def test_collect_followed_by_apply_and_filter():
    """COLLECT coexists with a post-GROUPBY APPLY and FILTER on sibling reducer
    output."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COUNT', '0', 'AS', 'cnt',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@name', 'ASC',
            'AS', 'names',
        'APPLY', 'upper(@color)', 'AS', 'color_upper',
        'FILTER', '@color == "red"')

    env.assertEqual(len(res['results']), 1)
    attrs = res['results'][0]['extra_attributes']
    env.assertEqual(attrs['color'], 'red')
    env.assertEqual(attrs['color_upper'], 'RED')
    env.assertEqual(int(attrs['cnt']), 2)
    env.assertEqual(attrs['names'], [{'name': 'apple'}, {'name': 'strawberry'}])


# ---------------------------------------------------------------------------
# Tie-breaking on COLLECT + SORTBY.
# ---------------------------------------------------------------------------
@skip(cluster=True)
def test_collect_sortby_tiebreak_stable_under_asc():
    """Single shard, ASC SORTBY. All rows tie on the sort key, so the order
    must come entirely from the doc_id tie-breaker — smaller doc_id wins
    under ASC, matching FT.AGGREGATE SORTBY."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)
    conn = getConnectionByEnv(env)
    # All docs share color='black' and sweetness=1 — the SORTBY key fully ties.
    names_in_insert_order = ['a', 'b', 'c', 'd', 'e']
    for i, name in enumerate(names_in_insert_order):
        conn.execute_command('HSET', f'doc:{i}', 'name', name,
                             'color', 'black', 'sweetness', '1')

    cmd = (
        'FT.AGGREGATE', 'idx', '@color:{black}',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'ASC',
            'AS', 'names')

    res1 = env.cmd(*cmd)
    res2 = env.cmd(*cmd)
    env.assertEqual(res1, res2, message='order must be stable across runs')

    attrs = res1['results'][0]['extra_attributes']
    # ASC + ties → smaller doc_id wins → insertion order preserved.
    env.assertEqual([r['name'] for r in attrs['names']], names_in_insert_order)


@skip(cluster=True)
def test_collect_sortby_tiebreak_stable_under_desc():
    """Single shard, DESC SORTBY. All rows tie → larger doc_id wins."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)
    conn = getConnectionByEnv(env)
    # All docs share color='black' and sweetness=1 — the SORTBY key fully ties.
    names_in_insert_order = ['a', 'b', 'c', 'd', 'e']
    for i, name in enumerate(names_in_insert_order):
        conn.execute_command('HSET', f'doc:{i}', 'name', name,
                             'color', 'black', 'sweetness', '1')

    cmd = (
        'FT.AGGREGATE', 'idx', '@color:{black}',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'DESC',
            'AS', 'names')

    res1 = env.cmd(*cmd)
    res2 = env.cmd(*cmd)
    env.assertEqual(res1, res2, message='order must be stable across runs')

    attrs = res1['results'][0]['extra_attributes']
    # DESC + ties → larger doc_id wins → reverse insertion order.
    env.assertEqual([r['name'] for r in attrs['names']],
                    list(reversed(names_in_insert_order)))


@skip(cluster=False)
def test_collect_cluster_sortby_tiebreak_result_complete():
    """Multi-shard. Shard payloads arriving on the coordinator carry no
    document id, so the tie-break collapses on coord — matching
    FT.AGGREGATE SORTBY's coord semantics: order depends on shard
    arrival sequence and is not guaranteed stable across runs. This
    test therefore only verifies completeness of the result set."""
    env = Env(shardsCount=3, protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)
    conn = getConnectionByEnv(env)
    # Spread documents across shards via hash tags; all share the same
    # sweetness so the SORTBY key fully ties.
    for i in range(9):
        conn.execute_command('HSET', f'doc:{i}{{shard:{i % 3}}}',
                             'name', f'n{i}', 'color', 'black', 'sweetness', '1')

    cmd = (
        'FT.AGGREGATE', 'idx', '@color:{black}',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'ASC',
            'AS', 'names')
    res = env.cmd(*cmd)
    attrs = res['results'][0]['extra_attributes']
    names = sorted(r['name'] for r in attrs['names'])
    env.assertEqual(names, [f'n{i}' for i in range(9)])


@skip(cluster=True)
def test_collect_sortby_tiebreak_two_collects_same_groupby():
    """Two sibling COLLECT … SORTBY reducers in the same GROUPBY must both
    receive the doc_id tie-break — not only the first one. Regression
    guard for the read-then-write fix on `RLookup_GetKey_Write`, which
    returns NULL on a duplicate name and would silently leave the second
    reducer with `doc_id_key == NULL`."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)
    conn = getConnectionByEnv(env)
    # All docs share color='black' and sweetness=1 — both SORTBY keys fully
    # tie, so the only thing that can order the rows is the doc_id
    # tie-break.
    names_in_insert_order = ['a', 'b', 'c', 'd', 'e']
    for i, name in enumerate(names_in_insert_order):
        conn.execute_command('HSET', f'doc:{i}', 'name', name,
                             'color', 'black', 'sweetness', '1')

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '@color:{black}',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'ASC',
            'AS', 'first_names',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'ASC',
            'AS', 'second_names')

    attrs = res['results'][0]['extra_attributes']
    first  = [r['name'] for r in attrs['first_names']]
    second = [r['name'] for r in attrs['second_names']]

    # Under ASC with all sort-values tied, both must collapse to
    # insertion (doc_id) order.
    env.assertEqual(first, names_in_insert_order)
    env.assertEqual(second, names_in_insert_order,
                    message="second COLLECT lost its doc_id tie-break "
                            "(RLookup_GetKey_Write returned NULL on a "
                            "duplicate `__docid` registration)")


@skip(cluster=True)
def test_collect_sortby_tiebreak_two_collects_same_groupby_asc_desc():
    """Two sibling COLLECT … SORTBY reducers with opposite directions must
    be exact reverses of each other when all sort values tie. Holds only
    on a single shard (real doc_ids drive the tie-break); in cluster
    mode each shard pre-sorts its slice in its own direction, and the
    coordinator merges differently ordered shard payloads without global
    doc_ids, so the reverse-invariant breaks."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _setup_hash(env)
    conn = getConnectionByEnv(env)
    # All docs share color='black' and sweetness=1 — the SORTBY key fully
    # ties, so ASC/DESC differ only by the doc_id tie-break direction.
    names_in_insert_order = ['a', 'b', 'c', 'd', 'e']
    for i, name in enumerate(names_in_insert_order):
        conn.execute_command('HSET', f'doc:{i}', 'name', name,
                             'color', 'black', 'sweetness', '1')

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '@color:{black}',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'ASC',
            'AS', 'asc_names',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', '@name',
                'SORTBY', '2', '@sweetness', 'DESC',
            'AS', 'desc_names')

    attrs = res['results'][0]['extra_attributes']
    asc_names = [r['name'] for r in attrs['asc_names']]
    desc_names = [r['name'] for r in attrs['desc_names']]

    env.assertEqual(desc_names, list(reversed(asc_names)))


@skip(cluster=True)
def test_collect_sortby_does_not_clobber_user_schema_field_on_slot_name_collision():
    """COLLECT ... SORTBY reserves an internal hidden slot in the source lookup
    to plant the upstream doc id for tie-breaking.
    Slot reservation must not bind to a user-visible schema field, on collision
    the reducer must skip tie-break (logs a warning) and pass user data through
    untouched. The slot name must match `COLLECT_DOCID_SLOT` in
    `src/aggregate/reducers/collect.c`."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    slot_name = '__internal_collect_docid'
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'color', 'TAG', 'SORTABLE',
               'sweetness', 'NUMERIC', 'SORTABLE',
               slot_name, 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    user_values = ['alpha', 'beta', 'gamma', 'delta']
    for i, val in enumerate(user_values):
        conn.execute_command('HSET', f'doc:{i}',
                             'color', 'black', 'sweetness', str(i),
                             slot_name, val)

    res = env.cmd(
        'FT.AGGREGATE', 'idx', '@color:{black}',
        'GROUPBY', '1', '@color',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '1', f'@{slot_name}',
                'SORTBY', '2', '@sweetness', 'ASC',
            'AS', 'collected')

    attrs = res['results'][0]['extra_attributes']
    collected_values = [str(r.get(slot_name, '')) for r in attrs['collected']]
    # The user's field must come through untouched; the internal
    # tie-break slot must not bind to the user-visible field of the same name.
    env.assertEqual(sorted(collected_values), sorted(user_values),
                    message=f'user @{slot_name} clobbered by internal slot: {collected_values}')
