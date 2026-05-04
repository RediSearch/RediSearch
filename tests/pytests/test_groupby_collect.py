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
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'team', 'TAG', 'SORTABLE',
               'score', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:alpha', 'name', 'alice', 'team', 't', 'score', '7')
    conn.execute_command('HSET', 'doc:bravo', 'name', 'bob',   'team', 't', 'score', '5')
    enable_unstable_features(env)


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
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'team', 'TAG', 'SORTABLE',
               'extra', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:full',    'name', 'full',    'team', 'g', 'extra', 'set')
    conn.execute_command('HSET', 'doc:partial', 'name', 'partial', 'team', 'g')
    enable_unstable_features(env)

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
