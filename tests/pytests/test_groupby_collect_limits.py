from common import *


# Limits exercised here, mirrored from C constants so a future bump fails loudly:
#   COLLECT FIELDS count : SPEC_MAX_FIELDS + 1  (src/aggregate/reducers/collect.c)
#   COLLECT SORTBY keys  : SORTASCMAP_MAXFIELDS (src/result_processor.h)
SPEC_MAX_FIELDS = 1024
COLLECT_MAX_FIELD_ARGS = SPEC_MAX_FIELDS + 1
COLLECT_MAX_SORT_KEYS = 8

# TEXT fields share the t_fieldMask bitmap (sizeof(t_fieldMask) * 8 = 128) so
# only the first 128 fields can be TEXT; the rest must use TAG or NUMERIC.
SPEC_MAX_TEXT_FIELDS = 128


def _field_type(i):
    """Distribute field types across the schema while staying within the TEXT
    bitmap cap."""
    if i < SPEC_MAX_TEXT_FIELDS:
        return 'TEXT'
    # Alternate TAG and NUMERIC for the remainder.
    return 'TAG' if (i % 2 == 0) else 'NUMERIC'


def _field_value(i):
    """Numeric fields require parseable numeric values or indexing fails."""
    return str(i) if _field_type(i) == 'NUMERIC' else f'v{i}'


def _create_wide_index(env, n_fields):
    """Create idx with `n_fields` fields named f0..f{n-1}"""
    schema = []
    for i in range(n_fields):
        schema.extend([f'f{i}', _field_type(i)])
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *schema).ok()


def _create_sortby_index(env):
    """Index with 9 sortable fields so we can build SORTBY lists at and over
    COLLECT_MAX_SORT_KEYS (8)."""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               's0', 'TAG', 'SORTABLE',
               's1', 'NUMERIC', 'SORTABLE',
               's2', 'NUMERIC', 'SORTABLE',
               's3', 'NUMERIC', 'SORTABLE',
               's4', 'NUMERIC', 'SORTABLE',
               's5', 'NUMERIC', 'SORTABLE',
               's6', 'NUMERIC', 'SORTABLE',
               's7', 'NUMERIC', 'SORTABLE',
               's8', 'NUMERIC', 'SORTABLE').ok()
    getConnectionByEnv(env).execute_command(
        'HSET', 'doc:1',
        's0', 'g', 's1', '1', 's2', '2', 's3', '3', 's4', '4',
        's5', '5', 's6', '6', 's7', '7', 's8', '8')


# ---------------------------------------------------------------------------
# COLLECT FIELDS count limit
# ---------------------------------------------------------------------------
def test_collect_fields_at_spec_max_fields():
    """COLLECT FIELDS listing all SPEC_MAX_FIELDS (1024) schema fields runs
    end-to-end."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _create_wide_index(env, SPEC_MAX_FIELDS)

    # Create a single doc with all fields set.
    args = ['HSET', 'doc:1']
    for i in range(SPEC_MAX_FIELDS):
        args.extend([f'f{i}', _field_value(i)])
    getConnectionByEnv(env).execute_command(*args)

    names = [f'@f{i}' for i in range(SPEC_MAX_FIELDS)]
    reduce_argc = 2 + SPEC_MAX_FIELDS  # FIELDS <count> <name...>
    # LOAD '*' pulls every hash field into the RLookup; without it only the
    # SORTABLE @f0 would survive into the COLLECT input.
    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'LOAD', '*',
        'GROUPBY', '1', '@f0',
            'REDUCE', 'COLLECT', str(reduce_argc),
                'FIELDS', str(SPEC_MAX_FIELDS), *names,
            'AS', 'rows')

    env.assertEqual(len(res['results']), 1)
    collected = res['results'][0]['extra_attributes']['rows']
    env.assertEqual(len(collected), 1)
    env.assertEqual(len(collected[0]), SPEC_MAX_FIELDS)
    env.assertEqual(collected[0]['f0'], _field_value(0))
    last = SPEC_MAX_FIELDS - 1
    env.assertEqual(collected[0][f'f{last}'], _field_value(last))


def test_collect_fields_sparse_docs():
    """One doc per schema field, each setting only that single field."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _create_wide_index(env, SPEC_MAX_FIELDS)

    # Create SPEC_MAX_FIELDS docs, each with a single field set.
    conn = getConnectionByEnv(env)
    for i in range(SPEC_MAX_FIELDS):
        conn.execute_command('HSET', f'doc:{i}', f'f{i}', _field_value(i))

    names = [f'@f{i}' for i in range(SPEC_MAX_FIELDS)]
    reduce_argc = 2 + SPEC_MAX_FIELDS
    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'LOAD', '*',
        'GROUPBY', '0',
            'REDUCE', 'COLLECT', str(reduce_argc),
                'FIELDS', str(SPEC_MAX_FIELDS), *names,
            'AS', 'rows')

    env.assertEqual(len(res['results']), 1)
    collected = res['results'][0]['extra_attributes']['rows']
    env.assertEqual(len(collected), SPEC_MAX_FIELDS)
    expected = sorted(
        ({f'f{i}': _field_value(i)} for i in range(SPEC_MAX_FIELDS)),
        key=lambda d: next(iter(d)))
    got = sorted(collected, key=lambda d: next(iter(d)))
    env.assertEqual(got, expected)


def test_collect_fields_count_over_max_errors():
    """COLLECT FIELDS count > COLLECT_MAX_FIELD_ARGS is rejected at parse time,
    so we don't need to actually provide the oversized name list."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _create_wide_index(env, 2)
    over = COLLECT_MAX_FIELD_ARGS + 1
    env.expect(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@f0',
            'REDUCE', 'COLLECT', '3',
                'FIELDS', str(over), '@f0',
            'AS', 'rows'
    ).error().contains(
        f'FIELDS count must be in [1, {COLLECT_MAX_FIELD_ARGS}]')


# ---------------------------------------------------------------------------
# COLLECT SORTBY key-count limit
# ---------------------------------------------------------------------------
def test_collect_sortby_at_max_keys():
    """COLLECT SORTBY with exactly COLLECT_MAX_SORT_KEYS keys succeeds."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _create_sortby_index(env)

    sort_tokens = []
    for i in range(COLLECT_MAX_SORT_KEYS):
        sort_tokens.extend([f'@s{i + 1}', 'ASC'])

    reduce_argc = 5 + len(sort_tokens)  # FIELDS 1 @x SORTBY <n> <tokens>
    res = env.cmd(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@s0',
            'REDUCE', 'COLLECT', str(reduce_argc),
                'FIELDS', '1', '@s1',
                'SORTBY', str(len(sort_tokens)), *sort_tokens,
            'AS', 'rows')

    env.assertEqual(len(res['results']), 1)
    env.assertEqual(res['results'][0]['extra_attributes']['rows'],
                    [{'s1': '1'}])


def test_collect_sortby_over_max_keys_errors():
    """COLLECT SORTBY with > COLLECT_MAX_SORT_KEYS keys is rejected on the
    standalone (local) parse path."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    _create_sortby_index(env)

    n = COLLECT_MAX_SORT_KEYS + 1
    sort_tokens = []
    for i in range(n):
        sort_tokens.extend([f'@s{i}', 'ASC'])

    reduce_argc = 5 + len(sort_tokens)  # FIELDS 1 @x SORTBY <n> <tokens>
    env.expect(
        'FT.AGGREGATE', 'idx', '*',
        'GROUPBY', '1', '@s0',
            'REDUCE', 'COLLECT', str(reduce_argc),
                'FIELDS', '1', '@s1',
                'SORTBY', str(len(sort_tokens)), *sort_tokens,
            'AS', 'rows'
    ).error().contains(
        f'SORTBY exceeds maximum of {COLLECT_MAX_SORT_KEYS} fields')
