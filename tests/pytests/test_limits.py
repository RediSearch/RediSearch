# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *


def testDebugSetMaxIndexes(env):
    """Test that FT.DEBUG SET_MAX_INDEXES works"""
    DEFAULT_MAX_INDEXES = 200_000
    env.expect(debug_cmd(), 'SET_MAX_INDEXES', DEFAULT_MAX_INDEXES).ok()
    env.expect(debug_cmd(), 'SET_MAX_INDEXES', 0).error()\
        .contains(f'Invalid value. Must be between 1 and {DEFAULT_MAX_INDEXES}')
    env.expect(debug_cmd(), 'SET_MAX_INDEXES', DEFAULT_MAX_INDEXES + 1).error()\
        .contains(f'Invalid value. Must be between 1 and {DEFAULT_MAX_INDEXES}')
    env.expect(debug_cmd(), 'SET_MAX_INDEXES', 1000).ok()

    num_indexes = 10
    for i in range(num_indexes):
        env.expect('FT.CREATE', f'idx{i}', 'ON', 'HASH',
                   'SCHEMA', 'name', 'TEXT').ok()

    env.expect(debug_cmd(), 'SET_MAX_INDEXES', num_indexes).ok()
    env.expect(debug_cmd(), 'SET_MAX_INDEXES', num_indexes - 1).error()\
        .contains(f'Invalid value. Must be at least current number of indexes: {num_indexes}')
    env.expect(debug_cmd(), 'SET_MAX_INDEXES', DEFAULT_MAX_INDEXES).ok()


def testMaxIndexesLimit(env):
    """Test that creating more indexes than MAX_INDEXES fails"""
    MAX_INDEXES = 5

    # Set a small MAX_INDEXES limit for testing using the debug command
    env.expect(debug_cmd(), 'SET_MAX_INDEXES', MAX_INDEXES).ok()

    # Create indexes up to the limit (should succeed)
    for i in range(MAX_INDEXES):
        env.expect('FT.CREATE', f'idx{i}', 'ON', 'HASH',
                   'SCHEMA', 'name', 'TEXT').ok()

    # Read number of indexes from INFO SEARCH
    info_search = env.cmd('INFO', 'SEARCH')
    env.assertEqual(info_search['search_number_of_indexes'], MAX_INDEXES)

    # Creating one more index should fail
    err_message = f'Maximum number of indexes ({MAX_INDEXES}) reached'
    env.expect('FT.CREATE', 'idx_overflow', 'ON', 'HASH',
               'PREFIX', '1', 'overflow:',
               'SCHEMA', 'name', 'TEXT').error().contains(err_message)

    # Drop one index and verify we can create a new one
    env.expect('FT.DROPINDEX', 'idx0').ok()
    info_search = env.cmd('INFO', 'SEARCH')
    env.assertEqual(info_search['search_number_of_indexes'], MAX_INDEXES - 1)

    env.expect('FT.CREATE', 'idx_new', 'ON', 'HASH',
               'PREFIX', '1', 'new:',
               'SCHEMA', 'name', 'TEXT').ok()
    info_search = env.cmd('INFO', 'SEARCH')
    env.assertEqual(info_search['search_number_of_indexes'], MAX_INDEXES)

    # Verify we're back at the limit
    env.expect('FT.CREATE', 'idx_overflow2', 'ON', 'HASH',
               'PREFIX', '1', 'overflow2:',
               'SCHEMA', 'name', 'TEXT').error().contains(err_message)
    info_search = env.cmd('INFO', 'SEARCH')
    env.assertEqual(info_search['search_number_of_indexes'], MAX_INDEXES)

def testMaxSchemaPrefixesLimit(env):
    """Test that creating an index with more than MAX_SCHEMA_PREFIXES (1024)
    prefixes fails"""
    MAX_SCHEMA_PREFIXES = 1_000_000

    # Generate prefixes exceeding the limit
    prefixes = [f'prefix{i}:' for i in range(MAX_SCHEMA_PREFIXES + 1)]

    # Build the FT.CREATE command with too many prefixes
    cmd = ['FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', str(len(prefixes))]
    cmd += prefixes + ['SCHEMA', 'name', 'text']

    # Expect an error due to exceeding the prefix limit
    err_message = f'Number of prefixes ({MAX_SCHEMA_PREFIXES + 1}) exceeds maximum allowed ({MAX_SCHEMA_PREFIXES})'
    env.expect(*cmd).error().contains(err_message)

    # Verify that creating an index with exactly MAX_SCHEMA_PREFIXES works
    prefixes_at_limit = prefixes[:-1]
    cmd_at_limit = ['FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX']
    cmd_at_limit += [str(len(prefixes_at_limit))] + prefixes_at_limit
    cmd_at_limit += ['SCHEMA', 'name', 'text']
    env.expect(*cmd_at_limit).ok()


def testMaxSynonymGroupIdsLimit(env):
    """Test that adding a term to more than MAX_SYNONYM_GROUP_IDS (4096)
    synonym groups fails"""
    MAX_SYNONYM_GROUP_IDS = 4096

    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'text').ok()

    # Add the same term to MAX_SYNONYM_GROUP_IDS groups (should succeed)
    # Each FT.SYNUPDATE adds a term to a group, and a term can belong to
    # multiple groups
    term = 'commonterm'
    for i in range(MAX_SYNONYM_GROUP_IDS):
        env.expect('FT.SYNUPDATE', 'idx', f'group{i}', term).ok()

    # Adding the term to one more group should fail
    env.expect('FT.SYNUPDATE', 'idx', f'group{MAX_SYNONYM_GROUP_IDS}', term) \
        .error().contains('Maximum group IDs per term limit reached')


def testMaxSynonymTermsLimit(env):
    """Test that adding more than MAX_SYNONYM_TERMS (1,000,000) unique terms
    to a synonym map fails (regardless of how many groups they belong to),
    and that a rejected batch is applied atomically (no partial insertion)."""
    MAX_SYNONYM_TERMS = 1_000_000
    BATCH_SIZE = 1000  # Add 1000 terms per FT.SYNUPDATE call for efficiency

    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'text').ok()

    # Fill to MAX_SYNONYM_TERMS - 1 so the overflow batch below has one term
    # that would succeed and one that would trip the limit -- this is what
    # exercises atomicity.
    num_batches = MAX_SYNONYM_TERMS // BATCH_SIZE
    for batch in range(num_batches):
        terms = [f'term{batch * BATCH_SIZE + i}' for i in range(BATCH_SIZE)]
        if batch == num_batches - 1:
            terms = terms[:-1]  # leave exactly one free slot
        env.expect('FT.SYNUPDATE', 'idx', f'group{batch}', *terms).ok()

    # Verify we have MAX_SYNONYM_TERMS - 1 unique terms in the map.
    dump = env.cmd('FT.SYNDUMP', 'idx')
    dumped_terms = set(dump[::2])
    env.assertEqual(len(dumped_terms), MAX_SYNONYM_TERMS - 1)

    # A 2-term batch must fail AND leave neither term inserted.
    env.expect('FT.SYNUPDATE', 'idx', 'overflow_group',
               'overflow_a', 'overflow_b') \
        .error().contains('Maximum synonym terms limit reached')

    # FT.SYNDUMP returns a flat [term, [ids...], term, [ids...], ...] list.
    dump = env.cmd('FT.SYNDUMP', 'idx')
    dumped_terms = set(dump[::2])
    env.assertEqual(len(dumped_terms), MAX_SYNONYM_TERMS - 1)
    env.assertNotIn('overflow_a', dumped_terms)
    env.assertNotIn('overflow_b', dumped_terms)

    # Fill the last remaining slot so the map sits at exactly MAX_SYNONYM_TERMS.
    # Duplicated fresh terms count as one distinct pending term.
    env.expect('FT.SYNUPDATE', 'idx', 'last_slot_group',
            'fresh_term', 'fresh_term').ok()

    dump = env.cmd('FT.SYNDUMP', 'idx')
    pairs = dict(zip(dump[::2], dump[1::2]))
    env.assertIn('fresh_term', pairs)
    env.assertEqual(len(pairs), MAX_SYNONYM_TERMS)
    env.assertEqual(pairs['fresh_term'], ['last_slot_group'])

    # Any further new term must now fail with the limit error.
    env.expect('FT.SYNUPDATE', 'idx', 'overflow_group2', 'overflow_c') \
        .error().contains('Maximum synonym terms limit reached')

    # An *existing* term can still be added to a new group even when the map
    # is at MAX_SYNONYM_TERMS (no insertion happens, so the limit check
    # must not trip).
    env.expect('FT.SYNUPDATE', 'idx', 'reuse_group', 'term0').ok()


def testMaxSynonymGroupIdsLimitAtomic(env):
    """A batch that would push an existing term over MAX_SYNONYM_GROUP_IDS
    must be rejected without applying any of its updates (atomicity)."""
    MAX_SYNONYM_GROUP_IDS = 4096

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'text').ok()

    # Saturate `crowded` with MAX_SYNONYM_GROUP_IDS distinct groups.
    crowded = 'crowded'
    for i in range(MAX_SYNONYM_GROUP_IDS):
        env.expect('FT.SYNUPDATE', 'idx', f'g{i}', crowded).ok()

    # FT.SYNUPDATE with a new term plus `crowded` (at the group-ID limit)
    # must be rejected as a whole.
    env.expect('FT.SYNUPDATE', 'idx', 'new_group', 'fresh_term', crowded) \
        .error().contains('Maximum group IDs per term limit reached')

    dump = env.cmd('FT.SYNDUMP', 'idx')
    pairs = dict(zip(dump[::2], dump[1::2]))
    env.assertNotIn('fresh_term', pairs)
    # crowded must NOT have gained 'new_group'.
    env.assertNotIn('new_group', pairs[crowded])


def testMaxFieldsLimit(env):
    """Test that creating an index with more than SPEC_MAX_FIELDS (4096) fields
    fails"""
    SPEC_MAX_FIELDS = 4096

    # Verify that creating an index with exactly SPEC_MAX_FIELDS works
    fields = []
    for i in range(SPEC_MAX_FIELDS):
        fields.extend([f'field{i}', 'TAG'])
    cmd_at_limit = ['FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA'] + fields
    env.expect(*cmd_at_limit).ok()

    # Build the FT.CREATE command with too many fields
    cmd_overflow = ['FT.CREATE', 'idx2', 'ON', 'HASH', 'SCHEMA'] + fields
    cmd_overflow.extend(['overflow_field', 'TAG'])

    # Expect an error due to exceeding the field limit
    err_message = f'Schema is limited to {SPEC_MAX_FIELDS} fields'
    env.expect(*cmd_overflow).error().contains(err_message)


def testMaxStopwordsLimit(env):
    """Test that creating an index with more than MAX_STOPWORDLIST_SIZE (1024)
    stopwords silently truncates to the limit.

    Note: Unlike other limits, exceeding MAX_STOPWORDLIST_SIZE does NOT return
    an error - it silently truncates the list. This test verifies the current
    behavior."""
    MAX_STOPWORDLIST_SIZE = 1024

    # Generate stopwords exceeding the limit
    stopwords = [f'w{i}' for i in range(MAX_STOPWORDLIST_SIZE + 100)]

    # Build the FT.CREATE command with too many stopwords
    cmd = ['FT.CREATE', 'idx', 'ON', 'HASH',  'STOPWORDS', str(len(stopwords))]
    cmd += stopwords + ['SCHEMA', 'name', 'TEXT']

    # Currently this succeeds (silently truncates) rather than returning an error
    env.expect(*cmd).ok()

    # Verify the index was created
    env.expect('FT._LIST').contains('idx')

    # Verify the stopwords list is truncated to the limit
    idx_info = env.cmd('FT.INFO', 'idx')
    idx_stopwords = idx_info[idx_info.index('stopwords_list') + 1]
    env.assertEqual(len(idx_stopwords), MAX_STOPWORDLIST_SIZE)


def testMaxSortableFieldsWithSparseDoc(env):
    """More than 1024 SORTABLE fields: a sparse document must not abort the
    server (the Rust sorting-vector ceiling must follow SPEC_MAX_FIELDS)."""
    n_fields = 4096
    schema = []
    for i in range(n_fields):
        schema.extend([f'n{i}', 'NUMERIC', 'SORTABLE'])
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *schema).ok()

    conn = getConnectionByEnv(env)
    last = n_fields - 1
    conn.execute_command('HSET', 'doc:1', f'n{last}', '7')
    conn.execute_command('HSET', 'doc:2', f'n{last}', '3')
    waitForIndex(env, 'idx')

    res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', f'n{last}', 'ASC', 'NOCONTENT')
    env.assertEqual(res[1:], ['doc:2', 'doc:1'])


def testWideSchemaFieldLookupAcrossReloadAndAlter(env):
    """Field lookup by name on a schema large enough for the by-name table:
    indexing, duplicate detection, RDB reload and FT.ALTER up to the limit."""
    SPEC_MAX_FIELDS = 4096
    n_initial = SPEC_MAX_FIELDS - 6
    conn = getConnectionByEnv(env)

    def schema_for(i):
        # Every 7th field is aliased so lookups must match the alias, not the path.
        name = f'f{i}'
        head = [f'hp{i}', 'AS', name] if i % 7 == 0 else [name]
        kind = ['TAG'] if i % 7 == 0 else ['NUMERIC', 'SORTABLE']
        return head + kind

    def doc_args(indices):
        args = []
        for i in indices:
            args.extend([f'hp{i}' if i % 7 == 0 else f'f{i}',
                         f't{i}' if i % 7 == 0 else str(i)])
        return args

    schema = []
    for i in range(n_initial):
        schema.extend(schema_for(i))
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *schema).ok()
    # A name that is already in the schema is rejected wherever it sits.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f0', 'TAG').error() \
        .contains('Duplicate field in schema')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', f'f{n_initial - 1}', 'TAG').error() \
        .contains('Duplicate field in schema')

    first = list(range(0, n_initial, 3))
    conn.execute_command('HSET', 'doc:a', *doc_args(first))
    conn.execute_command('HSET', 'doc:b', *doc_args([n_initial - 1, 1, 7]))
    waitForIndex(env, 'idx')

    def check_lookups():
        res = env.cmd('FT.SEARCH', 'idx', f'@f{n_initial - 1}:[{n_initial - 1} {n_initial - 1}]',
                      'NOCONTENT')
        env.assertEqual(sorted(res[1:]), ['doc:a', 'doc:b'])
        env.expect('FT.SEARCH', 'idx', '@f3000:[3000 3000]', 'NOCONTENT').equal([1, 'doc:a'])
        env.expect('FT.SEARCH', 'idx', '@f7:{t7}', 'NOCONTENT').equal([1, 'doc:b'])
        env.expect('FT.SEARCH', 'idx', '@f21:{t21}', 'NOCONTENT').equal([1, 'doc:a'])
        # The path name of an aliased field is not a schema field name.
        env.expect('FT.SEARCH', 'idx', '@hp7:{t7}').error()
        res = env.cmd('FT.SEARCH', 'idx', '@f1:[1 1]', 'NOCONTENT', 'SORTBY', 'f3000', 'ASC')
        env.assertEqual(res[1:], ['doc:b'])
        res = env.cmd('FT.AGGREGATE', 'idx', '@f3000:[3000 3000]', 'LOAD', 1, '@f3000')
        env.assertEqual(res[0], 1)
        res = env.cmd('FT.AGGREGATE', 'idx', '@f3000:[3000 3000]', 'LOAD', '*')
        row = dict(zip(res[1][::2], res[1][1::2]))
        env.assertEqual(row['f3000'], '3000')

    check_lookups()

    if not env.isCluster():
        env.cmd('DEBUG', 'RELOAD')
        waitForIndex(env, 'idx')
        check_lookups()
        # Names loaded from the RDB take part in duplicate detection.
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f0', 'TAG').error() \
            .contains('Duplicate field in schema')
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f7', 'TAG').error() \
            .contains('Duplicate field in schema')

    # A failed ALTER leaves the schema, and so its by-name lookups, untouched.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'newa', 'NUMERIC', 'newa', 'TAG').error()
    env.expect('FT.SEARCH', 'idx', '@newa:[0 1]').error()
    # The rolled-back name is free again, and only the retry defines it.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'newa', 'TAG').ok()
    conn.execute_command('HSET', 'doc:newa', 'newa', 'tagval')
    waitForIndex(env, 'idx')
    env.expect('FT.SEARCH', 'idx', '@newa:{tagval}', 'NOCONTENT').equal([1, 'doc:newa'])
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'newa', 'TAG').error() \
        .contains('Duplicate field in schema')

    added = [f'n{k}' for k in range(5)]
    alter = []
    for name in added:
        alter.extend([name, 'NUMERIC', 'SORTABLE'])
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *alter).ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'one_too_many', 'TAG').error() \
        .contains(f'Schema is limited to {SPEC_MAX_FIELDS} fields')

    conn.execute_command('HSET', 'doc:c', 'n4', '5', 'f3000', '3000')
    waitForIndex(env, 'idx')
    env.expect('FT.SEARCH', 'idx', '@n4:[5 5]', 'NOCONTENT').equal([1, 'doc:c'])
    check_lookups_after = env.cmd('FT.SEARCH', 'idx', '@f3000:[3000 3000]', 'NOCONTENT')
    env.assertEqual(sorted(check_lookups_after[1:]), ['doc:a', 'doc:c'])

    if not env.isCluster():
        env.cmd('DEBUG', 'RELOAD')
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH', 'idx', '@n4:[5 5]', 'NOCONTENT').equal([1, 'doc:c'])
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n0', 'TAG').error() \
            .contains(f'Schema is limited to {SPEC_MAX_FIELDS} fields')


def testFieldNameLookupAroundTableThresholds(env):
    """Schemas just below and above the sizes where the by-name table is built
    or grows: lookups, duplicate detection, case sensitivity and ALTER."""
    for n in (8, 9, 32, 33, 64, 65):
        env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
                   *[x for i in range(n) for x in (f'f{i}', 'TAG')]).ok()
        for name in ('f0', f'f{n // 2}', f'f{n - 1}'):
            env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', name, 'TAG').error() \
                .contains('Duplicate field in schema')
        # Names differing only in case are distinct fields.
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'F0', 'TAG').ok()
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'F0', 'TAG').error() \
            .contains('Duplicate field in schema')
        env.cmd('HSET', f'doc:{n}', f'f{n - 1}', 'last', 'F0', 'upper')
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH', 'idx', f'@f{n - 1}:{{last}}', 'NOCONTENT').equal([1, f'doc:{n}'])
        env.expect('FT.SEARCH', 'idx', '@F0:{upper}', 'NOCONTENT').equal([1, f'doc:{n}'])
        env.expect('FT.SEARCH', 'idx', '@f0:{upper}', 'NOCONTENT').equal([0])
        env.cmd('FT.DROPINDEX', 'idx')
        env.cmd('FLUSHALL')

    # A duplicate inside one FT.CREATE is caught past the linear-scan size too.
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               *[x for i in range(20) for x in (f'f{i}', 'TAG')], 'f3', 'TAG').error() \
        .contains('Duplicate field in schema')


def testFailedAlterShrinksNameTable(env):
    """A failed FT.ALTER that crossed a table-size boundary restores lookups for
    the original fields and leaves the added names free."""
    n = 32
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               *[x for i in range(n) for x in (f'f{i}', 'TAG')]).ok()
    # Fields 33 and 34 grow the table before the duplicate aborts the batch.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'x1', 'TAG', 'x2', 'TAG', 'x1', 'TAG').error() \
        .contains('Duplicate field in schema')
    env.expect('FT.SEARCH', 'idx', '@x1:{a}').error()
    env.expect('FT.SEARCH', 'idx', '@x2:{a}').error()
    for i in (0, 15, n - 1):
        env.expect('FT.SEARCH', 'idx', f'@f{i}:{{a}}', 'NOCONTENT').equal([0])
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'x1', 'TAG').ok()
    env.cmd('HSET', 'doc:1', 'x1', 'a', f'f{n - 1}', 'b')
    waitForIndex(env, 'idx')
    env.expect('FT.SEARCH', 'idx', '@x1:{a}', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', f'@f{n - 1}:{{b}}', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'x2', 'TAG').ok()


@skip(no_json=True)
def testJsonAliasLookupOnWideSchema(env):
    """On a JSON schema past the linear-scan size, fields resolve by alias
    (the field name) and not by JSON path, for queries and LOAD."""
    n = 40
    schema = [x for i in range(n) for x in (f'$.n{i}', 'AS', f'n{i}', 'NUMERIC', 'SORTABLE')]
    schema += ['$.p.q', 'AS', 'alias', 'TAG']
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *schema).ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'alias', 'TAG').error() \
        .contains('Duplicate field in schema')
    env.expect('JSON.SET', 'doc:1', '$', '{"n3": 3, "n39": 39, "p": {"q": "x"}}').ok()
    waitForIndex(env, 'idx')
    env.expect('FT.SEARCH', 'idx', '@alias:{x}', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@n39:[39 39]', 'NOCONTENT').equal([1, 'doc:1'])
    res = env.cmd('FT.AGGREGATE', 'idx', '@alias:{x}', 'LOAD', 3, '@alias', '@n3', '@n39')
    env.assertEqual(res[0], 1)
    row = dict(zip(res[1][::2], res[1][1::2]))
    env.assertEqual(row, {'alias': 'x', 'n3': '3', 'n39': '39'})
    res = env.cmd('FT.AGGREGATE', 'idx', '@alias:{x}', 'LOAD', '*')
    row = dict(zip(res[1][::2], res[1][1::2]))
    env.assertEqual(row.get('n39'), '39')
    env.assertEqual(row.get('alias'), 'x')
