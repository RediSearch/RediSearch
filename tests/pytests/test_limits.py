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
    to a synonym map fails (regardless of how many groups they belong to)"""
    MAX_SYNONYM_TERMS = 1_000_000
    BATCH_SIZE = 1000  # Add 1000 terms per FT.SYNUPDATE call for efficiency

    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'text').ok()

    # Add terms in batches until we reach the limit
    num_batches = MAX_SYNONYM_TERMS // BATCH_SIZE
    for batch in range(num_batches):
        terms = [f'term{batch * BATCH_SIZE + i}' for i in range(BATCH_SIZE)]
        env.expect('FT.SYNUPDATE', 'idx', f'group{batch}', *terms).ok()

    # Adding one more term should fail
    env.expect('FT.SYNUPDATE', 'idx', 'overflow_group', 'overflow_term') \
        .error().contains('Maximum synonym terms limit reached')


def testMaxFieldsLimit(env):
    """Test that creating an index with more than SPEC_MAX_FIELDS (1024) fields
    fails"""
    SPEC_MAX_FIELDS = 1024

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
