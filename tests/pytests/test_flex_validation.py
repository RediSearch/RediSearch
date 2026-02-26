from common import *

@skip(cluster=True)
def test_flex_max_index_limit(env):
    """Test that creating more than 10 indices fails when search-_simulate-in-flex is true"""

    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()

    # Create 10 indices successfully (the maximum allowed)
    for i in range(10):
        index_name = f'idx{i}'
        env.expect('FT.CREATE', index_name, 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT').ok()

    # Verify all 10 indices were created
    info_result = env.cmd('FT._LIST')
    env.assertEqual(len(info_result), 10)

    # Try to create the 11th index - this should fail
    env.expect('FT.CREATE', 'idx10', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Max number of indexes reached for Flex indexes: 10')

@skip(cluster=True)
def test_invalid_field_type(env):
    """Test that creating an index with an invalid field type fails when search-_simulate-in-flex is true"""
    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'GEO') \
        .error().contains('GEO fields are not supported in Flex indexes')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'GEOSHAPE') \
        .error().contains('GEOSHAPE fields are not supported in Flex indexes')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'NUMERIC') \
        .error().contains('NUMERIC fields are not supported in Flex indexes')

@skip(cluster=True)
def test_valid_field_types(env):
    """Test that creating an index with valid field types succeeds when search-_simulate-in-flex is true"""
    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()

    # Create index with only TEXT fields (the only supported type in Flex)
    env.expect('FT.CREATE', 'valid_idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
               'title', 'TEXT',
               'description', 'TEXT', 'WEIGHT', '2.0',
               'content', 'TEXT', 'SORTABLE').ok()

    # Verify the index was created
    info_result = env.cmd('FT.INFO', 'valid_idx')

    # Find the attributes section
    schema_info = None
    for i in range(0, len(info_result), 2):
        if info_result[i] == 'attributes':
            schema_info = info_result[i + 1]
            break
    env.assertEqual(len(schema_info), 3)

    # Parse field information correctly
    field_names = []
    field_types = []

    for field_info in schema_info:
        # Each field_info is a list like ['identifier', 'title', 'attribute', 'title', 'type', 'TEXT', ...]
        attribute_name = field_info[3]  # The actual field name
        type_index = field_info.index('type') + 1
        field_type = field_info[type_index]

        field_names.append(attribute_name)
        field_types.append(field_type)

    # Verify field names
    env.assertIn('title', field_names)
    env.assertIn('description', field_names)
    env.assertIn('content', field_names)

    # All fields should be TEXT type
    for field_type in field_types:
        env.assertEqual(field_type, 'TEXT')

@skip(cluster=True)
def test_valid_flex_arguments(env):
    """Test that supported FT.CREATE arguments work correctly in Flex mode"""
    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()

    # Test with all supported Flex arguments
    env.expect('FT.CREATE', 'flex_args_idx', 'ON', 'HASH', 'SKIPINITIALSCAN',
               'PREFIX', '2', 'doc:', 'item:',
               'FILTER', '@status=="active"',
               'LANGUAGE', 'english',
               'LANGUAGE_FIELD', 'lang',
               'SCORE', '0.5',
               'SCORE_FIELD', 'score',
               'STOPWORDS', '2', 'the', 'and',
               'SCHEMA', 'title', 'TEXT', 'body', 'TEXT').ok()

    # Verify the index was created successfully
    info_result = env.cmd('FT.INFO', 'flex_args_idx')
    env.assertTrue(info_result is not None)

@skip(cluster=True)
def test_unsupported_flex_arguments(env):
    """Test that unsupported FT.CREATE arguments fail in Flex mode"""
    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()

    # Test unsupported arguments that are valid in regular mode
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'NOOFFSETS', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unsupported argument for Flex index: `NOOFFSETS`')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'NOHL', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unsupported argument for Flex index: `NOHL`')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'NOFIELDS', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unsupported argument for Flex index: `NOFIELDS`')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'NOFREQS', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unsupported argument for Flex index: `NOFREQS`')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'ASYNC', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unsupported argument for Flex index: `ASYNC`')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'MAXTEXTFIELDS', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unsupported argument for Flex index: `MAXTEXTFIELDS`')

    # Test unsupported arguments that are invalid in RAM, should give same error
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'RANDOM_NAME', 'payload', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unknown argument `RANDOM_NAME`')

@skip(cluster=True)
def test_missing_skip_initial_scan(env):
    """Test that SKIPINITIALSCAN is required when search-_simulate-in-flex is true"""
    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Flex index requires SKIPINITIALSCAN argument')

@skip(cluster=True)
def test_invalid_on_json(env):
    """Test that ON JSON fails when search-_simulate-in-flex is true"""
    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Only HASH is supported as index data type for Flex indexes')

@skip(cluster=True)
def test_default_on_hash(env):
    """Test that ON HASH fails when search-_simulate-in-flex is false"""
    # Set the simulate-in-flex configuration to false
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'no').ok()

    env.expect('FT.CREATE', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT').ok()

    info_result = env.cmd('FT.INFO', 'idx')

    # Find the index_definition section
    index_definition = None
    for i in range(0, len(info_result), 2):
        if info_result[i] == 'index_definition':
            index_definition = info_result[i + 1]
            break

    # Extract key_type from index_definition
    key_type = None
    for i in range(0, len(index_definition), 2):
        if index_definition[i] == 'key_type':
            key_type = index_definition[i + 1]
            break

    env.assertEqual(key_type, 'HASH')

@skip(cluster=True)
def test_flex_workers_minimum(env):
    """Test WORKERS validation in Flex mode: CONFIG SET silently corrects, FT.CONFIG fails"""
    # First set workers to a non-zero value (to ensure we test the validation,
    # since Redis config API may not call the setter if value is unchanged)
    env.expect('CONFIG', 'SET', 'search-workers', '2').ok()

    # Set the simulate-in-flex configuration to true
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()

    # Verify that setting WORKERS to 0 silently sets it to 1 via CONFIG SET
    env.expect('CONFIG', 'SET', 'search-workers', '0').ok()
    env.expect('CONFIG', 'GET', 'search-workers').equal(['search-workers', '1'])

    # Verify that setting WORKERS to 0 fails via the deprecated FT.CONFIG SET
    env.expect('FT.CONFIG', 'SET', 'WORKERS', '0').ok()
    env.expect('FT.CONFIG', 'GET', 'WORKERS').equal([['WORKERS', '1']])

    # Verify that setting WORKERS to higher values still works
    env.expect('CONFIG', 'SET', 'search-workers', '2').ok()
    env.expect('CONFIG', 'GET', 'search-workers').equal(['search-workers', '2'])

    env.expect('FT.CONFIG', 'SET', 'WORKERS', '3').ok()
    env.expect('FT.CONFIG', 'GET', 'WORKERS').equal([['WORKERS', '3']])


@skip(cluster=True)
def test_flex_gc_config_defaults_and_set(env):
    """In Flex mode (simulate-in-flex), GET returns current values; SET overrides them."""
    env.expect('CONFIG', 'SET', 'search-_simulate-in-flex', 'yes').ok()

    # Get current values (fork defaults when not in real Flex)
    env.expect(config_cmd(), 'GET', 'FORK_GC_RUN_INTERVAL').equal([['FORK_GC_RUN_INTERVAL', '30']])
    env.expect(config_cmd(), 'GET', 'FORK_GC_CLEAN_THRESHOLD').equal([['FORK_GC_CLEAN_THRESHOLD', '100']])

    # SET new values
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 600).ok()
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 20000).ok()

    # GET reflects the change
    env.expect(config_cmd(), 'GET', 'FORK_GC_RUN_INTERVAL').equal([['FORK_GC_RUN_INTERVAL', '600']])
    env.expect(config_cmd(), 'GET', 'FORK_GC_CLEAN_THRESHOLD').equal([['FORK_GC_CLEAN_THRESHOLD', '20000']])


@skip(cluster=True)
def test_flex_gc_config_explicit_override(env):
    """Explicit config args on startup; first GET returns those values."""
    custom_env = Env(
        moduleArgs='FORK_GC_RUN_INTERVAL 60 FORK_GC_CLEAN_THRESHOLD 500',
        noDefaultModuleArgs=True,
    )
    if custom_env.env == 'existing-env':
        custom_env.skip()

    custom_env.expect(config_cmd(), 'GET', 'FORK_GC_RUN_INTERVAL').equal([['FORK_GC_RUN_INTERVAL', '60']])
    custom_env.expect(config_cmd(), 'GET', 'FORK_GC_CLEAN_THRESHOLD').equal([['FORK_GC_CLEAN_THRESHOLD', '500']])
    custom_env.stop()
