from common import *


def with_simulate_in_flex(enabled, module_args='', no_default_module_args=False):
    mode = 'true' if enabled else 'false'
    args = f'_SIMULATE_IN_FLEX {mode}'
    if module_args:
        args = f'{args} {module_args}'

    def decorator(test_fn):
        def wrapper():
            env = Env(moduleArgs=args, noDefaultModuleArgs=no_default_module_args)
            if env.env == 'existing-env':
                env.skip()
            try:
                return test_fn(env)
            finally:
                env.stop()
        return wrapper

    return decorator


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_max_index_limit(env):
    """Test that creating more than 10 indices fails when search-_simulate-in-flex is true"""
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
@with_simulate_in_flex(True)
def test_invalid_field_type(env):
    """Test that creating an index with an invalid field type fails when search-_simulate-in-flex is true"""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'GEO') \
        .error().contains('GEO fields are not supported in Flex indexes')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'GEOSHAPE') \
        .error().contains('GEOSHAPE fields are not supported in Flex indexes')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'NUMERIC') \
        .error().contains('NUMERIC fields are not supported in Flex indexes')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_valid_field_types(env):
    """Test that creating an index with valid field types succeeds when search-_simulate-in-flex is true"""
    # Create index with TEXT fields (supported in Flex, but without SORTABLE)
    env.expect('FT.CREATE', 'valid_idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
               'title', 'TEXT',
               'description', 'TEXT', 'WEIGHT', '2.0',
               'content', 'TEXT').ok()

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
@with_simulate_in_flex(True)
def test_valid_flex_arguments(env):
    """Test that supported FT.CREATE arguments work correctly in Flex mode"""
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
@with_simulate_in_flex(True)
def test_unsupported_flex_arguments(env):
    """Test that unsupported FT.CREATE arguments fail in Flex mode"""
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
@with_simulate_in_flex(True)
def test_unsupported_schema_options(env):
    """Test that unsupported schema field options fail in Flex mode"""
    # Test SORTABLE is not supported
    env.expect('FT.CREATE', 'idx1', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT', 'SORTABLE') \
        .error().contains('Disk index does not support SORTABLE fields')

    # Test NOINDEX is not supported
    env.expect('FT.CREATE', 'idx2', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT', 'NOINDEX') \
        .error().contains('Disk index does not support NOINDEX fields')

    # Test INDEXMISSING is not supported
    env.expect('FT.CREATE', 'idx3', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT', 'INDEXMISSING') \
        .error().contains('Disk index does not support INDEXMISSING fields')

    # Test INDEXEMPTY is not supported
    env.expect('FT.CREATE', 'idx4', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT', 'INDEXEMPTY') \
        .error().contains('Disk index does not support INDEXEMPTY fields')

    # Test non-TEXT/TAG/VECTOR field types are not supported (already covered in test_invalid_field_type,
    # but this uses the new error message path)
    env.expect('FT.CREATE', 'idx5', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'NUMERIC') \
        .error().contains('Disk index does not support non-TEXT/VECTOR/TAG fields')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_missing_skip_initial_scan(env):
    """Test that SKIPINITIALSCAN is required when search-_simulate-in-flex is true"""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Flex index requires SKIPINITIALSCAN argument')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_invalid_on_json(env):
    """Test that ON JSON fails when search-_simulate-in-flex is true"""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SKIPINITIALSCAN', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Only HASH is supported as index data type for Flex indexes')


@skip(cluster=True)
@with_simulate_in_flex(False)
def test_default_on_hash(env):
    """Test that ON HASH fails when search-_simulate-in-flex is false"""
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
@with_simulate_in_flex(True)
def test_flex_workers_minimum(env):
    """Test WORKERS validation in Flex mode: CONFIG SET silently corrects, FT.CONFIG fails"""
    # First set workers to a non-zero value (to ensure we test the validation,
    # since Redis config API may not call the setter if value is unchanged)
    env.expect('CONFIG', 'SET', 'search-workers', '2').ok()

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
@with_simulate_in_flex(True)
def test_flex_gc_config_defaults_and_set(env):
    """In Flex mode (simulate-in-flex), GET returns current values; SET overrides them."""
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
@with_simulate_in_flex(
    True,
    module_args='FORK_GC_RUN_INTERVAL 60 FORK_GC_CLEAN_THRESHOLD 500',
    no_default_module_args=True,
)
def test_flex_gc_config_explicit_override(env):
    """Explicit config args on startup; first GET returns those values."""
    env.expect(config_cmd(), 'GET', 'FORK_GC_RUN_INTERVAL').equal([['FORK_GC_RUN_INTERVAL', '60']])
    env.expect(config_cmd(), 'GET', 'FORK_GC_CLEAN_THRESHOLD').equal([['FORK_GC_CLEAN_THRESHOLD', '500']])


def _create_flex_search_fixture(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('HSET', 'doc:1', 't', 'hello world').equal(1)


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_requires_nocontent_or_return_0(env):
    """In Flex mode, FT.SEARCH must use NOCONTENT (explicit) or RETURN 0."""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello') \
        .error().contains('NOCONTENT or RETURN 0 must be provided for disk indexes')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_allows_nocontent(env):
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc:1'])


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_allows_return_0(env):
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'RETURN', '0').equal([1, 'doc:1'])


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_allows_nocontent_withscores(env):
    _create_flex_search_fixture(env)

    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'WITHSCORES')
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], 'doc:1')
    env.assertGreater(float(res[2]), 0.0)


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_rejects_load_with_nocontent_or_return_0(env):
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'LOAD', '1', '@t') \
        .error().contains('LOAD is not supported for disk indexes')

    env.expect('FT.SEARCH', 'idx', 'hello', 'RETURN', '0', 'LOAD', '1', '@t') \
        .error().contains('LOAD is not supported for disk indexes')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_aggregate_and_hybrid_commands(env):
    _create_flex_search_fixture(env)

    env.expect('FT.AGGREGATE', 'idx', '*') \
        .error().contains('FT.AGGREGATE is not supported in disk mode')
    env.expect('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*') \
        .error().contains('FT.AGGREGATE is not supported in disk mode')

    env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB') \
        .error().contains('FT.HYBRID is not supported in disk mode')
    env.expect('FT.PROFILE', 'idx', 'HYBRID', 'QUERY', 'SEARCH', '*', 'VSIM', '@v', '$BLOB') \
        .error().contains('FT.HYBRID is not supported in disk mode')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_dict_commands(env):
    _create_flex_search_fixture(env)

    env.expect('FT.DICTADD', 'dict', 'foo') \
        .error().contains('FT.DICTADD is not supported in disk mode')
    env.expect('FT.DICTDEL', 'dict', 'foo') \
        .error().contains('FT.DICTDEL is not supported in disk mode')
    env.expect('FT.DICTDUMP', 'dict') \
        .error().contains('FT.DICTDUMP is not supported in disk mode')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_alter_command(env):
    _create_flex_search_fixture(env)

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 't2', 'TEXT') \
        .error().contains('FT.ALTER is not supported in disk mode')
    env.expect('FT._ALTERIFNX', 'idx', 'SCHEMA', 'ADD', 't2', 'TEXT') \
        .error().contains('FT._ALTERIFNX is not supported in disk mode')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_cursor_commands(env):
    _create_flex_search_fixture(env)
    env.expect('FT.CURSOR', 'READ', 'idx', '1') \
        .error().contains('FT.CURSOR is not supported in disk mode')
    env.expect('FT.CURSOR', 'DEL', 'idx', '1') \
        .error().contains('FT.CURSOR is not supported in disk mode')
    env.expect('FT.CURSOR', 'GC', 'idx') \
        .error().contains('FT.CURSOR is not supported in disk mode')
    
@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_debug_wrappers_for_aggregate_and_hybrid(env):
    _create_flex_search_fixture(env)

    env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'TIMEOUT_AFTER_N', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.AGGREGATE is not supported in disk mode')
    env.expect(debug_cmd(), 'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'TIMEOUT_AFTER_N', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.AGGREGATE is not supported in disk mode')

    env.expect(debug_cmd(), 'FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
               'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.HYBRID is not supported in disk mode')
    env.expect(debug_cmd(), 'FT.PROFILE', 'idx', 'HYBRID', 'QUERY', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
               'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.HYBRID is not supported in disk mode')

@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_suggest_commands(env):
    _create_flex_search_fixture(env)

    env.expect('FT.SUGADD', 'idx', 'foo', '1') \
        .error().contains('FT.SUGADD is not supported in disk mode')
    env.expect('FT.SUGGET', 'idx', 'fo') \
        .error().contains('FT.SUGGET is not supported in disk mode')
    env.expect('FT.SUGDEL', 'idx', 'foo') \
        .error().contains('FT.SUGDEL is not supported in disk mode')
    env.expect('FT.SUGLEN', 'idx') \
        .error().contains('FT.SUGLEN is not supported in disk mode')

