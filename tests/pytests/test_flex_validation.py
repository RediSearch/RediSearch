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
        .error().contains('NOCONTENT or RETURN 0 must be provided in Redis Flex')


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
        .error().contains('LOAD is not supported in Redis Flex')

    env.expect('FT.SEARCH', 'idx', 'hello', 'RETURN', '0', 'LOAD', '1', '@t') \
        .error().contains('LOAD is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_aggregate_and_hybrid_commands(env):
    _create_flex_search_fixture(env)

    env.expect('FT.AGGREGATE', 'idx', '*') \
        .error().contains('FT.AGGREGATE is not supported in Redis Flex')
    env.expect('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*') \
        .error().contains('FT.AGGREGATE is not supported in Redis Flex')

    env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB') \
        .error().contains('FT.HYBRID is not supported in Redis Flex')
    env.expect('FT.PROFILE', 'idx', 'HYBRID', 'QUERY', 'SEARCH', '*', 'VSIM', '@v', '$BLOB') \
        .error().contains('FT.HYBRID is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_dict_commands(env):
    _create_flex_search_fixture(env)

    env.expect('FT.DICTADD', 'dict', 'foo') \
        .error().contains('FT.DICTADD is not supported in Redis Flex')
    env.expect('FT.DICTDEL', 'dict', 'foo') \
        .error().contains('FT.DICTDEL is not supported in Redis Flex')
    env.expect('FT.DICTDUMP', 'dict') \
        .error().contains('FT.DICTDUMP is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_alter_command(env):
    _create_flex_search_fixture(env)

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 't2', 'TEXT') \
        .error().contains('FT.ALTER is not supported in Redis Flex')
    env.expect('FT._ALTERIFNX', 'idx', 'SCHEMA', 'ADD', 't2', 'TEXT') \
        .error().contains('FT._ALTERIFNX is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_cursor_commands(env):
    _create_flex_search_fixture(env)
    env.expect('FT.CURSOR', 'READ', 'idx', '1') \
        .error().contains('FT.CURSOR is not supported in Redis Flex')
    env.expect('FT.CURSOR', 'DEL', 'idx', '1') \
        .error().contains('FT.CURSOR is not supported in Redis Flex')
    env.expect('FT.CURSOR', 'GC', 'idx') \
        .error().contains('FT.CURSOR is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_debug_wrappers_for_aggregate_and_hybrid(env):
    _create_flex_search_fixture(env)

    env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'TIMEOUT_AFTER_N', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.AGGREGATE is not supported in Redis Flex')
    env.expect(debug_cmd(), 'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'TIMEOUT_AFTER_N', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.AGGREGATE is not supported in Redis Flex')

    env.expect(debug_cmd(), 'FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
               'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.HYBRID is not supported in Redis Flex')
    env.expect(debug_cmd(), 'FT.PROFILE', 'idx', 'HYBRID', 'QUERY', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
               'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('FT.HYBRID is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_suggest_commands(env):
    _create_flex_search_fixture(env)

    env.expect('FT.SUGADD', 'idx', 'foo', '1') \
        .error().contains('FT.SUGADD is not supported in Redis Flex')
    env.expect('FT.SUGGET', 'idx', 'fo') \
        .error().contains('FT.SUGGET is not supported in Redis Flex')
    env.expect('FT.SUGDEL', 'idx', 'foo') \
        .error().contains('FT.SUGDEL is not supported in Redis Flex')
    env.expect('FT.SUGLEN', 'idx') \
        .error().contains('FT.SUGLEN is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_slop_argument(env):
    """Test that SLOP argument is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello world', 'NOCONTENT', 'SLOP', '1') \
        .error().contains('SLOP is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_drop_and_dropindex_dd(env):
    """Test that FT.DROP and FT.DROPINDEX with DD are not supported in Flex mode"""
    _create_flex_search_fixture(env)

    # FT.DROP is not supported (deprecated command that deletes docs)
    env.expect('FT.DROP', 'idx') \
        .error().contains('FT.DROP is not supported in Redis Flex')

    # FT.DROPINDEX with DD (delete docs) is not supported
    env.expect('FT.DROPINDEX', 'idx', 'DD') \
        .error().contains('DD is not supported in Redis Flex')

    # FT.DROPINDEX without DD should work
    env.expect('FT.DROPINDEX', 'idx').ok()


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_inorder_argument(env):
    """Test that INORDER argument is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello world', 'NOCONTENT', 'INORDER') \
        .error().contains('INORDER is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_highlight_argument(env):
    """Test that HIGHLIGHT argument is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'HIGHLIGHT') \
        .error().contains('HIGHLIGHT is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_summarize_argument(env):
    """Test that SUMMARIZE argument is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'SUMMARIZE') \
        .error().contains('SUMMARIZE is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_tfidf_scorer(env):
    """Test that TFIDF scorer is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'SCORER', 'TFIDF') \
        .error().contains('TFIDF scorer is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_tfidf_docnorm_scorer(env):
    """Test that TFIDF.DOCNORM scorer is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'SCORER', 'TFIDF.DOCNORM') \
        .error().contains('TFIDF.DOCNORM scorer is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_bm25_scorer(env):
    """Test that BM25 (deprecated) scorer is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'SCORER', 'BM25') \
        .error().contains('BM25 scorer is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_sortby_argument(env):
    """Test that SORTBY argument is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'SORTBY', 't') \
        .error().contains('SORTBY is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_temporary_indexes(env):
    """Test that TEMPORARY indexes are not supported in Flex mode"""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'TEMPORARY', '120',
               'SCHEMA', 'field', 'TEXT') \
        .error().contains('Unsupported argument for Flex index: `TEMPORARY`')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_withsuffixtrie_text_field(env):
    """Test that WITHSUFFIXTRIE on TEXT fields is blocked in Redis Flex"""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN',
               'SCHEMA', 'field', 'TEXT', 'WITHSUFFIXTRIE') \
        .error().contains('WITHSUFFIXTRIE is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_withsuffixtrie_tag_field(env):
    """Test that WITHSUFFIXTRIE on TAG fields is blocked in Redis Flex"""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN',
               'SCHEMA', 'field', 'TAG', 'WITHSUFFIXTRIE') \
        .error().contains('WITHSUFFIXTRIE is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_deprecated_add_commands(env):
    """Test that FT.ADD and FT.SAFEADD are blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.ADD', 'idx', 'doc:2', '1.0', 'FIELDS', 't', 'test') \
        .error().contains('FT.ADD is not supported in Redis Flex')
    env.expect('FT.SAFEADD', 'idx', 'doc:2', '1.0', 'FIELDS', 't', 'test') \
        .error().contains('FT.SAFEADD is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_deprecated_del_command(env):
    """Test that FT.DEL is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.DEL', 'idx', 'doc:1') \
        .error().contains('FT.DEL is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_deprecated_get_commands(env):
    """Test that FT.GET and FT.MGET are blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.GET', 'idx', 'doc:1') \
        .error().contains('FT.GET is not supported in Redis Flex')
    env.expect('FT.MGET', 'idx', 'doc:1') \
        .error().contains('FT.MGET is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_tagvals_command(env):
    """Test that FT.TAGVALS is blocked in Redis Flex"""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN',
               'SCHEMA', 'tag_field', 'TAG').ok()
    env.expect('HSET', 'doc:1', 'tag_field', 'value1').equal(1)

    env.expect('FT.TAGVALS', 'idx', 'tag_field') \
        .error().contains('FT.TAGVALS is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_spellcheck_command(env):
    """Test that FT.SPELLCHECK is blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    env.expect('FT.SPELLCHECK', 'idx', 'helo') \
        .error().contains('FT.SPELLCHECK is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_synonym_commands(env):
    """Test that FT.SYNUPDATE, FT.SYNDUMP, and FT.SYNADD are blocked in Redis Flex"""
    _create_flex_search_fixture(env)

    # FT.SYNUPDATE is blocked
    env.expect('FT.SYNUPDATE', 'idx', 'group1', 'hello', 'hi', 'hey') \
        .error().contains('FT.SYNUPDATE is not supported in Redis Flex')

    # FT.SYNDUMP is blocked
    env.expect('FT.SYNDUMP', 'idx') \
        .error().contains('FT.SYNDUMP is not supported in Redis Flex')

    # FT.SYNADD is deprecated and blocked (returns different error but should be blocked)
    env.expect('FT.SYNADD', 'idx', 'hello', 'hi') \
        .error().contains('FT.SYNADD is not supported in Redis Flex')
