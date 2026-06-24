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
               'SCHEMA', 'title', 'TEXT', 'body', 'TEXT', 'INDEXEMPTY').ok()

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



@skip(cluster=True)
@with_simulate_in_flex(True)
def test_missing_skip_initial_scan(env):
    """Test that SKIPINITIALSCAN is required when search-_simulate-in-flex is true"""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'field', 'TEXT') \
        .error().contains('Flex index requires SKIPINITIALSCAN argument')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_on_json_is_supported(env):
    """Test that ON JSON is accepted when search-_simulate-in-flex is true"""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SKIPINITIALSCAN', 'SCHEMA',
               '$.field', 'AS', 'field', 'TEXT').ok()


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_disk_json_rejects_multi_value_jsonpath(env):
    """Test that disk validation rejects non-single JSONPath fields"""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SKIPINITIALSCAN', 'SCHEMA',
               '$.field[*]', 'AS', 'field', 'TEXT') \
        .error().contains('Disk JSON index supports only single-value JSONPath fields')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_disk_json_ingestion_rejects_array_payload_for_single_path_field(env):
    """Valid disk JSON schema should be created, but array payload ingestion should fail."""

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SKIPINITIALSCAN', 'PREFIX', '1', 'doc:',
               'SCHEMA', '$.name', 'AS', 'name', 'TEXT').ok()

    errs = index_errors(env, 'idx')
    env.assertEqual(int(errs['indexing failures']), 0)

    env.expect('JSON.SET', 'doc:1', '$', '{"name":["a","b"]}').ok()

    errs = index_errors(env, 'idx')
    env.assertEqual(int(errs['indexing failures']), 1)
    env.assertContains('Disk JSON index supports JSON array values only for VECTOR fields',
                       errs['last indexing error'])

    env.expect('FT.SEARCH', 'idx', '@name:a', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([0])

    # Valid scalar value should be indexed after the failed attempt.
    env.expect('JSON.SET', 'doc:1', '$.name', '"alice"').ok()
    env.expect('FT.SEARCH', 'idx', '@name:alice', 'NOCONTENT').equal([1, 'doc:1'])


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


def _create_flex_search(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('HSET', 'doc:1', 't', 'hello world').equal(1)


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_requires_nocontent_or_return_0(env):
    """In Flex mode, FT.SEARCH must use NOCONTENT (explicit) or RETURN 0."""
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello') \
        .error().contains('NOCONTENT or RETURN 0 must be provided in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_allows_nocontent(env):
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc:1'])


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_allows_return_0(env):
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'RETURN', '0').equal([1, 'doc:1'])


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_allows_nocontent_withscores(env):
    _create_flex_search(env)

    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'WITHSCORES')
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], 'doc:1')
    env.assertGreater(float(res[2]), 0.0)


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_search_rejects_load_with_nocontent_or_return_0(env):
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'LOAD', '1', '@t') \
        .error().contains('LOAD is not supported in Redis Flex')

    env.expect('FT.SEARCH', 'idx', 'hello', 'RETURN', '0', 'LOAD', '1', '@t') \
        .error().contains('LOAD is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_aggregate_and_hybrid_commands(env):
    _create_flex_search(env)

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
    _create_flex_search(env)

    env.expect('FT.DICTADD', 'dict', 'foo') \
        .error().contains('FT.DICTADD is not supported in Redis Flex')
    env.expect('FT.DICTDEL', 'dict', 'foo') \
        .error().contains('FT.DICTDEL is not supported in Redis Flex')
    env.expect('FT.DICTDUMP', 'dict') \
        .error().contains('FT.DICTDUMP is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_disk_hnsw_rerank_value(env):
    env.expect(
        'FT.CREATE', 'idx_ok', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '14',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK', 'TRUE',
    ).ok()

    env.expect(
        'FT.CREATE', 'idx_ok_false', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '14',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK', 'FALSE',
    ).ok()

    env.expect(
        'FT.CREATE', 'idx_missing', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '12',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
    ).error().contains('Disk HNSW index requires RERANK parameter')

    env.expect(
        'FT.CREATE', 'idx_no_value', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '13',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK',
    ).error().contains('RERANK requires an argument')

    env.expect(
        'FT.CREATE', 'idx_bad_value', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '14',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK', 'MAYBE',
    ).error().contains('Syntax error: RERANK value must be TRUE or FALSE')

    env.expect(
        'FT.CREATE', 'idx_dup', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '16',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK', 'TRUE',
        'RERANK', 'TRUE',
    ).error().contains('Duplicate RERANK parameter')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_disk_hnsw_float16(env):
    # MOD-15148: disk HNSW vector indexes accept FLOAT16 in addition to FLOAT32,
    # while every other element type is still rejected. Creation only.
    env.expect(
        'FT.CREATE', 'idx_fp16', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '14',
        'TYPE', 'FLOAT16',
        'DIM', '4',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK', 'TRUE',
    ).ok()

    # FLOAT32 remains accepted (no regression).
    env.expect(
        'FT.CREATE', 'idx_fp32', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '14',
        'TYPE', 'FLOAT32',
        'DIM', '4',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK', 'TRUE',
    ).ok()

    # Unsupported element types are still rejected on disk.
    for vec_type in ('FLOAT64', 'BFLOAT16'):
        env.expect(
            'FT.CREATE', f'idx_{vec_type.lower()}', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'v', 'VECTOR', 'HNSW', '14',
            'TYPE', vec_type,
            'DIM', '4',
            'DISTANCE_METRIC', 'L2',
            'M', '16',
            'EF_CONSTRUCTION', '200',
            'EF_RUNTIME', '10',
            'RERANK', 'TRUE',
        ).error().contains('Disk index does not support')


@skip(cluster=True)
@with_simulate_in_flex(False)
def test_ram_hnsw_rerank_rejected(env):
    # In RAM mode RERANK is a disk-only option and must be rejected outright,
    # including the otherwise-valid RERANK TRUE form (MOD-16015). The disk guard
    # is the first check in the RERANK branch, so it short-circuits before the
    # duplicate / missing-arg / value checks and never reads what follows the
    # keyword. We therefore reject every RERANK form identically:
    #   - RERANK TRUE: the form the bug silently accepted.
    #   - RERANK FALSE: a valid value that is still disk-only in RAM mode.
    #   - RERANK with no value: the guard fires before the missing-arg check.
    #   - duplicate RERANK: pins the guard's precedence over the other RERANK
    #     checks, so reordering them would surface as a regression here.
    err = 'RERANK is only supported for disk-based vector indexes'

    env.expect(
        'FT.CREATE', 'idx_true', 'ON', 'HASH', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '8',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'RERANK', 'TRUE',
    ).error().contains(err)

    env.expect(
        'FT.CREATE', 'idx_false', 'ON', 'HASH', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '8',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'RERANK', 'FALSE',
    ).error().contains(err)

    env.expect(
        'FT.CREATE', 'idx_no_value', 'ON', 'HASH', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '7',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'RERANK',
    ).error().contains(err)

    env.expect(
        'FT.CREATE', 'idx_dup', 'ON', 'HASH', 'SCHEMA',
        'v', 'VECTOR', 'HNSW', '10',
        'TYPE', 'FLOAT32',
        'DIM', '2',
        'DISTANCE_METRIC', 'L2',
        'RERANK', 'TRUE',
        'RERANK', 'TRUE',
    ).error().contains(err)


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_disk_hnsw_rerank_rdb_roundtrip(env):
    # _SIMULATE_IN_FLEX validates syntax but does not persist the rerank value
    # (it is discarded in parseVectorField when sp->diskSpec is NULL), and the
    # RDB save/load gate is on SearchDisk_IsEnabled() so no byte is written
    # either. The value-preservation check therefore lives in C/Enterprise CI
    # where isFlex is true; here we only confirm the dump/reload path stays
    # error-free across both rerank settings.
    for idx, rerank_value in [('idx_rerank_true', 'TRUE'), ('idx_rerank_false', 'FALSE')]:
        env.expect(
            'FT.CREATE', idx, 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'v', 'VECTOR', 'HNSW', '14',
            'TYPE', 'FLOAT32',
            'DIM', '2',
            'DISTANCE_METRIC', 'L2',
            'M', '16',
            'EF_CONSTRUCTION', '100',
            'EF_RUNTIME', '10',
            'RERANK', rerank_value,
        ).ok()

    env.dumpAndReload()

    for idx in ('idx_rerank_true', 'idx_rerank_false'):
        env.expect('FT.INFO', idx).noError()


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_disk_vector_query_validation(env: Env):

    env.expect(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        't', 'TEXT',
        'v', 'VECTOR', 'HNSW', '14',
        'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2',
        'M', '16', 'EF_CONSTRUCTION', '100', 'EF_RUNTIME', '10', 'RERANK', 'TRUE',
    ).ok()

    docs = {
        'doc:1': ([1.0, 1.0], 'hello'),
        'doc:2': ([2.0, 2.0], 'hello'),
        'doc:3': ([50.0, 50.0], 'goodbye'),
    }

    with env.getClusterConnectionIfNeeded() as conn:
        for doc_id, (vector, text) in docs.items():
            conn.execute_command('HSET', doc_id, 'v', create_np_array_typed(vector, 'FLOAT32').tobytes(), 't', text)

    query_blob = create_np_array_typed([1.0, 1.0], 'FLOAT32').tobytes()

    env.expect('FT.SEARCH', 'idx', '@t:hello=>[KNN 2 @v $b]', 'NOCONTENT',
                'PARAMS', '2', 'b', query_blob).error().contains(
                    'Redis Flex pre-filtered vector queries currently require explicit HYBRID_POLICY')

    valid_queries = [
        '@t:hello=>[KNN 2 @v $b HYBRID_POLICY BATCHES]',
        '@t:hello=>[KNN 2 @v $b HYBRID_POLICY ADHOC_BF]',
        '@t:hello=>[KNN 2 @v $b]=>{$HYBRID_POLICY:BATCHES;}',
        '@t:hello=>[KNN 2 @v $b]=>{$HYBRID_POLICY:ADHOC_BF;}',
    ]

    for query in valid_queries:
        res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', 'PARAMS', '2', 'b', query_blob)
        env.assertEqual(res[0], 2, message=f'Expected 2 results for query "{query}"')
        env.assertEqual(set(res[1:]), {'doc:1', 'doc:2'}, message=f'Expected results doc:1 and doc:2 for query "{query}"')

    # Vector range queries are supported on Flex disk indexes. With L2 (squared)
    # distance and a query vector of [1.0, 1.0]: doc:1 -> 0, doc:2 -> 2,
    # doc:3 -> 4802. Radius 10 returns doc:1 and doc:2; radius 0 returns doc:1
    # only; a very large radius returns all docs.
    range_cases = [
        ('@v:[VECTOR_RANGE 10 $b]', {'doc:1', 'doc:2'}),
        ('@v:[VECTOR_RANGE 0 $b]', {'doc:1'}),
        ('@v:[VECTOR_RANGE 100000 $b]', {'doc:1', 'doc:2', 'doc:3'}),
        # Hybrid range with text prefilter exercises the BY_ID intersection path.
        ('@t:hello @v:[VECTOR_RANGE 10 $b]', {'doc:1', 'doc:2'}),
        ('@t:goodbye @v:[VECTOR_RANGE 100000 $b]', {'doc:3'}),
    ]

    for query, expected in range_cases:
        res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT',
                      'PARAMS', '2', 'b', query_blob)
        env.assertEqual(res[0], len(expected),
                        message=f'Expected {len(expected)} results for query "{query}"')
        env.assertEqual(set(res[1:]), expected,
                        message=f'Unexpected results for query "{query}"')

    # Negative radius is still rejected by the vector index validation path.
    env.expect('FT.SEARCH', 'idx', '@v:[VECTOR_RANGE -1 $b]', 'NOCONTENT',
               'PARAMS', '2', 'b', query_blob).error()


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_ft_info_reports_vector_index_memory(env):
    """Regression test for MOD-14840.

    HNSW vector indexes are kept in memory even in Flex/ROF mode, so
    FT.INFO must report a non-zero `vector_index_sz_mb` and the vector
    memory must be included in `total_index_memory_sz_mb`.
    """
    dim = 4
    env.expect(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        't', 'TEXT',
        'tag', 'TAG',
        'v', 'VECTOR', 'HNSW', '14',
        'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2',
        'M', '16', 'EF_CONSTRUCTION', '100', 'EF_RUNTIME', '10', 'RERANK', 'TRUE',
    ).ok()

    n_docs = 100
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(n_docs):
            vector = create_np_array_typed([float(i)] * dim, 'FLOAT32').tobytes()
            conn.execute_command('HSET', f'doc:{i}', 't', f'hello{i}',
                                 'tag', f'tag{i}', 'v', vector)

    info = index_info(env, 'idx')
    vector_size_mb = float(info['vector_index_sz_mb'])
    total_size_mb = float(info['total_index_memory_sz_mb'])

    env.assertGreater(vector_size_mb, 0,
                      message=f'Expected vector_index_sz_mb > 0 for HNSW index, got {vector_size_mb}')
    env.assertGreater(total_size_mb, 0,
                      message=f'Expected total_index_memory_sz_mb > 0, got {total_size_mb}')
    env.assertGreaterEqual(total_size_mb, vector_size_mb,
                           message=f'total_index_memory_sz_mb ({total_size_mb}) must include '
                                   f'vector_index_sz_mb ({vector_size_mb})')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_alter_command(env):
    _create_flex_search(env)

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 't2', 'TEXT') \
        .error().contains('FT.ALTER is not supported in Redis Flex')
    env.expect('FT._ALTERIFNX', 'idx', 'SCHEMA', 'ADD', 't2', 'TEXT') \
        .error().contains('FT._ALTERIFNX is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_cursor_commands(env):
    _create_flex_search(env)
    env.expect('FT.CURSOR', 'READ', 'idx', '1') \
        .error().contains('FT.CURSOR is not supported in Redis Flex')
    env.expect('FT.CURSOR', 'DEL', 'idx', '1') \
        .error().contains('FT.CURSOR is not supported in Redis Flex')
    env.expect('FT.CURSOR', 'GC', 'idx') \
        .error().contains('FT.CURSOR is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_debug_wrappers_for_aggregate_and_hybrid(env):
    _create_flex_search(env)

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
    _create_flex_search(env)

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
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello world', 'NOCONTENT', 'SLOP', '1') \
        .error().contains('SLOP is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_drop_and_dropindex_dd(env):
    """Test that FT.DROP and FT.DROPINDEX with DD are not supported in Flex mode"""
    _create_flex_search(env)

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
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello world', 'NOCONTENT', 'INORDER') \
        .error().contains('INORDER is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_highlight_argument(env):
    """Test that HIGHLIGHT argument is blocked in Redis Flex"""
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'HIGHLIGHT') \
        .error().contains('HIGHLIGHT is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_summarize_argument(env):
    """Test that SUMMARIZE argument is blocked in Redis Flex"""
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'SUMMARIZE') \
        .error().contains('SUMMARIZE is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_dialect_4(env):
    """Test that DIALECT 4 is blocked in Redis Flex while dialects 1-3 work.

    DIALECT 4 enables the query optimizer (QEXEC_OPTIMIZE) by default, which
    relies on RAM-only structures (DocTable / NumericRangeTree) that disk specs
    do not populate, so it is unsupported on disk (MOD-15997).
    """
    _create_flex_search(env)

    # Dialects 1-3 still return the document on a disk index.
    for dialect in (1, 2, 3):
        env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'DIALECT', str(dialect)) \
            .equal([1, 'doc:1'])

    # DIALECT 4 is rejected.
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'DIALECT', '4') \
        .error().contains('DIALECT 4 is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_withoutcount_argument(env):
    """Test that WITHOUTCOUNT is blocked in Redis Flex.

    WITHOUTCOUNT explicitly enables the query optimizer (QEXEC_OPTIMIZE), which
    is unsupported on disk for the same reason as DIALECT 4 (MOD-15997).
    """
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'WITHOUTCOUNT') \
        .error().contains('WITHOUTCOUNT is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_configured_default_dialect_4(env):
    """Test that a configured default of DIALECT 4 is also blocked in Redis Flex.

    When the query omits an explicit DIALECT it inherits search-default-dialect,
    so a default of 4 must be rejected too (MOD-15997).
    """
    _create_flex_search(env)

    env.expect('CONFIG', 'SET', 'search-default-dialect', '4').ok()
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT') \
        .error().contains('DIALECT 4 is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_sortby_on_non_vector_fields(env):
    """Test that SORTBY on non-vector-score fields is blocked in Redis Flex"""
    _create_flex_search(env)

    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT', 'SORTBY', 't') \
        .error().contains('SORTBY in Redis Flex is restricted to sorting results by vector distance')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_allows_sortby_on_vector_distance_fields(env):
    """Test that SORTBY on vector distance fields (from KNN queries) is allowed in Redis Flex"""
    # Create index with both text and vector fields
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'HNSW', '14',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2',
               'M', '16', 'EF_CONSTRUCTION', '100', 'EF_RUNTIME', '10', 'RERANK', 'TRUE',
    ).ok()

    # Add test documents
    docs = {
        'doc:1': ([0.1, 0.1], 'hello world'),
        'doc:2': ([0.2, 0.2], 'hello again'),
        'doc:3': ([0.5, 0.5], 'hello there'),
    }

    with env.getClusterConnectionIfNeeded() as conn:
        for doc_id, (vector, text) in docs.items():
            conn.execute_command('HSET', doc_id, 'v', create_np_array_typed(vector, 'FLOAT32').tobytes(), 't', text)

    query_blob = create_np_array_typed([0.0, 0.0], 'FLOAT32').tobytes()

    # SORTBY on default vector distance field (__v_score) should be allowed
    # Note: Pure KNN queries (with *) don't require HYBRID_POLICY
    res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 3 @v $b]', 'NOCONTENT',
                  'SORTBY', '__v_score', 'ASC',
                  'PARAMS', '2', 'b', query_blob,
                  'DIALECT', '2')
    env.assertEqual(res[0], 3)

    # SORTBY on custom vector distance field (using AS) should be allowed
    res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 3 @v $b AS my_dist]', 'NOCONTENT',
                  'SORTBY', 'my_dist', 'ASC',
                  'PARAMS', '2', 'b', query_blob,
                  'DIALECT', '2')
    env.assertEqual(res[0], 3)

    # SORTBY on non-vector field should still be blocked
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 3 @v $b]', 'NOCONTENT',
               'SORTBY', 't', 'ASC',
               'PARAMS', '2', 'b', query_blob,
               'DIALECT', '2') \
        .error().contains('SORTBY in Redis Flex is restricted to sorting results by vector distance')


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
    _create_flex_search(env)

    env.expect('FT.ADD', 'idx', 'doc:2', '1.0', 'FIELDS', 't', 'test') \
        .error().contains('FT.ADD is not supported in Redis Flex')
    env.expect('FT.SAFEADD', 'idx', 'doc:2', '1.0', 'FIELDS', 't', 'test') \
        .error().contains('FT.SAFEADD is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_deprecated_del_command(env):
    """Test that FT.DEL is blocked in Redis Flex"""
    _create_flex_search(env)

    env.expect('FT.DEL', 'idx', 'doc:1') \
        .error().contains('FT.DEL is not supported in Redis Flex')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_deprecated_get_commands(env):
    """Test that FT.GET and FT.MGET are blocked in Redis Flex"""
    _create_flex_search(env)

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
    _create_flex_search(env)

    env.expect('FT.SPELLCHECK', 'idx', 'helo') \
        .error().contains('FT.SPELLCHECK is not supported in Redis Flex')


def _create_flex_tag(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', 'tag', 'TAG').ok()
    env.expect('HSET', 'doc:1', 'tag', 'hello').equal(1)


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_tag_prefix_query(env):
    """Test that prefix queries on TAG fields are blocked in Flex mode"""
    _create_flex_tag(env)

    env.expect('FT.SEARCH', 'idx', '@tag:{hel*}', 'NOCONTENT') \
        .error().contains('TAG prefix/suffix/infix queries are not supported on Flex indexes')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_tag_wildcard_query(env):
    """Test that wildcard pattern queries on TAG fields are blocked in Flex mode"""
    _create_flex_tag(env)

    env.expect('FT.SEARCH', 'idx', "@tag:{w'hel*o'}", 'NOCONTENT', 'DIALECT', '2') \
        .error().contains('TAG wildcard queries are not supported on Flex indexes')


@skip(cluster=True)
@with_simulate_in_flex(True)
def test_flex_blocks_synonym_commands(env):
    """Test that FT.SYNUPDATE, FT.SYNDUMP, and FT.SYNADD are blocked in Redis Flex"""
    _create_flex_search(env)

    # FT.SYNUPDATE is blocked
    env.expect('FT.SYNUPDATE', 'idx', 'group1', 'hello', 'hi', 'hey') \
        .error().contains('FT.SYNUPDATE is not supported in Redis Flex')

    # FT.SYNDUMP is blocked
    env.expect('FT.SYNDUMP', 'idx') \
        .error().contains('FT.SYNDUMP is not supported in Redis Flex')

    # FT.SYNADD is deprecated and blocked (returns different error but should be blocked)
    env.expect('FT.SYNADD', 'idx', 'hello', 'hi') \
        .error().contains('FT.SYNADD is not supported in Redis Flex')
