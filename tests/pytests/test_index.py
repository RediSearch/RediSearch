from common import *

def validate_spec_invidx_info(env, expected_reply, msg, depth=0):
    debug_output = env.cmd(debug_cmd(), "SPEC_INVIDXES_INFO", "idx")
    dict_debug_output = to_dict(debug_output)
    env.assertEqual(to_dict(debug_output), expected_reply, message=msg, depth=depth+1)

    return dict_debug_output

def test_lazy_index_creation(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # We will update expected_reply in every call that is prone to change the inverted index size,
    # so we will raise an assertion only for the command that unexpectedly changed the size
    expected_reply = {
        "inverted_indexes_dict_size": 0,
        "inverted_indexes_memory": 0,
    }

    # create index with all fields types
    env.expect(
        "FT.CREATE","idx","SCHEMA",
        "t", "TEXT",
        "n",  "NUMERIC",
        "g", "GEO",
        "tags", "TAG",
        "geom", "GEOSHAPE",
        "vec", "VECTOR",
            "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
    ).ok()

    # Sanity check - empty spec
    expected_reply = validate_spec_invidx_info(env, expected_reply, "after creation")

    # call ft.info
    # we expect no new inverted index to be created
    env.cmd("FT.INFO", "idx")
    expected_reply = validate_spec_invidx_info(env, expected_reply, "calling ft.info")

    # query each field
    env.expect("FT.SEARCH", "idx", "@t:meow").equal([0])
    expected_reply = validate_spec_invidx_info(env, expected_reply,"query text")

    env.expect("FT.SEARCH", "idx", '@n:[1 1]').equal([0])
    expected_reply = validate_spec_invidx_info(env, expected_reply, "query numeric")

    env.expect("FT.SEARCH", "idx", '@g:[1.23 4.56 2 km]').equal([0])
    expected_reply = validate_spec_invidx_info(env, expected_reply, "query geo")

    env.expect("FT.SEARCH", "idx", '@tags:{foo bar}').equal([0])
    expected_reply = validate_spec_invidx_info(env, expected_reply, "query tag")

    vec = np.random.rand(2).astype(np.float32).tobytes()
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 10 @vec $vec_param]',
            'PARAMS', 2, 'vec_param', vec).equal([0])
    expected_reply = validate_spec_invidx_info(env, expected_reply, "query vector")

    # Currently, geoshape is created during query.
    # TODO: inverted_indexes_dict_size should be 0 once this is fixed.
    query = 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))'
    env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([0])
    expected_reply["inverted_indexes_dict_size"] += 1
    expected_reply = validate_spec_invidx_info(env, expected_reply, "query geometry (created in query)")

def test_lazy_index_creation_debug_commands(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    expected_reply = {
        "inverted_indexes_dict_size": 0,
        "inverted_indexes_memory": 0,
    }

    # create index with all fields types
    env.expect(
        "FT.CREATE","idx","SCHEMA",
        "t", "TEXT",
        "n",  "NUMERIC",
        "g", "GEO",
        "tags", "TAG",
        "geom", "GEOSHAPE",
        "vec", "VECTOR",
            "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
        "vec2", "VECTOR",
            "HNSW", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
    ).ok()

    # debug command

    ## NUMERIC
    env.cmd(debug_cmd(), "NUMIDX_SUMMARY", "idx", 'n')
    expected_reply = validate_spec_invidx_info(env, expected_reply, "NUMIDX_SUMMARY")

    env.cmd(debug_cmd(), "DUMP_NUMIDX", "idx", 'n', 'WITH_HEADERS')
    expected_reply = validate_spec_invidx_info(env, expected_reply, "DUMP_NUMIDX")

    env.cmd(debug_cmd(), "DUMP_NUMIDXTREE", "idx", 'n')
    expected_reply = validate_spec_invidx_info(env, expected_reply, "DUMP_NUMIDXTREE")

    env.cmd(debug_cmd(), "GC_CLEAN_NUMERIC", "idx", 'n')
    expected_reply = validate_spec_invidx_info(env, expected_reply, "GC_CLEAN_NUMERIC")

    ## TAG
    env.cmd(debug_cmd(), "DUMP_TAGIDX", "idx", 'tags')
    expected_reply = validate_spec_invidx_info(env, expected_reply, "DUMP_TAGIDX")

    env.cmd(debug_cmd(), "INFO_TAGIDX", "idx", 'tags')
    expected_reply = validate_spec_invidx_info(env, expected_reply, "INFO_TAGIDX")

    env.cmd(debug_cmd(), "DUMP_SUFFIX_TRIE", "idx", 'tags')
    expected_reply = validate_spec_invidx_info(env, expected_reply, "DUMP_SUFFIX_TRIE")

    ## GEOMETRY - created in debug command
    env.cmd(debug_cmd(), "DUMP_GEOMIDX", "idx", 'geom')
    expected_reply["inverted_indexes_dict_size"] += 1
    expected_reply = validate_spec_invidx_info(env, expected_reply, "DUMP_GEOMIDX")

    ## VECTOR - created in debug command
    env.cmd(debug_cmd(), "VECSIM_INFO", "idx", 'vec')
    expected_reply["inverted_indexes_dict_size"] += 1
    expected_reply = validate_spec_invidx_info(env, expected_reply, "VECSIM_INFO")

    env.cmd(debug_cmd(), "DUMP_HNSW", "idx", 'vec2')
    expected_reply["inverted_indexes_dict_size"] += 1
    expected_reply = validate_spec_invidx_info(env, expected_reply, "DUMP_HNSW")

def test_lazy_index_creation_info_modules(env):
    # create index with all fields types
    env.expect(
        "FT.CREATE","idx","SCHEMA",
        "t", "TEXT",
        "n",  "NUMERIC",
        "g", "GEO",
        "tags", "TAG",
        "geom", "GEOSHAPE",
        "vec", "VECTOR",
            "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
    ).ok()

    expected_reply = {
        "inverted_indexes_dict_size": 0,
        "inverted_indexes_memory": 0,
    }
    env.cmd('INFO', 'MODULES')
    validate_spec_invidx_info(env, expected_reply, "after INFO MODULES")

@skip(cluster=True)
def test_restore_schema(env: Env):

    # Test that the command is not exposed to normal users
    env.expect('_FT._RESTOREIFNX', 'SCHEMA').error().contains('unknown subcommand')
    # Mark the client as internal for the rest of the test
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

    # create index with all fields types
    env.expect(
        "FT.CREATE", "idx", "SCHEMA",
        "t", "TEXT", 'WEIGHT', '2.1',
        "n", "NUMERIC", 'NOINDEX',
        "g", "GEO",
        "tags", "TAG", 'CASESENSITIVE', 'SEPARATOR', ';',
        "geom", "GEOSHAPE",
        "vec", "VECTOR",
            "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
    ).ok()

    # add some synonyms
    env.expect('FT.SYNUPDATE', 'idx', 'meow', 'cat').ok()
    env.expect('FT.SYNUPDATE', 'idx', 'bark', 'dog').ok()

    # Test restore failures
    # env.expect('_FT._RESTOREIFNX').error().contains('wrong number of arguments') # TODO: Uncomment when redis issue is fixed
    env.expect('_FT._RESTOREIFNX', 'SCHEMA').error().contains('wrong number of arguments')
    env.expect('_FT._RESTOREIFNX', 'SCHEMA', 'Too', 'many', 'arguments').error().contains('wrong number of arguments')
    env.expect('_FT._RESTOREIFNX', 'SCHEMA', 'Ten', 'blob').error().contains('Invalid encoding version')
    env.expect('_FT._RESTOREIFNX', 'SCHEMA', '42', 'blob').error().contains('Failed to deserialize schema')

    # dump the index
    dump, encode = env.cmd(debug_cmd(), 'DUMP_SCHEMA', 'idx', NEVER_DECODE=True)

    # Test that we manage to call restore while the index exists
    env.expect('_FT._RESTOREIFNX', 'SCHEMA', encode, dump).ok()
    env.assertEqual(env.cmd('FT._LIST'), ['idx'], message="Expected only one index after restoring existing index")

    # drop the index
    env.expect('FT.DROPINDEX', 'idx').ok()
    env.assertEqual(env.cmd('FT._LIST'), [], message="Expected no indexes after dropping the index")

    # restore the index
    env.expect('_FT._RESTOREIFNX', 'SCHEMA', encode, dump).ok()
    env.assertEqual(env.cmd('FT._LIST'), ['idx'], message="Expected one index after restoring the index")

    # Test that the restored index works as expected, and that the schema is as expected
    expected = [
        [
            'identifier', 't',
            'attribute', 't',
            'type', 'TEXT',
            'WEIGHT', '2.1'
        ], [
            'identifier', 'n',
            'attribute', 'n',
            'type', 'NUMERIC',
            'NOINDEX',
        ], [
            'identifier', 'g',
            'attribute', 'g',
            'type', 'GEO',
        ], [
            'identifier', 'tags',
            'attribute', 'tags',
            'type', 'TAG',
            'SEPARATOR', ';',
            'CASESENSITIVE',
        ], [
            'identifier', 'geom',
            'attribute', 'geom',
            'type', 'GEOSHAPE',
            'coord_system', 'SPHERICAL',
        ], [
            'identifier', 'vec',
            'attribute', 'vec',
            'type', 'VECTOR',
            'algorithm', 'FLAT',
            'data_type', 'FLOAT32',
            'dim', 2,
            'distance_metric', 'L2',
        ]
    ]
    env.assertEqual(index_info(env)['attributes'], expected)

    # Test that synonyms were also restored correctly
    env.expect('FT.SYNDUMP', 'idx').equal(['cat', ['meow'], 'dog', ['bark']])
