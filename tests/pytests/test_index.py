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
