from common import *

def check_dict_size(env, msg, inverted_indexes_dict_size = 0, inverted_indexes_memory = 0):
    expected_reply = {
        "inverted_indexes_dict_size": inverted_indexes_dict_size,
        "inverted_indexes_memory": inverted_indexes_memory,
    }
    debug_output = env.cmd(debug_cmd(), "SPEC_INVIDXES_INFO", "idx")
    env.assertEqual(to_dict(debug_output), expected_reply, message=msg)

def test_lazy_index_creation(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

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
    check_dict_size(env, "after creation")

    # call ft.info
    env.cmd("FT.INFO", "idx")
    check_dict_size(env, "calling ft.info")

    # query each field
    env.expect("FT.SEARCH", "idx", "@t:meow").equal([0])
    check_dict_size(env, "query text")

    env.expect("FT.SEARCH", "idx", '@n:[1 1]').equal([0])
    check_dict_size(env, "query numeric")

    env.expect("FT.SEARCH", "idx", '@g:[1.23 4.56 2 km]').equal([0])
    check_dict_size(env, "query geo")

    env.expect("FT.SEARCH", "idx", '@tags:{foo bar}').equal([0])
    check_dict_size(env, "query tag")

    vec = np.random.rand(2).astype(np.float32).tobytes()
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 10 @v $vec_param]',
            'PARAMS', 2, 'vec_param', vec).equal([0])
    check_dict_size(env, "query vector")

    # Currently, geoshape is created during query. change this once it is fixed.
    query = 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))'
    env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([0])
    check_dict_size(env, "query geometry (created in query)", inverted_indexes_dict_size=1)

def test_lazy_index_creation_debug_commands(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

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
    check_dict_size(env, "NUMIDX_SUMMARY")

    env.cmd(debug_cmd(), "DUMP_NUMIDX", "idx", 'n', 'WITH_HEADERS')
    check_dict_size(env, "DUMP_NUMIDX")

    env.cmd(debug_cmd(), "DUMP_NUMIDXTREE", "idx", 'n')
    check_dict_size(env, "DUMP_NUMIDXTREE")

    env.cmd(debug_cmd(), "GC_CLEAN_NUMERIC", "idx", 'n')
    check_dict_size(env, "GC_CLEAN_NUMERIC")

    ## TAG
    env.cmd(debug_cmd(), "DUMP_TAGIDX", "idx", 'tags')
    check_dict_size(env, "DUMP_TAGIDX")

    env.cmd(debug_cmd(), "INFO_TAGIDX", "idx", 'tags')
    check_dict_size(env, "INFO_TAGIDX")

    env.cmd(debug_cmd(), "DUMP_SUFFIX_TRIE", "idx", 'tags')
    check_dict_size(env, "DUMP_SUFFIX_TRIE")

    ## GEOMETRY - created in debug command
    env.cmd(debug_cmd(), "DUMP_GEOMIDX", "idx", 'geom')
    check_dict_size(env, "DUMP_GEOMIDX", inverted_indexes_dict_size=1)

    ## VECTOR - created in debug command
    env.cmd(debug_cmd(), "VECSIM_INFO", "idx", 'vec')
    check_dict_size(env, "VECSIM_INFO", inverted_indexes_dict_size=2)

    env.cmd(debug_cmd(), "DUMP_HNSW", "idx", 'vec2')
    check_dict_size(env, "DUMP_HNSW", inverted_indexes_dict_size=3)
