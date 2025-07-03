# -*- coding: utf-8 -*-
import random
import time

from RLTest import Env
from common import *
from includes import *
from random import randrange
from redis import ResponseError

'''************* Helper methods for vecsim tests ************'''
EPSILONS = {'FLOAT32': 1E-6, 'FLOAT64': 1E-9, 'FLOAT16': 1E-2, 'BFLOAT16': 1E-2}

# Helper method for comparing expected vs. results of KNN query, where the only
# returned field except for the doc id is the vector distance
def assert_query_results(env: Env, expected_res, actual_res, error_msg=None, data_type='FLOAT32'):
    # Assert that number of returned results from the query is as expected
    env.assertEqual(expected_res[0], actual_res[0], depth=1, message=error_msg)
    for i in range(1, len(expected_res), 2):
        # For each result, assert its id and its distance (use float equality)
        env.assertEqual(expected_res[i], actual_res[i], depth=1, message=error_msg)
        env.assertAlmostEqual(expected_res[i+1][1], float(actual_res[i+1][1]), EPSILONS[data_type], depth=1, message=error_msg)


def get_vecsim_memory(env, index_key, field_name):
    return float(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_key, field_name))["MEMORY"])/0x100000


def get_vecsim_index_size(env, index_key, field_name):
    return int(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_key, field_name))["INDEX_SIZE"])


def load_vectors_with_texts_into_redis(con, vector_field, dim, num_vectors, data_type='FLOAT32'):
    id_vec_list = []
    p = con.pipeline(transaction=False)
    for i in range(1, num_vectors+1):
        vector = create_np_array_typed([i]*dim, data_type)
        p.execute_command('HSET', i, vector_field, vector.tobytes(), 't', 'text value')
        id_vec_list.append((i, vector))
    p.execute()
    return id_vec_list


def execute_hybrid_query(env, query_string, query_data, non_vector_field, sort_by_vector=True, sort_by_non_vector_field=False,
                         hybrid_mode='HYBRID_BATCHES', scorer='BM25STD'):
    if sort_by_vector:
        ret = env.expect('FT.SEARCH', 'idx', query_string,
                         'SORTBY', '__v_score',
                         'PARAMS', 2, 'vec_param', query_data.tobytes(),
                         'RETURN', 2, '__v_score', non_vector_field, 'LIMIT', 0, 10)

    else:

        if sort_by_non_vector_field:
            ret = env.expect('FT.SEARCH', 'idx', query_string, 'WITHSCORES', 'SCORER', scorer,
                             'SORTBY', non_vector_field,
                             'PARAMS', 2, 'vec_param', query_data.tobytes(),
                             'RETURN', 2, non_vector_field, '__v_score', 'LIMIT', 0, 10)

        else:
            ret = env.expect('FT.SEARCH', 'idx', query_string, 'WITHSCORES', 'SCORER', scorer,
                             'PARAMS', 2, 'vec_param', query_data.tobytes(),
                             'RETURN', 2, non_vector_field, '__v_score', 'LIMIT', 0, 10)

    env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], hybrid_mode, depth=1)
    return ret


'''******************* vecsim tests *****************************'''


def test_sanity_cosine():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    score_field_syntaxs = ['AS dist]', ']=>{$yield_distance_as:dist}']
    for index_type in VECSIM_ALGOS:
        for data_type in VECSIM_DATA_TYPES:
            if index_type == "SVS-VAMANA" and data_type not in ("FLOAT16", "FLOAT32"):
                continue
            compressions_types = [None]
            if index_type == "SVS-VAMANA":
                compressions_types += ["LVQ8"]
            for compression in compressions_types:
                for i, score_field_syntax in enumerate(score_field_syntaxs):
                    params = ['TYPE', data_type,'DIM', '2', 'DISTANCE_METRIC', 'COSINE']
                    if compression:
                        params += ["COMPRESSION", compression]
                    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', index_type, str(len(params)), *params).ok()
                    conn.execute_command('HSET', 'a', 'v', create_np_array_typed([0.1, 0.1], data_type).tobytes())
                    conn.execute_command('HSET', 'b', 'v', create_np_array_typed([0.1, 0.2], data_type).tobytes())
                    conn.execute_command('HSET', 'c', 'v', create_np_array_typed([0.1, 0.3], data_type).tobytes())
                    conn.execute_command('HSET', 'd', 'v', create_np_array_typed([0.1, 0.4], data_type).tobytes())

                    query_vec = create_np_array_typed([0.1, 0.1], data_type)

                    # Compute the expected distances from the query vector using scipy.spatial
                    expected_res = [4, 'a', ['dist', spatial.distance.cosine(np.array([0.1, 0.1]), query_vec)],
                              'b', ['dist', spatial.distance.cosine(np.array([0.1, 0.2]), query_vec)],
                              'c', ['dist', spatial.distance.cosine(np.array([0.1, 0.3]), query_vec)],
                              'd', ['dist', spatial.distance.cosine(np.array([0.1, 0.4]), query_vec)]]

                    actual_res = env.expect('FT.SEARCH', 'idx', f'*=>[KNN 4 @v $blob {score_field_syntax}', 'PARAMS', '2',
                                            'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                    assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)
                    if i==1:  # range query can use only query attributes as score field syntax
                        range_dist = spatial.distance.cosine(np.array([0.1, 0.4]), query_vec) + EPSILONS[data_type]
                        actual_res = env.expect('FT.SEARCH', 'idx', f'@v:[VECTOR_RANGE {range_dist} $blob {score_field_syntax}', 'PARAMS', '2',
                                                'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                        assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                    # Rerun with a different query vector
                    query_vec = create_np_array_typed([0.1, 0.2], data_type)
                    expected_res = [4, 'b', ['dist', spatial.distance.cosine(np.array([0.1, 0.2]), query_vec)],
                                    'c', ['dist', spatial.distance.cosine(np.array([0.1, 0.3]), query_vec)],
                                    'd', ['dist', spatial.distance.cosine(np.array([0.1, 0.4]), query_vec)],
                                    'a', ['dist', spatial.distance.cosine(np.array([0.1, 0.1]), query_vec)]]

                    actual_res = env.expect('FT.SEARCH', 'idx', f'*=>[KNN 4 @v $blob  {score_field_syntax}', 'PARAMS', '2',
                                            'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                    assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)
                    if i==1:  # range query can use only query attributes as score field syntax
                        range_dist = spatial.distance.cosine(np.array([0.1, 0.1]), query_vec) + EPSILONS[data_type]
                        actual_res = env.expect('FT.SEARCH', 'idx', f'@v:[VECTOR_RANGE {range_dist} $blob {score_field_syntax}', 'PARAMS', '2',
                                                'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                        assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                    # Delete one vector and search again
                    conn.execute_command('DEL', 'b')
                    # Expect to get only 3 results (the same as before but without 'b')
                    expected_res = [3, 'c', ['dist', spatial.distance.cosine(np.array([0.1, 0.3]), query_vec)],
                                    'd', ['dist', spatial.distance.cosine(np.array([0.1, 0.4]), query_vec)],
                                    'a', ['dist', spatial.distance.cosine(np.array([0.1, 0.1]), query_vec)]]
                    actual_res = env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $blob AS dist]', 'PARAMS', '2',
                                            'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                    assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                    if i==1:
                        # Test range query
                        range_dist = spatial.distance.cosine(np.array([0.1, 0.1]), query_vec) + EPSILONS[data_type]
                        actual_res = env.expect('FT.SEARCH', 'idx', f'@v:[VECTOR_RANGE {range_dist} $blob]=>{{$yield_distance_as: dist}}',
                                                'PARAMS', '2', 'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                        assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                    conn.execute_command('FT.DROPINDEX', 'idx', 'DD')


def test_sanity_l2():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    for index_type in VECSIM_ALGOS:
        for data_type in VECSIM_DATA_TYPES:
            if index_type == "SVS-VAMANA" and data_type not in ("FLOAT16", "FLOAT32"):
                continue
            compressions_types = [None]
            if index_type == "SVS-VAMANA":
                compressions_types += ["LVQ8"]
            for compression in compressions_types:
                params = ['TYPE', data_type,'DIM', '2', 'DISTANCE_METRIC', 'L2']
                if compression:
                    params += ["COMPRESSION", compression]
                env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', index_type, str(len(params)), *params).ok()
                conn.execute_command('HSET', 'a', 'v', create_np_array_typed([0.1, 0.1], data_type).tobytes())
                conn.execute_command('HSET', 'b', 'v', create_np_array_typed([0.1, 0.2], data_type).tobytes())
                conn.execute_command('HSET', 'c', 'v', create_np_array_typed([0.1, 0.3], data_type).tobytes())
                conn.execute_command('HSET', 'd', 'v', create_np_array_typed([0.1, 0.4], data_type).tobytes())

                query_vec = create_np_array_typed([0.1, 0.1], data_type)

                # Compute the expected distances from the query vector using scipy.spatial
                expected_res = [4, 'a', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.1]), query_vec)],
                                'b', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.2]), query_vec)],
                                'c', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.3]), query_vec)],
                                'd', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.4]), query_vec)]]

                actual_res = env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $blob]=>{$yield_distance_as: dist}', 'PARAMS', '2',
                                        'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                # Test range query
                range_dist = spatial.distance.sqeuclidean(np.array([0.1, 0.4]), query_vec) + EPSILONS[data_type]
                actual_res = env.expect('FT.SEARCH', 'idx', f'@v:[VECTOR_RANGE {range_dist} $blob]=>{{$yield_distance_as: dist}}',
                                        'PARAMS', '2', 'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)
                # Rerun with a different query vector
                query_vec = create_np_array_typed([0.1, 0.19], data_type)
                expected_res = [4, 'b', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.2]), query_vec)],
                                'a', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.1]), query_vec)],
                                'c', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.3]), query_vec)],
                                'd', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.4]), query_vec)]]

                actual_res = env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $blob AS dist]', 'PARAMS', '2',
                                        'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)
                # Test range query
                range_dist = spatial.distance.sqeuclidean(np.array([0.1, 0.4]), query_vec) + EPSILONS[data_type]
                actual_res = env.expect('FT.SEARCH', 'idx', f'@v:[VECTOR_RANGE {range_dist} $blob]=>{{$yield_distance_as: dist}}',
                                        'PARAMS', '2', 'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                # Delete one vector and search again
                conn.execute_command('DEL', 'b')
                # Expect to get only 3 results (the same as before but without 'b')
                expected_res = [3, 'a', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.1]), query_vec)],
                                'c', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.3]), query_vec)],
                                'd', ['dist', spatial.distance.sqeuclidean(np.array([0.1, 0.4]), query_vec)]]
                actual_res = env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $blob AS dist]', 'PARAMS', '2',
                                        'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                # Test range query
                range_dist = spatial.distance.sqeuclidean(np.array([0.1, 0.4]), query_vec) + EPSILONS[data_type]
                actual_res = env.expect('FT.SEARCH', 'idx', f'@v:[VECTOR_RANGE {range_dist} $blob]=>{{$yield_distance_as: dist}}',
                                        'PARAMS', '2', 'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist').res
                assert_query_results(env, expected_res, actual_res, error_msg=f"{index_type, data_type}", data_type=data_type)

                conn.execute_command('FT.DROPINDEX', 'idx', 'DD')


def test_sanity_zero_results():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 4

    for index_type in VECSIM_ALGOS:
        for data_type in VECSIM_DATA_TYPES:
            if index_type == "SVS-VAMANA" and data_type not in ("FLOAT16", "FLOAT32"):
                continue
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', index_type, '6', 'TYPE', data_type,
                       'DIM', dim, 'DISTANCE_METRIC', 'L2', 'n', 'NUMERIC').ok()
            conn.execute_command('HSET', 'a', 'n', 0xa, 'v', create_np_array_typed(np.random.rand(dim), data_type).tobytes())
            conn.execute_command('HSET', 'b', 'n', 0xb, 'v', create_np_array_typed(np.random.rand(dim), data_type).tobytes())
            conn.execute_command('HSET', 'c', 'n', 0xc, 'v', create_np_array_typed(np.random.rand(dim), data_type).tobytes())
            conn.execute_command('HSET', 'd', 'n', 0xd, 'v', create_np_array_typed(np.random.rand(dim), data_type).tobytes())

            query_vec = create_np_array_typed(np.random.rand(dim), data_type)

            # Test looking for 0 results
            env.expect('FT.SEARCH', 'idx', '*=>[KNN 0 @v $blob AS dist]', 'PARAMS', '2', 'blob', query_vec.tobytes()).equal([0])
            env.expect('FT.SEARCH', 'idx', '*=>[KNN $K @v $blob AS dist]', 'PARAMS', '4', 'K', 0, 'blob', query_vec.tobytes()).equal([0])
            env.expect('FT.SEARCH', 'idx', '*=>[KNN 0 @v $blob AS dist]', 'PARAMS', '2', 'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist', 'LIMIT', 0, 10).equal([0])

            # Test looking for 0 results with a filter
            env.expect('FT.SEARCH', 'idx', '@n:[0 10]=>[KNN 0 @v $blob AS dist]', 'PARAMS', '2', 'blob', query_vec.tobytes()).equal([0])
            env.expect('FT.SEARCH', 'idx', '@n:[0 10]=>[KNN $K @v $blob AS dist]', 'PARAMS', '4', 'K', 0, 'blob', query_vec.tobytes()).equal([0])
            env.expect('FT.SEARCH', 'idx', '@n:[0 10]=>[KNN 0 @v $blob AS dist]', 'PARAMS', '2', 'blob', query_vec.tobytes(), 'SORTBY', 'dist', 'RETURN', '1', 'dist', 'LIMIT', 0, 10).equal([0])

            # End of round cleanup
            conn.execute_command('FT.DROPINDEX', 'idx', 'DD')


def test_del_reuse():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    def del_insert(env):
        conn = getConnectionByEnv(env)

        conn.execute_command('DEL', 'a')
        conn.execute_command('DEL', 'b')
        conn.execute_command('DEL', 'c')
        conn.execute_command('DEL', 'd')

        env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([0])

        res = [''.join(random.choice(str(x).lower()) for x in range(8)),
               ''.join(random.choice(str(x).lower()) for x in range(8)),
               ''.join(random.choice(str(x).lower()) for x in range(8)),
               ''.join(random.choice(str(x).lower()) for x in range(8))]

        conn.execute_command('HSET', 'a', 'v', res[0])
        conn.execute_command('HSET', 'b', 'v', res[1])
        conn.execute_command('HSET', 'c', 'v', res[2])
        conn.execute_command('HSET', 'd', 'v', res[3])
        return res

    # test start
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')

    vecs = del_insert(env)
    res = [4, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh', 'RETURN', '1', 'v').equal(res)

    vecs = del_insert(env)
    res = [4, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh', 'RETURN', '1', 'v').equal(res)

    vecs = del_insert(env)
    res = [4, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh', 'RETURN', '1', 'v').equal(res)


# test for issue https://github.com/RediSearch/RediSearch/pull/2705
@skip(no_json=True)
def test_update_with_bad_value():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'ON', 'JSON',
                        'SCHEMA', '$.v', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')
    conn.execute_command('FT.CREATE', 'idx2',
                        'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')

    res = [1, 'doc:1', ['$', '{"v":[1,3]}']]
    # Add doc contains a vector to the index
    env.assertOk(conn.execute_command('JSON.SET', 'doc:1', '$', '{"v":[1,2]}'))
    # Override with bad vector value (wrong blob size)
    env.assertEqual(conn.execute_command('JSON.ARRINSERT', 'doc:1', '$.v', '2', '3'), [3])
    # Override again with legal vector value
    env.assertEqual(conn.execute_command('JSON.ARRPOP', 'doc:1', '$.v', '1'), ['2'])
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v[*]'), '[1,3]')
    env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.v'), [2])
    waitForIndex(env, 'idx')
    # before the issue fix, the second query will result in empty result, as the first vector value was not deleted when
    # its value was override with a bad value
    env.expect('FT.SEARCH', 'idx', '*').equal(res)
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', '????????', 'RETURN', '1', '$').equal(res)

    res = [1, 'h1', ['vec', '????>>>>']]
    # Add doc contains a vector to the index
    env.assertEqual(conn.execute_command('HSET', 'h1', 'vec', '????????'), 1)
    # Override with bad vector value (wrong blob size)
    env.assertEqual(conn.execute_command('HSET', 'h1', 'vec', 'bad-val'), 0)
    # Override again with legal vector value
    env.assertEqual(conn.execute_command('HSET', 'h1', 'vec', '????>>>>'), 0)
    waitForIndex(env, 'idx2')
    # before the issue fix, the second query will result in empty result, as the first vector value was not deleted when
    # its value was override with a bad value
    env.expect('FT.SEARCH', 'idx2', '*').equal(res)
    env.expect('FT.SEARCH', 'idx2', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', '????????', 'RETURN', '1', 'vec').equal(res)

@skip(cluster=True)
def test_create():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # A value to use as a dummy value for memory fields in the info command (and any other irrelevant fields)
    # as we don't care about the actual value of these fields in this test
    dummy_val = 'dummy_supplement'

    # Test for INT32, INT64 as well when support for these types is added.
    for data_type in VECSIM_DATA_TYPES + ['INT8', 'UINT8']:
        conn.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'v_HNSW', 'VECTOR', 'HNSW', '14', 'TYPE', data_type,
                             'DIM', '1024', 'DISTANCE_METRIC', 'COSINE', 'INITIAL_CAP', '10', 'M', '16',
                             'EF_CONSTRUCTION', '200', 'EF_RUNTIME', '10')
        conn.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'v_FLAT', 'VECTOR', 'FLAT', '8', 'TYPE', data_type,
                             'DIM', '1024', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', '10')

        expected_HNSW = ['ALGORITHM', 'TIERED', 'TYPE', data_type, 'DIMENSION', 1024, 'METRIC', 'COSINE', 'IS_MULTI_VALUE', 0, 'INDEX_SIZE', 0, 'INDEX_LABEL_COUNT', 0, 'MEMORY', dummy_val, 'LAST_SEARCH_MODE', 'EMPTY_MODE', 'MANAGEMENT_LAYER_MEMORY', dummy_val, 'BACKGROUND_INDEXING', 0, 'TIERED_BUFFER_LIMIT', 1024, 'FRONTEND_INDEX', ['ALGORITHM', 'FLAT', 'TYPE', data_type, 'DIMENSION', 1024, 'METRIC', 'COSINE', 'IS_MULTI_VALUE', 0, 'INDEX_SIZE', 0, 'INDEX_LABEL_COUNT', 0, 'MEMORY', dummy_val, 'LAST_SEARCH_MODE', 'EMPTY_MODE', 'BLOCK_SIZE', 1024], 'BACKEND_INDEX', ['ALGORITHM', 'HNSW', 'TYPE', data_type, 'DIMENSION', 1024, 'METRIC', 'COSINE', 'IS_MULTI_VALUE', 0, 'INDEX_SIZE', 0, 'INDEX_LABEL_COUNT', 0, 'MEMORY', dummy_val, 'LAST_SEARCH_MODE', 'EMPTY_MODE', 'BLOCK_SIZE', 1024, 'M', 16, 'EF_CONSTRUCTION', 200, 'EF_RUNTIME', 10, 'MAX_LEVEL', -1, 'ENTRYPOINT', -1, 'EPSILON', '0.01', 'NUMBER_OF_MARKED_DELETED', 0], 'TIERED_HNSW_SWAP_JOBS_THRESHOLD', 1024]
        expected_FLAT = ['ALGORITHM', 'FLAT', 'TYPE', data_type, 'DIMENSION', 1024, 'METRIC', 'L2', 'IS_MULTI_VALUE', 0, 'INDEX_SIZE', 0, 'INDEX_LABEL_COUNT', 0, 'MEMORY', dummy_val, 'LAST_SEARCH_MODE', 'EMPTY_MODE', 'BLOCK_SIZE', 1024]

        # SVS-VAMANA only supports FLOAT32 and FLOAT16 data types
        if data_type in ('FLOAT32', 'FLOAT16'):
            conn.execute_command('FT.CREATE', 'idx3', 'SCHEMA', 'v_SVS_VAMANA', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', data_type,
                                 'DIM', '1024', 'DISTANCE_METRIC', 'L2')
            expected_SVS_VAMANA = ['ALGORITHM', 'TIERED', 'TYPE', data_type, 'DIMENSION', 1024, 'METRIC', 'L2', 'IS_MULTI_VALUE', 0, 'INDEX_SIZE', 0, 'INDEX_LABEL_COUNT', 0, 'MEMORY', dummy_val, 'LAST_SEARCH_MODE', 'EMPTY_MODE', 'MANAGEMENT_LAYER_MEMORY', dummy_val, 'BACKGROUND_INDEXING', 0, 'TIERED_BUFFER_LIMIT', 1024, 'FRONTEND_INDEX', ['ALGORITHM', 'FLAT', 'TYPE', data_type, 'DIMENSION', 1024, 'METRIC', 'L2', 'IS_MULTI_VALUE', 0, 'INDEX_SIZE', 0, 'INDEX_LABEL_COUNT', 0, 'MEMORY', dummy_val, 'LAST_SEARCH_MODE', 'EMPTY_MODE', 'BLOCK_SIZE', 1024], 'BACKEND_INDEX', ['ALGORITHM', 'SVS', 'TYPE', data_type, 'DIMENSION', 1024, 'METRIC', 'L2', 'IS_MULTI_VALUE', 0, 'INDEX_SIZE', 0, 'INDEX_LABEL_COUNT', 0, 'MEMORY', dummy_val, 'LAST_SEARCH_MODE', 'EMPTY_MODE', 'BLOCK_SIZE', 1024, 'COMPRESSION', 'NONE', 'ALPHA', 1.2, 'GRAPH_MAX_DEGREE', 32, 'CONSTRUCTION_WINDOW_SIZE', 200, 'MAX_CANDIDATE_POOL_SIZE', 600, 'PRUNE_TO', 28, 'USE_SEARCH_HISTORY', 1, 'NUM_THREADS', 1, 'NUMBER_OF_MARKED_DELETED_NODES', 0, 'SEARCH_WINDOW_SIZE', 10, 'EPSILON', 0.01], 'TIERED_SVS_TRAINING_THRESHOLD', 10240]

        for _ in env.reloadingIterator():
            info = ['identifier', 'v_HNSW', 'attribute', 'v_HNSW', 'type', 'VECTOR']
            env.assertEqual(index_info(env, 'idx1')['attributes'][0][:len(info)], info)
            info_data_HNSW = conn.execute_command(debug_cmd(), "VECSIM_INFO", "idx1", "v_HNSW")
            # replace memory values with a dummy value - irrelevant for the test
            info_data_HNSW[info_data_HNSW.index('MEMORY') + 1] = dummy_val
            info_data_HNSW[info_data_HNSW.index('MANAGEMENT_LAYER_MEMORY') + 1] = dummy_val
            front = info_data_HNSW[info_data_HNSW.index('FRONTEND_INDEX') + 1]
            front[front.index('MEMORY') + 1] = dummy_val
            back = info_data_HNSW[info_data_HNSW.index('BACKEND_INDEX') + 1]
            back[back.index('MEMORY') + 1] = dummy_val

            env.assertEqual(info_data_HNSW, expected_HNSW)

            info_data_FLAT = conn.execute_command(debug_cmd(), "VECSIM_INFO", "idx2", "v_FLAT")
            # replace memory value with a dummy value - irrelevant for the test
            info_data_FLAT[info_data_FLAT.index('MEMORY') + 1] = dummy_val

            env.assertEqual(info_data_FLAT, expected_FLAT)

            # Test SVS-VAMANA index only for supported data types
            if data_type in ('FLOAT32', 'FLOAT16'):
                info = ['identifier', 'v_SVS_VAMANA', 'attribute', 'v_SVS_VAMANA', 'type', 'VECTOR']
                env.assertEqual(index_info(env, 'idx3')['attributes'][0][:len(info)], info)
                info_data_SVS_VAMANA = conn.execute_command(debug_cmd(), "VECSIM_INFO", "idx3", "v_SVS_VAMANA")
                # replace memory values with a dummy value - irrelevant for the test
                info_data_SVS_VAMANA[info_data_SVS_VAMANA.index('MEMORY') + 1] = dummy_val
                info_data_SVS_VAMANA[info_data_SVS_VAMANA.index('MANAGEMENT_LAYER_MEMORY') + 1] = dummy_val
                front_svs = info_data_SVS_VAMANA[info_data_SVS_VAMANA.index('FRONTEND_INDEX') + 1]
                front_svs[front_svs.index('MEMORY') + 1] = dummy_val
                back_svs = info_data_SVS_VAMANA[info_data_SVS_VAMANA.index('BACKEND_INDEX') + 1]
                back_svs[back_svs.index('MEMORY') + 1] = dummy_val

                # TODO: enable once info iterator implemented for svs-vamana
                # env.assertEqual(info_data_SVS_VAMANA, expected_SVS_VAMANA)

        conn.execute_command('FLUSHALL')

@skip(cluster=True)
def test_search_ints(env:Env):
    dim = 4
    idx = 'idx'
    fld = 'v'
    dataset = {
        'a': [40]*dim,
        'b': [30]*dim,
        'c': [20]*dim,
        'd': [10]*dim,
    }
    expected_scores = {k: str(int(spatial.distance.sqeuclidean(np.array(v), np.zeros(dim)))) for k, v in dataset.items()}

    for data_type in ['INT8', 'UINT8']:
        env.flush()

        env.cmd('FT.CREATE', idx, 'SCHEMA', fld, 'VECTOR', 'FLAT', '6', 'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2')
        for k, v in dataset.items():
            env.cmd('HSET', k, fld, create_np_array_typed(v, data_type).tobytes())

        query_vec = create_np_array_typed([0]*dim, data_type)

        expected_res = [len(dataset)]
        for k in dataset:
            expected_res.extend([k, ['score', expected_scores[k]]])
        res = env.cmd('FT.SEARCH', idx, f'*=>[KNN 4 @{fld} $blob AS score]', 'DIALECT', 2,
                      'PARAMS', 2, 'blob', query_vec.tobytes(), 'RETURN', 1, 'score')
        env.assertEqual(res, expected_res, message=data_type)

        for _ in env.reloadingIterator():
            expected_res = [len(dataset)]
            for k in sorted(dataset, key=lambda x: int(expected_scores[x])):
                expected_res.extend([k, ['score', expected_scores[k]]])
            res = env.cmd('FT.SEARCH', idx, f'*=>[KNN 4 @{fld} $blob AS score]', 'DIALECT', 2, 'SORTBY', 'score',
                          'PARAMS', 2, 'blob', query_vec.tobytes(), 'RETURN', 1, 'score')
            env.assertEqual(res, expected_res, message=data_type)

@skip(cluster=True)
def test_create_multiple_vector_fields():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    dim = 2
    conn = getConnectionByEnv(env)
    # Create index with 2 vector fields, where the first is a prefix of the second.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'COSINE',
               'v_flat', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    # Validate each index type.
    info_data = to_dict(conn.execute_command(debug_cmd(), "VECSIM_INFO", "idx", "v"))
    for nested in ['BACKEND_INDEX', 'FRONTEND_INDEX']:
        info_data[nested] = to_dict(info_data[nested])

    env.assertEqual(info_data['ALGORITHM'], 'TIERED')
    env.assertEqual(info_data['BACKEND_INDEX']['ALGORITHM'], 'HNSW')
    info_data = to_dict(conn.execute_command(debug_cmd(), "VECSIM_INFO", "idx", "v_flat"))
    env.assertEqual(info_data['ALGORITHM'], 'FLAT')

    # Insert one vector only to each index, validate it was inserted only to the right index.
    conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
    info_data = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))
    env.assertEqual(info_data['INDEX_SIZE'], 1)
    info_data = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v_flat"))
    env.assertEqual(info_data['INDEX_SIZE'], 0)

    conn.execute_command('HSET', 'b', 'v_flat', 'bbbbbbbb')
    info_data = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))
    env.assertEqual(info_data['INDEX_SIZE'], 1)
    info_data = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v_flat"))
    env.assertEqual(info_data ['INDEX_SIZE'], 1)

    # Search in every index once, validate it was performed only to the right index.
    env.cmd('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh')
    info_data = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))
    env.assertEqual(info_data['LAST_SEARCH_MODE'], 'STANDARD_KNN')
    info_data = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v_flat"))
    env.assertEqual(info_data['LAST_SEARCH_MODE'], 'EMPTY_MODE')


def test_create_errors():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    # missing init args
    ## flat algorithm
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR').error().contains('Bad arguments for vector similarity algorithm')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT').error().contains('Bad arguments for vector similarity number of parameters')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6').error().contains('Expected 6 parameters but got 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '1').error().contains('Bad number of arguments for vector similarity index: got 1 but expected even number')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '2', 'SIZE').error().contains('Bad arguments for algorithm FLAT: SIZE')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '2', 'TYPE').error().contains('Bad arguments for vector similarity FLAT index `TYPE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'TYPE', 'FLOAT32', 'DIM').error().contains('Bad arguments for vector similarity FLAT index `DIM`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create FLAT index without specifying TYPE argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create FLAT index without specifying DIM argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'TYPE', 'FLOAT32', 'DIM', '1024').error().contains('Missing mandatory parameter: cannot create FLAT index without specifying DISTANCE_METRIC argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC').error().contains('Bad arguments for vector similarity FLAT index `DISTANCE_METRIC`')
    ## HNSW algorithm
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW').error().contains('Bad arguments for vector similarity number of parameters')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6').error().contains('Expected 6 parameters but got 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '1').error().contains('Bad number of arguments for vector similarity index: got 1 but expected even number')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '2', 'SIZE').error().contains('Bad arguments for algorithm HNSW: SIZE')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '2', 'TYPE').error().contains('Bad arguments for vector similarity HNSW index `TYPE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'TYPE', 'FLOAT32', 'DIM').error().contains('Bad arguments for vector similarity HNSW index `DIM`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create HNSW index without specifying TYPE argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create HNSW index without specifying DIM argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'TYPE', 'FLOAT32', 'DIM', '1024').error().contains('Missing mandatory parameter: cannot create HNSW index without specifying DISTANCE_METRIC argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC').error().contains('Bad arguments for vector similarity HNSW index `DISTANCE_METRIC`')
    ## SVS-VAMANA algorithm
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA').error().contains('Bad arguments for vector similarity number of parameters')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '6').error().contains('Expected 6 parameters but got 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '1').error().contains('Bad number of arguments for vector similarity index: got 1 but expected even number')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '2', 'SIZE').error().contains('Bad arguments for algorithm SVS-VAMANA: SIZE')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '2', 'TYPE').error().contains('Bad arguments for vector similarity SVS-VAMANA index `TYPE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '4', 'TYPE', 'FLOAT32', 'DIM').error().contains('Bad arguments for vector similarity SVS-VAMANA index `DIM`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '4', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create SVS-VAMANA index without specifying TYPE argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '4', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create SVS-VAMANA index without specifying DIM argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '4', 'TYPE', 'FLOAT32', 'DIM', '1024').error().contains('Missing mandatory parameter: cannot create SVS-VAMANA index without specifying DISTANCE_METRIC argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC').error().contains('Bad arguments for vector similarity SVS-VAMANA index `DISTANCE_METRIC`')


    # invalid init args
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'DOUBLE', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity HNSW index `TYPE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', 'str', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity HNSW index `DIM`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'REDIS').error().contains('Bad arguments for vector similarity HNSW index `DISTANCE_METRIC`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'REDIS', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity algorithm')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', 'str', 'BLOCK_SIZE', '16') \
        .error().contains('Bad arguments for vector similarity FLAT index `INITIAL_CAP`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '10', 'BLOCK_SIZE', 'str') \
        .error().contains('Bad arguments for vector similarity FLAT index `BLOCK_SIZE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', 'str', 'M', '16', 'EF_CONSTRUCTION', '200') \
        .error().contains('Bad arguments for vector similarity HNSW index `INITIAL_CAP`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', 'str', 'EF_CONSTRUCTION', '200') \
        .error().contains('Bad arguments for vector similarity HNSW index `M`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EF_CONSTRUCTION', 'str') \
        .error().contains('Bad arguments for vector similarity HNSW index `EF_CONSTRUCTION`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EF_RUNTIME', 'str') \
        .error().contains('Bad arguments for vector similarity HNSW index `EF_RUNTIME`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EF_RUNTIME', '14.3') \
        .error().contains('Bad arguments for vector similarity HNSW index `EF_RUNTIME`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EF_RUNTIME', '-10') \
        .error().contains('Bad arguments for vector similarity HNSW index `EF_RUNTIME`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EPSILON', 'str') \
        .error().contains('Bad arguments for vector similarity HNSW index `EPSILON`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EPSILON', '-1') \
        .error().contains('Bad arguments for vector similarity HNSW index `EPSILON`')
    # SVS-VAMANA related
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', 'DOUBLE', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity SVS-VAMANA index `TYPE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', 'FLOAT64', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Not supported data type is given. Expected: FLOAT16, FLOAT32')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', 'FLOAT32', 'DIM', 'str', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity SVS-VAMANA index `DIM`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'REDIS').error().contains('Bad arguments for vector similarity SVS-VAMANA index `DISTANCE_METRIC`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '10', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', 'str') \
        .error().contains('Bad arguments for algorithm SVS-VAMANA: INITIAL_CAP')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 8, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', 'str') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `GRAPH_MAX_DEGREE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 8, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '-1') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `GRAPH_MAX_DEGREE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 8, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '0.1') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `GRAPH_MAX_DEGREE`')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'CONSTRUCTION_WINDOW_SIZE', 'str') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `CONSTRUCTION_WINDOW_SIZE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'CONSTRUCTION_WINDOW_SIZE', '-1') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `CONSTRUCTION_WINDOW_SIZE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'CONSTRUCTION_WINDOW_SIZE', '0.5') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `CONSTRUCTION_WINDOW_SIZE`')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'SEARCH_WINDOW_SIZE', 'str') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `SEARCH_WINDOW_SIZE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'SEARCH_WINDOW_SIZE', '-1') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `SEARCH_WINDOW_SIZE`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'SEARCH_WINDOW_SIZE', '0.5') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `SEARCH_WINDOW_SIZE`')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'COMPRESSION', '4') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `COMPRESSION`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'COMPRESSION', '0') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `COMPRESSION`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'COMPRESSION', 'LWQ4x4') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `COMPRESSION`')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 12, 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'CONSTRUCTION_WINDOW_SIZE', '200', 'EPSILON', 'str') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `EPSILON`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 12, 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8','CONSTRUCTION_WINDOW_SIZE', '200', 'EPSILON', '-1') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `EPSILON`')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'TRAINING_THRESHOLD', '-1') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `TRAINING_THRESHOLD`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'TRAINING_THRESHOLD', 'str') \
        .error().contains('Bad arguments for vector similarity SVS-VAMANA index `TRAINING_THRESHOLD`')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'TRAINING_THRESHOLD', '1') \
        .error().contains('Invalid TRAINING_THRESHOLD')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 10, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'TRAINING_THRESHOLD', '2048') \
        .error().contains('TRAINING_THRESHOLD is irrelevant when compression was not requested')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', 12, 'TYPE', 'FLOAT32', 'DIM', 16, 'DISTANCE_METRIC', 'IP', 'GRAPH_MAX_DEGREE', '8', 'TRAINING_THRESHOLD', '2048', 'COMPRESSION', 'LVQ8') \
        .ok()   # valid case when training threshold is set while compression is set also, but after.

def test_index_errors():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                         'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2')
    error_count = 0

    # Check that the index errors are empty
    env.assertEqual(index_errors(env)['indexing failures'], error_count)
    env.assertEqual(index_errors(env)['last indexing error'], 'N/A')
    env.assertEqual(index_errors(env)['last indexing error key'], 'N/A')
    assertEqual_dicts_on_intersection(env, field_errors(env), index_errors(env))

    for i in range(0, 5, 2):
        conn.execute_command('HSET', i, 'v', create_np_array_typed([0]).tobytes())
        error_count += 1
        cur_index_errors = index_errors(env)
        env.assertEqual(cur_index_errors['indexing failures'], error_count)
        env.assertEqual(cur_index_errors['last indexing error'], f'Could not add vector with blob size 4 (expected size 8)')
        env.assertEqual(cur_index_errors['last indexing error key'], str(i))
        assertEqual_dicts_on_intersection(env, cur_index_errors, field_errors(env))

        conn.execute_command('HSET', i + 1, 'v', create_np_array_typed([0, 0, 0]).tobytes())
        error_count += 1
        cur_index_errors = index_errors(env)
        env.assertEqual(cur_index_errors['indexing failures'], error_count)
        env.assertEqual(cur_index_errors['last indexing error'], f'Could not add vector with blob size 12 (expected size 8)')
        env.assertEqual(cur_index_errors['last indexing error key'], str(i + 1))
        assertEqual_dicts_on_intersection(env, cur_index_errors, field_errors(env))


def test_search_errors():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    dim = 2
    other_types = [t for t in VECSIM_DATA_TYPES if t != 'FLOAT32']
    v_flat = 'v_flat_float64' # Arbitrary choice of flat vector index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 's', 'TEXT', 't', 'TAG', 'SORTABLE',
                'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
                'INITIAL_CAP', '10', 'M', '16', 'EF_CONSTRUCTION', '200').ok()
    for data_type in other_types:
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', f'v_flat_{data_type.lower()}',
                    'VECTOR', 'FLAT', '6', 'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(4):
            v = [i] * dim
            hset_args = ['HSET', i, 'v', create_np_array_typed(v).tobytes(), 's', 'hello']
            for data_type in other_types:
                hset_args += [f'v_flat_{data_type.lower()}', create_np_array_typed(v, data_type).tobytes()]
            conn.execute_command(*hset_args)

    env.expect('FT.SEARCH', 'idx', '*=>[REDIS 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN str @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '*=>[K 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')

    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefg').error().contains('Error parsing vector similarity query: query vector blob size (7) does not match index\'s expected size (8).')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefghi').error().contains('Error parsing vector similarity query: query vector blob size (9) does not match index\'s expected size (8).')
    for data_type in other_types:
        query_size = dim * (np.dtype(data_type.lower()).itemsize if data_type.lower() != 'bfloat16' else 2)
        for bad_size in [query_size - 1, query_size + 1]:
            env.expect('FT.SEARCH', 'idx', f'*=>[KNN 2 @v_flat_{data_type.lower()} $b]', 'PARAMS', '2', 'b', '?' * bad_size).error(
                ).contains(f'Error parsing vector similarity query: query vector blob size ({bad_size}) does not match index\'s expected size ({query_size}).')

    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @t $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Expected a VECTOR field at offset 10 near t')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS v]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `v` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS s]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `s` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS t]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `t` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS $score]', 'PARAMS', '4', 'score', 't', 'b', 'abcdefgh').error().contains('Property `t` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]=>{$yield_distance_as:v;}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `v` already exists in schema')

    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_RUNTIME -42]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_RUNTIME 2.71828]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_RUNTIME 5 EF_RUNTIME 6]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Parameter was specified twice')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_FUNTIME 30]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid option')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]=>{$EF_RUNTIME: 5; $EF_RUNTIME: 6;}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Parameter was specified twice')

    # ef_runtime is invalid for FLAT index.
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN 2 @{v_flat} $b EF_RUNTIME 30]', 'PARAMS', '2', 'b', 'abcdefghabcdefgh').error().contains('Error parsing vector similarity parameters: Invalid option')

    # Hybrid attributes with non-hybrid query is invalid.
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b BATCH_SIZE 100]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: hybrid query attributes were sent for a non-hybrid query')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b HYBRID_POLICY ADHOC_BF]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: hybrid query attributes were sent for a non-hybrid query')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b HYBRID_POLICY BATCHES]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: hybrid query attributes were sent for a non-hybrid query')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b HYBRID_POLICY BATCHES BATCH_SIZE 100]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: hybrid query attributes were sent for a non-hybrid query')

    # Invalid hybrid attributes.
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b BATCH_SIZE 0]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b BATCH_SIZE -6]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b BATCH_SIZE 34_not_a_number]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b BATCH_SIZE 8 BATCH_SIZE 0]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Parameter was specified twice')
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b HYBRID_POLICY bad_policy]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('invalid hybrid policy was given')
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b]=>{$HYBRID_POLICY: bad_policy;}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('invalid hybrid policy was given')

    # Invalid hybrid attributes combinations.
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b HYBRID_POLICY ADHOC_BF BATCH_SIZE 100]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains("Error parsing vector similarity parameters: 'batch size' is irrelevant for 'ADHOC_BF' policy")
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b HYBRID_POLICY ADHOC_BF EF_RUNTIME 100]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains("Error parsing vector similarity parameters: 'EF_RUNTIME' is irrelevant for 'ADHOC_BF' policy")

    # Invalid query combination with query attributes syntax.
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS score]=>{$yield_distance_as:score2;}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Distance field was specified twice for vector query: score and score2')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_RUNTIME 100]=>{$EF_RUNTIME:200;}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Parameter was specified twice')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS $score_1]=>{$yield_distance_as:$score_2;}', 'PARAMS', '6', 'b', 'abcdefgh', 'score_1', 'score_1_val', 'score_2', 'score_2_val').error().contains('Distance field was specified twice for vector query: score_1_val and score_2_val')
    env.expect('FT.SEARCH', 'idx', 'hello=>[KNN 2 @v $b AS score]=>{$yield_distance_as:__v_score;}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Distance field was specified twice for vector query: score and __v_score')
    env.expect('FT.SEARCH', 'idx', 'hello=>[KNN 2 @v $b AS score]=>{$yield_distance_as:score;}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Distance field was specified twice for vector query: score and score')

    # Invalid range queries
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]', 'PARAMS', '2', 'b', 'abcdefg').error().contains('Error parsing vector similarity query: query vector blob size (7) does not match index\'s expected size (8).')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]', 'PARAMS', '2', 'b', 'abcdefghi').error().contains('Error parsing vector similarity query: query vector blob size (9) does not match index\'s expected size (8).')
    env.expect('FT.SEARCH', 'idx', '@bad:[vector_range 0.1 $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Unknown field at offset 0 near bad')
    env.expect('FT.SEARCH', 'idx', '@v:[vector 0.1 $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range -1 $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().equal('Error parsing vector similarity query: negative radius (-1) given in a range query')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$yield_distance_as:t}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `t` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$yield_distance_as:dist} @v:[vector_range 0.2 $b]=>{$yield_distance_as:dist}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `dist` specified more than once')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$yield_distance_as:$dist}', 'PARAMS', '4', 'b', 'abcdefgh', 'dist', 't').error().contains('Property `t` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$EF_RUNTIME:10}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid option')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$HYBRID_POLICY:BATCHES}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: hybrid query attributes were sent for a non-hybrid query')

    # Invalid epsilon param for range queries
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$EPSILON: -1}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$EPSILON: 0}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$EPSILON: not_a_num}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid value was given')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$EPSILON: 0.1; $EPSILON: 0.2}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Parameter was specified twice')
    env.expect('FT.SEARCH', 'idx', '@v:[vector_range 0.1 $b]=>{$EPSILON: 0.1; $EF_RUNTIME: 20}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid option')

    # epsilon is invalid for non-range queries, and also for flat index.
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EPSILON 2.71828]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: range query attributes were sent for a non-range query')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]=>{$EPSILON: 2.71828}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: range query attributes were sent for a non-range query')
    env.expect('FT.SEARCH', 'idx', '@s:hello=>[KNN 2 @v $b]=>{$EPSILON: 0.1}', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: range query attributes were sent for a non-range query')
    env.expect('FT.SEARCH', 'idx', f'@{v_flat}:[vector_range 0.1 $b]=>{{$epsilon:0.1}}', 'PARAMS', '2', 'b', 'abcdefghabcdefgh').equal('Error parsing vector similarity parameters: Invalid option')


def test_with_fields():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 MIN_OPERATION_WORKERS 0')
    conn = getConnectionByEnv(env)
    dimension = 128
    qty = 100

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2', 't', 'TEXT')
    load_vectors_with_texts_into_redis(conn, 'v', dimension, qty)

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        query_data = np.float32(np.random.random((1, dimension)))
        res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN 100 @v $vec_param AS score]',
                        'SORTBY', 'score', 'PARAMS', 2, 'vec_param', query_data.tobytes(),
                        'RETURN', 2, 'score', 't')
        res_nocontent = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN 100 @v $vec_param AS score]',
                        'SORTBY', 'score', 'PARAMS', 2, 'vec_param', query_data.tobytes(),
                        'NOCONTENT')
        dist_range = dimension * qty**2  # distance from query vector to the furthest vector in the index.
        res_range = conn.execute_command('FT.SEARCH', 'idx', '@v:[VECTOR_RANGE $r $vec_param]=>{$YIELD_DISTANCE_AS:score}',
                                         'SORTBY', 'score', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', dist_range,
                                         'RETURN', 2, 'score', 't')
        env.assertEqual(res[1::2], res_nocontent[1:])
        env.assertEqual('t', res[2][2])
        # TODO: in coordinator, the first field that indicates the number of total results in 10 when running
        #  KNN query instead of 100 (but not for range) - should be fixed
        env.assertEqual(res[1:], res_range[1:])

    # MOD-7887 - bad error message
    # Expect to get the same result when using a score field and when not, in case of a field name that does not exist in the schema
    exp = env.expect('FT.SEARCH', 'idx', '*=>[KNN 100 @not_a_field $vec_param]', 'PARAMS', 2, 'vec_param', query_data.tobytes()).res
    res = env.expect('FT.SEARCH', 'idx', '*=>[KNN 100 @not_a_field $vec_param AS score]', 'PARAMS', 2, 'vec_param', query_data.tobytes()).res
    env.assertEqual(exp, res)
    res = env.expect('FT.SEARCH', 'idx', '*=>[KNN 100 @not_a_field $vec_param AS score]', 'SORTBY', 'score', 'PARAMS', 2, 'vec_param', query_data.tobytes()).res
    env.assertEqual(exp, res)
    res = env.expect('FT.SEARCH', 'idx', '@not_a_field:(vectors are cool)').res
    env.assertEqual(exp, res.replace('0', '12')) # in this case the field offset is different


def test_memory_info():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    # This test flow adds two vectors and deletes them. The test checks for memory increase in Redis and RediSearch upon insertion and decrease upon delete.
    conn = getConnectionByEnv(env)
    dimension = 128
    index_key = 'idx'
    vector_field = 'v'

    # Create index. Flat index implementation will free memory when deleting vectors, so it is a good candidate for this test with respect to memory consumption.
    conn.execute_command('FT.CREATE', index_key, 'SCHEMA', vector_field, 'VECTOR', 'FLAT', '8', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2', 'BLOCK_SiZE', '1')
    # Verify redis memory >= redisearch index memory
    if not env.isCluster():
        vecsim_memory = get_vecsim_memory(env, index_key=index_key, field_name=vector_field)
    redisearch_memory = get_redisearch_vector_index_memory(env, index_key=index_key)
    redis_memory = get_redis_memory_in_mb(env)
    if not env.isCluster():
        env.assertEqual(redisearch_memory, vecsim_memory)
    env.assertLessEqual(redisearch_memory, redis_memory)
    vector = np.float32(np.random.random((1, dimension)))

    # Add vector.
    conn.execute_command('HSET', 1, vector_field, vector.tobytes())
    # Verify current memory readings > previous memory readings.
    cur_redisearch_memory = get_redisearch_vector_index_memory(env, index_key=index_key)
    env.assertLessEqual(redisearch_memory, cur_redisearch_memory)
    redis_memory = get_redis_memory_in_mb(env)
    redisearch_memory = cur_redisearch_memory
    # Verify redis memory >= redisearch index memory
    env.assertLessEqual(redisearch_memory, redis_memory)
    if not env.isCluster():
        cur_vecsim_memory = get_vecsim_memory(env, index_key=index_key, field_name=vector_field)
        env.assertLessEqual(vecsim_memory, cur_vecsim_memory)
        vecsim_memory = cur_vecsim_memory
        #verify vecsim memory == redisearch memory
        env.assertEqual(cur_vecsim_memory, cur_redisearch_memory)

    # Add vector.
    conn.execute_command('HSET', 2, vector_field, vector.tobytes())
    # Verify current memory readings > previous memory readings.
    cur_redisearch_memory = get_redisearch_vector_index_memory(env, index_key=index_key)
    env.assertLessEqual(redisearch_memory, cur_redisearch_memory)
    redis_memory = get_redis_memory_in_mb(env)
    redisearch_memory = cur_redisearch_memory
    # Verify redis memory >= redisearch index memory
    env.assertLessEqual(redisearch_memory, redis_memory)
    if not env.isCluster():
        cur_vecsim_memory = get_vecsim_memory(env, index_key=index_key, field_name=vector_field)
        env.assertLessEqual(vecsim_memory, cur_vecsim_memory)
        vecsim_memory = cur_vecsim_memory
        #verify vecsim memory == redisearch memory
        env.assertEqual(cur_vecsim_memory, cur_redisearch_memory)

    # Delete vector
    conn.execute_command('DEL', 2)
    # Verify current memory readings < previous memory readings.
    cur_redisearch_memory = get_redisearch_vector_index_memory(env, index_key=index_key)
    env.assertLessEqual(cur_redisearch_memory, redisearch_memory)
    redis_memory = get_redis_memory_in_mb(env)
    redisearch_memory = cur_redisearch_memory
    # Verify redis memory >= redisearch index memory
    env.assertLessEqual(redisearch_memory, redis_memory)
    if not env.isCluster():
        cur_vecsim_memory = get_vecsim_memory(env, index_key=index_key, field_name=vector_field)
        env.assertLessEqual(cur_vecsim_memory, vecsim_memory)
        vecsim_memory = cur_vecsim_memory
        #verify vecsim memory == redisearch memory
        env.assertEqual(cur_vecsim_memory, cur_redisearch_memory)

    # Delete vector
    conn.execute_command('DEL', 1)
    # Verify current memory readings < previous memory readings.
    cur_redisearch_memory = get_redisearch_vector_index_memory(env, index_key=index_key)
    env.assertLessEqual(cur_redisearch_memory, redisearch_memory)
    redis_memory = get_redis_memory_in_mb(env)
    redisearch_memory = cur_redisearch_memory
    # Verify redis memory >= redisearch index memory
    env.assertLessEqual(redisearch_memory, redis_memory)
    if not env.isCluster():
        cur_vecsim_memory = get_vecsim_memory(env, index_key=index_key, field_name=vector_field)
        env.assertLessEqual(cur_vecsim_memory, vecsim_memory)
        vecsim_memory = cur_vecsim_memory
        #verify vecsim memory == redisearch memory
        env.assertEqual(cur_vecsim_memory, cur_redisearch_memory)


def test_hybrid_query_batches_mode_with_text():
    # Set high GC threshold so to eliminate sanitizer warnings from of false leaks from forks (MOD-6229)
    env = Env(moduleArgs='DEFAULT_DIALECT 2 FORK_GC_CLEAN_THRESHOLD 10000')
    conn = getConnectionByEnv(env)
    # Index size is chosen so that batches mode will be selected by the heuristics.
    dim = 2
    index_size = 6000 * env.shardsCount
    data_type = 'FLOAT64'

    env.expect('FT.CREATE', f'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2', 't', 'TEXT').ok()
    load_vectors_with_texts_into_redis(conn, 'v', dim, index_size, data_type)
    query_data = create_np_array_typed([index_size] * dim, data_type)

    # Expect to find no result (internally, build the child iterator as empty iterator).
    env.expect('FT.SEARCH', 'idx', '(nothing)=>[KNN 10 @v $vec_param]', 'PARAMS', 2, 'vec_param', query_data.tobytes()).equal([0])

    expected_res = [10]
    # Expect to get result in reverse order to the id, starting from the max id in the index.
    for i in range(10):
        expected_res.append(str(index_size-i))
        expected_res.append(['__v_score', str(dim*i**2), 't', 'text value'])
    execute_hybrid_query(env, '(@t:(text value))=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)
    execute_hybrid_query(env, '(text value)=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)
    execute_hybrid_query(env, '("text value")=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)

    # Change the text value to 'other' for 20% of the vectors (with ids 5, 10, ..., index_size)
    for i in range(1, int(index_size/5) + 1):
        vector = create_np_array_typed([5*i] * dim, data_type)
        conn.execute_command('HSET', 5*i, 'v', vector.tobytes(), 't', 'other')

    # Expect to get only vector that passes the filter (i.e, has "other" in t field)
    expected_res = [10]
    for i in range(10):
        expected_res.append(str(index_size-5*i))
        expected_res.append(['__v_score', str(dim*(5*i)**2), 't', 'other'])
    execute_hybrid_query(env, '(other)=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)

    # Expect empty score for the intersection (disjoint sets of results)
    # The hybrid policy changes to ad hoc after the first batch
    execute_hybrid_query(env, '(@t:other text)=>[KNN 10 @v $vec_param]', query_data, 't',
                            hybrid_mode='HYBRID_BATCHES_TO_ADHOC_BF').equal([0])

    # Expect the same results as in above ('other AND NOT text')
    execute_hybrid_query(env, '(@t:other -text)=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)

    # Test with union - expect that all docs will pass the filter.
    expected_res = [10]
    for i in range(10):
        expected_res.append(str(index_size-i))
        expected_res.append(['__v_score', str(dim*i**2), 't', 'other' if i % 5 == 0 else 'text value'])
    execute_hybrid_query(env, '(@t:other|text)=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)

    # Expect for top 10 results from vector search that still has the original text "text value".
    expected_res = [10]
    res_count = 0
    for i in range(13):
        # The desired ids are the top 10 ids that do not divide by 5.
        if (index_size-i) % 5 == 0:
            continue
        expected_res.append(str(index_size-i))
        expected_res.append(['__v_score', str(dim*i**2), 't', 'text value'])
        res_count += 1
        if res_count == 10:
            break
    execute_hybrid_query(env, '(te*)=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)
    # This time the fuzzy matching should return only documents with the original 'text value'.
    execute_hybrid_query(env, '(%test%)=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)

    execute_hybrid_query(env, '(-(@t:other))=>[KNN 10 @v $vec_param]', query_data, 't').equal(expected_res)

    # Test with invalid wildcard (less than 2 chars before the wildcard)
    env.expect('FT.SEARCH', 'idx', '(t*)=>[KNN 10 @v $vec_param]', 'PARAMS', 2, 'vec_param', query_data.tobytes()).equal([0])
    # Intersect valid with invalid iterators in intersection (should return 0 results as well)
    env.expect('FT.SEARCH', 'idx', '(@t:t* @t:text)=>[KNN 10 @v $vec_param]', 'PARAMS', 2, 'vec_param', query_data.tobytes()).equal([0])


def test_hybrid_query_batches_mode_with_tags():
    # Set high GC threshold so to eliminate sanitizer warnings from of false leaks from forks (MOD-6229)
    env = Env(moduleArgs='DEFAULT_DIALECT 2 FORK_GC_CLEAN_THRESHOLD 10000')
    conn = getConnectionByEnv(env)
    # Index size is chosen so that batches mode will be selected by the heuristics.
    dim = 2
    index_size = 6000 * env.shardsCount
    data_type = random.choice(['FLOAT32', 'FLOAT64'])
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', data_type,
                        'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 'tags', 'TAG', 'text', 'TEXT')

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = create_np_array_typed([i]*dim, data_type)
        p.execute_command('HSET', i, 'v', vector.tobytes(), 'tags', 'hybrid', 'text', 'text')
    p.execute()

    query_data = create_np_array_typed([index_size/2]*dim, data_type)

    expected_res = [10]
    # Expect to get result which are around index_size/2, closer results will come before (secondary sorting by id).
    expected_res.extend([str(int(index_size/2)), ['__v_score', str(0), 'tags', 'hybrid']])
    for i in range(1, 10):
        expected_res.append(str(int(index_size/2 + (-1*(i+1)/2 if i % 2 else i/2))))
        expected_res.append(['__v_score', str((dim*(int((i+1)/2)**2))), 'tags', 'hybrid'])
    execute_hybrid_query(env, '(@tags:{hybrid})=>[KNN 10 @v $vec_param]', query_data, 'tags').equal(expected_res)
    execute_hybrid_query(env, '(@tags:{nothing})=>[KNN 10 @v $vec_param]', query_data, 'tags').equal([0])
    execute_hybrid_query(env, '(@tags:{hybrid} @text:hello)=>[KNN 10 @v $vec_param]', query_data, 'tags').equal([0])

    # Change the tag values to 'different, tag' for vectors with ids 5, 10, 20, ..., 6000)
    for i in range(1, int(index_size/5) + 1):
        vector = create_np_array_typed([5*i]*dim, data_type)
        conn.execute_command('HSET', 5*i, 'v', vector.tobytes(), 'tags', 'different, tag')

    expected_res = [10]
    # Expect to get result which are around index_size/2 that divide by 5, closer results
    # will come before (secondary sorting by id).
    expected_res.extend([str(int(index_size/2)), ['__v_score', str(0), 'tags', 'different, tag']])
    for i in range(1, 10):
        expected_res.append(str(int(index_size/2) + (-1*int((5*i+5)/2) if i % 2 else int(5*i/2))))
        expected_res.append(['__v_score', str(dim*(5*int((i+1)/2))**2), 'tags', 'different, tag'])
    execute_hybrid_query(env, '(@tags:{different})=>[KNN 10 @v $vec_param]', query_data, 'tags').equal(expected_res)
    # Expect for top 10 results from vector search that still has the original text "text value".
    expected_res = [10]
    res_count = 0
    for i in range(index_size):
        # The desired ids are the top 10 ids that do not divide by 5.
        if (int(index_size/2) + int((i+1)/2)) % 5 == 0:
            continue
        expected_res.append(str(int(index_size/2) + (-1*int((i+1)/2) if i % 2 else int(i/2))))
        expected_res.append(['__v_score', str(dim*int((i+1)/2)**2), 'tags', 'hybrid'])
        res_count += 1
        if res_count == 10:
            break
    execute_hybrid_query(env, '(@tags:{hybrid})=>[KNN 10 @v $vec_param]', query_data, 'tags').equal(expected_res)
    execute_hybrid_query(env, '(@tags:{hy*})=>[KNN 10 @v $vec_param]', query_data, 'tags').equal(expected_res)

    # Search with tag list. Expect that docs with 'hybrid' will have lower score (1 vs 2), since they are more frequent.
    expected_res = [10]
    expected_res.extend([str(int(index_size/2) - 5), '2', ['__v_score', str(dim*5**2), 'tags',  'different, tag'],
                            str(int(index_size/2)), '2', ['__v_score', str(0), 'tags',  'different, tag']])
    for i in range(1, 10):
        if i == 5:      # ids that divide by 5 were already inserted.
            continue
        expected_res.extend([str(int(index_size/2) - 5 + i), '1'])
        expected_res.append(['__v_score', str(dim*abs(5-i)**2), 'tags', 'hybrid'])
    execute_hybrid_query(env, '(@tags:{hybrid|tag})=>[KNN 10 @v $vec_param]', query_data, 'tags',
                            sort_by_vector=False, scorer='TFIDF').equal(expected_res)


def test_hybrid_query_with_numeric():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    index_size = 6000 * env.shardsCount
    data_type = random.choice(['FLOAT32', 'FLOAT64'])
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 1000, 'num', 'NUMERIC').ok()

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = create_np_array_typed([i]*dim, data_type)
        p.execute_command('HSET', i, 'v', vector.tobytes(), 'num', i)
    p.execute()

    query_data = create_np_array_typed([index_size]*dim, data_type)
    expected_res = [10]
    # Expect to get result in reverse order to the id, starting from the max id in the index.
    for i in range(10):
        expected_res.append(str(index_size-i))
        expected_res.append(['__v_score', str(dim*i**2), 'num', str(index_size-i)])

    execute_hybrid_query(env, f'(@num:[0 {index_size}])=>[KNN 10 @v $vec_param]', query_data, 'num').equal(expected_res)
    execute_hybrid_query(env, '(@num:[0 inf])=>[KNN 10 @v $vec_param]', query_data, 'num').equal(expected_res)

    # Expect that no result will pass the filter.
    execute_hybrid_query(env, '(@num:[0 0.5])=>[KNN 10 @v $vec_param]', query_data, 'num').equal([0])

    # Expect to get results with maximum numeric value of the top 100 id in the shard.
    lower_bound_num = 100 * env.shardsCount
    expected_res = [10]
    for i in range(10):
        expected_res.append(str(index_size-lower_bound_num-i))
        expected_res.append(['__v_score', str(dim*(lower_bound_num+i)**2), 'num', str(index_size-lower_bound_num-i)])
    # We switch from batches to ad-hoc BF mode during the run.
    execute_hybrid_query(env, f'(@num:[-inf {index_size - lower_bound_num}])=>[KNN 10 @v $vec_param]', query_data, 'num',
                            hybrid_mode='HYBRID_BATCHES_TO_ADHOC_BF').equal(expected_res)
    execute_hybrid_query(env, '(@num:[-inf {}] | @num:[{} {}])=>[KNN 10 @v $vec_param]'
                            .format(lower_bound_num, index_size-2*lower_bound_num, index_size-lower_bound_num), query_data, 'num',
                            hybrid_mode='HYBRID_BATCHES_TO_ADHOC_BF').equal(expected_res)

    # Expect for 5 results only (45-49), this will use ad-hoc BF since ratio between docs that pass the filter to
    # index size is low.
    expected_res = [5]
    expected_res.extend([str(50-i) for i in range(1, 6)])
    env.expect('FT.SEARCH', 'idx', '(@num:[45 (50])=>[KNN 10 @v $vec_param]',
                'SORTBY', '__v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes(), 'RETURN', 0).equal(expected_res)
    env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_ADHOC_BF')


def test_hybrid_query_with_geo():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    data_type = random.choice(VECSIM_DATA_TYPES)
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 'coordinate', 'GEO').ok()

    index_size = 1000   # for this index size, ADHOC BF mode will always be selected by the heuristics.
    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = create_np_array_typed([i/100]*dim, data_type)
        p.execute_command('HSET', i, 'v', vector.tobytes(), 'coordinate', str(i/100)+","+str(i/100))
    p.execute()
    if not env.isCluster():
        env.assertEqual(get_vecsim_index_size(env, 'idx', 'v'), index_size)

    query_data = create_np_array_typed([index_size/100]*dim, data_type)
    # Expect that ids 1-31 will pass the geo filter, and that the top 10 from these will return.
    expected_res = [10]
    for i in range(10):
        expected_res.append(str(31-i))
        expected_res.append(['coordinate', str((31-i)/100)+","+str((31-i)/100)])
    env.expect('FT.SEARCH', 'idx', '(@coordinate:[0.0 0.0 50 km])=>[KNN 10 @v $vec_param]',
                'SORTBY', '__v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes(), 'RETURN', 1, 'coordinate').equal(expected_res)

    # Expect that no results will pass the filter
    execute_hybrid_query(env, '(@coordinate:[-1.0 -1.0 1 m])=>[KNN 10 @v $vec_param]', query_data, 'coordinate',
                            hybrid_mode='HYBRID_ADHOC_BF').equal([0])


def test_hybrid_query_batches_mode_with_complex_queries():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 4
    index_size = 6000 * env.shardsCount
    data_type = random.choice(VECSIM_DATA_TYPES)
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', data_type,
                        'DIM', dimension, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 'num', 'NUMERIC',
                        't1', 'TEXT', 't2', 'TEXT')

    p = conn.pipeline(transaction=False)
    close_vector = create_np_array_typed([1]*dimension, data_type)
    distant_vector = create_np_array_typed([10]*dimension, data_type)
    conn.execute_command('HSET', 1, 'v', close_vector.tobytes(), 'num', 1, 't1', 'text value', 't2', 'hybrid query')
    conn.execute_command('HSET', 2, 'v', distant_vector.tobytes(), 'num', 2, 't1', 'text value', 't2', 'hybrid query')
    conn.execute_command('HSET', 3, 'v', distant_vector.tobytes(), 'num', 3, 't1', 'other', 't2', 'hybrid query')
    conn.execute_command('HSET', 4, 'v', close_vector.tobytes(), 'num', 4, 't1', 'other', 't2', 'hybrid query')
    for i in range(5, index_size+1):
        further_vector = create_np_array_typed([i]*dimension, data_type)
        p.execute_command('HSET', i, 'v', further_vector.tobytes(), 'num', i, 't1', 'text value', 't2', 'hybrid query')
    p.execute()
    expected_res_1 = [2, '1', '5']
    # Search for the "close_vector" that some the vector in the index contain. The batch of vectors should start with
    # ids 1, 4. The intersection "child iterator" has two children - intersection iterator (@t2:(hybrid query))
    # and not iterator (-@t1:other). When the hybrid iterator will perform "skipTo(4)" for the child iterator,
    # the inner intersection iterator will skip to 4, but the not iterator will stay at 2 (4 is not a valid id).
    # The child iterator will point to 2, and return NOT_FOUND. This test makes sure that the hybrid iterator can
    # handle this situation (without going into infinite loop).
    env.expect('FT.SEARCH', 'idx', '(@t2:(hybrid query) -@t1:other)=>[KNN 2 @v $vec_param]',
                'SORTBY', '__v_score', 'LIMIT', 0, 2,
                'PARAMS', 2, 'vec_param', close_vector.tobytes(),
                'RETURN', 0).equal(expected_res_1)
    env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')

    # test modifier list
    expected_res_2 = [10, '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']
    env.expect('FT.SEARCH', 'idx', '(@t1|t2:(value text) @num:[10 30])=>[KNN 10 @v $vec_param]',
                'SORTBY', '__v_score',
                'PARAMS', 2, 'vec_param', close_vector.tobytes(),
                'RETURN', 0).equal(expected_res_2)

    # test with query attributes
    env.expect('FT.SEARCH', 'idx', '(@t1|t2:(value text)=>{$inorder: true} @num:[10 30])=>[KNN 10 @v $vec_param]',
                'SORTBY', '__v_score',
                'WITHSCORES',
                'PARAMS', 2, 'vec_param', close_vector.tobytes(),
                'RETURN', 2, 't1', 't2').equal([0])


def test_hybrid_query_non_vector_score():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 128
    qty = 100
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
                        'DIM', dimension, 'DISTANCE_METRIC', 'L2', 't', 'TEXT')
    load_vectors_with_texts_into_redis(conn, 'v', dimension, qty)

    # Change the text value to 'other' for 10 vectors (with id 10, 20, ..., 100)
    for i in range(1, 11):
        vector = np.float32([10*i for j in range(dimension)])
        conn.execute_command('HSET', 10*i, 'v', vector.tobytes(), 't', 'other')

    query_data = np.float32([qty for j in range(dimension)])

    # All documents should match, so TOP 10 takes the 10 with the largest ids. Since we sort by default score
    # and "value" is optional, expect that 100 will come first, and the rest will be sorted by id in ascending order.
    expected_res_1 = [10,
                      '100', '3', ['__v_score', '0', 't', 'other'],
                      '91', '2', ['__v_score', '10368', 't', 'text value'],
                      '92', '2', ['__v_score', '8192', 't', 'text value'],
                      '93', '2', ['__v_score', '6272', 't', 'text value'],
                      '94', '2', ['__v_score', '4608', 't', 'text value'],
                      '95', '2', ['__v_score', '3200', 't', 'text value'],
                      '96', '2', ['__v_score', '2048', 't', 'text value'],
                      '97', '2', ['__v_score', '1152', 't', 'text value'],
                      '98', '2', ['__v_score', '512', 't', 'text value'],
                      '99', '2', ['__v_score', '128', 't', 'text value']]
    execute_hybrid_query(env, '((text ~"value")|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False,
                         hybrid_mode='HYBRID_ADHOC_BF', scorer='TFIDF').equal(expected_res_1)
    execute_hybrid_query(env, '((text ~"value")|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False,
                         sort_by_non_vector_field=True, hybrid_mode='HYBRID_ADHOC_BF', scorer='TFIDF').equal(expected_res_1)

    # Same as above, but here we use fuzzy for 'text'
    expected_res_2 = [10,
                      '100', '3', ['__v_score', '0', 't', 'other'],
                      '91', '1', ['__v_score', '10368', 't', 'text value'],
                      '92', '1', ['__v_score', '8192', 't', 'text value'],
                      '93', '1', ['__v_score', '6272', 't', 'text value'],
                      '94', '1', ['__v_score', '4608', 't', 'text value'],
                      '95', '1', ['__v_score', '3200', 't', 'text value'],
                      '96', '1', ['__v_score', '2048', 't', 'text value'],
                      '97', '1', ['__v_score', '1152', 't', 'text value'],
                      '98', '1', ['__v_score', '512', 't', 'text value'],
                      '99', '1', ['__v_score', '128', 't', 'text value']]
    execute_hybrid_query(env, '(%test%|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False,
                         hybrid_mode='HYBRID_ADHOC_BF', scorer='TFIDF').equal(expected_res_2)
    execute_hybrid_query(env, '(%test%|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False,
                         sort_by_non_vector_field=True, hybrid_mode='HYBRID_ADHOC_BF', scorer='TFIDF').equal(expected_res_2)

    # use TFIDF.DOCNORM scorer
    expected_res_3 = [10,
                      '100', '3', ['__v_score', '0', 't', 'other'],
                      '91', '0.5', ['__v_score', '10368', 't', 'text value'],
                      '92', '0.5', ['__v_score', '8192', 't', 'text value'],
                      '93', '0.5', ['__v_score', '6272', 't', 'text value'],
                      '94', '0.5', ['__v_score', '4608', 't', 'text value'],
                      '95', '0.5', ['__v_score', '3200', 't', 'text value'],
                      '96', '0.5', ['__v_score', '2048', 't', 'text value'],
                      '97', '0.5', ['__v_score', '1152', 't', 'text value'],
                      '98', '0.5', ['__v_score', '512', 't', 'text value'],
                      '99', '0.5', ['__v_score', '128', 't', 'text value']]
    res = env.cmd('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'TFIDF.DOCNORM', 'WITHSCORES',
               'PARAMS', 2, 'vec_param', query_data.tobytes(),
               'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10)
    compare_lists(env, res, expected_res_3, delta=0.01)

    # Those scorers are scoring per shard.
    if not env.isCluster():
        # use BM25 scorer
        expected_res_4 = [10, '100', '1.0489510218434552', ['__v_score', '0', 't', 'other'], '91', '0.34965034061448513', ['__v_score', '10368', 't', 'text value'], '92', '0.34965034061448513', ['__v_score', '8192', 't', 'text value'], '93', '0.34965034061448513', ['__v_score', '6272', 't', 'text value'], '94', '0.34965034061448513', ['__v_score', '4608', 't', 'text value'], '95', '0.34965034061448513', ['__v_score', '3200', 't', 'text value'], '96', '0.34965034061448513', ['__v_score', '2048', 't', 'text value'], '97', '0.34965034061448513', ['__v_score', '1152', 't', 'text value'], '98', '0.34965034061448513', ['__v_score', '512', 't', 'text value'], '99', '0.34965034061448513', ['__v_score', '128', 't', 'text value']]
        res = env.cmd('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'BM25', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10)
        compare_lists(env, res, expected_res_4, delta=0.01)

        # use BM25STD scorer
        expected_res_5 = [10, '100', '2.8811302846039606', ['__v_score', '0', 't', 'other'], '91', '0.005061343269334967', ['__v_score', '10368', 't', 'text value'], '92', '0.005061343269334967', ['__v_score', '8192', 't', 'text value'], '93', '0.005061343269334967', ['__v_score', '6272', 't', 'text value'], '94', '0.005061343269334967', ['__v_score', '4608', 't', 'text value'], '95', '0.005061343269334967', ['__v_score', '3200', 't', 'text value'], '96', '0.005061343269334967', ['__v_score', '2048', 't', 'text value'], '97', '0.005061343269334967', ['__v_score', '1152', 't', 'text value'], '98', '0.005061343269334967', ['__v_score', '512', 't', 'text value'], '99', '0.005061343269334967', ['__v_score', '128', 't', 'text value']]
        res = env.cmd('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'BM25STD', 'WITHSCORES',
                  'PARAMS', 2, 'vec_param', query_data.tobytes(),
                  'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10)
        compare_lists(env, res, expected_res_5, delta=0.01)

        # use DISMAX scorer
        expected_res_6 = [10, '91', '1', ['__v_score', '10368', 't', 'text value'], '92', '1', ['__v_score', '8192', 't', 'text value'], '93', '1', ['__v_score', '6272', 't', 'text value'], '94', '1', ['__v_score', '4608', 't', 'text value'], '95', '1', ['__v_score', '3200', 't', 'text value'], '96', '1', ['__v_score', '2048', 't', 'text value'], '97', '1', ['__v_score', '1152', 't', 'text value'], '98', '1', ['__v_score', '512', 't', 'text value'], '99', '1', ['__v_score', '128', 't', 'text value'], '100', '1', ['__v_score', '0', 't', 'other']]
        env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'DISMAX', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10).equal(expected_res_6)

        # use DOCSCORE scorer
        env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'DOCSCORE', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 100).equal(expected_res_6)

@skip(cluster=False)
def test_single_entry():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 MIN_OPERATION_WORKERS 0')
    # This test should test 3 shards with only one entry. 2 shards should return an empty response to the coordinator.
    # Execution should finish without failure.
    conn = getConnectionByEnv(env)
    dimension = 128
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2').ok()
    vector = np.random.rand(1, dimension).astype(np.float32)
    conn.execute_command('HSET', 0, 'v', vector.tobytes())

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 10 @v $vec_param]',
                'SORTBY', '__v_score',
                'RETURN', '0',
                'PARAMS', 2, 'vec_param', vector.tobytes()).equal([1, '0'])


def test_hybrid_query_adhoc_bf_mode():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 MIN_OPERATION_WORKERS 0')
    conn = getConnectionByEnv(env)
    dimension = 128
    qty = 100

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
                'DIM', dimension, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 't', 'TEXT').ok()
    load_vectors_with_texts_into_redis(conn, 'v', dimension, qty)

    # Change the text value to 'other' for 10 vectors (with id 10, 20, ..., 100)
    for i in range(1, 11):
        vector = create_np_array_typed([10*i]*dimension)
        conn.execute_command('HSET', 10*i, 'v', vector.tobytes(), 't', 'other')

    # Expect to get only vector that passes the filter (i.e, has "other" in text field)
    # Expect also that heuristics will choose adhoc BF over batches.
    query_data = create_np_array_typed([100]*dimension)

    expected_res = [10,
                    '100', ['__v_score', '0', 't', 'other'],
                    '90', ['__v_score', '12800', 't', 'other'],
                    '80', ['__v_score', '51200', 't', 'other'],
                    '70', ['__v_score', '115200', 't', 'other'],
                    '60', ['__v_score', '204800', 't', 'other'],
                    '50', ['__v_score', '320000', 't', 'other'],
                    '40', ['__v_score', '460800', 't', 'other'],
                    '30', ['__v_score', '627200', 't', 'other'],
                    '20', ['__v_score', '819200', 't', 'other'],
                    '10', ['__v_score', '1036800', 't', 'other']]

    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        execute_hybrid_query(env, '(other)=>[KNN 10 @v $vec_param]', query_data, 't',
                                hybrid_mode='HYBRID_ADHOC_BF').equal(expected_res)


def test_wrong_vector_size():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 128

    for data_type in VECSIM_DATA_TYPES:
        vector = create_np_array_typed(np.random.rand(1+dimension), data_type)

        conn.execute_command('HSET', '0', 'v', vector[:dimension-1].tobytes())
        conn.execute_command('HSET', '1', 'v', vector[:dimension].tobytes())
        conn.execute_command('HSET', '2', 'v', vector[:dimension+1].tobytes())

        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', data_type, 'DIM', dimension, 'DISTANCE_METRIC', 'L2').ok()
        waitForIndex(env, 'idx')

        vector = create_np_array_typed(np.random.rand(1+dimension), data_type)
        conn.execute_command('HSET', '3', 'v', vector[:dimension-1].tobytes())
        conn.execute_command('HSET', '4', 'v', vector[:dimension].tobytes())
        conn.execute_command('HSET', '5', 'v', vector[:dimension+1].tobytes())

        waitForIndex(env, 'idx')
        assertInfoField(env, 'idx', 'num_docs', 2)
        assertInfoField(env, 'idx', 'hash_indexing_failures', 4)
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 6 @v $q]', 'NOCONTENT', 'PARAMS', 2, 'q',
                   create_np_array_typed([1]*dimension, data_type).tobytes()).equal([2, '1', '4'])

        conn.execute_command('FLUSHALL')


def test_hybrid_query_cosine():
    # Set high GC threshold so to eliminate sanitizer warnings from of false leaks from forks (MOD-6229)
    env = Env(moduleArgs='DEFAULT_DIALECT 2 FORK_GC_CLEAN_THRESHOLD 10000')
    conn = getConnectionByEnv(env)
    dim = 4
    index_size = 6000 * env.shardsCount
    data_type = random.choice(['FLOAT32', 'FLOAT64'])
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'COSINE', 't', 'TEXT').ok()

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        first_coordinate = create_np_array_typed([float(i)/index_size], data_type)
        vector = np.concatenate((first_coordinate, create_np_array_typed([1]*(dim-1), data_type)))
        p.execute_command('HSET', i, 'v', vector.tobytes(), 't', 'text value')
    p.execute()

    query_data = create_np_array_typed([1]*dim, data_type)

    expected_res_ids = [str(index_size-i) for i in range(15)]
    res = conn.execute_command('FT.SEARCH', 'idx', '(text value)=>[KNN 10 @v $vec_param]',
                                'SORTBY', '__v_score',
                                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                                'RETURN', 0)
    debug_info = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))
    env.assertEqual(debug_info['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')
    actual_res_ids = [res[1:][i] for i in range(10)]
    if data_type == 'FLOAT64':
        # for FLOAT64, expect to get better accuracy
        env.assertEqual(actual_res_ids, expected_res_ids[:10])
    else:
        # The order of ids is not accurate due to floating point numeric errors, but the top k should be
        # in the last 15 ids.
        for res_id in actual_res_ids:
            env.assertContains(res_id, expected_res_ids)

    # Change the text value to 'other' for 10 vectors (with id 10, 20, ..., index_size)
    for i in range(1, int(index_size/10) + 1):
        first_coordinate = create_np_array_typed([float(10*i)/index_size], data_type)
        vector = np.concatenate((first_coordinate, create_np_array_typed([1]*(dim-1), data_type)))
        conn.execute_command('HSET', 10*i, 'v', vector.tobytes(), 't', 'other')

    # Expect to get only vector that passes the filter (i.e, has "other" in text field)
    expected_res_ids = [str(index_size-10*i) for i in range(10)]
    res = conn.execute_command('FT.SEARCH', 'idx', '(other)=>[KNN 10 @v $vec_param]',
                                'SORTBY', '__v_score',
                                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                                'RETURN', 0)
    debug_info = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))
    env.assertEqual(debug_info['LAST_SEARCH_MODE'], 'HYBRID_ADHOC_BF')
    actual_res_ids = [res[1:][i] for i in range(10)]
    if data_type == 'FLOAT64':
        # for FLOAT64, expect to get better accuracy
        env.assertEqual(actual_res_ids, expected_res_ids)
    else:
        for res_id in actual_res_ids:
            env.assertContains(res_id, expected_res_ids)


def test_ft_aggregate_basic():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    dim = 1
    conn = getConnectionByEnv(env)

    for algo in VECSIM_ALGOS:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', algo, '6', 'TYPE', 'FLOAT32',
                            'DIM', dim, 'DISTANCE_METRIC', 'L2', 'n', 'NUMERIC')

        # Use {1} and {3} hash slot to verify the distribution of the documents among 2 different shards.
        for i in range(1, 11, 2):
            conn.execute_command("HSET", f'doc{i}{{1}}', "v", create_np_array_typed([i] * dim).tobytes(), 'n', f'{11-i}')

        for i in range(2, 11, 2):
            conn.execute_command("HSET", f'doc{i}{{3}}', "v", create_np_array_typed([i] * dim).tobytes(), 'n', f'{11-i}')

        # Expect both queries to return doc1, doc2 and doc3, as these are the closest 3 documents in terms of
        # the vector fields, and the ones with distance lower than 10.
        expected_res = [['dist', '1'], ['dist', '4'], ['dist', '9']]

        query = "*=>[KNN 3 @v $BLOB]=>{$yield_distance_as: dist}"
        res = conn.execute_command("FT.AGGREGATE", "idx", query,
                                       "PARAMS", 2, "BLOB", create_np_array_typed([0] * dim).tobytes())
        env.assertEqual(res[1:], expected_res)

        # For range query we explicitly yield the distance metric and sort by it, as it wouldn't be
        # the case in default, unlike in KNN.
        query = "@v:[VECTOR_RANGE 10 $BLOB]=>{$yield_distance_as: dist}"
        res = conn.execute_command("FT.AGGREGATE", "idx", query, 'SORTBY', '1', '@dist',
                                   "PARAMS", 2, "BLOB", create_np_array_typed([0] * dim).tobytes())
        env.assertEqual(res[1:], expected_res)

        # Test simple hybrid query - get results with n value between 0 and 5, that is ids 6-10. The top 3 among those
        # are doc6, doc7 and doc8 (where the dist is id**2).
        query = "(@n:[0 5])=>[KNN 3 @v $BLOB]=>{$yield_distance_as: dist}"
        res = conn.execute_command("FT.AGGREGATE", "idx", query, 'SORTBY', '1', '@dist',
                                       "PARAMS", 2, "BLOB", create_np_array_typed([0] * dim).tobytes())
        expected_res = [['dist', '36'], ['dist', '49'], ['dist', '64']]
        env.assertEqual(res[1:], expected_res)

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')


def test_fail_on_v1_dialect():
    env = Env(moduleArgs='DEFAULT_DIALECT 1')
    dim = 1
    conn = getConnectionByEnv(env)
    one_vector = np.full((1, 1), 1, dtype = np.float32)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
                        'DIM', dim, 'DISTANCE_METRIC', 'COSINE')
    conn.execute_command("HSET", "i", "v", one_vector.tobytes())
    for query in ["*=>[KNN 10 @v $BLOB]", "@v:[VECTOR_RANGE 10 $BLOB"]:
        res = env.expect("FT.SEARCH", "idx", query, "PARAMS", 2, "BLOB", one_vector.tobytes())
        res.error().contains("Syntax error")


def test_hybrid_query_with_global_filters():
    # Set high GC threshold so to eliminate sanitizer warnings from of false leaks from forks (MOD-6229)
    env = Env(moduleArgs='DEFAULT_DIALECT 2 FORK_GC_CLEAN_THRESHOLD 10000')
    conn = getConnectionByEnv(env)
    dim = 2
    index_size = 1000
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 't', 'TEXT', 'num', 'NUMERIC', 'coordinate', 'GEO').ok()

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = np.full(dim, i, dtype='float32')
        p.execute_command('HSET', i, 'v', vector.tobytes(), 't', 'hybrid', 'num', i, 'coordinate',
                             str(i/100)+","+str(i/100))
    p.execute()
    if not env.isCluster():
        env.assertEqual(get_vecsim_index_size(env, 'idx', 'v'), index_size)
    query_data = np.full(dim, index_size, dtype='float32')

    # Run VecSim query in KNN mode (non-hybrid), and expect to find only one result (with key=index_size-2).
    inkeys = [index_size-2]
    expected_res = [1, str(index_size-2), ['__v_score', str(dim*2**2)]]
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 10 @v $vec_param]', 'INKEYS', len(inkeys), *inkeys,
               'RETURN', 1, '__v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes()).equal(expected_res)

    # Change the text value to 'other' for 20% of the vectors (with ids 5, 10, ..., index_size)
    for i in range(1, int(index_size/5) + 1):
        vector = np.full(dim, 5*i, dtype='float32')
        conn.execute_command('HSET', 5*i, 'v', vector.tobytes(), 't', 'other')

    # Run VecSim query in hybrid mode, expect to get only the vectors that passes the filters
    # (index_size-10 and index_size-100, since they has "other" in its 't' field and its id is in inkeys list).
    inkeys = [index_size-2, index_size-10, index_size-100]
    expected_res = [2, str(index_size-100), ['__v_score', str(dim*100**2)], str(index_size-10), ['__v_score', str(dim*10**2)]]
    env.expect('FT.SEARCH', 'idx', '(other)=>[KNN 10 @v $vec_param]', 'INKEYS', len(inkeys), *inkeys,
               'RETURN', 1, '__v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes()).equal(expected_res)


def test_hybrid_query_change_policy():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    n = 6000 * env.shardsCount
    data_type = random.choice(VECSIM_DATA_TYPES)
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)
    np.random.seed(10)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'COSINE', 'tag1', 'TAG', 'tag2', 'TAG').ok()

    file = 1
    tags = range(10)
    subset_size = int(n/2)
    with conn.pipeline(transaction=False) as p:
        for i in range(subset_size):
            v = create_np_array_typed(np.random.rand(dim), data_type)
            p.execute_command('HSET', i, 'v', v.tobytes(),
                              'tag1', str(tags[randrange(10)]), 'tag2', 'word'+str(file))
        p.execute()

    file = 2
    with conn.pipeline(transaction = False) as p:
        for i in range(subset_size, n):
            v = create_np_array_typed(np.random.rand(dim), data_type)
            conn.execute_command('HSET', i + subset_size, 'v', v.tobytes(),
                                 'tag1', str(10 + tags[randrange(10)]), 'tag2', 'word'+str(file))
        p.execute()

    # This should return 10 results and run in HYBRID_BATCHES mode
    query_string = '(@tag1:{0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9} @tag2:{word1})=>[KNN 10 @v $vec_param]'
    query_vec = create_np_array_typed(np.random.rand(dim), data_type)
    res = execute_hybrid_query(env, query_string, query_vec, 'tag2', hybrid_mode='HYBRID_BATCHES').res
    env.assertEqual(res[0], 10)

    # Ask explicitly to use AD-HOC policy.
    query_string = '(@tag1:{0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9} @tag2:{word1})=>[KNN 10 @v $vec_param HYBRID_POLICY ADHOC_BF]'
    adhoc_res = execute_hybrid_query(env, query_string, query_vec, 'tag2', hybrid_mode='HYBRID_ADHOC_BF').res

    # Validate that the same scores are back for the top k results (not necessarily the exact same ids)
    for i, res_fields in enumerate(res[2::2]):
        env.assertEqual(res_fields, adhoc_res[2+2*i])

    # This query has 0 results, since none of the tags in @tag1 go along with 'word2' in @tag2.
    # However, the estimated number of the "child" results should be index_size/2.
    # While running the batches, the policy should change dynamically to AD-HOC BF.
    query_string = '(@tag1:{0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9} @tag2:{word2})=>[KNN 10 @v $vec_param]'
    execute_hybrid_query(env, query_string, query_vec, 'tag2',
                            hybrid_mode='HYBRID_BATCHES_TO_ADHOC_BF').equal([0])
    # Ask explicitly to use AD-HOC policy.
    query_string_adhoc_bf = '(@tag1:{0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9} @tag2:{word2})=>[KNN 10 @v $vec_param]=>{$HYBRID_POLICY: ADHOC_BF}'
    execute_hybrid_query(env, query_string_adhoc_bf, query_vec, 'tag2', hybrid_mode='HYBRID_ADHOC_BF').equal([0])

    # Add one valid document and re-run the query (still expect to change to AD-HOC BF)
    # This doc should return in the first batch, and then it is removed and reinserted to the results heap
    # after the policy is changed to ad-hoc.
    conn.execute_command('HSET', n, 'v', query_vec.tobytes(),
                            'tag1', str(1), 'tag2', 'word'+str(file))
    res = execute_hybrid_query(env, query_string, query_vec, 'tag2', hybrid_mode='HYBRID_BATCHES_TO_ADHOC_BF').res
    env.assertEqual(res[:2], [1, str(n)])


def test_system_memory_limits():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    system_memory = int(env.cmd('info', 'memory')['total_system_memory'])
    currIdx = 0
    dim = 16
    type_size = 4
    data_type = 'FLOAT32'

    # Test that huge BLOCK_SIZE is ignored in FLAT and huge INITIAL_CAP is ignored in FLAT and in HNSW (both deprecated)
    env.assertOk(conn.execute_command('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', data_type,
                                      'DIM', dim, 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', system_memory, 'BLOCK_SIZE', system_memory))
    currIdx+=1

    env.assertOk(conn.execute_command('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', data_type,
                                      'DIM', dim, 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', system_memory))
    currIdx+=1


    # Test cases where maxBlockSize becomes zero due to high element size

    # Case 1: High dimension causing maxBlockSize to be zero for all algorithms
    # Calculate a dimension that would cause element size to exceed memory limits
    # For FLAT: element_size = dim * type_size
    # For HNSW: element_size = dim * type_size + graph_overhead (M * 2 * sizeof(idType) + metadata)
    # For SVS: element_size = dim * type_size + graph_overhead (graph_max_degree * sizeof(uint32_t))

    memory_limit = system_memory // 10  # BLOCK_MEMORY_LIMIT is 10% of system memory

    # High dimension test - should fail for all algorithms
    high_dim = (memory_limit // type_size) + 1000  # Ensure element size exceeds limit

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
               'DIM', high_dim, 'DISTANCE_METRIC', 'L2').error().contains(
               'Vector index element size')
    currIdx+=1

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', data_type,
               'DIM', high_dim, 'DISTANCE_METRIC', 'L2').error().contains(
               'Vector index element size')
    currIdx+=1

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', data_type,
               'DIM', high_dim, 'DISTANCE_METRIC', 'L2').error().contains(
               'Vector index element size')
    currIdx+=1

    # Case 2: High M parameter in HNSW causing maxBlockSize to be zero
    # HNSW element size includes: dim * type_size + sizeof(ElementGraphData) + sizeof(idType) * M * 2
    # Calculate M that would cause element size to exceed memory limits
    base_element_size = dim * type_size + 64  # Approximate overhead for ElementGraphData and metadata
    remaining_memory = memory_limit - base_element_size
    if remaining_memory > 0:
        high_M = (remaining_memory // (4 * 2)) + 1000  # sizeof(idType) = 4, M * 2 connections

        env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', data_type,
                   'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', high_M).error().contains(
                   'Vector index element size')
        currIdx+=1

    # Case 3: High graph_max_degree in SVS-VAMANA causing maxBlockSize to be zero
    # SVS element size includes: dim * type_size + sizeof(uint32_t) * (graph_max_degree + 1)
    base_element_size = dim * type_size
    remaining_memory = memory_limit - base_element_size
    if remaining_memory > 0:
        high_graph_degree = (remaining_memory // 4) + 1000  # sizeof(uint32_t) = 4

        env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '8', 'TYPE', data_type,
                   'DIM', dim, 'DISTANCE_METRIC', 'L2', 'GRAPH_MAX_DEGREE', high_graph_degree).error().contains(
                   'Vector index element size')
        currIdx+=1

    conn.execute_command("FLUSHALL")


@skip(cluster=True)
def test_redisearch_memory_limit():
    # test element size with VSS_MAX_RESIZE configure
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    used_memory = int(conn.execute_command('info', 'memory')['used_memory'])
    maxmemory = used_memory * 5
    conn.execute_command('CONFIG SET', 'maxmemory', maxmemory)
    currIdx = 0
    data_type = 'FLOAT32'
    data_byte_size = 4

    # Calculate dimension that will cause element size to exceed 10% memory limit
    # BLOCK_MEMORY_LIMIT = maxmemory / 10 (default 10% of system memory)
    memory_limit_10_percent = maxmemory // 10
    # For FLAT: element_size = dim * data_byte_size
    # We want: element_size > memory_limit_10_percent
    dim = (memory_limit_10_percent // data_byte_size) + 1000  # Dimension that exceeds 10% limit

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2').error().contains('Vector index element size')
    currIdx+=1

    env.expect(config_cmd(), 'SET', 'VSS_MAX_RESIZE', maxmemory).ok()

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    currIdx+=1

    # reset env (for clean RLTest run with env reuse)
    env.expect(config_cmd(), 'SET', 'VSS_MAX_RESIZE', '0').ok()
    env.assertTrue(conn.execute_command('CONFIG SET', 'maxmemory', '0'))

@skip(cluster=True)
def test_rdb_memory_limit():
    # test element size with VSS_MAX_RESIZE configure
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    used_memory = int(conn.execute_command('info', 'memory')['used_memory'])
    maxmemory = used_memory * 5
    idx = "idx"
    data_type = 'FLOAT32'
    data_byte_size = 4

    # Calculate dimension that will cause element size to exceed 10% memory limit
    # BLOCK_MEMORY_LIMIT = maxmemory / 10 (default 10% of system memory)
    memory_limit_10_percent = maxmemory // 10
    # For FLAT: element_size = dim * data_byte_size
    # We want: element_size > memory_limit_10_percent
    dim = (memory_limit_10_percent // data_byte_size) + 1000  # Dimension that exceeds 10% limit

    env.expect('FT.CREATE', idx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    # Try reload with current max memory
    env.dumpAndReload()

    # assert that index was loaded successfully and contains the vector field using ft.info
    assertInfoField(env, idx, 'attributes',
                    [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'algorithm', 'FLAT', 'data_type', 'FLOAT32', 'dim', dim, 'distance_metric', 'L2']])

    # The actual test: try creating indexes from rdb.
    # should fail since vector index element size exceeds BLOCK_MEMORY_LIMIT and creating index will fail the loading.
    conn.execute_command('CONFIG SET', 'maxmemory', maxmemory)
    try:
        env.dumpAndReload()
        env.assertTrue(False, message='Expected to fail')
    except ResponseError as e:
        env.assertContains('Error trying to load the RDB dump', str(e))

    # reset env (for clean RLTest run with env reuse)
    env.assertTrue(conn.execute_command('CONFIG SET', 'maxmemory', '0'))


class TestTimeoutReached(object):
    def __init__(self):
        if SANITIZER:
            raise SkipTest()
        self.env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT FAIL')
        n_shards = self.env.shardsCount
        self.index_sizes = {'FLAT': 80000 * n_shards, 'HNSW': 10000 * n_shards, 'SVS-VAMANA': 10000 * n_shards}
        self.hybrid_modes = ['BATCHES', 'ADHOC_BF']
        self.dim = 10
        self.type = 'FLOAT32'

    def tearDown(self): # cleanup after each test
        self.env.flush()

    def run_long_queries(self, n_vec, query_vec):
        # STANDARD KNN
        # run query with no timeout. should succeed.
        res = self.env.cmd('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_vec,
                                   'PARAMS', 4, 'K', n_vec, 'vec_param', query_vec.tobytes(),
                                   'TIMEOUT', 0)
        self.env.assertEqual(res[0], n_vec)
        # run query with 1 millisecond timeout. should fail.
        self.env.expect(
            'FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]',
            'NOCONTENT', 'LIMIT', 0, n_vec, 'PARAMS', 4, 'K', n_vec,
            'vec_param', query_vec.tobytes(), 'TIMEOUT', 1
        ).error().contains('Timeout limit was reached')

        # RANGE QUERY
        # run query with no timeout. should succeed.
        res = self.env.cmd('FT.SEARCH', 'idx', '@vector:[VECTOR_RANGE 10000 $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_vec,
                                   'PARAMS', 2,  'vec_param', query_vec.tobytes(),
                                   'TIMEOUT', 0)
        self.env.assertEqual(res[0], n_vec)
        # run query with 1 millisecond timeout. should fail.
        self.env.expect('FT.SEARCH', 'idx', '@vector:[VECTOR_RANGE 10000 $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_vec,
                   'PARAMS', 2, 'vec_param', query_vec.tobytes(),
                   'TIMEOUT', 1).error().contains('Timeout limit was reached')

        # HYBRID MODES
        for mode in self.hybrid_modes:
            res = self.env.cmd('FT.SEARCH', 'idx', '(-dummy)=>[KNN $K @vector $vec_param HYBRID_POLICY $hp]',
                               'NOCONTENT', 'LIMIT', 0, n_vec, 'PARAMS', 6, 'K', n_vec, 'vec_param',
                               query_vec.tobytes(), 'hp', mode, 'TIMEOUT', 0)
            self.env.assertEqual(res[0], n_vec)

            self.env.expect(
                'FT.SEARCH', 'idx', '(-dummy)=>[KNN $K @vector $vec_param HYBRID_POLICY $hp]',
                'NOCONTENT', 'LIMIT', 0, n_vec, 'PARAMS', 6, 'K', n_vec,
                'vec_param', query_vec.tobytes(), 'hp', mode, 'TIMEOUT', 1
            ).error().contains('Timeout limit was reached')

    def test_flat(self):
        # Create index and load vectors.
        n_vec = self.index_sizes['FLAT']
        query_vec = load_vectors_to_redis(self.env, n_vec, 0, self.dim, self.type)
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'FLAT', '8', 'TYPE', self.type,
                    'DIM', self.dim, 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', n_vec).ok()
        waitForIndex(self.env, 'idx')

        self.run_long_queries(n_vec, query_vec)

    def test_hnsw(self):
        # Create index and load vectors.
        n_vec = self.index_sizes['HNSW']
        query_vec = load_vectors_to_redis(self.env, n_vec, 0, self.dim, self.type)
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', self.type,
                        'DIM', self.dim, 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', n_vec).ok()
        waitForIndex(self.env, 'idx')

        self.run_long_queries(n_vec, query_vec)

    def test_svs(self):
        # Create index and load vectors.
        n_vec = self.index_sizes['SVS-VAMANA']
        query_vec = load_vectors_to_redis(self.env, n_vec, 0, self.dim, self.type)
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', self.type,
                        'DIM', self.dim, 'DISTANCE_METRIC', 'L2').ok()
        waitForIndex(self.env, 'idx')

        self.run_long_queries(n_vec, query_vec)

@skip(no_json=True)
def test_create_multi_value_json():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 4
    multi_paths = ['$..vec', '$.vecs[*]', '$.*.vec']
    single_paths = ['$.path.to.vec', '$.vecs[0]']

    path = 'not a valid path'
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', path, 'AS', 'vec', 'VECTOR', 'FLAT',
               '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',).error().equal(
                f"Invalid JSONPath '{path}' in attribute 'vec' in index 'idx'")

    for algo in VECSIM_ALGOS:
        for path in multi_paths:
            conn.flushall()
            #TODO: enable when SVS-VAMANA supports multi value
            if algo == 'SVS-VAMANA':
                env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', path, 'AS', 'vec', 'VECTOR', algo,
                           '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',).error().contains('Multi-value index is currently not supported for SVS-VAMANA algorithm')
            else:
                env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', path, 'AS', 'vec', 'VECTOR', algo,
                           '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',).ok()
                env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "vec"))['IS_MULTI_VALUE'], 1, message=f'{algo}, {path}')

        for path in single_paths:
            conn.flushall()
            env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', path, 'AS', 'vec', 'VECTOR', algo,
                       '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',).ok()
            env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "vec"))['IS_MULTI_VALUE'], 0, message=f'{algo}, {path}')

@skip(no_json=True)
def test_index_multi_value_json():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 MIN_OPERATION_WORKERS 0')
    conn = getConnectionByEnv(env)
    dim = 4
    n = 100
    per_doc = 5

    for data_t in VECSIM_DATA_TYPES:
        conn.flushall()
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
            '$.vecs[*]', 'AS', 'hnsw', 'VECTOR', 'HNSW', '6', 'TYPE', data_t, 'DIM', dim, 'DISTANCE_METRIC', 'L2',
            '$.vecs[*]', 'AS', 'flat', 'VECTOR', 'FLAT', '6', 'TYPE', data_t, 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

        for i in range(0, n, 2):
            # Test setting vectors with python list
            conn.json().set(i, '.', {'vecs': [[i + j] * dim for j in range(per_doc)]})
            # Test setting vectors with numpy array of the same type
            conn.json().set(i + 1, '.', {'vecs': [
                create_np_array_typed([i + 1 + j] * dim, data_t).tolist() for j in range(per_doc)]})

        score_field_name = 'dist'
        k = min(10, n)
        element = create_np_array_typed([0]*dim, data_t)
        cmd_knn = ['FT.SEARCH', 'idx', '', 'PARAMS', '2', 'b', element.tobytes(), 'RETURN', '1', score_field_name, 'SORTBY', score_field_name]

        expected_res_knn = []  # the expected ids are going to be unique
        for i in range(k):
            expected_res_knn.append(str(i))                                 # Expected id
            expected_res_knn.append([score_field_name, str(i * i * dim)])   # Expected score

        radius = dim * k**2
        element = create_np_array_typed([n]*dim, data_t)
        cmd_range = ['FT.SEARCH', 'idx', '', 'PARAMS', '2', 'b', element.tobytes(), 'RETURN', '1', score_field_name, 'LIMIT', 0, n]
        expected_res_range = []
        for i in range(n-k-per_doc+1, n-per_doc+1):
            expected_res_range.append(str(i))
            expected_res_range.append([score_field_name, str(dim * (n-per_doc-i+1)**2)])
        for i in range(n-per_doc+1, n):        # Ids for which there is a vector whose distance to the query vec is zero.
            expected_res_range.append(str(i))
            expected_res_range.append([score_field_name, '0'])
        expected_res_range.insert(0, int(len(expected_res_range)/2))

        for _ in env.reloadingIterator():
            waitForIndex(env, 'idx')
            info = index_info(env, 'idx')
            env.assertEqual(info['num_docs'], n)
            env.assertEqual(info['num_records'], n * per_doc * len(info['attributes']))
            env.assertEqual(info['hash_indexing_failures'], 0)

            cmd_knn[2] = f'*=>[KNN {k} @hnsw $b AS {score_field_name}]'
            hnsw_res = conn.execute_command(*cmd_knn)[1:]
            env.assertEqual(hnsw_res, expected_res_knn)

            cmd_knn[2] = f'*=>[KNN {k} @flat $b AS {score_field_name}]'
            flat_res = conn.execute_command(*cmd_knn)[1:]
            env.assertEqual(flat_res, expected_res_knn)

            cmd_range[2] = f'@hnsw:[VECTOR_RANGE {radius} $b]=>{{$yield_distance_as:{score_field_name}}}'
            hnsw_res = conn.execute_command(*cmd_range)
            env.assertEqual(sortedResults(hnsw_res), expected_res_range)

            cmd_range[2] = f'@flat:[VECTOR_RANGE {radius} $b]=>{{$yield_distance_as:{score_field_name}}}'
            flat_res = conn.execute_command(*cmd_range)
            env.assertEqual(sortedResults(flat_res), expected_res_range)

@skip(no_json=True)
def test_bad_index_multi_value_json():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 4
    per_doc = 5

    failures = 0

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.vecs', 'AS', 'vecs', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    # By default, we assume that a static path leads to a single value, so we can't index an array of vectors as multi-value
    conn.json().set(46, '.', {'vecs': [[0.46] * dim] * per_doc})
    failures += 1
    env.assertEqual(index_info(env, 'idx')['hash_indexing_failures'], failures)

    # We also don't support an array of length 1 that wraps an array for single value
    conn.json().set(46, '.', {'vecs': [[0.46] * dim]})
    failures += 1
    env.assertEqual(index_info(env, 'idx')['hash_indexing_failures'], failures)

    conn.flushall()
    failures = 0
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.vecs[*]', 'AS', 'vecs', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    # dynamic path returns a non array type
    conn.json().set(46, '.', {'vecs': [np.ones(dim).tolist(), 'not a vector']})
    failures += 1
    env.assertEqual(index_info(env, 'idx')['hash_indexing_failures'], failures)

    # we should NOT fail if some of the vectors are NULLs
    conn.json().set(46, '.', {'vecs': [np.ones(dim).tolist(), None, (np.ones(dim) * 2).tolist()]})
    env.assertEqual(index_info(env, 'idx')['hash_indexing_failures'], failures)
    env.assertEqual(index_info(env, 'idx')['num_records'], 2)

    # ...or if the path returns NULL
    conn.json().set(46, '.', {'vecs': None})
    env.assertEqual(index_info(env, 'idx')['hash_indexing_failures'], failures)

    # some of the vectors are not of the right dimension
    conn.json().set(46, '.', {'vecs': [np.ones(dim).tolist(), np.ones(dim + 46).tolist()]})
    failures += 1
    conn.json().set(46, '.', {'vecs': [np.ones(dim).tolist(), []]})
    failures += 1
    env.assertEqual(index_info(env, 'idx')['hash_indexing_failures'], failures)

    # some of the elements in some of vectors are not numerics
    vec = [42] * dim
    vec[-1] = 'not a number'
    conn.json().set(46, '.', {'vecs': [np.ones(dim).tolist(), vec]})
    failures += 1
    env.assertEqual(index_info(env, 'idx')['hash_indexing_failures'], failures)


def test_range_query_basic():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 4
    n = 99
    id_diff = 46

    for data_type in VECSIM_DATA_TYPES:
        for index in VECSIM_ALGOS:
            if index == "SVS-VAMANA" and data_type not in ("FLOAT16", "FLOAT32"):
                continue
            msg = f'{data_type}, {index}'
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', index, '6', 'TYPE', data_type, 'DIM',
                       dim, 'DISTANCE_METRIC', 'L2', 't', 'TEXT').ok()

            # search in an empty index
            query_data = create_np_array_typed([1]*dim, data_type)
            env.expect('FT.SEARCH', 'idx', '@v:[VECTOR_RANGE 0.1 $vec_param]=>{$YIELD_DISTANCE_AS:$score_field}',
                                   'SORTBY', 'score', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'score_field', 'score',
                                   'RETURN', 1, 'score', 'LIMIT', 0, n).equal([0])

            # load vectors, where vector with id i is [i, i, ..., i]
            load_vectors_with_texts_into_redis(conn, 'v', dim, n, data_type)

            # Expect to get the `id_diff` docs with the highest ids.
            dist_range = dim * id_diff**2
            query_data = create_np_array_typed([n+1]*dim, data_type)
            res = conn.execute_command('FT.SEARCH', 'idx', '@v:[VECTOR_RANGE $r $vec_param]=>{$YIELD_DISTANCE_AS:$score_field}',
            'SORTBY', 'score', 'PARAMS', 6, 'vec_param', query_data.tobytes(), 'r', dist_range, 'score_field', 'score',
            'RETURN', 1, 'score', 'LIMIT', 0, n)
            env.assertEqual(res[0], id_diff, message=msg)
            for i, doc_id in enumerate(res[1::2]):
                env.assertEqual(str(n-i), doc_id, message=msg)
            for i, score in enumerate(res[2::2], 1):
                env.assertEqual(['score', str(dim * i**2)], score, message=msg)

            # Run again without score field
            res = conn.execute_command('FT.SEARCH', 'idx', '@v:[VECTOR_RANGE $r $vec_param]',
                                       'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', dist_range,
                                       'RETURN', 0, 'LIMIT', 0, n)
            env.assertEqual(res[0], id_diff, message=msg)
            for expected_id, doc_id in enumerate(res[1:], n - id_diff + 1):
                env.assertEqual(str(expected_id), doc_id, message=msg)  # results should be sorted by id (by default)
            conn.flushall()


def test_range_query_basic_random_vectors():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 128
    n = 10000

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM',
               dim, 'DISTANCE_METRIC', 'COSINE', 'M', '4', 'EF_CONSTRUCTION', '4', 'EPSILON', '0.001').ok()

    query_data = load_vectors_to_redis(env, n, 0, dim)

    radius = 0.23
    res_default_epsilon = conn.execute_command('FT.SEARCH', 'idx', '@vector:[VECTOR_RANGE $r $vec_param]=>{$YIELD_DISTANCE_AS:dist}',
                               'SORTBY', 'dist', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius,
                               'RETURN', 1, 'dist', 'LIMIT', 0, n)

    res_higher_epsilon = conn.execute_command('FT.SEARCH', 'idx', '@vector:[VECTOR_RANGE $r $vec_param]=>{$YIELD_DISTANCE_AS:dist; $EPSILON: 0.1}',
                               'SORTBY', 'dist', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius,
                               'RETURN', 1, 'dist', 'LIMIT', 0, n)

    # Expect getting better results when epsilon is higher
    env.assertGreater(res_higher_epsilon[0], res_default_epsilon[0])
    for dist in res_higher_epsilon[2::2]:
        env.assertGreaterEqual(radius, float(dist[1]))
    docs_higher_epsilon = [doc for doc in res_higher_epsilon[1::2]]
    ids_found = 0
    for doc_id in res_default_epsilon[1::2]:
        if doc_id in docs_higher_epsilon:
            ids_found += 1
    # Results found with lower epsilon are subset of the results found with higher epsilon.
    env.assertEqual(ids_found, len(res_default_epsilon[1::2]))


def test_range_query_complex_queries():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 128
    index_size = 1000
    default = env.cmd(config_cmd(), 'GET', 'UNION_ITERATOR_HEAP')

    union_iterator_heap_configs = [
        default,
    # Todo: this test reveals inconsistent behavior when UNION_ITERATOR_HEAP is set to 1, that isn't caused by vector
    # range queries. This is a temporary workaround to bypass this failure and should be removed once we have a fix.
    # Related to mod_4374 and mod_4375 (see tests)
    #     1,  # small
    ]

    for union_iterator_heap in union_iterator_heap_configs:
        env.expect(config_cmd(), 'SET', 'UNION_ITERATOR_HEAP', union_iterator_heap).ok
        loop_case = f'union config: {union_iterator_heap}'

        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
                'DIM', dim, 'DISTANCE_METRIC', 'L2', 't', 'TEXT', 'num', 'NUMERIC', 'coordinate', 'GEO').ok()

        p = conn.pipeline(transaction=False)
        for i in range(1, index_size+1):
            vector = create_np_array_typed([i]*dim)
            p.execute_command('HSET', i, 'v', vector.tobytes(), 't', 'text', 'num', i, 'coordinate',
                            str(i/100)+","+str(i/100))
        p.execute()
        if not env.isCluster():
            env.assertEqual(get_vecsim_index_size(env, 'idx', 'v'), index_size)

        # Change the text value to 'other' for 20% of the vectors (with id 5, 10, ..., index_size)
        for i in range(5, index_size + 1, 5):
            vector = create_np_array_typed([i]*dim)
            conn.execute_command('HSET', i, 'v', vector.tobytes(), 't', 'other', 'num', -i, 'coordinate',
                                    str(i/100)+","+str(i/100))

        query_data = create_np_array_typed([index_size]*dim)
        radius = dim * 9**2

        # Expect to get the results whose ids are in [index_size-9, index_size] and don't multiply by 5.
        expected_res = [8]
        for i in range(1, 10):
            if i == 5:
                continue
            expected_res.extend([str(index_size-i), ['dist', str(dim * i**2), 't', 'text', 'num', str(index_size-i)]])
        res = env.cmd('FT.SEARCH', 'idx', '@t:text @v:[VECTOR_RANGE $r $vec_param]=>{$YIELD_DISTANCE_AS:dist}',
                        'SORTBY', 'dist', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius,
                        'RETURN', 3, 'dist', 't', 'num', 'LIMIT', 0, index_size)
        env.assertEqual(res, expected_res, message=loop_case)

        # Expect to get 10 results whose ids are a multiplication of 5 whose distance within the range.
        radius = dim * 49**2
        expected_res = [10]
        for i in range(0, 50, 5):
            expected_res.extend([str(index_size-i), ['dist', str(dim * i**2), 't', 'other', 'num', str(i-index_size)]])
        res = env.cmd('FT.SEARCH', 'idx', 'other @v:[VECTOR_RANGE $r $vec_param]=>{$YIELD_DISTANCE_AS:dist}',
                        'SORTBY', 'dist', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius,
                        'RETURN', 3, 'dist', 't', 'num' ,'LIMIT', 0, index_size)
        env.assertEqual(res, expected_res, message=loop_case)

        # Expect to get 20 results whose ids are a multiplication of 5 OR has a value in 'num' field
        # which are in the range [950, 960), and whose corresponding vector distance within the range. These are ids
        # [index_size, index_size-5, ... , index_size-50] U [index_size-51, index_size-52, ..., index_size-59]
        radius = dim * 59**2
        expected_res = [20]
        for i in range(0, 50, 5):
            expected_res.extend([str(index_size-i), ['dist', str(dim * i**2), 't', 'other', 'num', str(i-index_size)]])
        for i in range(50, 60):
            expected_res.extend([str(index_size-i), ['dist', str(dim * i**2), 't', 'other' if (index_size-i) % 5 == 0 else 'text',
                                'num', str(i-index_size if (index_size-i) % 5 == 0 else index_size-i)]])
        res = env.cmd('FT.SEARCH', 'idx',
                f'(@t:other | @num:[{index_size-60} ({index_size-50}]) @v:[VECTOR_RANGE $r $vec_param]=>{{$YIELD_DISTANCE_AS:dist}}',
                'SORTBY', 'dist', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius,
                'RETURN', 3, 'dist', 't', 'num', 'LIMIT', 0, index_size)
        env.assertEqual(res, expected_res, message=loop_case)

        # Test again with NOT operator - expect to get the same result, since NOT 'text' means that @t contains 'other'
        res = env.cmd('FT.SEARCH', 'idx',
                f'(-text | @num:[{index_size-60} ({index_size-50}]) @v:[VECTOR_RANGE $r $vec_param]=>{{$YIELD_DISTANCE_AS:dist}}',
                'SORTBY', 'dist', 'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius,
                'RETURN', 3, 'dist', 't', 'num', 'LIMIT', 0, index_size)
        env.assertEqual(res, expected_res, message=loop_case)

        # Test with global filters. Use range query with all types of global filters exists
        radius = dim * 100**2  # ids in range [index_size-100, index_size] are within the radius.
        inkeys = [i for i in range(3, index_size+1, 3)]
        expected_res = [str(i) for i in range(index_size, index_size-100, -1)
                        if i in inkeys and i % 5 != 0] # i % 5 != 0 because we also search for `text` in the query
        expected_res.insert(0, len(expected_res))
        res = env.cmd('FT.SEARCH', 'idx', 'text @v:[VECTOR_RANGE $r $vec_param]=>{$yield_distance_as:dist}',
                        'INKEYS', len(inkeys), *inkeys,
                        'SORTBY', 'dist', 'NOCONTENT', 'LIMIT', 0, index_size,
                        'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius)
        env.assertEqual(res, expected_res, message=loop_case)

        # Rerun with global filters, put the range query in the root this time (expect the same result set)
        res = env.cmd('FT.SEARCH', 'idx', 'text @v:[VECTOR_RANGE $r $vec_param]=>{$yield_distance_as:dist}',
                        'INKEYS', len(inkeys), *inkeys,
                        'SORTBY', 'dist', 'NOCONTENT', 'LIMIT', 0, index_size,
                        'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius)
        env.assertEqual(res, expected_res, message=loop_case)

        # Test with tf-idf scores. for ids that are a multiplication of 5, tf_idf score is 2, while for other
        # ids the tf-idf score is 1 (note that the range query doesn't affect the score).
        # Change the score of a single doc, so it'll get the max score.
        con = env.getConnectionByKey(str(index_size), 'HSET')
        env.assertEqual(con.execute_command('HSET', str(index_size), 't', 'unique'), 0)

        radius = dim * 10**2
        expected_res = [11, str(index_size), '8' if env.isCluster() and env.shardsCount > 1 else '9']  # Todo: fix this inconsistency
        for i in range(index_size-10, index_size, 5):
            expected_res.extend([str(i), '2'])
        for i in sorted(set(range(index_size-10, index_size))-set(range(index_size-10, index_size+1, 5))):
            expected_res.extend([str(i), '1'])
        res = env.cmd('FT.SEARCH', 'idx', '(text|other|unique) @v:[VECTOR_RANGE $r $vec_param]', 'SCORER', 'TFIDF', 'WITHSCORES',
                        'PARAMS', 4, 'vec_param', query_data.tobytes(), 'r', radius,
                        'RETURN', 0, 'LIMIT', 0, 11)
        env.assertEqual(res, expected_res, message=loop_case)

        conn.flushall()


def test_multiple_range_queries():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 16
    n = 100
    data_type = 'FLOAT32'

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v_flat', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2',
                't', 'TEXT', 'num', 'NUMERIC',
                'v_hnsw', 'VECTOR', 'HNSW', '6', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    # Run queries over an empty index
    query_vec_flat = create_np_array_typed([n/4]*dim, data_type)
    query_vec_hnsw = create_np_array_typed([n/2]*dim, data_type)
    intersect_query = '(@v_flat:[VECTOR_RANGE $r $vec_param_flat]=>{$YIELD_DISTANCE_AS:dist_flat} @v_hnsw:[VECTOR_RANGE $r $vec_param_hnsw]=>{$YIELD_DISTANCE_AS:dist_hnsw})'
    union_query = '(@v_flat:[VECTOR_RANGE $r $vec_param_flat]=>{$YIELD_DISTANCE_AS:dist_flat} | @v_hnsw:[VECTOR_RANGE $r $vec_param_hnsw]=>{$YIELD_DISTANCE_AS:dist_hnsw})'
    for query in [intersect_query, union_query]:
        env.expect('FT.SEARCH', 'idx', query, 'SORTBY', 'dist_flat', 'PARAMS', 6, 'vec_param_flat', query_vec_flat.tobytes(),
                    'vec_param_hnsw', query_vec_hnsw.tobytes(), 'r', 1,
                    'RETURN', 2, 'dist_flat', 'dist_hnsw').equal([0])

    p = conn.pipeline(transaction=False)
    for i in range(1, n+1):
        vector = create_np_array_typed([i]*dim, data_type)
        p.execute_command('HSET', i, 'v_flat', vector.tobytes(), 'v_hnsw', vector.tobytes(),
                            't', 'text' if i % 5 else 'other', 'num', i)
    p.execute()

    # vectors with ids [0, index_size/2] are within the radius of query_vec_flat, while
    # vectors with ids [index_size/4, index_size*3/4] are within the radius of query_vec_hnsw.
    # Expected res is the intersection of both (we return 10 results that are closest to query_vec_flat)
    radius = dim * (n/4)**2
    expected_res = [int(n/4) + 1]
    for i in range(int(n/4), int(n/4) + 10):
        expected_res.extend([str(i), ['dist_flat', str(int(dim * (n/4-i)**2)), 'dist_hnsw', str(int(dim * (n/2-i)**2))]])

    env.expect('FT.SEARCH', 'idx', intersect_query, 'SORTBY', 'dist_flat', 'PARAMS', 6, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'r', radius,
                'RETURN', 2, 'dist_flat', 'dist_hnsw').equal(expected_res)

    # Run again, sort by results that are closest to query_vec_hnsw
    expected_res = [int(n/4) + 1]
    for i in range(int(n/2), int(n/2)-10, -1):
        expected_res.extend([str(i), ['dist_flat', str(int(dim * (n/4-i)**2)), 'dist_hnsw', str(int(dim * (n/2-i)**2))]])
    env.expect('FT.SEARCH', 'idx', intersect_query, 'SORTBY', 'dist_hnsw', 'PARAMS', 6, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'r', radius,
                'RETURN', 2, 'dist_flat', 'dist_hnsw').equal(expected_res)

    # Run union query - expect to get a union of both ranges, sorted by id. The distances of a range query
    # will be given as output only for ids that are in the corresponding subquery range.
    expected_res = [int(n*3/4)]
    for i in range(1, int(n/4)):
        expected_res.extend([str(i), ['dist_flat', str(int(dim * abs(n/4-i)**2))]])
    for i in range(int(n/4), int(n/2) + 1):
        expected_res.extend([str(i), ['dist_flat', str(int(dim * abs(n/4-i)**2)), 'dist_hnsw', str(int(dim * (n/2-i)**2))]])
    for i in range(int(n/2) + 1, int(n*3/4) + 1):
        expected_res.extend([str(i), ['dist_hnsw', str(int(dim * (n/2-i)**2))]])
    env.expect('FT.SEARCH', 'idx', union_query, 'PARAMS', 6, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'r', radius, 'SORTBY', 'num', 'LIMIT', 0, n,
                'RETURN', 2, 'dist_flat', 'dist_hnsw').equal(expected_res)

    # Run union query with another field - expect to get the results from before, followed by the results
    # that are within the numeric range without the distance field.
    numeric_range = (n/2, n*9/10)
    extended_union_query = union_query + f' | @num:[{numeric_range[0]} {numeric_range[1]}]'
    expected_res[0] = int(numeric_range[1])
    for i in range(int(n*3/4)+1, int(numeric_range[1]) + 1):
        expected_res.extend([str(i), []])
    env.expect('FT.SEARCH', 'idx', extended_union_query, 'PARAMS', 6, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'r', radius, 'SORTBY', 'num', 'LIMIT', 0, n,
                'RETURN', 2, 'dist_flat', 'dist_hnsw').equal(expected_res)

    intersect_over_union_q = union_query + f' @t:other'
    # result set should be every doc in the union of the ranges that is multiply by 5.
    expected_res = [int((n*3/4) / 5)]
    for i in range(int(n*3/4), int(n/2), -5):
        expected_res.extend([str(i), ['dist_hnsw', str(int(dim * (n/2-i)**2))]])
    for i in range(int(n/2), int(n/4)-5, -5):
        expected_res.extend([str(i), ['dist_flat', str(int(dim * abs(n/4-i)**2)), 'dist_hnsw', str(int(dim * (n/2-i)**2))]])
    for i in range(int(n/4)-5, 0, -5):
        expected_res.extend([str(i), ['dist_flat', str(int(dim * abs(n/4-i)**2))]])
    env.expect('FT.SEARCH', 'idx', intersect_over_union_q, 'SORTBY', 'num', 'DESC', 'PARAMS', 6, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'r', radius,  'LIMIT', 0, n,
                'RETURN', 2, 'dist_flat', 'dist_hnsw').equal(expected_res)

    union_over_intersection_q = intersect_query + f' | (@num:[{n/3} {n*3/4}] @t:other)'
    # result set should be every doc in the intersection of both ranges OR in the numeric range that is multiply by 5.
    expected_res = [int(n/4) + int(n/4 / 5) + 1]
    for i in range(int(n/4), int(n/2)+1):
            expected_res.extend([str(i), ['dist_flat', str(int(dim * (n/4-i)**2)), 'dist_hnsw', str(int(dim * (n/2-i)**2))]])
    for i in range(int(n/2)+5, int(n*3/4)+1, 5):
        expected_res.extend([str(i), []])
    env.expect('FT.SEARCH', 'idx', union_over_intersection_q, 'SORTBY', 'num', 'PARAMS', 6, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'r', radius,  'LIMIT', 0, n,
                'RETURN', 2, 'dist_flat', 'dist_hnsw').equal(expected_res)

    # Range + KNN queries #
    # Range query should have 0 results, and so does the entire query.
    query_vec_knn = create_np_array_typed([0]*dim, data_type)
    env.expect('FT.SEARCH', 'idx', '@v_flat:[VECTOR_RANGE $r $vec_param_flat]=>[KNN 10 @v_hnsw $knn_vec AS knn_dist]',
                'SORTBY', 'knn_dist', 'PARAMS', 6, 'vec_param_flat', create_np_array_typed([2*n]*dim, data_type).tobytes(),
                'knn_vec', query_vec_knn.tobytes(),
                'r', dim, 'LIMIT', 0, n, 'RETURN', 2, 'dist_hnsw', 'knn_dist').equal([0])

    # Range query should have 2 results, and so does the entire query. range query doesn't yield scores.
    query_vec_knn = create_np_array_typed([0]*dim, data_type)
    expected_res = [2, '99', ['knn_dist', '156816'], '100', ['knn_dist', '160000']]
    env.expect('FT.SEARCH', 'idx', '@v_flat:[VECTOR_RANGE $r $vec_param_flat]=>[KNN 10 @v_hnsw $knn_vec AS knn_dist]',
                'SORTBY', 'knn_dist', 'PARAMS', 6, 'vec_param_flat', create_np_array_typed([n]*dim, data_type).tobytes(),
                'knn_vec', query_vec_knn.tobytes(),
                'r', dim, 'LIMIT', 0, n, 'RETURN', 2, 'dist_hnsw', 'knn_dist').equal(expected_res)

    # This query should return the TOP 10 results closest to query_vec_knn that are in both ranges -
    # These are the lower ids that are >= n/4
    expected_res = [10]
    for i in range(int(n/4), int(n/4)+10):
        expected_res.extend([str(i), ['knn_dist', str(dim * i**2), 'dist_flat', str(int(dim * (n/4-i)**2)),
                                        'dist_hnsw', str(int(dim * (n/2-i)**2))]])
    filtered_q = intersect_query + '=>[KNN 10 @v_hnsw $knn_vec AS knn_dist]'
    env.expect('FT.SEARCH', 'idx', filtered_q, 'SORTBY', 'knn_dist', 'PARAMS', 8, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'knn_vec', query_vec_knn.tobytes(), 'r', radius,
                'RETURN', 3, 'dist_flat', 'dist_hnsw', 'knn_dist').equal(expected_res)

    # This query should return the TOP 20 results closest to query_vec_knn that are in at least one of the ranges,
    # AND has 'other' in their text field. These are ids 5, 10, ..., n*3/4.
    expected_res = [min(20, int(n*3/4 /5))]
    for i in range(5, int(n/4), 5):
        expected_res.extend([str(i), ['knn_dist', str(dim * i**2)]])
    for i in range(int(n/4), int(n*3/4) + 1, 5):
        expected_res.extend([str(i), ['knn_dist', str(dim * i**2), 'dist_hnsw', str(int(dim * (n/2-i)**2))]])
    filtered_q = '(' + union_query + ' @t:other)=>[KNN 20 @v_hnsw $knn_vec AS knn_dist]'
    env.expect('FT.SEARCH', 'idx', filtered_q, 'SORTBY', 'knn_dist', 'PARAMS', 8, 'vec_param_flat', query_vec_flat.tobytes(),
                'vec_param_hnsw', query_vec_hnsw.tobytes(), 'knn_vec', query_vec_knn.tobytes(), 'r', radius,
                'RETURN', 2, 'dist_hnsw', 'knn_dist', 'LIMIT', 0, 20).equal(expected_res)


# Test that a query that contains KNN as subset is parsed correctly (specially in coordinator, where we
# have a special treatment for these cases)
def test_query_with_knn_substr():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
                         'DIM', dim, 'DISTANCE_METRIC', 'L2', 't', 'TEXT')

    for i in range(10):
        conn.execute_command("HSET", f'doc{i}', "v", create_np_array_typed([i] * dim).tobytes(),
                             't', 'knn' if i % 2 else 'val')

    # Expect that doc1, doc3 and doc5 that has "knn" in their @t field and their vector in @v
    # field is the closest to the query vector will be returned.
    query_with_vecsim = "(@t:KNN)=>[KNN 3 @v $BLOB]=>{$yield_distance_as: dist}"
    expected_res = [{'dist': '2'}, {'dist': '18'}, {'dist': '50'}]
    res = conn.execute_command("FT.AGGREGATE", "idx", query_with_vecsim,
                               "PARAMS", 2, "BLOB", create_np_array_typed([0] * dim).tobytes())
    env.assertEqual([to_dict(res_item) for res_item in res[1:]], expected_res)

    res = conn.execute_command("FT.SEARCH", "idx", query_with_vecsim, 'SORTBY', 'dist',
                               "PARAMS", 2, "BLOB", create_np_array_typed([0] * dim).tobytes(), 'RETURN', '1', 'dist')
    env.assertEqual([to_dict(res_item) for res_item in res[2::2]], expected_res)

    # Expect that all the odd numbers documents (doc1, doc3, doc5, doc7 and doc9) that has "knn" in their @t field
    # will be returned.
    query_without_vecsim = "(@t:KNN)"
    expected_res = ['doc1', 'doc3', 'doc5', 'doc7', 'doc9']
    res = conn.execute_command("FT.AGGREGATE", "idx", query_without_vecsim, 'LOAD', '1', '@__key',
                               'SORTBY', '1', '@__key')
    env.assertEqual([res_item[1] for res_item in res[1:]], expected_res)

    res = conn.execute_command("FT.SEARCH", "idx", query_without_vecsim,
                               "PARAMS", 2, "BLOB", create_np_array_typed([0] * dim).tobytes(), 'nocontent',
                               'LOAD', '1', '@__key', 'SORTBY', '__key')
    env.assertEqual(res[1:], expected_res)


def test_score_name_case_sensitivity():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2

    k = 10
    score_name = 'SCORE'
    vec_fieldname = 'VEC'
    default_score_name = f'__{vec_fieldname}_score'
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                         vec_fieldname, 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2')

    def expected(cur_score_name = None):
        expected = [k]
        if cur_score_name is not None:
            for i in range(k):
                expected += [f'doc{i}', [cur_score_name, str(i * i * dim)]]
        else:
            for i in range(k):
                expected += [f'doc{i}', []]
        return expected

    for i in range(10):
        conn.execute_command("HSET", f'doc{i}', vec_fieldname, create_np_array_typed([i] * dim).tobytes())

    # Test yield_distance_as
    res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN $k @{vec_fieldname} $BLOB]=>{{$yield_distance_as: {score_name}}}',
                               'PARAMS', 4, 'k', k, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
                               'RETURN', '1', score_name)
    env.assertEqual(res, expected(score_name))
        # mismatch cases
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN $k @{vec_fieldname} $BLOB]=>{{$yield_distance_as: {score_name}}}',
               'PARAMS', 4, 'k', k, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
               'RETURN', '1', score_name.lower()).equal(expected())
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN $k @{vec_fieldname} $BLOB]=>{{$yield_distance_as: {score_name.lower()}}}',
               'PARAMS', 4, 'k', k, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
               'RETURN', '1', score_name).equal(expected())

    # Test AS
    res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB AS {score_name}]',
                               'PARAMS', 2, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
                               'RETURN', '1', score_name)
    env.assertEqual(res, expected(score_name))
        # mismatch cases
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB AS {score_name}]',
               'PARAMS', 2, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
               'RETURN', '1', score_name.lower()).equal(expected())
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB AS {score_name.lower()}]',
               'PARAMS', 2, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
               'RETURN', '1', score_name).equal(expected())

    # Test default score name
    res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB]',
                               'PARAMS', 2, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
                               'RETURN', '1', default_score_name)
    env.assertEqual(res, expected(default_score_name))
        # mismatch case
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB]',
               'PARAMS', 4, 'k', k, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
               'RETURN', '1', score_name.lower()).equal(expected())


@skip(cluster=True)
def test_tiered_index_gc():
    N = 100
    env = Env(moduleArgs=f'WORKERS 2 FORK_GC_RUN_INTERVAL 1000000000000 FORK_GC_CLEAN_THRESHOLD {N}')
    conn = getConnectionByEnv(env)
    dim = 16
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                         'v1', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
                         'v2', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT64', 'DIM', dim, 'DISTANCE_METRIC', 'COSINE',
                         't', 'TEXT')
    # Create another vector index with `FT.ALTER` command. (relevant for the GC - related to MOD-6276)
    conn.execute_command('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'v1', 'AS',
                         'v3', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2')

    # Insert random vectors to an index with two vector fields.
    for i in range(N):
        res = conn.execute_command('hset', i, 't', f'some string with to be cleaned by GC for id {i}',
                                   'v1', create_np_array_typed(np.random.random((1, dim))).tobytes(),
                                   'v2', create_np_array_typed(np.random.random((1, dim)), 'FLOAT64').tobytes())
        env.assertEqual(res, 3)

    def get_debug_info():
        return {v: get_vecsim_debug_dict(env, 'idx', v) for v in ['v1', 'v2', 'v3']}

    # Wait until all vectors are indexed into HNSW.
    debug_info = get_debug_info()
    while np.any([index['BACKGROUND_INDEXING'] for index in debug_info.values()]):
        time.sleep(0.1)
        debug_info = get_debug_info()

    # Delete all documents. Note that we have less than TIERED_HNSW_SWAP_JOBS_THRESHOLD docs (1024),
    # so we know that we won't execute swap jobs during the 'DEL' command execution.
    for i in range(N):
        res = conn.execute_command('DEL', i)
        env.assertEqual(res, 1)

    debug_info = get_debug_info()
    env.assertEqual(to_dict(debug_info['v1']['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], N)
    env.assertEqual(to_dict(debug_info['v2']['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], N)
    env.assertEqual(to_dict(debug_info['v3']['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], N)

    # Wait for all repair jobs to be finish, then run GC to remove the deleted vectors.
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
    env.expect(debug_cmd(), 'GC_FORCEINVOKE', 'idx').equal('DONE')

    debug_info = get_debug_info()
    env.assertEqual(to_dict(debug_info['v1']['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)
    env.assertEqual(to_dict(debug_info['v2']['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)
    env.assertEqual(to_dict(debug_info['v3']['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)

@skip(cluster=True)
def test_switch_write_mode_multiple_indexes(env):
    conn = getConnectionByEnv(env)
    dim = 32
    n_indexes = 100
    n_vectors = 100
    for index_i in range(n_indexes):
        # Create an index and insert vectors synchronously.
        index_prefix = f'idx_{index_i}z'
        conn.execute_command('FT.CREATE', f'index:{index_prefix}', 'PREFIX', 1, index_prefix, 'SCHEMA',
                         'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', 64)
        for vec_i in range(n_vectors):
            conn.execute_command('HSET', f'{index_prefix}_vec_{vec_i}', 'v',
                                 create_np_array_typed(np.random.random((1, dim))).tobytes())

    # While reindexing occurs in the background for all indexes, switch the vecsim write mode to 'async'.
    workers_info = getWorkersThpoolStats(env)
    env.assertEqual(workers_info['totalJobsDone'], 0)
    env.assertEqual(workers_info['totalPendingJobs'], 0)
    env.expect(config_cmd(), 'SET', 'WORKERS', 4).ok()

    # Delete half of the vectors from each index.
    for index_i in range(n_indexes):
        index_prefix = f'idx_{index_i}z'
        for vec_i in range(0, n_vectors, 2):
            conn.execute_command('DEL', f'{index_prefix}_vec_{vec_i}')

    # Return to in-place mode, wait for async jobs to be finished so that reindexing that was done async is finished
    # (insert to HNSW jobs are done), and validate indexes status.
    env.expect(config_cmd(), 'SET', 'WORKERS', 0).ok()
    conn.execute_command(debug_cmd(), 'WORKERS', 'DRAIN')

    bg_indexing = 0
    for index_i in range(n_indexes):
        index_prefix = f'idx_{index_i}z'
        bg_indexing += index_info(env, f'index:{index_prefix}')['indexing']
        vector_index_info = get_vecsim_debug_dict(env, f'index:{index_prefix}', 'v')
        env.assertEqual(to_dict(vector_index_info['BACKEND_INDEX'])['INDEX_LABEL_COUNT'], n_vectors // 2,
                        message=(index_prefix, vector_index_info))
    if bg_indexing == 0:
        prefix = "::warning title=Bad scenario in test_vecsim:test_switch_write_mode_multiple_indexes::" if GHA else ''
        print(f"{prefix}All vectors were done reindex before switching back to in-place mode")

def test_max_knn_k():
    env = Env(moduleArgs='DEFAULT_DIALECT 3')
    conn = getConnectionByEnv(env)
    dim = 2
    k = pow(2, 59)
    score_name = 'SCORE'
    vec_fieldname = 'VEC'
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                         vec_fieldname, 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2')
    for i in range(10):
        conn.execute_command("HSET", f'doc{i}', vec_fieldname, create_np_array_typed([i] * dim).tobytes())
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB AS {score_name.lower()}]',
               'PARAMS', 2, 'BLOB', create_np_array_typed([0] * dim).tobytes(),
               'RETURN', '1', score_name).error().contains('KNN K parameter is too large')

def test_vector_index_ptr_valid(env):
    conn = getConnectionByEnv(env)
    # Scenerio1: Vecsim Index scheme with numeric (or non-vector type) and vector type with invalid parameter
    #            Insert partial doc - only numeric
    #            Update Doc

    # HNSW parameters the causes an execution throw (M > UINT16_MAX)
    UINT16_MAX = 2**16
    M = UINT16_MAX + 1
    dim = 4

    env.expect('FT.CREATE', 'idx','SCHEMA', 'n', 'NUMERIC',
                    'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT16', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', M).ok()

    res = conn.execute_command('HSET', 'doc', 'n', 0)
    env.assertEqual(res, 1)
    # before bug fix, the following command would cause a server crash due to null pointer access to the vector index that filed to be created.
    res = conn.execute_command('HSET', 'doc', 'n', 1)
    env.assertEqual(res, 0)

    # Sanity check - insert a vector, expect indexing failure
    res = conn.execute_command('HSET', 'doc1', 'v', create_np_array_typed([0]*dim,'FLOAT16').tobytes())
    env.assertEqual(res, 1)

    index_errors_dict = index_errors(env, 'idx')
    env.assertEqual(index_errors_dict['last indexing error'], "Could not open vector for indexing")

    # Check FlushAll - before bug fix, the following command would cause a server crash due to the null pointer access
    # Server will reply OK but crash afterwards, so a PING is required to verify
    env.expect('FLUSHALL').noError()
    env.expect('PING').noError()


@skip(cluster=True)
def test_svs_vamana_info():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    dim = 16
    data_type = 'FLOAT32'

    # Create SVS VAMANA index with LVQ8 compression
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '8', 'TYPE', data_type,
                'DIM', dim, 'DISTANCE_METRIC', 'L2', 'COMPRESSION', 'LVQ8').ok()

    # Simple platform-agnostic check for Intel CPU.
    import platform
    def is_intel_cpu():
        if platform.system() != 'Linux' or platform.machine() != 'x86_64':
            return False
        # Check CPU vendor in /proc/cpuinfo on Linux
        try:
            with open('/proc/cpuinfo', 'r') as f:
                cpuinfo = f.read().lower()
                if 'vendor_id' in cpuinfo and 'genuineintel' in cpuinfo:
                    return True
        except (IOError, FileNotFoundError):
            return False

    # Validate that ft.info returns the default params for SVS VAMANA, along with compression
    # compression in runtime is LVQ8 if we are running on intel machine and GlobalSQ otherwise.
    compression_runtime = 'LVQ8' if is_intel_cpu() else 'GlobalSQ8'
    assertInfoField(env, 'idx', 'attributes',
                    [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'algorithm', 'SVS-VAMANA',
                      'data_type', 'FLOAT32', 'dim', 16, 'distance_metric', 'L2', 'graph_max_degree', 32,
                      'construction_window_size', 200, 'compression', compression_runtime, 'training_threshold', 10240]])
