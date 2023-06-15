# -*- coding: utf-8 -*-
import random

import numpy as np
from RLTest import Env

from common import *
from includes import *
from redis.client import NEVER_DECODE
from random import randrange


def test_sanity():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    vecsim_type = ['FLAT', 'HNSW']
    for vs_type in vecsim_type:
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()
        conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
        conn.execute_command('HSET', 'b', 'v', 'aaaabaaa')
        conn.execute_command('HSET', 'c', 'v', 'aaaaabaa')
        conn.execute_command('HSET', 'd', 'v', 'aaaaaaba')

        res = [4, 'a', ['score', '0', 'v', 'aaaaaaaa'],
                  'b', ['score', '3.09485009821e+26', 'v', 'aaaabaaa'],
                  'c', ['score', '2.02824096037e+31', 'v', 'aaaaabaa'],
                  'd', ['score', '1.32922799578e+36', 'v', 'aaaaaaba']]
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $blob AS score]', 'PARAMS', '2', 'blob', 'aaaaaaaa', 'SORTBY', 'score', 'ASC').equal(res)

        # todo: make test work on coordinator
        res = [4, 'c', ['score', '0', 'v', 'aaaaabaa'],
                  'b', ['score', '2.01242627636e+31', 'v', 'aaaabaaa'],
                  'a', ['score', '2.02824096037e+31', 'v', 'aaaaaaaa'],
                  'd', ['score', '1.31886368448e+36', 'v', 'aaaaaaba']]
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $blob AS score]', 'PARAMS', '2', 'blob', 'aaaaabaa', 'SORTBY', 'score', 'ASC').equal(res)

        expected_res = ['__v_score', '0', 'v', 'aaaaaaaa']
        res = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 1 @v $blob]', 'PARAMS', '2', 'blob', 'aaaaaaaa', 'SORTBY', '__v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[2], expected_res)

        #####################
        ## another example ##
        #####################
        message = 'aaaaabaa'
        res = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b]', 'PARAMS', '2', 'b', message, 'SORTBY', '__v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[2], ['__v_score', '0', 'v', 'aaaaabaa'])

        env.execute_command('FT.DROPINDEX', 'idx', 'DD')


def testDel():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    vecsim_type = ['FLAT', 'HNSW']
    for vs_type in vecsim_type:
        env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')

        conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
        conn.execute_command('HSET', 'b', 'v', 'aaaaaaba')
        conn.execute_command('HSET', 'c', 'v', 'aaaabaaa')
        conn.execute_command('HSET', 'd', 'v', 'aaaaabaa')

        expected_res = ['a', ['score', '0', 'v', 'aaaaaaaa'], 'c', ['score', '3.09485009821e+26', 'v', 'aaaabaaa'],
                        'd', ['score', '2.02824096037e+31', 'v', 'aaaaabaa'], 'b', ['score', '1.32922799578e+36', 'v', 'aaaaaaba']]

        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[1:3], expected_res[0:2])

        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'score', 'ASC', 'LIMIT', 0, 2)
        env.assertEqual(res[1:5], expected_res[0:4])

        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 3 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'score', 'ASC', 'LIMIT', 0, 3)
        env.assertEqual(res[1:7], expected_res[0:6])

        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'score', 'ASC', 'LIMIT', 0, 4)
        env.assertEqual(res[1:9], expected_res[0:8])

        conn.execute_command('DEL', 'a')

        expected_res = ['c', ['__v_score', '3.09485009821e+26', 'v', 'aaaabaaa'],
                        'd', ['__v_score', '2.02824096037e+31', 'v', 'aaaaabaa'],
                        'b', ['__v_score', '1.32922799578e+36', 'v', 'aaaaaaba']]
        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', '__v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[1:3], expected_res[:2])
        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', '__v_score', 'ASC', 'LIMIT', 0, 2)
        env.assertEqual(res[1:5], expected_res[:4])
        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 3 @v $b]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', '__v_score', 'ASC', 'LIMIT', 0, 3)
        env.assertEqual(res[1:7], expected_res[:6])

        # '''
        # This test returns 4 results instead of the expected 3. The HNSW library return the additional results.
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh', 'RETURN', '1', 'v').\
            equal([3, 'b', ['v', 'aaaaaaba'], 'c', ['v', 'aaaabaaa'], 'd', ['v', 'aaaaabaa']])
        # '''

        env.execute_command('FT.DROPINDEX', 'idx', 'DD')


def testDelReuse():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')

    def test_query_empty(env):
        conn = getConnectionByEnv(env)
        vecsim_type = ['FLAT', 'HNSW']
        for vs_type in vecsim_type:
            env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')
            env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([0])
            conn.execute_command('HSET', 'a', 'v', 'redislab')
            env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([1, 'a', ['v', 'redislab']])
            conn.execute_command('DEL', 'a')
            env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([0])
            env.execute_command('FT.DROPINDEX', 'idx', 'DD')

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
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')

    vecs = del_insert(env)
    res = [4, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh', 'RETURN', '1', 'v').equal(res)

    vecs = del_insert(env)
    res = [4, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh', 'RETURN', '1', 'v').equal(res)

    vecs = del_insert(env)
    res = [4, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh', 'RETURN', '1', 'v').equal(res)

def load_vectors_to_redis(env, n_vec, query_vec_index, vec_size):
    conn = getConnectionByEnv(env)
    for i in range(n_vec):
        vector = np.random.rand(1, vec_size).astype(np.float32)
        if i == query_vec_index:
            query_vec = vector
        conn.execute_command('HSET', i, 'vector', vector.tobytes())
    return query_vec

def query_vector(env, idx, query_vec):
    conn = getConnectionByEnv(env)
    return env.execute_command('FT.SEARCH', idx, '*=>[KNN 5 @vector $v AS score]', 'PARAMS', '2', 'v', query_vec.tobytes(),
                               'SORTBY', 'score', 'ASC', 'RETURN', 1, 'score', 'LIMIT', 0, 5, **{NEVER_DECODE: []})

def testDelReuseLarge():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    INDEX_NAME = 'items'
    prefix = 'item'
    n_vec = 5
    query_vec_index = 3
    vec_size = 1280

    env.execute_command('FT.CREATE', INDEX_NAME, 'ON', 'HASH',
                        'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '1280',
                        'DISTANCE_METRIC', 'L2')
    for _ in range(3):
        query_vec = load_vectors_to_redis(env, n_vec, query_vec_index, vec_size)
        res = query_vector(env, INDEX_NAME, query_vec)
        for i in range(4):
            env.assertLessEqual(float(res[2 + i * 2][1]), float(res[2 + (i + 1) * 2][1]))

def testCreate():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '14', 'TYPE', 'FLOAT32',
                        'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '10', 'M', '16',
                        'EF_CONSTRUCTION', '200', 'EF_RUNTIME', '10')
    for _ in env.retry_with_rdb_reload():
        info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR']]
        assertInfoField(env, 'idx1', 'attributes', info)
        info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx1", "v")
        env.assertEqual(info_data[:-3], ['ALGORITHM', 'HNSW', 'TYPE', 'FLOAT32', 'DIMENSION', 1024, 'METRIC', 'IP', 'INDEX_SIZE', 0, 'M', 16, 'EF_CONSTRUCTION', 200, 'EF_RUNTIME', 10, 'MAX_LEVEL', -1, 'ENTRYPOINT', -1, 'MEMORY'])
        # skip the memory value
        env.assertEqual(info_data[-2:], ['LAST_SEARCH_MODE', 'EMPTY_MODE'])

    # Uncomment these tests when support for FLOAT64, INT32, INT64, is added.
    # Trying to run these tests right now will cause 'Bad arguments for vector similarity HNSW index type' error

    # env.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '14', 'TYPE', 'FLOAT64', 'DIM', '4096', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', '10', 'M', '32', 'EF_CONSTRUCTION', '100', 'EF_RUNTIME', '20')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'HNSW', 'TYPE', 'FLOAT64', 'DIM', '4096', 'DISTANCE_METRIC', 'L2', 'M', '32', 'EF_CONSTRUCTION', '100', 'EF_RUNTIME', '20']]
    # assertInfoField(env, 'idx2', 'attributes', info)

    # env.execute_command('FT.CREATE', 'idx3', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '14', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'INITIAL_CAP', '10', 'M', '64', 'EF_CONSTRUCTION', '400', 'EF_RUNTIME', '50')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'HNSW', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'M', '64', 'EF_CONSTRUCTION', '400', 'EF_RUNTIME', '50']]
    # assertInfoField(env, 'idx3', 'attributes', info)

    # env.execute_command('FT.CREATE', 'idx4', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'INT64', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'HNSW', 'TYPE', 'INT64', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'M', '16', 'EF_CONSTRUCTION', '200', 'EF_RUNTIME', '10']]
    # assertInfoField(env, 'idx4', 'attributes', info)

    # env.execute_command('FT.CREATE', 'idx5', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'FLAT', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'BLOCK_SIZE', str(1024 * 1024)]]
    # assertInfoField(env, 'idx5', 'attributes', info)


def test_create_multiple_vector_fields():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    env.skipOnCluster()
    dim = 2
    conn = getConnectionByEnv(env)
    # Create index with 2 vector fields, where the first is a prefix of the second.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'COSINE',
               'v_flat', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    # Validate each index type.
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v")
    env.assertEqual(info_data[:2], ['ALGORITHM', 'HNSW'])
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v_flat")
    env.assertEqual(info_data[:2], ['ALGORITHM', 'FLAT'])

    # Insert one vector only to each index, validate it was inserted only to the right index.
    conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v")
    env.assertEqual(info_data[8:10], ['INDEX_SIZE', 1])
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v_flat")
    env.assertEqual(info_data[8:10], ['INDEX_SIZE', 0])

    conn.execute_command('HSET', 'b', 'v_flat', 'bbbbbbbb')
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v")
    env.assertEqual(info_data[8:10], ['INDEX_SIZE', 1])
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v_flat")
    env.assertEqual(info_data[8:10], ['INDEX_SIZE', 1])

    # Search in every index once, validate it was performed only to the right index.
    env.cmd('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh')
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v")
    env.assertEqual(info_data[-2:], ['LAST_SEARCH_MODE', 'STANDARD_KNN'])
    info_data = env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v_flat")
    env.assertEqual(info_data[-2:], ['LAST_SEARCH_MODE', 'EMPTY_MODE'])


def testCreateErrors():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    # missing init args
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR').error().contains('Bad arguments for vector similarity algorithm')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT').error().contains('Bad arguments for vector similarity number of parameters')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6').error().contains('Expected 6 parameters but got 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '1').error().contains('Bad number of arguments for vector similarity index: got 1 but expected even number')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '2', 'SIZE').error().contains('Bad arguments for algorithm FLAT: SIZE')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '2', 'TYPE').error().contains('Bad arguments for vector similarity FLAT index type')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'TYPE', 'FLOAT32', 'DIM').error().contains('Bad arguments for vector similarity FLAT index dim')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create FLAT index without specifying TYPE argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create FLAT index without specifying DIM argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '4', 'TYPE', 'FLOAT32', 'DIM', '1024').error().contains('Missing mandatory parameter: cannot create FLAT index without specifying DISTANCE_METRIC argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC').error().contains('Bad arguments for vector similarity FLAT index metric')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW').error().contains('Bad arguments for vector similarity number of parameters')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6').error().contains('Expected 6 parameters but got 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '1').error().contains('Bad number of arguments for vector similarity index: got 1 but expected even number')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '2', 'SIZE').error().contains('Bad arguments for algorithm HNSW: SIZE')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '2', 'TYPE').error().contains('Bad arguments for vector similarity HNSW index type')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'TYPE', 'FLOAT32', 'DIM').error().contains('Bad arguments for vector similarity HNSW index dim')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create HNSW index without specifying TYPE argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'IP').error().contains('Missing mandatory parameter: cannot create HNSW index without specifying DIM argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '4', 'TYPE', 'FLOAT32', 'DIM', '1024').error().contains('Missing mandatory parameter: cannot create HNSW index without specifying DISTANCE_METRIC argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC').error().contains('Bad arguments for vector similarity HNSW index metric')

    # invalid init args
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'DOUBLE', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity HNSW index type')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', 'str', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity HNSW index dim')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'REDIS').error().contains('Bad arguments for vector similarity HNSW index metric')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'REDIS', '6', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP').error().contains('Bad arguments for vector similarity algorithm')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', 'str', 'BLOCK_SIZE', '16') \
        .error().contains('Bad arguments for vector similarity FLAT index initial cap')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '10', 'BLOCK_SIZE', 'str') \
        .error().contains('Bad arguments for vector similarity FLAT index blocksize')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', 'str', 'M', '16', 'EF_CONSTRUCTION', '200') \
        .error().contains('Bad arguments for vector similarity HNSW index initial cap')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', 'str', 'EF_CONSTRUCTION', '200') \
        .error().contains('Bad arguments for vector similarity HNSW index m')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EF_CONSTRUCTION', 'str') \
        .error().contains('Bad arguments for vector similarity HNSW index efConstruction')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '100', 'M', '16', 'EF_RUNTIME', 'str') \
        .error().contains('Bad arguments for vector similarity HNSW index efRuntime')


def testSearchErrors():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 's', 'TEXT', 't', 'TAG', 'SORTABLE', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '10', 'M', '16', 'EF_CONSTRUCTION', '200')
    conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
    conn.execute_command('HSET', 'b', 'v', 'bbbbbbbb')
    conn.execute_command('HSET', 'c', 'v', 'cccccccc')
    conn.execute_command('HSET', 'd', 'v', 'dddddddd')

    env.expect('FT.SEARCH', 'idx', '*=>[REDIS 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN str @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')

    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefg').error().contains('Error parsing vector similarity query: query vector blob size (7) does not match index\'s expected size (8).')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefghi').error().contains('Error parsing vector similarity query: query vector blob size (9) does not match index\'s expected size (8).')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @t $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([0]) # wrong field
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS v]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `v` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS s]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `s` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS t]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Property `t` already exists in schema')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b AS $score]', 'PARAMS', '4', 'score', 't', 'b', 'abcdefgh').error().contains('Property `t` already exists in schema')

    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_RUNTIME -42]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Attribute not supported for term')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_RUNTIME 2.71828]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Attribute not supported for term')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_RUNTIME 5 EF_RUNTIME 6]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Field was specified twice')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b EF_FUNTIME 30]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Error parsing vector similarity parameters: Invalid option')


def load_vectors_with_texts_into_redis(con, vector_field, dim, num_vectors):
    id_vec_list = []
    p = con.pipeline(transaction=False)
    for i in range(1, num_vectors+1):
        vector = np.float32([i for j in range(dim)])
        con.execute_command('HSET', i, vector_field, vector.tobytes(), 't', 'text value')
        id_vec_list.append((i, vector))
    p.execute()
    return id_vec_list


def test_with_fields():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 128
    qty = 100

    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2', 't', 'TEXT')
    load_vectors_with_texts_into_redis(conn, 'v', dimension, qty)

    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        query_data = np.float32(np.random.random((1, dimension)))
        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 100 @v $vec_param AS score]',
                        'SORTBY', 'score', 'PARAMS', 2, 'vec_param', query_data.tobytes(),
                        'RETURN', 2, 'score', 't')
        res_nocontent = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 100 @v $vec_param AS score]',
                        'SORTBY', 'score', 'PARAMS', 2, 'vec_param', query_data.tobytes(),
                        'NOCONTENT')
        env.assertEqual(res[1::2], res_nocontent[1:])
        env.assertEqual('t', res[2][2])


def get_vecsim_memory(env, index_key, field_name):
    return float(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", index_key, field_name))["MEMORY"])/0x100000


def get_vecsim_index_size(env, index_key, field_name):
    return int(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", index_key, field_name))["INDEX_SIZE"])


def test_memory_info():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    # This test flow adds two vectors and deletes them. The test checks for memory increase in Redis and RediSearch upon insertion and decrease upon delete.
    conn = getConnectionByEnv(env)
    dimension = 128
    index_key = 'idx'
    vector_field = 'v'

    # Create index. Flat index implementation will free memory when deleting vectors, so it is a good candidate for this test with respect to memory consumption.
    env.execute_command('FT.CREATE', index_key, 'SCHEMA', vector_field, 'VECTOR', 'FLAT', '8', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2', 'BLOCK_SiZE', '1')
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


def execute_hybrid_query(env, query_string, query_data, non_vector_field, sort_by_vector=True, sort_by_non_vector_field=False,
                         hybrid_mode='HYBRID_BATCHES'):
    if sort_by_vector:
        ret = env.expect('FT.SEARCH', 'idx', query_string,
                   'SORTBY', '__v_score',
                   'PARAMS', 2, 'vec_param', query_data.tobytes(),
                   'RETURN', 2, '__v_score', non_vector_field, 'LIMIT', 0, 10)

    else:
        if sort_by_non_vector_field:
            ret = env.expect('FT.SEARCH', 'idx', query_string, 'WITHSCORES',
                'SORTBY', non_vector_field,
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, non_vector_field, '__v_score', 'LIMIT', 0, 10)

        else:
            ret = env.expect('FT.SEARCH', 'idx', query_string, 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, non_vector_field, '__v_score', 'LIMIT', 0, 10)

    # in cluster mode, we send `_FT.DEBUG' to the local shard.
    prefix = '_' if env.isCluster() else ''
    # returns the test name and line number from called this helper function.
    caller_pos = f'{sys._getframe(1).f_code.co_filename.split("/")[-1]}:{sys._getframe(1).f_lineno}'
    env.assertEqual(env.cmd(prefix+"FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], hybrid_mode, message=caller_pos)
    return ret


def test_hybrid_query_batches_mode_with_text():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    # Index size is chosen so that batches mode will be selected by the heuristics.
    dim = 2
    index_size = 6000 * env.shardsCount
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 't', 'TEXT').ok()
    load_vectors_with_texts_into_redis(conn, 'v', dim, index_size)
    query_data = np.full(dim, index_size, dtype='float32')

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
        vector = np.full(dim, 5*i, dtype='float32')
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
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    # Index size is chosen so that batches mode will be selected by the heuristics.
    dim = 2
    index_size = 6000 * env.shardsCount
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
                        'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 'tags', 'TAG')

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = np.full(dim, i, dtype='float32')
        conn.execute_command('HSET', i, 'v', vector.tobytes(), 'tags', 'hybrid')
    p.execute()

    query_data = np.full(dim, index_size/2, dtype='float32')

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
        vector = np.full(dim, 5*i, dtype='float32')
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
                         sort_by_vector=False).equal(expected_res)


def test_hybrid_query_with_numeric():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    index_size = 6000 * env.shardsCount
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 1000, 'num', 'NUMERIC').ok()

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = np.full(dim, i, dtype='float32')
        conn.execute_command('HSET', i, 'v', vector.tobytes(), 'num', i)
    p.execute()

    query_data = np.full(dim, index_size, dtype='float32')
    expected_res = [10]
    # Expect to get result in reverse order to the id, starting from the max id in the index.
    for i in range(10):
        expected_res.append(str(index_size-i))
        expected_res.append(['__v_score', str(dim*i**2), 'num', str(index_size-i)])

    execute_hybrid_query(env, '(@num:[0 {}])=>[KNN 10 @v $vec_param]'.format(index_size), query_data, 'num').equal(expected_res)
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
    execute_hybrid_query(env, '(@num:[-inf {}])=>[KNN 10 @v $vec_param]'.format(index_size-lower_bound_num), query_data, 'num',
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
    prefix = "_" if env.isCluster() else ""
    env.assertEqual(env.cmd(prefix+"FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], 'HYBRID_ADHOC_BF')


def test_hybrid_query_with_geo():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 'coordinate', 'GEO').ok()

    index_size = 1000   # for this index size, ADHOC BF mode will always be selected by the heuristics.
    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = np.full(dim, i, dtype='float32')
        conn.execute_command('HSET', i, 'v', vector.tobytes(), 'coordinate', str(i/100)+","+str(i/100))
    p.execute()
    if not env.isCluster():
        env.assertEqual(get_vecsim_index_size(env, 'idx', 'v'), index_size)

    query_data = np.full(dim, index_size, dtype='float32')
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
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 4
    index_size = 6000 * env.shardsCount
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
                        'DIM', dimension, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 'num', 'NUMERIC',
                        't1', 'TEXT', 't2', 'TEXT')

    p = conn.pipeline(transaction=False)
    close_vector = np.full(dimension, 1, dtype='float32')
    distant_vector = np.full(dimension, 10, dtype='float32')
    conn.execute_command('HSET', 1, 'v', close_vector.tobytes(), 'num', 1, 't1', 'text value', 't2', 'hybrid query')
    conn.execute_command('HSET', 2, 'v', distant_vector.tobytes(), 'num', 2, 't1', 'text value', 't2', 'hybrid query')
    conn.execute_command('HSET', 3, 'v', distant_vector.tobytes(), 'num', 3, 't1', 'other', 't2', 'hybrid query')
    conn.execute_command('HSET', 4, 'v', close_vector.tobytes(), 'num', 4, 't1', 'other', 't2', 'hybrid query')
    for i in range(5, index_size+1):
        further_vector = np.full(dimension, i, dtype='float32')
        conn.execute_command('HSET', i, 'v', further_vector.tobytes(), 'num', i, 't1', 'text value', 't2', 'hybrid query')
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
    prefix = "_" if env.isCluster() else ""
    env.assertEqual(env.cmd(prefix+"FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], 'HYBRID_BATCHES')

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
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 128
    qty = 100
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
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
    execute_hybrid_query(env, '((text ~value)|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False,
                         hybrid_mode='HYBRID_ADHOC_BF').equal(expected_res_1)
    execute_hybrid_query(env, '((text ~value)|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False,
                         sort_by_non_vector_field=True, hybrid_mode='HYBRID_ADHOC_BF').equal(expected_res_1)

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
                         hybrid_mode='HYBRID_ADHOC_BF').equal(expected_res_2)
    execute_hybrid_query(env, '(%test%|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False,
                         sort_by_non_vector_field=True, hybrid_mode='HYBRID_ADHOC_BF').equal(expected_res_2)

    # use TFIDF.DOCNORM scorer
    expected_res_3 = [10,
                      '100', '3', ['__v_score', '0', 't', 'other'],
                      '91', '0.33333333333333331', ['__v_score', '10368', 't', 'text value'],
                      '92', '0.33333333333333331', ['__v_score', '8192', 't', 'text value'],
                      '93', '0.33333333333333331', ['__v_score', '6272', 't', 'text value'],
                      '94', '0.33333333333333331', ['__v_score', '4608', 't', 'text value'],
                      '95', '0.33333333333333331', ['__v_score', '3200', 't', 'text value'],
                      '96', '0.33333333333333331', ['__v_score', '2048', 't', 'text value'],
                      '97', '0.33333333333333331', ['__v_score', '1152', 't', 'text value'],
                      '98', '0.33333333333333331', ['__v_score', '512', 't', 'text value'],
                      '99', '0.33333333333333331', ['__v_score', '128', 't', 'text value']]
    res = env.cmd('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'TFIDF.DOCNORM', 'WITHSCORES',
               'PARAMS', 2, 'vec_param', query_data.tobytes(),
               'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10)
    compare_lists(env, res, expected_res_3, delta=0.01)

    # Those scorers are scoring per shard.
    if not env.isCluster():
        # use BM25 scorer
        expected_res_4 = [10, '100', '0.72815531789441912', ['__v_score', '0', 't', 'other'], '91', '0.24271843929813972', ['__v_score', '10368', 't', 'text value'], '92', '0.24271843929813972', ['__v_score', '8192', 't', 'text value'], '93', '0.24271843929813972', ['__v_score', '6272', 't', 'text value'], '94', '0.24271843929813972', ['__v_score', '4608', 't', 'text value'], '95', '0.24271843929813972', ['__v_score', '3200', 't', 'text value'], '96', '0.24271843929813972', ['__v_score', '2048', 't', 'text value'], '97', '0.24271843929813972', ['__v_score', '1152', 't', 'text value'], '98', '0.24271843929813972', ['__v_score', '512', 't', 'text value'], '99', '0.24271843929813972', ['__v_score', '128', 't', 'text value']]
        res = env.cmd('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'BM25', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10)
        compare_lists(env, res, expected_res_4, delta=0.01)

        # use DISMAX scorer
        expected_res_5 = [10, '91', '1', ['__v_score', '10368', 't', 'text value'], '92', '1', ['__v_score', '8192', 't', 'text value'], '93', '1', ['__v_score', '6272', 't', 'text value'], '94', '1', ['__v_score', '4608', 't', 'text value'], '95', '1', ['__v_score', '3200', 't', 'text value'], '96', '1', ['__v_score', '2048', 't', 'text value'], '97', '1', ['__v_score', '1152', 't', 'text value'], '98', '1', ['__v_score', '512', 't', 'text value'], '99', '1', ['__v_score', '128', 't', 'text value'], '100', '1', ['__v_score', '0', 't', 'other']]
        env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'DISMAX', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10).equal(expected_res_5)

        # use DOCSCORE scorer
        env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'DOCSCORE', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 100).equal(expected_res_5)


def test_single_entry():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    SkipOnNonCluster(env)
    # This test should test 3 shards with only one entry. 2 shards should return an empty response to the coordinator.
    # Execution should finish without failure.
    conn = getConnectionByEnv(env)
    dimension = 128
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2').ok()
    vector = np.random.rand(1, dimension).astype(np.float32)
    conn.execute_command('HSET', 0, 'v', vector.tobytes())

    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 10 @v $vec_param]',
                'SORTBY', '__v_score',
                'RETURN', '0',
                'PARAMS', 2, 'vec_param', vector.tobytes()).equal([1, '0'])


def test_hybrid_query_adhoc_bf_mode():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 128
    qty = 100
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', dimension, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 't', 'TEXT').ok()
    load_vectors_with_texts_into_redis(conn, 'v', dimension, qty)

    # Change the text value to 'other' for 10 vectors (with id 10, 20, ..., 100)
    for i in range(1, 11):
        vector = np.float32([10*i for j in range(dimension)])
        conn.execute_command('HSET', 10*i, 'v', vector.tobytes(), 't', 'other')

    # Expect to get only vector that passes the filter (i.e, has "other" in text field)
    # Expect also that heuristics will choose adhoc BF over batches.
    query_data = np.float32([100 for j in range(dimension)])

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

    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        execute_hybrid_query(env, '(other)=>[KNN 10 @v $vec_param]', query_data, 't',
                             hybrid_mode='HYBRID_ADHOC_BF').equal(expected_res)


def test_wrong_vector_size():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dimension = 128

    vector = np.random.rand(1+dimension).astype(np.float32)
    conn.execute_command('HSET', '0', 'v', vector[:dimension-1].tobytes())
    conn.execute_command('HSET', '1', 'v', vector[:dimension].tobytes())
    conn.execute_command('HSET', '2', 'v', vector[:dimension+1].tobytes())

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2').ok()
    waitForIndex(env, 'idx')

    vector = np.random.rand(1+dimension).astype(np.float32)
    conn.execute_command('HSET', '3', 'v', vector[:dimension-1].tobytes())
    conn.execute_command('HSET', '4', 'v', vector[:dimension].tobytes())
    conn.execute_command('HSET', '5', 'v', vector[:dimension+1].tobytes())

    waitForIndex(env, 'idx')
    assertInfoField(env, 'idx', 'num_docs', '2')
    assertInfoField(env, 'idx', 'hash_indexing_failures', '4')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 6 @v $q]', 'NOCONTENT', 'PARAMS', 2, 'q', np.ones(dimension, 'float32').tobytes()).equal([2, '1', '4'])

def test_hybrid_query_cosine():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 4
    index_size = 6000 * env.shardsCount
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'COSINE', 't', 'TEXT').ok()

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        first_coordinate = np.float32([float(i)/index_size])
        vector = np.concatenate((first_coordinate, np.ones(dim-1, dtype='float32')))
        conn.execute_command('HSET', i, 'v', vector.tobytes(), 't', 'text value')
    p.execute()

    query_data = np.ones(dim, dtype='float32')

    expected_res_ids = [str(index_size-i) for i in range(15)]
    res = env.execute_command('FT.SEARCH', 'idx', '(text value)=>[KNN 10 @v $vec_param]',
                              'SORTBY', '__v_score',
                              'PARAMS', 2, 'vec_param', query_data.tobytes(),
                              'RETURN', 0)
    prefix = "_" if env.isCluster() else ""
    env.assertEqual(env.cmd(prefix + "FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], 'HYBRID_BATCHES')
    # The order of ids is not accurate due to floating point numeric errors, but the top k should be
    # in the last 15 ids.
    actual_res_ids = [res[1:][i] for i in range(10)]
    for res_id in actual_res_ids:
        env.assertContains(res_id, expected_res_ids)

    # Change the text value to 'other' for 10 vectors (with id 10, 20, ..., index_size)
    for i in range(1, int(index_size/10) + 1):
        first_coordinate = np.float32([float(10*i)/index_size])
        vector = np.concatenate((first_coordinate, np.ones(dim-1, dtype='float32')))
        conn.execute_command('HSET', 10*i, 'v', vector.tobytes(), 't', 'other')

    # Expect to get only vector that passes the filter (i.e, has "other" in text field)
    expected_res_ids = [str(index_size-10*i) for i in range(10)]
    res = env.execute_command('FT.SEARCH', 'idx', '(other)=>[KNN 10 @v $vec_param]',
                              'SORTBY', '__v_score',
                              'PARAMS', 2, 'vec_param', query_data.tobytes(),
                              'RETURN', 0)
    env.assertEqual(env.cmd(prefix+"FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], 'HYBRID_ADHOC_BF')
    actual_res_ids = [res[1:][i] for i in range(10)]
    for res_id in actual_res_ids:
        env.assertContains(res_id, expected_res_ids)

def test_fail_ft_aggregate():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    dim = 1
    conn = getConnectionByEnv(env)
    one_vector = np.full((1, 1), 1, dtype = np.float32)
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
                        'DIM', dim, 'DISTANCE_METRIC', 'COSINE')
    conn.execute_command("HSET", "i", "v", one_vector.tobytes())
    res = env.expect("FT.AGGREGATE", "idx", "*=>[KNN 10 @v $BLOB]", "PARAMS", 2, "BLOB", one_vector.tobytes())
    if not env.isCluster():
        res.error().contains("VSS is not yet supported on FT.AGGREGATE")
    else:
        # Currently coordinator does not return errors returned from shard during shard execution. It returns empty list
        res.equal([0])

def test_fail_on_v1_dialect():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    dim = 1
    conn = getConnectionByEnv(env)
    one_vector = np.full((1, 1), 1, dtype = np.float32)
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
                        'DIM', dim, 'DISTANCE_METRIC', 'COSINE')
    conn.execute_command("HSET", "i", "v", one_vector.tobytes())
    res = env.expect("FT.SEARCH", "idx", "*=>[KNN 10 @v $BLOB]", "PARAMS", 2, "BLOB", one_vector.tobytes())
    res.error().contains("Syntax error")


def test_hybrid_query_with_global_filters():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    index_size = 1000
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 't', 'TEXT', 'num', 'NUMERIC', 'coordinate', 'GEO').ok()

    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = np.full(dim, i, dtype='float32')
        conn.execute_command('HSET', i, 'v', vector.tobytes(), 't', 'hybrid', 'num', i, 'coordinate',
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

    # Test legacy numeric and geo global filters
    expected_res = [10]
    # Expect to get top 10 ids where maximum is index_size/2 in the index (due to the numeric filter).
    for i in range(10):
        expected_res.append(str(int(index_size/2-i)))
        expected_res.append(['__v_score', str(int(dim*(index_size/2+i)**2))])
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 10 @v $vec_param]', 'filter', 'num', 0, index_size/2, 'SORTBY', '__v_score',
               'RETURN', 1, '__v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes()).equal(expected_res)

    # Expect to get top 10 ids where maximum is 31 in the index (due to the geo filter).
    expected_res = [10]
    for i in range(10):
        expected_res.append(str(31-i))
        expected_res.append(['coordinate', str((31-i)/100)+","+str((31-i)/100)])
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 10 @v $vec_param]', 'geofilter', 'coordinate', 0.0, 0.0, 50, 'km',
               'SORTBY', '__v_score', 'RETURN', 1, 'coordinate',
               'PARAMS', 2, 'vec_param', query_data.tobytes()).equal(expected_res)

    # Test complex query with multiple global filters - this query applies 4 global filters:
    # 2 numeric filters - the second filter is a subset of the first one - enforces that the result ids
    # will be between index_size/3 to index_size/2.
    # geo filter - get all ids whose coordinates are in 50 km radius from (5.0, 5.0) - will enforce that the result ids
    # are in the range of (index_size/2 - 31, index_size/2 + 31), according to the previous query and the fact that the
    # coordinates of index_size/2 are (5.0, 5.0).
    # Finally, inkeys filter enforces that only ids that multiply by 7 are valid.
    # On top of all that, we have the hybrid query that filters out ids that multiply by 5
    # (i.e., their text field is not 'hybrid') - and this is the reason for the expected ids
    expected_res = ([str(i) for i in range(int(index_size/2), int(index_size/2) - 32, -1)
                    if i % 5 != 0 and i % 7 == 0])
    expected_res.insert(0, len(expected_res))

    inkeys = [i for i in range(index_size) if i % 7 == 0]
    env.expect('FT.SEARCH', 'idx', '(hybrid)=>[KNN 5 @v $vec_param]', 'INKEYS', len(inkeys), *inkeys,
               'filter', 'num', 0, index_size/2, 'filter', 'num', index_size/3, index_size/2,
               'geofilter', 'coordinate', 5.0, 5.0, 50, 'km', 'SORTBY', '__v_score',
               'NOCONTENT', 'PARAMS', 2, 'vec_param', query_data.tobytes()).equal(expected_res)


def test_hybrid_query_change_policy():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    n = 6000 * env.shardsCount
    np.random.seed(10)
    vectors = np.random.rand(n, dim).astype(np.float32)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'COSINE', 'tag1', 'TAG', 'tag2', 'TAG').ok()

    file = 1
    tags = range(10)
    subset_size = int(n/2)
    with conn.pipeline(transaction = False) as p:
        for i, v in enumerate(vectors[:subset_size]):
            conn.execute_command('HSET', i, 'v', v.tobytes(),
                                 'tag1', str(tags[randrange(10)]), 'tag2', 'word'+str(file))
        p.execute()

    file = 2
    with conn.pipeline(transaction = False) as p:
        for i, v in enumerate(vectors[subset_size:]):
            conn.execute_command('HSET', i + subset_size, 'v', v.tobytes(),
                                 'tag1', str(10 + tags[randrange(10)]), 'tag2', 'word'+str(file))
        p.execute()

    # This should return 10 results and run in HYBRID_BATCHES mode
    query_string = '(@tag1:{0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9} @tag2:{word1})=>[KNN 10 @v $vec_param]'
    res = execute_hybrid_query(env, query_string, vectors[0], 'tag2', hybrid_mode='HYBRID_BATCHES').res
    env.assertEqual(res[0], 10)

    # This query has 0 results, since none of the tags in @tag1 go along with 'word2' in @tag2.
    # However, the estimated number of the "child" results should be index_size/2.
    # While running the batches, the policy should change dynamically to AD-HOC BF.
    query_string = '(@tag1:{0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9} @tag2:{word2})=>[KNN 10 @v $vec_param]'
    execute_hybrid_query(env, query_string, vectors[0], 'tag2',
                         hybrid_mode='HYBRID_BATCHES_TO_ADHOC_BF').equal([0])

    # Add one valid document and re-run the query (still expect to change to AD-HOC BF)
    # This doc should return in the first batch, and then it is removed and reinserted to the results heap
    # after the policy is changed to ad-hoc.
    conn.execute_command('HSET', n, 'v', vectors[0].tobytes(),
                         'tag1', str(1), 'tag2', 'word'+str(file))
    res = execute_hybrid_query(env, query_string, vectors[0], 'tag2', hybrid_mode='HYBRID_BATCHES_TO_ADHOC_BF').res
    env.assertEqual(res[:2], [1, str(n)])


def test_system_memory_limits():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    system_memory = int(env.cmd('info', 'memory')['total_system_memory'])
    currIdx = 0

    # OK parameters
    env.assertOk(conn.execute_command('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
                                      'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 10000, 'BLOCK_SIZE', 100))
    currIdx+=1

    env.assertOk(conn.execute_command('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
                                      'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 10000))
    currIdx+=1

    # Index initial size exceeded limits
    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', system_memory).error().contains(
               f'Vector index initial capacity {system_memory} exceeded server limit')
    currIdx+=1

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', system_memory, 'BLOCK_SIZE', 100).error().contains(
               f'Vector index initial capacity {system_memory} exceeded server limit')
    currIdx+=1
    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', system_memory).error().contains(
               f'Vector index block size {system_memory} exceeded server limit')
    currIdx+=1

    # Block size with no configuration limits fails
    block_size = system_memory // (16*4) // 9 # memory needed for this block size is more than 10% of system memory
    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 0, 'BLOCK_SIZE', block_size).error().contains(
               f'Vector index block size {block_size} exceeded server limit')
    currIdx+=1

    # TODO: uncomment when BLOCK_SIZE is added to FT.CREATE on HNSW
    # env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '10', 'TYPE', 'FLOAT32',
    #            'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 0, 'BLOCK_SIZE', block_size).error().contains(
    #            f'Vector index block size {block_size} exceeded server limit')

def test_redis_memory_limits():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    used_memory = int(env.cmd('info', 'memory')['used_memory'])
    currIdx = 0

    # Config max memory (redis server memory limit)
    maxmemory = used_memory * 5
    conn.execute_command('CONFIG SET', 'maxmemory', maxmemory)

    # Index initial capacity exceeded new limits
    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', maxmemory).error().contains(
               f'Vector index initial capacity {maxmemory} exceeded server limit')
    currIdx+=1

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', maxmemory, 'BLOCK_SIZE', 100).error().contains(
               f'Vector index initial capacity {maxmemory} exceeded server limit')
    currIdx+=1

    # Block size
    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', maxmemory).error().contains(
               f'Vector index block size {maxmemory} exceeded server limit')
    currIdx+=1

    # Block size with new limits fail
    block_size = maxmemory // (16*4) // 2 # half of memory limit divided by blob size
    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', block_size).error().contains(
               f'Vector index block size {block_size} exceeded server limit')
    currIdx+=1
    # TODO: uncomment when BLOCK_SIZE is added to FT.CREATE on HNSW
    # env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '10', 'TYPE', 'FLOAT32',
    #            'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', block_size).error().contains(
    #            f'Vector index block size {block_size} exceeded server limit')

    # reset env (for clean RLTest run with env reuse)
    env.assertTrue(conn.execute_command('CONFIG SET', 'maxmemory', '0'))


def test_default_block_size_and_initial_capacity():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    env.skipOnCluster()
    conn = getConnectionByEnv(env)

    dim = 1024
    default_blockSize = 1024 # default block size
    currIdx = 0
    used_memory = None
    maxmemory = None

    def set_memory_limit(data_byte_size = 1):
        nonlocal used_memory, maxmemory
        used_memory = int(conn.execute_command('info', 'memory')['used_memory'])
        maxmemory = used_memory + (20 * 1024 * 1024) # 20MB
        conn.execute_command('CONFIG SET', 'maxmemory', maxmemory)
        return maxmemory // 10 // (dim*data_byte_size)

    def check_algorithm_and_type_combination(with_memory_limit):
        nonlocal currIdx
        exp_block_size = default_blockSize

        for algo in ['FLAT', 'HNSW']:
            if with_memory_limit:
                exp_block_size = set_memory_limit(4)
                env.assertLess(exp_block_size, default_blockSize)
                # Explicitly, the default values should fail:
                env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', algo, '8', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
                            'INITIAL_CAP', default_blockSize).error().contains(
                            f"Vector index initial capacity {default_blockSize} exceeded server limit")
                if algo == 'FLAT': # TODO: remove condition when BLOCK_SIZE is added to FT.CREATE on HNSW
                    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', algo, '10', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 0,
                            'BLOCK_SIZE', default_blockSize).error().contains(
                    f"Vector index block size {default_blockSize} exceeded server limit")

            env.assertOk(conn.execute_command('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', algo, '6',
                                                'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2'))
            debug_info = to_dict(conn.execute_command("FT.DEBUG", "VECSIM_INFO", currIdx, 'v'))
            if algo == 'FLAT': # TODO: remove condition when BLOCK_SIZE is added to FT.CREATE on HNSW
                env.assertLessEqual(debug_info['BLOCK_SIZE'], exp_block_size)
            # TODO: if we ever add INITIAL_CAP to debug data, uncomment this
            # env.assertEqual(debug_info['BLOCK_SIZE'], debug_info['INITIAL_CAP'])
            currIdx+=1

    # Test defaults with no memory limit
    check_algorithm_and_type_combination(False)

    # set memory limits and reload, to verify that we succeed to load with the new limits
    num_indexes = len(conn.execute_command('FT._LIST'))
    set_memory_limit()
    env.dumpAndReload()
    env.assertEqual(num_indexes, len(conn.execute_command('FT._LIST')))

    # Test defaults with memory limit
    check_algorithm_and_type_combination(True)

    # reset env (for clean RLTest run with env reuse)
    env.assertTrue(conn.execute_command('CONFIG SET', 'maxmemory', '0'))

def test_redisearch_memory_limit():
    # test block size with VSS_MAX_RESIZE_MB configure
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    env.skipOnCluster()
    conn = getConnectionByEnv(env)

    used_memory = int(env.cmd('info', 'memory')['used_memory'])
    maxmemory = used_memory * 5
    conn.execute_command('CONFIG SET', 'maxmemory', maxmemory)
    currIdx = 0
    block_size = maxmemory // (16*4) // 2 # half of memory limit divided by blob size

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
                'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', block_size).error().contains(
                f'Vector index block size {block_size} exceeded server limit')
    currIdx+=1
    # TODO: uncomment when BLOCK_SIZE is added to FT.CREATE on HNSW
    # env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '10', 'TYPE', 'FLOAT32',
    #            'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', block_size).error().contains(
    #            f'Vector index block size {block_size} exceeded server limit')
    # currIdx+=1

    env.expect('FT.CONFIG', 'SET', 'VSS_MAX_RESIZE', maxmemory).ok()

    env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
                'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', block_size).ok()
    # TODO: uncomment when BLOCK_SIZE is added to FT.CREATE on HNSW
    # currIdx+=1
    # env.expect('FT.CREATE', currIdx, 'SCHEMA', 'v', 'VECTOR', 'HNSW', '10', 'TYPE', 'FLOAT32',
    #            'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', 100, 'BLOCK_SIZE', block_size).ok()

    # reset env (for clean RLTest run with env reuse)
    env.assertTrue(conn.execute_command('CONFIG SET', 'maxmemory', '0'))
    env.expect('FT.CONFIG', 'SET', 'VSS_MAX_RESIZE', '0').ok()

def test_rdb_memory_limit():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    env.skipOnCluster()
    conn = getConnectionByEnv(env)

    used_memory = int(env.cmd('info', 'memory')['used_memory'])
    maxmemory = used_memory * 5
    block_size = maxmemory // (16*4) // 2 # half of memory limit divided by blob size

    # succeed to create indexes with no limits
    env.expect('FT.CREATE', 'idx-flat', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '10', 'TYPE', 'FLOAT32',
               'DIM', '16', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', block_size, 'BLOCK_SIZE', block_size).ok()
    # TODO: add block size to HNSW index for testing change in block size when block size is available
    env.expect('FT.CREATE', 'idx-hnsw', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', '128', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', block_size).ok()
    # sets memory limit
    env.assertTrue(conn.execute_command('CONFIG SET', 'maxmemory', maxmemory))

    # The actual test: try creating indexes from rdb.
    # should succeed after changing initial cap and block size to 0 and default
    env.dumpAndReload()

    info_data = to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx-flat", "v"))
    env.assertNotEqual(info_data['BLOCK_SIZE'], block_size)
    # TODO: if we ever add INITIAL_CAP to debug data, add check here
    # TODO: uncomment when BLOCK_SIZE is added to FT.CREATE on HNSW
    # info_data = to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx-hnsw", "v"))
    # env.assertNotEqual(info_data['BLOCK_SIZE'], block_size)
    # # TODO: if we ever add INITIAL_CAP to debug data, add check here

    # reset env (for clean RLTest run with env reuse)
    env.assertTrue(conn.execute_command('CONFIG SET', 'maxmemory', '0'))

def test_timeout_reached():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT FAIL')
    conn = getConnectionByEnv(env)
    nshards = env.shardsCount
    timeout_expected = 0 if env.isCluster() else 'Timeout limit was reached'
    prefix = "_" if env.isCluster() else ""

    vecsim_algorithms_and_sizes = [('FLAT', 80000 * nshards), ('HNSW', 80000 * nshards)]
    hybrid_modes = ['HYBRID_BATCHES', 'HYBRID_ADHOC_BF']
    dim = 10

    for algo, n_vec in vecsim_algorithms_and_sizes:
        # succeed to create indexes with no limits
        query_vec = load_vectors_to_redis(env, n_vec, 0, dim)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', algo, '8', 'TYPE', 'FLOAT32',
                   'DIM', dim, 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', n_vec, 't', 'TEXT').ok()
        waitForIndex(env, 'idx')

        # STANDART KNN
        # run query with no timeout. should succeed.
        res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_vec,
                                   'PARAMS', 4, 'K', n_vec, 'vec_param', query_vec.tobytes(),
                                   'TIMEOUT', 0)
        env.assertEqual(res[0], n_vec)
        # run query with 1 milisecond timeout. should fail.
        res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_vec,
                                   'PARAMS', 4, 'K', n_vec, 'vec_param', query_vec.tobytes(),
                                   'TIMEOUT', 1)
        env.assertEqual(res[0], timeout_expected)

        # HYBRID MODES
        # Todo: uncomment when hybrid optional arguments is merged, in the meantime arrange manually settings
        #  that will test both modes.
        for i in range(int(n_vec/20)):
            vector = np.random.rand(1, dim).astype(np.float32)
            conn.execute_command('HSET', n_vec+i, 'vector', vector.tobytes(), 't', 'dummy')

        for mode in hybrid_modes:
            # to trigger ad-hoc BF, use a filter that is passed by a ~5% of the vectors, and for batches use a filter
            # that is passed by ~95% of the vectors.
            hybrid_query_filter = "dummy" if mode == 'HYBRID_ADHOC_BF' else '-dummy'
            n_res = int(n_vec/20) if mode == 'HYBRID_ADHOC_BF' else n_vec
            res = conn.execute_command('FT.SEARCH', 'idx', f'({hybrid_query_filter})=>[KNN $K @vector $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_res,
                                       'PARAMS', 4, 'K', n_res, 'vec_param', query_vec.tobytes(),
                                       'TIMEOUT', 0)
            env.assertEqual(res[0], n_res)
            env.assertEqual(env.cmd(prefix + "FT.DEBUG", "VECSIM_INFO", "idx", "vector")[-1], mode)

            res = conn.execute_command('FT.SEARCH', 'idx', f'({hybrid_query_filter})=>[KNN $K @vector $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_res,
                                       'PARAMS', 4, 'K', n_res, 'vec_param', query_vec.tobytes(),
                                       'TIMEOUT', 1)
            env.assertEqual(res[0], timeout_expected)
            env.assertEqual(env.cmd(prefix + "FT.DEBUG", "VECSIM_INFO", "idx", "vector")[-1], mode)

        conn.flushall()


def test_score_name_case_sensitivity():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2

    k = 10
    query_blob = np.array([0] * dim).astype(np.float32).tobytes()
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
        conn.execute_command("HSET", f'doc{i}', vec_fieldname, np.array([i] * dim).astype(np.float32).tobytes())

    # Test AS
    res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB AS {score_name}]',
                               'PARAMS', 2, 'BLOB', query_blob,
                               'RETURN', '1', score_name)
    env.assertEqual(res, expected(score_name))
        # mismatch cases
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB AS {score_name}]',
               'PARAMS', 2, 'BLOB', query_blob,
               'RETURN', '1', score_name.lower()).equal(expected())
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB AS {score_name.lower()}]',
               'PARAMS', 2, 'BLOB', query_blob,
               'RETURN', '1', score_name).equal(expected())

    # Test default score name
    res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB]',
                               'PARAMS', 2, 'BLOB', query_blob,
                               'RETURN', '1', default_score_name)
    env.assertEqual(res, expected(default_score_name))
        # mismatch case
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN {k} @{vec_fieldname} $BLOB]',
               'PARAMS', 4, 'k', k, 'BLOB', query_blob,
               'RETURN', '1', score_name.lower()).equal(expected())
