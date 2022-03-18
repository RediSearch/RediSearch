# -*- coding: utf-8 -*-
import base64
import random
import string
import unittest
from time import sleep

import numpy as np
from RLTest import Env

from common import *
from includes import *
from redis.client import NEVER_DECODE


def test_sanity(env):
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


def testDel(env):
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


def testDelReuse(env):

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

def testDelReuseLarge(env):
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

def testCreate(env):
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

def testCreateErrors(env):
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


def testSearchErrors(env):
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


def test_with_fields(env):
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


def test_memory_info(env):
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
                         batches_mode=True):
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
    if batches_mode:
        env.assertEqual(env.cmd(prefix+"FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], 'HYBRID_BATCHES')
    else:
        env.assertEqual(env.cmd(prefix+"FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], 'HYBRID_ADHOC_BF')
    return ret


def test_hybrid_query_batches_mode_with_text(env):
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
    execute_hybrid_query(env, '(@t:other text)=>[KNN 10 @v $vec_param]', query_data, 't').equal([0])

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


def test_hybrid_query_batches_mode_with_tags(env):
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


def test_hybrid_query_with_numeric_and_geo(env):
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

    # Expect to get results with maximum numeric value of index_size-100
    expected_res = [10]
    for i in range(10):
        expected_res.append(str(index_size-100-i))
        expected_res.append(['__v_score', str(dim*(100+i)**2), 'num', str(index_size-100-i)])
    execute_hybrid_query(env, '(@num:[-inf {}])=>[KNN 10 @v $vec_param]'.format(index_size-100), query_data, 'num').equal(expected_res)
    execute_hybrid_query(env, '(@num:[-inf {}] | @num:[100 {}])=>[KNN 10 @v $vec_param]'
                         .format(index_size-200, index_size-100), query_data, 'num').equal(expected_res)

    # Expect for 5 results only (45-49), this will use ad-hoc BF since ratio between docs that pass the filter to
    # index size is low.
    expected_res = [5]
    expected_res.extend([str(50-i) for i in range(1, 6)])
    env.expect('FT.SEARCH', 'idx', '(@num:[45 (50])=>[KNN 10 @v $vec_param]',
               'SORTBY', '__v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes(), 'RETURN', 0).equal(expected_res)
    prefix = "_" if env.isCluster() else ""
    env.assertEqual(env.cmd(prefix+"FT.DEBUG", "VECSIM_INFO", "idx", "v")[-1], 'HYBRID_ADHOC_BF')

    # Testing with geo-filters
    env.execute_command('FT.DROPINDEX', 'idx')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', 100, 'coordinate', 'GEO').ok()

    index_size = 1000   # for this index size, ADHOC BF mode will always be selected by the heuristics.
    p = conn.pipeline(transaction=False)
    for i in range(1, index_size+1):
        vector = np.full(dim, i, dtype='float32')
        conn.execute_command('HSET', i, 'v', vector.tobytes(), 'coordinate', str(i)+","+str(i))
    p.execute()

    # Expect that ids 1-32 will pass the geo filter, and that the top 10 from these will return.
    expected_res = [10]
    for i in range(10):
        expected_res.append(str(32-i))
        expected_res.append(['coordinate', str(32-i)+","+str(32-i)])
    env.expect('FT.SEARCH', 'idx', '(@coordinate:[0.0 0.0 5000 km])=>[KNN 10 @v $vec_param]',
               'SORTBY', '__v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes(), 'RETURN', 1, 'coordinate').equal(expected_res)

    # Expect that no results will pass the filter
    execute_hybrid_query(env, '(@coordinate:[-1.0 -1.0 1 m])=>[KNN 10 @v $vec_param]', query_data, 'coordinate', batches_mode=False).equal([0])


def test_hybrid_query_batches_mode_with_complex_queries(env):
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


def test_hybrid_query_non_vector_score(env):
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
    execute_hybrid_query(env, '((text ~value)|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False, batches_mode=False).equal(expected_res_1)
    execute_hybrid_query(env, '((text ~value)|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False, sort_by_non_vector_field=True, batches_mode=False).equal(expected_res_1)

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
    execute_hybrid_query(env, '(%test%|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False, batches_mode=False).equal(expected_res_2)
    execute_hybrid_query(env, '(%test%|other)=>[KNN 10 @v $vec_param]', query_data, 't', sort_by_vector=False, sort_by_non_vector_field=True, batches_mode=False).equal(expected_res_2)

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
    env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'TFIDF.DOCNORM', 'WITHSCORES',
               'PARAMS', 2, 'vec_param', query_data.tobytes(),
               'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10).equal(expected_res_3)

    # Those scorers are scoring per shard.
    if not env.isCluster():
        # use BM25 scorer
        expected_res_4 = [10, '100', '0.72815531789441912', ['__v_score', '0', 't', 'other'], '91', '0.24271843929813972', ['__v_score', '10368', 't', 'text value'], '92', '0.24271843929813972', ['__v_score', '8192', 't', 'text value'], '93', '0.24271843929813972', ['__v_score', '6272', 't', 'text value'], '94', '0.24271843929813972', ['__v_score', '4608', 't', 'text value'], '95', '0.24271843929813972', ['__v_score', '3200', 't', 'text value'], '96', '0.24271843929813972', ['__v_score', '2048', 't', 'text value'], '97', '0.24271843929813972', ['__v_score', '1152', 't', 'text value'], '98', '0.24271843929813972', ['__v_score', '512', 't', 'text value'], '99', '0.24271843929813972', ['__v_score', '128', 't', 'text value']]
        env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'BM25', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10).equal(expected_res_4)

        # use DISMAX scorer
        expected_res_5 = [10, '91', '1', ['__v_score', '10368', 't', 'text value'], '92', '1', ['__v_score', '8192', 't', 'text value'], '93', '1', ['__v_score', '6272', 't', 'text value'], '94', '1', ['__v_score', '4608', 't', 'text value'], '95', '1', ['__v_score', '3200', 't', 'text value'], '96', '1', ['__v_score', '2048', 't', 'text value'], '97', '1', ['__v_score', '1152', 't', 'text value'], '98', '1', ['__v_score', '512', 't', 'text value'], '99', '1', ['__v_score', '128', 't', 'text value'], '100', '1', ['__v_score', '0', 't', 'other']]
        env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'DISMAX', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 10).equal(expected_res_5)

        # use DOCSCORE scorer
        env.expect('FT.SEARCH', 'idx', '(text|other)=>[KNN 10 @v $vec_param]', 'SCORER', 'DOCSCORE', 'WITHSCORES',
                'PARAMS', 2, 'vec_param', query_data.tobytes(),
                'RETURN', 2, 't', '__v_score', 'LIMIT', 0, 100).equal(expected_res_5)


def test_single_entry(env):
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


def test_hybrid_query_adhoc_bf_mode(env):
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
        execute_hybrid_query(env, '(other)=>[KNN 10 @v $vec_param]', query_data, 't', batches_mode=False).equal(expected_res)


def test_wrong_vector_size(env):
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

def test_hybrid_query_cosine(env):
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

def test_fail_ft_aggregate(env):
    dim = 1
    conn = getConnectionByEnv(env)
    index_size = 6000 * env.shardsCount
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
