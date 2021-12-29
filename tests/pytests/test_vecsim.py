# -*- coding: utf-8 -*-
import base64
import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env
import random
import string
import numpy as np

def test_sanity(env):
    conn = getConnectionByEnv(env)
    vecsim_type = ['FLAT', 'HNSW']
    for vs_type in vecsim_type:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')
        conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
        conn.execute_command('HSET', 'b', 'v', 'aaaabaaa')
        conn.execute_command('HSET', 'c', 'v', 'aaaaabaa')
        conn.execute_command('HSET', 'd', 'v', 'aaaaaaba')

        res = [4L, 'a', ['v_score', '0', 'v', 'aaaaaaaa'],
                   'b', ['v_score', '3.09485009821e+26', 'v', 'aaaabaaa'],
                   'c', ['v_score', '2.02824096037e+31', 'v', 'aaaaabaa'],
                   'd', ['v_score', '1.32922799578e+36', 'v', 'aaaaaaba']]
        res1 = conn.execute_command('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $blob AS score]', 'PARAMS', '2', 'blob', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC') # NEW API TEST
        env.assertEqual(res, res1)

        # todo: make test work on coordinator
        res = [4L, 'c', ['v_score', '0', 'v', 'aaaaabaa'],
                   'b', ['v_score', '2.01242627636e+31', 'v', 'aaaabaaa'],
                   'a', ['v_score', '2.02824096037e+31', 'v', 'aaaaaaaa'],
                   'd', ['v_score', '1.31886368448e+36', 'v', 'aaaaaaba']]
        res1 = conn.execute_command('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $blob AS score]', 'PARAMS', '2', 'blob', 'aaaaabaa', 'SORTBY', 'v_score', 'ASC') # NEW API TEST
        env.assertEqual(res, res1)

        expected_res = ['v_score', '0', 'v', 'aaaaaaaa']
        res = conn.execute_command('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $blob AS score]', 'PARAMS', '2', 'blob', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1) # NEW API TEST
        env.assertEqual(res[2], expected_res)

        #####################
        ## another example ##
        #####################
        message = 'aaaaabaa'
        res = conn.execute_command('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $b AS score]', 'PARAMS', '2', 'b', message, 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1) # NEW API TEST
        env.assertEqual(res[2], ['v_score', '0', 'v', 'aaaaabaa'])

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

def testEscape(env):
    conn = getConnectionByEnv(env)

    vecsim_type = ['FLAT', 'HNSW']
    for vs_type in vecsim_type:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')
        conn.execute_command('HSET', 'a', 'v', '////////')
        conn.execute_command('HSET', 'b', 'v', '++++++++')
        conn.execute_command('HSET', 'c', 'v', 'abcdefgh')
        conn.execute_command('HSET', 'd', 'v', 'aacdefgh')
        conn.execute_command('HSET', 'e', 'v', 'aaadefgh')

        messages = ['\+\+\+\+\+\+\+\+', '\/\/\/\/\/\/\/\/', 'abcdefgh', 'aacdefgh', 'aaadefgh']
        for message in messages:
            res = conn.execute_command('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $b AS score]', 'PARAMS', '2', 'b', message, 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1) # NEW API TEST
            env.assertEqual(res[2][3], message.replace('\\', ''))

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

def testDel(env):
    conn = getConnectionByEnv(env)
    vecsim_type = ['FLAT', 'HNSW']
    for vs_type in vecsim_type:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')

        conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
        conn.execute_command('HSET', 'b', 'v', 'aaaaaaba')
        conn.execute_command('HSET', 'c', 'v', 'aaaabaaa')
        conn.execute_command('HSET', 'd', 'v', 'aaaaabaa')

        expected_res = ['a', ['v_score', '0', 'v', 'aaaaaaaa'], 'c', ['v_score', '3.09485009821e+26', 'v', 'aaaabaaa'],
                        'd', ['v_score', '2.02824096037e+31', 'v', 'aaaaabaa'], 'b', ['v_score', '1.32922799578e+36', 'v', 'aaaaaaba']]

        res = env.cmd('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1) # NEW API TEST
        env.assertEqual(res[1:3], expected_res[0:2])
        
        res = env.cmd('FT.SEARCH', 'idx', '*=>[TOP_K 2 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 2) # NEW API TEST
        env.assertEqual(res[1:5], expected_res[0:4])
        
        res = env.cmd('FT.SEARCH', 'idx', '*=>[TOP_K 3 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 3) # NEW API TEST
        env.assertEqual(res[1:7], expected_res[0:6])
        
        res = env.cmd('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 4) # NEW API TEST
        env.assertEqual(res[1:9], expected_res[0:8])
        
        conn.execute_command('DEL', 'a')
        
        res = ['d', ['v_score', '3.09485009821e+26', 'v', 'aaaabaaa'],
               'b', ['v_score', '2.02824096037e+31', 'v', 'aaaaabaa'],
               'c', ['v_score', '1.32922799578e+36', 'v', 'aaaaaaba']]
        res = env.cmd('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1) # NEW API TEST
        env.assertEqual(res[1:3], res[1:3])
        res = env.cmd('FT.SEARCH', 'idx', '*=>[TOP_K 2 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 2) # NEW API TEST
        env.assertEqual(res[1:5], res[1:5])
        res = env.cmd('FT.SEARCH', 'idx', '*=>[TOP_K 3 @v $b AS score]', 'PARAMS', '2', 'b', 'aaaaaaaa', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 3) # NEW API TEST
        env.assertEqual(res[1:7], res[1:7])

        '''
        This test returns 4 results instead of the expected 3. The HNSW library return the additional results.
        env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([3L, 'b', ['v', 'abcdefgg'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
        '''

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')


def testDelReuse(env):

    def test_query_empty(env):
        conn = getConnectionByEnv(env)
        vecsim_type = ['FLAT', 'HNSW']
        for vs_type in vecsim_type:
            conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')
            env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([0L]) # NEW API TEST
            conn.execute_command('HSET', 'a', 'v', 'redislab')
            env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([1L, 'a', ['v', 'redislab']]) # NEW API TEST
            conn.execute_command('DEL', 'a')
            env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 1 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([0L]) # NEW API TEST
            conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

    def del_insert(env):
        conn = getConnectionByEnv(env)

        conn.execute_command('DEL', 'a')
        conn.execute_command('DEL', 'b')
        conn.execute_command('DEL', 'c')
        conn.execute_command('DEL', 'd')

        env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal([0L]) # NEW API TEST

        res = [''.join(random.choice(string.lowercase) for x in range(8)),
            ''.join(random.choice(string.lowercase) for x in range(8)),
            ''.join(random.choice(string.lowercase) for x in range(8)),
            ''.join(random.choice(string.lowercase) for x in range(8))]

        conn.execute_command('HSET', 'a', 'v', res[0])
        conn.execute_command('HSET', 'b', 'v', res[1])
        conn.execute_command('HSET', 'c', 'v', res[2])
        conn.execute_command('HSET', 'd', 'v', res[3])
        return res

    # test start
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')

    vecs = del_insert(env)
    res = [4L, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal(res) # NEW API TEST

    vecs = del_insert(env)
    res = [4L, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal(res) # NEW API TEST

    vecs = del_insert(env)
    res = [4L, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '*=>[TOP_K 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').equal(res) # NEW API TEST

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
    return conn.execute_command('FT.SEARCH', idx, '*=>[TOP_K 5 @vector $v AS score]', 'PARAMS', '2', 'v', query_vec.tobytes(),
                                'SORTBY', 'vector_score', 'ASC', 'RETURN', 1, 'vector_score', 'LIMIT', 0, 5) # NEW API TEST

def testDelReuseLarge(env):
    conn = getConnectionByEnv(env)
    INDEX_NAME = 'items'
    prefix = 'item'
    n_vec = 5
    query_vec_index = 3
    vec_size = 1280

    conn.execute_command('FT.CREATE', INDEX_NAME, 'ON', 'HASH',
                         'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '1280', 'DISTANCE_METRIC', 'L2')
    for _ in range(3):
        query_vec = load_vectors_to_redis(env, n_vec, query_vec_index, vec_size)
        res = query_vector(env, INDEX_NAME, query_vec) # NEW API TEST
        for i in range(4):
            env.assertLessEqual(float(res[2 + i * 2][1]), float(res[2 + (i + 1) * 2][1]))

def testCreate(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '14', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '10', 'M', '16', 'EF_CONSTRUCTION', '200', 'EF_RUNTIME', '10')
    info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'HNSW', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'M', '16', 'EF_CONSTRUCTION', '200', 'EF_RUNTIME', '10']]
    assertInfoField(env, 'idx1', 'attributes', info)

    # Uncomment these tests when support for FLOAT64, INT32, INT64, is added.
    # Trying to run these tests right now will cause 'Bad arguments for vector similarity HNSW index type' error

    # conn.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '14', 'TYPE', 'FLOAT64', 'DIM', '4096', 'DISTANCE_METRIC', 'L2', 'INITIAL_CAP', '10', 'M', '32', 'EF_CONSTRUCTION', '100', 'EF_RUNTIME', '20')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'HNSW', 'TYPE', 'FLOAT64', 'DIM', '4096', 'DISTANCE_METRIC', 'L2', 'M', '32', 'EF_CONSTRUCTION', '100', 'EF_RUNTIME', '20']]
    # assertInfoField(env, 'idx2', 'attributes', info)

    # conn.execute_command('FT.CREATE', 'idx3', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '14', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'INITIAL_CAP', '10', 'M', '64', 'EF_CONSTRUCTION', '400', 'EF_RUNTIME', '50')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'HNSW', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'M', '64', 'EF_CONSTRUCTION', '400', 'EF_RUNTIME', '50']]
    # assertInfoField(env, 'idx3', 'attributes', info)

    # conn.execute_command('FT.CREATE', 'idx4', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'INT64', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'HNSW', 'TYPE', 'INT64', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'M', '16', 'EF_CONSTRUCTION', '200', 'EF_RUNTIME', '10']]
    # assertInfoField(env, 'idx4', 'attributes', info)

    # conn.execute_command('FT.CREATE', 'idx5', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE')
    # info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'ALGORITHM', 'FLAT', 'TYPE', 'INT32', 'DIM', '64', 'DISTANCE_METRIC', 'COSINE', 'BLOCK_SIZE', str(1024 * 1024)]]
    # assertInfoField(env, 'idx5', 'attributes', info)

def testErrors(env):
    env.skipOnCluster()
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

    # test wrong query word
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '12', 'TYPE', 'FLOAT32', 'DIM', '1024', 'DISTANCE_METRIC', 'IP', 'INITIAL_CAP', '10', 'M', '16', 'EF_CONSTRUCTION', '200')
    env.expect('FT.SEARCH', 'idx', '*=>[REDIS 4 @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '*=>[TOP_K str @v $b]', 'PARAMS', '2', 'b', 'abcdefgh').error().contains('Syntax error')


def load_vectors_into_redis(con, vector_field, dim, num_vectors):
    data = np.float32(np.random.random((num_vectors, dim)))
    id_vec_list = []
    p = con.pipeline(transaction=False)
    for i, vector in enumerate(data):
        con.execute_command('HSET', i, vector_field, vector.tobytes(), 't', i % 10)
        id_vec_list.append((i, vector))
    p.execute()
    return id_vec_list

def test_with_fields(env):
    conn = getConnectionByEnv(env)
    dimension = 128
    qty = 100

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dimension, 'DISTANCE_METRIC', 'L2', 't', 'TEXT')
    load_vectors_into_redis(conn, 'v', dimension, qty)

    query_data = np.float32(np.random.random((1, dimension)))
    res = env.cmd('FT.SEARCH', 'idx', '5 *=>[TOP_K 100 @v $vec_param AS score]',
                    'SORTBY', 'v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes(),
                    'RETURN', 2, 'v_score', 't') # NEW API TEST
    res_nocontent = env.cmd('FT.SEARCH', 'idx', '5 *=>[TOP_K 100 @v $vec_param AS score]',
                    'SORTBY', 'v_score', 'PARAMS', 2, 'vec_param', query_data.tobytes(),
                    'NOCONTENT') # NEW API TEST
    env.assertEqual(res[1::2], res_nocontent[1:])
    env.assertEqual('t', res[2][2])
