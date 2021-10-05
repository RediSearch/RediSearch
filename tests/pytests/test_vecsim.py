# -*- coding: utf-8 -*-
import base64
import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep
from RLTest import Env
import random
import string
import numpy as np

def test_1st(env):
    # env.skip() # @@diag
    conn = getConnectionByEnv(env)
    vecsim_type = ['BF', 'HNSW']
    for vs_type in vecsim_type:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'INT32', '2', 'L2', vs_type)
        conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa')
        conn.execute_command('HSET', 'b', 'v', 'aaaabaaa')
        conn.execute_command('HSET', 'c', 'v', 'aaaaabaa')
        conn.execute_command('HSET', 'd', 'v', 'aaaaaaba')

        res = [4L, 'a', ['v_score', '0', 'v', 'aaaaaaaa'],
                   'b', ['v_score', '3.09485009821e+26', 'v', 'aaaabaaa'],
                   'c', ['v_score', '2.02824096037e+31', 'v', 'aaaaabaa'],
                   'd', ['v_score', '1.32922799578e+36', 'v', 'aaaaaaba']]
        res1 = conn.execute_command('FT.SEARCH', 'idx', '@v:[aaaaaaaa TOPK 4]', 'SORTBY', 'v_score', 'ASC')
        env.assertEqual(res, res1)

        # todo: make test work on coordinator
        res = [4L, 'c', ['v_score', '0', 'v', 'aaaaabaa'],
                   'b', ['v_score', '2.01242627636e+31', 'v', 'aaaabaaa'],
                   'a', ['v_score', '2.02824096037e+31', 'v', 'aaaaaaaa'],
                   'd', ['v_score', '1.31886368448e+36', 'v', 'aaaaaaba']]
        res1 = conn.execute_command('FT.SEARCH', 'idx', '@v:[aaaaabaa TOPK 4]', 'SORTBY', 'v_score', 'ASC')
        env.assertEqual(res, res1)

        expected_res = ['v_score', '0', 'v', 'aaaaaaaa']
        res = conn.execute_command('FT.SEARCH', 'idx', '@v:[aaaaaaaa TOPK 1]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[2], expected_res)

        message = 'aaaaaaaa'
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        # print message_bytes
        # print base64_bytes
        # print base64_message

        # RANGE uses topk but translate to base64 before
        res = conn.execute_command('FT.SEARCH', 'idx', '@v:[' + base64_message +' RANGE 1]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[2], expected_res)
        res = conn.execute_command('FT.SEARCH', 'idx', '@v:[' + base64_message +' TOPK 1] => {$base64:true}', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[2], expected_res)
        res = conn.execute_command('FT.SEARCH', 'idx', '@v:[' + base64_message +' TOPK 1] => { $base64:true; $efRuntime:100}', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[2], expected_res)

        #####################
        ## another example ##
        #####################
        message = 'aaaaabaa'
        res = conn.execute_command('FT.SEARCH', 'idx', '@v:[' + message +' TOPK 1]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1)
        env.assertEqual(res[2], ['v_score', '0', 'v', 'aaaaabaa'])

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

def test_escape(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)

    vecsim_type = ['BF', 'HNSW']
    for vs_type in vecsim_type:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'INT32', '2', 'L2', vs_type)
        conn.execute_command('HSET', 'a', 'v', '////////')
        conn.execute_command('HSET', 'b', 'v', '++++++++')
        conn.execute_command('HSET', 'c', 'v', 'abcdefgh')
        conn.execute_command('HSET', 'd', 'v', 'aacdefgh')
        conn.execute_command('HSET', 'e', 'v', 'aaadefgh')

        messages = ['\+\+\+\+\+\+\+\+', '\/\/\/\/\/\/\/\/', 'abcdefgh', 'aacdefgh', 'aaadefgh']
        for message in messages:
            res = conn.execute_command('FT.SEARCH', 'idx', '@v:[' + message + ' TOPK 1]')
            env.assertEqual(res[2][1], message.replace('\\', ''))

            message_bytes = message.encode('ascii')
            base64_bytes = base64.b64encode(message_bytes)
            base64_message = base64_bytes.decode('ascii')
            # print message_bytes
            # print base64_bytes
            # print base64_message

            # RANGE uses topk but translate to base64 before
            res = conn.execute_command('FT.SEARCH', 'idx', '@v:[' + base64_message + ' RANGE 1]')
            env.assertEqual(res[2][1], message.replace('\\', ''))

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

def testDel(env):
    # env.skip() # @@diag
    conn = getConnectionByEnv(env)
    vecsim_type = ['BF', 'HNSW']
    for vs_type in vecsim_type:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'INT32', '2', 'L2', vs_type)
        conn.execute_command('HSET', 'a', 'v', 'abcdefgh')
        conn.execute_command('HSET', 'b', 'v', 'abcdefgg')
        conn.execute_command('HSET', 'c', 'v', 'aacdefgh')
        conn.execute_command('HSET', 'd', 'v', 'azcdefgh')

        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'a', ['v', 'abcdefgh']])
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgg TOPK 1]').equal([1L, 'b', ['v', 'abcdefgg']])
        env.expect('FT.SEARCH', 'idx', '@v:[aacdefgh TOPK 1]').equal([1L, 'c', ['v', 'aacdefgh']])
        env.expect('FT.SEARCH', 'idx', '@v:[azcdefgh TOPK 1]').equal([1L, 'd', ['v', 'azcdefgh']])

        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1)   \
            .equal([1L, 'a', ['v_score', '0', 'v', 'abcdefgh']])
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 2]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 2)   \
            .equal([2L, 'a', ['v_score', '0', 'v', 'abcdefgh'], 'c', ['v_score', '8.30767497366e+34', 'v', 'aacdefgh']])
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 3]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 3)   \
            .equal([3L, 'a', ['v_score', '0', 'v', 'abcdefgh'], 'c', ['v_score', '8.30767497366e+34', 'v', 'aacdefgh'],
                            'd', ['v_score', '4.78522078483e+37', 'v', 'azcdefgh']])
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 4)   \
            .equal([4L, 'a', ['v_score', '0', 'v', 'abcdefgh'], 'c', ['v_score', '8.30767497366e+34', 'v', 'aacdefgh'],
                            'd', ['v_score', '4.78522078483e+37', 'v', 'azcdefgh'], 'b', ['v_score', 'inf', 'v', 'abcdefgg']])
        conn.execute_command('DEL', 'a')
        
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 1)    \
            .equal([1L, 'c', ['v_score', '8.30767497366e+34', 'v', 'aacdefgh']])
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 2]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 2)    \
            .equal([2L, 'c', ['v_score', '8.30767497366e+34', 'v', 'aacdefgh'], 'd', ['v_score', '4.78522078483e+37', 'v', 'azcdefgh']])
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 3]', 'SORTBY', 'v_score', 'ASC', 'LIMIT', 0, 3)    \
            .equal([3L, 'c', ['v_score', '8.30767497366e+34', 'v', 'aacdefgh'], 'd', ['v_score', '4.78522078483e+37', 'v', 'azcdefgh'],
                        'b', ['v_score', 'inf', 'v', 'abcdefgg']])

        '''
        This test returns 4 results instead of the expected 3. The HNSW library return the additional results.
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal([3L, 'b', ['v', 'abcdefgg'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
        '''

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

def test_query_empty(env):
    #env.skip()
    conn = getConnectionByEnv(env)
    vecsim_type = ['BF', 'HNSW']
    for vs_type in vecsim_type:
        conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'INT32', '2', 'L2', vs_type)
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([0L])
        conn.execute_command('HSET', 'a', 'v', 'redislab')
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'a', ['v', 'redislab']])
        conn.execute_command('DEL', 'a')
        env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([0L])
        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

def del_insert(env):
    conn = getConnectionByEnv(env)

    conn.execute_command('DEL', 'a')
    conn.execute_command('DEL', 'b')
    conn.execute_command('DEL', 'c')
    conn.execute_command('DEL', 'd')

    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal([0L])

    res = [''.join(random.choice(string.lowercase) for x in range(8)),
           ''.join(random.choice(string.lowercase) for x in range(8)),
           ''.join(random.choice(string.lowercase) for x in range(8)),
           ''.join(random.choice(string.lowercase) for x in range(8))]

    conn.execute_command('HSET', 'a', 'v', res[0])
    conn.execute_command('HSET', 'b', 'v', res[1])
    conn.execute_command('HSET', 'c', 'v', res[2])
    conn.execute_command('HSET', 'd', 'v', res[3])

    return res

def testDelReuse(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'INT32', '2', 'L2', 'HNSW')

    vecs = del_insert(env)
    print (env.cmd('FT.INFO', 'idx'))
    res = [4L, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal(res)

    vecs = del_insert(env)
    print (env.cmd('FT.INFO', 'idx'))
    res = [4L, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal(res)

    vecs = del_insert(env)
    print (env.cmd('FT.INFO', 'idx'))
    res = [4L, 'a', ['v', vecs[0]], 'b', ['v', vecs[1]], 'c', ['v', vecs[2]], 'd', ['v', vecs[3]]]
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal(res)

def load_vectors_to_redis(env, n_vec, query_vec_index, vec_size):
    conn = getConnectionByEnv(env)
    for i in range(n_vec):
        vector = np.random.rand(1, vec_size).astype(np.float32)
        if i == query_vec_index:
            query_vec = vector
#         base64_vector = base64.b64encode(vector).decode('ascii')
        conn.execute_command('HSET', i, 'vector', vector.tobytes())
    return query_vec

def query_vector(env, idx, query_vec):
    conn = getConnectionByEnv(env)
    base64_vector = base64.b64encode(query_vec).decode('ascii')
    base64_vector_escaped = base64_vector.replace("=", r"\=").replace("/", r"\/").replace("+", r"\+")
    return conn.execute_command('FT.SEARCH', idx, '@vector:[' + base64_vector_escaped + ' RANGE 5]',
                                'SORTBY', 'vector_score', 'ASC', 'RETURN', 1, 'vector_score', 'LIMIT', 0, 5)

def testDelReuseDvir(env):
    # env.skip() # @@diag
    conn = getConnectionByEnv(env)
    INDEX_NAME = 'items'
    prefix = 'item'
    n_vec = 5
    query_vec_index = 3
    vec_size = 1280

    conn.execute_command('FT.CREATE', INDEX_NAME, 'ON', 'HASH',
                         'SCHEMA', 'vector', 'VECTOR', 'FLOAT32', '1280', 'L2', 'HNSW')
    for _ in range(3):
        query_vec = load_vectors_to_redis(env, n_vec, query_vec_index, vec_size)
        res = query_vector(env, INDEX_NAME, query_vec)
        print res
        for i in range(4):
            env.assertLessEqual(float(res[2 + i * 2][1]), float(res[2 + (i + 1) * 2][1]))

def test_create(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'v', 'VECTOR', 'FLOAT32', '16', 'IP',     'HNSW', 'INITIAL_CAP', '10', 'M', '16', 'EF', '200')
    conn.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'v', 'VECTOR', 'FLOAT32', '16', 'L2',     'HNSW', 'INITIAL_CAP', '10', 'M', '16', 'EF', '200')
    conn.execute_command('FT.CREATE', 'idx3', 'SCHEMA', 'v', 'VECTOR', 'FLOAT32', '16', 'COSINE', 'HNSW', 'INITIAL_CAP', '10', 'M', '16', 'EF', '200')
    
    # test wrong query word
    res = conn.execute_command('FT.SEARCH', 'idx1', '@v:[abcdefgh REDIS 4]')
    env.assertEqual(res, [0L])
    res = conn.execute_command('FT.SEARCH', 'idx2', '@v:[abcdefgh REDIS 4]')
    env.assertEqual(res, [0L])
    res = conn.execute_command('FT.SEARCH', 'idx3', '@v:[abcdefgh REDIS 4]')
    env.assertEqual(res, [0L])
