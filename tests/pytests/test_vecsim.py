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
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR INT32 2 L2 HNSW').ok()
    conn.execute_command('HSET', 'a', 'v', 'abcdefgh')
    conn.execute_command('HSET', 'b', 'v', 'abcdefgg')
    conn.execute_command('HSET', 'c', 'v', 'aacdefgh')
    conn.execute_command('HSET', 'd', 'v', 'abbdefgh')

    res = [4L, 'a', ['v', 'abcdefgh'], 'c', ['v', 'aacdefgh'],
               'b', ['v', 'abcdefgg'], 'd', ['v', 'abbdefgh']]
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]', 'SORTBY', 'v', 'ASC').equal(res)

    message = 'abcdefgh'
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'a', ['v', 'abcdefgh']])

    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    print message_bytes
    print base64_bytes
    print base64_message

    # RANGE uses topk but translate to base64 before
    env.expect('FT.SEARCH', 'idx', '@v:[' + base64_message +' RANGE 1]').equal([1L, 'a', ['v', 'abcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[' + base64_message +' TOPK 1] => {$base64:true}').equal([1L, 'a', ['v', 'abcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[' + base64_message +' TOPK 1] => { $base64:true; $efRuntime:100}').equal([1L, 'a', ['v', 'abcdefgh']])

    #####################
    ## another example ##
    #####################
    message = 'aacdefgh'
    env.expect('FT.SEARCH', 'idx', '@v:[' + message +' TOPK 1]').equal([1L, 'c', ['v', 'aacdefgh']])

    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    #print message_bytes
    #print base64_bytes
    #print base64_message

def test_escape(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR INT32 2 L2 HNSW').ok()
    conn.execute_command('HSET', 'a', 'v', '////////')
    conn.execute_command('HSET', 'b', 'v', '++++++++')
    conn.execute_command('HSET', 'c', 'v', 'abcdefgh')
    conn.execute_command('HSET', 'd', 'v', 'aacdefgh')
    conn.execute_command('HSET', 'e', 'v', 'aaadefgh')

    messages = ['\+\+\+\+\+\+\+\+', '\/\/\/\/\/\/\/\/', 'abcdefgh', 'aacdefgh', 'aaadefgh']
    for message in messages:
        res = env.cmd('FT.SEARCH', 'idx', '@v:[' + message + ' TOPK 1]')
        env.assertEqual(res[2][1], message.replace('\\', ''))

        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        print message_bytes
        print base64_bytes
        print base64_message

        # RANGE uses topk but translate to base64 before
        res = env.cmd('FT.SEARCH', 'idx', '@v:[' + base64_message + ' RANGE 1]')
        env.assertEqual(res[2][1], message.replace('\\', ''))

def testDel(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'INT32', '2', 'L2', 'HNSW')
    conn.execute_command('HSET', 'a', 'v', 'abcdefgh')
    conn.execute_command('HSET', 'b', 'v', 'abcdefgg')
    conn.execute_command('HSET', 'c', 'v', 'aacdefgh')
    conn.execute_command('HSET', 'd', 'v', 'azcdefgh')

    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'a', ['v', 'abcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgg TOPK 1]').equal([1L, 'b', ['v', 'abcdefgg']])
    env.expect('FT.SEARCH', 'idx', '@v:[aacdefgh TOPK 1]').equal([1L, 'c', ['v', 'aacdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[azcdefgh TOPK 1]').equal([1L, 'd', ['v', 'azcdefgh']])

    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]', 'SORTBY', 'v', 'ASC')   \
        .equal([1L, 'a', ['v', 'abcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 2]', 'SORTBY', 'v', 'ASC')   \
        .equal([2L, 'a', ['v', 'abcdefgh'], 'c', ['v', 'aacdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 3]', 'SORTBY', 'v', 'ASC')   \
        .equal([3L, 'a', ['v', 'abcdefgh'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]', 'SORTBY', 'v', 'ASC')   \
        .equal([4L, 'a', ['v', 'abcdefgh'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh'], 'b', ['v', 'abcdefgg']])
    env.expect('DEL', 'a').equal(1)
    
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]', 'SORTBY', 'v', 'ASC')    \
        .equal([1L, 'c', ['v', 'aacdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 2]', 'SORTBY', 'v', 'ASC')    \
        .equal([2L, 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 3]', 'SORTBY', 'v', 'ASC')    \
        .equal([3L, 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh'], 'b', ['v', 'abcdefgg']])

    '''
    This test returns 4 results instead of the expected 3. The HNSW library return the additional results.
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal([3L, 'b', ['v', 'abcdefgg'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
    '''

def test_query_empty(env):
    env.skip()
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'INT32', '2', 'L2', 'HNSW')
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([0L])
    conn.execute_command('HSET', 'a', 'v', 'redislab')
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'a', ['v', 'redislab']])
    conn.execute_command('DEL', 'a')
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([0L])

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
    env.skip()
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
    for i in range(n_vec):
        vector = np.random.rand(1, vec_size).astype(np.float32)
        if i == query_vec_index:
            query_vec = vector
#         base64_vector = base64.b64encode(vector).decode('ascii')
        env.execute_command('HSET', i, 'vector', vector.tobytes())
    return query_vec

def query_vector(env, idx, query_vec):
    base64_vector = base64.b64encode(query_vec).decode('ascii')
    base64_vector_escaped = base64_vector.replace("=", r"\=").replace("/", r"\/").replace("+", r"\+")
    return env.cmd('FT.SEARCH', idx, '@vector:[' + base64_vector_escaped + ' RANGE 5]', 'SORTBY', 'vector', 'ASC', 'NOCONTENT', 'WITHSCORES')

def testDelReuseDvir(env):
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
        for i in range(4):
            env.assertLessEqual(res[2 + i * 2], res[2 + (i + 1) * 2])

def test_create(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR FLOAT32 16 L2 HNSW INITIAL_CAP 10 M 16 EF 200').ok()
    
    # test wrong query word
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh REDIS 4]').equal([0L])

def test_with_weight(env):
    env.skip()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR INT32 2 L2 HNSW').ok()
    conn.execute_command('HSET', 'a', 'v', 'abcdefgh')
    conn.execute_command('HSET', 'b', 'v', 'abcdefgg')
    conn.execute_command('HSET', 'c', 'v', 'zzzzxxxx')
    conn.execute_command('HSET', 'd', 'v', 'abbdefgh')

    res = [4L, 'a', ['v', 'abcdefgh'], 'b', ['v', 'abcdefgg'],
               'c', ['v', 'zzzzxxxx'], 'd', ['v', 'abbdefgh']]
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal(res)
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4] => {$weight: 2000000}', 'WITHSCORES').equal(res)
    
    
    profile = [[4L, 'a', ['v', 'abcdefgh'], 'b', ['v', 'abcdefgg'],
                    'c', ['v', 'zzzzxxxx'], 'd', ['v', 'abbdefgh']],
               [['Total profile time', '0.13600000000000001'],
                ['Parsing time', '0.072999999999999995'],
                ['Pipeline creation time', '0.0040000000000000001'],
                ['Iterators profile',
                    ['Type', 'LIST', 'Time', '0', 'Counter', 0L]],
                ['Result processors profile',
                    ['Type', 'Index', 'Time', '0.01', 'Counter', 4L],
                    ['Type', 'Scorer', 'Time', '0.011000000000000001', 'Counter', 4L],
                    ['Type', 'Sorter', 'Time', '0.0099999999999999985', 'Counter', 4L],
                    ['Type', 'Loader', 'Time', '0.016', 'Counter', 4L]]]]
    env.expect('FT.PROFILE', 'idx', 'SEARCH', 'QUERY',
               '@v:[abcdefgh TOPK 4] => {$weight: 2000000}').equal(profile)

    message = 'abcdefgh'
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'a', ['v', 'abcdefgh']])
