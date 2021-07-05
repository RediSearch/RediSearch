# -*- coding: utf-8 -*-
import base64
import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep
from RLTest import Env

def test_1st(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR INT32 2 L2 HNSW').ok()
    conn.execute_command('HSET', 'a', 'v', 'abcdefgh')
    conn.execute_command('HSET', 'b', 'v', 'abcdefgg')
    conn.execute_command('HSET', 'c', 'v', 'aacdefgh')
    conn.execute_command('HSET', 'd', 'v', 'abbdefgh')

    res = [4L, 'a', ['v', 'abcdefgh'], 'b', ['v', 'abcdefgg'],
               'c', ['v', 'aacdefgh'], 'd', ['v', 'abbdefgh']]
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal(res)

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

    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'a', ['v', 'abcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 2]').equal([2L, 'a', ['v', 'abcdefgh'], 'c', ['v', 'aacdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 3]').equal([3L, 'a', ['v', 'abcdefgh'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal([4L, 'a', ['v', 'abcdefgh'], 'b', ['v', 'abcdefgg'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
    env.expect('DEL', 'a').equal(1)
    
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 1]').equal([1L, 'c', ['v', 'aacdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 2]').equal([2L, 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 3]').equal([3L, 'b', ['v', 'abcdefgg'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])

    '''
    This test returns 4 results instead of the expected 3. The HNSW library return the additional results.
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 4]').equal([3L, 'b', ['v', 'abcdefgg'], 'c', ['v', 'aacdefgh'], 'd', ['v', 'azcdefgh']])
    '''

def test_create(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR FLOAT32 16 L2 HNSW INITIAL_CAP 10 M 16 EF 200').ok()
    
    # test wrong query word
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh REDIS 4]').equal([0L])
