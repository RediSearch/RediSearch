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

    env.expect('FT.SEARCH', 'idx', '@v:[' + base64_message +' RANGE 1]').equal([1L, 'c', ['v', 'aacdefgh']])

def test_create(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR FLOAT32 16 L2 HNSW INITIAL_CAP 10 M 16 EF 200').ok()
    
    # test wrong query word
    env.expect('FT.SEARCH', 'idx', '@v:[abcdefgh REDIS 4]').equal([0L])
