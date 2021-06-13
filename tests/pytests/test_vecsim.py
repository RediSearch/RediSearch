# -*- coding: utf-8 -*-

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

def test_create(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx1 SCHEMA v VECTOR FLOAT32 16 L2 HNSW INITIAL_CAP 10 M 16 EF 200').ok()
