# -*- coding: utf-8 -*-

from cmath import inf
from email import message
from includes import *
from common import *
from RLTest import Env


def testEmptyBuffer():
    env = Env(moduleArgs='WORKER_THREADS 8 ENABLE_THREADS TRUE')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    env.expect('ft.search', 'idx', '*', 'sortby', 'n').equal([0])

def testSimpleBuffer():
    env = Env(moduleArgs='WORKER_THREADS 8 ENABLE_THREADS TRUE')
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    docs_count = 10
    expected_res = [docs_count]
    for n in range (1, docs_count + 1):
        doc_name = f'doc{n}'
        doc_values = ['n', f'{n}']
        expected_res.append(doc_name)
        expected_res.append(doc_values)
        conn.execute_command('HSET', doc_name, 'n', f'{n}') 
    env.expect('FT.SEARCH', 'idx', '*', 'sortby', 'n').equal(expected_res)
    
def testMultipleBlocksBuffer():
    env = Env(moduleArgs='WORKER_THREADS 8 ENABLE_THREADS TRUE')

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    docs_count = 2500
    for n in range (1, docs_count + 1):
        doc_name = f'doc{n}'
        conn.execute_command('HSET', doc_name, 'n', f'{n}') 
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'n')
    assert res[0] == docs_count
    i = 1
    for elem in res[2:2:]:
        assert(elem[1] == i)
        i +=1

