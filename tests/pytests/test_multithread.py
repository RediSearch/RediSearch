# -*- coding: utf-8 -*-

from cmath import inf
from email import message
from includes import *
from common import *
from RLTest import Env

env = Env(moduleArgs='WORKER_THREADS 8 ENABLE_THREADS TRUE')

def testEmptyBuffer():
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    env.expect('ft.search', 'idx', '*', 'sortby', 'n').equal([0])

def testSimpleBuffer():
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
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    docs_count = 2500
    print(f"docs = {docs_count}" )
    for n in range (1, docs_count + 1):
        doc_name = f'doc{n}'
        conn.execute_command('HSET', doc_name, 'n', f'{n}') 
    print(f"res = {conn.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'n', 'NOCONTENT')}")
