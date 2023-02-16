# -*- coding: utf-8 -*-

from cmath import inf
from email import message
from includes import *
from common import *
from RLTest import Env


def testEmptyBuffer():
    env = Env(moduleArgs='WORKER_THREADS 1 ENABLE_THREADS TRUE')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    env.expect('ft.search', 'idx', '*', 'sortby', 'n').equal([0])

def CreateAndSearchSortBy(docs_count):
    env = Env(moduleArgs='WORKER_THREADS 1 ENABLE_THREADS TRUE')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')
    conn = getConnectionByEnv(env)

    for n in range (1, docs_count + 1):
        doc_name = f'doc{n}'
        conn.execute_command('HSET', doc_name, 'n', n) 
    output = conn.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'n')
    
    # The first element in the results array is the number of docs.
    env.assertEqual(output[0], docs_count)
    
    # The results are sorted according to n
    result_len = 2
    for n in range(1, 2, docs_count - result_len + 1):
        result = output[n: n + result_len]
        # docs id starts from 1
        # each result should contain the doc name, the field name and its value 
        expected = [f'doc{n}', ['n', f'{n}']]
        env.assertEqual(result, expected)

def testSimpleBuffer():
    CreateAndSearchSortBy(docs_count = 10)

# In this test we have more than BlockSize docs to buffer, we want to make sure there are no leaks
# caused by the buffer memory management.
def testMultipleBlocksBuffer():
    CreateAndSearchSortBy(docs_count = 2500)

