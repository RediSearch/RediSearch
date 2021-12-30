# -*- coding: utf-8 -*- 

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep, time
from RLTest import Env
import random


def testBenchmarkNumeric(env):
  random.seed()
  num_docs = 1000000
  copies = 10
  num_queries = 1
  pipe_batch = 1000

  pl = env.getConnection().pipeline()
  for i in range(num_docs):
    pl.execute_command('HSET','doc%d' % i, 'n', (i % copies) * 0.99)
    if i % pipe_batch == 0:
      pl.execute()
  pl.execute()
  print 'create index'
  env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
  waitForIndex(env, 'idx')

  for i in range(num_queries):
    pl.execute_command('FT.SEARCH','idx', '@n:[0 %d]' % num_docs, 'LIMIT', 0, 0)
    
  start_time = time()
  pl.execute()
  print time() - start_time

  print env.cmd('ft.info idx')
