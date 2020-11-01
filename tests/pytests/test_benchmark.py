# -*- coding: utf-8 -*- 

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep, time
from RLTest import Env
import random


def testBenchmarkNumeric(env):
  random.seed()
  num_docs = 100000
  num_queries = 10
  pipe_batch = 1000

  pl = env.getConnection().pipeline()
  for i in range(num_docs):
    pl.execute_command('HSET','doc%d' % i, 'n', i)
    if i % pipe_batch == 0:
      pl.execute()
  pl.execute()
  print 'create index'
  env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
  waitForIndex(env, 'idx')

  for i in range(num_queries):
    pl.execute_command('FT.SEARCH','idx', '@n:[0 %d]' % i * 2)
    
  start_time = time()
  pl.execute()
  print time() - start_time
