# -*- coding: utf-8 -*-
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).


import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep, time
from RLTest import Env
import random


def testBenchmarkText(env):
  random.seed()
  num_docs = 1000000
  copies = 1000
  num_queries = 10
  pipe_batch = 1000

  pl = env.getConnection().pipeline()
  for i in range(num_docs):
    pl.execute_command('HSET','doc%d' % i, 'n', i % copies)
    if i % pipe_batch == 0:
      pl.execute()
  pl.execute()
  print 'create index'
  env.expect('FT.CREATE idx SCHEMA t TEXT').ok()
  waitForIndex(env, 'idx')

  for i in range(num_queries):
    pl.execute_command('FT.SEARCH','idx', '*', 'nocontent', 'LIMIT', 0, 100)

  start_time = time()
  pl.execute()
  print time() - start_time

  #print env.cmd('ft.info idx')
