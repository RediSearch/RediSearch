# -*- coding: utf-8 -*-
from time import sleep

from common import *
from includes import *




def testBasicSuffix(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'SUFFIX', 'body', 'TEXT', 'SUFFIX')
  conn.execute_command('HSET', 'doc1', 'title', 'hello world', 'body', 'this is a test')
  # for now use prefix syntax
  env.expect('FT.SEARCH', 'idx', 'orl*', 'NOCONTENT').equal([1L, 'doc1'])

