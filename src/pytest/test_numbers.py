# -*- coding: utf-8 -*-

import unittest
from includes import *
from time import sleep
from RLTest import Env
import math

def testCompression(env):
	env.skipOnCluster()
	accuracy = 0.000001
	repeat = int(math.sqrt(1 / accuracy))

	env.expect('ft.config', 'set', '_NUMERIC_COMPRESS', 'false').equal('OK')
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
	for i in range(repeat):
		value = accuracy * i
		env.cmd('ft.add', 'idx', i, 1, 'fields', 'n', str(value))

	for i in range(repeat):
		value = accuracy * i
		env.expect('ft.search', 'idx', ('@n:[%s %s]' % (value, value))).equal([1L, str(i), ['n', str(value)]])
  
def testSanity(env):
	env.skipOnCluster()
	repeat = 100000
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
	for i in range(repeat):
		env.cmd('ft.add', 'idx', i, 1, 'fields', 'n', i % 100)
	env.expect('ft.search', 'idx', ('@n:[0 %d]' % (repeat)), 'limit', 0 ,0).equal([repeat])
	env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'n') \
				.equal(['numRanges', 12L, 'numEntries', 100000L, 'lastDocId', 100000L, 'revisionId', 11L])

def testCompressionConfig(env):
	env.skipOnCluster()
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')

	# w/o compression. exact number match.
	env.expect('ft.config', 'set', '_NUMERIC_COMPRESS', 'false').equal('OK')
	for i in range(100):
	  env.execute_command('ft.add', 'idx', i, 1, 'fields', 'n', str(1 + i / 100.0))
	for i in range(100):
		num = str(1 + i / 100.0)
		env.expect('ft.search', 'idx', '@n:[%s %s]' % (num, num)).equal([1L, str(i), ['n', num]])

	# with compression. no exact number match.
	env.expect('ft.config', 'set', '_NUMERIC_COMPRESS', 'true').equal('OK')
	for i in range(100):
	  env.execute_command('ft.add', 'idx', i, 1, 'replace', 'fields', 'n', str(1 + i / 100.0))
	
	# delete keys where compression does not change value
	env.execute_command('ft.del', 'idx', '0')
	env.execute_command('ft.del', 'idx', '25')
	env.execute_command('ft.del', 'idx', '50')
	env.execute_command('ft.del', 'idx', '75')

	for i in range(100):
		num = str(1 + i / 100.0)
		env.expect('ft.search', 'idx', '@n:[%s %s]' % (num, num)).equal([0L])
