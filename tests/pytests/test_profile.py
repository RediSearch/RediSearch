# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep
from RLTest import Env

def testProfileSearch(env):
	tests = 1000000

	conn = getConnectionByEnv(env)
	pl = conn.pipeline()
	env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
	env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
	for i in range(tests):
		pl.execute_command('hset', i, 't', str(i))
		if (i % 999) is 0:
			pl.execute()
	pl.execute()
	
	print env.cmd('FT.search', 'idx', '12*|69*', 'limit', 0, 0)
	res = env.cmd('FT.PROFILE', 'search', 'idx', '12*', 'limit', 0, 10, 'nocontent')
	print res
