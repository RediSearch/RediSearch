# -*- coding: utf-8 -*-

from includes import *
from common import *
from RLTest import Env


def testOptimizer(env):
	env.skipOnCluster()
	repeat = 20000
	conn = getConnectionByEnv(env)
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric', 't', 'text', 'tag', 'TAG')
	env.cmd('ft.create', 'idx_sortable', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 't', 'text', 'tag', 'TAG')

	for i in range(0,repeat,2):
		conn.execute_command('hset', i, 't', 'foo', 'tag', 'foo', 'n', i % 100)
		conn.execute_command('hset', i + 1, 't', 'bar', 'tag', 'bar', 'n', i % 100)

	numeric_info = conn.execute_command('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n')
	env.debugPrint(str(numeric_info), force=True)

    ### wildcard ###
    # stop after enough results were collected
	env.expect('ft.search', 'idx', '*', 'limit', 0 , 2, 'NOCONTENT').equal([1, '0', '1'])
	env.expect('ft.search', 'idx', '*', 'limit', 0 , 3, 'NOCONTENT').equal([1, '0', '1', '2'])
	env.expect('ft.search', 'idx_sortable', '*', 'limit', 0 , 2, 'NOCONTENT').equal([1, '0', '1'])
	env.expect('ft.search', 'idx_sortable', '*', 'limit', 0 , 3, 'NOCONTENT').equal([1, '0', '1', '2'])

    ### filter with score ###
    # search over all matches
	env.expect('ft.search', 'idx', 'foo', 'limit', 0 , 2, 'NOCONTENT').equal([10000, '0', '2'])
	env.expect('ft.search', 'idx', 'foo', 'limit', 0 , 3, 'NOCONTENT').equal([10000, '0', '2', '4'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'limit', 0 , 2, 'NOCONTENT').equal([10000, '0', '2'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'limit', 0 , 3, 'NOCONTENT').equal([10000, '0', '2', '4'])

    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([1200, '0', '1'])
	env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([1200, '0', '1'])
	env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([9600, '19999', '19998'])
	env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([1200, '0', '1'])
	env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([1200, '0', '1'])
	env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([9600, '19999', '19998'])

    ### only range ###
    # stop after enough results were collected
	env.expect('ft.search', 'idx', '@n:[10 20]', 'limit', 0 , 2, 'NOCONTENT').equal([1, '10', '11'])
	env.expect('ft.search', 'idx', '@n:[10 20]', 'limit', 0 , 3, 'NOCONTENT').equal([1, '10', '11', '12'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'limit', 0 , 2, 'NOCONTENT').equal([1, '10', '11'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'limit', 0 , 3, 'NOCONTENT').equal([1, '10', '11', '12'])

    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2400, '10', '11'])
	env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2400, '10', '11'])
	env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2400, '19921', '19920'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2400, '10', '11'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2400, '10', '11'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2400, '19921', '19920'])

    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '198', '98'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '198', '98'])

	result = env.cmd('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'limit', 0 , 1500, 'NOCONTENT')
	env.assertEqual(result[0], 1500)

    ### filter without score ###
    # stop after enough results were collected
	env.expect('ft.search', 'idx', '@tag:{foo}', 'limit', 0 , 2, 'NOCONTENT').equal([1, '0', '2'])
	env.expect('ft.search', 'idx', '@tag:{foo}', 'limit', 0 , 3, 'NOCONTENT').equal([1, '0', '2', '4'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'limit', 0 , 2, 'NOCONTENT').equal([1, '0', '2'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'limit', 0 , 3, 'NOCONTENT').equal([1, '0', '2', '4'])

    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '198', '98'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '198', '98'])

	result = env.cmd('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 1500, 'NOCONTENT')
	env.assertEqual(result[0], 1500)

    ### range and filter ###
	# This solution require hybrid in case first iteration fails to produce enough results
    # stop after enough results were collected
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'limit', 0 , 2, 'NOCONTENT').equal([1200, '10', '12'])
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'limit', 0 , 3, 'NOCONTENT').equal([1200, '10', '12', '14'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'limit', 0 , 2, 'NOCONTENT').equal([1200, '10', '12'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'limit', 0 , 3, 'NOCONTENT').equal([1200, '10', '12', '14'])

    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2, '10', '110'])
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '10', '110'])
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '120', '20'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, 'NOCONTENT').equal([2, '10', '110'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '10', '110'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, 'NOCONTENT').equal([2, '120', '20'])


	result = env.cmd('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 1500, 'NOCONTENT')
	env.assertEqual(result[0], 1200) # develop hybrid to get more results

	#input('stop')
