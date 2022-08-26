# -*- coding: utf-8 -*-

from email import message
from includes import *
from common import *
from RLTest import Env
def check_sortby(env, query, params, msg=None):
	cmds = ['ft.search', 'ft.aggregate']
	idx = 2 if query[0] == cmds[0] else 1
	msg = cmds[idx % 2] + ' limit %d %d : ' % (params[1], params[2]) + msg

	sort_order = ['ASC', 'DESC']
	for sort in range(len(sort_order)):
		print_err = False
		res = env.cmd(*query, sort_order[sort], *params)

		# put all `n` values into a list
		res_list = [int(to_dict(n)['n']) for n in res[idx::idx]]
		err_msg = msg + ' : ' + sort_order[sort] + ' : len=%d' % len(res_list)

		for i in range(len(res_list) - 1):
			 # ascending order
			if sort_order[sort] == sort_order[0]:
				# env.assertTrue(res_list[i] <= res_list[i - 1])
				if res_list[i] > res_list[i + 1]:
					#print('index %d : ' % i + str(res_list[i])+' > '+str(res_list[i + 1]) + ' : ' + err_msg)
					print_err = True
			 # descending order
			if sort_order[sort] == sort_order[1]:
				# env.assertTrue(res_list[i] <= res_list[i - 1])
				if res_list[i] < res_list[i + 1]:
					#print('index %d : ' % i + str(res_list[i])+' < '+str(res_list[i + 1]) + ' : ' + err_msg)
					print_err = True
	
		if print_err:
			if (len(res)) < 100:
				env.debugPrint(str(res), force=True)
				env.debugPrint(str(res_list), force=True)
				# input('stop')

		env.assertFalse(print_err, message=err_msg)

def testSortby(env):
	# separate test which only has queries with sortby
	repeat = 10000
	conn = getConnectionByEnv(env)
	env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT', 'tag', 'TAG')
	env.cmd('FT.CREATE', 'idx_sortable', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'tag', 'TAG')

	words = ['hello', 'world', 'foo', 'bar', 'baz']
	for i in range(0, repeat, len(words)):
		for j in range(len(words)):
			conn.execute_command('hset', i + j, 't', words[j], 'tag', words[j], 'n', i % 1000 + j)


	limits = [[0, 5], [0, 30], [0, 150], [5, 5], [20, 30], [100, 10], [500, 100], [5000, 1000], [9900, 1010], [0, 100000]]
	ranges = [[-5, 105], [0, 3], [30, 60], [-10, 5], [950, 1100], [2000, 3000], [42, 42]]
	params = ['limit', 0 , 0]

	for _ in env.retry_with_rdb_reload():
		for i in range(len(limits)):
			params[1] = limits[i][0]
			params[2] = limits[i][1]
			print(params)
			for j in range(len(ranges)):
				numRange = str('@n:[%d %d]' % (ranges[j][0],ranges[j][1]))
				print(numRange)
				### (1) TEXT and range with sort ###
				check_sortby(env, ['ft.search', 'idx', 'foo ' + numRange, 'SORTBY', 'n'], params, 'case 1 ' + numRange)

				### (3) TAG and range with sort ###
				check_sortby(env, ['ft.search', 'idx', '@tag:{foo} ' + numRange, 'SORTBY', 'n'], params, 'case 3 ' + numRange)

				### (5) numeric range with sort ###
				check_sortby(env, ['ft.search', 'idx', numRange, 'SORTBY', 'n'], params, 'case 5 ' + numRange)

			### (7) filter with sort ###
			# Search only minimal number of ranges
			check_sortby(env, ['ft.search', 'idx', 'foo', 'SORTBY', 'n'], params, 'case 7')

			### (9) no sort, no score, with sortby ###
			# Search only minimal number of ranges
			check_sortby(env, ['ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n'], params, 'case 9')

			### (11) wildcard with sort ###
			# Search only minimal number of ranges
			check_sortby(env, ['ft.search', 'idx', '*', 'SORTBY', 'n'], params, 'case 11')


	# update parameters for ft.aggregate
	params = ['limit', 0 , 0, 'LOAD', 4, '@__key', '@n', '@t', '@tag']

	for _ in env.retry_with_rdb_reload():
		for i in range(len(limits)):
			params[1] = limits[i][0]
			params[2] = limits[i][1]
			
			for j in range(len(ranges)):
				numRange = '@n:[%d %d]' % (ranges[j][0],ranges[j][1])

				### (1) TEXT and range with sort ###
				check_sortby(env, ['ft.aggregate', 'idx', 'foo ' + numRange, 'SORTBY', 2, '@n'], params, 'case 1 ' + numRange)

				### (3) TAG and range with sort ###
				check_sortby(env, ['ft.aggregate', 'idx', '@tag:{foo} ' + numRange, 'SORTBY', 2, '@n'], params, 'case 3 ' + numRange)

				### (5) numeric range with sort ###
				check_sortby(env, ['ft.aggregate', 'idx', numRange, 'SORTBY', 2, '@n'], params, 'case 5 ' + numRange)

			### (7) filter with sort ###
			# aggregate only minimal number of ranges
			check_sortby(env, ['ft.aggregate', 'idx', 'foo', 'SORTBY', 2, '@n'], params, 'case 7')

			### (9) no sort, no score, with sortby ###
			# aggregate only minimal number of ranges
			check_sortby(env, ['ft.aggregate', 'idx', '@tag:{foo}', 'SORTBY', 2, '@n'], params, 'case 9')

			### (11) wildcard with sort ###
			# aggregate only minimal number of ranges
			check_sortby(env, ['ft.aggregate', 'idx', '*', 'SORTBY', 2, '@n'], params, 'case 11')

	#input('stop')
