# -*- coding: utf-8 -*-

from includes import *
from common import *
from RLTest import Env

# /**********************************************************************
# * NUM * TEXT  * TAG *     with SORTBY          *    w/o SORTBY        *
# ***********************************************************************
# *  Y  *   Y   * --- *    Q_OPT_HYBRID (1)      *   (note1)   (2)      *
# ***********************************************************************
# *  Y  *   N   *  Y  *    Q_OPT_HYBRID (3)      *  Q_OPT_HYBRID (4)    *
# ***********************************************************************
# *  Y  *   N   *  N  * Q_OPT_PARTIAL_RANGE (5)  * Q_OPT_NO_SORTER (6)  *
# ***********************************************************************
# *  N  *   Y   * --- *    Q_OPT_HYBRID (7)      *   Q_OPT_NONE  (8)    *
# ***********************************************************************
# *  N  *   N   *  Y  *    Q_OPT_HYBRID (9)      * Q_OPT_NO_SORTER (10) *
# ***********************************************************************
# *  N  *   N   *  N  * Q_OPT_PARTIAL_RANGE (11) * Q_OPT_NO_SORTER (12) *
# **********************************************************************/

def testOptimizer(env):
	env.skipOnCluster()
	repeat = 20000
	conn = getConnectionByEnv(env)
	env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT', 'tag', 'TAG')
	env.cmd('FT.CREATE', 'idx_sortable', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'tag', 'TAG')

	for i in range(0,repeat,2):
		conn.execute_command('hset', i, 't', 'foo', 'tag', 'foo', 'n', i % 100)
		conn.execute_command('hset', i + 1, 't', 'bar', 'tag', 'bar', 'n', i % 100)

	numeric_info = conn.execute_command('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n')
	env.debugPrint(str(numeric_info), force=True)
	params = ['NOCONTENT', 'OPTIMIZE']

    ### (1) range and filter with sort ###
    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])

    ### (2) range and filter w/o sort ###
	# This solution require hybrid in case first iteration fails to produce enough results
    # stop after enough results were collected
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'limit', 0 , 2, *params).equal([1200, '10', '12'])
	env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'limit', 0 , 3, *params).equal([1200, '10', '12', '14'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'limit', 0 , 2, *params).equal([1200, '10', '12'])
	env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'limit', 0 , 3, *params).equal([1200, '10', '12', '14'])

    ### (3) TAG and range with sort ###
    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])

    ### (4) TAG and range w/o sort ###
    # stop after enough results were collected
	env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '12'])
	env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '12', '14'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '12'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '12', '14'])

    ### (5) numeric range with sort ###
    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2400, '10', '11'])
	env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2400, '10', '11'])
	env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2400, '19921', '19920'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2400, '10', '11'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2400, '10', '11'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2400, '19921', '19920'])

    ### (6) only range ###
    # stop after enough results were collected
	env.expect('ft.search', 'idx', '@n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '11'])
	env.expect('ft.search', 'idx', '@n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '11', '12'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '11'])
	env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '11', '12'])

    ### (7) filter with sort ###
    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])

	result = env.cmd('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'limit', 0 , 1500, *params)
	env.assertEqual(result[0], 1500)

    ### (8) filter w/o sort (by score) ###
    # search over all matches
	env.expect('ft.search', 'idx', 'foo', 'limit', 0 , 2, *params).equal([10000, '0', '2'])
	env.expect('ft.search', 'idx', 'foo', 'limit', 0 , 3, *params).equal([10000, '0', '2', '4'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'limit', 0 , 2, *params).equal([10000, '0', '2'])
	env.expect('ft.search', 'idx_sortable', 'foo', 'limit', 0 , 3, *params).equal([10000, '0', '2', '4'])

    ### (9) no sort, no score, with sortby ###
    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])

    ### (10) no sort, no score, no sortby ###
    # stop after enough results were collected
	env.expect('ft.search', 'idx', '@tag:{foo}', 'limit', 0 , 2, *params).equal([1, '0', '2'])
	env.expect('ft.search', 'idx', '@tag:{foo}', 'limit', 0 , 3, *params).equal([1, '0', '2', '4'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'limit', 0 , 2, *params).equal([1, '0', '2'])
	env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'limit', 0 , 3, *params).equal([1, '0', '2', '4'])

    ### (11) wildcard with sort ###
    # Search only minimal number of ranges
	env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([1200, '0', '1'])
	env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([1200, '0', '1'])
	env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([9600, '19999', '19998'])
	env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([1200, '0', '1'])
	env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([1200, '0', '1'])
	env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([9600, '19999', '19998'])

    ### (12) wildcard w/o sort ###
    # stop after enough results were collected
	env.expect('ft.search', 'idx', '*', 'limit', 0 , 2, *params).equal([1, '0', '1'])
	env.expect('ft.search', 'idx', '*', 'limit', 0 , 3, *params).equal([1, '0', '1', '2'])
	env.expect('ft.search', 'idx_sortable', '*', 'limit', 0 , 2, *params).equal([1, '0', '1'])
	env.expect('ft.search', 'idx_sortable', '*', 'limit', 0 , 3, *params).equal([1, '0', '1', '2'])

	result = env.cmd('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 1500, *params)
	env.assertEqual(result[0], 1500)


	result = env.cmd('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 1500, *params)
	# env.assertEqual(result[0], 1200) # develop hybrid to get more results

	#input('stop')
