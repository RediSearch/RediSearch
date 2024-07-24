# -*- coding: utf-8 -*-

import profile
from includes import *
from common import *
from RLTest import Env

# /**********************************************************************
# * NUM * TEXT  * TAG *  with SORTBY on NUMERIC  *    w/o SORTBY        *
# ***********************************************************************
# *  Y  *   Y   * Y/N *    Q_OPT_HYBRID (1)      *   Q_OPT_NONE (2)     *
# ***********************************************************************
# *  Y  *   N   *  Y  *    Q_OPT_HYBRID (3)      *  Q_OPT_HYBRID (4)    *
# ***********************************************************************
# *  Y  *   N   *  N  * Q_OPT_PARTIAL_RANGE (5)  * Q_OPT_NO_SORTER (6)  *
# ***********************************************************************
# *  N  *   Y   * Y/N *    Q_OPT_HYBRID (7)      *   Q_OPT_NONE  (8)    *
# ***********************************************************************
# *  N  *   N   *  Y  *    Q_OPT_HYBRID (9)      * Q_OPT_NO_SORTER (10) *
# ***********************************************************************
# *  N  *   N   *  N  * Q_OPT_PARTIAL_RANGE (11) * Q_OPT_NO_SORTER (12) *
# **********************************************************************/

# transfer query to be a profile query
def print_profile(env, query, params, optimize=False):

    isSearch = (query[0] == 'ft.search')
    query_list = ['ft.profile']
    query_list.append('search' if isSearch else 'aggregate')
    query_list.append('QUERY')
    query_list.append(*query)

    if optimize:
        params.append('WITHOUTCOUNT')
    env.debugPrint(env.cmd(*query_list, *params))

def compare_optimized_to_not(env, query, params, msg=None):
    not_res = env.cmd(*query, *params)
    opt_res = env.cmd(*query, 'WITHOUTCOUNT', *params)
    #print(not_res)
    #print(opt_res)

    # check length of list to avoid errors
    if len(not_res) == 1 or len(opt_res) == 1:
        env.assertEqual(len(not_res), len(opt_res), message=msg)
        if len(not_res) != len(opt_res):
            env.debugPrint(str(not_res), force=True)
            env.debugPrint(str(opt_res), force=True)
        return

    # put all `n` values into a list
    i = 2 if query[0] == 'ft.search' else 1
    not_list = [to_dict(n)['n'] for n in not_res[i::i]]
    opt_list = [to_dict(n)['n'] for n in opt_res[i::i]]
    #not_list = not_res[1:]
    #opt_list = opt_res[1:]

    cmds = ['ft.search', 'ft.aggregate']
    msg = cmds[i%2] + ' limit %d %d : ' % (params[1], params[2]) + msg

    # check length and content
    env.assertEqual(len(not_res), len(opt_res), message=msg)
    env.assertEqual(not_list, opt_list, message=msg)
    if not_list != opt_list:
        print(str(not_res))
        print(str(opt_res))
        print_profile(env, query, params, optimize=False)
        print_profile(env, query, params, optimize=True)

@skip(cluster=True)
def testOptimizer(env):
    env.cmd(config_cmd(), 'SET', 'TIMEOUT', '0')
    env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
    env.cmd(config_cmd(), 'SET', '_PRIORITIZE_INTERSECT_UNION_CHILDREN', 'true')
    repeat = 20000
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT', 'tag', 'TAG')
    env.cmd('FT.CREATE', 'idx_sortable', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'tag', 'TAG')

    for i in range(0,repeat,2):
        conn.execute_command('hset', i, 't', 'foo', 'tag', 'foo', 'n', i % 100)
        conn.execute_command('hset', i + 1, 't', 'bar', 'tag', 'bar', 'n', i % 100)

    numeric_info = conn.execute_command(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n')
    env.debugPrint(str(numeric_info), force=True)
    params = ['NOCONTENT', 'WITHOUTCOUNT']

    ### (1) range and filter with sort ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])
    env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])

    profiler =  {'Iterators profile':
                    ['Type', 'OPTIMIZER', 'Counter', 10, 'Optimizer mode', 'Hybrid', 'Child iterator',
                        ['Type', 'TEXT', 'Term', 'foo', 'Counter', 801, 'Size', 10000]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 10],
                    ['Type', 'Loader', 'Counter', 10],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', 'foo @n:[10 15]', 'SORTBY', 'n', *params)
    env.assertEqual(res[0], [10, '10', '110', '210', '310', '410', '510', '610', '710', '810', '910'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (2) range and filter w/o sort ###
    env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'limit', 0 , 2, *params).equal([2, '10', '12'])
    env.expect('ft.search', 'idx', 'foo @n:[10 20]', 'limit', 0 , 3, *params).equal([3, '10', '12', '14'])
    env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'limit', 0 , 2, *params).equal([2, '10', '12'])
    env.expect('ft.search', 'idx_sortable', 'foo @n:[10 20]', 'limit', 0 , 3, *params).equal([3, '10', '12', '14'])

    profiler =  {'Iterators profile':
                    ['Type', 'INTERSECT', 'Counter', 1200, 'Child iterators', [
                        ['Type', 'TEXT', 'Term', 'foo', 'Counter', 1401, 'Size', 10000],
                        ['Type', 'UNION', 'Query type', 'NUMERIC', 'Counter', 1200, 'Child iterators', [
                            ['Type', 'NUMERIC', 'Term', '6 - 12', 'Counter', 400, 'Size', 1600],
                            ['Type', 'NUMERIC', 'Term', '14 - 50', 'Counter', 800, 'Size', 7600]]]]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 1200],
                    ['Type', 'Scorer', 'Counter', 1200],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', 'foo @n:[10 20]', *params)
    env.assertEqual(res[0], [10, '10', '12', '14', '16', '18', '20', '110', '112', '114', '116'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (3) TAG and range with sort ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '110'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '120', '20'])

    profiler =  {'Iterators profile':
                    ['Type', 'OPTIMIZER', 'Counter', 10, 'Optimizer mode', 'Query partial range', 'Child iterator',
                        ['Type', 'TAG', 'Term', 'foo', 'Counter', 1401, 'Size', 10000]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 10],
                    ['Type', 'Loader', 'Counter', 10],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '@tag:{foo} @n:[10 20]', 'SORTBY', 'n', *params)
    env.assertEqual(res[0], [10, '10', '110', '210', '310', '410', '510', '610', '710', '810', '910'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (4) TAG and range w/o sort ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '12'])
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '12', '14'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '12'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo} @n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '12', '14'])

    profiler =  {'Iterators profile':
                    ['Type', 'INTERSECT', 'Counter', 10, 'Child iterators', [
                        ['Type', 'TAG', 'Term', 'foo', 'Counter', 14, 'Size', 10000],
                        ['Type', 'UNION', 'Query type', 'NUMERIC', 'Counter', 10, 'Child iterators', [
                            ['Type', 'NUMERIC', 'Term', '6 - 12', 'Counter', 7, 'Size', 1600],
                            ['Type', 'NUMERIC', 'Term', '14 - 50', 'Counter', 4, 'Size', 7600]]]]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 9],
                    ['Type', 'Pager/Limiter', 'Counter', 9]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '@tag:{foo} @n:[10 15]', *params)
    env.assertEqual(res[0][1:], ['10', '12', '14', '110', '112', '114', '210', '212', '214', '310'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (5) numeric range with sort ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '11'])
    env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '11'])
    env.expect('ft.search', 'idx', '@n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '19921', '19920'])
    env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '10', '11'])
    env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '10', '11'])
    env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '19921', '19920'])

    profiler =  {'Iterators profile':
                    ['Type', 'UNION', 'Query type', 'NUMERIC', 'Counter', 1200, 'Child iterators', [
                        ['Type', 'NUMERIC', 'Term', '6 - 12', 'Counter', 800, 'Size', 1600],
                        ['Type', 'NUMERIC', 'Term', '14 - 50', 'Counter', 400, 'Size', 7600]]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 1200],
                    ['Type', 'Loader', 'Counter', 1200],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '@n:[10 15]', 'SORTBY', 'n', *params)
    env.assertEqual(res[0], [10, '10', '11', '110', '111', '210', '211', '310', '311', '410', '411'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (6) only range ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '@n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '11'])
    env.expect('ft.search', 'idx', '@n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '11', '12'])
    env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'limit', 0 , 2, *params).equal([1, '10', '11'])
    env.expect('ft.search', 'idx_sortable', '@n:[10 20]', 'limit', 0 , 3, *params).equal([1, '10', '11', '12'])

    profiler =  {'Iterators profile':
                    ['Type', 'UNION', 'Query type', 'NUMERIC', 'Counter', 10, 'Child iterators', [
                        ['Type', 'NUMERIC', 'Term', '6 - 12', 'Counter', 8, 'Size', 1600],
                        ['Type', 'NUMERIC', 'Term', '14 - 50', 'Counter', 3, 'Size', 7600]]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 9],
                    ['Type', 'Pager/Limiter', 'Counter', 9]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '@n:[10 15]', *params)
    env.assertEqual(res[0], [1, '10', '11', '12', '13', '14', '15', '110', '111', '112', '113'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (7) filter with sort ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])
    env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx_sortable', 'foo', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])

    profiler =  {'Iterators profile':
                    ['Type', 'OPTIMIZER', 'Counter', 10, 'Optimizer mode', 'Hybrid', 'Child iterator',
                        ['Type', 'TEXT', 'Term', 'foo', 'Counter', 800, 'Size', 10000]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 10],
                    ['Type', 'Loader', 'Counter', 10],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', 'foo', 'SORTBY', 'n', *params)
    env.assertEqual(res[0], [10, '0', '100', '200', '300', '400', '500', '600', '700', '800', '900'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    result = env.cmd('ft.search', 'idx', 'foo', 'SORTBY', 'n', 'limit', 0 , 1500, *params)
    env.assertEqual(result[0], 1500)

    ### (8) filter w/o sort (by score) ###
    # search over all matches
    env.expect('ft.search', 'idx', 'foo', 'limit', 0 , 2, *params).equal([2, '0', '2'])
    env.expect('ft.search', 'idx', 'foo', 'limit', 0 , 3, *params).equal([3, '0', '2', '4'])
    env.expect('ft.search', 'idx_sortable', 'foo', 'limit', 0 , 2, *params).equal([2, '0', '2'])
    env.expect('ft.search', 'idx_sortable', 'foo', 'limit', 0 , 3, *params).equal([3, '0', '2', '4'])

    profiler =  {'Iterators profile':
                    ['Type', 'TEXT', 'Term', 'foo', 'Counter', 10000, 'Size', 10000],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 10000],
                    ['Type', 'Scorer', 'Counter', 10000],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', 'foo', *params)
    env.assertEqual(res[0], [10, '0', '2', '4', '6', '8', '10', '12', '14', '16', '18'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (9) no sort, no score, with sortby ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '100'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '198', '98'])

    profiler =  {'Iterators profile':
                    ['Type', 'OPTIMIZER', 'Counter', 10, 'Optimizer mode', 'Query partial range', 'Child iterator',
                        ['Type', 'TAG', 'Term', 'foo', 'Counter', 800, 'Size', 10000]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 10],
                    ['Type', 'Loader', 'Counter', 10],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '@tag:{foo}', 'SORTBY', 'n', *params)
    env.assertEqual(res[0], [10, '0', '100', '200', '300', '400', '500', '600', '700', '800', '900'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (10) no sort, no score, no sortby ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '@tag:{foo}', 'limit', 0 , 2, *params).equal([1, '0', '2'])
    env.expect('ft.search', 'idx', '@tag:{foo}', 'limit', 0 , 3, *params).equal([1, '0', '2', '4'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'limit', 0 , 2, *params).equal([1, '0', '2'])
    env.expect('ft.search', 'idx_sortable', '@tag:{foo}', 'limit', 0 , 3, *params).equal([1, '0', '2', '4'])

    profiler =  {'Iterators profile':
                    ['Type', 'TAG', 'Term', 'foo', 'Counter', 10, 'Size', 10000],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 9],
                    ['Type', 'Pager/Limiter', 'Counter', 9]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '@tag:{foo}', *params)
    env.assertEqual(res[0][1:], ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (11) wildcard with sort ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '1'])
    env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '1'])
    env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '99', '98'])
    env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'limit', 0 , 2, *params).equal([2, '0', '1'])
    env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'ASC', 'limit', 0 , 2, *params).equal([2, '0', '1'])
    env.expect('ft.search', 'idx_sortable', '*', 'SORTBY', 'n', 'DESC', 'limit', 0 , 2, *params).equal([2, '99', '98'])

    profiler =  {'Iterators profile':
                    ['Type', 'OPTIMIZER', 'Counter', 10, 'Optimizer mode', 'Query partial range', 'Child iterator',
                        ['Type', 'WILDCARD', 'Counter', 1400]],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 10],
                    ['Type', 'Loader', 'Counter', 10],
                    ['Type', 'Sorter', 'Counter', 10]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '*', 'SORTBY', 'n', *params)
    env.assertEqual(res[0], [10, '0', '1', '100', '101', '200', '201', '300', '301', '400', '401'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    ### (12) wildcard w/o sort ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '*', 'limit', 0 , 2, *params).equal([1, '0', '1'])
    env.expect('ft.search', 'idx', '*', 'limit', 0 , 3, *params).equal([1, '0', '1', '2'])
    env.expect('ft.search', 'idx_sortable', '*', 'limit', 0 , 2, *params).equal([1, '0', '1'])
    env.expect('ft.search', 'idx_sortable', '*', 'limit', 0 , 3, *params).equal([1, '0', '1', '2'])

    profiler =  {'Iterators profile':
                    ['Type', 'WILDCARD', 'Counter', 10],
                 'Result processors profile': [
                    ['Type', 'Index', 'Counter', 9],
                    ['Type', 'Pager/Limiter', 'Counter', 9]]}
    res = env.cmd('ft.profile', 'idx', 'search', 'query', '*', *params)
    env.assertEqual(res[0][1:], ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'])
    actual_profiler = to_dict(res[1][1][0])
    env.assertEqual(actual_profiler['Iterators profile'], profiler['Iterators profile'])
    env.assertEqual(actual_profiler['Result processors profile'], profiler['Result processors profile'])

    result = env.cmd('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', 'limit', 0 , 1500, *params)
    env.assertEqual(result[0], 1500)

    result = env.cmd('ft.search', 'idx', 'foo @n:[10 20]', 'SORTBY', 'n', 'limit', 0 , 1500, *params)
    env.assertEqual(result[0], 1200)

@skip(cluster=True)
def testWOLimit(env):
    env.cmd(config_cmd(), 'set', 'timeout', '0')
    env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
    repeat = 100
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT', 'tag', 'TAG')
    env.cmd('FT.CREATE', 'idx_sortable', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'tag', 'TAG')

    words = ['hello', 'world', 'foo', 'bar', 'baz']
    for i in range(0,repeat,5):
        for j in range(5):
            conn.execute_command('hset', i + j, 't', words[j], 'tag', words[j], 'n', i + j)

    numeric_info = conn.execute_command(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n')
    env.debugPrint(str(numeric_info), force=True)
    params = ['NOCONTENT', 'WITHOUTCOUNT']

    res10 = ['12', '17', '22', '27', '32', '37', '42', '47', '52', '57']
    res6 = ['12', '17', '22', '27', '32', '37']

    ### (1) range and filter with sort ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', 'foo @n:[10 70]', 'SORTBY', 'n', *params).equal([10] + res10)
    env.expect('ft.search', 'idx', 'foo @n:[10 40]', 'SORTBY', 'n', *params).equal([6] + res6)

    ### (2) range and filter w/o sort ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', 'foo @n:[10 70]', *params).equal([10] + res10)
    env.expect('ft.search', 'idx', 'foo @n:[10 40]', *params).equal([6] + res6)

    ### (3) TAG and range with sort ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 70]', 'SORTBY', 'n', *params).equal([10] + res10)
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 40]', 'SORTBY', 'n', *params).equal([6] + res6)

    ### (4) TAG and range w/o sort ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 70]', *params).equal([1] + res10)
    env.expect('ft.search', 'idx', '@tag:{foo} @n:[10 40]', *params).equal([1] + res6)

    ### (5) numeric range with sort ###
    # Search only minimal number of ranges
    res3 = ['10', ['n', '10'], '11', ['n', '11'], '12', ['n', '12']]
    res10 = ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19']
    env.expect('ft.search', 'idx', '@n:[10 50]', 'SORTBY', 'n', *params).equal([10] + res10)
    env.expect('ft.search', 'idx', '@n:[10 12]', 'SORTBY', 'n', 'WITHOUTCOUNT', 'RETURN', 1, 'n').equal([3] + res3)

    ### (6) only range ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '@n:[10 50]', *params).equal([1] + res10)
    env.expect('ft.search', 'idx', '@n:[10 12]', 'WITHOUTCOUNT', 'RETURN', 1, 'n').equal([1] + res3)

    ### (7) filter with sort ###
    # Search only minimal number of ranges
    res10 = ['2', '7', '12', '17', '22', '27', '32', '37', '42', '47']
    env.expect('ft.search', 'idx', 'foo', 'SORTBY', 'n', *params).equal([10] + res10)

    ### (8) filter w/o sort (by score) ###
    # search over all matches
    env.expect('ft.search', 'idx', 'foo', *params).equal([10] + res10)

    ### (9) no sort, no score, with sortby ###
    # Search only minimal number of ranges
    env.expect('ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n', *params).equal([10] + res10)

    ### (10) no sort, no score, no sortby ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '@tag:{foo}', *params).equal([1] + res10)

    ### (11) wildcard with sort ###
    # Search only minimal number of ranges
    res10 = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    env.expect('ft.search', 'idx', '*', 'SORTBY', 'n', *params).equal([10] + res10)

    ### (12) wildcard w/o sort ###
    # stop after enough results were collected
    env.expect('ft.search', 'idx', '*', *params).equal([1] + res10)

@skip(cluster=True)
def testSearch(env):
    repeat = 1000
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT', 'tag', 'TAG')
    env.cmd('FT.CREATE', 'idx_sortable', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'tag', 'TAG')

    words = ['hello', 'world', 'foo', 'bar', 'baz']
    for i in range(0, repeat, len(words)):
        for j in range(len(words)):
            conn.execute_command('hset', i + j, 't', words[j], 'tag', words[j], 'n', i % 100 + j)

    params = ['limit', 0 , 0]
    limits = [[0, 5], [0, 30], [0, 150], [5, 5], [20, 30], [100, 10], [500, 1]]
    ranges = [[-5, 105], [0, 3], [30, 60], [-10, 5], [95, 110], [200, 300], [42, 42]]

    for _ in env.reloadingIterator():
        for i in range(len(limits)):
            params[1] = limits[i][0]
            params[2] = limits[i][1]
            for j in range(len(ranges)):
                numRange = '@n:[%d %d]' % (ranges[j][0],ranges[j][1])

                ### (1) TEXT and range with sort ###
                compare_optimized_to_not(env, ['ft.search', 'idx', 'foo ' + numRange, 'SORTBY', 'n'], params, 'case 1 ' + numRange)

                ### (2) TEXT and range w/o sort ###
                compare_optimized_to_not(env, ['ft.search', 'idx', 'foo ' + numRange], params, 'case 2 ' + numRange)

                ### (3) TAG and range with sort ###
                compare_optimized_to_not(env, ['ft.search', 'idx', '@tag:{foo} ' + numRange, 'SORTBY', 'n'], params, 'case 3 ' + numRange)

                ### (4) TAG and range w/o sort ###
                compare_optimized_to_not(env, ['ft.search', 'idx', '@tag:{foo} ' + numRange], params, 'case 4 ' + numRange)

                ### (5) numeric range with sort ###
                compare_optimized_to_not(env, ['ft.search', 'idx', numRange, 'SORTBY', 'n'], params, 'case 5 ' + numRange)

                ### (6) only range ###
                compare_optimized_to_not(env, ['ft.search', 'idx', numRange], params, 'case 6')

            ### (7) filter with sort ###
            # Search only minimal number of ranges
            compare_optimized_to_not(env, ['ft.search', 'idx', 'foo', 'SORTBY', 'n'], params, 'case 7')

            ### (8) filter w/o sort (by score) ###
            # search over all matches
            compare_optimized_to_not(env, ['ft.search', 'idx', 'foo'], params, 'case 8')

            ### (9) no sort, no score, with sortby ###
            # Search only minimal number of ranges
            compare_optimized_to_not(env, ['ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n'], params, 'case 9')

            ### (10) no sort, no score, no sortby ###
            # stop after enough results were collected
            compare_optimized_to_not(env, ['ft.search', 'idx', '@tag:{foo}'], params, 'case 10')

            ### (11) wildcard with sort ###
            # Search only minimal number of ranges
            compare_optimized_to_not(env, ['ft.search', 'idx', '*', 'SORTBY', 'n'], params, 'case 11')

            ### (12) wildcard w/o sort ###
            # stop after enough results were collected
            compare_optimized_to_not(env, ['ft.search', 'idx', '*'], params, 'case 12')
        #input('stop')

@skip(cluster=True)
def testAggregate(env):
    repeat = 1000
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'SORTABLE', 'tag', 'TAG', 'SORTABLE')

    words = ['hello', 'world', 'foo', 'bar', 'baz']
    for i in range(0, repeat, len(words)):
        for j in range(len(words)):
            conn.execute_command('hset', i + j, 't', words[j], 'tag', words[j], 'n', i % 100 + j)

    limits = [[0, 5], [0, 30], [0, 150], [5, 5], [20, 30], [100, 10], [500, 1]]
    ranges = [[-5, 105], [0, 3], [30, 60], [-10, 5], [95, 110], [200, 300], [42, 42]]
    params = ['limit', 0 , 0, 'LOAD', 4, '@__key', '@n', '@t', '@tag']

    for _ in env.reloadingIterator():
        for i in range(len(limits)):
            params[1] = limits[i][0]
            params[2] = limits[i][1]

            for j in range(len(ranges)):
                numRange = '@n:[%d %d]' % (ranges[j][0],ranges[j][1])

                ### (1) TEXT and range with sort ###
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', 'foo ' + numRange, 'SORTBY', 2, '@n', 'ASC'], params, 'case 1 ' + numRange)

                ### (2) TEXT and range w/o sort ###
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', 'foo ' + numRange], params, 'case 2 ' + numRange)

                ### (3) TAG and range with sort ###
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', '@tag:{foo} ' + numRange, 'SORTBY', 2, '@n', 'ASC'], params, 'case 3 ' + numRange)

                ### (4) TAG and range w/o sort ###
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', '@tag:{foo} ' + numRange], params, 'case 4 ' + numRange)

                ### (5) numeric range with sort ###
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', numRange, 'SORTBY', 2, '@n', 'ASC'], params, 'case 5 ' + numRange)

                ### (6) only range ###
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', numRange], params, 'case 6 ' + numRange)

            ### (7) filter with sort ###
            # aggregate only minimal number of ranges
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', 'foo', 'SORTBY', 2, '@n', 'ASC'], params, 'case 7')

            ### (8) filter w/o sort (by score) ###
            # aggregate over all matches
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', 'foo'], params, 'case 8')

            ### (9) no sort, no score, with sortby ###
            # aggregate only minimal number of ranges
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', '@tag:{foo}', 'SORTBY', 2, '@n', 'ASC'], params, 'case 9')

            ### (10) no sort, no score, no sortby ###
            # stop after enough results were collected
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', '@tag:{foo}'], params, 'case 10')

            ### (11) wildcard with sort ###
            # aggregate only minimal number of ranges
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', '*', 'SORTBY', 2, '@n', 'ASC'], params, 'case 11')

            ### (12) wildcard w/o sort ###
            # stop after enough results were collected
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', '*'], params, 'case 12')
        #input('stop')

@skip()  # TODO: solve flakiness (MOD-5257)
def testCoordinator(env):
    # separate test which only has queries with sortby since otherwise the coordinator has random results
    repeat = 10000
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT', 'tag', 'TAG')
    env.cmd('FT.CREATE', 'idx_sortable', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'tag', 'TAG')

    words = ['hello', 'world', 'foo', 'bar', 'baz']
    for i in range(0, repeat, len(words)):
        for j in range(len(words)):
            conn.execute_command('hset', i + j, 't', words[j], 'tag', words[j], 'n', i % 1000 + j)

    limits = [[0, 5], [0, 30], [0, 150], [5, 5], [20, 30], [100, 10], [500, 1]]
    ranges = [[-5, 105], [0, 3], [30, 60], [-10, 5], [95, 110], [200, 300], [42, 42]]
    params = ['limit', 0 , 0]

    for _ in env.reloadingIterator():
        for i in range(len(limits)):
            params[1] = limits[i][0]
            params[2] = limits[i][1]
            for j in range(len(ranges)):
                rg = ranges[j]
                numRange = f'@n:[{rg[0]} {rg[1]}]'

                ### (1) TEXT and range with sort
                compare_optimized_to_not(env, ['ft.search', 'idx', 'foo ' + numRange, 'SORTBY', 'n'], params, 'case 1 ' + numRange)

                ### (3) TAG and range with sort
                compare_optimized_to_not(env, ['ft.search', 'idx', '@tag:{foo} ' + numRange, 'SORTBY', 'n'], params, 'case 3 ' + numRange)

                ### (5) numeric range with sort
                compare_optimized_to_not(env, ['ft.search', 'idx', numRange, 'SORTBY', 'n'], params, 'case 5 ' + numRange)

            ### (7) filter with sort
            # Search only minimal number of ranges
            compare_optimized_to_not(env, ['ft.search', 'idx', 'foo', 'SORTBY', 'n'], params, 'case 7')

            ### (9) no sort, no score, with sortby
            # Search only minimal number of ranges
            compare_optimized_to_not(env, ['ft.search', 'idx', '@tag:{foo}', 'SORTBY', 'n'], params, 'case 9')

            ### (11) wildcard with sort
            # Search only minimal number of ranges
            compare_optimized_to_not(env, ['ft.search', 'idx', '*', 'SORTBY', 'n'], params, 'case 11')

    # update parameters for ft.aggregate
    params = ['limit', 0 , 0, 'LOAD', 4, '@__key', '@n', '@t', '@tag']

    for _ in env.reloadingIterator():
        for i in range(len(limits)):
            params[1] = limits[i][0]
            params[2] = limits[i][1]

            for j in range(len(ranges)):
                rg = ranges[j]
                numRange = f'@n:[{rg[0]} {rg[1]}]'

                ### (1) TEXT and range with sort
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', 'foo ' + numRange, 'SORTBY', 2, '@n', 'ASC'], params, 'case 1 ' + numRange)

                ### (3) TAG and range with sort
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', '@tag:{foo} ' + numRange, 'SORTBY', 2, '@n', 'ASC'], params, 'case 3 ' + numRange)

                ### (5) numeric range with sort
                compare_optimized_to_not(env, ['ft.aggregate', 'idx', numRange, 'SORTBY', 2, '@n', 'ASC'], params, 'case 5 ' + numRange)

            ### (7) filter with sort ###
            # aggregate only minimal number of ranges
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', 'foo', 'SORTBY', 2, '@n', 'ASC'], params, 'case 7')

            ### (9) no sort, no score, with sortby
            # aggregate only minimal number of ranges
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', '@tag:{foo}', 'SORTBY', 2, '@n', 'ASC'], params, 'case 9')

            ### (11) wildcard with sort
            # aggregate only minimal number of ranges
            compare_optimized_to_not(env, ['ft.aggregate', 'idx', '*', 'SORTBY', 2, '@n', 'ASC'], params, 'case 11')

def testVector():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 _PRINT_PROFILE_CLOCK FALSE')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE idx SCHEMA v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 t TEXT n NUMERIC').ok()
    conn.execute_command('hset', '1', 'v', 'bababaca', 't', "hello", 'n', 1)
    conn.execute_command('hset', '2', 'v', 'babababa', 't', "hello", 'n', 2)
    conn.execute_command('hset', '3', 'v', 'aabbaabb', 't', "hello", 'n', 3)
    conn.execute_command('hset', '4', 'v', 'bbaabbaa', 't', "hello world", 'n', 4)
    conn.execute_command('hset', '5', 'v', 'aaaabbbb', 't', "hello world", 'n', 5)

    search = ['FT.SEARCH', 'idx']
    profile = ['FT.PROFILE', 'idx', 'search', 'query']

    queries = [
        # ['(@t:hello world)=>[KNN 3 @v $vec]', 'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa'],
        ['(@t:hello @n:[1 4])=>[KNN 3 @v $vec]', 'SORTBY', 'n', 'PARAMS', '2', 'vec', 'aaaaaaaa'],
        ['@n:[1 4]=>[KNN 3 @v $vec]', 'PARAMS', '2', 'vec', 'aaaaaaaa'],
    ]

    for idx, query in enumerate(queries):
        # A query with a vector KNN should not be optimized, but should succeed
        env.assertEqual(conn.execute_command(*search, *query), conn.execute_command(*search, *query, 'WITHOUTCOUNT'), message=str(idx))
        if not env.isCluster():
            # Run the same query with profiling, and make sure the query is not optimized
            # (same iterators and pipeline should be used)
            env.assertEqual(conn.execute_command(*profile, *query), conn.execute_command(*profile, *query, 'WITHOUTCOUNT'), message=str(idx))

def testOptimizeArgs(env):
    ''' Test enabling/disabling optimization according to args and dialect '''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')
    for i in range(0, 20):
        conn.execute_command('HSET', i, 'n', i)
    query = ['FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '0', '10']

    # DIALECT 4 ==> WITHOUTCOUNT (if not explicitly specified)
    env.assertEqual(conn.execute_command(*query, 'DIALECT', 4), conn.execute_command(*query, 'WITHOUTCOUNT'))
    env.assertNotEqual(conn.execute_command(*query, 'DIALECT', 4), conn.execute_command(*query, 'WITHCOUNT'))
    # DIALECT 4 and WITHCOUNT explicitly specified ==> WITHCOUNT
    env.assertEqual(conn.execute_command(*query, 'WITHCOUNT', 'DIALECT', 4), conn.execute_command(*query, 'WITHCOUNT'))
    env.assertNotEqual(conn.execute_command(*query, 'WITHCOUNT', 'DIALECT', 4), conn.execute_command(*query, 'WITHOUTCOUNT'))
    # DIALECT 3 ==> WITHCOUNT (if not explicitly specified)
    env.assertEqual(conn.execute_command(*query, 'DIALECT', 3), conn.execute_command(*query, 'WITHCOUNT'))
    env.assertNotEqual(conn.execute_command(*query, 'DIALECT', 3), conn.execute_command(*query, 'WITHOUTCOUNT'))
    # DIALECT 3 and WITHOUTCOUNT explicitly specified ==> WITHOUTCOUNT
    env.assertEqual(conn.execute_command(*query, 'WITHOUTCOUNT', 'DIALECT', 3), conn.execute_command(*query, 'WITHOUTCOUNT'))
    env.assertNotEqual(conn.execute_command(*query, 'WITHOUTCOUNT', 'DIALECT', 3), conn.execute_command(*query, 'WITHCOUNT'))

def testOptimizeArgsDefault():
    ''' Test enabling/disabling optimization according to args and default dialect '''

    env = Env(moduleArgs='DEFAULT_DIALECT 4')
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')
    for i in range(0, 20):
        conn.execute_command('HSET', i, 'n', i)
    query = ['FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '0', '10']

    # DEFAULT DIALECT 4 ==> WITHOUTCOUNT (if not explicitly specified)
    env.assertEqual(conn.execute_command(*query), conn.execute_command(*query, 'WITHOUTCOUNT'))
    env.assertNotEqual(conn.execute_command(*query), conn.execute_command(*query, 'WITHCOUNT'))
    # DEFAULT DIALECT 4 and WITHCOUNT explicitly specified ==> WITHCOUNT
    env.assertEqual(conn.execute_command(*query, 'WITHCOUNT'), conn.execute_command(*query, 'WITHCOUNT'))
    env.assertNotEqual(conn.execute_command(*query, 'WITHCOUNT'), conn.execute_command(*query, 'WITHOUTCOUNT'))
