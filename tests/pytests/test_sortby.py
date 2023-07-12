# -*- coding: utf-8 -*-

from cmath import inf
from email import message
from includes import *
from common import *
from RLTest import Env

def check_order(env, item1, item2, asc=True):
    if not asc:
        item1, item2 = item2, item1

    try:
        item1 = float(item1)
    except:
        item1 = -inf if '-inf' in item1 else inf
    try:
        item1 = float(item1)
    except:
        item2 = -inf if '-inf' in item2 else inf

    if float(item1) > float(item2):
        env.assertTrue(float(item1) <= float(item2))
        return False
    return True


# check order within returned results
def check_sortby(env, query, params, msg=None):
    cmds = ['ft.search', 'ft.aggregate']
    idx = 2 if query[0] == cmds[0] else 1
    msg = cmds[idx % 2] + ' limit %d %d : ' % (params[1], params[2]) + msg

    sort_order = ['ASC', 'DESC']
    for sort in range(len(sort_order)):
        print_err = False
        res = env.cmd(*query, sort_order[sort], *params)

        # put all `n` values into a list
        res_list = [to_dict(n)['n'] for n in res[idx::idx]]
        err_msg = msg + ' : ' + sort_order[sort] + ' : len=%d' % len(res_list)

        for i in range(len(res_list) - 1):
            if not check_order(env, res_list[i], res_list[i+1], sort_order[sort] == sort_order[0]):
                print_err = True

        if print_err:
            if (len(res)) < 100:
                env.debugPrint(str(res), force=TEST_DEBUG)
                env.debugPrint(str(res_list), force=TEST_DEBUG)
                input('stop')

        env.assertFalse(print_err, message=err_msg)

# check ASC vs DESC
# number of result must be less than limit
def compare_asc_desc(env, query, params, msg=None):
    asc_res = env.cmd(*query, 'ASC', *params)[1:]
    desc_res = env.cmd(*query, 'DESC', *params)[1:]
    #env.debugPrint(str(asc_res), force=TEST_DEBUG)
    #env.debugPrint(str(desc_res), force=TEST_DEBUG)

    desc_res.reverse()
    cmp_res = []
    for i, j in zip(desc_res[0::2], desc_res[1::2]):
        cmp_res.extend([j, i])
    #env.debugPrint(str(cmp_res), force=TEST_DEBUG)

    failed = False
    for i in range(1,len(asc_res),2):
        if asc_res[i][1] != cmp_res[i][1]:
            env.assertEqual(asc_res[i][1], cmp_res[i][1], message=query)
    env.assertFalse(failed, message = query)


def testSortby(env):
    repeat = 10000
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT', 'tag', 'TAG')
    env.cmd('FT.CREATE', 'idx_sortable', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT', 'tag', 'TAG')

    words = ['hello', 'world', 'foo', 'bar', 'baz']
    for i in range(0, repeat, len(words)):
        for j in range(len(words)):
            conn.execute_command('hset', i + j, 't', words[j], 'tag', words[j], 'n', i % 1000 + j)

    # with inf values
    for j in range(len(words)):
        conn.execute_command('hset', repeat + j, 't', words[j], 'tag', words[j], 'n', 'inf')
    for j in range(len(words)):
        conn.execute_command('hset', repeat + 5 + j, 't', words[j], 'tag', words[j], 'n', '+inf')
    for j in range(len(words)):
        conn.execute_command('hset', repeat + 10 + j, 't', words[j], 'tag', words[j], 'n', '-inf')

    limits = [[0, 5], [0, 30], [0, 150], [5, 5], [20, 30], [100, 10], [500, 100], [5000, 1000], [9900, 1010], [0, 100000]]
    ranges = [['-5', '105'], ['0', '3'], ['30', '60'], ['-10', '5'], ['950', '1100'], ['2000', '3000'],
              ['42', '42'], ['-inf', '+inf'], ['-inf', '100'], ['990', 'inf']]
    params = ['limit', 0 , 0]

    for _ in env.retry_with_rdb_reload():
        for i in range(len(limits)):
            params[1] = limits[i][0]
            params[2] = limits[i][1]
            for j in range(len(ranges)):
                numRange = str('@n:[%s %s]' % (ranges[j][0],ranges[j][1]))

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
                numRange = '@n:[%s %s]' % (ranges[j][0],ranges[j][1])

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

    # with inf values
    params = ['limit', 0, 100, 'return', 1, 'n']
    compare_asc_desc(env, ['ft.search', 'idx', 'foo @n:[995 inf]', 'SORTBY', 'n'], params)
    compare_asc_desc(env, ['ft.search', 'idx', '@n:[999 inf]', 'SORTBY', 'n'], params)
    compare_asc_desc(env, ['ft.search', 'idx', 'foo @n:[-inf 5]', 'SORTBY', 'n'], params)
    compare_asc_desc(env, ['ft.search', 'idx', '@n:[-inf 1]', 'SORTBY', 'n'], params)
    params[2] = 10015
    compare_asc_desc(env, ['ft.search', 'idx', 'foo @n:[-inf inf]', 'SORTBY', 'n'], params)
    compare_asc_desc(env, ['ft.search', 'idx', '@n:[-inf inf]', 'SORTBY', 'n'], params)

