from common import *

import bz2
import json
import distro
import unittest
from datetime import datetime, timezone

GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')

def add_values(env, number_of_iterations=1):
    env.cmd('FT.CREATE', 'games', 'ON', 'HASH',
                        'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                        'brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                        'description', 'TEXT', 'price', 'NUMERIC',
                        'categories', 'TAG')
    con = env.getClusterConnectionIfNeeded()
    for i in range(number_of_iterations):
        fp = bz2.BZ2File(GAMES_JSON, 'r')
        for line in fp:
            obj = json.loads(line)
            id = obj['asin'] + (str(i) if i > 0 else '')
            del obj['asin']
            obj['price'] = obj.get('price') or 0
            obj['categories'] = ','.join(obj['categories'])
            cmd = ['HSET', id] + \
                [str(x) if x is not None else '' for x in itertools.chain(
                    *obj.items())]
            con.execute_command(*cmd)
        fp.close()

def _get_total_results(res):
    if isinstance(res, dict):
        return res['total_results']
    else:
        return res[0]


queries_and_expected_counts = [
    # WITHOUTCOUNT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'SORTBY', '2', '@title', 'ASC'], # Success (not an error)
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'SORTBY', '2', '@price', 'ASC'], # crash
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'SORTBY', '2', '@title', 'ASC'], # Success (not an error)
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'SORTBY', '2', '@price', 'ASC'], # crash
    # WITHCOUNT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '1', '@price'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'],
]


def __test_protocol(protocol):
    env = Env(protocol=protocol)
    add_values(env)
    expected_count = 2265
    for query in queries_and_expected_counts:
        print(query)
        for dialect in [2]:
            query.append('DIALECT')
            query.append(dialect)
            res = env.cmd(*query)
            print('dialect:', dialect, 'count:', _get_total_results(res), expected_count)
            err_message = f"query: {' '.join(str(x) for x in query)}"
            if 'WITHCOUNT' in query:
                env.assertEqual(_get_total_results(res), expected_count, message=err_message)
            else:
                env.assertLess(_get_total_results(res), expected_count, message=err_message)

def test3():
    __test_protocol(3)

def test2():
    __test_protocol(2)


# def _testWithCount(env):
#     for dialect in [2]:
#         env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
#         err_message = f'dialect: {dialect}'

#         expected = 2265

#         # Test WITHCOUNT without SORTBY
#         res = env.cmd('FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'NOCONTENT')
#         env.assertEqual(_get_total_results(res), expected, message=err_message)

#         # Test WITHCOUNT with SORTBY numeric field
#         res = env.cmd(
#             'FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'NOCONTENT',
#             'SORTBY', '1', '@price')
#         env.assertEqual(_get_total_results(res), expected, message=err_message)

#         # Test WITHCOUNT with SORTBY text field
#         res = env.cmd(
#             'FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'NOCONTENT',
#             'SORTBY', '1', '@title')
#         env.assertEqual(_get_total_results(res), expected, message=err_message)


# def _testWithoutCount(env):
#     for dialect in [2]:
#         env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
#         err_message = f'dialect: {dialect}'

#         # Test default behavior
#         res = env.cmd('FT.AGGREGATE', 'games', '*', 'NOCONTENT')
#         # By default WITHCOUNT is off, so we don't get total count
#         env.assertLess(_get_total_results(res), 2265, message=err_message)

#         # Test explicit WITHOUTCOUNT
#         res = env.cmd('FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT')
#         env.assertLess(_get_total_results(res), 2265, message=err_message)

#         # TODO: Crash: Optimized + SORTBY numeric
#         res = env.cmd(
#             'FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT',
#             'SORTBY', '1', '@price')
#         env.assertLess(_get_total_results(res), 2265, message=err_message)
#         res = env.cmd(
#             'FT.SEARCH', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT',
#             'SORTBY', 'price')
#         env.assertLess(_get_total_results(res), 2265, message=err_message)

#         # TODO: This is returning: Success (not an error) if Optimized + SORTBY TEXT
#         res = env.cmd(
#             'FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT',
#             'SORTBY', '1', '@title')
#         env.assertLess(_get_total_results(res), 2265, message=err_message)

#         # TODO: This is returning: Success (not an error)
#         res = env.cmd(
#             'FT.AGGREGATE', 'games', '*', 'NOCONTENT',
#             'SORTBY', '1', '@title')
#         env.assertLess(_get_total_results(res), 2265, message=err_message)


# def test_resp3():
#     env = Env(protocol=3)
#     add_values(env)
#     _testWithoutCount(env)
#     _testWithCount(env)


# def test_resp2():
#     env = Env(protocol=2)
#     add_values(env)
#     _testWithoutCount(env)
#     _testWithCount(env)


