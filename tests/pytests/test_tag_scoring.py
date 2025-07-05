# -*- coding: utf-8 -*-

from RLTest import Env
from includes import *
from common import *

def _prepare_index(env, idx, dim=4):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', idx, 'SCHEMA',
               'tag', 'TAG',
               'txt', 'TEXT',
               'num', 'NUMERIC').ok()

    conn.execute_command('HSET', 'doc1', 'num', '1', 'tag', 'pizza', 'txt', 'tomato')
    conn.execute_command('HSET', 'doc2', 'num', '2', 'tag', 'pizza', 'txt', 'cheese')
    conn.execute_command('HSET', 'doc3', 'num', '3', 'tag', 'pizza', 'txt', 'tomatoes')
    conn.execute_command('HSET', 'doc4', 'num', '4', 'tag', 'pizza')
    conn.execute_command('HSET', 'doc5', 'num', '5', 'tag', 'beer', 'txt', 'cheese')
    conn.execute_command('HSET', 'doc6', 'num', '6', 'tag', 'beer')
    conn.execute_command('HSET', 'doc7', 'num', '7', 'txt', 'tomato')
    conn.execute_command('HSET', 'doc8', 'num', '8')


def _print_scores(env, res):
    scores = {row[row.index('__key') + 1]: row[row.index('__score') + 1] for row in res[1:]}
    for k, v in scores.items():
        print(k, v)


def testTagScoring(env):
    _prepare_index(env, 'idx')

    queries_and_expected_scores = [
            '@tag:{pizza} | @tag:{beer}',
            '@num:[1 8]',
            '@tag:{pizza} | @txt:(tomato)',
        # (
        #
        #     [0] # TODO
        # ),
        # (
        #     '@tag:{pizza} | @txt:(tomato) | @num:[1 7] | @num:[2 6]',
        #     [0] # TODO
        # ),
    ]

    for query in queries_and_expected_scores:
        res = env.cmd(
            'FT.SEARCH', 'idx', query, 'WITHSCORES', 'DIALECT', 2)
        print(res)

        res = env.cmd(
            'FT.AGGREGATE', 'idx', query, 'ADDSCORES',
            'LOAD', 2, '@__key', '@tag',
            'SORTBY', 4, '@__score', 'DESC', '@__key', 'ASC',
            'APPLY', 'case(exists(@txt), @__score, 0)', 'AS', 'txt_scores',
            'DIALECT', 2
        )
        agg_num_res = res[0]
        agg_scores = [float(row[row.index('__score') + 1]) for row in res[1:]]
        txt_scores = [float(row[row.index('txt_scores') + 1]) for row in res[1:]]
        print(res)
        _print_scores(env, res)

        res = env.cmd(
            'FT.HYBRID', 'idx', query, 'ADDSCORES',
            'LOAD', 2, '@__key', '@tag',
            'SORTBY', 4, '@__score', 'DESC', '@__key', 'ASC',
            'DIALECT', 2
        )
        env.assertEqual(agg_num_res, res[0])
        print(res)
        _print_scores(env, res)
        hyb_scores = [float(row[row.index('__score') + 1]) for row in res[1:]]

        print(agg_scores, hyb_scores)
        env.assertGreaterEqual(agg_scores, hyb_scores)


