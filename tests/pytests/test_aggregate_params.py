import json

import bz2
import itertools
from RLTest import Env

from includes import *
from common import *
import os

GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')


def add_values(env, number_of_iterations=1):
    env.cmd('FT.CREATE', 'games', 'ON', 'HASH',
                        'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                        'brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                        'description', 'TEXT', 'price', 'NUMERIC',
                        'categories', 'TAG')

    for i in range(number_of_iterations):
        fp = bz2.BZ2File(GAMES_JSON, 'r')
        for line in fp:
            obj = json.loads(line)
            id_key = obj['asin'] + (str(i) if i > 0 else '')
            del obj['asin']
            obj['price'] = obj.get('price') or 0
            obj['categories'] = ','.join(obj['categories'])
            cmd = ['FT.ADD', 'games', id_key, 1, 'FIELDS', ] + \
                [str(x) if x is not None else '' for x in itertools.chain(
                    *obj.items())]
            env.cmd(*cmd)
        fp.close()


class TestAggregateParams:
    def __init__(self):
        self.env = Env()
        add_values(self.env)

    def test_group_by(self):
        # cmd = ['ft.aggregate', 'games', '*',
        #        'GROUPBY', '1', '@brand',
        #        'REDUCE', 'count', '0', 'AS', 'count',
        #        'SORTBY', 2, '@count', '$sortfield',
        #        'LIMIT', '0', '5',
        #        'PARAMS', '2', 'sortfield', 'desc'
        #        ]
        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'SORTBY', 2, '@count', 'desc',
               'LIMIT', '0', '5']

        res = self.env.cmd(*cmd)
        self.env.assertIsNotNone(res)
        self.env.assertEqual([292, ['brand', '', 'count', '1518'], ['brand', 'mad catz', 'count', '43'],
                                    ['brand', 'generic', 'count', '40'], ['brand', 'steelseries', 'count', '37'],
                                    ['brand', 'logitech', 'count', '35']], res)

def test_apply(env):
    for dialect in [2, 5]:
        env = Env(moduleArgs = 'DEFAULT_DIALECT {}'.format(dialect))
        conn = getConnectionByEnv(env)
        env.flush()
        env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'dkey', 'SCHEMA',
                   'name', 'TEXT', 'breed', 'TEXT', 'loc', 'GEO',
                   'code', 'TAG').ok()
        waitForIndex(env, 'idx')
        conn.execute_command('HSET', 'dkey:1', 'name', 'Lassie', 'breed', 'Rough Collie', 'code', 'ca?33-22')
        conn.execute_command('HSET', 'dkey:2', 'name', 'lessly', 'breed', 'Poodle', 'code', 'ca?33-22')
        conn.execute_command('HSET', 'dkey:3', 'name', 'Perrito', 'breed', 'poodle', 'code', 'ca?33-22')
        conn.execute_command('HSET', 'dkey:4', 'name', 'Lou Dog', 'breed', 'Dalmatian', 'code', 'ca:99-##')
        conn.execute_command('HSET', 'dkey:5', 'name', 'dipper', 'breed', 'dalmatian', 'code', 'ca:99-##')
        conn.execute_command('HSET', 'dkey:6', 'name', 'Duff', 'breed', 'Dalmatian', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:7', 'name', 'Triumph', 'breed', 'Mountain Hound', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:8', 'name', 'Chuck', 'breed', 'Saluki', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:9', 'name', 'Tuk', 'breed', 'Husky', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:10', 'name', 'Jul', 'breed', 'St. Bernard', 'code', 'gp-33-22')
    for dialect in [2, 5]:
        env = Env(moduleArgs = 'DEFAULT_DIALECT {}'.format(dialect))
        conn = getConnectionByEnv(env)
        env.flush()
        env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'dkey', 'SCHEMA',
                   'name', 'TEXT', 'breed', 'TEXT', 'loc', 'GEO',
                   'code', 'TAG').ok()
        waitForIndex(env, 'idx')
        conn.execute_command('HSET', 'dkey:1', 'name', 'Lassie', 'breed', 'Rough Collie', 'code', 'ca?33-22')
        conn.execute_command('HSET', 'dkey:2', 'name', 'lessly', 'breed', 'Poodle', 'code', 'ca?33-22')
        conn.execute_command('HSET', 'dkey:3', 'name', 'Perrito', 'breed', 'poodle', 'code', 'ca?33-22')
        conn.execute_command('HSET', 'dkey:4', 'name', 'Lou Dog', 'breed', 'Dalmatian', 'code', 'ca:99-##')
        conn.execute_command('HSET', 'dkey:5', 'name', 'dipper', 'breed', 'dalmatian', 'code', 'ca:99-##')
        conn.execute_command('HSET', 'dkey:6', 'name', 'Duff', 'breed', 'Dalmatian', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:7', 'name', 'Triumph', 'breed', 'Mountain Hound', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:8', 'name', 'Chuck', 'breed', 'Saluki', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:9', 'name', 'Tuk', 'breed', 'Husky', 'code', 'gp-33-22')
        conn.execute_command('HSET', 'dkey:10', 'name', 'Jul', 'breed', 'St. Bernard', 'code', 'gp-33-22')

        res1 = env.cmd('ft.aggregate', 'idx', '@breed:(Dal*|Poo*|Ru*|Mo*)', 'LOAD', '2', '@name', '@breed', 'FILTER', 'exists(@breed)', 'APPLY', 'upper(@name)', 'AS', 'n', 'APPLY', 'upper(@breed)', 'AS', 'b', 'SORTBY', '4', '@b', 'ASC', '@n', 'ASC')
        res2 = env.cmd('ft.aggregate', 'idx', '@breed:($p1*|$p2*|$p3*|$p4*)', 'LOAD', '2', '@name', '@breed', 'FILTER', 'exists(@breed)', 'APPLY', 'upper(@name)', 'AS', 'n', 'APPLY', 'upper(@breed)', 'AS', 'b', 'SORTBY', '4', '@b', 'ASC', '@n', 'ASC', 'PARAMS', '8', 'p1', 'Dal', 'p2', 'Poo', 'p3', 'Ru', 'p4', 'Mo')
        env.assertEqual(res2, res1)
        res1 = env.cmd('ft.aggregate', 'idx', '@breed:(Dal*|Poo*|Ru*|Mo*)', 'SORTBY', '1', '@name')
        res2 = env.cmd('ft.aggregate', 'idx', '@breed:($p1*|$p2*|$p3*|$p4*)', 'PARAMS', '8', 'p1', 'Dal', 'p2', 'Poo', 'p3', 'Ru', 'p4', 'Mo', 'SORTBY', '1', '@name')
        env.assertEqual(res2, res1)

        # Tag autoescaping
        if dialect == 5:
            res1 = env.cmd('ft.aggregate', 'idx', '@code:{ca?33-22}',
                           'GROUPBY', '1', '@code',
                           'REDUCE', 'COUNT', 0, 'AS', 'total')
            env.assertEqual(res1, [1, ['code', 'ca?33-22', 'total', '3']])
            res2 = env.cmd('ft.aggregate', 'idx', '@code:{$p1}',
                           'PARAMS', '2', 'p1', 'ca?33-22', 
                           'GROUPBY', '1', '@code',
                           'REDUCE', 'COUNT', 0, 'AS', 'total')
            env.assertEqual(res2, res1)

            res1 = env.cmd('ft.aggregate', 'idx', "@code:{*:99-##}",
                           'GROUPBY', '1', '@code',
                           'REDUCE', 'COUNT', 0, 'AS', 'total')
            env.assertEqual(res1, [1, ['code', 'ca:99-##', 'total', '2']])
            res2 = env.cmd('ft.aggregate', 'idx', "@code:{*$p1}",
                           'PARAMS', '2', 'p1', ':99-##', 
                           'GROUPBY', '1', '@code',
                           'REDUCE', 'COUNT', 0, 'AS', 'total')
            env.assertEqual(res2, res1)
