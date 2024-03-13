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
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'dkey', 'SCHEMA', 'name', 'TEXT', 'breed', 'TEXT', 'loc', 'GEO').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'dkey:1', 'name', 'Lassie', 'breed', 'Rough Collie')
    conn.execute_command('HSET', 'dkey:2', 'name', 'lessly', 'breed', 'Poodle')
    conn.execute_command('HSET', 'dkey:3', 'name', 'Perrito', 'breed', 'poodle')
    conn.execute_command('HSET', 'dkey:4', 'name', 'Lou Dog', 'breed', 'Dalmatian')
    conn.execute_command('HSET', 'dkey:5', 'name', 'dipper', 'breed', 'dalmatian')
    conn.execute_command('HSET', 'dkey:6', 'name', 'Duff', 'breed', 'Dalmatian')
    conn.execute_command('HSET', 'dkey:7', 'name', 'Triumph', 'breed', 'Mountain Hound')
    conn.execute_command('HSET', 'dkey:8', 'name', 'Chuck', 'breed', 'Saluki')
    conn.execute_command('HSET', 'dkey:9', 'name', 'Tuk', 'breed', 'Husky')
    conn.execute_command('HSET', 'dkey:10', 'name', 'Jul', 'breed', 'St. Bernard')

    res1 = env.cmd('ft.aggregate', 'idx', '@breed:(Dal*|Poo*|Ru*|Mo*)', 'LOAD', '2', '@name', '@breed', 'FILTER', 'exists(@breed)', 'APPLY', 'upper(@name)', 'AS', 'n', 'APPLY', 'upper(@breed)', 'AS', 'b', 'SORTBY', '4', '@b', 'ASC', '@n', 'ASC')
    res2 = env.cmd('ft.aggregate', 'idx', '@breed:($p1*|$p2*|$p3*|$p4*)', 'LOAD', '2', '@name', '@breed', 'FILTER', 'exists(@breed)', 'APPLY', 'upper(@name)', 'AS', 'n', 'APPLY', 'upper(@breed)', 'AS', 'b', 'SORTBY', '4', '@b', 'ASC', '@n', 'ASC', 'PARAMS', '8', 'p1', 'Dal', 'p2', 'Poo', 'p3', 'Ru', 'p4', 'Mo')
    env.assertEqual(res2, res1)
    res1 = env.cmd('ft.aggregate', 'idx', '@breed:(Dal*|Poo*|Ru*|Mo*)', 'SORTBY', '1', '@name')
    res2 = env.cmd('ft.aggregate', 'idx', '@breed:($p1*|$p2*|$p3*|$p4*)', 'PARAMS', '8', 'p1', 'Dal', 'p2', 'Poo', 'p3', 'Ru', 'p4', 'Mo', 'SORTBY', '1', '@name')
    env.assertEqual(res2, res1)
