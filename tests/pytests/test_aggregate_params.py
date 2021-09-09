import json

import bz2
import itertools
from RLTest import Env

from includes import *


def to_dict(res):
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d


GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')


def add_values(env, number_of_iterations=1):
    env.execute_command('FT.CREATE', 'games', 'ON', 'HASH',
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
            env.execute_command(*cmd)
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
        self.env.assertEqual([292L, ['brand', '', 'count', '1518'], ['brand', 'mad catz', 'count', '43'],
                                    ['brand', 'generic', 'count', '40'], ['brand', 'steelseries', 'count', '37'],
                                    ['brand', 'logitech', 'count', '35']], res)

    def test_min_max(self):
        # cmd = ['ft.aggregate', 'games', 'sony',
        #        'GROUPBY', '1', '@brand',
        #        'REDUCE', 'count', '0',
        #        'REDUCE', 'min', '1', '$prop', 'as', '$propas',
        #        'SORTBY', '2', '@minPrice', 'DESC',
        #        'PARAMS', '4', 'prop', '@price', 'propas', 'minPrice']
        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'REDUCE', 'min', '1', '@price', 'as', 'minPrice',
               'SORTBY', '2', '@minPrice', 'DESC']
        res = self.env.cmd(*cmd)
        self.env.assertIsNotNone(res)
        row = to_dict(res[1])
        self.env.assertEqual(88, int(float(row['minPrice'])))

        # cmd = ['ft.aggregate', 'games', 'sony',
        #        'GROUPBY', '1', '@brand',
        #        'REDUCE', 'count', '0',
        #        'REDUCE', 'max', '1', '@price', 'as', 'maxPrice',
        #        'SORTBY', '2', '$sortProp', '$sortOrder',
        #        'PARAMS', '4', 'sortProp', '@maxPrice', '$sortOrder', 'DESC']
        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'REDUCE', 'max', '1', '@price', 'as', 'maxPrice',
               'SORTBY', '2', '@maxPrice', 'DESC']
        res = self.env.cmd(*cmd)
        row = to_dict(res[1])
        self.env.assertEqual(695, int(float(row['maxPrice'])))

