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
            cmd = ['FT.ADD', 'games', id, 1, 'FIELDS', ] + \
                [str(x) if x is not None else '' for x in itertools.chain(
                    *obj.items())]
            con.execute_command(*cmd)
        fp.close()


class TestAggregateCount():
    def __init__(self):
        self.env = Env()
        add_values(self.env)

    def testDefaultCount(self):
        res = self.env.cmd('FT.AGGREGATE', 'games', '*', 'NOCONTENT')
        # By default WITHCOUNT is off, so we don't get total count
        self.env.assertEqual(res[0], 1)

        res = self.env.cmd('FT.SEARCH', 'games', '*', 'NOCONTENT')
        # By default WITHCOUNT is on, so we get total count
        self.env.assertEqual(res[0], 2265)

    def testWithCount(self):
        res = self.env.cmd('FT.SEARCH', 'games', '*', 'WITHCOUNT', 'NOCONTENT')
        expected_count = res[0]

        # Test WITHCOUNT without SORTBY
        res = self.env.cmd('FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'NOCONTENT')
        self.env.assertEqual(res[0], expected_count)

        # Test WITHCOUNT with SORTBY numeric field
        res = self.env.cmd(
            'FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'NOCONTENT',
            'SORTBY', '1', '@price')
        self.env.assertEqual(res[0], expected_count)

        # Test WITHCOUNT with SORTBY text field
        res = self.env.cmd(
            'FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'NOCONTENT',
            'SORTBY', '1', '@title')
        self.env.assertEqual(res[0], expected_count)


    def testWithoutCount(self):
        # res = self.env.cmd(
        #     'FT.SEARCH', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT')
        # self.env.assertEqual(res[0], 1)

        res = self.env.cmd(
            'FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT')
        self.env.assertEqual(res[0], 1)

        res = self.env.cmd(
            'FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT',
            'SORTBY', '1', '@price')
        self.env.assertEqual(res[0], 1)

        res = self.env.cmd(
            'FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'NOCONTENT',
            'SORTBY', '1', '@title')
        self.env.assertEqual(res[0], 1)


