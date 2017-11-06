# -*- coding: utf-8 -*-

from rmtest import ModuleTestCase
import redis
import unittest
import os

SRCTEXT=os.path.join(os.path.dirname(__file__), '..', 'tests', 'cn_sample.txt')
GENTXT=os.path.join(os.path.dirname(__file__), '..', 'tests', 'genesis.txt')

class CnTestCase(ModuleTestCase('../redisearch.so')):
    def testCn(self):
        text = open(SRCTEXT).read()
        self.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'LANGUAGE', 'CHINESE', 'FIELDS', 'txt', text)
        res = self.cmd('ft.search', 'idx', '之旅', 'SUMMARIZE', 'HIGHLIGHT')
        print res[2][1]
        # print self.cmd('ft.search', 'idx', 'hacker', 'HIGHLIGHT')

        # gentxt = open(GENTXT).read()
        # self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'txt', gentxt)
        # print self.cmd('ft.search', 'idx', 'abraham', 'summarize', 'fraglen', '2', 'separator', 3)