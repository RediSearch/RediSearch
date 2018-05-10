from rmtest import ModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time


class FuzzyTestCase(ModuleTestCase('../redisearch.so')):

    def testBasicFuzzy(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 'hello world',
                                            'body', 'this is a test'))

            res = r.execute_command('ft.search', 'idx', '%word%')
            self.assertEqual(res, [1L, 'doc1', ['title', 'hello world', 'body', 'this is a test']])

    
    def testFuzzyMultipleResults(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 'hello world',
                                            'body', 'this is a test'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                            'title', 'hello word',
                                            'body', 'this is a test'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'fields',
                                            'title', 'hello ward',
                                            'body', 'this is a test'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc4', 1.0, 'fields',
                                            'title', 'hello wakld',
                                            'body', 'this is a test'))


            res = r.execute_command('ft.search', 'idx', '%word%')
            self.assertEqual(res, [3L, 'doc3', ['title', 'hello ward', 'body', 'this is a test'], 'doc2', ['title', 'hello word', 'body', 'this is a test'], 'doc1', ['title', 'hello world', 'body', 'this is a test']])

    
    def testFuzzySyntaxError(self):
        unallowChars = ('*', '$', '~', '&', '@', '!')
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 'hello world',
                                            'body', 'this is a test'))
            for ch in unallowChars:
                error = None
                try:
                    r.execute_command('ft.search', 'idx', '%%wor%sd%%' % ch)
                except Exception as e:
                    error = str(e)
                self.assertTrue('Syntax error' in error)