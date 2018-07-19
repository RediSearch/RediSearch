from rmtest import BaseModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time


class SpellCheckTestCase(BaseModuleTestCase):

    def testDictAdd(self):
            self.cmd('flushdb')
            res = self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term3')
            self.assertEqual(res, 3)
            res = self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term4')
            self.assertEqual(res, 1)

    def testDictAddWrongArity(self):
        self.cmd('flushdb')
        with self.assertResponseError():
            self.cmd('ft.dictadd', 'dict')

    def testDictDelete(self):
        self.cmd('flushdb')
        self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term3')
        res = self.cmd('ft.dictdel', 'dict', 'term1', 'term2', 'term4')
        self.assertEqual(res, 2)
        res = self.cmd('ft.dictdel', 'dict', 'term3')
        self.assertEqual(res, 1)
        res = self.cmd('keys', '*')
        self.assertEqual(res, [])

    def testDictDeleteWrongArity(self):
        self.cmd('flushdb')
        with self.assertResponseError():
            self.cmd('ft.dictdel', 'dict')

    def testDictDeleteOnNoneExistingKey(self):
        self.cmd('flushdb')
        res = self.cmd('ft.dictdel', 'dict', 'term1')
        self.assertEqual(res, 0)

    def testDictDump(self):
        self.cmd('flushdb')
        self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term3')
        res = self.cmd('ft.dictdump', 'dict')
        self.assertEqual(res, ['term1', 'term2', 'term3'])

    def testDictDumpWrongArity(self):
        self.cmd('flushdb')
        with self.assertResponseError():
            self.cmd('ft.dictdump')

    def testDictDumpOnNoneExistingKey(self):
        self.cmd('flushdb')
        with self.assertResponseError():
            self.cmd('ft.dictdump', 'dict')

    def testBasicSpellCheck(self):
        self.cmd('flushdb')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name')
        self.assertEqual(res, [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1']]]])
        res = self.cmd('ft.spellcheck', 'idx', '@body:name')
        self.assertEqual(res, [['TERM', 'name', [['0.66666666666666663', 'name2']]]])

    def testSpellCheckOnExistingTerm(self):
        self.cmd('flushdb')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name')
        self.assertEqual(res, [])

    def testSpellCheckWithIncludeDict(self):
        self.cmd('flushdb')
        self.cmd('ft.dictadd', 'dict', 'name3', 'name4', 'name5')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
        self.assertEqual(res, [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1'],
                                                 ['0', 'name3'], ['0', 'name4'], ['0', 'name5']]]])

    def testSpellCheckWithDuplications(self):
        self.cmd('flushdb')
        self.cmd('ft.dictadd', 'dict', 'name1', 'name4', 'name5')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
        self.assertEqual(res, [['TERM', 'name',
                               [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1'], ['0', 'name4'], ['0', 'name5']]]])

    def testSpellCheckExcludeDict(self):
        self.cmd('flushdb')
        self.cmd('ft.dictadd', 'dict', 'name')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict')
        self.assertEqual(res, [])

    def testSpellCheckNoneExistingIndex(self):
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict')

    def testSpellCheckWrongArity(self):
        self.cmd('flushdb')
        self.cmd('ft.dictadd', 'dict', 'name')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx')
        with self.assertResponseError():
            self.cmd('ft.spellcheck')

    def testSpellCheckBadFormat(self):
        self.cmd('flushdb')
        self.cmd('ft.dictadd', 'dict', 'name')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE', 0)
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE', -1)
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE', 101)

    def testSpellCheckNoneExistingDicts(self):
        self.cmd('flushdb')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict')
