import redis
import unittest
from hotels import hotels
import random
import time
from base_case import BaseSearchTestCase


class ConditionalUpdateTestCase(BaseSearchTestCase):

    def testConditionalUpdateOnNoneExistingNumericField(self):
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 'id1', 'numeric', 'SORTABLE'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'id1', '5'))

        # adding field to the schema
        self.assertOk(self.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'id2', 'numeric', 'SORTABLE'))

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 > @id2',
                                  'fields', 'id1', '3', 'id2', '4'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 < @id2',
                                  'fields', 'id1', '3', 'id2', '4'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 == @id2',
                                  'fields', 'id1', '3', 'id2', '4'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 != @id2',
                                  'fields', 'id1', '3', 'id2', '4'), 'NOADD')

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                               'REPLACE', 'PARTIAL',
                               'IF', '@id1 == 5',
                               'fields', 'id1', '3', 'id2', '4'))

    def testConditionalUpdateOnNoneExistingTextField(self):
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 'id1', 'text', 'SORTABLE'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'id1', 'some_text'))

        # adding field to the schema
        self.assertOk(self.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'id2', 'text', 'SORTABLE'))

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 > @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 < @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 == @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 != @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                               'REPLACE', 'PARTIAL',
                               'IF', '@id1 == "some_text"',
                               'fields', 'id1', 'some_text', 'id2', 'some_text'))

    def testConditionalUpdateOnNoneExistingTagField(self):
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 'id1', 'tag', 'SORTABLE'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'id1', 'some_text'))

        # adding field to the schema
        self.assertOk(self.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'id2', 'tag', 'SORTABLE'))

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 > @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 < @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 == @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertEqual(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                                  'REPLACE', 'PARTIAL',
                                  'IF', '@id1 != @id2',
                                  'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                               'REPLACE', 'PARTIAL',
                               'IF', '@id1 == "some_text"',
                               'fields', 'id1', 'some_text', 'id2', 'some_text'))
