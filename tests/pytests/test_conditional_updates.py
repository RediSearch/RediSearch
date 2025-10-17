import redis
import unittest
from hotels import hotels
import random
import time
from includes import *

def testConditionalUpdateOnNoneExistingNumericField(env):
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id1', 'numeric', 'SORTABLE'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields', 'id1', '5'))

    # adding field to the schema
    env.assertOk(env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'id2', 'numeric', 'SORTABLE'))

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 > @id2',
                              'fields', 'id1', '3', 'id2', '4'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 < @id2',
                              'fields', 'id1', '3', 'id2', '4'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 == @id2',
                              'fields', 'id1', '3', 'id2', '4'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 != @id2',
                              'fields', 'id1', '3', 'id2', '4'), 'NOADD')

    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                           'REPLACE', 'PARTIAL',
                           'IF', '@id1 == 5',
                           'fields', 'id1', '3', 'id2', '4'))

def testConditionalUpdateOnNoneExistingTextField(env):
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id1', 'text', 'SORTABLE'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields', 'id1', 'some_text'))

    # adding field to the schema
    env.assertOk(env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'id2', 'text', 'SORTABLE'))

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 > @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 < @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 == @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 != @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                           'REPLACE', 'PARTIAL',
                           'IF', '@id1 == "some_text"',
                           'fields', 'id1', 'some_text', 'id2', 'some_text'))

def testConditionalUpdateOnNoneExistingTagField(env):
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id1', 'tag', 'SORTABLE'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields', 'id1', 'some_text'))

    # adding field to the schema
    env.assertOk(env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'id2', 'tag', 'SORTABLE'))

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 > @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 < @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 == @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertEqual(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                              'REPLACE', 'PARTIAL',
                              'IF', '@id1 != @id2',
                              'fields', 'id1', 'some_text', 'id2', 'some_text'), 'NOADD')

    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 1.0,
                           'REPLACE', 'PARTIAL',
                           'IF', '@id1 == "some_text"',
                           'fields', 'id1', 'some_text', 'id2', 'some_text'))
