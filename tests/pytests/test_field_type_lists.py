# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).


# The spec keeps the positions of its VECTOR, GEOSHAPE and TEXT fields so that per-document and
# per-query loops need not walk a wide schema. These tests run those loops on a schema wide enough
# that the fields of interest sit among many others, and keep the lists honest across FT.ALTER,
# a failed FT.ALTER and an RDB reload.

from common import *
import struct

FILLER = 150


def _vec(x, y):
    return struct.pack('<2f', x, y)


def _schema():
    schema = []
    for i in range(FILLER):
        schema += [f'n{i}', 'NUMERIC']
    schema += ['title', 'TEXT', 'body', 'TEXT',
               'name', 'TEXT', 'PHONETIC', 'dm:en',
               'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2',
               'geom', 'GEOSHAPE', 'FLAT']
    return schema


def _knn(env, x, y, k=3):
    res = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN {k} @vec $v AS d]', 'PARAMS', 2, 'v', _vec(x, y),
                  'SORTBY', 'd', 'NOCONTENT', 'DIALECT', 2)
    return res[1:]


def _within(env, x0, y0, x1, y1):
    poly = f'POLYGON(({x0} {y0}, {x0} {y1}, {x1} {y1}, {x1} {y0}, {x0} {y0}))'
    res = env.cmd('FT.SEARCH', 'idx', '@geom:[WITHIN $q]', 'PARAMS', 2, 'q', poly,
                  'NOCONTENT', 'DIALECT', 3)
    return sorted(res[1:])


def _check_replaced_documents(env, conn):
    """Replacing a document must move or drop its vector and shape, leaving no stale entry."""
    conn.execute_command('HSET', 'doc:1', 'vec', _vec(0, 0), 'geom', 'POINT(1 1)', 'title', 'one')
    conn.execute_command('HSET', 'doc:2', 'vec', _vec(10, 10), 'geom', 'POINT(50 50)', 'title', 'two')
    waitForIndex(env, 'idx')
    env.assertEqual(_knn(env, 0, 0, 1), ['doc:1'])
    env.assertEqual(_within(env, 0, 0, 5, 5), ['doc:1'])

    # Only a TEXT field changes: both entries of doc:1 survive the replacement.
    conn.execute_command('HSET', 'doc:1', 'title', 'uno')
    waitForIndex(env, 'idx')
    env.assertEqual(_knn(env, 0, 0, 1), ['doc:1'])
    env.assertEqual(_within(env, 0, 0, 5, 5), ['doc:1'])

    # The vector and the shape both change: the old entries must be gone.
    conn.execute_command('HSET', 'doc:1', 'vec', _vec(20, 20), 'geom', 'POINT(70 70)')
    waitForIndex(env, 'idx')
    env.assertEqual(_knn(env, 20, 20, 1), ['doc:1'])
    env.assertEqual(_knn(env, 0, 0, 2), ['doc:2', 'doc:1'])
    env.assertEqual(_within(env, 0, 0, 5, 5), [])
    env.assertEqual(_within(env, 65, 65, 75, 75), ['doc:1'])

    # Dropping the fields drops the entries.
    conn.execute_command('HDEL', 'doc:2', 'vec', 'geom')
    waitForIndex(env, 'idx')
    env.assertEqual(_knn(env, 10, 10, 5), ['doc:1'])
    env.assertEqual(_within(env, 45, 45, 55, 55), [])


@skip(cluster=True)
def testVectorAndGeoReplacementOnWideSchema(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *_schema()).ok()
    _check_replaced_documents(env, getConnectionByEnv(env))


@skip(cluster=True)
def testTypeListsSurviveReloadAndAlter(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *_schema()).ok()
    conn.execute_command('HSET', 'doc:1', 'vec', _vec(1, 1), 'geom', 'POINT(2 2)')
    waitForIndex(env, 'idx')

    env.cmd('DEBUG', 'RELOAD')
    waitForIndex(env, 'idx')
    # The lists come back with the schema: replacing a loaded document moves its entries.
    conn.execute_command('HSET', 'doc:1', 'vec', _vec(30, 30), 'geom', 'POINT(60 60)')
    waitForIndex(env, 'idx')
    env.assertEqual(_knn(env, 30, 30, 5), ['doc:1'])
    env.assertEqual(_within(env, 0, 0, 5, 5), [])
    env.assertEqual(_within(env, 55, 55, 65, 65), ['doc:1'])

    # A failed ALTER leaves the lists as they were; the new fields of the rejected batch are gone.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'vec2', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', '2', 'DISTANCE_METRIC', 'L2', 'geom2', 'GEOSHAPE', 'FLAT',
               'title', 'TEXT').error()
    conn.execute_command('HSET', 'doc:1', 'vec', _vec(31, 31), 'geom', 'POINT(61 61)')
    waitForIndex(env, 'idx')
    env.assertEqual(_knn(env, 31, 31, 1), ['doc:1'])
    env.assertEqual(_within(env, 60, 60, 62, 62), ['doc:1'])

    # Fields added later join the lists.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'vec2', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', '2', 'DISTANCE_METRIC', 'L2', 'geom2', 'GEOSHAPE', 'FLAT').ok()
    waitForIndexFinishScan(env)
    conn.execute_command('HSET', 'doc:1', 'vec2', _vec(5, 5), 'geom2', 'POINT(8 8)')
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'doc:1', 'vec2', _vec(6, 6), 'geom2', 'POINT(9 9)', 'title', 'x')
    waitForIndex(env, 'idx')
    res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 5 @vec2 $v AS d]', 'PARAMS', 2, 'v', _vec(6, 6),
                  'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res[1:], ['doc:1'])
    res = env.cmd('FT.SEARCH', 'idx', '@geom2:[WITHIN $q]', 'PARAMS', 2, 'q',
                  'POLYGON((8 8, 8 10, 10 10, 10 8, 8 8))', 'NOCONTENT', 'DIALECT', 3)
    env.assertEqual(res[1:], ['doc:1'])


@skip(cluster=True)
def testTextFieldLookupsOnWideSchema(env):
    """Field masks, phonetics and slop/inorder resolve the TEXT fields among many others."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *_schema()).ok()
    conn.execute_command('HSET', 'doc:1', 'title', 'hello brave world', 'body', 'other',
                         'name', 'john')
    conn.execute_command('HSET', 'doc:2', 'title', 'nothing', 'body', 'hello brave world',
                         'name', 'smith')
    waitForIndex(env, 'idx')

    def ids(*args):
        res = env.cmd('FT.SEARCH', 'idx', *args, 'NOCONTENT')
        return sorted(res[1:])

    env.assertEqual(ids('@title:(hello)'), ['doc:1'])
    env.assertEqual(ids('@title|body:(hello)'), ['doc:1', 'doc:2'])
    env.assertEqual(ids('@body:(hello brave)', 'SLOP', 0, 'INORDER'), ['doc:2'])
    env.assertEqual(ids('hello', 'INFIELDS', 1, 'body'), ['doc:2'])
    # Phonetic matching applies to the PHONETIC field only.
    env.assertEqual(ids('@name:(jon)'), ['doc:1'])
    env.assertEqual(ids('@title:(jon)'), [])
    res = env.cmd('FT.SEARCH', 'idx', '@title:(hello)', 'HIGHLIGHT', 'FIELDS', 1, 'title')
    env.assertEqual(res[0], 1)
    env.assertContains('<b>hello</b>', res[2][res[2].index('title') + 1])
