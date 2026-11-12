# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

def testFlushall(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(con.execute_command('FT.ADD', 'idx', 'doc1', 1, 'FIELDS', 't', 'RediSearch'))
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'doc1', ['t', 'RediSearch']])
    env.assertEqual(collectKeys(env), ['doc1'])

    env.flush()

    env.expect('FT.SEARCH', 'idx', '*').error().contains('SEARCH_INDEX_NOT_FOUND Index not found: idx')
    env.expect('KEYS', '*').equal([])

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
    env.assertOk(con.execute_command('FT.ADD', 'idx', 'doc1', '1', 'FIELDS', 't', 'RediSearch'))
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'doc1', ['t', 'RediSearch']])
    env.assertEqual(collectKeys(env), ['doc1'])
