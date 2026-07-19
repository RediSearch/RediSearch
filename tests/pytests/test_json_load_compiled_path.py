# -*- coding: utf-8 -*-
# Regression coverage for MOD-16899: the aggregate LOAD path compiles each JSONPath once per
# query and evaluates it via the RedisJSON `getWithPath` API (when available) instead of
# recompiling the path string for every document. The optimization must be transparent, so these
# tests assert that loading nested / multi-value / missing JSON paths through FT.AGGREGATE LOAD
# still returns exactly the values the string-based path returned.

from common import *
from includes import *


def _build_index(env):
    conn = getConnectionByEnv(env)
    env.expect(
        'FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', '1', 'entity:', 'SCHEMA',
        '$.entityName', 'AS', 'entityName', 'TEXT', 'SORTABLE',
        '$.event.id', 'AS', 'eventId', 'NUMERIC', 'SORTABLE',
        '$.event.type', 'AS', 'type', 'TAG',
        '$.event.dueDate', 'AS', 'dueDate', 'TEXT',
    ).ok()
    # Two entities, three events; note entity:3 has no event.type (missing nested field).
    conn.execute_command('JSON.SET', 'entity:1', '$',
                          '{"entityName":"Alpha","event":{"id":10,"type":"A","dueDate":"2025-01-01"}}')
    conn.execute_command('JSON.SET', 'entity:2', '$',
                          '{"entityName":"Alpha","event":{"id":20,"type":"B","dueDate":"2025-02-02"}}')
    conn.execute_command('JSON.SET', 'entity:3', '$',
                          '{"entityName":"Beta","event":{"id":30,"dueDate":"2025-03-03"}}')
    waitForIndex(env, 'idx')


@skip(msan=True, no_json=True)
def testAggregateLoadNestedJsonPaths(env):
    # Per-document LOAD of several nested JSONPaths, deterministic row order via SORTBY.
    _build_index(env)
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'LOAD', '4', '@entityName', '@eventId', '@type', '@dueDate',
                  'SORTBY', '2', '@eventId', 'ASC')
    # res = [count, row1, row2, ...] ; rows are flat [field, value, ...]
    rows = [dict(zip(r[::2], r[1::2])) for r in res[1:]]
    env.assertEqual(len(rows), 3)
    env.assertEqual(rows[0]['entityName'], 'Alpha')
    env.assertEqual(rows[0]['eventId'], '10')
    env.assertEqual(rows[0]['type'], 'A')
    env.assertEqual(rows[0]['dueDate'], '2025-01-01')
    env.assertEqual(rows[1]['eventId'], '20')
    env.assertEqual(rows[1]['type'], 'B')
    # entity:3's event.type is missing -> the field must be absent, not garbage.
    env.assertEqual(rows[2]['eventId'], '30')
    env.assertTrue('type' not in rows[2])
    env.assertEqual(rows[2]['dueDate'], '2025-03-03')


@skip(msan=True, no_json=True)
def testAggregateGroupByJsonLoad(env):
    # The GROUPBY accumulate path (the profiled hot path) must group correctly on a loaded JSON
    # field and load the reduced fields with correct values.
    _build_index(env)
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                  'LOAD', '2', '@entityName', '@eventId',
                  'GROUPBY', '1', '@entityName',
                  'REDUCE', 'COUNT', '0', 'AS', 'cnt',
                  'SORTBY', '2', '@entityName', 'ASC')
    groups = {r[1]: dict(zip(r[::2], r[1::2])) for r in res[1:]}
    env.assertEqual(set(groups.keys()), {'Alpha', 'Beta'})
    env.assertEqual(groups['Alpha']['cnt'], '2')
    env.assertEqual(groups['Beta']['cnt'], '1')
