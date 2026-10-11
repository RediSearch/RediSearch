# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
from contextlib import contextmanager
import json
from test_alter_vector_relabel import get_internal_id

# A move is not an indexing op: it shows as an unchanged geoshape ops count while DUMP_GEOMIDX
# lists the same shapes under the new doc-ids.

# Valid in FLAT and SPHERICAL.
SHAPES = [
    'POLYGON((1 1, 1 20, 20 20, 20 1, 1 1))',
    'POLYGON((30 30, 30 60, 60 60, 60 30, 30 30), (40 40, 50 40, 50 50, 40 50, 40 40))',
    'POINT(10 70)',
    'POLYGON((70 70, 70 75, 75 75, 75 70, 70 70))',
]
N = len(SHAPES)
WITHIN_ALL = 'POLYGON((0 0, 0 80, 80 80, 80 0, 0 0))'
EXTRA = ('extra', 'x')


def _start(*schema, on='HASH'):
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'ON', on, 'SCHEMA', *schema).ok()
    return env, getConnectionByEnv(env)

def _geoshape_ops(env):
    infos = run_command_on_all_shards(env, 'INFO', 'MODULES')
    return sum(int(i['search_total_indexing_ops_geoshape_fields']) for i in infos)

def _dump(env, field='geom'):
    """{doc-id: shape} from DUMP_GEOMIDX; asserts no stale or duplicate entry."""
    res = env.cmd(debug_cmd(), 'DUMP_GEOMIDX', 'idx', field)
    res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    shapes = {}
    for doc in res['docs']:
        doc = {doc[i]: doc[i + 1] for i in range(0, len(doc), 2)}
        env.assertContains('geoshape', doc, message=f'{field}: R-tree entry without a shape')
        env.assertNotContains(doc['id'], shapes, message=f'{field}: doc-id listed twice')
        shapes[doc['id']] = doc.get('geoshape')
    env.assertEqual(res['num_docs'], len(shapes))
    return shapes

def _ids(env, keys):
    return {k: get_internal_id(env, k) for k in keys}

def _moved(dump, ids_before, ids_after):
    """`dump` re-keyed from old to new doc-ids."""
    old_to_new = {ids_before[k]: ids_after[k] for k in ids_before}
    return {old_to_new.get(i, i): shape for i, shape in dump.items()}

def _search(env, query_type, shape, field='geom'):
    res = env.cmd('FT.SEARCH', 'idx', f'@{field}:[{query_type} $q]', 'PARAMS', 2, 'q', shape,
                  'NOCONTENT', 'LIMIT', 0, 1000, 'DIALECT', 3)
    ids = [r['id'] for r in res['results']]
    env.assertEqual(len(ids), len(set(ids)), message=f'duplicates in {ids}')
    env.assertEqual(res['total_results'], len(ids))
    return sorted(ids)

def _keys(ids):
    return sorted(f'doc:{i}' for i in ids)

def _write_docs(conn, n=N, added=lambda i: EXTRA):
    for i in range(n):
        conn.execute_command('HSET', f'doc:{i}', 'title', 't', 'geom', SHAPES[i % N], *added(i))

def _alter_and_wait(env, *schema_add):
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *schema_add).ok()
    waitForIndexFinishScan(env)

@contextmanager
def _alter_paused_before_scan(env, *schema_add):
    """FT.ALTER ADD with the scan paused while the body runs."""
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
    try:
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *schema_add).ok()
        waitForIndexStatus(env, 'NEW')
        yield
    finally:
        env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'false').ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env)


def _check_moves_preexisting(coords):
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', coords)
    _write_docs(conn)
    keys = _keys(range(N))
    ids_before = _ids(env, keys)
    dump_before = _dump(env)
    within_before = _search(env, 'WITHIN', WITHIN_ALL)
    contains_before = _search(env, 'CONTAINS', 'POINT(5 5)')
    env.assertEqual(within_before, keys)
    env.assertEqual(contains_before, ['doc:0'])
    ops_before = _geoshape_ops(env)

    _alter_and_wait(env, 'extra', 'TAG')

    ids_after = _ids(env, keys)
    for k in keys:
        env.assertGreater(ids_after[k], ids_before[k], message=f'{k} internal id')
    env.assertEqual(_geoshape_ops(env) - ops_before, 0, message='moved, not re-indexed')
    env.assertEqual(_dump(env), _moved(dump_before, ids_before, ids_after))
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), within_before)
    env.assertEqual(_search(env, 'CONTAINS', 'POINT(5 5)'), contains_before)

@skip(cluster=True)
def test_alter_moves_preexisting_geoshape_flat():
    _check_moves_preexisting('FLAT')

@skip(cluster=True)
def test_alter_moves_preexisting_geoshape_spherical():
    _check_moves_preexisting('SPHERICAL')


@skip(cluster=True)
def test_alter_added_geoshape_field_inserted():
    """doc:3 has no geom2, so the selective scan skips it."""
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT')
    _write_docs(conn, added=lambda i: ('geom2', SHAPES[N - 1 - i]) if i < 3 else ())
    keys = _keys(range(N))
    ids_before = _ids(env, keys)
    dump_before = _dump(env)
    ops_before = _geoshape_ops(env)

    _alter_and_wait(env, 'geom2', 'GEOSHAPE', 'FLAT')

    ids_after = _ids(env, keys)
    env.assertEqual(ids_after['doc:3'], ids_before['doc:3'])
    env.assertEqual(_geoshape_ops(env) - ops_before, 3, message='only geom2 is indexed')
    env.assertEqual(_dump(env), _moved(dump_before, ids_before, ids_after))
    env.assertEqual(sorted(_dump(env, 'geom2')), sorted(ids_after[k] for k in _keys(range(3))))
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys)
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL, field='geom2'), _keys(range(3)))


@skip(cluster=True)
def test_alter_two_preexisting_geoshape_fields():
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT', 'geom2', 'GEOSHAPE', 'FLAT')
    _write_docs(conn, n=3, added=lambda i: (*EXTRA, 'geom2', SHAPES[3]) if i < 2 else EXTRA)
    keys = _keys(range(3))
    ids_before = _ids(env, keys)
    dumps_before = [_dump(env, f) for f in ('geom', 'geom2')]
    env.assertEqual(len(dumps_before[1]), 2)
    ops_before = _geoshape_ops(env)

    _alter_and_wait(env, 'extra', 'TAG')

    ids_after = _ids(env, keys)
    env.assertEqual(_geoshape_ops(env) - ops_before, 0)
    for f, before in zip(('geom', 'geom2'), dumps_before):
        env.assertEqual(_dump(env, f), _moved(before, ids_before, ids_after), message=f)
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL, field='geom2'), _keys(range(2)))


@skip(cluster=True, no_json=True)
def test_alter_json_geoshape():
    """doc:3's geom is JSON null: no entry, but one indexing op, as on any reindex."""
    env, conn = _start('$.title', 'AS', 'title', 'TEXT', '$.geom', 'AS', 'geom', 'GEOSHAPE',
                       'FLAT', on='JSON')
    for i in range(N):
        geom = SHAPES[i] if i < 3 else None
        conn.execute_command('JSON.SET', f'doc:{i}', '$',
                             json.dumps({'title': 't', 'geom': geom, 'tag': 'x'}))
    keys = _keys(range(N))
    ids_before = _ids(env, keys)
    dump_before = _dump(env)
    env.assertEqual(len(dump_before), 3)
    env.assertNotContains(ids_before['doc:3'], dump_before)
    ops_before = _geoshape_ops(env)

    _alter_and_wait(env, '$.tag', 'AS', 'tag', 'TAG')

    ids_after = _ids(env, keys)
    env.assertGreater(ids_after['doc:3'], ids_before['doc:3'])
    env.assertEqual(_geoshape_ops(env) - ops_before, 1)
    env.assertEqual(_dump(env), _moved(dump_before, ids_before, ids_after))
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), _keys(range(3)))


@skip(cluster=True)
def test_alter_geoshape_writes_during_paused_scan():
    """Every shape the scan meets is the one the writes stored, so all move."""
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT')
    _write_docs(conn)
    new_shape = 'POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))'

    with _alter_paused_before_scan(env, 'extra', 'TAG'):
        conn.execute_command('HSET', 'doc:0', 'geom', new_shape)
        conn.execute_command('HSET', 'doc:1', 'title', 'changed')
        conn.execute_command('DEL', 'doc:2')
        conn.execute_command('HDEL', 'doc:3', 'geom')
        conn.execute_command('HSET', 'doc:9', 'title', 'n', 'geom', SHAPES[2], *EXTRA)
        # After the writes, so only the scan is counted.
        keys = _keys([0, 1, 9])
        ids_before = _ids(env, keys)
        dump_before = _dump(env)
        ops_before = _geoshape_ops(env)

    ids_after = _ids(env, keys)
    for k in keys:
        env.assertGreater(ids_after[k], ids_before[k], message=f'{k} internal id')
    env.assertEqual(_geoshape_ops(env) - ops_before, 0)
    dump = _dump(env)
    env.assertEqual(dump, _moved(dump_before, ids_before, ids_after))
    env.assertEqual(sorted(dump), sorted(ids_after.values()))
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys)
    env.assertEqual(_search(env, 'CONTAINS', 'POINT(2.5 2.5)'), ['doc:0'])


def _check_mid_backfill_queries(coords):
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', coords)
    n = 200
    pipe = conn.pipeline(transaction=False)
    for i in range(n):
        x, y, x1, y1 = 1 + (i % 20) * 3, 1 + (i // 20) * 3, 2 + (i % 20) * 3, 2 + (i // 20) * 3
        square = f'POLYGON(({x} {y}, {x} {y1}, {x1} {y1}, {x1} {y}, {x} {y}))'
        pipe.execute_command('HSET', f'doc:{i}', 'title', 't', 'geom', square, *EXTRA)
    pipe.execute()
    keys = _keys(range(n))
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys)
    old_ids = set(_dump(env))
    ops_before = _geoshape_ops(env)
    # SPHERICAL may refuse a move and insert instead, so only FLAT pins the ops count.
    pin_ops = coords == 'FLAT'

    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', n // 2).ok()
    try:
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
        waitForIndexStatus(env, 'PAUSED')
        if pin_ops:
            env.assertEqual(_geoshape_ops(env) - ops_before, 0, message='ops when the scan paused')
        env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys, message='mid-scan')
        dump = _dump(env)
        env.assertEqual(len(dump), n)
        env.assertEqual(len(set(dump) - old_ids), n // 2, message='half moved at the pause')
    finally:
        env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 0).ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env)

    if pin_ops:
        env.assertEqual(_geoshape_ops(env) - ops_before, 0)
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys, message='after the scan')
    dump = _dump(env)
    env.assertEqual(len(dump), n)
    env.assertEqual(set(dump) & old_ids, set(), message='every shape under its new doc-id')

@skip(cluster=True)
def test_alter_geoshape_mid_backfill_queries_flat():
    _check_mid_backfill_queries('FLAT')

@skip(cluster=True)
def test_alter_geoshape_mid_backfill_queries_spherical():
    _check_mid_backfill_queries('SPHERICAL')


@skip(cluster=True)
def test_alter_full_scan_reinserts_geoshape():
    """SORTABLE or INDEXMISSING forces a full scan, which re-adds every shape."""
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT')
    _write_docs(conn)
    keys = _keys(range(N))
    for schema_add in (('extra', 'TAG', 'SORTABLE'), ('extra2', 'TAG', 'INDEXMISSING')):
        ids_before = _ids(env, keys)
        dump_before = _dump(env)
        ops_before = _geoshape_ops(env)

        _alter_and_wait(env, *schema_add)

        ids_after = _ids(env, keys)
        env.assertEqual(_geoshape_ops(env) - ops_before, N, message=schema_add)
        env.assertEqual(_dump(env), _moved(dump_before, ids_before, ids_after), message=schema_add)
        env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys, message=schema_add)


@skip(cluster=True)
def test_alter_geoshape_relabel_disabled_by_config():
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT')
    _write_docs(conn)
    keys = _keys(range(N))
    ids_before = _ids(env, keys)
    dump_before = _dump(env)
    ops_before = _geoshape_ops(env)
    try:
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-optimize-partial-update', 'no')
        _alter_and_wait(env, 'extra', 'TAG')
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-optimize-partial-update', 'yes')

    ids_after = _ids(env, keys)
    env.assertEqual(_geoshape_ops(env) - ops_before, N)
    env.assertEqual(_dump(env), _moved(dump_before, ids_before, ids_after))
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys)


@skip(cluster=True)
def test_alter_skipinitialscan_geoshape():
    env, conn = _start('title', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT')
    _write_docs(conn)
    keys = _keys(range(N))
    ids_before = _ids(env, keys)
    dump_before = _dump(env)
    ops_before = _geoshape_ops(env)

    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
    waitForIndexFinishScan(env)

    env.assertEqual(_ids(env, keys), ids_before)
    env.assertEqual(_geoshape_ops(env) - ops_before, 0)
    env.assertEqual(_dump(env), dump_before)
    env.assertEqual(_search(env, 'WITHIN', WITHIN_ALL), keys)
