# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
from contextlib import contextmanager
import json
# Same counters, same gate-off handling, as the non-ALTER relabel suite.
from test_vector_relabel import _vector_ops

# FT.ALTER's background backfill replaces each document that has an added field under a new
# doc-id. A pre-existing vector field is compared with the document's blob, as MOD-17688 does for
# any update without a change set: a confirmed vector is relabelled onto the new doc-id, anything
# else is deleted and re-inserted. On lossy storage (HNSW SQ8, compressed SVS) the comparison
# cannot confirm a quantized vector, so those vectors are re-inserted. A vector field the ALTER
# added is inserted without a comparison (MOD-18169).

DIM = 4
SQ8_DIM = 64
# WORKERS 1: makes HNSW tiered, the only configuration where dropping a vector leaves a
# tombstone rather than removing in place -- that tombstone is what _marked_deleted observes,
# and it is how these tests tell a relabel (no tombstone) apart from a delete + re-add (one
# tombstone) for a value that ends up identical either way. WORKERS DRAIN (see _drain) then
# moves vectors into the backend before each snapshot, matching test_vector_relabel.py's
# rationale for the same setting.
# FORK_GC_RUN_INTERVAL 50000: keeps the fork GC from clearing tombstones between the snapshot
# taken before an ALTER and the one taken after it.
MODULE_ARGS = 'WORKERS 1 FORK_GC_RUN_INTERVAL 50000'
# protocol=3 (RESP3): FT.SEARCH and FT.INFO reply with maps instead of flat arrays, which is
# what _knn, _marked_deleted and _vector_ops all read.

def _blob(fill, dim=DIM):
    return create_np_array_typed([fill] * dim, 'FLOAT32').tobytes()

def _doc_vec(i):                      # distinct per doc, so KNN 1 identifies one document
    return _blob(float(i + 1))

def _sq8_vec(i):                      # shape of test_vecsim_hnsw_sq8.sq8_vector
    return create_np_array_typed([float(i + 1)] + [1.0] * (SQ8_DIM - 1), 'FLOAT32').tobytes()

def _vec2(i):                         # doc i's value for the added vector field `vec2`
    return _blob(100.0 + i)

def _hnsw(dim, *extra):
    p = ['TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2', *extra]
    return ['VECTOR', 'HNSW', len(p), *p]

def _flat(dim):
    p = ['TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2']
    return ['VECTOR', 'FLAT', len(p), *p]

def _sq8():
    return _hnsw(SQ8_DIM, 'COMPRESSION', 'SQ8', 'TRAINING_THRESHOLD', 4)

def _drain(env):
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()

def get_internal_id(env, key, idx='idx'):
    # internal_id is the primary observable for this feature: a document whose newly added
    # fields are all absent keeps its id (skipped), while a full reindex always goes through
    # the REPLACE path, which deletes the old doc-table entry and mints a new, larger id.
    docinfo = to_dict(env.cmd(debug_cmd(), 'DOCINFO', idx, key, 'REVEAL'))
    return docinfo['internal_id']

def _marked_deleted(env, attribute='vec', index='idx'):
    """Tombstones on the named vector field, once any pending ingest jobs have settled.

    Selected by attribute rather than by position: `field statistics` carries an entry per
    schema field and only the vector one has this key, so an index-based lookup silently reads
    the TEXT field instead. Attribute and not identifier because a JSON schema's identifier is
    the path (`$.vector`).
    """
    verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')
    stats = index_info(env, index)['field statistics']
    vector_stats = [f for f in stats if f['attribute'] == attribute]
    env.assertEqual(len(vector_stats), 1)
    return vector_stats[0]['marked_deleted']

def _ops_delta(before, after):
    return (after[0] - before[0], after[1] - before[1])     # (indexing ops, relabel ops)

def _knn(env, blob, k=1, field='vec', index='idx'):
    res = env.cmd('FT.SEARCH', index, f'*=>[KNN {k} @{field} $b AS score]', 'PARAMS', '2', 'b',
                  blob, 'RETURN', '1', 'score', 'LIMIT', '0', k, 'DIALECT', '2')
    return [(r['id'], r['extra_attributes']['score']) for r in res['results']]

def _assert_exact_hit(env, blob, key, field='vec', index='idx'):
    env.assertEqual(_knn(env, blob, 1, field, index), [(key, '0')], message=f'{field} of {key}')

def _knn_ids(env, blob, k, field='vec', index='idx'):
    return sorted(doc_id for doc_id, _ in _knn(env, blob, k, field, index))

def _alter_and_wait(env, *schema_add, index='idx'):
    env.expect('FT.ALTER', index, 'SCHEMA', 'ADD', *schema_add).ok()
    waitForIndexFinishScan(env, index)
    _drain(env)

def _assert_sq8_trained(env, n, index='idx'):
    info = get_vecsim_debug_dict(env, index, 'vec')
    env.assertEqual(to_dict(info['FRONTEND_INDEX'])['INDEX_SIZE'], 0, message=info)
    env.assertEqual(to_dict(info['BACKEND_INDEX'])['INDEX_SIZE'], n, message=info)

def _keys(ids):
    return sorted(f'doc:{i}' for i in ids)

def _assert_hits(env, ids, vec=_doc_vec, field='vec'):
    for i in ids:
        _assert_exact_hit(env, vec(i), f'doc:{i}', field)

# title TEXT plus the pre-existing HNSW vector `vec`: the schema most tests ALTER.
TITLE_AND_VEC = ('title', 'TEXT', 'vec', *_hnsw(DIM))

def _start(*schema, on='HASH'):
    """A fresh env and connection, with `idx` created over `schema`."""
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    env.expect('FT.CREATE', 'idx', 'ON', on, 'SCHEMA', *schema).ok()
    return env, getConnectionByEnv(env)

EXTRA = ('extra', 'x')               # a document's value for the added TAG field

def _write_docs(env, conn, n, added=lambda i: EXTRA):
    """HSETs doc:0..n-1, each with a title and its own vector _doc_vec(i), plus `added(i)`: the
    document's values for the field(s) the ALTER will add (by default `extra`). Drains, so every
    vector is in the backend before the caller snapshots anything."""
    for i in range(n):
        conn.execute_command('HSET', f'doc:{i}', 'title', 't', 'vec', _doc_vec(i), *added(i))
    _drain(env)

def _measure_alter(env, *schema_add, vector_fields=('vec',)):
    """Runs FT.ALTER ADD `schema_add` and waits for its backfill. Returns the (indexing ops,
    relabel ops) it performed and the tombstones it left on each of `vector_fields`."""
    ops_before = _vector_ops(env)
    tombstones_before = [_marked_deleted(env, f) for f in vector_fields]
    _alter_and_wait(env, *schema_add)
    tombstones = [_marked_deleted(env, f) - b for f, b in zip(vector_fields, tombstones_before)]
    return _ops_delta(ops_before, _vector_ops(env)), tombstones

@contextmanager
def _alter_paused_before_scan(env, *schema_add):
    """Runs FT.ALTER ADD `schema_add` with its backfill paused before the scan, so the body's
    writes land first; then resumes it and waits for it to finish."""
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
    try:
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *schema_add).ok()
        waitForIndexStatus(env, 'NEW', 'idx')
        yield
    finally:
        env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'false').ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx')
    _drain(env)


@skip(cluster=True)
def test_alter_relabels_preexisting_vector():
    """The headline case: FT.ALTER's backfill moves a pre-existing HNSW FP32 vector onto each
    document's new doc-id instead of deleting and re-inserting it, because the comparison sees
    each blob is unchanged. This also passes on unmodified master (MOD-17688); it pins that the
    ALTER entry point keeps that compare-then-relabel path."""
    env, conn = _start(*TITLE_AND_VEC)
    _write_docs(env, conn, 5)
    ids_before = [get_internal_id(env, f'doc:{i}') for i in range(5)]

    ops, tombstones = _measure_alter(env, 'extra', 'TAG')

    for i in range(5):
        env.assertGreater(get_internal_id(env, f'doc:{i}'), ids_before[i],
                          message=f'doc:{i} internal id')
    env.assertEqual(ops, (0, 5))
    env.assertEqual(tombstones, [0])
    res = env.cmd('FT.SEARCH', 'idx', '@extra:{x}', 'NOCONTENT')
    env.assertEqual(sorted(r['id'] for r in res['results']), _keys(range(5)))
    _assert_hits(env, range(5))


@skip(cluster=True)
def test_alter_reinserts_sq8_vectors():
    """Pins master's behaviour on lossy storage: SQ8's comparison cannot confirm a quantized
    vector, so a selective ALTER backfill deletes and re-inserts each pre-existing vector,
    (6, 0) with one tombstone each, rather than moving it unverified."""
    env, conn = _start('title', 'TEXT', 'vec', *_sq8())
    for i in range(6):
        conn.execute_command('HSET', f'doc:{i}', 'title', 't', 'vec', _sq8_vec(i), 'extra', 'x')
    _drain(env)
    _assert_sq8_trained(env, 6)

    ops, tombstones = _measure_alter(env, 'extra', 'TAG')

    env.assertEqual(ops, (6, 0))
    env.assertEqual(tombstones, [6])
    env.assertEqual(_knn_ids(env, _sq8_vec(0), 6), _keys(range(6)))


@skip(cluster=True)
def test_alter_skipped_docs_do_no_vector_work():
    """A document without the added field is skipped entirely by the selective backfill: no
    vector work at all, not even a relabel."""
    env, conn = _start(*TITLE_AND_VEC)
    _write_docs(env, conn, 6, added=lambda i: EXTRA if i % 2 == 0 else ())
    ids_before = {i: get_internal_id(env, f'doc:{i}') for i in (1, 3, 5)}

    ops, _ = _measure_alter(env, 'extra', 'TAG')

    env.assertEqual(ops, (0, 3))
    for i in (1, 3, 5):
        env.assertEqual(get_internal_id(env, f'doc:{i}'), ids_before[i])
    _assert_hits(env, range(6))


@skip(cluster=True)
def test_alter_added_vector_field_inserted_normally():
    """A newly added vector field has nothing pre-existing to move, so it is inserted fresh on
    the documents that have it, while a pre-existing vector field on those same documents is
    still relabeled rather than re-added. Master gives (3, 3) too: there the added field is first
    compared with an entry that does not exist; the case that differs is
    test_alter_added_vector_written_during_scan_is_reinserted."""
    env, conn = _start(*TITLE_AND_VEC)
    _write_docs(env, conn, 6, added=lambda i: ('vec2', _vec2(i)) if i < 3 else ())

    ops, tombstones = _measure_alter(env, 'vec2', *_hnsw(DIM))

    env.assertEqual(ops, (3, 3))
    _assert_hits(env, range(6))
    _assert_hits(env, range(3), vec=_vec2, field='vec2')
    env.assertEqual(_knn_ids(env, _vec2(0), 10, field='vec2'), _keys(range(3)))
    env.assertEqual(tombstones, [0])


@skip(cluster=True)
def test_alter_multiple_preexisting_vector_fields():
    """Every pre-existing vector field on a document is relabeled by the selective backfill,
    not just one of them."""
    env, conn = _start('title', 'TEXT', 'v_flat', *_flat(DIM), 'v_hnsw', *_hnsw(DIM))
    for i in range(4):
        conn.execute_command('HSET', f'doc:{i}', 'title', 't', 'v_flat', _doc_vec(i), 'v_hnsw',
                             _doc_vec(i), 'extra', 'x')
    _drain(env)

    ops, tombstones = _measure_alter(env, 'extra', 'TAG', vector_fields=('v_hnsw',))

    env.assertEqual(ops, (0, 8))
    _assert_hits(env, range(4), field='v_flat')
    _assert_hits(env, range(4), field='v_hnsw')
    env.assertEqual(tombstones, [0])


def _json_vecs(i):
    """doc:i's single `vec` fill and its two multi-value `vecs` fills."""
    return float(i + 1), float(10 * (i + 1)), float(10 * (i + 1) + 5)

@skip(cluster=True, no_json=True)
def test_alter_json_single_and_multi_value():
    """A JSON document's single-value and multi-value vector fields are both relabeled by a
    selective ALTER backfill."""
    env, conn = _start('$.title', 'AS', 'title', 'TEXT', '$.vec', 'AS', 'vec', *_hnsw(DIM),
                       '$.vecs[*]', 'AS', 'vecs', *_hnsw(DIM), on='JSON')
    for i in range(3):
        v, a, b = _json_vecs(i)
        conn.execute_command('JSON.SET', f'doc:{i}', '$', json.dumps({
            'title': 't',
            'vec': [v] * DIM,
            'vecs': [[a] * DIM, [b] * DIM],
            'tag': 'x',
        }))
    _drain(env)

    ops, tombstones = _measure_alter(env, '$.tag', 'AS', 'tag', 'TAG',
                                     vector_fields=('vec', 'vecs'))

    env.assertEqual(ops, (0, 6))
    env.assertEqual(tombstones, [0, 0])
    for i in range(3):
        v, a, b = _json_vecs(i)
        _assert_exact_hit(env, _blob(v), f'doc:{i}', field='vec')
        _assert_exact_hit(env, _blob(a), f'doc:{i}', field='vecs')
        _assert_exact_hit(env, _blob(b), f'doc:{i}', field='vecs')
    env.assertEqual(get_vecsim_debug_dict(env, 'idx', 'vecs')['INDEX_LABEL_COUNT'], 3)


@skip(cluster=True)
def test_alter_relabel_disabled_by_config():
    """The kill switch: with the relabel config off, FT.ALTER's selective backfill must fall
    back to delete + re-add rather than relabel the pre-existing vector; with it on, the same
    backfill relabels every vector instead (see test_alter_relabels_preexisting_vector)."""
    env, conn = _start(*TITLE_AND_VEC)
    _write_docs(env, conn, 4)

    # Not _measure_alter: both snapshots must be taken with the config on, since INFO omits the
    # relabel counter while it is off and a delta read then would always show zero relabels.
    before = _vector_ops(env)
    deleted_before = _marked_deleted(env)
    try:
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-optimize-partial-update', 'no')
        _alter_and_wait(env, 'extra', 'TAG')
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-optimize-partial-update', 'yes')

    env.assertEqual(_ops_delta(before, _vector_ops(env)), (4, 0))
    env.assertEqual(_marked_deleted(env), deleted_before + 4)
    _assert_hits(env, range(4))


@skip(cluster=True)
def test_alter_missing_old_entry_inserts_current_value():
    """A pre-existing vector field with no entry under the document's old doc-id: the
    comparison finds nothing to move, so the backfill inserts the document's current value. A
    relabel is attempted only after the comparison found the entry, so VecSim's OldLabelMissing
    refusal is not reached. RAM VecSim indexes are created lazily, so a field added with
    SKIPINITIALSCAN has no VecSim index until the first write reaches it. The doc:9 write below
    creates that index; docs 0 and 1 then reach the comparison for their never-indexed vec."""
    env, conn = _start('title', 'TEXT')
    _write_docs(env, conn, 4, added=lambda i: EXTRA if i < 2 else ())

    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'vec', *_hnsw(DIM)).ok()
    conn.execute_command('HSET', 'doc:9', 'title', 't', 'vec', _blob(9.0))
    _drain(env)
    env.assertEqual(_knn_ids(env, _doc_vec(0), 10), ['doc:9'])

    ops, _ = _measure_alter(env, 'extra', 'TAG')

    env.assertEqual(ops, (2, 0))
    env.assertEqual(_knn_ids(env, _doc_vec(0), 10), _keys([0, 1, 9]))
    _assert_hits(env, [0, 1])
    _assert_exact_hit(env, _blob(9.0), 'doc:9')


@skip(cluster=True)
def test_alter_writes_during_paused_scan():
    """Writes that land while the ALTER's selective scan is paused must all be reflected once
    the scan finishes: an updated vector, a text-only update, a deletion, and a brand new
    document that already has the added field. The scan compares every entry those writes left
    behind, confirms it, and relabels all five surviving documents, (0, 5)."""
    env, conn = _start(*TITLE_AND_VEC)
    _write_docs(env, conn, 5)

    with _alter_paused_before_scan(env, 'extra', 'TAG'):
        conn.execute_command('HSET', 'doc:0', 'vec', _blob(50.0))
        conn.execute_command('HSET', 'doc:1', 'title', 'changed')
        conn.execute_command('DEL', 'doc:2')
        conn.execute_command('HSET', 'doc:9', 'title', 'n', 'vec', _blob(90.0), 'extra', 'x')
        # After the writes, so only the scan's own vector work is counted.
        before = _vector_ops(env)

    env.assertEqual(_ops_delta(before, _vector_ops(env)), (0, 5))
    _assert_exact_hit(env, _blob(50.0), 'doc:0')
    _assert_hits(env, [1, 3, 4])
    _assert_exact_hit(env, _blob(90.0), 'doc:9')
    env.assertEqual(_knn_ids(env, _doc_vec(0), 10), _keys([0, 1, 3, 4, 9]))
    env.assertEqual(get_vecsim_debug_dict(env, 'idx', 'vec')['INDEX_LABEL_COUNT'], 5)


@skip(cluster=True)
def test_alter_added_vector_written_during_scan_is_reinserted():
    """Proves that the selective scan hands the ALTER-added field range to the reindex
    (IndexSpec_UpdateDocForAlter), which no other flow test observes: every other outcome in this
    file is the same with or without that hand-off.

    A write that lands while the scan is paused gives doc:0 an entry for the vector field the ALTER
    added, under its current doc-id. The scan then inserts that field without a comparison (delete
    + insert), while the pre-existing field is still compared and relabelled: (3, 3) over the three
    documents. Fails on master, and with a scanner that reindexes through IndexSpec_UpdateDoc: there
    the added field is compared too, the comparison confirms doc:0's entry and relabels it, giving
    (2, 4). The end state is the same either way. FLAT for the added field: no tiered buffer, so
    the comparison is exact."""
    env, conn = _start(*TITLE_AND_VEC)
    _write_docs(env, conn, 3, added=lambda i: ('vec2', _vec2(i)))

    with _alter_paused_before_scan(env, 'vec2', *_flat(DIM)):
        conn.execute_command('HSET', 'doc:0', 'vec2', _blob(200.0))
        # After the write, so only the scan's own vector work is counted.
        before = _vector_ops(env)

    env.assertEqual(_ops_delta(before, _vector_ops(env)), (3, 3))
    _assert_hits(env, range(3))
    _assert_exact_hit(env, _blob(200.0), 'doc:0', field='vec2')
    _assert_hits(env, (1, 2), vec=_vec2, field='vec2')
    env.assertEqual(_knn_ids(env, _vec2(0), 10, field='vec2'), _keys(range(3)))


@skip(cluster=True)
def test_alter_added_vector_after_non_vector_field_is_inserted():
    """Proves that every field in the ALTER-added range is treated as added, not only the first:
    `FT.ALTER ADD extra TAG vec2 VECTOR` puts the vector second in the range, and the scan still
    inserts `vec2` on all three documents without a comparison, (3, 0). The schema has no other
    vector field, so the counters see only `vec2`.

    A write that lands while the scan is paused gives doc:0 a `vec2` entry under its current
    doc-id, as in test_alter_added_vector_written_during_scan_is_reinserted. Fails if only the
    first added field were treated as added: `vec2` would then be compared, the comparison would
    confirm doc:0's entry and relabel it, giving (2, 1). FLAT: no tiered buffer, so the comparison
    is exact."""
    env, conn = _start('title', 'TEXT')
    for i in range(3):
        conn.execute_command('HSET', f'doc:{i}', 'title', 't', 'extra', 'x', 'vec2', _vec2(i))

    with _alter_paused_before_scan(env, 'extra', 'TAG', 'vec2', *_flat(DIM)):
        conn.execute_command('HSET', 'doc:0', 'vec2', _blob(200.0))
        # After the write, so only the scan's own vector work is counted.
        before = _vector_ops(env)

    env.assertEqual(_ops_delta(before, _vector_ops(env)), (3, 0))
    _assert_exact_hit(env, _blob(200.0), 'doc:0', field='vec2')
    _assert_hits(env, (1, 2), vec=_vec2, field='vec2')
    env.assertEqual(_knn_ids(env, _vec2(0), 10, field='vec2'), _keys(range(3)))
    res = env.cmd('FT.SEARCH', 'idx', '@extra:{x}', 'NOCONTENT')
    env.assertEqual(sorted(r['id'] for r in res['results']), _keys(range(3)))


@skip(cluster=True)
def test_alter_knn_mid_backfill_no_duplicates():
    """KNN queries issued while the selective backfill is paused, after exactly half the
    documents were relabelled onto new doc-ids, return the documents they returned before the
    ALTER, each once; so do queries after the backfill finishes. The query is a filtered KNN with
    an explicit HYBRID_POLICY BATCHES, which routes through the tiered index's own batch iterator
    (a plain, filter-less KNN never does). Related to MOD-18494, but a pause cannot reproduce that
    race: no relabel runs while these queries do. The vectors are FP32 and unchanged, so the
    comparison confirms each one and it is relabelled. `cat` is the filter field, present on
    every document from creation so the query stays valid throughout; `extra` is the field the
    ALTER adds."""
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    conn = getConnectionByEnv(env)
    n = 2000
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'cat', 'TAG', 'vec',
               *_hnsw(DIM)).ok()
    for chunk_start in range(0, n, 500):
        pipe = conn.pipeline(transaction=False)
        for i in range(chunk_start, chunk_start + 500):
            pipe.execute_command('HSET', f'doc:{i}', 'title', 't', 'cat', 'x', 'vec',
                                 _doc_vec(i), 'extra', 'x')
        pipe.execute()
    _drain(env)

    query = '(@cat:{x})=>[KNN 50 @vec $b HYBRID_POLICY BATCHES]'

    # TIMEOUT 0: the default timeout returns a partial reply, which the comparisons below would
    # report as a lost document on a slow host.
    def query_args(center):
        return ['PARAMS', 2, 'b', _doc_vec(center), 'NOCONTENT', 'LIMIT', 0, 50, 'DIALECT', 2,
                'TIMEOUT', 0]

    # The 50 nearest documents to either end of the range, as the index answers before the ALTER.
    centers = (0, n - 1)

    def knn_ids(center):
        res = conn.execute_command('FT.SEARCH', 'idx', query, *query_args(center))
        return sorted(r['id'] for r in res['results'])

    expected = {c: knn_ids(c) for c in centers}
    for c in centers:
        env.assertEqual(len(set(expected[c])), 50, message=expected[c])

    before = _vector_ops(env)
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', n // 2).ok()
    try:
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
        waitForIndexStatus(env, 'PAUSED', 'idx')
        env.assertEqual(_ops_delta(before, _vector_ops(env)), (0, n // 2),
                        message='vector ops when the scan paused')

        # Confirm the query reaches the batch-iterator path this test is about, in the mixed
        # state, rather than silently falling back to STANDARD_KNN or HYBRID_ADHOC_BF.
        profile_res = conn.execute_command('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', query,
                                           *query_args(0))
        modes = [shard['Iterators profile']['Vector search mode']
                 for shard in profile_res['Profile']['Shards']]
        env.assertEqual(modes, ['HYBRID_BATCHES'],
                        message='query does not exercise the batch-iterator path')
        for c in centers:
            env.assertEqual(knn_ids(c), expected[c], message=f'KNN around doc:{c} mid-scan')
    finally:
        env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 0).ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx')
    _drain(env)

    env.assertEqual(_ops_delta(before, _vector_ops(env)), (0, n))
    for c in centers:
        env.assertEqual(knn_ids(c), expected[c], message=f'KNN around doc:{c} after the scan')
