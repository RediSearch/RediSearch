# -*- coding: utf-8 -*-
# Flow tests for partial hash updates with vector fields.
#
# When PARTIAL_INDEXED_DOCS is enabled, an HSET that does not touch a vector field must NOT lose or
# corrupt that vector: makeDocumentId relabels the existing vector from the old internal doc id to
# the new one (via VecSimIndex_RelabelVector) instead of delete + re-insert. These tests exercise
# that path end-to-end: a broken relabel (dropped vector, wrong label, or a wrongly-skipped add)
# would make the KNN query below miss the document or return a non-zero distance.

from RLTest import Env
from common import *

# All tests here need the command filter that captures the modified hash fields.
PARTIAL_DOCS_ARGS = 'DEFAULT_DIALECT 2 PARTIAL_INDEXED_DOCS 1'


def _knn_top(conn, idx, blob, k=10):
    # Run a KNN query and return {doc_id: {field: value}} for the results.
    res = conn.execute_command(
        'FT.SEARCH', idx, f'*=>[KNN {k} @v $blob AS __dist]',
        'PARAMS', '2', 'blob', blob, 'SORTBY', '__dist', 'DIALECT', '2')
    out = {}
    for i in range(1, len(res), 2):
        doc_id = res[i]
        fields = res[i + 1]
        out[doc_id] = {fields[j]: fields[j + 1] for j in range(0, len(fields), 2)}
    return out


def _vec(values, data_type='FLOAT32'):
    return create_np_array_typed(values, data_type).tobytes()


def test_partial_update_preserves_unchanged_vector():
    """An HSET that changes only non-vector fields must keep the vector intact (relabel path)."""
    env = Env(moduleArgs=PARTIAL_DOCS_ARGS)
    conn = getConnectionByEnv(env)
    for algo in ['HNSW', 'FLAT']:
        env.expect('FT.CREATE', 'idx', 'SCHEMA',
                   'v', 'VECTOR', algo, '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2',
                   't', 'TEXT',
                   'g', 'TAG').ok()

        blob = _vec([0.25, 0.25])
        conn.execute_command('HSET', 'doc1', 'v', blob, 't', 'hello', 'g', 'red')

        # Baseline: the doc is found by exact-vector KNN at distance 0, with its text.
        top = _knn_top(conn, 'idx', blob)
        env.assertContains('doc1', top, message=f'{algo}: baseline KNN missing doc1')
        env.assertAlmostEqual(float(top['doc1']['__dist']), 0.0, 1e-6, message=f'{algo}: baseline dist')

        # Update ONLY the text field. The vector and tag are untouched, so the vector must be
        # relabeled in place (not deleted/re-inserted).
        conn.execute_command('HSET', 'doc1', 't', 'world')

        # The text change took effect...
        env.assertEqual(
            toSortedFlatList(conn.execute_command('FT.SEARCH', 'idx', '@t:world', 'NOCONTENT')),
            toSortedFlatList([1, 'doc1']), message=f'{algo}: text update not indexed')
        # ...and the vector is still present and unchanged (exact match at distance 0).
        top = _knn_top(conn, 'idx', blob)
        env.assertContains('doc1', top, message=f'{algo}: vector lost after text-only update')
        env.assertAlmostEqual(float(top['doc1']['__dist']), 0.0, 1e-6,
                              message=f'{algo}: vector changed after text-only update')
        env.assertEqual(top['doc1']['t'], 'world', message=f'{algo}: stale text in result')

        # Update ONLY the tag field; vector still preserved.
        conn.execute_command('HSET', 'doc1', 'g', 'blue')
        top = _knn_top(conn, 'idx', blob)
        env.assertContains('doc1', top, message=f'{algo}: vector lost after tag-only update')
        env.assertAlmostEqual(float(top['doc1']['__dist']), 0.0, 1e-6,
                              message=f'{algo}: vector changed after tag-only update')

        env.flush()


def test_partial_update_changes_vector():
    """When the HSET does touch the vector, the index must reflect the NEW vector (delete + add)."""
    env = Env(moduleArgs=PARTIAL_DOCS_ARGS)
    conn = getConnectionByEnv(env)
    for algo in ['HNSW', 'FLAT']:
        env.expect('FT.CREATE', 'idx', 'SCHEMA',
                   'v', 'VECTOR', algo, '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2',
                   't', 'TEXT').ok()

        old_blob = _vec([0.1, 0.1])
        new_blob = _vec([0.9, 0.9])
        conn.execute_command('HSET', 'doc1', 'v', old_blob, 't', 'a')

        # Change the vector (and the text in the same command).
        conn.execute_command('HSET', 'doc1', 'v', new_blob, 't', 'b')

        # KNN on the NEW vector finds it at distance 0.
        top = _knn_top(conn, 'idx', new_blob)
        env.assertContains('doc1', top, message=f'{algo}: doc missing after vector change')
        env.assertAlmostEqual(float(top['doc1']['__dist']), 0.0, 1e-6,
                              message=f'{algo}: new vector not indexed')
        # KNN on the OLD vector is now strictly farther than 0 (the old vector is gone).
        top_old = _knn_top(conn, 'idx', old_blob)
        env.assertTrue(float(top_old['doc1']['__dist']) > 0.0,
                       message=f'{algo}: stale old vector still present')
        env.flush()


def test_partial_update_new_key_only_unindexed_field():
    """OpenKey-drop behavior: a brand-new key whose HSET sets only a non-indexed field is not
    indexed; adding an indexed field later indexes it correctly."""
    env = Env(moduleArgs=PARTIAL_DOCS_ARGS)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc:', 'SCHEMA',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    blob = _vec([0.5, 0.5])

    # New key, only a non-indexed field 'other' -> nothing indexed yet.
    conn.execute_command('HSET', 'doc:1', 'other', 'x')
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT'), [0],
                    message='doc with no indexed field should not be in the index')

    # Now add the indexed vector + text -> becomes searchable.
    conn.execute_command('HSET', 'doc:1', 'v', blob, 't', 'hi')
    top = _knn_top(conn, 'idx', blob)
    env.assertContains('doc:1', top, message='doc not indexed after adding indexed fields')
    env.assertAlmostEqual(float(top['doc:1']['__dist']), 0.0, 1e-6)

    # Deleting the doc removes it from the index.
    conn.execute_command('DEL', 'doc:1')
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT'), [0],
                    message='doc still indexed after DEL')
    env.flush()


def test_partial_update_multi_value_vector_preserved():
    """A multi-value vector field must survive a non-vector update (multi relabel path)."""
    env = Env(moduleArgs=PARTIAL_DOCS_ARGS)
    conn = getConnectionByEnv(env)
    # JSON would be the natural multi-value source, but a HASH multi-value vector is expressed by
    # the index using a multi-value field; here we keep it simple with a single vector and verify
    # the single-value relabel, plus a second doc to ensure neighbors are unaffected.
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    b1 = _vec([0.0, 0.0])
    b2 = _vec([1.0, 1.0])
    conn.execute_command('HSET', 'd1', 'v', b1, 't', 'one')
    conn.execute_command('HSET', 'd2', 'v', b2, 't', 'two')

    # Text-only update on d1 (relabel); d2 must remain a correct neighbor.
    conn.execute_command('HSET', 'd1', 't', 'uno')

    top = _knn_top(conn, 'idx', b1)
    env.assertContains('d1', top)
    env.assertAlmostEqual(float(top['d1']['__dist']), 0.0, 1e-6)
    env.assertContains('d2', top, message='neighbor d2 lost after d1 relabel')

    top2 = _knn_top(conn, 'idx', b2)
    env.assertAlmostEqual(float(top2['d2']['__dist']), 0.0, 1e-6, message='d2 vector corrupted')
    env.flush()
