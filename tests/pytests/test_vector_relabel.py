# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

# Relabeling an unchanged vector onto a document's new doc-id, instead of deleting
# the entry and re-adding the blob (MOD-17688). Gated behind OPTIMIZE_UPDATE_VEC,
# which defaults to on.
#
# These tests cover what the C++ suite (`VectorRelabelTest`) cannot: the whole
# chain from a real `HSET` through the hash subkey notification to the indexer's
# change set. The C++ tests call `IndexSpec_UpdateDoc` with a change set directly,
# so the notification plumbing itself is only exercised here.
#
# The observable is `FT.INFO`'s per-field `marked_deleted`. It needs a *tiered*
# HNSW index (`WORKERS 1`), because that is the configuration where dropping a
# vector leaves a tombstone: plain HNSW removes in place and counts nothing, which
# would make relabel and delete + re-add indistinguishable. The vector must also
# have reached the HNSW backend before the update -- a delete from the flat
# frontend is likewise in-place -- hence the `WORKERS DRAIN` after loading.

DIM = 4
MODULE_ARGS = 'WORKERS 1 FORK_GC_RUN_INTERVAL 50000'

def _blob(fill):
    return create_np_array_typed([fill] * DIM, 'FLOAT32').tobytes()

VEC_A = _blob(0.25)
VEC_B = _blob(0.75)

def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'title', 'TEXT',
               'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', DIM,
               'DISTANCE_METRIC', 'L2').ok()

def _load_doc(env, conn):
    conn.execute_command('HSET', 'doc:1', 'title', 'hello', 'vector', VEC_A)
    # Move the vector out of the flat frontend and into the HNSW backend, so that a
    # delete of its label would be recorded as a tombstone rather than done in place.
    verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')

def _create_json_index(env):
    env.expect('FT.CREATE', 'jsonidx', 'ON', 'JSON', 'SCHEMA',
               '$.title', 'AS', 'title', 'TEXT',
               '$.vector', 'AS', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', DIM,
               'DISTANCE_METRIC', 'L2').ok()

def _load_json_doc(env, conn):
    # A JSON vector is an array of numbers, not the binary blob a hash field holds.
    # Same values as VEC_A, so the KNN assertions can reuse it as the query.
    conn.execute_command('JSON.SET', 'doc:1', '$',
                         '{"title":"hello","vector":[0.25,0.25,0.25,0.25]}')
    verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')

def _marked_deleted(env, index='idx'):
    """Tombstones on the vector field, once any pending ingest jobs have settled.

    Selected by attribute rather than by position: `field statistics` carries an
    entry per schema field and only the vector one has this key, so an index-based
    lookup silently reads the TEXT field instead. Attribute and not identifier
    because a JSON schema's identifier is the path (`$.vector`).
    """
    verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')
    stats = index_info(env, index)['field statistics']
    vector_stats = [f for f in stats if f['attribute'] == 'vector']
    env.assertEqual(len(vector_stats), 1)
    return vector_stats[0]['marked_deleted']

def _vector_ops(env):
    """(indexing ops, relabel ops) for vector fields, summed across shards.

    The two are disjoint: moving an entry is not an indexing operation, so exactly one of them
    is counted per vector field per update. Asserting both is what pins that -- either alone
    would still pass if a move were counted twice.
    """
    # The relabel counter is gated on OPTIMIZE_UPDATE_VEC and absent from INFO when the
    # config is off, so a gate-off caller (e.g. test_relabel_requires_optimize_update_vec)
    # needs the absent key to read as zero rather than raising.
    infos = run_command_on_all_shards(env, 'INFO', 'MODULES')
    return (sum(int(i['search_total_indexing_ops_vector_fields']) for i in infos),
            sum(int(i.get('search_total_relabel_ops_vector_fields', 0)) for i in infos))

def _search_ids(env, query, index='idx'):
    # RESP3 (needed for FT.INFO's nested field statistics) replies with a map, not
    # the flat RESP2 array.
    res = env.cmd('FT.SEARCH', index, query, 'NOCONTENT')
    return [doc['id'] for doc in res['results']]

def _assert_doc_is_queryable(env, expected_title, expected_vector, index='idx'):
    # Holds whichever path ran: the point of the optimization is that it is
    # invisible except in the tombstone count.
    env.assertEqual(_search_ids(env, expected_title, index), ['doc:1'])
    res = env.cmd('FT.SEARCH', index, '*=>[KNN 1 @vector $b AS score]', 'PARAMS', '2', 'b',
                  expected_vector, 'RETURN', '1', 'score', 'DIALECT', '2')
    env.assertEqual(res['total_results'], 1)
    env.assertEqual(res['results'][0]['id'], 'doc:1')
    # Distance 0 to the blob the caller expects: this is what catches a relabel that
    # moved an entry holding the wrong vector.
    env.assertEqual(res['results'][0]['extra_attributes']['score'], '0')

def test_relabel_unchanged_vector_on_text_update():
    """A text-only update must move the existing vector entry, leaving no tombstone.

    Deliberately not gated on subkey-notification support. Both routes to that outcome are
    in scope: the change set names `title` and not `vector`, and without one the blob
    comparison finds the stored vector unchanged. The assertions below hold either way, so
    running this against a server that predates subkey notifications is the coverage for
    the comparison route on a hash.
    """
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    conn = env.getClusterConnectionIfNeeded()

    _create_index(env)
    _load_doc(env, conn)
    env.assertEqual(_marked_deleted(env), 0)

    conn.execute_command('HSET', 'doc:1', 'title', 'goodbye')

    env.assertEqual(_marked_deleted(env), 0,
                    message='a tombstone here means the unchanged vector was deleted and re-added')
    # One indexing op for the initial load, and the update counted as a move rather than a
    # second indexing op.
    env.assertEqual(_vector_ops(env), (1, 1))
    _assert_doc_is_queryable(env, 'goodbye', VEC_A)
    env.assertEqual(_search_ids(env, 'hello'), [])

def test_vector_change_reindexes():
    """The safety direction: when the vector *does* change it must be re-added, not moved.

    A relabel here would leave the old blob in the index under the new doc-id, so
    the KNN assertion on VEC_B is what catches it; the tombstone assertion pins
    that the old entry was dropped rather than orphaned.

    Ungated for the same reason as `test_relabel_unchanged_vector_on_text_update`: the
    change set names `vector`, and without one the comparison sees VEC_B differ from
    VEC_A. Both reach the re-add this asserts.
    """
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    conn = env.getClusterConnectionIfNeeded()

    _create_index(env)
    _load_doc(env, conn)

    conn.execute_command('HSET', 'doc:1', 'title', 'goodbye', 'vector', VEC_B)

    env.assertEqual(_marked_deleted(env), 1)
    # Load plus a genuine re-add: two indexing ops, no move.
    env.assertEqual(_vector_ops(env), (2, 0))
    _assert_doc_is_queryable(env, 'goodbye', VEC_B)

def test_relabel_requires_optimize_update_vec():
    """The gate: with the config off, the same update takes the delete + re-add path.

    Deliberately the same input and the same query assertions as
    `test_relabel_unchanged_vector_on_text_update` -- only the tombstone count
    differs, which is the whole claim of Requirement 1 (no behavior difference with
    the config off). Needs no subkey-notification support: with the config off the
    change set is not consulted at all.
    """
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-optimize-update-vec', 'no')
    conn = env.getClusterConnectionIfNeeded()

    _create_index(env)
    _load_doc(env, conn)

    conn.execute_command('HSET', 'doc:1', 'title', 'goodbye')

    env.assertEqual(_marked_deleted(env), 1,
                    message='with the flag off the vector must be deleted and re-added')
    env.assertEqual(_vector_ops(env), (2, 0), message='the gate is off, so nothing is moved')
    _assert_doc_is_queryable(env, 'goodbye', VEC_A)
    env.assertEqual(_search_ids(env, 'hello'), [])

@skip(no_json=True)
def test_json_doc_relabels_unchanged_vector():
    """JSON relabels too, decided by comparing the vector against what the index holds.

    RedisJSON's API cannot report which paths a command wrote, so there is no change
    set here and `AddDocumentCtx_MarkForRelabel` can only mark the field
    `ChangedField_Unverified`. The insert site then settles it: the new value equals the
    stored one, so the entry moves.

    This is the only place that proves the move for the unverified path -- for an
    unchanged blob a move and a re-add are indistinguishable in the index contents, and
    only the tombstone count separates them.
    """
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    conn = env.getClusterConnectionIfNeeded()

    _create_json_index(env)
    _load_json_doc(env, conn)
    env.assertEqual(_marked_deleted(env, 'jsonidx'), 0)

    conn.execute_command('JSON.SET', 'doc:1', '$.title', '"goodbye"')

    env.assertEqual(_marked_deleted(env, 'jsonidx'), 0,
                    message='a tombstone means the unchanged vector was deleted and re-added')
    _assert_doc_is_queryable(env, 'goodbye', VEC_A, 'jsonidx')
    env.assertEqual(_search_ids(env, 'hello', 'jsonidx'), [])

@skip(no_json=True)
def test_json_doc_with_changed_vector_reindexes():
    """The safety half of the JSON path: a changed vector must not be moved.

    Nothing external says the vector changed, so only the blob comparison stands
    between this and a stale entry surviving under the new doc-id.
    """
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    conn = env.getClusterConnectionIfNeeded()

    _create_json_index(env)
    _load_json_doc(env, conn)

    conn.execute_command('JSON.SET', 'doc:1', '$.vector', '[0.75,0.75,0.75,0.75]')

    env.assertEqual(_marked_deleted(env, 'jsonidx'), 1)
    _assert_doc_is_queryable(env, 'hello', VEC_B, 'jsonidx')

def _vector_values_for_type(data_type):
    # INT8/UINT8 truncate 0.25/0.75 to 0, which is indistinguishable from "no value" for the
    # comparison this exercises, so those two get their own pair of values.
    if data_type in ('INT8', 'UINT8'):
        return 1, 2
    return 0.25, 0.75

@skip(no_json=True)
def test_json_doc_relabel_dispatches_on_vector_type():
    """`VectorIndex_HoldsVectors` (vector_compare.cpp) dispatches on the index's VecSim data
    type, but every other test in this file only ever creates a FLOAT32 index -- so the other
    five instantiations (FLOAT64, FLOAT16, BFLOAT16, INT8, UINT8) are otherwise never
    exercised. A type-specific bug there -- a wrong cast, element width, or cosine
    normalization step -- would ship silently.

    JSON is the vehicle for the same reason as the FLOAT32 JSON tests above: it never carries
    a change set, so this always takes the comparison path rather than the change-set
    shortcut. One index per type, in the same env, so a failure on one type doesn't hide a
    failure on another.
    """
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    conn = env.getClusterConnectionIfNeeded()

    for data_type in [t for t in VECSIM_DATA_TYPES if t != 'FLOAT32'] + ['INT8', 'UINT8']:
        index = f'jsonidx_{data_type.lower()}'
        unchanged, changed = _vector_values_for_type(data_type)

        env.expect('FT.CREATE', index, 'ON', 'JSON', 'SCHEMA',
                   '$.title', 'AS', 'title', 'TEXT',
                   '$.vector', 'AS', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', data_type,
                   'DIM', DIM, 'DISTANCE_METRIC', 'L2').ok(message=data_type)

        unchanged_vec = create_np_array_typed([unchanged] * DIM, data_type).tobytes()
        changed_vec = create_np_array_typed([changed] * DIM, data_type).tobytes()

        conn.execute_command('JSON.SET', 'doc:1', '$',
                             f'{{"title":"hello","vector":[{unchanged}, {unchanged}, {unchanged}, {unchanged}]}}')
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')
        env.assertEqual(_marked_deleted(env, index), 0, message=data_type)

        # A text-only change: the vector is unchanged, so it must relabel rather than reindex.
        conn.execute_command('JSON.SET', 'doc:1', '$.title', '"goodbye"')
        env.assertEqual(_marked_deleted(env, index), 0,
                        message=f'{data_type}: a tombstone means the unchanged vector was deleted and re-added')
        _assert_doc_is_queryable(env, 'goodbye', unchanged_vec, index)

        # A real vector change: the comparison must catch it and reindex, not relabel.
        conn.execute_command('JSON.SET', 'doc:1', '$.vector',
                             f'[{changed}, {changed}, {changed}, {changed}]')
        env.assertEqual(_marked_deleted(env, index), 1, message=data_type)
        _assert_doc_is_queryable(env, 'goodbye', changed_vec, index)

        conn.execute_command('DEL', 'doc:1')

@skip(no_json=True)
def test_json_doc_relabel_requires_optimize_update_vec():
    """The gate, on the JSON path: config off, so no comparison and no move."""
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-optimize-update-vec', 'no')
    conn = env.getClusterConnectionIfNeeded()

    _create_json_index(env)
    _load_json_doc(env, conn)

    conn.execute_command('JSON.SET', 'doc:1', '$.title', '"goodbye"')

    env.assertEqual(_marked_deleted(env, 'jsonidx'), 1,
                    message='with the flag off the vector must be deleted and re-added')
    _assert_doc_is_queryable(env, 'goodbye', VEC_A, 'jsonidx')

@skip(no_json=True)
def test_json_doc_nulling_the_vector_leaves_no_orphan():
    """A JSON document that stops carrying a vector must not strand the old entry.

    Two ways for the field to lose its value, and they take different routes through the
    marking. Setting the path to `null` still loads the field -- as `FLD_VAR_T_NULL` -- so it
    is still marked for relabeling, and `Indexer_HandleReplacedDocVectorAndGeometry` has to
    notice there is no value to move and drop the entry anyway. Removing the path outright
    never produces a document field, so it is never marked and takes the ordinary delete.

    Either way the entry must go: an orphan sits at a doc-id no document owns, is returned by
    KNN queries, and is never collected.
    """
    env = Env(protocol=3, moduleArgs=MODULE_ARGS)
    conn = env.getClusterConnectionIfNeeded()

    _create_json_index(env)

    for description, document in (('null vector', '{"title":"goodbye","vector":null}'),
                                  ('no vector', '{"title":"goodbye"}')):
        conn.execute_command('DEL', 'doc:1')
        _load_json_doc(env, conn)
        conn.execute_command('JSON.SET', 'doc:1', '$', document)
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')

        # Still indexed for its text, but owning no vector, so a KNN query must find nothing
        # rather than the entry the old doc-id used to hold.
        env.assertEqual(_search_ids(env, 'goodbye', 'jsonidx'), ['doc:1'], message=description)
        res = env.cmd('FT.SEARCH', 'jsonidx', '*=>[KNN 1 @vector $b]', 'PARAMS', '2', 'b', VEC_A,
                      'NOCONTENT', 'DIALECT', '2')
        env.assertEqual(res['total_results'], 0,
                        message=f'{description}: an orphaned vector entry is still queryable')
