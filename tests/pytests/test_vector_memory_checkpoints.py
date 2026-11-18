# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""The memory checkpoints of vector queries, driven by the MEMORY_PRESSURE debug hook. With
AT_CALL n a query stops at its n-th checkpoint call, so a query that reaches no checkpoint
returns all of its results."""

import json

from common import *
from vecsim_utils import get_tiered_debug_info, wait_for_background_indexing

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
COORD_OOM_WARNING = "Coordinator failed to execute the query due to insufficient memory"
SHARD_OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"

N_DOCS = 100
DIM = 4
# Doc i is [i; DIM] in L2, so a radius of RADIUS around QUERY_VALUE matches the docs within 10
# of it.
QUERY_VALUE = 50
RADIUS = DIM * 10 ** 2
RANGE_MATCHES = 21
# The same docs as the range matches, so the baseline's per-row calls equal the vector query's.
RANGE_BASELINE = f'@n:[{QUERY_VALUE - 10} {QUERY_VALUE + 10}]'
# Calls a vector range query adds to its baseline: the range checkpoint, plus on FLAT the one
# VecSim's stop callback makes for its single block.
FLAT_RANGE_CALLS = 2
HNSW_RANGE_CALLS = 1


def _vec(value):
    return np.full(DIM, value, dtype=np.float32).tobytes()


def _create_hash_index(env):
    env.expect('FT.CREATE', 'hidx', 'ON', 'HASH', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', DIM, 'DISTANCE_METRIC', 'L2',
               'vh', 'VECTOR', 'HNSW', 6, 'TYPE', 'FLOAT32', 'DIM', DIM, 'DISTANCE_METRIC', 'L2',
               'n', 'NUMERIC', 't', 'TAG').ok()
    conn = getConnectionByEnv(env)
    for i in range(1, N_DOCS + 1):
        conn.execute_command('HSET', f'doc{i}', 'v', _vec(i), 'vh', _vec(i), 'n', i,
                             't', 'even' if i % 2 == 0 else 'odd')


def _memory_pressure(env, *args):
    run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', *args)


def _range_queries():
    """Range queries on FLAT, and on HNSW yielding the distance through a sorter."""
    params = ['PARAMS', 2, 'b', _vec(QUERY_VALUE)]
    return {
        'flat': ['FT.SEARCH', 'hidx', f'@v:[VECTOR_RANGE {RADIUS} $b]', *params,
                 'NOCONTENT', 'LIMIT', 0, N_DOCS],
        'hnsw_yield': ['FT.SEARCH', 'hidx',
                       f'@vh:[VECTOR_RANGE {RADIUS} $b]=>{{$YIELD_DISTANCE_AS: d}}', *params,
                       'SORTBY', 'd', 'RETURN', 1, 'd', 'LIMIT', 0, N_DOCS],
        'aggregate': ['FT.AGGREGATE', 'hidx', f'@v:[VECTOR_RANGE {RADIUS} $b]', *params,
                      'LOAD', 1, '@n', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, N_DOCS],
    }


def _checkpoint_calls(env, query):
    """The checkpoint calls `query` makes in each process (each shard in a cluster) when nothing
    stops it, counted by positional pressure armed out of reach."""
    try:
        _memory_pressure(env, 'AT_CALL', 2 ** 62)
        env.cmd(*query)
        statuses = run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')
    finally:
        _memory_pressure(env, 'RESET')
    return [status[5] for status in statuses]


def _vector_calls(env, query, baseline_filter):
    """The checkpoint calls `query` makes per process before any row is replied (LIMIT 0 0), and
    how many of them its vector part adds to the same query on `baseline_filter`."""
    def calls(q):
        q = list(q)
        at = q.index('LIMIT')
        q[at + 1:at + 3] = [0, 0]
        return _checkpoint_calls(env, q)

    with_vector = calls(query)
    baseline = calls([*query[:2], baseline_filter, *query[3:]])
    return with_vector, [c - b for c, b in zip(with_vector, baseline, strict=True)]


def _vector_checkpoint_positions(env, query, vector_calls, baseline_filter=RANGE_BASELINE):
    """Call positions that include the vector checkpoints of `query`, a vector range query whose
    filter is `query[2]`. The calls it adds to the same query on `baseline_filter` must be
    `vector_calls` in every process, so a query without a vector checkpoint fails here; stopping
    at each position up to the vector's covers it without depending on its order among the
    others. Counted at LIMIT 0 0, so the reply loop's own checkpoints stay out of the count."""
    with_vector, extra = _vector_calls(env, query, baseline_filter)
    env.assertEqual(extra, [vector_calls] * len(extra), message=(with_vector, extra))
    # In a cluster the first process also coordinates, and its calls are not all the shard's.
    return range(1, min(with_vector[1:] or with_vector) + 1)


def _arm_pressure_at_call(env, at_calls):
    """AT_CALL per process: `at_calls` lists one position per shard, None means 1 everywhere."""
    if at_calls is None:
        _memory_pressure(env, 'AT_CALL', 1)
        return
    for conn, at_call in zip(env.getOSSMasterNodesConnectionList(), at_calls, strict=True):
        conn.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', at_call)


def _shards_only_at_call(env, at_call):
    """AT_CALL for `_expect_stopped_by_policy` with the first process unarmed: it also
    coordinates, and its calls race with its shard's, so which one hits `at_call` varies.
    TODO: arm it too, and expect an empty RETURN, once the MEMORY_PRESSURE hook scopes AT_CALL to
    the shard role."""
    return [2 ** 62] + [at_call] * (env.shardsCount - 1)


def _expect_stopped_by_policy(env, query, n_results, at_calls=None, keeps_some_results=False):
    """Under memory pressure at checkpoint call `at_calls` (see `_arm_pressure_at_call`), `query`
    replies per ON_OOM: RETURN an empty result with the OOM warning, FAIL the OOM error, IGNORE
    the same results as without pressure, which are `n_results` (None: not checked). The cluster
    FT.SEARCH coordinator relays the shards' warning. `keeps_some_results`: processes left unarmed
    reply, so RETURN keeps their rows instead of none."""
    warning = SHARD_OOM_WARNING if env.isCluster() else COORD_OOM_WARNING
    unpressured = env.cmd(*query)
    env.assertEqual(unpressured['warning'], [], message=unpressured)
    if n_results is not None:
        env.assertEqual(len(unpressured['results']), n_results, message=unpressured)
    try:
        _arm_pressure_at_call(env, at_calls)
        res = env.cmd(*query)
        if keeps_some_results:
            if n_results is not None:
                env.assertLess(len(res['results']), n_results, message=res)
        else:
            env.assertEqual(res['results'], [], message=res)
        env.assertEqual(res['warning'], [warning], message=res)

        allShards_change_oom_policy(env, 'fail')
        _arm_pressure_at_call(env, at_calls)
        env.expect(*query).error().contains(OOM_QUERY_ERROR)

        allShards_change_oom_policy(env, 'ignore')
        _arm_pressure_at_call(env, at_calls)
        env.assertEqual(env.cmd(*query), unpressured)
    finally:
        allShards_change_oom_policy(env, 'return')
        _memory_pressure(env, 'RESET')

    env.assertEqual(env.cmd(*query), unpressured)


@skip(cluster=True)
@env_spec(protocol=3)
def test_range_stops_for_oom(env):
    """A vector range query checks memory once VecSim has its reply and before copying it; under
    pressure it stops for memory, with and without workers and under either ON_TIMEOUT."""
    _create_indexed_hash_index(env)
    # Counted without workers: an aggregation with workers also counts its rows' calls at LIMIT 0 0.
    vector_calls = {'flat': FLAT_RANGE_CALLS, 'hnsw_yield': HNSW_RANGE_CALLS,
                    'aggregate': FLAT_RANGE_CALLS}
    positions = {name: _vector_checkpoint_positions(env, query, vector_calls[name])
                 for name, query in _range_queries().items()}
    try:
        for workers in (0, 2):
            set_workers(env, workers)
            for timeout_policy in ('return', 'fail'):
                allShards_change_timeout_policy(env, timeout_policy)
                for name, query in _range_queries().items():
                    env.debugPrint(f'{name} WORKERS {workers} ON_TIMEOUT {timeout_policy}')
                    for at_call in positions[name]:
                        _expect_stopped_by_policy(env, query, RANGE_MATCHES, [at_call])
    finally:
        allShards_change_timeout_policy(env, 'return')
        set_workers(env, 0)


@skip(cluster=False, min_shards=2)
@env_spec(protocol=3)
def test_range_stops_for_oom_on_shards(env):
    """Shards stopped in the range checkpoint report the shard OOM warning or error, which the
    coordinator turns into its own ON_OOM outcome."""
    _create_indexed_hash_index(env)
    query = _range_queries()['flat']
    for at_call in _vector_checkpoint_positions(env, query, FLAT_RANGE_CALLS):
        _expect_stopped_by_policy(env, query, RANGE_MATCHES, _shards_only_at_call(env, at_call),
                                  keeps_some_results=True)


@skip(cluster=True, no_json=True)
@env_spec(protocol=3)
def test_json_multi_value_range_stops_for_oom(env):
    """A range query on a multi-value JSON vector stops the same way. The index has no numeric
    field, so the baseline is the wildcard: at LIMIT 0 0 it adds no per-row calls."""
    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.vecs[*]', 'AS', 'mv', 'VECTOR',
               'HNSW', 6, 'TYPE', 'FLOAT32', 'DIM', DIM, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(1, N_DOCS + 1):
        conn.execute_command('JSON.SET', f'jdoc{i}', '$',
                             json.dumps({'vecs': [[i] * DIM, [i + 0.5] * DIM]}))
    if get_tiered_debug_info(env, 'jidx', 'mv')['ALGORITHM'] == 'TIERED':
        wait_for_background_indexing(env, 'jidx', 'mv', backend_index_size=2 * N_DOCS)
    query = ['FT.SEARCH', 'jidx', f'@mv:[VECTOR_RANGE {RADIUS} $b]', 'PARAMS', 2, 'b',
             _vec(QUERY_VALUE), 'NOCONTENT', 'LIMIT', 0, N_DOCS]
    for at_call in _vector_checkpoint_positions(env, query, HNSW_RANGE_CALLS, '*'):
        _expect_stopped_by_policy(env, query, RANGE_MATCHES, [at_call])


KNN_K = 10


def _create_indexed_hash_index(env):
    """`_create_hash_index`, waiting until the tiered HNSW field holds every vector: while the
    flat buffer drains, its exact answers turn into the graph's approximate ones, so the same
    batched KNN query could return different docs from one call to the next."""
    _create_hash_index(env)
    if get_tiered_debug_info(env, 'hidx', 'vh')['ALGORITHM'] == 'TIERED':
        wait_for_background_indexing(env, 'hidx', 'vh', backend_index_size=N_DOCS)


def _batches_queries():
    """Filtered KNN queries whose filter keeps docs far from the query vector, so the top k takes
    more than one batch: on FLAT, and on HNSW in small batches."""
    params = ['PARAMS', 2, 'b', _vec(QUERY_VALUE)]
    return {
        'flat': ['FT.SEARCH', 'hidx', f'(@n:[1 20])=>[KNN {KNN_K} @v $b HYBRID_POLICY BATCHES]',
                 *params, 'NOCONTENT', 'LIMIT', 0, KNN_K],
        'hnsw': ['FT.SEARCH', 'hidx',
                 f'(@n:[90 +inf])=>[KNN {KNN_K} @vh $b HYBRID_POLICY BATCHES BATCH_SIZE 5]',
                 *params, 'NOCONTENT', 'LIMIT', 0, KNN_K],
    }


@skip(cluster=True)
@env_spec(protocol=3)
def test_knn_batches_stop_for_oom(env):
    """A filtered KNN in batches stops for memory at its first checkpoint, with and without
    workers and under either ON_TIMEOUT. That is VecSim's stop callback at the start of the first
    batch. The read before a further batch is not covered end to end: its stop is
    indistinguishable in the reply from VecSim's poll at the start of that batch, so
    batch_memory.rs pins it."""
    _create_indexed_hash_index(env)
    try:
        for workers in (0, 2):
            set_workers(env, workers)
            for timeout_policy in ('return', 'fail'):
                allShards_change_timeout_policy(env, timeout_policy)
                for name, query in _batches_queries().items():
                    env.debugPrint(f'{name} WORKERS {workers} ON_TIMEOUT {timeout_policy}')
                    # The HNSW batch iterator is approximate: its count is not checked.
                    _expect_stopped_by_policy(env, query, KNN_K if name == 'flat' else None)
    finally:
        allShards_change_timeout_policy(env, 'return')
        set_workers(env, 0)


@skip(cluster=False, min_shards=2)
@env_spec(protocol=3)
def test_knn_batches_stop_for_oom_on_shards(env):
    """Shards stopped at their second checkpoint report the OOM to the coordinator. The query's
    vector part adds at least two calls (VecSim's stop callback and the read before a further
    batch), which proves the checkpoints exist; the position only drives the shard report. The
    reply cannot tell the further-batch read from an interval poll inside the first batch, so
    batch_memory.rs pins it."""
    _create_indexed_hash_index(env)
    query = _batches_queries()['hnsw']
    _, extra = _vector_calls(env, query, '(@n:[90 +inf])')
    env.assertTrue(all(c >= 2 for c in extra), message=extra)
    _expect_stopped_by_policy(env, query, None, _shards_only_at_call(env, 2),
                              keeps_some_results=True)


# A FLAT scan calls VecSim's stop callback once per vector block (1024 vectors), so every shard
# of a 3-shard cluster needs more than one block.
N_LONG_SEARCH_DOCS = 6000


def _long_search_queries():
    """Range and plain KNN queries on FLAT, HNSW and SVS whose single VecSim search visits enough
    vectors or candidates to call VecSim's stop callback, on every shard of a cluster."""
    params = ['PARAMS', 2, 'b', _vec(QUERY_VALUE)]
    all_radius = DIM * N_LONG_SEARCH_DOCS ** 2
    return {
        'flat_range': ['FT.SEARCH', 'lidx', f'@v:[VECTOR_RANGE {RADIUS} $b]', *params,
                       'NOCONTENT', 'LIMIT', 0, N_DOCS],
        # The HNSW range search visits about as many candidates as it matches: all of them.
        'hnsw_range': ['FT.SEARCH', 'lidx',
                       f'@vh:[VECTOR_RANGE {DIM * N_LONG_SEARCH_DOCS ** 2} $b]', *params,
                       'NOCONTENT', 'LIMIT', 0, N_DOCS],
        'flat_knn': ['FT.SEARCH', 'lidx', f'*=>[KNN {KNN_K} @v $b]', *params,
                     'NOCONTENT', 'LIMIT', 0, KNN_K],
        'hnsw_knn': ['FT.SEARCH', 'lidx', f'*=>[KNN {KNN_K} @vh $b EF_RUNTIME 1000]', *params,
                     'NOCONTENT', 'LIMIT', 0, KNN_K],
        'svs_range': ['FT.SEARCH', 'lidx', f'@vs:[VECTOR_RANGE {all_radius} $b]', *params,
                      'NOCONTENT', 'LIMIT', 0, N_DOCS],
        'svs_knn': ['FT.SEARCH', 'lidx', f'*=>[KNN {KNN_K} @vs $b]', *params,
                    'NOCONTENT', 'LIMIT', 0, KNN_K],
    }


def _expect_stopped_inside_vecsim(env, queries=None, at_calls=None, keeps_some_results=False):
    """Each query's vector part adds a checkpoint inside its VecSim search to the baseline: a range
    query calls one more after the search, a plain KNN none. That count proves the checkpoint
    exists; pressure at call 1 only drives the stop and its ON_OOM reply."""
    for name, query in (queries or _long_search_queries()).items():
        env.debugPrint(name)
        after_search = 1 if name.endswith('range') else 0
        _, extra = _vector_calls(env, query, '*')
        for process_extra in extra:
            env.assertGreater(process_extra, after_search, message=name)
        _expect_stopped_by_policy(env, query, RANGE_MATCHES if name == 'flat_range' else None,
                                  at_calls, keeps_some_results)


def _create_long_search_index(env):
    env.expect('FT.CREATE', 'lidx', 'ON', 'HASH', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', DIM, 'DISTANCE_METRIC', 'L2',
               'vh', 'VECTOR', 'HNSW', 6, 'TYPE', 'FLOAT32', 'DIM', DIM,
               'DISTANCE_METRIC', 'L2',
               # A wide search window, so an SVS KNN search takes enough steps to poll.
               'vs', 'VECTOR', 'SVS-VAMANA', 8, 'TYPE', 'FLOAT32', 'DIM', DIM,
               'DISTANCE_METRIC', 'L2', 'SEARCH_WINDOW_SIZE', 1000).ok()
    pipe = getConnectionByEnv(env).pipeline(transaction=False)
    for i in range(1, N_LONG_SEARCH_DOCS + 1):
        pipe.execute_command('HSET', f'ldoc{i}', 'v', _vec(i), 'vh', _vec(i), 'vs', _vec(i))
    pipe.execute()
    for field in ('vh', 'vs'):
        if get_tiered_debug_info(env, 'lidx', field)['ALGORITHM'] == 'TIERED':
            wait_for_background_indexing(env, 'lidx', field,
                                         backend_index_size=N_LONG_SEARCH_DOCS)


@skip(cluster=True)
@env_spec(protocol=3)
def test_single_vecsim_search_stops_for_oom(env):
    """VecSim's stop callback reads memory once per vector block or every few graph steps, so a
    single range or KNN search stops for OOM while it runs, with and without workers and under
    either ON_TIMEOUT."""
    _create_long_search_index(env)
    try:
        for workers in (0, 2):
            set_workers(env, workers)
            for timeout_policy in ('return', 'fail'):
                allShards_change_timeout_policy(env, timeout_policy)
                env.debugPrint(f'WORKERS {workers} ON_TIMEOUT {timeout_policy}')
                _expect_stopped_inside_vecsim(env)
    finally:
        allShards_change_timeout_policy(env, 'return')
        set_workers(env, 0)


@skip(cluster=False, min_shards=2)
@env_spec(protocol=3)
def test_single_vecsim_search_stops_for_oom_on_shards(env):
    """Shards stopped inside a VecSim search report the OOM to the coordinator."""
    _create_long_search_index(env)
    _expect_stopped_inside_vecsim(env, at_calls=_shards_only_at_call(env, 1),
                                  keeps_some_results=True)


# The default tiered HNSW flat buffer limit: one VecSim block.
FLAT_BUFFER_LIMIT = 1024


@skip(cluster=True)
@env_spec(protocol=3)
def test_vecsim_search_of_a_full_flat_buffer_stops_for_oom(env):
    """A tiered HNSW index whose flat buffer is full answers from the buffer's block scan, which
    stops for OOM too."""
    env.expect('FT.CREATE', 'bidx', 'ON', 'HASH', 'PREFIX', 1, 'bdoc', 'SCHEMA',
               'vh', 'VECTOR', 'HNSW', 6, 'TYPE', 'FLOAT32', 'DIM', DIM,
               'DISTANCE_METRIC', 'L2').ok()
    params = ['PARAMS', 2, 'b', _vec(QUERY_VALUE)]
    queries = {
        'buffer_range': ['FT.SEARCH', 'bidx', f'@vh:[VECTOR_RANGE {RADIUS} $b]', *params,
                         'NOCONTENT', 'LIMIT', 0, N_DOCS],
        'buffer_knn': ['FT.SEARCH', 'bidx', f'*=>[KNN {KNN_K} @vh $b]', *params,
                       'NOCONTENT', 'LIMIT', 0, KNN_K],
    }
    with paused_workers(env):
        pipe = getConnectionByEnv(env).pipeline(transaction=False)
        for i in range(1, FLAT_BUFFER_LIMIT + 1):
            pipe.execute_command('HSET', f'bdoc{i}', 'vh', _vec(i))
        pipe.execute()
        frontend = to_dict(get_tiered_debug_info(env, 'bidx', 'vh')['FRONTEND_INDEX'])
        env.assertEqual(frontend['INDEX_SIZE'], FLAT_BUFFER_LIMIT, message=frontend)
        _expect_stopped_inside_vecsim(env, queries)


ADHOC_MATCHES = 15
ADHOC_K = 20

def _adhoc_queries():
    """Filtered KNN forced to an adhoc brute-force scan whose K exceeds the filter's matches, so
    the scan walks every match and ends without a further batch."""
    params = ['PARAMS', 2, 'b', _vec(QUERY_VALUE)]
    flt = f'(@n:[1 {ADHOC_MATCHES}])'
    return {
        'flat': ['FT.SEARCH', 'hidx',
                 f'{flt}=>[KNN {ADHOC_K} @v $b HYBRID_POLICY ADHOC_BF]',
                 *params, 'NOCONTENT', 'LIMIT', 0, ADHOC_K],
        'hnsw': ['FT.SEARCH', 'hidx',
                 f'{flt}=>[KNN {ADHOC_K} @vh $b HYBRID_POLICY ADHOC_BF]',
                 *params, 'NOCONTENT', 'LIMIT', 0, ADHOC_K],
    }

@skip(cluster=True)
@env_spec(protocol=3)
def test_knn_adhoc_stops_for_oom(env):
    """An adhoc-BF scan checks memory per matching doc, which retains a captured child record
    each; under pressure it stops there for memory, with and without workers and under
    either ON_TIMEOUT."""
    _create_indexed_hash_index(env)
    try:
        for workers in (0, 2):
            set_workers(env, workers)
            for timeout_policy in ('return', 'fail'):
                allShards_change_timeout_policy(env, timeout_policy)
                for name, query in _adhoc_queries().items():
                    env.debugPrint(f'{name} WORKERS {workers} ON_TIMEOUT {timeout_policy}')
                    _expect_stopped_by_policy(env, query, ADHOC_MATCHES)
    finally:
        allShards_change_timeout_policy(env, 'return')
        set_workers(env, 0)

@skip(cluster=True)
@env_spec(protocol=3)
def test_knn_adhoc_stops_for_oom_anywhere_in_the_scan(env):
    """Pressure landing on the first, a middle, the last doc of the scan, or on the read after
    it, stops the query: the per-doc checkpoint is amortized over READ_INTERVAL calls, but a
    positional pressure and the closing read do not wait for it."""
    _create_indexed_hash_index(env)
    try:
        allShards_change_oom_policy(env, 'fail')
        for workers in (0, 2):
            set_workers(env, workers)
            for name, query in _adhoc_queries().items():
                for at_call in (1, 2, ADHOC_MATCHES // 2, ADHOC_MATCHES, ADHOC_MATCHES + 1):
                    env.debugPrint(f'{name} WORKERS {workers} AT_CALL {at_call}')
                    _memory_pressure(env, 'READ_INTERVAL', 1000)
                    _memory_pressure(env, 'AT_CALL', at_call)
                    env.expect(*query).error().contains(OOM_QUERY_ERROR)
                    _memory_pressure(env, 'RESET')
    finally:
        allShards_change_oom_policy(env, 'return')
        _memory_pressure(env, 'RESET')
        set_workers(env, 0)


@skip(cluster=True)
@env_spec(protocol=3)
def test_range_under_numeric_sort_optimizer_stops_for_oom(env):
    """The numeric sort optimizer wraps the range iterator for SORTBY on a numeric field with
    WITHOUTCOUNT or DIALECT 4; a stop at the range's first read must come back as the OOM reply,
    not be read as a result."""
    _create_indexed_hash_index(env)
    params = ['PARAMS', 2, 'b', _vec(QUERY_VALUE)]
    for opt in (['WITHOUTCOUNT'], ['DIALECT', 4]):
        for field in ('v', 'vh'):
            query = ['FT.SEARCH', 'hidx',
                     f'@{field}:[VECTOR_RANGE {RADIUS} $b]=>{{$YIELD_DISTANCE_AS: d}}', *params,
                     'SORTBY', 'n', 'NOCONTENT', 'LIMIT', 0, 5, *opt]
            env.debugPrint(f'{field} {opt}')
            vector_calls = FLAT_RANGE_CALLS if field == 'v' else HNSW_RANGE_CALLS
            for at_call in _vector_checkpoint_positions(env, query, vector_calls):
                _expect_stopped_by_policy(env, query, 5, [at_call])
