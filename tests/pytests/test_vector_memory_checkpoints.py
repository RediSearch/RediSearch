# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""The memory checkpoints of vector queries, driven by the MEMORY_PRESSURE debug hook. With
AT_CALL 1 a query stops at its first checkpoint call, so a query that reaches no checkpoint
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


def _expect_stopped_by_policy(env, query, n_results):
    """Under memory pressure at the first checkpoint call, `query` replies per ON_OOM: RETURN
    an empty result with the OOM warning, FAIL the OOM error, IGNORE the same results as without
    pressure, which are `n_results` (None: not checked). The cluster FT.SEARCH coordinator relays
    the shards' warning."""
    warning = SHARD_OOM_WARNING if env.isCluster() else COORD_OOM_WARNING
    unpressured = env.cmd(*query)
    env.assertEqual(unpressured['warning'], [], message=unpressured)
    if n_results is not None:
        env.assertEqual(len(unpressured['results']), n_results, message=unpressured)
    try:
        _memory_pressure(env, 'AT_CALL', 1)
        res = env.cmd(*query)
        env.assertEqual(res['results'], [], message=res)
        env.assertEqual(res['warning'], [warning], message=res)

        allShards_change_oom_policy(env, 'fail')
        _memory_pressure(env, 'AT_CALL', 1)
        env.expect(*query).error().contains(OOM_QUERY_ERROR)

        allShards_change_oom_policy(env, 'ignore')
        _memory_pressure(env, 'AT_CALL', 1)
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
    _create_hash_index(env)
    try:
        for workers in (0, 2):
            set_workers(env, workers)
            for timeout_policy in ('return', 'fail'):
                allShards_change_timeout_policy(env, timeout_policy)
                for name, query in _range_queries().items():
                    env.debugPrint(f'{name} WORKERS {workers} ON_TIMEOUT {timeout_policy}')
                    _expect_stopped_by_policy(env, query, RANGE_MATCHES)
    finally:
        allShards_change_timeout_policy(env, 'return')
        set_workers(env, 0)


@skip(cluster=False)
@env_spec(protocol=3)
def test_range_stops_for_oom_on_shards(env):
    """Shards stopped in the range checkpoint report the shard OOM warning or error, which the
    coordinator turns into its own ON_OOM outcome."""
    _create_hash_index(env)
    _expect_stopped_by_policy(env, _range_queries()['flat'], RANGE_MATCHES)


@skip(cluster=True, no_json=True)
@env_spec(protocol=3)
def test_json_multi_value_range_stops_for_oom(env):
    """A range query on a multi-value JSON vector stops the same way."""
    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.vecs[*]', 'AS', 'mv', 'VECTOR',
               'HNSW', 6, 'TYPE', 'FLOAT32', 'DIM', DIM, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(1, N_DOCS + 1):
        conn.execute_command('JSON.SET', f'jdoc{i}', '$',
                             json.dumps({'vecs': [[i] * DIM, [i + 0.5] * DIM]}))
    query = ['FT.SEARCH', 'jidx', f'@mv:[VECTOR_RANGE {RADIUS} $b]', 'PARAMS', 2, 'b',
             _vec(QUERY_VALUE), 'NOCONTENT', 'LIMIT', 0, N_DOCS]
    _expect_stopped_by_policy(env, query, RANGE_MATCHES)


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
    """A filtered KNN in batches checks memory before each further batch; under pressure
    it stops there for memory, with and without workers and under either ON_TIMEOUT."""
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


@skip(cluster=False)
@env_spec(protocol=3)
def test_knn_batches_stop_for_oom_on_shards(env):
    """Shards stopped between batches report the OOM to the coordinator. Small batches make every
    shard holding filtered docs fetch more than one."""
    _create_indexed_hash_index(env)
    _expect_stopped_by_policy(env, _batches_queries()['hnsw'], None)

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
