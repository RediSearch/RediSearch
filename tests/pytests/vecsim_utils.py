from RLTest import Env
from includes import *
from common import (
    getConnectionByEnv,
    to_dict,
    debug_cmd,
    index_info,
    TimeLimit,
    create_random_np_array_typed,
    getWorkersThpoolStats,
    workers_jobs_done,
)
import numpy as np
import time
from redis import exceptions as redis_exceptions

VECSIM_DISTANCE_METRICS = ['COSINE', 'L2', 'IP']

DEFAULT_BLOCK_SIZE = 1024
DEFAULT_INDEX_NAME = 'idx'
DEFAULT_FIELD_NAME = 'v'
DEFAULT_DOC_NAME_PREFIX = 'doc'

# @param additional_schema_args - additional arguments to pass to FT.CREATE beyond TYPE, DIM, DISTANCE_METRIC
def create_vector_index(env: Env, dim, index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME, datatype='FLOAT32', metric='L2',
                        alg='FLAT', additional_vec_params=None,
                        additional_schema_args=None, message='', depth=0):
    if additional_schema_args is None:
        additional_schema_args = []
    params = ['TYPE', datatype, 'DIM', dim, 'DISTANCE_METRIC', metric]
    if additional_vec_params is not None:
        params.extend(additional_vec_params)
    try:
        env.execute_command('FT.CREATE', index_name, 'SCHEMA',
                field_name, 'VECTOR', alg, len(params), *params,
                *additional_schema_args)
    except redis_exceptions.ResponseError as e:
        env.assertTrue(False, message=f"Failed to create index: '{index_name}', metric: {metric}, datatype: {datatype}, alg: {alg}, {message} with error: {e}", depth=depth+1)

# Will populate the database with hashes doc_name_prefix<doc_id> containing a single vector field
# @param ret_vec_offset - return the i-th vector that is indexed.
def populate_with_vectors(env, num_docs, dim, datatype='FLOAT32', field_name=DEFAULT_FIELD_NAME, initial_doc_id=1, doc_name_prefix=DEFAULT_DOC_NAME_PREFIX, normalize=False, ret_vec_offset=0):
    conn = getConnectionByEnv(env)
    p = conn.pipeline(transaction=False)
    ret = None
    for i in range(num_docs):
        vector = create_random_np_array_typed(dim, datatype, normalize=normalize)
        if i == ret_vec_offset:
            ret = vector
        p.execute_command('HSET', f'{doc_name_prefix}{initial_doc_id + i}', field_name, vector.tobytes())
    p.execute()

    return ret

def set_up_database_with_vectors(env: Env, dim, num_docs, index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME, datatype='FLOAT32', metric='L2', alg='FLAT', additional_vec_params=None, additional_schema_args=None):
    create_vector_index(env, dim=dim, metric=metric,
                        index_name=index_name,
                        field_name=field_name,
                        datatype=datatype,
                        alg=alg,
                        additional_vec_params=additional_vec_params,
                        additional_schema_args=additional_schema_args)

    populate_with_vectors(env, num_docs, dim, datatype, field_name)

def get_tiered_debug_info(env, index_name, field_name) -> dict:
    return to_dict(env.execute_command(debug_cmd(), "VECSIM_INFO", index_name, field_name))

def get_tiered_frontend_debug_info(env, index_name, field_name) -> dict:
    tiered_index_info = get_tiered_debug_info(env, index_name, field_name)
    return to_dict(tiered_index_info['FRONTEND_INDEX'])

def get_tiered_backend_debug_info(env, index_name, field_name) -> dict:
    tiered_index_info = get_tiered_debug_info(env, index_name, field_name)
    return to_dict(tiered_index_info['BACKEND_INDEX'])

def get_vecsim_memory(env, index_key, field_name):
    # Returns the vector-related memory in MB for a SINGLE field: the per-index
    # allocator (MEMORY) plus the process-wide VecSim shared memory (SHARED_MEMORY,
    # e.g. the shared SVS thread pool).
    #
    # NOTE: SHARED_MEMORY is process-wide, so it is included on every call. Do not
    # sum this across multiple vector fields of the same index/process — that would
    # count the shared term once per field. Compare per single field only (matching
    # how FT.INFO vector_index_sz_mb folds the shared term in exactly once).
    info = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_key, field_name))
    total = float(info["MEMORY"]) + float(info.get("SHARED_MEMORY", 0))
    return total / 0x100000

def get_vecsim_index_size(env, index_key, field_name):
    return int(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_key, field_name))["INDEX_SIZE"])

def get_redisearch_vector_index_memory(env, index_key):
    return float(index_info(env, index_key)["vector_index_sz_mb"])

def wait_for_background_indexing(env, index_name, field_name, message=''):
    index_sizes = [0] * env.shardsCount
    flat_index_sizes = [0] * env.shardsCount
    backend_index_sizes = [0] * env.shardsCount
    iter = 0
    is_trained = [False] * env.shardsCount
    index_state = f"iter: {iter}, index_sizes: {index_sizes}, flat_index_sizes: {flat_index_sizes}, backend_index_sizes: {backend_index_sizes}, is_trained: {is_trained}"

    try:
        with TimeLimit(250):
            while not all(is_trained):
                # 'BACKGROUND_INDEXING' == 0 means training is done
                for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
                    tiered_info = get_tiered_debug_info(con, index_name, field_name)
                    is_trained[i] = tiered_info['BACKGROUND_INDEXING'] == 0
                    index_sizes[i] = tiered_info['INDEX_SIZE']
                    flat_index_sizes[i] = to_dict(tiered_info['FRONTEND_INDEX'])['INDEX_SIZE']
                    backend_index_sizes[i] = to_dict(tiered_info['BACKEND_INDEX'])['INDEX_SIZE']

                time.sleep(0.1)
                iter += 1
                index_state = f"iter: {iter}, index_sizes: {index_sizes}, flat_index_sizes: {flat_index_sizes}, backend_index_sizes: {backend_index_sizes}, is_trained: {is_trained}"
            # Drain workers to ensure all background job cleanup (including job object
            # deallocation from tracked memory) has completed before returning.
            for con in env.getOSSMasterNodesConnectionList():
                con.execute_command(debug_cmd(), 'WORKERS', 'DRAIN')
        for id, con in enumerate(env.getOSSMasterNodesConnectionList()):
            index_size = get_tiered_debug_info(con, index_name, field_name)['INDEX_SIZE']
            env.assertGreater(get_tiered_backend_debug_info(con, index_name, field_name)['INDEX_SIZE'], 0, message=f"wait_for_background_indexing: shard: {id}, index size: {index_size}" + message)
    except Exception as e:
        message = f"wait_for_background_indexing: {index_state}, {message})"
        raise Exception(f'Timeout: {message}')

# --- Tiered index transfers -------------------------------------------------------------------
# A tiered index buffers writes in a frontend (flat) index and moves them into the backend index
# in a background job - for SVS-VAMANA, a training one while the backend is still empty, and an
# update one afterwards. The helpers below assert the state around such a transfer, on a single
# shard, and default to the index and field the vecsim tests share.

def svs_backend_marked_deleted(env, index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME):
    return get_tiered_backend_debug_info(env, index_name, field_name)['NUMBER_OF_MARKED_DELETED']

def assert_transfer_pending(env, message='', index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME):
    """A transfer is scheduled and has not started yet. Call it with the workers paused - that is
    what makes it deterministic. BACKGROUND_INDEXING alone would not do, since it conflates
    "queued", "running" and "the job mutex happened to be held", hence the queued-job check
    beside it; that count is not transfer specific, so this assumes a single index and no fork GC.
    """
    info = get_tiered_debug_info(env, index_name, field_name)
    stats = getWorkersThpoolStats(env)
    env.assertEqual(info['BACKGROUND_INDEXING'], 1,
                    message=f"{message}: the index reports no pending update")
    env.assertGreater(stats['lowPriorityPendingJobs'], 0,
                      message=f"{message}: no queued job to be raced with: {stats}")

def assert_transfer_did_not_complete(env, jobs_done_before, message=''):
    """No background job finished since `jobs_done_before` was taken, i.e. the transfer running
    then was still running throughout the operations in between - the window its deletions
    journal exists for. Without this, deletions that merely followed a finished transfer would
    take the ordinary backend delete path and satisfy the same assertions.

    Only holds where those operations do not contend with the transfer for the main index lock: a
    training transfer holds it shared, so deletions proceed alongside it, while an update transfer
    holds it exclusively, so they block and complete as it finishes. BACKGROUND_INDEXING would not
    do instead - a transfer clears its scheduled flag when it starts, and writes made while its
    batch is still in the frontend schedule the *next* transfer."""
    env.assertEqual(workers_jobs_done(env), jobs_done_before,
                    message=f"{message}: a background job completed while the deletions were "
                            f"issued, so they did not all race a running transfer")

def assert_deletions_reached_the_backend(env, marked_deleted_before, message='',
                                         index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME):
    """At least one doc deleted while the transfer was running had already been picked up by it,
    so it reached the backend index and had to be deleted from there - which shows up as newly
    marked-deleted entries. This is the witness that the deletions really did interleave with the
    transfer: had they all landed before it snapshotted the frontend, the docs would never have
    reached the backend and every other assertion would still pass, silently losing the coverage.

    Stays specific only as long as no doc is overwritten around a transfer: re-adding a label the
    backend already holds also marks the old entry deleted."""
    env.assertGreater(svs_backend_marked_deleted(env, index_name, field_name), marked_deleted_before,
                      message=f"{message}: no deleted doc reached the backend index, so the "
                              f"deletions did not interleave with the transfer")

def assert_svs_tiered_state(env, expected_live_docs, vectors_per_doc=1, message='',
                            index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME):
    """Assert that a single shard's tiered index holds exactly the live docs' labels, and
    `vectors_per_doc` live vectors for each - deleting a doc deletes all of its vectors.

    Call only after `wait_for_background_indexing` and with periodic fork GC disabled: nothing may
    mutate the index while this runs, since the tiered info and its two sub-index infos are read
    under separate locks. The frontend is not expected to be empty - a remainder below the
    transfer threshold legitimately stays there.

    The label count comes from the tiered index, which reports the *deduplicated* union of both
    sub-indexes' labels; summing their own counts would double count a multi-value doc whose
    vectors a transfer split between them."""
    info = get_tiered_debug_info(env, index_name, field_name)
    frontend, backend = to_dict(info['FRONTEND_INDEX']), to_dict(info['BACKEND_INDEX'])
    # The backend's size and marked-deleted count come from one read, so their difference is
    # consistent even if a GC did slip in.
    live_vectors = frontend['INDEX_SIZE'] + backend['INDEX_SIZE'] - backend['NUMBER_OF_MARKED_DELETED']
    ctx = (f"{message} | labels={info['INDEX_LABEL_COUNT']} live_vectors={live_vectors} "
           f"frontend={frontend['INDEX_SIZE']} backend={backend['INDEX_SIZE']} "
           f"backend_marked_deleted={backend['NUMBER_OF_MARKED_DELETED']}")
    env.assertEqual(info['INDEX_LABEL_COUNT'], expected_live_docs,
                    message=f"indexed labels != live docs: {ctx}")
    env.assertEqual(live_vectors, vectors_per_doc * expected_live_docs,
                    message=f"live vectors != vectors_per_doc * live docs: {ctx}")

def knn_search(env, query_vec, k, index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME):
    return env.execute_command('FT.SEARCH', index_name, f'*=>[KNN {k} @{field_name} $vec]',
                               'PARAMS', 2, 'vec', query_vec.tobytes(), 'NOCONTENT', 'LIMIT', 0, k)

def assert_doc_indexed_under_own_vector(env, k, query_vec, expected_doc, message='', **kwargs):
    """`expected_doc` is returned for a KNN query with its own vector, i.e. it is still indexed
    under its own label. Containment in the top `k` rather than an exact top-1, so as not to also
    bet on the recall of a compressed, approximate backend."""
    res = knn_search(env, query_vec, k, **kwargs)
    env.assertContains(expected_doc, res,
                       message=f"{message}: {expected_doc} was not returned for its own vector: {res}")

def assert_deleted_docs_not_returned(env, k, query_vec, deleted_docs, message='', **kwargs):
    """A KNN query with a deleted doc's own vector - so that an index entry left behind for it
    ranks first - returns a full page of `k` live docs, none of them deleted. The result count is
    the end-to-end symptom of a leftover entry: the pipeline drops results whose doc is gone from
    the doc table, so such an entry costs a result slot rather than showing up by name.

    Weaker than `assert_svs_tiered_state`, since an approximate search need not rank the leftover
    into the page at all, and valid only once the index has settled: while a transfer is in flight
    a doc deleted from its batch is legitimately re-added to the backend, and only removed when
    the transfer applies its deletions journal."""
    res = knn_search(env, query_vec, k, **kwargs)
    env.assertEqual(res[0], k,
                    message=f"{message}: got {res[0]} of {k} results, so a deleted doc is "
                            f"probably still in the vector index, taking up a result slot: {res}")
    env.assertEqual(deleted_docs.intersection(res[1:]), set(),
                    message=f"{message}: deleted docs were returned: {res}")
