# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from includes import *
from common import *


DEFAULT_TRAINING_THRESHOLD = 10 * 1024


def tq_params(compression, *extra, data_type='FLOAT32', dim=64, metric='IP'):
    return ['TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', metric,
            'COMPRESSION', compression, *extra]


def create_hnsw(env, index_name, params):
    env.expect('FT.CREATE', index_name,
               'SCHEMA', 'v', 'VECTOR', 'HNSW', len(params), *params).ok()


def expect_create_error(env, index_name, params, message):
    env.expect('FT.CREATE', index_name,
               'SCHEMA', 'v', 'VECTOR', 'HNSW', len(params), *params).error().contains(message)


def vector_field_info(env, index_name):
    return to_dict(index_info(env, index_name)['attributes'][0])


@skip(cluster=True)
def test_hnsw_tq_create_validation_and_info(env):
    """Parse TQ2/TQ3, report them in FT.INFO, and reject unsupported configurations."""
    for compression in ('TQ2', 'TQ3'):
        for metric in ('IP', 'COSINE'):
            name = f'{compression}_{metric}'
            create_hnsw(env, name, tq_params(compression.lower(), metric=metric))
            info = vector_field_info(env, name)
            env.assertEqual(info['compression'], compression, message=info)
            env.assertEqual(info['training_threshold'], DEFAULT_TRAINING_THRESHOLD,
                            message=info)
        # An explicit zero threshold disables mean training, as for SQ8.
        create_hnsw(env, f'{compression}_no_training',
                    tq_params(compression, 'TRAINING_THRESHOLD', 0, dim=768))
        env.assertEqual(vector_field_info(env, f'{compression}_no_training')['training_threshold'],
                        0)

        expect_create_error(env, 'float16', tq_params(compression, data_type='FLOAT16'),
                            'COMPRESSION TQ2 and TQ3 are only supported for FLOAT32 vectors')
        expect_create_error(env, 'float64', tq_params(compression, data_type='FLOAT64'),
                            'COMPRESSION is only supported for FLOAT32 and FLOAT16')
        expect_create_error(env, 'l2', tq_params(compression, metric='L2'),
                            'COMPRESSION TQ2 and TQ3 support only the IP and COSINE')
        for dim in (32, 96, 100):
            expect_create_error(env, f'dim{dim}', tq_params(compression, dim=dim),
                                'COMPRESSION TQ2 and TQ3 require DIM to be a multiple of 64')

    for unknown in ('TQ', 'TQ4', 'TQ23'):
        expect_create_error(env, f'unknown_{unknown}', tq_params(unknown),
                            'vector similarity HNSW index `COMPRESSION`')


@skip(cluster=True)
def test_hnsw_tq_params_survive_rdb_reload(env):
    """The TQ compression type and training threshold survive an RDB round trip."""
    create_hnsw(env, 'tq2', tq_params('TQ2', 'TRAINING_THRESHOLD', 2048))
    create_hnsw(env, 'tq3', tq_params('TQ3', metric='COSINE'))
    for _ in env.reloadingIterator():
        info2, info3 = vector_field_info(env, 'tq2'), vector_field_info(env, 'tq3')
        env.assertEqual((info2['compression'], info2['training_threshold']), ('TQ2', 2048))
        env.assertEqual((info3['compression'], info3['training_threshold']),
                        ('TQ3', DEFAULT_TRAINING_THRESHOLD))


@skip(cluster=True)
def test_hnsw_tq_query_scores_across_training_and_reload():
    """KNN results and distances stay close to exact through training, migration and reload."""
    env = Env(moduleArgs='WORKERS 2')
    conn = getConnectionByEnv(env)
    dim, count, k = 64, 200, 10
    rng = np.random.default_rng(42)
    vectors = rng.standard_normal((count, dim)).astype(np.float32) + 0.5
    queries = rng.standard_normal((5, dim)).astype(np.float32) + 0.5
    for compression in ('TQ2', 'TQ3'):
        for metric in ('IP', 'COSINE'):
            for threshold in (0, 100):
                data = vectors.copy()
                if metric == 'IP':
                    # TQ estimates 1 - IP for unit vectors, as RediSearch's IP does.
                    data /= np.linalg.norm(data, axis=1, keepdims=True)
                # A wide EF_RUNTIME keeps the graph search near exhaustive on 200 vectors.
                params = tq_params(compression, 'TRAINING_THRESHOLD', threshold,
                                   'EF_RUNTIME', count, dim=dim, metric=metric)
                create_hnsw(env, 'idx', params)
                for i, vector in enumerate(data):
                    conn.execute_command('HSET', f'doc{i}', 'v', vector.tobytes())
                unit = data / np.linalg.norm(data, axis=1, keepdims=True)

                def check_queries(stage):
                    for query in queries:
                        q = query / np.linalg.norm(query) if metric == 'IP' else query
                        exact = 1 - (unit @ (q / np.linalg.norm(q)) if metric == 'COSINE'
                                     else data @ q)
                        result = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN {k} @v $q AS dist]',
                                         'PARAMS', 2, 'q', q.astype(np.float32).tobytes(),
                                         'SORTBY', 'dist', 'RETURN', 1, 'dist', 'DIALECT', 2)
                        context = (compression, metric, threshold, stage, result)
                        env.assertEqual(result[0], k, message=context)
                        ids = [int(key[3:]) for key in result[1::2]]
                        # The exact nearest neighbor is always among the compressed top k.
                        env.assertTrue(int(np.argmin(exact)) in ids, message=context)
                        # Loose bounds for 2/3-bit codes at dim 64; dim 768 errors are ~10x smaller.
                        tolerance = 0.2 if compression == 'TQ2' else 0.12
                        for doc, fields in zip(ids, result[2::2]):
                            env.assertLess(abs(float(fields[1]) - exact[doc]), tolerance,
                                           message=context)

                env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
                check_queries('live')
                backend = to_dict(get_vecsim_debug_dict(env, 'idx', 'v')['BACKEND_INDEX'])
                env.assertEqual(backend['INDEX_SIZE'], count, message=backend)
                for _ in env.reloadingIterator():
                    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
                    check_queries('reload')
                env.expect('FT.DROPINDEX', 'idx', 'DD').ok()
