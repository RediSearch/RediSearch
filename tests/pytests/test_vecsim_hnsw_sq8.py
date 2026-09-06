# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from includes import *
from common import *


DEFAULT_TRAINING_THRESHOLD = 10 * 1024
MAX_TRAINING_THRESHOLD = 100 * 1024


def hnsw_params(data_type='FLOAT32', *extra):
    return [
        'TYPE', data_type,
        'DIM', 64,
        'DISTANCE_METRIC', 'L2',
        *extra,
    ]


def create_hnsw(env, index_name, params):
    env.expect(
        'FT.CREATE', index_name,
        'SCHEMA', 'v', 'VECTOR', 'HNSW', len(params), *params,
    ).ok()


def vector_field_info(env, index_name):
    return to_dict(index_info(env, index_name)['attributes'][0])


@skip(cluster=True)
def test_hnsw_sq8_create_validation_and_info(env):
    """Exercise HNSW SQ8 parsing, defaults, validation, and FT.INFO reporting."""
    create_hnsw(env, 'plain', hnsw_params())
    plain_info = vector_field_info(env, 'plain')
    env.assertEqual(plain_info['compression'], 'NO_COMPRESSION')
    env.assertFalse('training_threshold' in plain_info, message=plain_info)

    create_hnsw(env, 'default_threshold', hnsw_params('FLOAT32', 'COMPRESSION', 'SQ8'))
    default_info = vector_field_info(env, 'default_threshold')
    env.assertEqual(default_info['compression'], 'SQ8')
    env.assertEqual(default_info['training_threshold'], DEFAULT_TRAINING_THRESHOLD)

    # The threshold may precede COMPRESSION, and an explicit zero must not be replaced by the
    # default because it deliberately disables mean normalization.
    create_hnsw(
        env,
        'no_normalization',
        hnsw_params('FLOAT16', 'TRAINING_THRESHOLD', 0, 'COMPRESSION', 'sq8'),
    )
    no_normalization_info = vector_field_info(env, 'no_normalization')
    env.assertEqual(no_normalization_info['compression'], 'SQ8')
    env.assertEqual(no_normalization_info['training_threshold'], 0)

    float16_l2_with_normalization = hnsw_params('FLOAT16', 'COMPRESSION', 'SQ8')
    env.expect(
        'FT.CREATE', 'float16_l2_with_normalization',
        'SCHEMA', 'v', 'VECTOR', 'HNSW',
        len(float16_l2_with_normalization), *float16_l2_with_normalization,
    ).error().contains(
        'Mean normalization is not supported for FLOAT16 L2 compression'
    )

    create_hnsw(
        env,
        'float16_ip_with_normalization',
        [
            'TYPE', 'FLOAT16',
            'DIM', 64,
            'DISTANCE_METRIC', 'IP',
            'COMPRESSION', 'SQ8',
        ],
    )

    invalid_compression = hnsw_params('FLOAT32', 'COMPRESSION', 'SQ4')
    env.expect(
        'FT.CREATE', 'invalid_compression',
        'SCHEMA', 'v', 'VECTOR', 'HNSW', len(invalid_compression), *invalid_compression,
    ).error().contains('vector similarity HNSW index `COMPRESSION`')

    for data_type in ('FLOAT64', 'BFLOAT16', 'INT8', 'UINT8'):
        params = hnsw_params(data_type, 'COMPRESSION', 'SQ8')
        env.expect(
            'FT.CREATE', f'unsupported_{data_type}',
            'SCHEMA', 'v', 'VECTOR', 'HNSW', len(params), *params,
        ).error().contains('COMPRESSION is only supported for FLOAT32 and FLOAT16')

    threshold_without_compression = hnsw_params('FLOAT32', 'TRAINING_THRESHOLD', 1024)
    env.expect(
        'FT.CREATE', 'threshold_without_compression',
        'SCHEMA', 'v', 'VECTOR', 'HNSW',
        len(threshold_without_compression), *threshold_without_compression,
    ).error().contains('TRAINING_THRESHOLD is irrelevant when compression was not requested')

    excessive_threshold = hnsw_params(
        'FLOAT32', 'COMPRESSION', 'SQ8',
        'TRAINING_THRESHOLD', MAX_TRAINING_THRESHOLD + 1,
    )
    env.expect(
        'FT.CREATE', 'excessive_threshold',
        'SCHEMA', 'v', 'VECTOR', 'HNSW', len(excessive_threshold), *excessive_threshold,
    ).error().contains(f'TRAINING_THRESHOLD cannot exceed {MAX_TRAINING_THRESHOLD}')

    create_hnsw(
        env, 'maximum_threshold',
        hnsw_params('FLOAT32', 'COMPRESSION', 'SQ8',
                    'TRAINING_THRESHOLD', MAX_TRAINING_THRESHOLD),
    )
    env.assertEqual(vector_field_info(env, 'maximum_threshold')['training_threshold'],
                    MAX_TRAINING_THRESHOLD)

    for invalid_value in ('not-a-number', -1, '1.5', 2**64):
        params = hnsw_params(
            'FLOAT32', 'COMPRESSION', 'SQ8', 'TRAINING_THRESHOLD', invalid_value,
        )
        env.expect(
            'FT.CREATE', f'invalid_threshold_{invalid_value}',
            'SCHEMA', 'v', 'VECTOR', 'HNSW', len(params), *params,
        ).error().contains('vector similarity HNSW index `TRAINING_THRESHOLD`')


@skip(cluster=True)
def test_hnsw_sq8_params_survive_rdb_reload(env):
    """Verify the new HNSW SQ8 specification fields survive an RDB round trip."""
    create_hnsw(
        env,
        'idx',
        hnsw_params('FLOAT32', 'COMPRESSION', 'SQ8', 'TRAINING_THRESHOLD', 2048),
    )

    for _ in env.reloadingIterator():
        info = vector_field_info(env, 'idx')
        env.assertEqual(info['compression'], 'SQ8')
        env.assertEqual(info['training_threshold'], 2048)


def sq8_vector(value, data_type='FLOAT32'):
    return create_np_array_typed([value] + [1] * 63, data_type).tobytes()


def assert_sq8_documents(env, ids, data_type='FLOAT32'):
    result = env.cmd(
        'FT.SEARCH', 'idx', '*=>[KNN 10 @v $q]', 'PARAMS', 2,
        'q', sq8_vector(1, data_type), 'NOCONTENT', 'DIALECT', 2,
    )
    env.assertEqual([result[0], *sorted(result[1:])], [len(ids), *sorted(ids)])


def assert_sq8_storage(env, frontend_size, backend_size=None):
    info = get_vecsim_debug_dict(env, 'idx', 'v')
    env.assertEqual(to_dict(info['FRONTEND_INDEX'])['INDEX_SIZE'], frontend_size,
                    message=info)
    if backend_size is None:
        env.assertFalse('BACKEND_INDEX' in info, message=info)
    else:
        env.assertEqual(to_dict(info['BACKEND_INDEX'])['INDEX_SIZE'], backend_size,
                        message=info)


@skip(cluster=True)
def test_hnsw_sq8_reload_during_accumulation():
    """Rebuild a partially trained index, then cross the threshold with new writes."""
    # Workers are required to drain the migration jobs created at the SQ8 transition.
    env = Env(moduleArgs='WORKERS 2')
    create_hnsw(env, 'idx', hnsw_params(
        'FLOAT32', 'COMPRESSION', 'SQ8', 'TRAINING_THRESHOLD', 4))
    conn = getConnectionByEnv(env)
    for i in range(3):
        conn.execute_command('HSET', f'doc{i}', 'v', sq8_vector(i + 1))
    conn.execute_command('HSET', 'doc0', 'v', sq8_vector(5))
    conn.execute_command('DEL', 'doc2')

    for _ in env.reloadingIterator():
        assert_sq8_storage(env, 2)
        assert_sq8_documents(env, ['doc0', 'doc1'])
        env.assertEqual(vector_field_info(env, 'idx')['training_threshold'], 4)

    for i in (2, 3):
        conn.execute_command('HSET', f'doc{i}', 'v', sq8_vector(i + 1))
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
    assert_sq8_storage(env, 0, 4)
    assert_sq8_documents(env, ['doc0', 'doc1', 'doc2', 'doc3'])


@skip(cluster=True)
def test_hnsw_sq8_reload_after_training():
    """Rebuild populated SQ8 indexes for every supported type/metric combination."""
    env = Env(moduleArgs='WORKERS 2')
    conn = getConnectionByEnv(env)
    for data_type in ('FLOAT32', 'FLOAT16'):
        for metric in ('L2', 'IP', 'COSINE'):
            # VecSim supports FLOAT16 L2 only without mean normalization.
            threshold = 0 if (data_type, metric) == ('FLOAT16', 'L2') else 4
            create_hnsw(env, 'idx', [
                'TYPE', data_type, 'DIM', 64, 'DISTANCE_METRIC', metric,
                'COMPRESSION', 'SQ8', 'TRAINING_THRESHOLD', threshold,
            ])
            for i in range(4):
                conn.execute_command('HSET', f'doc{i}', 'v', sq8_vector(i + 1, data_type))

            for _ in env.reloadingIterator():
                env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
                assert_sq8_storage(env, 0, 4)
                assert_sq8_documents(env, ['doc0', 'doc1', 'doc2', 'doc3'], data_type)
                info = vector_field_info(env, 'idx')
                env.assertEqual(info['compression'], 'SQ8')
                env.assertEqual(info['training_threshold'], threshold)
            env.expect('FT.DROPINDEX', 'idx', 'DD').ok()


@skip(cluster=True)
def test_hnsw_sq8_reload_retrains_after_deletions():
    """A trained index below the threshold returns to accumulation on rebuild."""
    env = Env(moduleArgs='WORKERS 2')
    create_hnsw(env, 'idx', hnsw_params(
        'FLOAT32', 'COMPRESSION', 'SQ8', 'TRAINING_THRESHOLD', 4))
    conn = getConnectionByEnv(env)
    for i in range(4):
        conn.execute_command('HSET', f'doc{i}', 'v', sq8_vector(i + 1))
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
    assert_sq8_storage(env, 0, 4)
    conn.execute_command('DEL', 'doc2', 'doc3')
    assert_sq8_documents(env, ['doc0', 'doc1'])

    env.dumpAndReload()
    assert_sq8_storage(env, 2)
    assert_sq8_documents(env, ['doc0', 'doc1'])
    for i in (2, 3):
        conn.execute_command('HSET', f'doc{i}', 'v', sq8_vector(i + 1))
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
    assert_sq8_storage(env, 0, 4)
    assert_sq8_documents(env, ['doc0', 'doc1', 'doc2', 'doc3'])
