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

    for invalid_value in ('not-a-number', -1):
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
