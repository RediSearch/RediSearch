import numpy as np
from common import waitForIndex, skip, config_cmd, ANY

MAX_KNN_K = (1 << 58)
MIN_KNN_K = 1

def create_hybrid_index(env):
    env.cmd('FT.CREATE', 'idx', 'SCORE_FIELD', 'my_score',
            'SCHEMA',
            'text', 'TEXT',
            'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
                'DISTANCE_METRIC', 'L2',
            )

    vec1 = np.array([0.1, 0.1], dtype=np.float32).tobytes()
    env.cmd('HSET', 'doc1', 'text', 'hello', 'vec', vec1)
    waitForIndex(env, 'idx')
    return vec1

@skip(cluster=True)
def test_hybrid_rrf_extreme_constant(env):
    vec1 = create_hybrid_index(env)

    HYBRID_MIN_RRF_CONSTANT = 0.0
    HYBRID_MAX_RRF_CONSTANT = 100000.0
    # A value inside the limit
    res1 = env.cmd('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   'COMBINE', 'RRF', '2', 'CONSTANT', HYBRID_MAX_RRF_CONSTANT,
                   'PARAMS', '2', 'v', vec1)
    env.assertEqual(res1[1], 1)

    # A value inside the limit (scientific notation)
    res1 = env.cmd('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   'COMBINE', 'RRF', '2', 'CONSTANT', '1e2',
                   'PARAMS', '2', 'v', vec1)
    env.assertEqual(res1[1], 1)

    # A value above the limit, should throw an error
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', '*',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '2', 'CONSTANT', str(HYBRID_MAX_RRF_CONSTANT + 0.1),
               'PARAMS', '2', 'v', vec1).error().contains('Value above maximum')

    # A value above the limit (scientific notation), should throw an error
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', '*',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '2', 'CONSTANT', '1e6',
               'PARAMS', '2', 'v', vec1).error().contains('Value above maximum')

    # Zero is the minimum and must be accepted: with rank >= 1, the RRF
    # denominator (constant + rank) is still >= 1 so there is no div-by-zero.
    res1 = env.cmd('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   'COMBINE', 'RRF', '2', 'CONSTANT', '0',
                   'PARAMS', '2', 'v', vec1)
    env.assertEqual(res1[1], 1)

    # A negative value, should throw an error
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', '*',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '2', 'CONSTANT', '-0.1',
               'PARAMS', '2', 'v', vec1).error().contains('Value below minimum')

    # A value below the limit (scientific notation), should throw an error
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', '*',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '2', 'CONSTANT', '-1e1',
               'PARAMS', '2', 'v', vec1).error().contains('Value below minimum')

    # NaN must be rejected: comparisons against NaN are always false, so a naive
    # range check (val < min || val > max) lets NaN through and propagates it
    # into RRF scoring. Cover the common spellings strtod() accepts.
    for bad in ('nan', 'NaN', '-nan', 'nan(0)'):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', '*',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   'COMBINE', 'RRF', '2', 'CONSTANT', bad,
                   'PARAMS', '2', 'v', vec1).error()

    # ±inf must be rejected as well; strtod() parses these to ±HUGE_VAL, which
    # AC_GetDouble already rejects, but assert it here so the contract is pinned.
    for bad in ('inf', '-inf', 'infinity', '-infinity'):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', '*',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   'COMBINE', 'RRF', '2', 'CONSTANT', bad,
                   'PARAMS', '2', 'v', vec1).error()


@skip(cluster=True)
def test_hybrid_knn_k_bounds(env):
    vec1 = create_hybrid_index(env)

    # Valid K
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', 'hello',
                  'VSIM', '@vec', '$v', 'KNN', '2', 'K', str(MAX_KNN_K),
                  'PARAMS', '2', 'v', vec1)
    env.assertEqual(res[1], 1)

    # Valid K (scientific notation)
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', 'hello',
                  'VSIM', '@vec', '$v', 'KNN', '2', 'K', '1e3',
                  'PARAMS', '2', 'v', vec1)
    env.assertEqual(res[1], 1)

    # Invalid K (negative)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '-1',
               'PARAMS', '2', 'v', vec1).error().contains('Invalid K value')

    # Invalid K (below MIN_KNN_K)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', str(MIN_KNN_K - 1),
               'PARAMS', '2', 'v', vec1).error().contains('Invalid K value')

    # Invalid K (above MAX_KNN_K)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', str(MAX_KNN_K + 1),
               'COMBINE', 'LINEAR', '4', 'ALPHA', '0.5', 'BETA', '0.5',
               'PARAMS', '2', 'v', vec1).error().contains('KNN K parameter is too large')

    # Invalid K (scientific notation)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '1e19',
               'COMBINE', 'LINEAR', '4', 'ALPHA', '0.5', 'BETA', '0.5',
               'PARAMS', '2', 'v', vec1).error().contains('Invalid K value')

@skip(cluster=True)
def test_hybrid_window_bounds_in_linear(env):
    vec1 = create_hybrid_index(env)

    # Valid WINDOW in LINEAR
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', 'hello',
                  'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                  'COMBINE', 'LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5',
                  'WINDOW', '100',
                  'PARAMS', '2', 'v', vec1)
    env.assertEqual(res[1], 1)

    # Invalid WINDOW in LINEAR (negative)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5',
               'WINDOW', '-1',
               'PARAMS', '2', 'v', vec1).error().contains('Value is outside acceptable bounds')

    # Invalid WINDOW in LINEAR (below 1)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5',
               'WINDOW', '0',
               'PARAMS', '2', 'v', vec1).error().contains('Value below minimum')

    # Invalid WINDOW in LINEAR (above MAX_KNN_K)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5',
               'WINDOW', str(MAX_KNN_K + 1),
               'PARAMS', '2', 'v', vec1).error().contains('Value above maximum')

    # Invalid WINDOW in LINEAR (scientific notation)
    err_msg = 'Could not convert argument to expected type'
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5',
               'WINDOW', '1e19',
               'PARAMS', '2', 'v', vec1).error().contains(err_msg)

    # Invalid WINDOW in LINEAR (non-numeric / nan / inf). WINDOW is parsed as
    # an integer, so none of these spellings should be accepted.
    for bad in ('bad', 'nan', 'NaN', 'inf', '-inf', '1.5'):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   'COMBINE', 'LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5',
                   'WINDOW', bad,
                   'PARAMS', '2', 'v', vec1).error()

@skip(cluster=True)
def test_hybrid_window_bounds_in_rrf(env):
    vec1 = create_hybrid_index(env)

    # Valid WINDOW in RRF
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', 'hello',
                  'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                  'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', '100',
                  'PARAMS', '2', 'v', vec1)
    env.assertEqual(res[1], 1)

    # Invalid WINDOW in RRF (negative)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', '-1',
               'PARAMS', '2', 'v', vec1).error().contains('Value is outside acceptable bounds')

    # Invalid WINDOW in RRF (below 1)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', '0',
               'PARAMS', '2', 'v', vec1).error().contains('Value below minimum')

    # Invalid WINDOW in RRF (above MAX_KNN_K)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '4', 'CONSTANT', '60',
               'WINDOW', str(MAX_KNN_K + 1),
               'PARAMS', '2', 'v', vec1).error().contains('Value above maximum')

    # Invalid WINDOW in RRF (scientific notation)
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'hello',
               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
               'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', '1e19',
               'PARAMS', '2', 'v', vec1).error().contains('Could not convert argument to expected type')

    # Invalid WINDOW in RRF (non-numeric / nan / inf). WINDOW is parsed as
    # an integer, so none of these spellings should be accepted.
    for bad in ('bad', 'nan', 'NaN', 'inf', '-inf', '1.5'):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', bad,
                   'PARAMS', '2', 'v', vec1).error()


@skip(cluster=True)
def test_hybrid_default_window_respects_maxsearchresults(env):
    create_hybrid_index(env)
    for i in range(8):
        v = np.array([0.1 * i, 0.1 * i], dtype=np.float32).tobytes()
        env.cmd('HSET', f'doc{i}', 'text', 'hello', 'vec', v)
    waitForIndex(env, 'idx')
    qvec = np.array([0.0, 0.0], dtype=np.float32).tobytes()

    env.expect(config_cmd(), 'SET', 'MAXSEARCHRESULTS', '3').ok()

    # An explicit WINDOW above the configured cap must be rejected. This
    # pins down that maxWindow == 3 in this configuration.
    for combine in (
        ('RRF', '4', 'CONSTANT', '60', 'WINDOW', '10'),
        ('LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5', 'WINDOW', '10'),
    ):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '8',
                   'COMBINE', *combine,
                   'PARAMS', '2', 'v', qvec).error().contains('Value above maximum')

    # Omitting WINDOW (while still providing some combine sub-args) must
    # succeed AND must be equivalent to explicit WINDOW=3.
    for default_combine, explicit_combine in (
        (
            ('RRF', '2', 'CONSTANT', '60'),
            ('RRF', '4', 'CONSTANT', '60', 'WINDOW', '3'),
        ),
        (
            ('LINEAR', '4', 'ALPHA', '0.5', 'BETA', '0.5'),
            ('LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5', 'WINDOW', '3'),
        ),
    ):
        res_default = env.cmd('FT.HYBRID', 'idx',
                              'SEARCH', 'hello',
                              'VSIM', '@vec', '$v', 'KNN', '2', 'K', '8',
                              'COMBINE', *default_combine,
                              'PARAMS', '2', 'v', qvec)
        res_explicit = env.cmd('FT.HYBRID', 'idx',
                               'SEARCH', 'hello',
                               'VSIM', '@vec', '$v', 'KNN', '2', 'K', '8',
                               'COMBINE', *explicit_combine,
                               'PARAMS', '2', 'v', qvec)
        # Compare total_results + results; skip execution_time
        env.assertEqual(res_default[:4], res_explicit[:4])


@skip(cluster=True)
def test_hybrid_combine_accepts_zero_maxsearchresults(env):
    vec1 = create_hybrid_index(env)

    env.expect(config_cmd(), 'SET', 'MAXSEARCHRESULTS', '0').ok()
    expected = ['total_results', 0, 'results', [], 'warnings', [], 'execution_time', ANY]

    for combine in (
        (''),  # No COMBINE clause, default RRF
        ('COMBINE', 'RRF', '2', 'CONSTANT', '60'),
        ('COMBINE', 'LINEAR', '4', 'ALPHA', '0.5', 'BETA', '0.5'),
    ):
        res = env.cmd('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   *combine,
                   'PARAMS', '2', 'v', vec1)
        env.assertEqual(res, expected, message='MAXSEARCHRESULTS=0 -> no rows')

    for combine in (
        ('COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', '1'),
        ('COMBINE', 'LINEAR', '6', 'ALPHA', '0.5', 'BETA', '0.5', 'WINDOW', '1'),
    ):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', 'hello',
                   'VSIM', '@vec', '$v', 'KNN', '2', 'K', '10',
                   *combine,
                   'PARAMS', '2', 'v', vec1).error().contains('WINDOW: Value above maximum')
