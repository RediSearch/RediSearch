from RLTest import Env
from includes import *
from common import *

def testHybridDisablesTagScoring(env):
    """
    Test that FT.HYBRID disables tag scoring.
    """

    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'title', 'TEXT',
               'category', 'TAG',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3', 'DISTANCE_METRIC', 'COSINE').ok()

    env.cmd('HSET', 'doc:1',
               'title', 'hello world',
               'category', 'hello',
               'vector', 'BEUGBwgJCg==')

    hybrid_res_no_tag = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'hello @category:{hello}',
                        'VSIM', '@vector', 'BEUGBwgJCg==', 'COMBINE', 'LINEAR', '1.0', '0.0')
    hybrid_res_with_tag = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'hello @category:{hello}',
                         'VSIM', '@vector', 'BEUGBwgJCg==', 'COMBINE', 'LINEAR', '1.0', '0.0')

    # Extract scores from both hybrid results
    env.assertTrue(recursive_contains(hybrid_res_no_tag, '__score'))
    score_index = recursive_index(hybrid_res_no_tag, '__score')
    score_index[-1] += 1

    score_no_tag = float(access_nested_list(hybrid_res_no_tag, score_index))
    score_with_tag = float(access_nested_list(hybrid_res_with_tag, score_index))

    # Assert scores are equal (tag scoring disabled for hybrid)
    env.assertEqual(score_no_tag, score_with_tag)

    # To compare, in FT.SEARCH:
    # ft.search idx 'hello @category:{hello}' WITHSCORES
    # ft.search idx 'hello'
    # will have different scores

