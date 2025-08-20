from RLTest import Env
from includes import *
from common import *

def setup_hybrid_tag_scoring_index(env):
    """Setup index and populate test data"""
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'title', 'TEXT',
               'category', 'TAG',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3', 'DISTANCE_METRIC', 'COSINE').ok()

    env.cmd('HSET', 'doc:1',
               'title', 'hello world',
               'category', 'hello')
    env.cmd('HSET', 'doc:2',
               'title', 'hello world',
               'category', 'world')
    env.cmd('HSET', 'doc:3',
               'title', 'hello world',
               'category', 'hello,world')
    env.cmd('HSET', 'doc:4',
               'title', 'foo bar',
               'category', 'foo')

def run_test_scenario(env, no_tag_search_query, with_tag_search_query):
    """Test hybrid tag scoring for a specific query scenario"""
    # Hybrid searches
    hybrid_res_no_tag = env.cmd('FT.HYBRID', 'idx', 'SEARCH', no_tag_search_query, 'VSIM', '@vector', 'BEUGBwgJCg==', 'COMBINE', 'LINEAR', '1.0', '0.0')
    hybrid_res_with_tag = env.cmd('FT.HYBRID', 'idx', 'SEARCH', with_tag_search_query, 'VSIM', '@vector', 'BEUGBwgJCg==', 'COMBINE', 'LINEAR', '1.0', '0.0')

    # Extract scores from both hybrid results
    env.assertTrue(recursive_contains(hybrid_res_no_tag, '__score'))
    env.assertTrue(recursive_contains(hybrid_res_with_tag, '__score'))

    score_index = recursive_index(hybrid_res_no_tag, '__score')
    score_index[-1] += 1

    score_no_tag = float(access_nested_list(hybrid_res_no_tag, score_index))
    score_with_tag = float(access_nested_list(hybrid_res_with_tag, score_index))

    # Assert scores are equal (tag scoring disabled for hybrid)
    env.assertAlmostEqual(score_no_tag, score_with_tag, delta=1E-6)

    # Compare with regular search
    search_res = env.cmd('FT.SEARCH', 'idx', no_tag_search_query, 'WITHSCORES')
    search_score = float(search_res[2])
    env.assertAlmostEqual(search_score, score_no_tag, delta=1E-6)

#TODO: remove once FY.HYBRID for cluster is NotImplemented
@skip(cluster=True)
def testHybridTagScoring(env):
    """Test hybrid tag scoring with various query scenarios"""
    setup_hybrid_tag_scoring_index(env)

    # Test scenarios: (no_tag_search_query, with_tag_search_query)
    scenarios = [
        ('hello', 'hello @category:{hello}'),
        ('hello world', 'hello world @category:{world}'),
        ('hello', 'hello @category:{hello|world}'),
        ('hello', 'hello ~@category:{foo}')
    ]

    for no_tag_query, with_tag_query in scenarios:
        run_test_scenario(env, no_tag_query, with_tag_query)


