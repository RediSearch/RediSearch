from RLTest import Env
from includes import *
from common import *

SCORE_FIELD = "__score"

def setup_hybrid_tag_scoring_index(env):
    """Setup index and populate test data"""
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
               'title', 'TEXT',
               'category', 'TAG',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3', 'DISTANCE_METRIC', 'COSINE')

    conn.execute_command('HSET', 'doc:1',
               'title', 'hello world',
               'category', 'hello')
    conn.execute_command('HSET', 'doc:2',
               'title', 'hello world',
               'category', 'world')
    conn.execute_command('HSET', 'doc:3',
               'title', 'hello world',
               'category', 'hello,world')
    conn.execute_command('HSET', 'doc:4',
               'title', 'foo bar',
               'category', 'foo')

def run_test_scenario(env, no_tag_search_query, with_tag_search_query):
    """Test hybrid tag scoring for a specific query scenario"""
    # Hybrid searches
    hybrid_res_no_tag = env.cmd('FT.HYBRID', 'idx', 'SEARCH', no_tag_search_query, 'VSIM', '@vector', 'BEUGBwgJCg==', 'COMBINE', 'LINEAR', '4', 'ALPHA', '1.0', 'BETA', '0.0')
    hybrid_res_with_tag = env.cmd('FT.HYBRID', 'idx', 'SEARCH', with_tag_search_query, 'VSIM', '@vector', 'BEUGBwgJCg==', 'COMBINE', 'LINEAR', '4', 'ALPHA', '1.0', 'BETA', '0.0')
    hybrid_res_results_index = recursive_index(hybrid_res_no_tag, 'results')
    hybrid_res_results_index[-1] += 1

    results_no_tag, _ = get_results_from_hybrid_response(hybrid_res_no_tag)
    results_with_tag, _ = get_results_from_hybrid_response(hybrid_res_with_tag)
    shared_keys = results_no_tag.keys() & results_with_tag.keys()
    for key in shared_keys:
        score_no_tag = float(results_no_tag[key][SCORE_FIELD])
        score_with_tag = float(results_with_tag[key][SCORE_FIELD])
        env.assertAlmostEqual(score_with_tag, score_no_tag, delta=1E-6)

        # Compare with regular search
        search_res = env.cmd('FT.SEARCH', 'idx', no_tag_search_query, 'WITHSCORES')
        search_res_key = search_res[1]

        search_score = float(search_res[2])
        env.assertAlmostEqual(search_score, score_no_tag, delta=1E-6)


# TODO: remove once FY.HYBRID for cluster is implemented
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
        '''
    Tag filtering affects scoring in FT.SEARCH/FT.AGGREGATE commands.
    When tag constraints are added to a query, the scoring algorithm produces different results.

    Example:
        FT.SEARCH idx hello WITHSCORES -> doc:1 scores 0.35667496778059
        FT.SEARCH idx hello @category:{hello} WITHSCORES -> doc:1 scores 1.0498221483405352

    The hybrid search should maintain consistent scoring behavior regardless of tag filtering.
    '''
