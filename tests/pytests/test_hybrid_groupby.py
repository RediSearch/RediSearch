from RLTest import Env
from includes import *
from common import *
import numpy as np
from collections import Counter

"""
FT.HYBRID GROUPBY FLOW TESTS:
===============================

VECTOR SPACE LAYOUT:
===================
Query vector at origin [0, 0]

doc:1, doc:2 and doc:3 are at distance 1 from the query vector [0,0]
doc:4, doc:5 and doc:6 are at distance 2 from the query vector [0,0]
doc:7, doc:8 and doc:9 are at distance 3 from the query vector [0,0]

IMPORTANT: to save calculations, redis stores only the squared distance in the vector index,
therefore we square the radius and numpy l2 norm to get the squared distance

This allows controlling result set sizes with RADIUS:
- RADIUS 1**2 → 3 results (distance 1)
- RADIUS 2**2 → 6 results (distances 1-2)
- RADIUS 3**2 → 9 results (distances 1-3)
"""

def setup_hybrid_groupby_index(env):
    """Setup index with documents at controlled distances for precise result control"""
    conn = env.getClusterConnectionIfNeeded()

    # Create index with text, category, and vector fields
    env.expect('FT.CREATE idx SCHEMA description TEXT category TAG embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok()

    # Place documents at specific L2 distances from query vector [0,0]
    test_docs = {
        # Distance 1 (3 docs)
        'doc:1': {'description': 'red sports car', 'category': 'automotive', 'embedding': np.array([1.0, 0.0]).astype(np.float32).tobytes()},
        'doc:2': {'description': 'red racing vehicle', 'category': 'automotive', 'embedding': np.array([0.0, 1.0]).astype(np.float32).tobytes()},
        'doc:3': {'description': 'red dress shirt', 'category': 'clothing', 'embedding': np.array([-1.0, 0.0]).astype(np.float32).tobytes()},

        # Distance 2 (3 docs)
        'doc:4': {'description': 'red winter jacket', 'category': 'clothing', 'embedding': np.array([2.0, 0.0]).astype(np.float32).tobytes()},
        'doc:5': {'description': 'red leather shoes', 'category': 'footwear', 'embedding': np.array([0.0, 2.0]).astype(np.float32).tobytes()},
        'doc:6': {'description': 'red running sneakers', 'category': 'footwear', 'embedding': np.array([-2.0, 0.0]).astype(np.float32).tobytes()},

        # Distance 3 (3 docs)
        'doc:7': {'description': 'red apple fruit', 'category': 'food', 'embedding': np.array([3.0, 0.0]).astype(np.float32).tobytes()},
        'doc:8': {'description': 'red cherry tomato', 'category': 'food', 'embedding': np.array([0.0, 3.0]).astype(np.float32).tobytes()},
        'doc:9': {'description': 'red exercise equipment', 'category': 'fitness', 'embedding': np.array([-3.0, 0.0]).astype(np.float32).tobytes()},
    }

    for doc_id, doc_data in test_docs.items():
        conn.execute_command('HSET', doc_id,
                           'description', doc_data['description'],
                           'category', doc_data['category'],
                           'embedding', doc_data['embedding'])

    return test_docs

def parse_hybrid_groupby_response(response) -> Dict[str, int]:
    res_results_index = recursive_index(response, 'results')
    res_results_index[-1] += 1
    results = access_nested_list(response, res_results_index)

    parsed_results = {}
    for item in results:
        # Find the category field and get its value
        category_index = recursive_index(item, 'category')
        category_index[-1] += 1  # Move to the value after 'category'
        category_value = access_nested_list(item, category_index)

        # Find the count field and get its value
        count_index = recursive_index(item, 'count')
        count_index[-1] += 1  # Move to the value after 'count'
        count_value = int(access_nested_list(item, count_index))

        parsed_results[category_value] = count_value

    return parsed_results

def l2_from_bytes(a_bytes, b_bytes) -> float:
    a = np.frombuffer(a_bytes, dtype=np.float32)
    b = np.frombuffer(b_bytes, dtype=np.float32)
    return np.linalg.norm(a - b)

@skip(cluster=True)
def test_hybrid_groupby_total_group_limit():
    env = Env(protocol=3, moduleArgs='MAX_AGGREGATE_GROUPS 2')

    env.expect('FT.CREATE', 'idx',
               'SCHEMA',
               'description', 'TEXT',
               'embedding', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

    conn = getConnectionByEnv(env)
    for i in range(3):
        vector = np.array([float(i + 1), 0.0], dtype=np.float32).tobytes()
        conn.execute_command('HSET', f'doc:{i}',
                             'description', 'red',
                             'embedding', vector)

    query_vector = np.array([0.0, 0.0], dtype=np.float32).tobytes()
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'nomatch',
               'VSIM', '@embedding', '$BLOB',
                   'RANGE', '2', 'RADIUS', '16',
               'GROUPBY', '1', '@__key',
                   'REDUCE', 'COUNT', '0', 'AS', 'count',
               'PARAMS', '2', 'BLOB', query_vector).error() \
        .contains('MAX_AGGREGATE_GROUPS') \
        .contains('2')

@skip(cluster=False)
def test_hybrid_groupby_coordinator_group_limit():
    env = Env(shardsCount=3, protocol=3, moduleArgs='MAX_AGGREGATE_GROUPS 2')

    env.expect('FT.CREATE', 'idx',
               'SCHEMA',
               'description', 'TEXT',
               'embedding', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

    conn = getConnectionByEnv(env)
    shard_tags = ['shard:0', 'shard:1', 'shard:3']
    for i, shard_tag in enumerate(shard_tags):
        vector = np.array([float(i + 1), 0.0], dtype=np.float32).tobytes()
        conn.execute_command('HSET', f'doc:{i}{{{shard_tag}}}',
                             'description', 'red',
                             'embedding', vector)

    query_vector = np.array([0.0, 0.0], dtype=np.float32).tobytes()
    res = env.cmd('FT.HYBRID', 'idx',
                  'SEARCH', 'nomatch',
                  'VSIM', '@embedding', '$BLOB',
                      'RANGE', '2', 'RADIUS', '16',
                  'GROUPBY', '1', '@__key',
                      'REDUCE', 'COUNT', '0', 'AS', 'count',
                  'PARAMS', '2', 'BLOB', query_vector)

    warnings = res.get('warnings', [])
    env.assertTrue(len(warnings) > 0, message=f'Expected GROUPBY limit warning, got: {res}')
    warning = warnings[0]
    env.assertContains('MAX_AGGREGATE_GROUPS', warning)
    env.assertContains('2', warning)
    env.assertEqual(res['results'], [])

def test_hybrid_groupby_small():
    """Test hybrid search with small result set (3 docs) + groupby"""
    env = Env()
    test_docs = setup_hybrid_groupby_index(env)

    # Search for text that doesn't appear in any document - no text search results
    search_query_with_no_results = "xyznomatch"

    # Query vector at origin [0,0], RADIUS 1**2 returns 3 docs at distance 1
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 1**2
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query_with_no_results,
                       'VSIM', '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', str(radius), 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count',
                       'PARAMS', '2', 'BLOB', query_vector)

    results = parse_hybrid_groupby_response(response)
    # Distance 1 docs: doc:1, doc:2, doc:3 -> automotive, automotive, clothing
    expected_categories = Counter(doc['category'] for doc in test_docs.values() if l2_from_bytes(doc['embedding'], query_vector)**2 <= radius)
    env.assertEqual(Counter(results), expected_categories)

def test_hybrid_groupby_medium():
    """Test hybrid search with medium result set (6 docs) + groupby"""
    env = Env()
    test_docs = setup_hybrid_groupby_index(env)

    # Search for text that doesn't appear in any document
    search_query_with_no_results = "xyznomatch"

    # Query vector at origin [0,0], RADIUS 2**2 returns 6 docs at distances 1-2
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 2**2
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query_with_no_results,
                       'VSIM', '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', str(radius), 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count',
                       'PARAMS', '2', 'BLOB', query_vector)

    results = parse_hybrid_groupby_response(response)
    expected_categories = Counter(doc['category'] for doc in test_docs.values() if l2_from_bytes(doc['embedding'], query_vector)**2 <= radius)
    env.assertEqual(Counter(results), expected_categories)

def test_hybrid_groupby_large():
    """Test hybrid search with large result set (9 docs) + groupby"""
    env = Env()
    test_docs = setup_hybrid_groupby_index(env)

    # Search for text that doesn't appear in any document
    search_query_with_no_results = "xyznomatch"

    # Query vector at origin [0,0], RADIUS 3**2 returns 9 docs at distances 1-3
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 3**2
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query_with_no_results,
                       'VSIM', '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', str(radius), 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count',
                       'PARAMS', '2', 'BLOB', query_vector)

    results = parse_hybrid_groupby_response(response)
    # All 9 docs -> automotive(2), clothing(2), footwear(2), food(2), fitness(1)
    expected_categories = Counter(doc['category'] for doc in test_docs.values() if l2_from_bytes(doc['embedding'], query_vector)**2 <= radius)
    env.assertEqual(Counter(results), expected_categories)

def test_hybrid_groupby_with_filter():
    """Test hybrid search with groupby + filter to verify result count consistency"""
    env = Env()
    test_docs = setup_hybrid_groupby_index(env)

    # Search for text that doesn't appear in any document
    search_query_with_no_results = "xyznomatch"

    # Query vector at origin [0,0], RADIUS 3**2 returns 9 docs at distances 1-3
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 3**2

    # Apply filter to only include categories with count > 1 (should exclude fitness which has count=1)
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query_with_no_results,
                       'VSIM', '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', str(radius),
                       'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count',
                       'FILTER', '@count > 1',
                       'PARAMS', '2', 'BLOB', query_vector)

    # Parse the response to get both results and total_results
    results = parse_hybrid_groupby_response(response)

    # Get total_results from response using the same method as get_results_from_hybrid_response
    res_count_index = recursive_index(response, 'total_results')
    res_count_index[-1] += 1
    total_results = access_nested_list(response, res_count_index)

    # Verify that categories with count > 1 are returned (automotive, clothing, footwear, food)
    # fitness should be filtered out since it has count=1
    expected_categories = {
        'automotive': 2,  # doc:1, doc:2
        'clothing': 2,    # doc:3, doc:4
        'footwear': 2,    # doc:5, doc:6
        'food': 2         # doc:7, doc:8
    }
    env.assertEqual(results, expected_categories)

    # Verify that total_results equals the number of filtered groups (4)
    env.assertEqual(total_results, len(expected_categories))

    # Verify that the sum of individual counts equals the original document count before filtering
    sum_of_counts = sum(results.values())
    expected_sum = 8  # 2+2+2+2 (fitness with count=1 is filtered out)
    env.assertEqual(sum_of_counts, expected_sum)


def test_hybrid_groupby_multiple_collect_reducers():
    """FT.HYBRID with two COLLECT reducers in one GROUPBY, each producing a
    'top hits' collection per group ordered by a different yielded score
    (vector_score vs search_score)."""
    env = Env(protocol=3)
    enable_unstable_features(env)
    setup_hybrid_groupby_index(env)

    # All 9 docs contain "red", while VSIM RADIUS 2**2 covers doc:1..doc:6.
    # FT.HYBRID returns the union of both subqueries, so doc:7..doc:9 are
    # present too, but they do not carry vector_score.
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 2**2

    res = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', '@description:(red)',
            'YIELD_SCORE_AS', 'search_score',
        'VSIM', '@embedding', '$BLOB',
            'RANGE', '2', 'RADIUS', str(radius),
            'YIELD_SCORE_AS', 'vector_score',
        'GROUPBY', '1', '@category',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '2', '@__key', '@vector_score',
                'SORTBY', '1', '@vector_score',
                'AS', 'top_vector_results',
            'REDUCE', 'COLLECT', '7',
                'FIELDS', '2', '@__key', '@search_score',
                'SORTBY', '1', '@search_score',
                'AS', 'top_search_results',
        'SORTBY', '2', '@category', 'ASC',
        'PARAMS', '2', 'BLOB', query_vector)

    expected_by_category = {
        'automotive': {'doc:1', 'doc:2'},
        'clothing':   {'doc:3', 'doc:4'},
        'footwear':   {'doc:5', 'doc:6'},
        'food':       {'doc:7', 'doc:8'},
        'fitness':    {'doc:9'},
    }
    vector_scored_categories = {'automotive', 'clothing', 'footwear'}

    results = res['results']
    env.assertEqual(len(results), len(expected_by_category))

    for r in results:
        attrs = r.get('extra_attributes', r)
        cat = attrs['category']
        env.assertTrue(cat in expected_by_category,
                       message=f"Unexpected category: {cat}")
        expected_keys = expected_by_category[cat]

        # Both reducers should see the same set of docs per group; only the
        # ordering induced by SORTBY differs.
        vec_entries = attrs['top_vector_results']
        srch_entries = attrs['top_search_results']
        env.assertEqual({e['__key'] for e in vec_entries}, expected_keys)
        env.assertEqual({e['__key'] for e in srch_entries}, expected_keys)

        # Every doc has search_score. Only doc:1..doc:6 also have vector_score,
        # so vector sort order can only be asserted for those categories.
        srch_scores = [float(e['search_score']) for e in srch_entries]
        env.assertEqual(srch_scores, sorted(srch_scores))
        if cat in vector_scored_categories:
            vec_scores = [float(e['vector_score']) for e in vec_entries]
            env.assertEqual(vec_scores, sorted(vec_scores))
