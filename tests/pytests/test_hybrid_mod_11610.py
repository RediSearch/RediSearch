import numpy as np
from RLTest import Env
from includes import *
from common import *

# Test data simulating bikes dataset with "light" related terms
bikes_test_data = {
    'bike:1{hash_tag}': {
        'description': "lightweight carbon fiber road bike",
        'category': 'road',
        'embedding': np.array([0.1, 0.2, 0.3, 0.4]).astype(np.float32).tobytes()
    },
    'bike:2{hash_tag}': {
        'description': "light mountain bike for trails",
        'category': 'mountain',
        'embedding': np.array([0.2, 0.3, 0.4, 0.5]).astype(np.float32).tobytes()
    },
    'bike:3{hash_tag}': {
        'description': "lighting system for night cycling",
        'category': 'accessory',
        'embedding': np.array([0.3, 0.4, 0.5, 0.6]).astype(np.float32).tobytes()
    },
    'bike:4{hash_tag}': {
        'description': "light touring bike with panniers",
        'category': 'touring',
        'embedding': np.array([0.4, 0.5, 0.6, 0.7]).astype(np.float32).tobytes()
    },
    'bike:5{hash_tag}': {
        'description': "lightweight electric bike",
        'category': 'electric',
        'embedding': np.array([0.5, 0.6, 0.7, 0.8]).astype(np.float32).tobytes()
    },
    'bike:6{hash_tag}': {
        'description': "light hybrid bike for commuting",
        'category': 'hybrid',
        'embedding': np.array([0.6, 0.7, 0.8, 0.9]).astype(np.float32).tobytes()
    },
    'bike:7{hash_tag}': {
        'description': "lighting accessories and reflectors",
        'category': 'accessory',
        'embedding': np.array([0.7, 0.8, 0.9, 1.0]).astype(np.float32).tobytes()
    },
    'bike:8{hash_tag}': {
        'description': "light gravel bike for adventure",
        'category': 'gravel',
        'embedding': np.array([0.8, 0.9, 1.0, 0.1]).astype(np.float32).tobytes()
    },
    'bike:9{hash_tag}': {
        'description': "lightweight folding bike",
        'category': 'folding',
        'embedding': np.array([0.9, 1.0, 0.1, 0.2]).astype(np.float32).tobytes()
    },
    'bike:10{hash_tag}': {
        'description': "light city bike with basket",
        'category': 'city',
        'embedding': np.array([1.0, 0.1, 0.2, 0.3]).astype(np.float32).tobytes()
    },
    'bike:11{hash_tag}': {
        'description': "lighting kit for bike safety",
        'category': 'accessory',
        'embedding': np.array([0.1, 0.3, 0.5, 0.7]).astype(np.float32).tobytes()
    },
    'bike:12{hash_tag}': {
        'description': "light BMX bike for tricks",
        'category': 'bmx',
        'embedding': np.array([0.2, 0.4, 0.6, 0.8]).astype(np.float32).tobytes()
    },
    'bike:13{hash_tag}': {
        'description': "lightweight racing bike",
        'category': 'road',
        'embedding': np.array([0.3, 0.5, 0.7, 0.9]).astype(np.float32).tobytes()
    },
    'bike:14{hash_tag}': {
        'description': "light cruiser bike for beach",
        'category': 'cruiser',
        'embedding': np.array([0.4, 0.6, 0.8, 1.0]).astype(np.float32).tobytes()
    },
    'bike:15{hash_tag}': {
        'description': "lighting solutions for cyclists",
        'category': 'accessory',
        'embedding': np.array([0.5, 0.7, 0.9, 0.1]).astype(np.float32).tobytes()
    },
    # Add some non-matching documents
    'bike:16{hash_tag}': {
        'description': "heavy duty cargo bike",
        'category': 'cargo',
        'embedding': np.array([0.6, 0.8, 1.0, 0.2]).astype(np.float32).tobytes()
    },
    'bike:17{hash_tag}': {
        'description': "robust mountain bike frame",
        'category': 'mountain',
        'embedding': np.array([0.7, 0.9, 0.1, 0.3]).astype(np.float32).tobytes()
    },
    'bike:18{hash_tag}': {
        'description': "durable commuter bike",
        'category': 'city',
        'embedding': np.array([0.8, 1.0, 0.2, 0.4]).astype(np.float32).tobytes()
    },
    'bike:19{hash_tag}': {
        'description': "sturdy touring bicycle",
        'category': 'touring',
        'embedding': np.array([0.9, 0.1, 0.3, 0.5]).astype(np.float32).tobytes()
    },
    'bike:20{hash_tag}': {
        'description': "solid steel frame bike",
        'category': 'road',
        'embedding': np.array([1.0, 0.2, 0.4, 0.6]).astype(np.float32).tobytes()
    }
}

def setup_bikes_index(env):
    """Setup bikes index with vector field"""
    conn = env.getClusterConnectionIfNeeded()

    # Create index with text, tag, and vector fields
    env.expect('FT.CREATE', 'idx:bikes_vss', 'SCHEMA',
               'description', 'TEXT',
               'category', 'TAG',
               'description_embeddings', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '4', 'DISTANCE_METRIC', 'L2').ok

    # Load test data
    for doc_id, doc_data in bikes_test_data.items():
        conn.execute_command('HSET', doc_id,
                           'description', doc_data['description'],
                           'category', doc_data['category'],
                           'description_embeddings', doc_data['embedding'])


# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_mod_11610():
    """Test FT.SEARCH and FT.HYBRID with increasing parameters to get more than 10 results"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_bikes_index(env)

    # Query vector for similarity search
    query_vector = np.array([0.5, 0.5, 0.5, 0.5]).astype(np.float32).tobytes()
    # Test FT.HYBRID with increasing K, WINDOW, and LIMIT parameters
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'LIMIT', '0', '100',
                             'PARAMS', '2', 'BLOB', query_vector)

    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    env.assertEqual(hybrid_count, 20)
    env.assertEqual(len(hybrid_dict['results']), 20)

    # Test FT.HYBRID with increasing K, WINDOW, and LIMIT parameters at end
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'PARAMS', '2', 'BLOB', query_vector, 'LIMIT', '0', '100')

    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    env.assertEqual(hybrid_count, 20)
    env.assertEqual(len(hybrid_dict['results']), 20)

    # Test FT.HYBRID with LIMIT smaller than available results
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'PARAMS', '2', 'BLOB', query_vector, 'LIMIT', '0', '5')

    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    # Should return 5 results (limit is smaller than available 20)
    env.assertEqual(len(hybrid_dict['results']), 5)
    env.assertEqual(hybrid_count, 20)

    # Test FT.HYBRID with LIMIT larger than available results
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'PARAMS', '2', 'BLOB', query_vector, 'LIMIT', '0', '1000')

    # FT.HYBRID returns a structured response with key-value pairs
    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    # Should return 20 results (all available, even though limit is 1000)
    env.assertEqual(hybrid_count, 20)
    env.assertEqual(len(hybrid_dict['results']), 20)


# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_limit_with_filter():
    """Test FT.HYBRID with LIMIT and FILTER to ensure filtering is applied before limiting"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_bikes_index(env)

    # Query vector for similarity search
    query_vector = np.array([0.5, 0.5, 0.5, 0.5]).astype(np.float32).tobytes()

    # Test FT.HYBRID with FILTER and LIMIT
    # Filter for documents with category "road" (should have 3 docs: bike:1, bike:13, bike:20)
    # But only bike:1 and bike:13 have "light*" in description
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'LOAD', '2', '__key', 'category',
                             'FILTER', '@category=="road"',
                             'LIMIT', '0', '5',
                             'PARAMS', '2', 'BLOB', query_vector)

    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']

    env.assertEqual(len(hybrid_dict['results']), 3)
    env.assertEqual(hybrid_count, 3)

    # Verify all returned results have category "road"
    for result in hybrid_dict['results']:
        result_dict = dict(zip(result[::2], result[1::2]))
        env.assertEqual(result_dict['category'], 'road')

    # Test FT.HYBRID with FILTER for "mountain" category
    # Should have 2 docs: bike:2 (light mountain) and bike:17 (robust mountain)
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'LOAD', '2', '__key', 'category',
                             'FILTER', '@category=="mountain"',
                             'LIMIT', '0', '10',
                             'PARAMS', '2', 'BLOB', query_vector)

    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']

    env.assertEqual(len(hybrid_dict['results']), 2)
    env.assertEqual(hybrid_count, 2)

    # Verify the result has category "mountain"
    result_dict = dict(zip(hybrid_dict['results'][0][::2], hybrid_dict['results'][0][1::2]))
    env.assertEqual(result_dict['category'], 'mountain')

    # Test FT.HYBRID with FILTER for "accessory" category
    # Should have 4 docs: bike:3, bike:7, bike:11, bike:15
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'LOAD', '2', '__key', 'category',
                             'FILTER', '@category=="accessory"',
                             'LIMIT', '0', '3',
                             'PARAMS', '2', 'BLOB', query_vector)

    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    # Should return 3 results (limited by LIMIT, even though 4 match)
    env.assertEqual(len(hybrid_dict['results']), 3)
    env.assertEqual(hybrid_count, 4)

    # Verify all returned results have category "accessory"
    for result in hybrid_dict['results']:
        result_dict = dict(zip(result[::2], result[1::2]))
        env.assertEqual(result_dict['category'], 'accessory')
