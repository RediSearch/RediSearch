import numpy as np
from RLTest import Env
from includes import *
from common import *

# Test data simulating bikes dataset with "light" related terms
bikes_test_data = {
    'bike:1': {
        'description': "lightweight carbon fiber road bike",
        'embedding': np.array([0.1, 0.2, 0.3, 0.4]).astype(np.float32).tobytes()
    },
    'bike:2': {
        'description': "light mountain bike for trails",
        'embedding': np.array([0.2, 0.3, 0.4, 0.5]).astype(np.float32).tobytes()
    },
    'bike:3': {
        'description': "lighting system for night cycling",
        'embedding': np.array([0.3, 0.4, 0.5, 0.6]).astype(np.float32).tobytes()
    },
    'bike:4': {
        'description': "light touring bike with panniers",
        'embedding': np.array([0.4, 0.5, 0.6, 0.7]).astype(np.float32).tobytes()
    },
    'bike:5': {
        'description': "lightweight electric bike",
        'embedding': np.array([0.5, 0.6, 0.7, 0.8]).astype(np.float32).tobytes()
    },
    'bike:6': {
        'description': "light hybrid bike for commuting",
        'embedding': np.array([0.6, 0.7, 0.8, 0.9]).astype(np.float32).tobytes()
    },
    'bike:7': {
        'description': "lighting accessories and reflectors",
        'embedding': np.array([0.7, 0.8, 0.9, 1.0]).astype(np.float32).tobytes()
    },
    'bike:8': {
        'description': "light gravel bike for adventure",
        'embedding': np.array([0.8, 0.9, 1.0, 0.1]).astype(np.float32).tobytes()
    },
    'bike:9': {
        'description': "lightweight folding bike",
        'embedding': np.array([0.9, 1.0, 0.1, 0.2]).astype(np.float32).tobytes()
    },
    'bike:10': {
        'description': "light city bike with basket",
        'embedding': np.array([1.0, 0.1, 0.2, 0.3]).astype(np.float32).tobytes()
    },
    'bike:11': {
        'description': "lighting kit for bike safety",
        'embedding': np.array([0.1, 0.3, 0.5, 0.7]).astype(np.float32).tobytes()
    },
    'bike:12': {
        'description': "light BMX bike for tricks",
        'embedding': np.array([0.2, 0.4, 0.6, 0.8]).astype(np.float32).tobytes()
    },
    'bike:13': {
        'description': "lightweight racing bike",
        'embedding': np.array([0.3, 0.5, 0.7, 0.9]).astype(np.float32).tobytes()
    },
    'bike:14': {
        'description': "light cruiser bike for beach",
        'embedding': np.array([0.4, 0.6, 0.8, 1.0]).astype(np.float32).tobytes()
    },
    'bike:15': {
        'description': "lighting solutions for cyclists",
        'embedding': np.array([0.5, 0.7, 0.9, 0.1]).astype(np.float32).tobytes()
    },
    # Add some non-matching documents
    'bike:16': {
        'description': "heavy duty cargo bike",
        'embedding': np.array([0.6, 0.8, 1.0, 0.2]).astype(np.float32).tobytes()
    },
    'bike:17': {
        'description': "robust mountain bike frame",
        'embedding': np.array([0.7, 0.9, 0.1, 0.3]).astype(np.float32).tobytes()
    },
    'bike:18': {
        'description': "durable commuter bike",
        'embedding': np.array([0.8, 1.0, 0.2, 0.4]).astype(np.float32).tobytes()
    },
    'bike:19': {
        'description': "sturdy touring bicycle",
        'embedding': np.array([0.9, 0.1, 0.3, 0.5]).astype(np.float32).tobytes()
    },
    'bike:20': {
        'description': "solid steel frame bike",
        'embedding': np.array([1.0, 0.2, 0.4, 0.6]).astype(np.float32).tobytes()
    }
}

def setup_bikes_index(env):
    """Setup bikes index with vector field"""
    conn = env.getClusterConnectionIfNeeded()

    # Create index with text and vector fields
    env.expect('FT.CREATE', 'idx:bikes_vss', 'SCHEMA',
               'description', 'TEXT',
               'description_embeddings', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '4', 'DISTANCE_METRIC', 'L2').ok

    # Load test data
    for doc_id, doc_data in bikes_test_data.items():
        conn.execute_command('HSET', doc_id,
                           'description', doc_data['description'],
                           'description_embeddings', doc_data['embedding'])



# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_mod_11610():
    """Test FT.SEARCH and FT.HYBRID with increasing parameters to get more than 10 results"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_bikes_index(env)

    # Query vector for similarity search
    query_vector = np.array([0.5, 0.5, 0.5, 0.5]).astype(np.float32).tobytes()

    # First, test regular FT.SEARCH to establish baseline (avoid returning vector data)
    regular_search_response = env.cmd('FT.SEARCH', 'idx:bikes_vss', 'light*', 'DIALECT', '2', 'RETURN', '0')
    # FT.SEARCH in RESP2 returns [count, doc1, doc2, ...] format
    regular_count = regular_search_response[0]
    env.assertEqual(regular_count, 15)

    # Test FT.HYBRID with increasing K, WINDOW, and LIMIT parameters
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'LIMIT', '0', '100',
                             'PARAMS', '2', 'BLOB', query_vector)

    # FT.HYBRID returns a structured response with key-value pairs
    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    env.assertEqual(hybrid_count, 20)

    # Test FT.HYBRID with increasing K, WINDOW, and LIMIT parameters at end
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'PARAMS', '2', 'BLOB', query_vector, 'LIMIT', '0', '100')

    # FT.HYBRID returns a structured response with key-value pairs
    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    env.assertEqual(hybrid_count, 20)

        # Test FT.HYBRID with increasing K, WINDOW, and LIMIT parameters at end
    hybrid_response = env.cmd('FT.HYBRID', 'idx:bikes_vss',
                             'SEARCH', 'light*',
                             'VSIM', '@description_embeddings', '$BLOB',
                             'KNN', '2', 'K', '50',
                             'COMBINE', 'RRF', '2', 'WINDOW', '100',
                             'PARAMS', '2', 'BLOB', query_vector, 'LIMIT', '0', '5')

    # FT.HYBRID returns a structured response with key-value pairs
    hybrid_dict = to_dict(hybrid_response)
    hybrid_count = hybrid_dict['total_results']
    env.assertEqual(hybrid_count, 5)
