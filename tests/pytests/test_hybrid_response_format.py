import numpy as np
from RLTest import Env
from includes import *
from common import *

"""
VECTOR SPACE LAYOUT:
====================

The test data creates a 2D vector space with 4 documents positioned as follows:

    doc:3 ●────────────● doc:4
          │            │
          │            │
          │            │  ● Query
          │            │    Vector
    doc:1 ●────────────● doc:2

    Coordinates:
    - doc:1: (0.0, 0.0) - "red shoes"         - category: shoes
    - doc:2: (1.0, 0.0) - "red running shoes" - category: shoes
    - doc:3: (0.0, 1.0) - "running gear"      - category: gear
    - doc:4: (1.0, 1.0) - "blue shoes"        - category: shoes
    - Query Vector: (1.2, 0.2)

"""

# Test data with deterministic vectors
test_data = {
    'doc:1': {
        'description': "red shoes",
        'embedding': np.array([0.0, 0.0]).astype(np.float32).tobytes(),
        'category': "shoes"
    },
    'doc:2': {
        'description': "red running shoes",
        'embedding': np.array([1.0, 0.0]).astype(np.float32).tobytes(),
        'category': "shoes"
    },
    'doc:3': {
        'description': "running gear",
        'embedding': np.array([0.0, 1.0]).astype(np.float32).tobytes(),
        'category': "gear"
    },
    'doc:4': {
        'description': "blue shoes",
        'embedding': np.array([1.0, 1.0]).astype(np.float32).tobytes(),
        'category': "shoes"
    }
}

def _setup_basic_index(env):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect(
        'FT.CREATE idx SCHEMA description TEXT '
        'embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 '
        'category TAG'
    ).ok()

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command(
            'HSET', doc_id, 'description', doc_data['description'],
            'embedding', doc_data['embedding'],
            'category', doc_data['category']
        )

def _resp3_to_resp2(resp3_dict):
    """Convert RESP3 dict format to RESP2 flat list format"""
    """Example:
    resp3_dict = {
        'total_results': 2,
        'results': [
            {'__key': 'doc:4', '__score': ANY, 'description': 'blue shoes'},
            {'__key': 'doc:2', '__score': ANY, 'description': 'red running shoes'}
        ],
        'warnings': [],
        'execution_time': ANY
    }

    Returns:
    [
        'total_results', 2,
        'results', [
            ['__key', 'doc:4', '__score', ANY, 'description', 'blue shoes'],
            ['__key', 'doc:2', '__score', ANY, 'description', 'red running shoes']
        ],
        'warnings', [],
        'execution_time', ANY
    ]
    """
    result = []

    # Add top-level key-value pairs
    for key, value in resp3_dict.items():
        result.append(key)
        if key == 'results' and isinstance(value, list):
            # Convert list of dicts to list of flat lists
            converted_results = []
            for item_dict in value:
                flat_item = []
                for k, v in item_dict.items():
                    flat_item.extend([k, v])
                converted_results.append(flat_item)
            result.append(converted_results)
        else:
            result.append(value)

    return result

def _test_resp3_and_resp2(cmd, resp3_expected):
    env = Env(protocol=3)
    _setup_basic_index(env)
    # Test RESP3
    response = env.cmd(*cmd)
    env.assertEqual(response, resp3_expected)
    # Test RESP2
    env.cmd('HELLO', 2)
    response = env.cmd(*cmd)
    resp2_expected = _resp3_to_resp2(resp3_expected)
    env.assertEqual(response, resp2_expected)

def test_simple_query():
    cmd = [
        'FT.HYBRID', 'idx',
        'SEARCH', 'red',
        'VSIM' ,'@embedding', np.array([1.2, 0.2]).astype(np.float32).tobytes(),
        'LOAD', 3, '@__key', '@__score', '@description',
        'SORTBY', 2, '@description', 'ASC'
    ]
    resp3_expected = {
        'total_results': 4,
        'results': [
            {'__key': 'doc:4', '__score': ANY, 'description': 'blue shoes'},
            {'__key': 'doc:2', '__score': ANY, 'description': 'red running shoes'},
            {'__key': 'doc:1', '__score': ANY, 'description': 'red shoes'},
            {'__key': 'doc:3', '__score': ANY, 'description': 'running gear'}
        ],
        'warnings': [],
        'execution_time': ANY
    }
    _test_resp3_and_resp2(cmd, resp3_expected)

def test_query_with_groupby():
    cmd = [
        'FT.HYBRID', 'idx',
        'SEARCH', '*',
        'VSIM' ,'@embedding', np.array([1.2, 0.2]).astype(np.float32).tobytes(),
        'LOAD', 2, '@__key', '@category',
        'GROUPBY', 1, '@category', 'REDUCE', 'COUNT', 0, 'AS', 'count',
        'SORTBY', 2, '@category', 'ASC'
    ]
    resp3_expected = {
        'total_results': 2,
        'results': [
            {'category': 'gear', 'count': '1'},
            {'category': 'shoes', 'count': '3'}
        ],
        'warnings': [],
        'execution_time': ANY
    }
    _test_resp3_and_resp2(cmd, resp3_expected)

def test_query_with_apply():
    cmd = [
        'FT.HYBRID', 'idx',
        'SEARCH', '*',
        'VSIM' ,'@embedding', np.array([1.2, 0.2]).astype(np.float32).tobytes(),
        'LOAD', 2,'@category', '@description',
        'APPLY', 'strlen(@category) + strlen(@description)', 'AS', 'length',
        'SORTBY', 2, '@length', 'ASC'
    ]
    resp3_expected = {
        'total_results': 4,
        'results': [
            {'category': 'shoes', 'description': 'red shoes', 'length': '14'},
            {'category': 'shoes', 'description': 'blue shoes', 'length': '15'},
            {'category': 'gear', 'description': 'running gear', 'length': '16'},
            {'category': 'shoes', 'description': 'red running shoes', 'length': '22'}
        ],
        'warnings': [],
        'execution_time': ANY
    }
    _test_resp3_and_resp2(cmd, resp3_expected)

def test_query_with_yield_score_as():
    cmd = [
        'FT.HYBRID', 'idx',
        'SEARCH', '*',
        'VSIM' ,'@embedding', np.array([1.2, 0.2]).astype(np.float32).tobytes(),
        'KNN', '4', 'K', '10', 'YIELD_SCORE_AS', 'vector_score',
        'SORTBY', 2, '@description', 'ASC'
    ]
    resp3_expected = {
        'total_results': 4,
        'results': [
            {'__key': 'doc:4', '__score': ANY, 'vector_score': ANY, 'description': 'blue shoes'},
            {'__key': 'doc:2', '__score': ANY, 'vector_score': ANY, 'description': 'red running shoes'},
            {'__key': 'doc:1', '__score': ANY, 'vector_score': ANY, 'description': 'red shoes'},
            {'__key': 'doc:3', '__score': ANY, 'vector_score': ANY, 'description': 'running gear'}
        ],
        'warnings': [],
        'execution_time': ANY
    }
    _test_resp3_and_resp2(cmd, resp3_expected)
