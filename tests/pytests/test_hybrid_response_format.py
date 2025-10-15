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
    - doc:1: (0.0, 0.0) - "red shoes"
    - doc:2: (1.0, 0.0) - "red running shoes"
    - doc:3: (0.0, 1.0) - "running gear"
    - doc:4: (1.0, 1.0) - "blue shoes"
    - Query Vector: (1.2, 0.2)

"""

# Test data with deterministic vectors
test_data = {
    'doc:1': {
        'description': "red shoes",
        'embedding': np.array([0.0, 0.0]).astype(np.float32).tobytes()
    },
    'doc:2': {
        'description': "red running shoes",
        'embedding': np.array([1.0, 0.0]).astype(np.float32).tobytes()
    },
    'doc:3': {
        'description': "running gear",
        'embedding': np.array([0.0, 1.0]).astype(np.float32).tobytes()
    },
    'doc:4': {
        'description': "blue shoes",
        'embedding': np.array([1.0, 1.0]).astype(np.float32).tobytes()
    }
}

def setup_basic_index(env):
    """Setup basic index with test data"""
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect(
        'FT.CREATE idx SCHEMA description TEXT '
        'embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2'
    ).ok()

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command(
            'HSET', doc_id, 'description', doc_data['description'],
            'embedding', doc_data['embedding']
        )

def test_hybrid_response_format_resp3():
    env = Env(protocol=3)
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
                    np.array([1.2, 0.2]).astype(np.float32).tobytes())
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'red',
        'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'LOAD', 3, '__key', '__score', '@description',
        'SORTBY', 2, '@description', 'ASC'
    )
    expected = {
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
    env.assertEqual(response, expected)

def test_hybrid_response_format_resp2():
    env = Env(protocol=2)
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
                    np.array([1.2, 0.2]).astype(np.float32).tobytes())
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'red',
        'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'LOAD', 3, '__key', '__score', '@description',
        'SORTBY', 2, '@description', 'ASC'
    )
    expected = [
        'total_results', 4,
        'results', [
            ['__key', 'doc:4', '__score', ANY, 'description', 'blue shoes'],
            ['__key', 'doc:2', '__score', ANY, 'description', 'red running shoes'],
            ['__key', 'doc:1', '__score', ANY, 'description', 'red shoes'],
            ['__key', 'doc:3', '__score', ANY, 'description', 'running gear']
        ],
        'warnings',[],
        'execution_time', ANY
    ]
    env.assertEqual(response, expected)

# TODO: Add tests with YIELD_SCORE_AS, APPLY, GROUPBY, SORTBY, WITHSCORES
