import numpy as np
from RLTest import Env
from common import *

def setup_basic_index(env):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect(
        'FT.CREATE idx SCHEMA '
        'text TEXT '
        'vector VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 '
        'tag TAG'
    ).ok

    # Create 2 documents:
    conn.execute_command(
        'HSET', 'doc:1',
        'text', 'green apples',
        'vector', np.array([0.0, 1.0]).astype(np.float32).tobytes(),
        'tag', '57-300'
    )
    conn.execute_command(
        'HSET', 'doc:2',
        'text', 'red apples',
        'vector', np.array([0.0, 2.0]).astype(np.float32).tobytes(),
        'tag', '57-300'
    )


def exec_and_validate_query(env, hybrid_cmd):
    """Execute query and validate results"""
    response = env.cmd(*hybrid_cmd)
    results = get_results_from_hybrid_response(response)
    env.assertTrue(set(results.keys()) == {"doc:1", "doc:2"})


# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_dialects():
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.2]).astype(np.float32).tobytes()

    for dialect in [1, 2, 3, 4]:
        env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()

        # Simple query without parameters
        hybrid_cmd = [
            'FT.HYBRID', 'idx',
            'SEARCH', '@text:(apples)',
            'VSIM', '@vector', query_vector,
        ]
        exec_and_validate_query(env, hybrid_cmd)

        # Simple query with parameters in SEARCH
        hybrid_cmd = [
            'FT.HYBRID', 'idx',
            'SEARCH', '@text:($WORD)',
            'VSIM', '@vector', query_vector,
            'PARAMS', '2', 'WORD', 'apples'
        ]
        exec_and_validate_query(env, hybrid_cmd)

        # Simple query with parameters in VSIM
        hybrid_cmd = [
            'FT.HYBRID', 'idx',
            'SEARCH', '@text:(apples)',
            'VSIM', '@vector', '$BLOB',
            'PARAMS', '2', 'BLOB', query_vector,
        ]
        exec_and_validate_query(env, hybrid_cmd)

        # Tag autoescaping in SEARCH - invalid syntax in DIALECT 1, but default overrides to DIALECT 2
        hybrid_cmd = [
            'FT.HYBRID', 'idx',
            'SEARCH', '@tag:{"57-300"}',
            'VSIM', '@vector', query_vector,
        ]
        exec_and_validate_query(env, hybrid_cmd)

        # Tag autoescaping in VSIM FILTER - invalid syntax in DIALECT 1, but default overrides to DIALECT 2
        hybrid_cmd = [
            'FT.HYBRID', 'idx',
            'SEARCH', '@text:(apples)',
            'VSIM', '@vector', query_vector, 'FILTER', '@tag:{"57-300"}',
        ]
        exec_and_validate_query(env, hybrid_cmd)

        # Parameters in VSIM FILTER - invalid syntax in DIALECT 1, but default overrides to DIALECT 2
        hybrid_cmd = [
            'FT.HYBRID', 'idx',
            'SEARCH', '@text:(apples)',
            'VSIM', '@vector', query_vector, 'FILTER', '@text:($WORD)',
            'PARAMS', '2', 'WORD', 'apples',
        ]
        exec_and_validate_query(env, hybrid_cmd)

        # Post filter
        hybrid_cmd = [
            'FT.HYBRID', 'idx',
            'SEARCH', '@text:(apples)',
            'VSIM', '@vector', query_vector,
            'COMBINE', 'RRF', '2', 'CONSTANT', '30',
            'FILTER', '@__key == "doc:1" || @__key == "doc:2"',
        ]
        exec_and_validate_query(env, hybrid_cmd)


# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_dialect_errors():
    """Test DIALECT parameter error handling in hybrid queries"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.2]).astype(np.float32).tobytes()

    # Test DIALECT in SEARCH subquery - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', '@text:(apples)', 'DIALECT', '2',
        'VSIM', '@vector', query_vector
    ).error().contains('DIALECT is not supported in SEARCH subquery')

    # Test DIALECT in KNN subquery - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', '@text:(apples)',
        'VSIM', '@vector', query_vector, 'KNN', '2', 'DIALECT', '2'
    ).error().contains('DIALECT is not supported in vector subquery')

    # Test DIALECT in RANGE subquery - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', '@text:(apples)',
        'VSIM', '@vector', query_vector, 'RANGE', '2', 'DIALECT', '2'
    ).error().contains('DIALECT is not supported in vector subquery')

    # Test DIALECT in tail section - should succeed
    hybrid_cmd = [
        'FT.HYBRID', 'idx',
        'SEARCH', '@text:(apples)',
        'VSIM', '@vector', query_vector,
        'DIALECT', '2'
    ]
    env.expect(*hybrid_cmd).error().contains('DIALECT is not supported in hybrid queries')

    # Test DIALECT with other tail parameters - should succeed
    hybrid_cmd = [
        'FT.HYBRID', 'idx',
        'SEARCH', '@text:(apples)',
        'VSIM', '@vector', query_vector,
        'COMBINE', 'RRF', '2', 'CONSTANT', '30',
        'DIALECT', '3'
    ]
    env.expect(*hybrid_cmd).error().contains('DIALECT is not supported in hybrid queries')
