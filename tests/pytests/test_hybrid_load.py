from RLTest import Env
from includes import *
from common import *

"""
The test data creates a 2D vector space with 4 documents positioned as follows:
    Coordinates:
    - key    vector       text      tag
    - doc:1: (0.0, 0.0) - "one"     both
    - doc:2: (1.0, 0.0) -           vector
    - doc:3:            - "three"   text
    - doc:4:            -           none
    - Query Vector: (1.2, 0.2)

"""

QUERY_VECTOR = np.array([1.2, 0.2]).astype(np.float32).tobytes()
expected_doc1 = [b'text', b'one', b'vector',
                 b'\x00\x00\x00\x00\x00\x00\x00\x00', b'tag', b'both']
expected_doc2 = [b'vector', b'\x00\x00\x80?\x00\x00\x00\x00', b'tag', b'vector']
expected_doc3 = [b'text', b'three', b'tag', b'text']
expected_doc4 = [b'tag', b'none']

def setup_basic_index(env):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    cmd = ['FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT',
           'vector', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
           'DISTANCE_METRIC', 'L2',
           'tag', 'TAG']
    env.expect(*cmd).ok

    # Load test data
    conn.execute_command(
        'HSET', 'doc:1',
        'text', 'one',
        'vector', np.array([0.0, 0.0]).astype(np.float32).tobytes(),
        'tag', 'both'
    )
    conn.execute_command(
        'HSET', 'doc:2',
        'vector', np.array([1.0, 0.0]).astype(np.float32).tobytes(),
        'tag', 'vector'
    )
    conn.execute_command(
        'HSET', 'doc:3',
        'text', 'three',
        'tag', 'text'
    )
    conn.execute_command(
        'HSET', 'doc:4',
        'tag', 'none'
    )

def test_load_docs_vector_and_text(env):
    """Test `LOAD *` functionality, doc with vector and text fields"""
    setup_basic_index(env)
    hybrid_cmd = (
            'FT.HYBRID', 'idx',
            'SEARCH', '@tag:{"both"}',
            'VSIM', '@vector', '$BLOB', 'FILTER', '@tag:{"both"}',
            'LOAD', '*',
            'PARAMS', '2', 'BLOB', QUERY_VECTOR
    )
    res = env.cmd(*hybrid_cmd, **{NEVER_DECODE: []})
    env.assertEqual(res[1], 1)
    env.assertEqual(res[3], [expected_doc1])


def test_load_both_fields(env):
    """Test `LOAD *` functionality, doc with vector and text fields"""
    setup_basic_index(env)
    hybrid_cmd = (
            'FT.HYBRID', 'idx',
            'SEARCH', '@tag:{"both"}',
            'VSIM', '@vector', '$BLOB', 'FILTER', '@tag:{"both"}',
            'LOAD', '*',
            'PARAMS', '2', 'BLOB', QUERY_VECTOR
    )
    # Use NEVER_DECODE to handle binary vector data in response
    res = env.cmd(*hybrid_cmd, **{NEVER_DECODE: []})
    env.assertEqual(res[1], 1)
    env.assertEqual(res[3], [expected_doc1])


def test_load_docs_only_vector(env):
    """Test `LOAD *` functionality, doc with only vector fields"""
    setup_basic_index(env)
    hybrid_cmd = (
            'FT.HYBRID', 'idx',
            'SEARCH', '@tag:{"vector"}',
            'VSIM', '@vector', '$BLOB', 'FILTER', '@tag:{"vector"}',
            'LOAD', '*',
            'PARAMS', '2', 'BLOB', QUERY_VECTOR
    )
    res = env.cmd(*hybrid_cmd, **{NEVER_DECODE: []})
    env.assertEqual(res[1], 1)
    env.assertEqual(res[3], [expected_doc2])


def test_load_docs_only_text(env):
    """Test `LOAD *` functionality, doc with only text fields"""
    setup_basic_index(env)
    hybrid_cmd = (
            'FT.HYBRID', 'idx',
            'SEARCH', '@tag:{"text"}',
            'VSIM', '@vector', '$BLOB', 'FILTER', '@tag:{"text"}',
            'LOAD', '*',
            'PARAMS', '2', 'BLOB', QUERY_VECTOR
    )
    res = env.cmd(*hybrid_cmd, **{NEVER_DECODE: []})
    env.assertEqual(res[1], 1)
    env.assertEqual(res[3], [expected_doc3])


def test_load_docs_without_vector_or_text(env):
    """Test `LOAD *` functionality, doc with no vector or text fields"""
    setup_basic_index(env)
    hybrid_cmd = (
            'FT.HYBRID', 'idx',
            'SEARCH', '@tag:{"none"}',
            'VSIM', '@vector', '$BLOB', 'FILTER', '@tag:{"none"}',
            'LOAD', '*',
            'PARAMS', '2', 'BLOB', QUERY_VECTOR
    )
    res = env.cmd(*hybrid_cmd, **{NEVER_DECODE: []})
    env.assertEqual(res[1], 1)
    env.assertEqual(res[3], [expected_doc4])


def test_load_all_docs(env):
    """Test `LOAD *` functionality, all docs"""
    setup_basic_index(env)

    hybrid_cmd = (
            'FT.HYBRID', 'idx',
            'SEARCH', '*',
            'VSIM', '@vector', '$BLOB',
            'LOAD', '*',
            'PARAMS', '2', 'BLOB', QUERY_VECTOR,
    )
    # Use NEVER_DECODE to handle binary vector data in response
    response = env.cmd(*hybrid_cmd, **{NEVER_DECODE: []})
    env.assertEqual(response[1], 4)
    # Verify all documents are returned in any order
    results = response[3]
    expected_results = [expected_doc1, expected_doc2, expected_doc3, expected_doc4]
    set_of_tuples = set(tuple(sorted(lst)) for lst in results)
    expected_set = set(tuple(sorted(lst)) for lst in expected_results)
    env.assertEqual(set_of_tuples, expected_set)


def test_load_all_docs_and_yield():
    """Test `LOAD *` functionality, all docs and yield score"""
    env = Env(protocol=3)
    setup_basic_index(env)

    hybrid_cmd = (
            'FT.HYBRID', 'idx',
            'SEARCH', '@text:(one|three) | @tag:{"none"}',
                'YIELD_SCORE_AS', 'search_score',
            'VSIM', '@vector', '$BLOB',
                'YIELD_SCORE_AS', 'vector_score',
            'COMBINE', 'LINEAR', '6', 'ALPHA', 0.3, 'BETA', 0.7,
                'YIELD_SCORE_AS', 'fused_score',
            'LOAD', '*',
            'PARAMS', '2', 'BLOB', QUERY_VECTOR,
    )
    # Use NEVER_DECODE to handle binary vector data in response
    response = env.cmd(*hybrid_cmd, **{NEVER_DECODE: []})
    results = response[b'results']
    env.assertEqual(response.get(b'total_results'), 4)

    exp_doc1 = to_dict(expected_doc1)
    exp_doc1.update(
           {b'search_score': ANY, b'vector_score': ANY, b'fused_score': ANY})
    exp_doc2 = to_dict(expected_doc2)
    exp_doc2.update({b'vector_score': ANY, b'fused_score': ANY})
    exp_doc3 = to_dict(expected_doc3)
    exp_doc3.update({b'search_score': ANY, b'fused_score': ANY})
    # doc:4 is found by the search query, but with score 0
    exp_doc4 = to_dict(expected_doc4)
    exp_doc4.update({b'search_score': b'0', b'fused_score': b'0'})

    env.assertEqual(results[0], exp_doc2)
    env.assertEqual(results[1], exp_doc1)
    env.assertEqual(results[2], exp_doc3)
    env.assertEqual(results[3], exp_doc4)
