import numpy as np
from RLTest import Env
from includes import *
from common import *
import json

# Test data with deterministic vectors
test_data = {
    'doc:1': {
        'description': "red shoes",
        'embedding': [0.0, 0.0]
    },
    'doc:2': {
        'description': "red running shoes",
        'embedding': [1.0, 0.0]
    },
    'doc:3': {
        'description': "running gear and many different shoes",
        'embedding': [0.0, 1.0]
    },
    'doc:4': {
        'description': "blue shoes",
        'embedding': [1.0, 1.0]
    }
}

def setup_basic_index(env):
    """Setup basic JSON index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect(
        'FT.CREATE idx ON JSON '
        'SCHEMA $.description AS description TEXT '
        '$.embedding AS embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok()

    for doc_id, doc_data in test_data.items():
        json_doc = {
            "description": doc_data["description"],
            "embedding": doc_data["embedding"]
        }
        conn.execute_command('JSON.SET', doc_id, '$', json.dumps(json_doc))

def test_hybrid_vector_direct_blob_knn():
    env = Env()
    setup_basic_index(env)
    vector_blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    response = env.cmd(
        'FT.HYBRID', 'idx', 'SEARCH', 'green',
        'VSIM' ,'@embedding', vector_blob, 'KNN', '2', 'K', '1')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue(set(results.keys()) == {"doc:2"})

def test_hybrid_vector_direct_blob_knn_with_filter():
    env = Env()
    setup_basic_index(env)
    vector_blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    response = env.cmd(
        'FT.HYBRID', 'idx', 'SEARCH', 'green',
        'VSIM' ,'@embedding', vector_blob, 'KNN', '2', 'K', '2',
        'FILTER', '@description:blue')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue(set(results.keys()) == {"doc:4"})

def test_hybrid_vector_direct_blob_range():
    env = Env()
    setup_basic_index(env)
    vector_blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    response = env.cmd(
        'FT.HYBRID', 'idx', 'SEARCH', 'green',
        'VSIM' ,'@embedding', vector_blob, 'RANGE', '2', 'RADIUS', '1')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue(set(results.keys()) == {"doc:2", "doc:4"})

def test_hybrid_vector_direct_blob_range_with_filter():
    env = Env()
    setup_basic_index(env)
    vector_blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    response = env.cmd(
        'FT.HYBRID', 'idx', 'SEARCH', 'green',
        'VSIM' ,'@embedding', vector_blob, 'RANGE', '2', 'RADIUS', '1',
        'FILTER', '@description:blue')
    results, count = get_results_from_hybrid_response(response)
    env.assertTrue(set(results.keys()) == {"doc:4"})
    env.assertEqual(count, len(results.keys()))

def test_hybrid_vector_direct_blob_range_with_filter_and_limit():
    env = Env()
    setup_basic_index(env)
    vector_blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    response = env.cmd(
        'FT.HYBRID', 'idx', 'SEARCH', 'green',
        'VSIM' ,'@embedding', vector_blob, 'RANGE', '2', 'RADIUS', '1',
        'FILTER', '@description:blue', 'LIMIT', '0', '1')
    results, count = get_results_from_hybrid_response(response)
    env.assertTrue(set(results.keys()) == {"doc:4"})
    env.assertEqual(count, len(results.keys()))

def test_knn_default_output():
    env = Env(protocol=3)
    setup_basic_index(env)
    vector_blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    response = env.cmd(
        'FT.HYBRID', 'idx', 'SEARCH', 'blue',
        'VSIM' ,'@embedding', vector_blob, 'KNN', '2', 'K', '2',
        'COMBINE', 'RRF', '2', 'CONSTANT', '3')
    env.assertEqual(response['total_results'], 2)
    env.assertEqual(len(response['results']), 2)
    # DocId     | SEARCH_RANK | VECTOR_RANK | SCORE
    # ----------------------------------------------------
    # doc:4    | 1           | 2           | 1/4 + 1/5 = 0.45
    # doc:2    | -           | 1           |  0  + 1/4 = 0.25
    env.assertEqual(response['results'][0]['__key'], "doc:4")
    env.assertEqual(response['results'][0]['__score'], "0.45")
    env.assertEqual(response['results'][1]['__key'], "doc:2")
    env.assertEqual(response['results'][1]['__score'], "0.25")

def test_knn_load_key():
    env = Env(protocol=3)
    setup_basic_index(env)
    vector_blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    response = env.cmd(
        'FT.HYBRID', 'idx', 'SEARCH', 'blue',
        'VSIM' ,'@embedding', vector_blob, 'KNN', '2', 'K', '2',
        'LOAD', '3', '@__key', 'AS', 'my_key')
    env.assertEqual(response['total_results'], 2)
    env.assertEqual(len(response['results']), 2)
    env.assertEqual(response['results'][0]['my_key'], "doc:4")
    env.assertEqual(response['results'][1]['my_key'], "doc:2")



