from RLTest import Env
from includes import *
from common import *
from typing import Dict

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

# Query vector for testing
query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()


def get_results(response) -> Dict[str, float]:
    # return dict mapping key -> score from the results list
    res_results_index = recursive_index(response, 'results')
    res_results_index[-1] += 1

    results = {}
    for result in access_nested_list(response, res_results_index):
        key_index = recursive_index(result, '__key')
        key_index[-1] += 1
        score_index = recursive_index(result, '__score')
        score_index[-1] += 1

        key = access_nested_list(result, key_index)
        score = float(access_nested_list(result, score_index))

        results[key] = score
    return results

def setup_index(env):
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

def test_hybrid_timeout_policy_fail():
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4').error().contains('Timeout limit was reached')



def test_hybrid_timeout_policy_return(env):
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_index(env)
    def get_warnings(response):
        warnings_index = recursive_index(response, 'warning')
        warnings_index[-1] += 1
        return access_nested_list(response, warnings_index)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertTrue('Timeout limit was reached' in get_warnings(response))
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertTrue('Timeout limit was reached' in get_warnings(response))
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4')
    env.assertTrue('Timeout limit was reached' in get_warnings(response))

def test_hybrid_timeout_policy_return_results():
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_index(env)
    #vector will return no results
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', '0.05', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertEqual(get_results(response), {'doc:3': 0.016393442623, 'doc:2': 0.0161290322581})
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', '@embedding', '$BLOB', 'KNN', '2', 'K', '2', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertEqual(get_results(response), {'doc:2': 0.016393442623, 'doc:4': 0.0161290322581})

    # When timeout occurs after processing 1 result in both SEARCH and VSIM:
    # Expected: doc:2 (from VSIM) and doc:3 (from SEARCH), both with score 1/61

    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', \
                       '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, \
                       'TIMEOUT_AFTER_N_SEARCH', '1', 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '4')
    env.assertEqual(get_results(response), {'doc:2': 0.016393442623, 'doc:3': 0.016393442623})
