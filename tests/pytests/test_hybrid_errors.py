from RLTest import Env
from includes import *
from common import *

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

def get_warnings(response):
    warnings_index = recursive_index(response, 'warning')
    warnings_index[-1] += 1
    return access_nested_list(response, warnings_index)



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
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertTrue('Timeout limit was reached (SEARCH)' in get_warnings(response))
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertTrue('Timeout limit was reached (VSIM)' in get_warnings(response))
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4')
    env.assertTrue('Timeout limit was reached (SEARCH)' in get_warnings(response))
    env.assertTrue('Timeout limit was reached (VSIM)' in get_warnings(response))

def test_hybrid_timeout_policy_return_results():
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_index(env)
    # VSIM returns doc:2 and doc:4 (without timeout), SEARCH returns doc:3 (without timeout)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'gear', 'VSIM', \
                       '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, \
                       'TIMEOUT_AFTER_N_SEARCH', '1', 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '4')
    results = get_results_from_hybrid_response(response)
    env.assertTrue('doc:3' in results.keys())
    env.assertTrue(('doc:2' in results.keys()) ^ ('doc:4' in results.keys()))

def test_hybrid_warning_maxprefixexpansions():
    env = Env(enableDebugCommand=True)
    setup_index(env)
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc:5', 'description', 'runo')
    conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    #Only SEARCH returns results, VSIM returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', '0.01', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in get_warnings(response))
    #Only VSIM returns results, SEARCH returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in get_warnings(response))
    #Both SEARCH and VSIM return results
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    warning = get_warnings(response)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in warning)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in warning)

def test_tail_errors():
    env = Env()
    setup_index(env)
    response = env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', \
                          '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', \
                          query_vector, 'LOAD', '1', '__key', 'APPLY', '2*@__score',\
                          'AS', 'doubled_score').error().contains('Property `__score` not loaded nor in pipeline')


def test_real_timeout_vector_only_fail():
    """Test real timeout - vector only with FAIL policy"""
    env = Env(moduleArgs='ON_TIMEOUT FAIL')

    dim = 4
    num_docs = 10000

    # Create index with both fields but only populate vectors
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

    # Populate only vectors
    query_vector = load_vectors_to_redis(env, num_docs, query_vec_index=0, vec_size=dim, seed=10)

    # Test vector timeout with FAIL policy using FT.HYBRID
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@vector', '$BLOB',
               'PARAMS', '2', 'BLOB', query_vector.tobytes(),
               'TIMEOUT', '1').error().contains('Timeout limit was reached')

def test_real_timeout_vector_only_return():
    """Test real timeout - vector only with RETURN policy"""
    env = Env(moduleArgs='ON_TIMEOUT RETURN')

    dim = 4
    num_docs = 10000

    # Create index with both fields but only populate vectors
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

    # Populate only vectors
    query_vector = load_vectors_to_redis(env, num_docs, query_vec_index=0, vec_size=dim, seed=10)

    # Test vector timeout with RETURN policy using FT.HYBRID
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '123', 'VSIM', '@vector', '$BLOB',
                       'PARAMS', '2', 'BLOB', query_vector.tobytes(),
                       'TIMEOUT', '1')

    warnings = get_warnings(response)
    env.assertTrue('Timeout limit was reached (VSIM)' in warnings)


def test_real_timeout_text_only_fail():
    """Test real timeout - text only with FAIL policy"""
    env = Env(moduleArgs='ON_TIMEOUT FAIL')

    dim = 4
    num_docs = 10000

    # Create index with both fields but only populate text
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

    # Populate only text docs with faker
    populate_db_with_faker_text(env, num_docs, doc_len=5, seed=12345)

    # Create dummy query vector for VSIM part
    query_vector = np.random.rand(dim).astype(np.float32)

    # Test text timeout with FAIL policy using FT.HYBRID
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'description description description description', 'VSIM', '@vector', '$BLOB',
               'PARAMS', '2', 'BLOB', query_vector.tobytes(),
               'TIMEOUT', '1').error().contains('Timeout limit was reached')


def test_real_timeout_text_only_return():
    """Test real timeout - text only with RETURN policy"""
    env = Env(moduleArgs='ON_TIMEOUT RETURN')

    dim = 4
    num_docs = 10000

    # Create index with both fields but only populate text
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

    # Populate only text docs with faker
    populate_db_with_faker_text(env, num_docs, doc_len=5, seed=12345)

    # Create dummy query vector for VSIM part
    query_vector = np.random.rand(dim).astype(np.float32)

    # Test text timeout with RETURN policy using FT.HYBRID
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@vector', '$BLOB',
                       'PARAMS', '2', 'BLOB', query_vector.tobytes(),
                       'TIMEOUT', '1')

    warnings = get_warnings(response)
    env.assertTrue('Timeout limit was reached (SEARCH)' in warnings)

def test_real_timeout_hybrid_fail():
    """Test real timeout - hybrid (both text and vector) with FAIL policy"""
    env = Env(moduleArgs='ON_TIMEOUT FAIL')

    dim = 4
    num_docs = 10000

    # Create index with both text and vector fields
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

    # Populate both text and vectors
    populate_db_with_faker_text(env, num_docs, doc_len=5, seed=12345)
    query_vector = load_vectors_to_redis(env, num_docs, query_vec_index=0, vec_size=dim, seed=10)

    # Test hybrid timeout with FAIL policy
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
               '@vector', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector.tobytes(),
               'TIMEOUT', '1').error().contains('Timeout limit was reached')

def test_real_timeout_hybrid_return():
    """Test real timeout - hybrid (both text and vector) with RETURN policy"""
    env = Env(moduleArgs='ON_TIMEOUT RETURN')

    dim = 4
    num_docs = 10000

    # Create index with both text and vector fields
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

    # Populate both text and vectors
    populate_db_with_faker_text(env, num_docs, doc_len=5, seed=12345)
    query_vector = load_vectors_to_redis(env, num_docs, query_vec_index=0, vec_size=dim, seed=10)

    # Test hybrid timeout with RETURN policy
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                       '@vector', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector.tobytes(),
                       'TIMEOUT', '1')

    warnings = get_warnings(response)

    env.assertTrue('Timeout limit was reached (SEARCH)' in warnings)
    env.assertTrue('Timeout limit was reached (VSIM)' in warnings)