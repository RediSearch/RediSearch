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
    """Extract warnings from hybrid search response"""
    warnings_index = recursive_index(response, 'warnings')
    warnings_index[-1] += 1
    return access_nested_list(response, warnings_index)

def setup_basic_index(env):
    """Setup basic index with test data for debug timeout testing"""
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

# Debug timeout tests using TIMEOUT_AFTER_N_* parameters
#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_search():
    """Test FAIL policy with search timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')

def test_debug_timeout_fail_vsim():
    """Test FAIL policy with vector similarity timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_both():
    """Test FAIL policy with both components timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4').error().contains('Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_tail():
    """Test FAIL policy with tail timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_TAIL', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_tail():
    """Test FAIL policy with tail timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 
                       'TIMEOUT_AFTER_N_TAIL', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (POST PROCESSING)'], get_warnings(response))


#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_search():
    """Test RETURN policy with search timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 
                       'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (SEARCH)'], get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_vsim():
    """Test RETURN policy with vector similarity timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 
                       'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (VSIM)'], get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_both():
    """Test RETURN policy with both components timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 
                       'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '4')
    warnings = get_warnings(response)
    env.assertTrue('Timeout limit was reached (SEARCH)' in get_warnings(response))
    env.assertTrue('Timeout limit was reached (VSIM)' in get_warnings(response))
    # TODO: add test for tail timeout once MOD-11004 is merged

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_with_results():
    """Test RETURN policy returns partial results when components timeout"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    # VSIM returns doc:2 and doc:4 (without timeout), SEARCH returns doc:3 (without timeout)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'gear', 'VSIM', \
                       '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, \
                       'TIMEOUT_AFTER_N_SEARCH', '1', 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '4')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue('doc:3' in results.keys())
    # Expect exactly one document from VSIM since the timeout occurred after processing one result - should be either doc:2 or doc:4
    env.assertTrue(('doc:2' in results.keys()) ^ ('doc:4' in results.keys()))

# Warning and error tests
#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_maxprefixexpansions_warning_search_only():
    """Test max prefix expansions warning when only SEARCH component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc:5', 'description', 'runo')
    conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Only SEARCH returns results, VSIM returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', '0.01', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_maxprefixexpansions_warning_vsim_only():
    """Test max prefix expansions warning when only VSIM component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc:5', 'description', 'runo')
    conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Only VSIM returns results, SEARCH returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_maxprefixexpansions_warning_both_components():
    """Test max prefix expansions warning when both SEARCH and VSIM components are affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc:5', 'description', 'runo')
    conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Both SEARCH and VSIM return results
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    warning = get_warnings(response)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in warning)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in warning)

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_tail_property_not_loaded_error():
    """Test error when tail pipeline references property not loaded nor in pipeline"""
    env = Env()
    setup_basic_index(env)
    response = env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', \
                          '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', \
                          query_vector, 'LOAD', '1', '__key', 'APPLY', '2*@__score',\
                          'AS', 'doubled_score').error().contains('Property `__score` not loaded nor in pipeline')

# Real timeout tests - grouped in a class like TestTimeoutReached in test_vecsim.py
class TestRealTimeouts(object):
    """Tests for real timeout conditions with large datasets"""

    def __init__(self):
        self.dim = 128
        self.num_docs = 100000
        self.timeout_ms = 1  # Very short timeout to ensure timeout occurs
        self.doc_len = 20
        self.seed = 42
        self.env = Env()
        self._create_index(self.env)
        self._populate_vectors(self.env)
        self._populate_text(self.env)
        self.heavy_query = ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                   '@vector', '$BLOB', 'KNN', '2', 'K', '10000', 'COMBINE', 'RRF', '2', 'WINDOW', '10000', 'PARAMS', '2', 'BLOB', self.query_vector,
                   'TIMEOUT', str(self.timeout_ms)]

    def tearDown(self):
        """Cleanup after each test"""
        pass

    def _create_index(self, env):
        """Create index with both text and vector fields"""
        env.expect('FT.CREATE', 'idx', 'SCHEMA',
                   'description', 'TEXT',
                   'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(self.dim), 'DISTANCE_METRIC', 'L2').ok()

    def _populate_vectors(self, env):
        """Populate only vectors"""
        query_vector = load_vectors_to_redis(env, self.num_docs, query_vec_index=0, vec_size=self.dim, seed=self.seed)
        self.query_vector = query_vector.tobytes()

    def _populate_text(self, env):
        """Populate only text"""
        populate_db_with_faker_text(env, self.num_docs, doc_len=self.doc_len, seed=self.seed)

    def test_hybrid_fail(self):
        """Test real timeout - hybrid (both text and vector) with FAIL policy"""
        env = self.env
        env.cmd('CONFIG', 'SET', 'search-on-timeout', 'fail')

        # Test hybrid timeout with FAIL policy
        env.expect(*self.heavy_query).error().contains('Timeout limit was reached')

    def test_hybrid_return(self):
        """Test real timeout - hybrid (both text and vector) with RETURN policy"""
        env = self.env
        env.cmd('CONFIG', 'SET', 'search-on-timeout', 'return')

        # Test hybrid timeout with RETURN policy
        response = env.cmd(*self.heavy_query)

        warnings = get_warnings(response)

        env.assertTrue(
            'Timeout limit was reached (SEARCH)' in warnings,
            message=f"Expected 'Timeout limit was reached (SEARCH)' in warnings: {warnings}"
        )
        env.assertTrue(
            'Timeout limit was reached (VSIM)' in warnings,
            message=f"Expected 'Timeout limit was reached (VSIM)' in warnings: {warnings}"
        )
