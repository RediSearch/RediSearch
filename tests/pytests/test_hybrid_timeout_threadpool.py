"""
Test timeout handling in HYBRID command thread pool scenarios.
This test focuses on the timeout handling mechanisms:
1. Thread pool timeout prevention (don't queue jobs when already timed out)
2. RPDepleter timeout detection when starting execution
3. HybridMerger timeout handling via upstream return codes
"""

from common import *
import numpy as np

# Test vector for similarity search
query_vector = np.random.rand(128).astype(np.float32).tobytes()

def setup_basic_index(env):
    """Setup a basic index with text and vector fields for testing"""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 
               'title', 'TEXT', 
               'embedding', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'COSINE').ok()
    
    # Add some test documents
    for i in range(10):
        vector = np.random.rand(128).astype(np.float32).tobytes()
        env.expect('HSET', f'doc:{i}', 'title', f'running gear {i}', 'embedding', vector).ok()

def get_warnings(response):
    """Extract warnings from hybrid response"""
    if isinstance(response, dict) and 'warning' in response:
        return response['warning']
    return []

def get_results_from_hybrid_response(response):
    """Extract results from hybrid response"""
    if isinstance(response, dict) and 'results' in response:
        results = {}
        for result in response['results']:
            if len(result) >= 2:
                results[result[0]] = result[1:]
        return results
    return {}

# Test thread pool timeout detection
@skip(cluster=True)
def test_thread_pool_timeout_fail():
    """Test that thread pool timeout is detected with FAIL policy"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    
    # Use a very short timeout to trigger thread pool timeout
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
               'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT', '1').error().contains('Timeout')

@skip(cluster=True)
def test_thread_pool_timeout_return():
    """Test that thread pool timeout returns partial results with RETURN policy"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    
    # Use a very short timeout to trigger thread pool timeout
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
                       'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT', '1')
    
    # Should return partial results with timeout warning
    warnings = get_warnings(response)
    env.assertTrue(len(warnings) > 0, "Expected timeout warning")
    env.assertTrue(any('Timeout' in warning for warning in warnings), "Expected timeout in warnings")

# Test RPDepleter timeout handling
@skip(cluster=True)
def test_depleter_timeout_fail():
    """Test RPDepleter timeout detection with FAIL policy"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    
    # Use debug parameters to simulate timeout
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
               'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 
               'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout')

@skip(cluster=True)
def test_depleter_timeout_return():
    """Test RPDepleter timeout detection with RETURN policy"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    
    # Use debug parameters to simulate timeout
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
                       'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 
                       'DEBUG_PARAMS_COUNT', '2')
    
    # Should return partial results with timeout warning
    warnings = get_warnings(response)
    env.assertTrue(len(warnings) > 0, "Expected timeout warning")
    env.assertTrue(any('SEARCH' in warning for warning in warnings), "Expected SEARCH timeout in warnings")

# Test HybridMerger timeout handling
@skip(cluster=True)
def test_hybrid_merger_timeout_fail():
    """Test HybridMerger timeout handling with FAIL policy"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    
    # Use debug parameters to simulate timeout in tail pipeline
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
               'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_TAIL', '1', 
               'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout')

@skip(cluster=True)
def test_hybrid_merger_timeout_return():
    """Test HybridMerger timeout handling with RETURN policy"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    
    # Use debug parameters to simulate timeout in tail pipeline
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
                       'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_TAIL', '1', 
                       'DEBUG_PARAMS_COUNT', '2')
    
    # Should return partial results with timeout warning
    warnings = get_warnings(response)
    env.assertTrue(len(warnings) > 0, "Expected timeout warning")

# Test timeout policy consistency
@skip(cluster=True)
def test_timeout_policy_consistency():
    """Test that timeout policies work consistently across all components"""
    # Test FAIL policy
    env_fail = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env_fail)
    
    env_fail.expect('FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
                    'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT', '1').error().contains('Timeout')
    
    # Test RETURN policy
    env_return = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env_return)
    
    response = env_return.cmd('FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 
                              'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT', '1')
    
    # Should return response with warnings, not error
    env_return.assertTrue(isinstance(response, dict), "Expected dict response for RETURN policy")
    warnings = get_warnings(response)
    env_return.assertTrue(len(warnings) > 0, "Expected timeout warning for RETURN policy")
    