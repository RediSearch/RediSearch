from RLTest import Env
from includes import *
from common import *
import numpy as np

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
        'description': "running gear and many different shoes",
        'embedding': np.array([0.0, 1.0]).astype(np.float32).tobytes()
    },
    'doc:4': {
        'description': "blue shoes",
        'embedding': np.array([1.0, 1.0]).astype(np.float32).tobytes()
    }
}

def setup_basic_index(env):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

def test_hybrid_sortby_nosort_conflict():
    """Test that SORTBY and NOSORT cannot be used together in hybrid queries"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    
    # Test SORTBY followed by NOSORT - should fail
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'SORTBY', 'description', 'NOSORT').error().contains('NOSORT is not allowed with SORTBY')
    
    # Test NOSORT followed by SORTBY - should fail  
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'NOSORT', 'SORTBY', 'description').error().contains('NOSORT is not allowed with SORTBY')
    
    # Test that SORTBY alone works (should not fail)
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'SORTBY', 'description').ok()
    
    # Test that NOSORT alone works (should not fail)
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'NOSORT').ok()


def test_hybrid_sortby_nosort_with_combine():
    """Test SORTBY and NOSORT conflict with COMBINE clause"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    
    # Test SORTBY followed by NOSORT with COMBINE - should fail
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'COMBINE', 'RRF', '2', 'CONSTANT', '60',
               'SORTBY', 'description', 'NOSORT').error().contains('NOSORT is not allowed with SORTBY')
    
    # Test NOSORT followed by SORTBY with COMBINE - should fail
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'COMBINE', 'RRF', '2', 'CONSTANT', '60', 
               'NOSORT', 'SORTBY', 'description').error().contains('NOSORT is not allowed with SORTBY')
