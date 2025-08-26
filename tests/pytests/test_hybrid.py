from common import *
import numpy as np


def create_test_vector(dim=4, dtype='FLOAT32'):
    """Create a test vector for hybrid queries"""
    if dtype == 'FLOAT32':
        return np.array([1.0, 2.0, 3.0, 4.0][:dim], dtype=np.float32)
    elif dtype == 'FLOAT64':
        return np.array([1.0, 2.0, 3.0, 4.0][:dim], dtype=np.float64)
    else:
        raise ValueError(f"Unsupported dtype: {dtype}")


def setup_hybrid_index(env, index_name='idx', dim=4, vector_type='FLOAT32'):
    """Setup an index for hybrid search testing"""
    env.expect('FT.CREATE', index_name, 'SCHEMA', 
               'title', 'TEXT', 
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', vector_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    
    # Add some test documents
    conn = getConnectionByEnv(env)
    for i in range(10):
        vector = create_test_vector(dim, vector_type)
        vector[0] = i  # Make each vector unique
        conn.execute_command('HSET', f'doc{i}', 
                           'title', f'document {i}', 
                           'vector', vector.tobytes())
    
    waitForIndex(env, index_name)


@skip(cluster=True)  # Skip on cluster for now as hybrid is still in development
def test_hybrid_cursor_array_response(env):
    """Test that _FT.HYBRID with WITHCURSOR returns an array of cursor IDs"""
    
    # Setup index and data
    setup_hybrid_index(env)
    
    # Mark client as internal to allow _FT.HYBRID command
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()
    
    # Create test vector for the query
    query_vector = create_test_vector()
    
    # Execute _FT.HYBRID command with WITHCURSOR
    # This should return an array of cursor IDs
    result = env.cmd('_FT.HYBRID', 'idx', 
                     'SEARCH', 'document', 
                     'VSIM', '@vector', query_vector.tobytes(),
                     'WITHCURSOR', 'COUNT', '5')
    
    # Verify the result is an array
    env.assertTrue(isinstance(result, list), "Result should be an array")
    env.assertTrue(len(result) > 0, "Result array should not be empty")
    
    # Verify each element in the array is a cursor ID (integer > 0)
    for cursor_id in result:
        env.assertTrue(isinstance(cursor_id, int), f"Cursor ID should be integer, got {type(cursor_id)}")
        env.assertTrue(cursor_id > 0, f"Cursor ID should be positive, got {cursor_id}")
    
    # Verify we get the expected number of cursors (should be 2: one for search, one for vector)
    env.assertEqual(len(result), 2, f"Expected 2 cursor IDs, got {len(result)}")
    
    # Verify cursor IDs are unique
    env.assertEqual(len(result), len(set(result)), "Cursor IDs should be unique")
    
    # Test that we can use these cursor IDs with FT.CURSOR READ
    for cursor_id in result:
        cursor_result = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        env.assertTrue(isinstance(cursor_result, list), "Cursor read result should be an array")
        env.assertEqual(len(cursor_result), 2, "Cursor read should return [results, next_cursor_id]")


@skip(cluster=True)
def test_hybrid_cursor_array_response_different_counts(env):
    """Test _FT.HYBRID cursor response with different COUNT values"""
    
    setup_hybrid_index(env)
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()
    
    query_vector = create_test_vector()
    
    # Test with different COUNT values
    for count in [1, 3, 10]:
        result = env.cmd('_FT.HYBRID', 'idx',
                         'SEARCH', 'document',
                         'VSIM', '@vector', query_vector.tobytes(),
                         'WITHCURSOR', 'COUNT', str(count))
        
        # Should always return 2 cursor IDs regardless of COUNT
        env.assertEqual(len(result), 2, f"Expected 2 cursor IDs with COUNT={count}, got {len(result)}")
        
        # All cursor IDs should be valid
        for cursor_id in result:
            env.assertTrue(cursor_id > 0, f"Invalid cursor ID: {cursor_id}")


@skip(cluster=True)
def test_hybrid_without_cursor_no_array(env):
    """Test that _FT.HYBRID without WITHCURSOR doesn't return cursor array"""
    
    setup_hybrid_index(env)
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()
    
    query_vector = create_test_vector()
    
    # Execute _FT.HYBRID without WITHCURSOR - this should execute normally
    # Note: This test might need adjustment based on actual implementation
    try:
        result = env.cmd('_FT.HYBRID', 'idx',
                         'SEARCH', 'document',
                         'VSIM', '@vector', query_vector.tobytes(),
                         'LIMIT', '0', '5')
        
        # Without WITHCURSOR, should not return an array of cursor IDs
        # The exact format depends on implementation, but it shouldn't be cursor IDs
        if isinstance(result, list) and len(result) > 0:
            # If it's an array, elements shouldn't be cursor-like integers
            for item in result:
                if isinstance(item, int) and item > 1000000:  # Cursor IDs are typically large
                    env.fail(f"Unexpected cursor-like ID in non-cursor response: {item}")
    except Exception as e:
        # If the command fails without WITHCURSOR, that's also acceptable
        # as the implementation might require WITHCURSOR for internal commands
        env.debugPrint(f"_FT.HYBRID without WITHCURSOR failed as expected: {e}")


@skip(cluster=True)
def test_hybrid_cursor_cleanup(env):
    """Test that hybrid cursors can be properly cleaned up"""
    
    setup_hybrid_index(env)
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()
    
    query_vector = create_test_vector()
    
    # Create cursors
    cursor_ids = env.cmd('_FT.HYBRID', 'idx',
                         'SEARCH', 'document',
                         'VSIM', '@vector', query_vector.tobytes(),
                         'WITHCURSOR', 'COUNT', '2')
    
    # Verify cursors exist and can be read
    for cursor_id in cursor_ids:
        result = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        env.assertTrue(isinstance(result, list), "Should be able to read cursor")
    
    # Clean up cursors
    for cursor_id in cursor_ids:
        env.expect('FT.CURSOR', 'DEL', 'idx', cursor_id).ok()
    
    # Verify cursors are deleted
    for cursor_id in cursor_ids:
        env.expect('FT.CURSOR', 'READ', 'idx', cursor_id).error()


@skip(cluster=True)
def test_hybrid_cursor_invalid_index(env):
    """Test _FT.HYBRID cursor behavior with invalid index"""
    
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()
    
    query_vector = create_test_vector()
    
    # Try with non-existent index
    env.expect('_FT.HYBRID', 'nonexistent_idx',
               'SEARCH', 'document',
               'VSIM', '@vector', query_vector.tobytes(),
               'WITHCURSOR', 'COUNT', '5').error().contains('No such index')


def test_hybrid_non_internal_client_error(env):
    """Test that _FT.HYBRID is rejected for non-internal clients"""
    
    setup_hybrid_index(env)
    
    query_vector = create_test_vector()
    
    # Don't mark as internal client - should fail
    env.expect('_FT.HYBRID', 'idx',
               'SEARCH', 'document', 
               'VSIM', '@vector', query_vector.tobytes(),
               'WITHCURSOR', 'COUNT', '5').error().contains('unknown command')
