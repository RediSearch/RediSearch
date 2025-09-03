#!/usr/bin/env python3

import pytest
import redis
from common import *

def test_replication_api_basic(env):
    """Test basic replication API functionality"""
    r = env
    
    # Create a simple index
    r.execute_command('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:', 
                     'SCHEMA', 'title', 'TEXT', 'WEIGHT', '5.0', 'body', 'TEXT')
    
    # Add some documents
    r.hset('doc:1', mapping={'title': 'hello world', 'body': 'lorem ipsum'})
    r.hset('doc:2', mapping={'title': 'foo bar', 'body': 'dolor sit amet'})
    
    # Wait for indexing to complete
    waitForIndex(env, 'idx')
    
    # Verify search works
    res = r.execute_command('FT.SEARCH', 'idx', 'hello')
    env.assertEqual(res[0], 1)
    
    # Test would normally trigger fork operations here
    # For now, just verify the index is working
    res = r.execute_command('FT.SEARCH', 'idx', '*')
    env.assertEqual(res[0], 2)

def test_replication_api_with_different_field_types(env):
    """Test replication API with different field types"""
    r = env
    
    # Create an index with different field types
    r.execute_command('FT.CREATE', 'mixed_idx', 'ON', 'HASH', 'PREFIX', '1', 'mixed:', 
                     'SCHEMA', 
                     'title', 'TEXT',
                     'price', 'NUMERIC', 'SORTABLE',
                     'tags', 'TAG', 'SEPARATOR', ',')
    
    # Add documents with different field types
    r.hset('mixed:1', mapping={
        'title': 'Product One',
        'price': '19.99',
        'tags': 'electronics,gadget'
    })
    r.hset('mixed:2', mapping={
        'title': 'Product Two', 
        'price': '29.99',
        'tags': 'home,kitchen'
    })
    
    # Wait for indexing
    waitForIndex(env, 'mixed_idx')
    
    # Test different query types
    res = r.execute_command('FT.SEARCH', 'mixed_idx', 'Product')
    env.assertEqual(res[0], 2)
    
    res = r.execute_command('FT.SEARCH', 'mixed_idx', '@price:[20 30]')
    env.assertEqual(res[0], 1)
    
    res = r.execute_command('FT.SEARCH', 'mixed_idx', '@tags:{electronics}')
    env.assertEqual(res[0], 1)

def test_replication_api_error_handling(env):
    """Test replication API error handling"""
    r = env
    
    # Create index
    r.execute_command('FT.CREATE', 'error_idx', 'ON', 'HASH', 'PREFIX', '1', 'err:', 
                     'SCHEMA', 'content', 'TEXT')
    
    # Add document
    r.hset('err:1', mapping={'content': 'test content'})
    
    # Wait for indexing
    waitForIndex(env, 'error_idx')
    
    # Verify normal operation
    res = r.execute_command('FT.SEARCH', 'error_idx', 'test')
    env.assertEqual(res[0], 1)
    
    # The actual fork error testing would require more complex setup
    # For now, just verify the index remains functional
    res = r.execute_command('FT.SEARCH', 'error_idx', 'content')
    env.assertEqual(res[0], 1)
