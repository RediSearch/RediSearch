"""
Test for FT.AGGREGATE in cluster mode with random shard timeouts.
This test only runs in cluster mode and simulates random timeouts on shards
during aggregate operations.
"""

import time
import random
from common import *

@skip(cluster=False)
def test_cluster_aggregate_random_timeout(env):
    """
    Test FT.AGGREGATE in cluster mode where one of the shards randomly times out
    during private command processing.
    """
    if not env.isCluster():
        env.skip()

    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'price', 'NUMERIC', 'category', 'TAG').ok()

    # Add test documents across multiple shards
    conn = getConnectionByEnv(env)
    num_docs = 100

    for i in range(num_docs):
        doc_key = f'doc:{i}'
        title = f'Product {i} title with keywords'
        price = random.randint(10, 1000)
        category = random.choice(['electronics', 'books', 'clothing', 'home'])

        conn.execute_command('HSET', doc_key,
                           'title', title,
                           'price', price,
                           'category', category)

    # Wait for indexing to complete
    waitForIndex(env, 'idx')

    # Verify we have documents indexed
    info = index_info(env, 'idx')
    env.assertGreater(int(info['num_docs']), 0)

    # Test basic aggregate without timeout first
    result = env.cmd('FT.AGGREGATE', 'idx', '*',
                     'GROUPBY', '1', '@category',
                     'REDUCE', 'COUNT', '0', 'AS', 'count',
                     'REDUCE', 'AVG', '1', '@price', 'AS', 'avg_price')

    env.assertGreater(len(result), 1)  # Should have results

    # Now test with debug timeout simulation
    # This will cause one of the shards to timeout randomly during processing
    try:
        result = env.cmd('_FT.DEBUG', 'FT.AGGREGATE', 'idx', '*',
                        'GROUPBY', '1', '@category',
                        'REDUCE', 'COUNT', '0', 'AS', 'count',
                        'REDUCE', 'AVG', '1', '@price', 'AS', 'avg_price',
                        'TIMEOUT_AFTER_N', '10',  # Timeout after processing 10 results
                        'DEBUG_PARAMS_COUNT', '2')

        # If we get here, the query completed before timeout
        env.assertGreater(len(result), 1)

    except Exception as e:
        # Expected timeout error
        error_msg = str(e)
        env.assertTrue('Timeout' in error_msg or 'timeout' in error_msg)

@skip(cluster=False)
def test_cluster_aggregate_cursor_timeout(env):
    """
    Test FT.AGGREGATE with cursor in cluster mode with random shard timeouts.
    """
    if not env.isCluster():
        env.skip()

    # Create index
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'content', 'TEXT', 'score', 'NUMERIC').ok()

    # Add more documents to test cursor behavior
    conn = getConnectionByEnv(env)
    num_docs = 200

    for i in range(num_docs):
        doc_key = f'doc2:{i}'
        content = f'Document {i} with some content for testing aggregation'
        score = random.randint(1, 100)

        conn.execute_command('HSET', doc_key,
                           'content', content,
                           'score', score)

    # Wait for indexing to complete
    waitForIndex(env, 'idx2')

    # Test aggregate with cursor and potential timeout
    try:
        result, cursor = env.cmd('FT.AGGREGATE', 'idx2', '*',
                               'LOAD', '2', '@content', '@score',
                               'APPLY', '@score * 2', 'AS', 'double_score',
                               'SORTBY', '2', '@score', 'DESC',
                               'WITHCURSOR', 'COUNT', '50')

        env.assertGreater(len(result), 1)

        # Continue reading with cursor if available
        total_results = len(result)
        while cursor and cursor != 0:
            try:
                result, cursor = env.cmd('FT.CURSOR', 'READ', 'idx2', cursor)
                total_results += len(result)
            except Exception as e:
                # Cursor might timeout or be cleaned up
                break

        env.assertGreater(total_results, 50)

    except Exception as e:
        # Expected timeout or error during cursor operations
        error_msg = str(e)
        env.assertTrue('Timeout' in error_msg or 'timeout' in error_msg or 'Cursor' in error_msg)

@skip(cluster=False)
def test_cluster_aggregate_complex_query_timeout(env):
    """
    Test complex FT.AGGREGATE query in cluster mode with potential timeouts.
    """
    if not env.isCluster():
        env.skip()

    # Create index with multiple field types
    env.expect('FT.CREATE', 'idx3', 'SCHEMA',
               'name', 'TEXT', 'SORTABLE',
               'age', 'NUMERIC', 'SORTABLE',
               'city', 'TAG', 'SORTABLE',
               'salary', 'NUMERIC').ok()

    # Add test data
    conn = getConnectionByEnv(env)
    cities = ['New York', 'London', 'Tokyo', 'Paris', 'Berlin']
    names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry']

    num_docs = 150
    for i in range(num_docs):
        doc_key = f'person:{i}'
        name = random.choice(names) + f' {i}'
        age = random.randint(20, 65)
        city = random.choice(cities)
        salary = random.randint(30000, 150000)

        conn.execute_command('HSET', doc_key,
                           'name', name,
                           'age', age,
                           'city', city,
                           'salary', salary)

    # Wait for indexing
    waitForIndex(env, 'idx3')

    # Complex aggregate query that might timeout on some shards
    try:
        result = env.cmd('FT.AGGREGATE', 'idx3', '*',
                        'GROUPBY', '2', '@city', '@age',
                        'REDUCE', 'COUNT', '0', 'AS', 'count',
                        'REDUCE', 'AVG', '1', '@salary', 'AS', 'avg_salary',
                        'REDUCE', 'MAX', '1', '@salary', 'AS', 'max_salary',
                        'REDUCE', 'MIN', '1', '@salary', 'AS', 'min_salary',
                        'SORTBY', '2', '@avg_salary', 'DESC',
                        'LIMIT', '0', '20')

        env.assertGreater(len(result), 1)

        # Verify result structure
        for row in result[1:]:  # Skip count
            env.assertTrue(isinstance(row, list))
            env.assertGreater(len(row), 6)  # Should have city, age, count, avg_salary, max_salary, min_salary

    except Exception as e:
        # May timeout due to complex processing
        error_msg = str(e)
        env.assertTrue('Timeout' in error_msg or 'timeout' in error_msg)
