# -*- coding: utf-8 -*-

"""
Test suite for optional weight functionality in RediSearch.

This module tests the optional weight syntax: ~(condition=>{$weight:N})
which allows specifying optional conditions with custom weights in search queries.

The tests cover:
1. Basic optional weight functionality with TAG fields
2. Impact of optional weights on scoring
3. Multiple optional conditions in a single query
"""

from includes import *
from common import *
import time

def test_optional_weight_basic():
    """Test basic optional weight functionality with TAG fields"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    conn = getConnectionByEnv(env)

    # Create index with TAG and TEXT fields - use unique name per test run
    index_name = f'products_idx_basic_{int(time.time() * 1000)}'
    env.expect('FT.CREATE', index_name, 'SCHEMA',
               'product_id', 'TAG',
               'media_type', 'TAG',
               'type', 'TAG',
               'description', 'TEXT').ok()

    waitForIndex(env, index_name)

    # Populate with test documents
    test_docs = [
        ('doc1', {'product_id': 'p005', 'media_type': 'picture', 'type': 'electronics', 'description': 'High quality camera'}),
        ('doc2', {'product_id': 'p015', 'media_type': 'video', 'type': 'electronics', 'description': 'Professional video camera'}),
        ('doc3', {'product_id': 'p025', 'media_type': 'picture', 'type': 'accessories', 'description': 'Camera lens attachment'}),
        ('doc4', {'product_id': 'p035', 'media_type': 'picture', 'type': 'electronics', 'description': 'Digital photo frame'}),
        ('doc5', {'product_id': 'p045', 'media_type': 'audio', 'type': 'electronics', 'description': 'Wireless headphones'}),
        ('doc6', {'product_id': 'p005', 'media_type': 'video', 'type': 'electronics', 'description': 'Action camera with video'}),
        ('doc7', {'product_id': 'p015', 'media_type': 'picture', 'type': 'accessories', 'description': 'Photo editing software'}),
        ('doc8', {'product_id': 'p025', 'media_type': 'audio', 'type': 'electronics', 'description': 'Microphone for recording'}),
    ]

    for doc_id, fields in test_docs:
        conn.execute_command('HSET', doc_id,
                           'product_id', fields['product_id'],
                           'media_type', fields['media_type'],
                           'type', fields['type'],
                           'description', fields['description'])


    # Test the optional weight query
    query = "~(@media_type:{picture}=>{$weight:4})"

    res = env.cmd('FT.SEARCH', index_name, query,
                  'WITHSCORES', 'SCORER', 'BM25',
                  'RETURN', '3', 'type', 'media_type', 'description',
                  'LIMIT', '0', '100',
                  'DIALECT', '2',
                  'EXPLAINSCORE')

    print(res)

    time.sleep(10)
    print(f'NEWW ONE!!!')
    # Test the optional weight query
    query = "(~(@media_type:{picture}))=>{$weight:4}"

    res = env.cmd('FT.SEARCH', index_name, query,
                  'WITHSCORES', 'SCORER', 'BM25',
                  'RETURN', '3', 'type', 'media_type', 'description',
                  'LIMIT', '0', '100',
                  'DIALECT', '2',
                  'EXPLAINSCORE')
    print(res)

    query = "(~(@media_type:{picture})=>{$weight:2})=>{$weight:4}"
    time.sleep(10)
    print(f'NEWW ONE!!!')
    res = env.cmd('FT.SEARCH', index_name, query,
                  'WITHSCORES', 'SCORER', 'BM25',
                  'RETURN', '3', 'type', 'media_type', 'description',
                  'LIMIT', '0', '100',
                  'DIALECT', '2',
                  'EXPLAINSCORE')

    print(res)
    # Verify we get results
    env.assertGreater(res[0], 0)

    # Parse results based on observed format: [count, doc_id1, [score1, explain1], fields1, doc_id2, [score2, explain2], fields2, ...]
    count = res[0]
    env.assertGreater(count, 0)

    # Extract scores and verify they are positive
    scores = []
    doc_ids = []

    # Results start at index 1, with pattern: doc_id, [score, explain], fields
    i = 1
    while i < len(res):
        if i + 2 < len(res):
            doc_id = res[i]
            score_and_explain = res[i + 1]
            fields = res[i + 2]

            doc_ids.append(doc_id)
            if isinstance(score_and_explain, list) and len(score_and_explain) > 0:
                score = float(score_and_explain[0])
                scores.append(score)
                env.assertGreater(score, 0)

            i += 3  # Move to next result
        else:
            break

    # Verify we found some results with valid scores
    env.assertGreater(len(scores), 0)
    env.assertGreater(len(doc_ids), 0)

    # Verify the query is working by checking we have expected number of results
    env.assertEqual(count, len(doc_ids))

# def test_optional_weight_scoring_impact(env):
#     """Test that optional weight actually affects scoring"""
#     conn = getConnectionByEnv(env)

#     # Create index with unique name
#     index_name = f'test_idx_scoring_{int(time.time() * 1000)}'
#     env.expect('FT.CREATE', index_name, 'SCHEMA',
#                'product_id', 'TAG',
#                'media_type', 'TAG',
#                'description', 'TEXT').ok()

#     waitForIndex(env, index_name)

#     # Add documents - some with picture media_type, some without
#     conn.execute_command('HSET', 'doc1',
#                        'product_id', 'p001',
#                        'media_type', 'picture',
#                        'description', 'test document')

#     conn.execute_command('HSET', 'doc2',
#                        'product_id', 'p001',
#                        'media_type', 'video',
#                        'description', 'test document')

#     # Query without optional weight
#     res1 = env.cmd('FT.SEARCH', index_name, '@product_id:{p001}',
#                    'WITHSCORES', 'SCORER', 'BM25',
#                    'DIALECT', '2')

#     # Query with optional weight for picture media_type
#     res2 = env.cmd('FT.SEARCH', index_name, '@product_id:{p001} ~(@media_type:{picture}=>{$weight:4})',
#                    'WITHSCORES', 'SCORER', 'BM25',
#                    'DIALECT', '2')

#     # Both queries should return results
#     env.assertGreater(res1[0], 0)
#     env.assertGreater(res2[0], 0)

#     # Extract scores for comparison
#     scores1 = []
#     scores2 = []

#     for i in range(2, len(res1), 3):  # Skip count and doc_id, get scores
#         if i < len(res1):
#             scores1.append(float(res1[i]))

#     for i in range(2, len(res2), 3):  # Skip count and doc_id, get scores
#         if i < len(res2):
#             scores2.append(float(res2[i]))

#     # Verify we have scores from both queries
#     env.assertGreater(len(scores1), 0)
#     env.assertGreater(len(scores2), 0)

# def test_optional_weight_multiple_conditions(env):
#     """Test optional weight with multiple optional conditions"""
#     conn = getConnectionByEnv(env)

#     # Create index with unique name
#     index_name = f'multi_idx_{int(time.time() * 1000)}'
#     env.expect('FT.CREATE', index_name, 'SCHEMA',
#                'category', 'TAG',
#                'brand', 'TAG',
#                'color', 'TAG',
#                'description', 'TEXT').ok()

#     waitForIndex(env, index_name)

#     # Add test documents
#     test_data = [
#         ('item1', {'category': 'electronics', 'brand': 'sony', 'color': 'black', 'description': 'premium device'}),
#         ('item2', {'category': 'electronics', 'brand': 'apple', 'color': 'white', 'description': 'innovative product'}),
#         ('item3', {'category': 'clothing', 'brand': 'nike', 'color': 'black', 'description': 'sports apparel'}),
#     ]

#     for doc_id, fields in test_data:
#         conn.execute_command('HSET', doc_id,
#                            'category', fields['category'],
#                            'brand', fields['brand'],
#                            'color', fields['color'],
#                            'description', fields['description'])

#     # Query with multiple optional weights
#     query = '@category:{electronics} ~(@brand:{sony}=>{$weight:3}) ~(@color:{black}=>{$weight:2})'

#     res = env.cmd('FT.SEARCH', index_name, query,
#                   'WITHSCORES', 'SCORER', 'BM25',
#                   'DIALECT', '2')

#     # Verify we get results
#     env.assertGreater(res[0], 0)

#     # Verify scores are present
#     for i in range(2, len(res), 3):
#         if i < len(res):
#             score = float(res[i])
#             env.assertGreater(score, 0)
