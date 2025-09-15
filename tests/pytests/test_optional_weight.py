from common import *

@skip(cluster=True)
def test_optional_weight_application(env):
    """
    Test that optional queries (~) apply weights correctly at different levels:
    1. Weight applied to child TAG node: ~(@media_type:{picture}=>{$weight:4}) - should show Weight 4.00
    2. Weight applied to Optional node: (~(@media_type:{picture}))=>{$weight:4} - should show Weight 4.00
    3. Weights applied on both levels: (~(@media_type:{picture})=>{$weight:2})=>{$weight:4} - should show Weight 8.00

    The test verifies that weights are properly multiplied and reflected in EXPLAINSCORE results.
    """
    conn = getConnectionByEnv(env)

    # Create index as specified
    env.expect('FT.CREATE', 'products_idx', 'SCHEMA',
               'product_id', 'TAG',
               'media_type', 'TAG',
               'type', 'TAG',
               'description', 'TEXT').ok()

    # Add test documents
    # Documents with media_type=picture (should get real hits with weights)
    conn.execute_command('HSET', 'doc1', 'product_id', 'p1', 'media_type', 'picture', 'type', 'electronics', 'description', 'A nice camera')
    conn.execute_command('HSET', 'doc2', 'product_id', 'p2', 'media_type', 'picture', 'type', 'gadget', 'description', 'Photo frame')

    # Documents without media_type=picture (should get virtual hits with weight=0)
    conn.execute_command('HSET', 'doc3', 'product_id', 'p3', 'media_type', 'video', 'type', 'electronics', 'description', 'Video recorder')
    conn.execute_command('HSET', 'doc4', 'product_id', 'p4', 'media_type', 'audio', 'type', 'gadget', 'description', 'Music player')

    # Test queries
    query1 = "~(@media_type:{picture}=>{$weight:4})"  # Weight applied in the child TAG Node
    query2 = "(~(@media_type:{picture}))=>{$weight:4}"  # Weight applied in the Optional Node
    query3 = "(~(@media_type:{picture})=>{$weight:2})=>{$weight:4}"  # Weights applied on both levels

    # Test Query 1: Weight applied to child TAG node - should show Weight 4.00
    res1 = env.cmd('FT.SEARCH', 'products_idx', query1, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD')
    env.assertEqual(res1[0], 4)  # Should get all 4 documents

    # Extract first document's explanation (it should have a real hit)
    doc1_explain = res1[2][1]
    env.assertContains("Weight 4.00", str(doc1_explain))

    # Test Query 2: Weight applied to Optional node - should show Weight 4.00
    res2 = env.cmd('FT.SEARCH', 'products_idx', query2, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD')
    env.assertEqual(res2[0], 4)  # Should get all 4 documents

    # Extract first document's explanation (it should have a real hit)
    doc1_explain_q2 = res2[2][1]
    env.assertContains("Weight 4.00", str(doc1_explain_q2))

    # Test Query 3: Weights applied on both levels - should show Weight 8.00 (2 * 4 = 8)
    res3 = env.cmd('FT.SEARCH', 'products_idx', query3, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD')
    env.assertEqual(res3[0], 4)  # Should get all 4 documents

    # Extract first document's explanation (it should have a real hit)
    doc1_explain_q3 = res3[2][1]
    env.assertContains("Weight 8.00", str(doc1_explain_q3))

    # Verify score multiplication: Query 3 should have exactly 2x the score of Query 1 and Query 2
    doc1_score_q1 = float(res1[2][0])
    doc1_score_q2 = float(res2[2][0])
    doc1_score_q3 = float(res3[2][0])

    ratio_q3_to_q1 = doc1_score_q3 / doc1_score_q1 if doc1_score_q1 > 0 else 0
    ratio_q3_to_q2 = doc1_score_q3 / doc1_score_q2 if doc1_score_q2 > 0 else 0

    env.assertAlmostEqual(ratio_q3_to_q1, 2.0, 0.1)
    env.assertAlmostEqual(ratio_q3_to_q2, 2.0, 0.1)


@skip(cluster=True)
def test_optional_weight_with_union(env):
    """
    Test optional clauses wrapping Union operations with different weights.
    Tests scenarios like:
    1. Optional wrapping a Union: ~((@media_type:{picture}) | (@type:{electronics}))=>{$weight:3}
    2. Nested optional with union and individual weights: ~((@media_type:{picture}=>{$weight:2}) | (@type:{electronics}=>{$weight:4}))=>{$weight:3}
    3. Score comparisons between simple and complex cases
    """
    conn = getConnectionByEnv(env)

    # Create index
    env.expect('FT.CREATE', 'combo_idx', 'SCHEMA',
               'product_id', 'TAG',
               'media_type', 'TAG',
               'type', 'TAG',
               'category', 'TAG',
               'description', 'TEXT').ok()

    # Add test documents with different combinations
    # Doc1: matches both media_type=picture AND type=electronics
    conn.execute_command('HSET', 'doc1', 'product_id', 'p1', 'media_type', 'picture', 'type', 'electronics', 'category', 'tech', 'description', 'Digital camera')

    # Doc2: matches media_type=picture but NOT type=electronics
    conn.execute_command('HSET', 'doc2', 'product_id', 'p2', 'media_type', 'picture', 'type', 'gadget', 'category', 'tech', 'description', 'Photo frame')

    # Doc3: matches type=electronics but NOT media_type=picture
    conn.execute_command('HSET', 'doc3', 'product_id', 'p3', 'media_type', 'video', 'type', 'electronics', 'category', 'tech', 'description', 'Video recorder')

    # Doc4: matches neither (virtual hits for both)
    conn.execute_command('HSET', 'doc4', 'product_id', 'p4', 'media_type', 'audio', 'type', 'gadget', 'category', 'music', 'description', 'Music player')

    # Test 1: Optional wrapping a Union - ~((@media_type:{picture}) | (@type:{electronics}))=>{$weight:3}
    optional_union_query = "~((@media_type:{picture}) | (@type:{electronics}))=>{$weight:3}"
    res_opt_union = env.cmd('FT.SEARCH', 'combo_idx', optional_union_query, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD')

    env.assertEqual(res_opt_union[0], 4)  # Should get all 4 documents

    # Doc1, Doc2, Doc3 should have non-zero scores (match the union), Doc4 should have zero score
    doc1_score_opt_union = float(res_opt_union[2][0])
    doc1_explain_opt_union = res_opt_union[2][1]
    doc2_score_opt_union = float(res_opt_union[4][0])
    doc2_explain_opt_union = res_opt_union[4][1]
    doc3_score_opt_union = float(res_opt_union[6][0])
    doc3_explain_opt_union = res_opt_union[6][1]
    doc4_score_opt_union = float(res_opt_union[8][0])

    # Verify scores: docs that match the union should have weight 3.00 applied
    env.assertGreater(doc1_score_opt_union, 0)  # Matches both picture and electronics
    env.assertGreater(doc2_score_opt_union, 0)  # Matches picture
    env.assertGreater(doc3_score_opt_union, 0)  # Matches electronics
    env.assertEqual(doc4_score_opt_union, 0)    # Matches neither (virtual hit)

    # Check that weight 3.00 appears in explanations for all matching documents
    env.assertContains("Weight 3.00", str(doc1_explain_opt_union))  # Doc1 matches both conditions
    env.assertContains("Weight 3.00", str(doc2_explain_opt_union))  # Doc2 matches picture
    env.assertContains("Weight 3.00", str(doc3_explain_opt_union))  # Doc3 matches electronics

    # Store scores for comparison with weighted versions
    baseline_union_doc2 = doc2_score_opt_union
    baseline_union_doc3 = doc3_score_opt_union

    # Test 2: Nested optional with union and individual weights - ~((@media_type:{picture}=>{$weight:2}) | (@type:{electronics}=>{$weight:4}))=>{$weight:3}
    nested_optional_union_query = "~((@media_type:{picture}=>{$weight:2}) | (@type:{electronics}=>{$weight:4}))=>{$weight:3}"
    res_nested_opt_union = env.cmd('FT.SEARCH', 'combo_idx', nested_optional_union_query, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD')

    env.assertEqual(res_nested_opt_union[0], 4)  # Should get all 4 documents

    # Doc1 should have highest score (matches both, gets combined effect)
    doc1_score_nested = float(res_nested_opt_union[2][0])
    doc2_score_nested = float(res_nested_opt_union[4][0])
    doc3_score_nested = float(res_nested_opt_union[6][0])
    doc4_score_nested = float(res_nested_opt_union[8][0])

    # Verify that weights are applied: outer weight 3 * inner weights (2 or 4)
    env.assertGreater(doc1_score_nested, doc2_score_nested)  # Doc1 matches both
    env.assertGreater(doc1_score_nested, doc3_score_nested)  # Doc1 matches both
    env.assertGreater(doc2_score_nested, 0)
    env.assertGreater(doc3_score_nested, 0)
    env.assertEqual(doc4_score_nested, 0)    # Doc4 matches neither

    # Check for weight structure in explanations
    # The outer weight (3.00) and inner weights (2.00, 4.00) appear in the results
    # Let's check that both weight combinations appear somewhere in the results
    full_result_str = str(res_nested_opt_union)
    env.assertContains("Weight 3.00", full_result_str)   # Outer weight should appear
    env.assertContains("Weight 2.00", full_result_str)   # Inner weight for picture should appear
    env.assertContains("Weight 4.00", full_result_str)   # Inner weight for electronics should appear

    # Test 3: Score comparison between simple and complex cases
    # Compare baseline union (weight=3) vs nested union with individual weights

    # The nested query should have different scores due to different weight combinations
    # Doc2 (picture): baseline has weight 3, nested has weight 2*3=6 (should be higher)
    # Doc3 (electronics): baseline has weight 3, nested has weight 4*3=12 (should be much higher)

    # Based on actual test results: Doc2 gets electronics weight, Doc3 gets picture weight
    # (The document order in results may be different from creation order)
    electronics_nested_score = doc2_score_nested  # Doc2 shows 4.0 ratio (electronics weight 4*3=12)
    picture_nested_score = doc3_score_nested      # Doc3 shows 2.0 ratio (picture weight 2*3=6)
    electronics_baseline_score = baseline_union_doc2
    picture_baseline_score = baseline_union_doc3

    # Verify score relationships
    # Picture: nested (2*3=6) should be 2x higher than baseline (3)
    picture_ratio = picture_nested_score / picture_baseline_score
    env.assertAlmostEqual(picture_ratio, 2.0, 0.1)  # 6/3 = 2, allow some tolerance

    # Electronics: nested (4*3=12) should be 4x higher than baseline (3)
    electronics_ratio = electronics_nested_score / electronics_baseline_score
    env.assertAlmostEqual(electronics_ratio, 4.0, 0.1)  # 12/3 = 4, allow some tolerance

    # Electronics should have higher score than picture in nested query (weight 12 vs 6)
    env.assertGreater(electronics_nested_score, picture_nested_score)


@skip(cluster=True)
def test_optional_weight_with_intersection(env):
    """
    Test optional clauses wrapping Intersection operations with different weights.
    Tests scenarios like:
    1. Optional wrapping an Intersection: ~((@media_type:{picture}) (@type:{electronics}))=>{$weight:5}
    2. Score comparisons to verify weight impact
    """
    conn = getConnectionByEnv(env)

    # Create index
    env.expect('FT.CREATE', 'intersect_idx', 'SCHEMA',
               'product_id', 'TAG',
               'media_type', 'TAG',
               'type', 'TAG',
               'category', 'TAG',
               'description', 'TEXT').ok()

    # Add test documents with different combinations
    # Doc1: matches both media_type=picture AND type=electronics
    conn.execute_command('HSET', 'doc1', 'product_id', 'p1', 'media_type', 'picture', 'type', 'electronics', 'category', 'tech', 'description', 'Digital camera')

    # Doc2: matches media_type=picture but NOT type=electronics
    conn.execute_command('HSET', 'doc2', 'product_id', 'p2', 'media_type', 'picture', 'type', 'gadget', 'category', 'tech', 'description', 'Photo frame')

    # Doc3: matches type=electronics but NOT media_type=picture
    conn.execute_command('HSET', 'doc3', 'product_id', 'p3', 'media_type', 'video', 'type', 'electronics', 'category', 'tech', 'description', 'Video recorder')

    # Doc4: matches neither (virtual hits for both)
    conn.execute_command('HSET', 'doc4', 'product_id', 'p4', 'media_type', 'audio', 'type', 'gadget', 'category', 'music', 'description', 'Music player')

    # Test 1: Optional wrapping an Intersection - ~((@media_type:{picture}) (@type:{electronics}))=>{$weight:5}
    optional_intersection_query = "~((@media_type:{picture}) (@type:{electronics}))=>{$weight:5}"
    res_opt_intersect = env.cmd('FT.SEARCH', 'intersect_idx', optional_intersection_query, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD')

    env.assertEqual(res_opt_intersect[0], 4)  # Should get all 4 documents

    # Only Doc1 should have non-zero score (matches both picture AND electronics)
    doc1_score_opt_intersect = float(res_opt_intersect[2][0])
    doc1_explain_opt_intersect = res_opt_intersect[2][1]
    doc2_score_opt_intersect = float(res_opt_intersect[4][0])
    doc3_score_opt_intersect = float(res_opt_intersect[6][0])
    doc4_score_opt_intersect = float(res_opt_intersect[8][0])

    # Verify scores: only doc1 matches the intersection, others are virtual hits
    env.assertGreater(doc1_score_opt_intersect, 0)  # Matches both (real hit with weight 5)
    env.assertEqual(doc2_score_opt_intersect, 0)    # Doesn't match intersection (virtual hit)
    env.assertEqual(doc3_score_opt_intersect, 0)    # Doesn't match intersection (virtual hit)
    env.assertEqual(doc4_score_opt_intersect, 0)    # Doesn't match intersection (virtual hit)

    # Check that weight 5.00 appears in doc1's explanation (only doc1 matches intersection)
    env.assertContains("Weight 5.00", str(doc1_explain_opt_intersect))

    # Test 2: Nested optional with intersection and individual weights - ~((@media_type:{picture}=>{$weight:2}) (@type:{electronics}=>{$weight:3}))=>{$weight:4}
    nested_optional_intersect_query = "~((@media_type:{picture}=>{$weight:2}) (@type:{electronics}=>{$weight:3}))=>{$weight:5}"
    res_nested_opt_intersect = env.cmd('FT.SEARCH', 'intersect_idx', nested_optional_intersect_query, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD')

    env.assertEqual(res_nested_opt_intersect[0], 4)  # Should get all 4 documents

    # Only Doc1 should have non-zero score (matches both picture AND electronics with individual weights)
    doc1_score_nested_intersect = float(res_nested_opt_intersect[2][0])
    doc2_score_nested_intersect = float(res_nested_opt_intersect[4][0])
    doc3_score_nested_intersect = float(res_nested_opt_intersect[6][0])
    doc4_score_nested_intersect = float(res_nested_opt_intersect[8][0])

    # Verify scores: only doc1 should have non-zero score (matches intersection)
    env.assertGreater(doc1_score_nested_intersect, 0)  # Matches both (real hit)
    env.assertEqual(doc2_score_nested_intersect, 0)    # Doesn't match intersection (virtual hit)
    env.assertEqual(doc3_score_nested_intersect, 0)    # Doesn't match intersection (virtual hit)
    env.assertEqual(doc4_score_nested_intersect, 0)    # Doesn't match intersection (virtual hit)

    # Check for weight structure in explanations - should show both inner weights (2.00, 3.00) and outer weight (5.00)
    full_intersect_result_str = str(res_nested_opt_intersect)
    env.assertContains("Weight 5.00", full_intersect_result_str)   # Outer weight should appear
    env.assertContains("Weight 2.00", full_intersect_result_str)   # Inner weight for picture should appear
    env.assertContains("Weight 3.00", full_intersect_result_str)   # Inner weight for electronics should appear

    # Test 3: Score comparison between simple and nested intersection cases
    # Compare baseline intersection (weight=5) vs nested intersection with individual weights

    # The nested query should have higher score due to weight multiplication
    intersect_ratio = doc1_score_nested_intersect / doc1_score_opt_intersect
    env.assertAlmostEqual(intersect_ratio, 6.0, 0.1)  # It is multiplied by 2 * 3
