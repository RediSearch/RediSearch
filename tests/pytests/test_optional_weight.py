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
