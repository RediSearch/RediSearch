# -*- coding: utf-8 -*-

from includes import *
from common import *

def verify_highlighted_words(env, result, expected_highlighted_words):
    # Verify we got results
    env.assertEqual(result[0], 1)

    # Parse fields from result
    doc_fields = result[2]
    field_dict = {doc_fields[i]: doc_fields[i+1] for i in range(0, len(doc_fields), 2)}
    words = []
    for field_name, field_value in field_dict.items():
        words.extend(field_value.split())
    highlighted = set()
    for word in words:
        if '<b>' in word:
            highlighted.add(word)
    env.assertEqual(highlighted, expected_highlighted_words)


# simply highlight check
def test_highlight_simple(env):
    """Minimal test case for highlighting bug fix"""
    conn = getConnectionByEnv(env)
    # Create minimal index
    env.expect('FT.CREATE', 'minimal_index', 'ON', 'HASH', 'STOPWORDS', '0', 'SCHEMA',
               'id', 'TEXT', 'NOSTEM', 'title', 'TEXT', 'NOSTEM', 'content', 'TEXT', 'NOSTEM').ok()

    waitForIndex(env, 'minimal_index')

    # Add document
    conn.hset('doc:1', mapping={
        'id': 'DOC_001',
        'title': 'Product',
        'content': 'This Product is available now'
    })

    # Search with highlighting
    result = env.cmd('FT.SEARCH', 'minimal_index', 'Product', 'HIGHLIGHT')
    verify_highlighted_words(env, result, {'<b>Product</b>'})


# Test missing token count doesn'affect highlight result
def test_highlight_empty_id(env):
    """Test highlighting with empty id field"""
    conn = getConnectionByEnv(env)
    # Create minimal index
    env.expect('FT.CREATE', 'empty_id_index', 'ON', 'HASH', 'STOPWORDS', '0', 'SCHEMA',
               'title', 'TEXT', 'NOSTEM', 'content', 'TEXT', 'NOSTEM').ok()
    waitForIndex(env, 'empty_id_index')
    # Add document
    conn.hset('doc:1', mapping={
        'id': "",
        'title': 'Product',
        'content': 'This Product is available now'
    })
    # Search with highlighting
    result = env.cmd('FT.SEARCH', 'empty_id_index', 'Product', 'HIGHLIGHT')
    verify_highlighted_words(env, result, {'<b>Product</b>'})
