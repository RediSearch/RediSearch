# -*- coding: utf-8 -*-

from includes import *
from common import *

def verify_word_is_highlighted(env, result, word_to_check):
    # Verify we got results
    env.assertEqual(result[0], 1)

    # Parse fields from result
    doc_fields = result[2]
    field_dict = {doc_fields[i]: doc_fields[i+1] for i in range(0, len(doc_fields), 2)}

    # Check each field for highlighting issues
    for field_name, field_value in field_dict.items():
        # Check if there are any broken highlights (e.g., "<b>d</b>" instead of full word)
        if '<b>' in str(field_value) and '</b>' in str(field_value):
            # Extract all highlighted portions
            import re
            highlighted_parts = re.findall(r'<b>(.*?)</b>', str(field_value))
            for part in highlighted_parts:
                # If a highlighted part is just a single character or looks broken, fail
                if len(part) == 1 and part.isalpha():
                    env.assertTrue(False, message=f"Broken highlighting in field '{field_name}': found '<b>{part}</b>' in '{field_value}'")

    # Also check that the expected word is highlighted somewhere
    words = []
    for field_name, field_value in field_dict.items():
        words.extend(str(field_value).split())
    highlighted = set()
    for word in words:
        if '<b>' in word:
            highlighted.add(word)
    # Check that the expected word is in the highlighted set
    env.assertIn(word_to_check, highlighted)
    env.assertEqual(len(highlighted), 1, message=f"We only expect the given word to be highlighted, but found also others: {highlighted}") # it is the only one highlighted



# Test highlighting with complex real-world schema
def test_highlight_complex_schema_mod_11233(env):
    """Test highlighting with complex schema similar to production use case"""
    conn = getConnectionByEnv(env)
        # Create index with many fields (anonymized with city names)
    env.expect('FT.CREATE', 'large_index', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
               'lisbon', 'TEXT', 'NOSTEM',
               'vienna', 'TEXT', 'NOSTEM',
               'tokyo', 'TEXT', 'NOSTEM',
               'seattle', 'TEXT', 'NOSTEM').ok()

    waitForIndex(env, 'large_index')

    # Add document with city field names
    conn.hset('doc:4153814', mapping={
        'vienna': 'word here Alert',
        'lisbon': 'other sentence',
        'tokyo': '',
        'seattle': 'This Alert triggered for potential activity and will be included in the automated review process.',
    })

    # Search with highlighting - should highlight "Alert" in seattle and vienna fields
    result = env.cmd('FT.SEARCH', 'large_index', 'Alert', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    print(result)
    # Verify that "Alert" is highlighted (it appears in seattle and vienna fields)
    verify_word_is_highlighted(env, result, '<b>Alert</b>')
