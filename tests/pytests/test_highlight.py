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


# Test highlighting with complex real-world schema
def test_highlight_complex_schema_mod_11233(env):
    """Test highlighting with complex schema similar to production use case"""
    conn = getConnectionByEnv(env)

    # Create index with many fields (anonymized with city names)
    env.expect('FT.CREATE', 'large_index', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
               'tokyo', 'NUMERIC',
               'paris', 'NUMERIC',
               'london', 'TEXT', 'NOSTEM',
               'berlin', 'TEXT', 'NOSTEM',
               'madrid', 'TEXT', 'NOSTEM',
               'rome', 'TEXT', 'NOSTEM',
               'vienna', 'TEXT', 'NOSTEM',
               'oslo', 'NUMERIC', 'SORTABLE',
               'dublin', 'TEXT', 'NOSTEM',
               'lisbon', 'NUMERIC', 'SORTABLE',
               'prague', 'TEXT', 'NOSTEM',
               'athens', 'TEXT', 'NOSTEM',
               'warsaw', 'TEXT', 'NOSTEM',
               'budapest', 'TEXT', 'NOSTEM',
               'stockholm', 'TEXT', 'NOSTEM',
               'helsinki', 'TEXT', 'NOSTEM',
               'brussels', 'TEXT', 'NOSTEM',
               'amsterdam', 'TEXT', 'NOSTEM',
               'copenhagen', 'TEXT', 'NOSTEM',
               'zurich', 'TEXT', 'NOSTEM',
               'geneva', 'TEXT', 'NOSTEM',
               'milan', 'TEXT', 'NOSTEM',
               'barcelona', 'TEXT', 'NOSTEM',
               'munich', 'TEXT', 'NOSTEM',
               'hamburg', 'TEXT', 'NOSTEM',
               'lyon', 'TEXT', 'NOSTEM',
               'marseille', 'TEXT', 'NOSTEM',
               'naples', 'TEXT', 'NOSTEM',
               'turin', 'TEXT', 'NOSTEM',
               'valencia', 'TEXT', 'NOSTEM',
               'seville', 'TEXT', 'NOSTEM',
               'cologne', 'TEXT', 'NOSTEM',
               'frankfurt', 'TEXT', 'NOSTEM',
               'porto', 'TEXT', 'NOSTEM',
               'seattle', 'TEXT', 'NOSTEM',
               'boston', 'TEXT', 'NOSTEM',
               'denver', 'TEXT', 'NOSTEM',
               'austin', 'TEXT', 'NOSTEM',
               'phoenix', 'TEXT', 'NOSTEM',
               'dallas', 'TEXT', 'NOSTEM',
               'houston', 'TEXT', 'NOSTEM',
               'miami', 'TEXT', 'NOSTEM',
               'atlanta', 'TEXT', 'NOSTEM',
               'chicago', 'TEXT', 'NOSTEM',
               'detroit', 'TEXT', 'NOSTEM',
               'toronto', 'TEXT', 'NOSTEM',
               'montreal', 'TEXT', 'NOSTEM',
               'vancouver', 'TEXT', 'NOSTEM',
               'calgary', 'TEXT', 'NOSTEM',
               'ottawa', 'TEXT', 'NOSTEM',
               'quebec', 'TEXT', 'NOSTEM',
               'winnipeg', 'TEXT', 'NOSTEM',
               'edmonton', 'TEXT', 'NOSTEM',
               'sydney', 'TEXT', 'NOSTEM',
               'melbourne', 'TEXT', 'NOSTEM',
               'brisbane', 'TEXT', 'NOSTEM',
               'perth', 'TEXT', 'NOSTEM',
               'adelaide', 'TEXT', 'NOSTEM',
               'auckland', 'TEXT', 'NOSTEM',
               'wellington', 'TEXT', 'NOSTEM',
               'singapore', 'TEXT', 'NOSTEM',
               'bangkok', 'TEXT', 'NOSTEM',
               'manila', 'TEXT', 'NOSTEM',
               'jakarta', 'TEXT', 'NOSTEM',
               'kuala_lumpur', 'TEXT', 'NOSTEM',
               'hanoi', 'TEXT', 'NOSTEM',
               'seoul', 'TEXT', 'NOSTEM',
               'beijing', 'TEXT', 'NOSTEM',
               'shanghai', 'TEXT', 'NOSTEM',
               'hong_kong', 'TEXT', 'NOSTEM',
               'taipei', 'TEXT', 'NOSTEM',
               'osaka', 'TEXT', 'NOSTEM',
               'kyoto', 'TEXT', 'NOSTEM',
               'mumbai', 'TEXT', 'NOSTEM',
               'delhi', 'TEXT', 'NOSTEM',
               'bangalore', 'TEXT', 'NOSTEM',
               'chennai', 'TEXT', 'NOSTEM',
               'kolkata', 'TEXT', 'NOSTEM',
               'hyderabad', 'TEXT', 'NOSTEM',
               'pune', 'TEXT', 'NOSTEM',
               'cairo', 'TEXT', 'NOSTEM',
               'dubai', 'TEXT', 'NOSTEM',
               'istanbul', 'TEXT', 'NOSTEM',
               'riyadh', 'TEXT', 'NOSTEM',
               'tehran', 'TEXT', 'NOSTEM',
               'baghdad', 'TEXT', 'NOSTEM',
               'jerusalem', 'TEXT', 'NOSTEM',
               'amman', 'TEXT', 'NOSTEM',
               'beirut', 'TEXT', 'NOSTEM',
               'damascus', 'TEXT', 'NOSTEM',
               'doha', 'TEXT', 'NOSTEM',
               'muscat', 'TEXT', 'NOSTEM',
               'nairobi', 'TEXT', 'NOSTEM',
               'lagos', 'TEXT', 'NOSTEM',
               'johannesburg', 'TEXT', 'NOSTEM',
               'cape_town', 'TEXT', 'NOSTEM',
               'casablanca', 'TEXT', 'NOSTEM',
               'tunis', 'TEXT', 'NOSTEM',
               'algiers', 'TEXT', 'NOSTEM').ok()

    waitForIndex(env, 'large_index')

    # Add document with city field names
    conn.hset('doc:4153814', mapping={
        'vienna': '',
        'lisbon': '1756447303000',
        'london': 'REC_5126046',
        'tokyo': '4153814',
        'oslo': '1756462826000',
        'rome': '',
        'munich': '',
        'zurich': '',
        'seattle': 'This Alert triggered for potential activity and will be included in the automated review process.',
        'madrid': 'Alert',
        'dublin': 'AUTOMATED SYSTEM',
        'berlin': 'System',
        'paris': '5126046',
        'prague': 'DEPT',
        'athens': '',
        'amsterdam': 'System',
        'warsaw': '2025-08-29T10:20:15.699982Z',
        'budapest': '4',
        'stockholm': 'System'
    })

    # Search with highlighting - should highlight "Alert" in madrid and seattle fields
    result = env.cmd('FT.SEARCH', 'large_index', 'Alert', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    print(result)
    # Verify that "Alert" is highlighted (it appears in both madrid and seattle fields)
    verify_word_is_highlighted(env, result, '<b>Alert</b>')
