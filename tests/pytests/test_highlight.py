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


def test_highlight_complex_schema_mod_11233(env):
    """Test highlighting with complex schema similar to production use case"""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'large_index', 'ON', 'HASH', 'SCHEMA',
               'lisbon', 'TEXT',
               'vienna', 'TEXT',
               'tokyo', 'TEXT',
               'seattle', 'TEXT').ok()

    waitForIndex(env, 'large_index')

    conn.hset('doc:4153814', mapping={
        'vienna': 'word here Dog',
        'lisbon': 'other sentence',
        'tokyo': '',
        'seattle': 'my Dog sleeps',
    })

    result = env.cmd('FT.SEARCH', 'large_index', 'Dog', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    print(result)
    verify_word_is_highlighted(env, result, '<b>Dog</b>')


def test_highlight_one_space(env):
    """Test highlighting with complex schema similar to production use case"""
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'working_index', 'ON', 'HASH', 'SCHEMA',
        'lisbon', 'TEXT',
        'seattle', 'TEXT').ok()

    waitForIndex(env, 'working_index')

    conn.hset('doc:4153814', mapping={
        'lisbon': ' ', # one space
        'seattle': 'my Dog sleeps',
    })

    result = env.cmd('FT.SEARCH', 'working_index', 'Dog', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    verify_word_is_highlighted(env, result, '<b>Dog</b>')

def test_highlight_skip_field(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'lisbon_seattle_skip', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
            'lisbon', 'TEXT',
            'seattle', 'TEXT').ok()

    waitForIndex(env, 'lisbon_seattle_skip')

    conn.hset('doc:4153814', mapping={
        'seattle': 'my Dog sleeps',
    })

    result = env.cmd('FT.SEARCH', 'lisbon_seattle_skip', 'Dog', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    verify_word_is_highlighted(env, result, '<b>Dog</b>')

def test_highlight_empty_field(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'lisbon_seattle', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
               'lisbon', 'TEXT',
               'seattle', 'TEXT').ok()

    waitForIndex(env, 'lisbon_seattle')

    conn.hset('doc:4153814', mapping={
        'lisbon': '',
        'seattle': 'my Dog sleeps',
    })

    result = env.cmd('FT.SEARCH', 'lisbon_seattle', 'Dog', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    verify_word_is_highlighted(env, result, '<b>Dog</b>')

def test_highlight_empty_field_reverse_order_fields(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'seattle_lisbon', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
               'seattle', 'TEXT',
               'lisbon', 'TEXT').ok()

    waitForIndex(env, 'seattle_lisbon')

    conn.hset('doc:4153814', mapping={
        'seattle': '',
        'lisbon': 'my Dog sleeps',
    })

    result = env.cmd('FT.SEARCH', 'seattle_lisbon', 'Dog', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    verify_word_is_highlighted(env, result, '<b>Dog</b>')


def test_highlight_empty_field_index_empty(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'seattle_lisbon_index_empty', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
              'seattle', 'TEXT', 'INDEXEMPTY',
              'lisbon', 'TEXT').ok()

    waitForIndex(env, 'seattle_lisbon_index_empty')

    conn.hset('doc:4153814', mapping={
        'seattle': '',
        'lisbon': 'My Dog sleeps',
    })

    result = env.cmd('FT.SEARCH', 'seattle_lisbon_index_empty', 'Dog', 'LIMIT', '0', '1', 'SORTBY', 'lisbon', 'DESC', 'HIGHLIGHT')
    verify_word_is_highlighted(env, result, '<b>Dog</b>')
