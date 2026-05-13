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


def test_highlight_empty_string_on_index_empty():
    env = DialectEnv()
    MAX_DIALECT = set_max_dialect(env)

    # Test with dialect 2 (which should support empty string queries)
    env.set_dialect(2)

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 't_idx', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
              't', 'TEXT', 'INDEXEMPTY').ok()

    waitForIndex(env, 't_idx')

    conn.hset('doc:4153814', mapping={
        't': '',
    })

    result = env.cmd('FT.SEARCH', 't_idx', '@t:("")', 'HIGHLIGHT', 'FIELDS', '1', 't')
    verify_word_is_highlighted(env, result, '<b></b>')


def test_highlight_empty_string_not_index_empty():
    env = DialectEnv()
    MAX_DIALECT = set_max_dialect(env)

    # Test with dialect 2 (which should support empty string queries)
    env.set_dialect(2)

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 't_idx', 'ON', 'HASH', 'MAXTEXTFIELDS', 'STOPWORDS', '0', 'SCHEMA',
              't', 'TEXT').ok()

    waitForIndex(env, 't_idx')

    conn.hset('doc:4153814', mapping={
        't': '',
    })

    env.expect('FT.SEARCH', 't_idx', '@t:("")', 'HIGHLIGHT', 'FIELDS', '1', 't').error().contains("Use `INDEXEMPTY` in field creation in order to index and query for empty strings")


# Contract test: the highlight processor needs the RSIndexResult that carries
# term records and offsets. Buffering pipeline stages (RPSorter, RPDepleter,
# RPSafeDepleter) hold SearchResults across iterator advances, so they must
# take ownership of the RSIndexResult — a borrow into the iterator's
# `it->current` slot would dangle once the iterator is read again.
#
# These tests pin highlighted/summarized output to ground truth and run the
# same logical query through several pipeline shapes (default score sort,
# explicit numeric SORTBY, KNN-rooted query). All shapes go through a sorter
# stage; if any of them drops or truncates the RSIndexResult, the highlighted
# fragment will differ from ground truth — catching regressions that the
# pre-existing crash-only repros (e.g. test_mod_8695) miss when a stage
# silently NULLs the field instead of crashing.

def _docs(res):
    """Return {doc_id: {field: value}} from an FT.SEARCH reply (RESP2)."""
    out = {}
    for i in range(1, len(res), 2):
        fields = res[i + 1]
        out[res[i]] = dict(zip(fields[0::2], fields[1::2]))
    return out


def test_highlight_contract_across_sorter_modes(env):
    """Highlighted fragments must match across pipelines that route through
    different RPSorter modes. Verifies the deep-copied RSIndexResult preserves
    term records and offsets — not just doc-id and score."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'body', 'TEXT',
               'rank', 'NUMERIC', 'SORTABLE').ok()

    conn.hset('doc:1', mapping={'body': 'the quick brown fox jumps over the lazy dog', 'rank': 3})
    conn.hset('doc:2', mapping={'body': 'a brown dog ran past the brown fence',        'rank': 1})
    conn.hset('doc:3', mapping={'body': 'no match here',                                'rank': 2})

    waitForIndex(env, 'idx')

    expected = {
        'doc:1': 'the quick <b>brown</b> fox jumps over the lazy <b>dog</b>',
        'doc:2': 'a <b>brown</b> <b>dog</b> ran past the <b>brown</b> fence',
    }

    # Default: score sort → RPSorter_NewByScore.
    res_score = env.cmd('FT.SEARCH', 'idx', 'brown|dog', 'HIGHLIGHT', 'FIELDS', '1', 'body')
    env.assertEqual(res_score[0], 2)
    docs_score = _docs(res_score)
    for doc_id, want in expected.items():
        env.assertEqual(docs_score[doc_id]['body'], want)

    # Explicit SORTBY on a numeric field → RPSorter_NewByFields. Different
    # sorter mode, same buffering invariant; highlighted output must match.
    res_sortby = env.cmd('FT.SEARCH', 'idx', 'brown|dog', 'SORTBY', 'rank', 'ASC',
                         'HIGHLIGHT', 'FIELDS', '1', 'body')
    env.assertEqual(res_sortby[0], 2)
    docs_sortby = _docs(res_sortby)
    for doc_id, want in expected.items():
        env.assertEqual(docs_sortby[doc_id]['body'], want)


def test_highlight_contract_multi_field(env):
    """Highlighting touches every TEXT field in the matched document. Verifies
    the deep-copied IR carries per-field term records, not just one."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'title', 'TEXT',
               'body', 'TEXT').ok()

    conn.hset('doc:1', mapping={
        'title': 'a brown report on dogs',
        'body':  'the brown dog walked',
    })
    waitForIndex(env, 'idx')

    res = env.cmd('FT.SEARCH', 'idx', 'brown dog',
                  'HIGHLIGHT', 'FIELDS', '2', 'title', 'body')
    env.assertEqual(res[0], 1)
    docs = _docs(res)
    env.assertEqual(docs['doc:1']['title'], 'a <b>brown</b> report on <b>dogs</b>')
    env.assertEqual(docs['doc:1']['body'],  'the <b>brown</b> <b>dog</b> walked')


def test_summarize_contract_after_sorter(env):
    """Fragment-mode highlighting after the sorter. SUMMARIZE selects the
    fragment window from the deep-copied RSIndexResult's byte offsets;
    HIGHLIGHT wraps the matched term within that window using the same
    offsets. Both stages read from the RSIndexResult that RPSorter buffers
    across iterator advances — if the deep copy drops or truncates term
    offsets, either the window will not contain the term or the term will
    not be wrapped. SUMMARIZE alone does not apply tags (that is HIGHLIGHT's
    job), so we pass both clauses to exercise the full offset-consuming path
    end-to-end."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'body', 'TEXT').ok()

    body = ('lorem ipsum dolor sit amet ' * 4 +
            'and then a brown fox appeared ' +
            'lorem ipsum dolor sit amet ' * 4)
    conn.hset('doc:1', mapping={'body': body})
    waitForIndex(env, 'idx')

    res = env.cmd('FT.SEARCH', 'idx', 'brown',
                  'SUMMARIZE', 'FIELDS', '1', 'body', 'FRAGS', '1', 'LEN', '5',
                  'HIGHLIGHT', 'FIELDS', '1', 'body')
    env.assertEqual(res[0], 1)
    fragment = _docs(res)['doc:1']['body']
    # The exact window boundaries depend on the tokenizer; we only assert
    # that the matched term is wrapped, not the full fragment shape.
    env.assertContains('<b>brown</b>', fragment)


def test_highlight_contract_knn_matches_plain():
    """Mirror of test_mod_8695, broadened to multi-term queries. The KNN
    pipeline routes through HybridIterator + RPSorter; the plain pipeline
    routes through an inverted-index root + RPSorter. Both must produce
    identical highlighted output for the same matched documents."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

    env.cmd('HSET', 'doc1', 't', 'quick brown fox',  'v', '????????')
    env.cmd('HSET', 'doc2', 't', 'lazy brown dog',   'v', '????????')
    env.cmd('HSET', 'doc3', 't', 'a swift red fox',  'v', '????????')

    # Multi-term query — exercises offset preservation for two distinct terms.
    plain = env.cmd('FT.SEARCH', 'idx', 'brown fox',
                    'HIGHLIGHT', 'FIELDS', 1, 't', 'RETURN', 1, 't')
    knn = env.cmd('FT.SEARCH', 'idx', '(brown fox)=>[KNN 10 @v $BLOB]',
                  'PARAMS', 2, 'BLOB', '????????',
                  'HIGHLIGHT', 'FIELDS', 1, 't', 'RETURN', 1, 't')
    env.assertEqual(_docs(plain), _docs(knn))
