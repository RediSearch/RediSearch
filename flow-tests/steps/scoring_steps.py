"""
Step definitions for RediSearchDisk scoring tests (BM25, TFIDF).

This module implements scoring formula calculations matching RediSearch's scorers:
- TFIDF: weight * TF * IDF, normalized by maxFreq, multiplied by doc score
- BM25: Non-standard variation with b=0.5, k1=1.2, using avgDocLen (not per-doc len)
- BM25STD: Standard BM25 with b=0.75, k1=1.2, using per-document length

Debug/Test scorers (registered via _FT.DEBUG REGISTER_TEST_SCORERS):
- TEST_NUM_DOCS: returns the number of documents in the index
- TEST_NUM_TERMS: returns the number of unique terms in the index
- TEST_AVG_DOC_LEN: returns the average document length
- TEST_SUM_IDF: returns the sum of IDF values from all terms in the result
- TEST_SUM_BM25_IDF: returns the sum of BM25 IDF values from all terms in the result

IDF formulas:
- TFIDF IDF: logb(1 + (totalDocs + 1) / termDocs)  [logb = C's logb(), binary exponent extraction]
- BM25 IDF (standard): log(1 + (totalDocs - termDocs + 0.5) / (termDocs + 0.5))
"""
from common import *
import math
import os


TRUE_VALUES = {"true", "1", "yes", "on"}

# TODO: Turn this flag on to be able to test scores are well computed on restart/replication
check_rdb_reload_scoring = (
    os.getenv("CHECK_RDB_RELOAD_SCORING", "false").lower() in TRUE_VALUES
)

# =============================================================================
# Scoring Formula Implementations (matching RediSearch)
# =============================================================================

def logb(x: float) -> float:
    """C-compatible logb function.

    Returns the binary exponent of x, which is floor(log2(x)) for x >= 1.
    This matches the behavior of C's logb() function from <math.h>.
    """
    if x <= 0:
        return float('-inf')
    # math.frexp returns (mantissa, exponent) where x = mantissa * 2^exponent
    # and 0.5 <= |mantissa| < 1, so we need to adjust by -1
    _, exp = math.frexp(x)
    return float(exp - 1)


def calculate_idf_tfidf(total_docs: int, term_docs: int) -> float:
    """Calculate IDF for TFIDF scorer.

    Formula: logb(1 + (totalDocs + 1) / termDocs)
    where logb is C's logb() function (binary exponent extraction).
    """
    if term_docs == 0:
        term_docs = 1
    return logb(1.0 + (total_docs + 1) / term_docs)


def calculate_idf_bm25(total_docs: int, term_docs: int) -> float:
    """Calculate IDF for BM25 standard scorer.

    Formula: log(1 + (totalDocs - termDocs + 0.5) / (termDocs + 0.5))
    """
    total_docs = max(total_docs, term_docs)
    return math.log(1.0 + (total_docs - term_docs + 0.5) / (term_docs + 0.5))


def _parse_search_results_with_scores(result):
    """Parse FT.SEARCH results with WITHSCORES into a dict.

    When WITHSCORES is used, the result format is:
    [count, doc_id1, score1, [fields...], doc_id2, score2, [fields...], ...]

    Returns:
        dict: {doc_id: {'score': float, 'fields': {...}}, ...}
    """
    docs = {}
    for i in range(1, len(result), 3):
        doc_id = result[i]
        score = float(result[i + 1])
        fields_list = result[i + 2] if i + 2 < len(result) and isinstance(result[i + 2], list) else []
        fields = {fields_list[j]: fields_list[j + 1] for j in range(0, len(fields_list), 2)}
        docs[doc_id] = {'score': score, 'fields': fields}
        i += 3
    return docs


@when(parsers.parse('I search the index "{index_name}" for "{query}" with scorer "{scorer}" and scores'))
def search_with_scorer(redis_env, request, index_name, query, scorer):
    """Search an index with a specific scorer and return scores."""
    result = redis_env.cmd('FT.SEARCH', index_name, query, 'WITHSCORES', 'SCORER', scorer)
    request.node.search_result = result
    request.node.scored_results = _parse_search_results_with_scores(result)

@then(parsers.parse('the result "{doc_id}" should have score approximately {expected_score:f}'))
def verify_approximate_score(redis_env, request, doc_id, expected_score):
    """Verify a document has approximately the expected score."""
    scored_results = request.node.scored_results
    redis_env.assertContains(doc_id, scored_results, message=f"Document '{doc_id}' not found in results: {list(scored_results.keys())}")
    actual_score = scored_results[doc_id]['score']
    tolerance = 0.01
    redis_env.assertAlmostEqual(actual_score, expected_score, delta=tolerance,
        message=f"Expected score ~{expected_score} for '{doc_id}', got {actual_score}")


@then(parsers.parse('the result "{doc_id}" should have the highest score'))
def verify_highest_score(redis_env, request, doc_id):
    """Verify a document has the highest score among all results."""
    scored_results = request.node.scored_results
    redis_env.assertContains(doc_id, scored_results, message=f"Document '{doc_id}' not found in results: {list(scored_results.keys())}")
    doc_score = scored_results[doc_id]['score']
    for other_id, other_data in scored_results.items():
        if other_id != doc_id:
            redis_env.assertGreaterEqual(doc_score, other_data['score'],
                message=f"Expected '{doc_id}' (score={doc_score}) to have highest score, but '{other_id}' has {other_data['score']}")


@then(parsers.parse('the result "{doc_id}" should have the lowest score'))
def verify_lowest_score(redis_env, request, doc_id):
    """Verify a document has the lowest score among all results."""
    scored_results = request.node.scored_results
    redis_env.assertContains(doc_id, scored_results, message=f"Document '{doc_id}' not found in results: {list(scored_results.keys())}")
    doc_score = scored_results[doc_id]['score']
    for other_id, other_data in scored_results.items():
        if other_id != doc_id:
            redis_env.assertLessEqual(doc_score, other_data['score'],
                message=f"Expected '{doc_id}' (score={doc_score}) to have lowest score, but '{other_id}' has {other_data['score']}")


@then('all scores should be positive')
def verify_all_scores_positive(redis_env, request):
    """Verify all result scores are positive."""
    scored_results = request.node.scored_results
    for doc_id, data in scored_results.items():
        redis_env.assertGreater(data['score'], 0, message=f"Expected positive score for '{doc_id}', got {data['score']}")


# =============================================================================
# Steps for computing expected scores dynamically
# =============================================================================

def _get_max_term_freq(text: str) -> int:
    """Get the maximum frequency of any term in the text."""
    words = text.lower().split()
    if not words:
        return 1
    freq_count = {}
    for word in words:
        freq_count[word] = freq_count.get(word, 0) + 1
    return max(freq_count.values())


@when('I register documents for scoring:')
def register_documents_for_scoring(request, datatable):
    """Register documents with their content for expected score computation.

    This step stores the document content explicitly so we can compute
    per-document term frequencies, maxFreq, docLen, etc.
    """
    # datatable is a list of dicts, each with 'doc_id' and 'content'
    docs = {}
    for row in datatable:
        doc_id = row['doc_id']
        content = row['content']
        docs[doc_id] = {
            'content': content,
            'doc_len': len(content.split()),
            'max_freq': _get_max_term_freq(content)
        }
    request.node.registered_docs = docs

@then('the actual scores should match the expected scores')
def verify_scores_match_expected(redis_env, request):
    """Verify that actual scores match the computed expected scores."""
    scored_results = request.node.scored_results
    expected_scores = request.node.expected_scores

    tolerance = 0.01  # Allow small floating-point differences

    for doc_id, expected in expected_scores.items():
        redis_env.assertContains(doc_id, scored_results, message=f"Document '{doc_id}' not in actual results")
        actual = scored_results[doc_id]['score']
        redis_env.assertAlmostEqual(actual, expected, delta=tolerance,
            message=f"Score mismatch for '{doc_id}': expected {expected:.6f}, got {actual:.6f}")


@then(parsers.parse('the actual scores should match expected with tolerance {tolerance:f}'))
def verify_scores_match_with_tolerance(redis_env, request, tolerance):
    """Verify scores match with a custom tolerance."""
    scored_results = request.node.scored_results
    expected_scores = request.node.expected_scores

    for doc_id, expected in expected_scores.items():
        redis_env.assertContains(doc_id, scored_results, message=f"Document '{doc_id}' not in actual results")
        actual = scored_results[doc_id]['score']
        redis_env.assertAlmostEqual(actual, expected, delta=tolerance,
            message=f"Score mismatch for '{doc_id}': expected {expected:.6f}, got {actual:.6f} (tolerance={tolerance})")


# =============================================================================
# Test Scorer Steps (Debug Scoring Functions)
# =============================================================================

@given('the test scorers are registered')
@when('the test scorers are registered')
def register_test_scorers(redis_env):
    """Register the test scorers using the debug command.

    This registers the following test scorers:
    - TEST_NUM_DOCS: returns numDocs from index stats
    - TEST_NUM_TERMS: returns numTerms from index stats
    - TEST_AVG_DOC_LEN: returns avgDocLen from index stats
    - TEST_SUM_IDF: returns sum of TFIDF-style IDF values
    - TEST_SUM_BM25_IDF: returns sum of BM25-style IDF values

    Note: If scorers are already registered (e.g., after RDB reload), this is OK.
    """
    try:
        result = redis_env.cmd('_FT.DEBUG', 'REGISTER_TEST_SCORERS')
        redis_env.assertEqual(result, 'OK', message=f"Failed to register test scorers: {result}")
    except Exception as e:
        # Scorers may already be registered after RDB reload
        if 'already registered' in str(e):
            pass  # This is expected after RDB reload
        else:
            raise


@then(parsers.parse('all documents should have score {expected_score:f}'))
def verify_all_docs_have_score(redis_env, request, expected_score):
    """Verify all documents have the specified score."""
    scored_results = request.node.scored_results
    tolerance = 0.0001

    for doc_id, data in scored_results.items():
        actual_score = data['score']
        redis_env.assertAlmostEqual(actual_score, expected_score, delta=tolerance,
            message=f"Expected score {expected_score} for '{doc_id}', got {actual_score}")


@then(parsers.parse('all documents should have the expected TFIDF IDF score for {total_docs:d} total docs and {term_docs:d} term docs'))
def verify_tfidf_idf_score(redis_env, request, total_docs, term_docs):
    """Verify all documents have the expected TFIDF IDF score.

    TFIDF IDF formula: logb(1 + (totalDocs + 1) / termDocs)
    where logb is C's logb() function (binary exponent extraction).
    """
    scored_results = request.node.scored_results
    expected_score = calculate_idf_tfidf(total_docs, term_docs)
    tolerance = 0.01

    for doc_id, data in scored_results.items():
        actual_score = data['score']
        redis_env.assertAlmostEqual(actual_score, expected_score, delta=tolerance,
            message=f"Expected TFIDF IDF score {expected_score:.6f} for '{doc_id}', got {actual_score:.6f}")


@then(parsers.parse('all documents should have the expected BM25 IDF score for {total_docs:d} total docs and {term_docs:d} term docs'))
def verify_bm25_idf_score(redis_env, request, total_docs, term_docs):
    """Verify all documents have the expected BM25 IDF score.

    BM25 IDF formula: log(1 + (totalDocs - termDocs + 0.5) / (termDocs + 0.5))
    """
    scored_results = request.node.scored_results
    expected_score = calculate_idf_bm25(total_docs, term_docs)
    tolerance = 0.01

    for doc_id, data in scored_results.items():
        actual_score = data['score']
        redis_env.assertAlmostEqual(actual_score, expected_score, delta=tolerance,
            message=f"Expected BM25 IDF score {expected_score:.6f} for '{doc_id}', got {actual_score:.6f}")


@then(parsers.parse('all documents should have the expected {idf_type} IDF score for {total_docs:d} total documents and {term_docs:d} containing the term'))
def verify_parameterized_idf_score(redis_env, request, idf_type, total_docs, term_docs):
    """Verify all documents have the expected IDF score based on type.

    Supports:
    - TFIDF: log2(1 + (totalDocs + 1) / termDocs)
    - BM25: log(1 + (totalDocs - termDocs + 0.5) / (termDocs + 0.5))
    """
    scored_results = request.node.scored_results

    if idf_type == "TFIDF":
        expected_score = calculate_idf_tfidf(total_docs, term_docs)
    elif idf_type == "BM25":
        expected_score = calculate_idf_bm25(total_docs, term_docs)
    else:
        raise ValueError(f"Unknown IDF type: {idf_type}. Supported: TFIDF, BM25")

    tolerance = 0.01

    for doc_id, data in scored_results.items():
        actual_score = data['score']
        redis_env.assertAlmostEqual(actual_score, expected_score, delta=tolerance,
            message=f"Expected {idf_type} IDF score {expected_score:.6f} for '{doc_id}', got {actual_score:.6f}")


@then('the results should be ordered by score descending')
def verify_results_ordered_by_score(redis_env, request):
    """Verify results are ordered by score in descending order."""
    scored_results = request.node.scored_results
    scores = [data['score'] for data in scored_results.values()]

    for i in range(len(scores) - 1):
        redis_env.assertGreaterEqual(scores[i], scores[i + 1],
            message=f"Results not in descending order: score[{i}]={scores[i]} < score[{i+1}]={scores[i+1]}")
