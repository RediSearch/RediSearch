"""
Test cases for RRF (Reciprocal Rank Fusion) implementation

To run tests, use `pytest -v` from the `tests/pytests` directory
"""

import pytest
from .rrf import rrf_fusion, rrf_fusion_multiple


def test_rrf_basic_fusion():
    """Test basic RRF fusion with two lists"""
    list1 = [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7)]
    list2 = [("doc2", 0.95), ("doc3", 0.85), ("doc1", 0.75)]

    result = rrf_fusion(list1, list2, k=60)

    # Check that all documents are present
    doc_keys = [doc[0] for doc in result]
    assert set(doc_keys) == {"doc1", "doc2", "doc3"}

    # Check that results are sorted by score (descending)
    scores = [doc[1] for doc in result]
    assert scores == sorted(scores, reverse=True)

    # Verify specific RRF calculations
    # doc1: rank 1 in list1, rank 3 in list2 -> 1/(60+1) + 1/(60+3) = 1/61 + 1/63
    expected_doc1 = 1/61 + 1/63
    # doc2: rank 2 in list1, rank 1 in list2 -> 1/(60+2) + 1/(60+1) = 1/62 + 1/61
    expected_doc2 = 1/62 + 1/61
    # doc3: rank 3 in list1, rank 2 in list2 -> 1/(60+3) + 1/(60+2) = 1/63 + 1/62
    expected_doc3 = 1/63 + 1/62

    result_dict = {doc[0]: doc[1] for doc in result}
    assert abs(result_dict["doc1"] - expected_doc1) < 1e-10
    assert abs(result_dict["doc2"] - expected_doc2) < 1e-10
    assert abs(result_dict["doc3"] - expected_doc3) < 1e-10


def test_rrf_with_non_overlapping_docs():
    """Test RRF with documents that don't appear in both lists"""
    list1 = [("doc1", 0.9), ("doc2", 0.8)]
    list2 = [("doc3", 0.95), ("doc4", 0.85)]

    result = rrf_fusion(list1, list2, k=60)

    # All documents should be present
    doc_keys = [doc[0] for doc in result]
    assert set(doc_keys) == {"doc1", "doc2", "doc3", "doc4"}

    # Documents that appear in only one list should have lower scores
    result_dict = {doc[0]: doc[1] for doc in result}

    # doc3 appears only in list2 at rank 1: 1/(60+1) = 1/61
    assert abs(result_dict["doc3"] - 1/61) < 1e-10
    # doc4 appears only in list2 at rank 2: 1/(60+2) = 1/62
    assert abs(result_dict["doc4"] - 1/62) < 1e-10
    # doc1 appears only in list1 at rank 1: 1/(60+1) = 1/61
    assert abs(result_dict["doc1"] - 1/61) < 1e-10
    # doc2 appears only in list1 at rank 2: 1/(60+2) = 1/62
    assert abs(result_dict["doc2"] - 1/62) < 1e-10


def test_rrf_different_k_values():
    """Test RRF with different k values"""
    list1 = [("doc1", 0.9), ("doc2", 0.8)]
    list2 = [("doc1", 0.95), ("doc2", 0.85)]

    result_k60 = rrf_fusion(list1, list2, k=60)
    result_k10 = rrf_fusion(list1, list2, k=10)

    # With smaller k, the differences should be more pronounced
    scores_k60 = [doc[1] for doc in result_k60]
    scores_k10 = [doc[1] for doc in result_k10]

    # All scores with k=10 should be higher than with k=60
    assert all(s10 > s60 for s10, s60 in zip(scores_k10, scores_k60))


def test_rrf_multiple_lists():
    """Test RRF with multiple lists"""
    lists = [
        [("doc1", 0.9), ("doc2", 0.8)],
        [("doc2", 0.95), ("doc3", 0.85)],
        [("doc3", 0.9), ("doc1", 0.7)]
    ]

    result = rrf_fusion_multiple(lists, k=60)

    # All documents should be present
    doc_keys = [doc[0] for doc in result]
    assert set(doc_keys) == {"doc1", "doc2", "doc3"}

    # Check manual calculation for doc1:
    # List 1: rank 1 -> 1/(60+1) = 1/61
    # List 2: not present -> 0
    # List 3: rank 2 -> 1/(60+2) = 1/62
    # Total: 1/61 + 1/62
    expected_doc1 = 1/61 + 1/62

    result_dict = {doc[0]: doc[1] for doc in result}
    assert abs(result_dict["doc1"] - expected_doc1) < 1e-10


def test_rrf_empty_lists():
    """Test RRF with empty lists"""
    result = rrf_fusion([], [], k=60)
    assert result == []

    result_multiple = rrf_fusion_multiple([], k=60)
    assert result_multiple == []


def test_rrf_single_list():
    """Test RRF with single list in multiple fusion"""
    single_list = [("doc1", 0.9), ("doc2", 0.8)]
    result = rrf_fusion_multiple([single_list], k=60)

    # Should return the original list
    assert result == single_list


def test_rrf_identical_lists():
    """Test RRF with identical lists"""
    list1 = [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7)]
    list2 = [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7)]

    result = rrf_fusion(list1, list2, k=60)

    # Each document should have double the score of appearing in one list
    result_dict = {doc[0]: doc[1] for doc in result}

    # doc1: 2 * 1/(60+1) = 2/61
    assert abs(result_dict["doc1"] - 2/61) < 1e-10
    # doc2: 2 * 1/(60+2) = 2/62
    assert abs(result_dict["doc2"] - 2/62) < 1e-10
    # doc3: 2 * 1/(60+3) = 2/63
    assert abs(result_dict["doc3"] - 2/63) < 1e-10


def test_rrf_window_parameter():
    """Test RRF with window parameter limiting results"""
    # Create longer lists to test window effect
    list1 = [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7), ("doc4", 0.6), ("doc5", 0.5)]
    list2 = [("doc6", 0.95), ("doc7", 0.85), ("doc8", 0.75), ("doc9", 0.65), ("doc10", 0.55)]

    # Test with window=2 (should only consider top 2 from each list)
    result_window2 = rrf_fusion(list1, list2, k=60, window=2)

    # Should only have docs from the first 2 positions of each list
    expected_docs = {"doc1", "doc2", "doc6", "doc7"}
    result_docs = {doc_id for doc_id, _ in result_window2}
    assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

    # Test with window=3 (should consider top 3 from each list)
    result_window3 = rrf_fusion(list1, list2, k=60, window=3)

    # Should have docs from the first 3 positions of each list
    expected_docs_3 = {"doc1", "doc2", "doc3", "doc6", "doc7", "doc8"}
    result_docs_3 = {doc_id for doc_id, _ in result_window3}
    assert result_docs_3 == expected_docs_3, f"Expected {expected_docs_3}, got {result_docs_3}"

    # Test with large window (should include all docs)
    result_window_large = rrf_fusion(list1, list2, k=60, window=10)

    # Should have all docs from both lists
    all_docs = {"doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8", "doc9", "doc10"}
    result_docs_large = {doc_id for doc_id, _ in result_window_large}
    assert result_docs_large == all_docs, f"Expected {all_docs}, got {result_docs_large}"


def test_rrf_window_with_overlapping_docs():
    """Test RRF window parameter with overlapping documents"""
    # Lists with some overlapping docs
    list1 = [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7), ("doc4", 0.6)]
    list2 = [("doc2", 0.95), ("doc3", 0.85), ("doc5", 0.75), ("doc6", 0.65)]

    # Test with window=2
    result = rrf_fusion(list1, list2, k=60, window=2)

    # Should only consider: doc1, doc2 from list1 and doc2, doc3 from list2
    # So final docs should be: doc1, doc2, doc3
    expected_docs = {"doc1", "doc2", "doc3"}
    result_docs = {doc_id for doc_id, _ in result}
    assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

    # Verify doc2 has highest score (appears in both windowed lists at top positions)
    result_dict = {doc_id: score for doc_id, score in result}

    # doc2 should have highest score: 1/(60+2) + 1/(60+1) = 1/62 + 1/61
    expected_doc2_score = 1/62 + 1/61
    assert abs(result_dict["doc2"] - expected_doc2_score) < 1e-10


if __name__ == "__main__":
    # Run tests manually if pytest is not available
    test_rrf_basic_fusion()
    test_rrf_with_non_overlapping_docs()
    test_rrf_different_k_values()
    test_rrf_multiple_lists()
    test_rrf_empty_lists()
    test_rrf_single_list()
    test_rrf_identical_lists()
    test_rrf_window_parameter()
    test_rrf_window_with_overlapping_docs()

    print("All tests passed!")
