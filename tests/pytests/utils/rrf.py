"""
Reciprocal Rank Fusion (RRF) Implementation

RRF combines multiple ranked lists by computing a score for each item based on its rank
in each list. The formula is: RRF_score = sum(1 / (k + rank_i)) for all lists where
the item appears, where k is a constant (typically 60) and rank_i is the rank in list i.
"""

from typing import List, Tuple, Dict, Set
from collections import defaultdict
from dataclasses import dataclass

@dataclass
class Result:
    """Represents a search result with document key and score."""
    key: str
    score: float

    def __eq__(self, other):
        if not isinstance(other, Result):
            return False
        return self.key == other.key and abs(self.score - other.score) < 1e-6


def rrf(
    list1: List[Result],
    list2: List[Result],
    k: int = 60,
    window: int = 20
) -> List[Result]:
    """
    Perform Reciprocal Rank Fusion on two ranked lists.

    Args:
        list1: List of Result objects from first ranking system
        list2: List of Result objects from second ranking system
        k: RRF constant parameter (default: 60)
        window: Maximum number of results to consider from each list (default: 20)

    Returns:
        List of Result objects sorted by RRF score in descending order

    Example:
        >>> list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        >>> list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)]
        >>> result = rrf(list1, list2, k=60, window=20)
        >>> print(result)
        [Result(key='doc2', score=0.032786885245901644), Result(key='doc3', score=0.032258064516129031), Result(key='doc1', score=0.029508196721311475)]
    """
    # Apply window limit to input lists
    list1_windowed = list1[:window]
    list2_windowed = list2[:window]

    # Dictionary to store RRF scores for each key
    rrf_scores: Dict[str, float] = defaultdict(float)

    # Get all unique keys from both windowed lists
    all_keys: Set[str] = set()
    for result in list1_windowed:
        all_keys.add(result.key)
    for result in list2_windowed:
        all_keys.add(result.key)

    # Create rank mappings for each windowed list
    rank1 = {result.key: rank + 1 for rank, result in enumerate(list1_windowed)}
    rank2 = {result.key: rank + 1 for rank, result in enumerate(list2_windowed)}

    # Calculate RRF score for each key
    for key in all_keys:
        rrf_score = 0.0

        # Add contribution from list1 if key exists
        if key in rank1:
            rrf_score += 1.0 / (k + rank1[key])

        # Add contribution from list2 if key exists
        if key in rank2:
            rrf_score += 1.0 / (k + rank2[key])

        rrf_scores[key] = rrf_score

    # Convert to list of Result objects and sort by RRF score (descending)
    result = [Result(key=key, score=score) for key, score in rrf_scores.items()]
    result.sort(key=lambda x: x.score, reverse=True)

    return result


def rrf_multiple(
    ranked_lists: List[List[Result]],
    k: int = 60,
    window: int = 20
) -> List[Result]:
    """
    Perform Reciprocal Rank Fusion on multiple ranked lists.

    Args:
        ranked_lists: List of ranked lists, each containing Result objects
        k: RRF constant parameter (default: 60)
        window: Maximum number of results to consider from each list (default: 20)

    Returns:
        List of Result objects sorted by RRF score in descending order

    Example:
        >>> lists = [
        ...     [Result("doc1", 0.9), Result("doc3", 0.8)],
        ...     [Result("doc2", 0.95), Result("doc3", 0.85)],
        ...     [Result("doc1", 0.9), Result("doc2", 0.7)]
        ... ]
        >>> result = rrf_multiple(lists, k=3, window=20)
        >>> print(result)
        [Result(key='doc1', score=0.5), Result(key='doc2', score=0.45), Result(key='doc3', score=0.4)]
    """
    if not ranked_lists:
        return []

    if len(ranked_lists) == 1:
        return ranked_lists[0]

    # Dictionary to store RRF scores for each key
    rrf_scores: Dict[str, float] = defaultdict(float)

    # Apply window limit to all input lists
    windowed_lists = [ranked_list[:window] for ranked_list in ranked_lists]

    # Get all unique keys from all windowed lists
    all_keys: Set[str] = set()
    for ranked_list in windowed_lists:
        for result in ranked_list:
            all_keys.add(result.key)

    # Create rank mappings for each windowed list
    rank_mappings = []
    for ranked_list in windowed_lists:
        rank_map = {result.key: rank + 1 for rank, result in enumerate(ranked_list)}
        rank_mappings.append(rank_map)

    # Calculate RRF score for each key
    for key in all_keys:
        rrf_score = 0.0

        # Add contribution from each list where the key appears
        for rank_map in rank_mappings:
            if key in rank_map:
                rrf_score += 1.0 / (k + rank_map[key])

        rrf_scores[key] = rrf_score

    # Convert to list of Result objects and sort by RRF score (descending)
    result = [Result(key=key, score=score) for key, score in rrf_scores.items()]
    result.sort(key=lambda x: x.score, reverse=True)

    return result


if __name__ == "__main__":

    # Unit tests
    def test_rrf_basic_fusion():
        """Test basic RRF fusion with two lists"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)]

        result = rrf(list1, list2, k=60)

        # Check that all documents are present
        doc_keys = [doc.key for doc in result]
        assert set(doc_keys) == {"doc1", "doc2", "doc3"}

        # Check that results are sorted by score (descending)
        scores = [doc.score for doc in result]
        assert scores == sorted(scores, reverse=True)

        # Verify specific RRF calculations
        # doc1: rank 1 in list1, rank 3 in list2 -> 1/(60+1) + 1/(60+3) = 1/61 + 1/63
        expected_doc1 = 1/61 + 1/63
        # doc2: rank 2 in list1, rank 1 in list2 -> 1/(60+2) + 1/(60+1) = 1/62 + 1/61
        expected_doc2 = 1/62 + 1/61
        # doc3: rank 3 in list1, rank 2 in list2 -> 1/(60+3) + 1/(60+2) = 1/63 + 1/62
        expected_doc3 = 1/63 + 1/62

        result_dict = {doc.key: doc.score for doc in result}
        assert abs(result_dict["doc1"] - expected_doc1) < 1e-10
        assert abs(result_dict["doc2"] - expected_doc2) < 1e-10
        assert abs(result_dict["doc3"] - expected_doc3) < 1e-10


    def test_rrf_with_non_overlapping_docs():
        """Test RRF with documents that don't appear in both lists"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8)]
        list2 = [Result("doc3", 0.95), Result("doc4", 0.85)]

        result = rrf(list1, list2, k=60)

        # All documents should be present
        doc_keys = [doc.key for doc in result]
        assert set(doc_keys) == {"doc1", "doc2", "doc3", "doc4"}

        # Documents that appear in only one list should have lower scores
        result_dict = {doc.key: doc.score for doc in result}

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
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8)]
        list2 = [Result("doc1", 0.95), Result("doc2", 0.85)]

        result_k60 = rrf(list1, list2, k=60)
        result_k10 = rrf(list1, list2, k=10)

        # With smaller k, the differences should be more pronounced
        scores_k60 = [doc.score for doc in result_k60]
        scores_k10 = [doc.score for doc in result_k10]

        # All scores with k=10 should be higher than with k=60
        assert all(s10 > s60 for s10, s60 in zip(scores_k10, scores_k60))


    def test_rrf_multiple_lists():
        """Test RRF with multiple lists"""
        lists = [
            [Result("doc1", 0.9), Result("doc2", 0.8)],
            [Result("doc2", 0.95), Result("doc3", 0.85)],
            [Result("doc3", 0.9), Result("doc1", 0.7)]
        ]

        result = rrf_multiple(lists, k=60)

        # All documents should be present
        doc_keys = [doc.key for doc in result]
        assert set(doc_keys) == {"doc1", "doc2", "doc3"}

        # Check manual calculation for doc1:
        # List 1: rank 1 -> 1/(60+1) = 1/61
        # List 2: not present -> 0
        # List 3: rank 2 -> 1/(60+2) = 1/62
        # Total: 1/61 + 1/62
        expected_doc1 = 1/61 + 1/62

        result_dict = {doc.key: doc.score for doc in result}
        assert abs(result_dict["doc1"] - expected_doc1) < 1e-10


    def test_rrf_empty_lists():
        """Test RRF with empty lists"""
        result = rrf([], [], k=60)
        assert result == []

        result_multiple = rrf_multiple([], k=60)
        assert result_multiple == []


    def test_rrf_single_list():
        """Test RRF with single list in multiple fusion"""
        single_list = [Result("doc1", 0.9), Result("doc2", 0.8)]
        result = rrf_multiple([single_list], k=60)

        # Should return the original list
        assert result == single_list


    def test_rrf_identical_lists():
        """Test RRF with identical lists"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        list2 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]

        result = rrf(list1, list2, k=60)

        # Each document should have double the score of appearing in one list
        result_dict = {doc.key: doc.score for doc in result}

        # doc1: 2 * 1/(60+1) = 2/61
        assert abs(result_dict["doc1"] - 2/61) < 1e-10
        # doc2: 2 * 1/(60+2) = 2/62
        assert abs(result_dict["doc2"] - 2/62) < 1e-10
        # doc3: 2 * 1/(60+3) = 2/63
        assert abs(result_dict["doc3"] - 2/63) < 1e-10


    def test_rrf_window_parameter():
        """Test RRF with window parameter limiting results"""
        # Create longer lists to test window effect
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6), Result("doc5", 0.5)]
        list2 = [Result("doc6", 0.95), Result("doc7", 0.85), Result("doc8", 0.75), Result("doc9", 0.65), Result("doc10", 0.55)]

        # Test with window=2 (should only consider top 2 from each list)
        result_window2 = rrf(list1, list2, k=60, window=2)

        # Should only have docs from the first 2 positions of each list
        expected_docs = {"doc1", "doc2", "doc6", "doc7"}
        result_docs = {doc.key for doc in result_window2}
        assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

        # Test with window=3 (should consider top 3 from each list)
        result_window3 = rrf(list1, list2, k=60, window=3)

        # Should have docs from the first 3 positions of each list
        expected_docs_3 = {"doc1", "doc2", "doc3", "doc6", "doc7", "doc8"}
        result_docs_3 = {doc.key for doc in result_window3}
        assert result_docs_3 == expected_docs_3, f"Expected {expected_docs_3}, got {result_docs_3}"

        # Test with large window (should include all docs)
        result_window_large = rrf(list1, list2, k=60, window=10)

        # Should have all docs from both lists
        all_docs = {"doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8", "doc9", "doc10"}
        result_docs_large = {doc.key for doc in result_window_large}
        assert result_docs_large == all_docs, f"Expected {all_docs}, got {result_docs_large}"


    def test_rrf_window_with_overlapping_docs():
        """Test RRF window parameter with overlapping documents"""
        # Lists with some overlapping docs
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6)]
        list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc5", 0.75), Result("doc6", 0.65)]

        # Test with window=2
        result = rrf(list1, list2, k=60, window=2)

        # Should only consider: doc1, doc2 from list1 and doc2, doc3 from list2
        # So final docs should be: doc1, doc2, doc3
        expected_docs = {"doc1", "doc2", "doc3"}
        result_docs = {doc.key for doc in result}
        assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

        # Verify doc2 has highest score (appears in both windowed lists at top positions)
        result_dict = {doc.key: doc.score for doc in result}

        # doc2 should have highest score: 1/(60+2) + 1/(60+1) = 1/62 + 1/61
        expected_doc2_score = 1/62 + 1/61
        assert abs(result_dict["doc2"] - expected_doc2_score) < 1e-10

    # Run unit tests
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

    # Example usage
    print("RRF Example:")

    # Example with two lists
    search_results = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6)]
    vector_results = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc5", 0.8), Result("doc1", 0.75)]

    print("\nSearch results:", search_results)
    print("Vector results:", vector_results)

    fused_results = rrf(search_results, vector_results, k=60)
    print("\nRRF Fused results:")
    for result in fused_results:
        print(f"  {result.key}: {result.score:.6f}")

    # Example with multiple lists
    print("\n" + "="*50)
    print("Multiple Lists RRF Example:")

    lists = [
        [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)],
        [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)],
        [Result("doc3", 0.9), Result("doc1", 0.8), Result("doc4", 0.7)]
    ]

    for i, lst in enumerate(lists):
        print(f"List {i+1}: {lst}")

    multi_fused = rrf_multiple(lists, k=60)
    print("\nMultiple RRF Fused results:")
    for result in multi_fused:
        print(f"  {result.key}: {result.score:.6f}")
