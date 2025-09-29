"""
Linear Combination Algorithm Implementation
Linear combination merges multiple ranked lists by computing a weighted score for each item
based on its original scores in each list. The formula is:
Linear_score = alpha * score1 + beta * score2 (normalized by list positions)
where alpha and beta are weighting coefficients for each list.
"""

from typing import List, Dict, Set
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


def linear(
    list1: List[Result],
    list2: List[Result],
    alpha: int,
    beta: int,
    window: int = 20
) -> List[Result]:
    """
    Perform Linear scoring on two ranked lists with WINDOW support.

    Args:
        list1: List of Result objects from first ranking system (e.g., search)
        list2: List of Result objects from second ranking system (e.g., vector)
        alpha: Weight for first list scores (default: 0.5)
        beta: Weight for second list scores (default: 0.5)
        window: Maximum number of results to consider from each list (default: 20)
        limit: Maximum number of final results to return (default: None, no limit)

    Returns:
        List of Result objects sorted by linear score in descending order

    Example:
        >>> list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        >>> list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)]
        >>> result = linear_score(list1, list2, alpha=0.6, beta=0.4, window=20, limit=10)
        >>> print(result)
        [Result(key='doc2', score=0.86), Result(key='doc1', score=0.84), Result(key='doc3', score=0.76)]
    """
    # Apply window limit to input lists
    list1_windowed = list1[:window]
    list2_windowed = list2[:window]

    # Dictionary to store linear scores for each key
    linear_scores: Dict[str, float] = defaultdict(float)

    # Get all unique keys from both windowed lists
    all_keys: Set[str] = set()
    for result in list1_windowed:
        all_keys.add(result.key)
    for result in list2_windowed:
        all_keys.add(result.key)

    # Create score mappings for each windowed list
    scores1 = {result.key: result.score for result in list1_windowed}
    scores2 = {result.key: result.score for result in list2_windowed}

    # Calculate linear score for each key
    for key in all_keys:
        linear_score_val = 0.0

        # Add contribution from list1 if key exists
        if key in scores1:
            linear_score_val += alpha * scores1[key]

        # Add contribution from list2 if key exists
        if key in scores2:
            linear_score_val += beta * scores2[key]

        linear_scores[key] = linear_score_val

    # Convert to list of Result objects and sort by linear score (descending)
    result = [Result(key=key, score=score) for key, score in linear_scores.items()]
    result.sort(key=lambda x: x.score, reverse=True)

    # Apply limit if specified (like K parameter in RediSearch)
    if limit is not None and limit > 0:
        result = result[:limit]

    return result


if __name__ == "__main__":
    # Unit tests for linear scoring with WINDOW parameter
    def test_linear_basic():
        """Test basic linear scoring functionality"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)]

        result = linear_score(list1, list2, alpha=0.6, beta=0.4, window=20)

        # Expected scores:
        # doc1: 0.6 * 0.9 + 0.4 * 0.75 = 0.54 + 0.3 = 0.84
        # doc2: 0.6 * 0.8 + 0.4 * 0.95 = 0.48 + 0.38 = 0.86
        # doc3: 0.6 * 0.7 + 0.4 * 0.85 = 0.42 + 0.34 = 0.76

        expected_order = ["doc2", "doc1", "doc3"]
        actual_order = [doc.key for doc in result]
        assert actual_order == expected_order, f"Expected {expected_order}, got {actual_order}"

        # Check specific scores
        result_dict = {doc.key: doc.score for doc in result}
        assert abs(result_dict["doc1"] - 0.84) < 0.001, f"Expected doc1 score ~0.84, got {result_dict['doc1']}"
        assert abs(result_dict["doc2"] - 0.86) < 0.001, f"Expected doc2 score ~0.86, got {result_dict['doc2']}"
        assert abs(result_dict["doc3"] - 0.76) < 0.001, f"Expected doc3 score ~0.76, got {result_dict['doc3']}"
        print("✓ test_linear_basic passed")


    def test_linear_window_parameter():
        """Test LINEAR with window parameter limiting results"""
        # Create longer lists to test window effect
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6), Result("doc5", 0.5)]
        list2 = [Result("doc6", 0.95), Result("doc7", 0.85), Result("doc8", 0.75), Result("doc9", 0.65), Result("doc10", 0.55)]

        # Test with window=2 (should only consider top 2 from each list)
        result_window2 = linear_score(list1, list2, alpha=0.5, beta=0.5, window=2)

        # Should only have docs from the first 2 positions of each list
        expected_docs = {"doc1", "doc2", "doc6", "doc7"}
        result_docs = {doc.key for doc in result_window2}
        assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

        # Test with window=3 (should consider top 3 from each list)
        result_window3 = linear_score(list1, list2, alpha=0.5, beta=0.5, window=3)

        # Should have docs from the first 3 positions of each list
        expected_docs_3 = {"doc1", "doc2", "doc3", "doc6", "doc7", "doc8"}
        result_docs_3 = {doc.key for doc in result_window3}
        assert result_docs_3 == expected_docs_3, f"Expected {expected_docs_3}, got {result_docs_3}"

        # Test with large window (should include all docs)
        result_window_large = linear_score(list1, list2, alpha=0.5, beta=0.5, window=10)

        # Should have all docs from both lists
        all_docs = {"doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8", "doc9", "doc10"}
        result_docs_large = {doc.key for doc in result_window_large}
        assert result_docs_large == all_docs, f"Expected {all_docs}, got {result_docs_large}"
        print("✓ test_linear_window_parameter passed")


    def test_linear_window_with_overlapping_docs():
        """Test LINEAR window parameter with overlapping documents"""
        # Lists with some overlapping docs
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6)]
        list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc5", 0.75), Result("doc6", 0.65)]

        # Test with window=2
        result = linear_score(list1, list2, alpha=0.6, beta=0.4, window=2)

        # Should only consider: doc1, doc2 from list1 and doc2, doc3 from list2
        # So final docs should be: doc1, doc2, doc3
        expected_docs = {"doc1", "doc2", "doc3"}
        result_docs = {doc.key for doc in result}
        assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

        # Verify doc2 has highest score (appears in both windowed lists)
        result_dict = {doc.key: doc.score for doc in result}

        # doc2 should have highest score: 0.6 * 0.8 + 0.4 * 0.95 = 0.48 + 0.38 = 0.86
        expected_doc2_score = 0.6 * 0.8 + 0.4 * 0.95
        assert abs(result_dict["doc2"] - expected_doc2_score) < 0.001, f"Expected doc2 score ~{expected_doc2_score}, got {result_dict['doc2']}"
        print("✓ test_linear_window_with_overlapping_docs passed")


    def test_linear_single_list():
        """Test LINEAR scoring when document appears in only one list"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8)]
        list2 = [Result("doc3", 0.95), Result("doc4", 0.85)]

        result = linear_score(list1, list2, alpha=0.7, beta=0.3, window=10)

        # Each doc should get score from only one list
        result_dict = {doc.key: doc.score for doc in result}

        # doc1: only in list1, score = 0.7 * 0.9 = 0.63
        # doc2: only in list1, score = 0.7 * 0.8 = 0.56
        # doc3: only in list2, score = 0.3 * 0.95 = 0.285
        # doc4: only in list2, score = 0.3 * 0.85 = 0.255

        assert abs(result_dict["doc1"] - 0.63) < 0.001, f"Expected doc1 score ~0.63, got {result_dict['doc1']}"
        assert abs(result_dict["doc2"] - 0.56) < 0.001, f"Expected doc2 score ~0.56, got {result_dict['doc2']}"
        assert abs(result_dict["doc3"] - 0.285) < 0.001, f"Expected doc3 score ~0.285, got {result_dict['doc3']}"
        assert abs(result_dict["doc4"] - 0.255) < 0.001, f"Expected doc4 score ~0.255, got {result_dict['doc4']}"
        print("✓ test_linear_single_list passed")


    # Run all tests
    test_linear_basic()
    test_linear_window_parameter()
    test_linear_window_with_overlapping_docs()
    test_linear_single_list()
    print("All LINEAR scoring tests passed! ✓")
