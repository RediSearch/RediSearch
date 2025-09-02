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
    Perform linear combination on two ranked lists.

    Args:
        list1: List of Result objects from first ranking system
        list2: List of Result objects from second ranking system
        alpha: Weight coefficient for list1 scores
        beta: Weight coefficient for list2 scores
        window: Maximum number of results to consider from each list (default: 20)

    Returns:
        List of Result objects sorted by linear combination score in descending order

    Example:
        >>> list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        >>> list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)]
        >>> result = linear(list1, list2, alpha=1, beta=1, window=20)
        >>> print(result)
        [Result(key='doc2', score=1.75), Result(key='doc3', score=1.55), Result(key='doc1', score=1.65)]
    """
    # Apply window limit to input lists
    list1_windowed = list1[:window]
    list2_windowed = list2[:window]

    # Dictionary to store linear combination scores for each key
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

    # Calculate linear combination score for each key
    for key in all_keys:
        linear_score = 0.0

        # Add weighted contribution from list1 if key exists
        if key in scores1:
            linear_score += alpha * scores1[key]

        # Add weighted contribution from list2 if key exists
        if key in scores2:
            linear_score += beta * scores2[key]

        linear_scores[key] = linear_score

    # Convert to list of Result objects and sort by linear score (descending)
    result = [Result(key=key, score=score) for key, score in linear_scores.items()]
    result.sort(key=lambda x: x.score, reverse=True)

    return result


def linear_multiple(
    ranked_lists: List[List[Result]],
    weights: List[int],
    window: int = 20
) -> List[Result]:
    """
    Perform linear combination on multiple ranked lists.

    Args:
        ranked_lists: List of ranked lists, each containing Result objects
        weights: List of weight coefficients for each list (must match length of ranked_lists)
        window: Maximum number of results to consider from each list (default: 20)

    Returns:
        List of Result objects sorted by linear combination score in descending order

    Example:
        >>> lists = [
        ...     [Result("doc1", 0.9), Result("doc3", 0.8)],
        ...     [Result("doc2", 0.95), Result("doc3", 0.85)],
        ...     [Result("doc1", 0.9), Result("doc2", 0.7)]
        ... ]
        >>> weights = [1, 2, 1]
        >>> result = linear_multiple(lists, weights, window=20)
        >>> print(result)
        [Result(key='doc2', score=2.65), Result(key='doc1', score=1.8), Result(key='doc3', score=2.5)]
    """
    if not ranked_lists:
        return []

    if len(ranked_lists) == 1:
        return ranked_lists[0]

    if len(weights) != len(ranked_lists):
        raise ValueError("Number of weights must match number of ranked lists")

    # Dictionary to store linear combination scores for each key
    linear_scores: Dict[str, float] = defaultdict(float)

    # Apply window limit to all input lists
    windowed_lists = [ranked_list[:window] for ranked_list in ranked_lists]

    # Get all unique keys from all windowed lists
    all_keys: Set[str] = set()
    for ranked_list in windowed_lists:
        for result in ranked_list:
            all_keys.add(result.key)

    # Create score mappings for each windowed list
    score_mappings = []
    for ranked_list in windowed_lists:
        score_map = {result.key: result.score for result in ranked_list}
        score_mappings.append(score_map)

    # Calculate linear combination score for each key
    for key in all_keys:
        linear_score = 0.0

        # Add weighted contribution from each list where the key appears
        for i, score_map in enumerate(score_mappings):
            if key in score_map:
                linear_score += weights[i] * score_map[key]

        linear_scores[key] = linear_score

    # Convert to list of Result objects and sort by linear score (descending)
    result = [Result(key=key, score=score) for key, score in linear_scores.items()]
    result.sort(key=lambda x: x.score, reverse=True)

    return result


if __name__ == "__main__":

    # Unit tests
    def test_linear_basic_combination():
        """Test basic linear combination with two lists"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)]

        result = linear(list1, list2, alpha=1, beta=1)

        # Check that all documents are present
        doc_keys = [doc.key for doc in result]
        assert set(doc_keys) == {"doc1", "doc2", "doc3"}

        # Check that results are sorted by score (descending)
        scores = [doc.score for doc in result]
        assert scores == sorted(scores, reverse=True)

        # Verify specific linear combination calculations
        # doc1: 1 * 0.9 + 1 * 0.75 = 1.65
        # doc2: 1 * 0.8 + 1 * 0.95 = 1.75
        # doc3: 1 * 0.7 + 1 * 0.85 = 1.55
        result_dict = {doc.key: doc.score for doc in result}
        assert abs(result_dict["doc1"] - 1.65) < 1e-10
        assert abs(result_dict["doc2"] - 1.75) < 1e-10
        assert abs(result_dict["doc3"] - 1.55) < 1e-10

        # doc2 should be first (highest score)
        assert result[0].key == "doc2"


    def test_linear_with_non_overlapping_docs():
        """Test linear combination with documents that don't appear in both lists"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8)]
        list2 = [Result("doc3", 0.95), Result("doc4", 0.85)]

        result = linear(list1, list2, alpha=2, beta=3)

        # All documents should be present
        doc_keys = [doc.key for doc in result]
        assert set(doc_keys) == {"doc1", "doc2", "doc3", "doc4"}

        result_dict = {doc.key: doc.score for doc in result}

        # doc1 appears only in list1: 2 * 0.9 = 1.8
        assert abs(result_dict["doc1"] - 1.8) < 1e-10
        # doc2 appears only in list1: 2 * 0.8 = 1.6
        assert abs(result_dict["doc2"] - 1.6) < 1e-10
        # doc3 appears only in list2: 3 * 0.95 = 2.85
        assert abs(result_dict["doc3"] - 2.85) < 1e-10
        # doc4 appears only in list2: 3 * 0.85 = 2.55
        assert abs(result_dict["doc4"] - 2.55) < 1e-10


    def test_linear_different_alpha_beta_values():
        """Test linear combination with different alpha/beta values"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8)]
        list2 = [Result("doc1", 0.6), Result("doc2", 0.7)]

        # Test with alpha=2, beta=1 (favor list1)
        result_favor_list1 = linear(list1, list2, alpha=2, beta=1)

        # Test with alpha=1, beta=2 (favor list2)
        result_favor_list2 = linear(list1, list2, alpha=1, beta=2)

        dict1 = {doc.key: doc.score for doc in result_favor_list1}
        dict2 = {doc.key: doc.score for doc in result_favor_list2}

        # doc1 with alpha=2, beta=1: 2*0.9 + 1*0.6 = 2.4
        # doc1 with alpha=1, beta=2: 1*0.9 + 2*0.6 = 2.1
        assert abs(dict1["doc1"] - 2.4) < 1e-10
        assert abs(dict2["doc1"] - 2.1) < 1e-10

        # When favoring list1, doc1 should have higher relative score
        assert dict1["doc1"] > dict2["doc1"]


    def test_linear_multiple_lists():
        """Test linear combination with multiple lists"""
        lists = [
            [Result("doc1", 0.9), Result("doc2", 0.8)],
            [Result("doc2", 0.95), Result("doc3", 0.85)],
            [Result("doc3", 0.9), Result("doc1", 0.7)]
        ]
        weights = [1, 2, 1]

        result = linear_multiple(lists, weights)

        # All documents should be present
        doc_keys = [doc.key for doc in result]
        assert set(doc_keys) == {"doc1", "doc2", "doc3"}

        # Check manual calculation for doc1:
        # List 1: 1 * 0.9 = 0.9
        # List 2: not present = 0
        # List 3: 1 * 0.7 = 0.7
        # Total: 0.9 + 0 + 0.7 = 1.6
        expected_doc1 = 1.6

        result_dict = {doc.key: doc.score for doc in result}
        assert abs(result_dict["doc1"] - expected_doc1) < 1e-10


    def test_linear_empty_lists():
        """Test linear combination with empty lists"""
        result = linear([], [], alpha=1, beta=1)
        assert result == []

        result_multiple = linear_multiple([], [])
        assert result == []


    def test_linear_single_list():
        """Test linear combination with single list in multiple fusion"""
        single_list = [Result("doc1", 0.9), Result("doc2", 0.8)]
        result = linear_multiple([single_list], [1])

        # Should return the original list
        assert result == single_list


    def test_linear_identical_lists():
        """Test linear combination with identical lists"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
        list2 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]

        result = linear(list1, list2, alpha=1, beta=1)

        # Each document should have double its original score
        result_dict = {doc.key: doc.score for doc in result}

        # doc1: 1*0.9 + 1*0.9 = 1.8
        assert abs(result_dict["doc1"] - 1.8) < 1e-10
        # doc2: 1*0.8 + 1*0.8 = 1.6
        assert abs(result_dict["doc2"] - 1.6) < 1e-10
        # doc3: 1*0.7 + 1*0.7 = 1.4
        assert abs(result_dict["doc3"] - 1.4) < 1e-10


    def test_linear_window_parameter():
        """Test linear combination with window parameter limiting results"""
        # Create longer lists to test window effect
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6), Result("doc5", 0.5)]
        list2 = [Result("doc6", 0.95), Result("doc7", 0.85), Result("doc8", 0.75), Result("doc9", 0.65), Result("doc10", 0.55)]

        # Test with window=2 (should only consider top 2 from each list)
        result_window2 = linear(list1, list2, alpha=1, beta=1, window=2)

        # Should only have docs from the first 2 positions of each list
        expected_docs = {"doc1", "doc2", "doc6", "doc7"}
        result_docs = {doc.key for doc in result_window2}
        assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

        # Test with window=3 (should consider top 3 from each list)
        result_window3 = linear(list1, list2, alpha=1, beta=1, window=3)

        # Should have docs from the first 3 positions of each list
        expected_docs_3 = {"doc1", "doc2", "doc3", "doc6", "doc7", "doc8"}
        result_docs_3 = {doc.key for doc in result_window3}
        assert result_docs_3 == expected_docs_3, f"Expected {expected_docs_3}, got {result_docs_3}"


    def test_linear_window_with_overlapping_docs():
        """Test linear combination window parameter with overlapping documents"""
        # Lists with some overlapping docs
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6)]
        list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc5", 0.75), Result("doc6", 0.65)]

        # Test with window=2
        result = linear(list1, list2, alpha=1, beta=1, window=2)

        # Should only consider: doc1, doc2 from list1 and doc2, doc3 from list2
        # So final docs should be: doc1, doc2, doc3
        expected_docs = {"doc1", "doc2", "doc3"}
        result_docs = {doc.key for doc in result}
        assert result_docs == expected_docs, f"Expected {expected_docs}, got {result_docs}"

        # Verify doc2 has highest score (appears in both windowed lists)
        result_dict = {doc.key: doc.score for doc in result}

        # doc2 should have highest score: 1*0.8 + 1*0.95 = 1.75
        expected_doc2_score = 1.75
        assert abs(result_dict["doc2"] - expected_doc2_score) < 1e-10


    def test_linear_zero_weights():
        """Test linear combination with zero weights"""
        list1 = [Result("doc1", 0.9), Result("doc2", 0.8)]
        list2 = [Result("doc1", 0.6), Result("doc3", 0.7)]

        # Test with alpha=0 (ignore list1)
        result = linear(list1, list2, alpha=0, beta=1)
        result_dict = {doc.key: doc.score for doc in result}

        # doc1 should only get score from list2: 0*0.9 + 1*0.6 = 0.6
        assert abs(result_dict["doc1"] - 0.6) < 1e-10
        # doc2 should only get score from list1: 0*0.8 + 0 = 0.0
        assert abs(result_dict["doc2"] - 0.0) < 1e-10
        # doc3 should only get score from list2: 0 + 1*0.7 = 0.7
        assert abs(result_dict["doc3"] - 0.7) < 1e-10


    def test_linear_multiple_weights_mismatch():
        """Test linear_multiple with mismatched weights"""
        lists = [
            [Result("doc1", 0.9)],
            [Result("doc2", 0.8)]
        ]
        weights = [1]  # Only one weight for two lists

        try:
            linear_multiple(lists, weights)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Number of weights must match number of ranked lists" in str(e)

    # Run unit tests
    test_linear_basic_combination()
    test_linear_with_non_overlapping_docs()
    test_linear_different_alpha_beta_values()
    test_linear_multiple_lists()
    test_linear_empty_lists()
    test_linear_single_list()
    test_linear_identical_lists()
    test_linear_window_parameter()
    test_linear_window_with_overlapping_docs()
    test_linear_zero_weights()
    test_linear_multiple_weights_mismatch()

    print("All tests passed!")

    # Example usage
    print("\nLinear Combination Example:")

    # Example with two lists
    search_results = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6)]
    vector_results = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc5", 0.8), Result("doc1", 0.75)]

    print("\nSearch results:", search_results)
    print("Vector results:", vector_results)

    # Equal weighting
    fused_results = linear(search_results, vector_results, alpha=1, beta=1)
    print("\nLinear Combination (alpha=1, beta=1):")
    for result in fused_results:
        print(f"  {result.key}: {result.score:.6f}")

    # Favor search results
    fused_results_favor_search = linear(search_results, vector_results, alpha=2, beta=1)
    print("\nLinear Combination (alpha=2, beta=1 - favor search):")
    for result in fused_results_favor_search:
        print(f"  {result.key}: {result.score:.6f}")

    # Example with multiple lists
    print("\n" + "="*50)
    print("Multiple Lists Linear Combination Example:")

    lists = [
        [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)],
        [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)],
        [Result("doc3", 0.9), Result("doc1", 0.8), Result("doc4", 0.7)]
    ]
    weights = [1, 2, 1]  # Give double weight to the second list

    for i, lst in enumerate(lists):
        print(f"List {i+1} (weight={weights[i]}): {lst}")

    multi_fused = linear_multiple(lists, weights)
    print("\nMultiple Linear Combination results:")
    for result in multi_fused:
        print(f"  {result.key}: {result.score:.6f}")
