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


def linear_with_limit(
    list1: List[Result],
    list2: List[Result],
    alpha: int,
    beta: int,
    window: int = 20,
    limit: int = None
) -> List[Result]:
    """
    Perform linear combination with final result limit (like K parameter).
    """
    result = linear(list1, list2, alpha, beta, window)
    
    # Apply limit if specified (like K parameter in RediSearch)
    if limit is not None and limit > 0:
        result = result[:limit]
    
    return result


if __name__ == "__main__":
    # Basic test
    list1 = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7)]
    list2 = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc1", 0.75)]

    result = linear(list1, list2, alpha=1, beta=1)
    print("Linear combination test:")
    for r in result:
        print(f"  {r.key}: {r.score}")
    
    # Test with limit
    result_limited = linear_with_limit(list1, list2, alpha=1, beta=1, limit=2)
    print("\nWith limit=2:")
    for r in result_limited:
        print(f"  {r.key}: {r.score}")
