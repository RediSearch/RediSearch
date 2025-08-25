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


def rrf_fusion(
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
        >>> result = rrf_fusion(list1, list2, k=60, window=20)
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


def rrf_fusion_multiple(
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
        ...     [Result("doc1", 0.9), Result("doc2", 0.8)],
        ...     [Result("doc2", 0.95), Result("doc3", 0.85)],
        ...     [Result("doc3", 0.9), Result("doc1", 0.7)]
        ... ]
        >>> result = rrf_fusion_multiple(lists, k=60, window=20)
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
    # Example usage
    print("RRF Fusion Example:")

    # Example with two lists
    search_results = [Result("doc1", 0.9), Result("doc2", 0.8), Result("doc3", 0.7), Result("doc4", 0.6)]
    vector_results = [Result("doc2", 0.95), Result("doc3", 0.85), Result("doc5", 0.8), Result("doc1", 0.75)]

    print("\nSearch results:", search_results)
    print("Vector results:", vector_results)

    fused_results = rrf_fusion(search_results, vector_results, k=60)
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

    multi_fused = rrf_fusion_multiple(lists, k=60)
    print("\nMultiple RRF Fused results:")
    for result in multi_fused:
        print(f"  {result.key}: {result.score:.6f}")
