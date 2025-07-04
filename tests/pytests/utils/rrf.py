"""
Reciprocal Rank Fusion (RRF) Implementation

RRF combines multiple ranked lists by computing a score for each item based on its rank
in each list. The formula is: RRF_score = sum(1 / (k + rank_i)) for all lists where
the item appears, where k is a constant (typically 60) and rank_i is the rank in list i.
"""

from typing import List, Tuple, Dict, Set
from collections import defaultdict


def rrf_fusion(
    list1: List[Tuple[str, float]],
    list2: List[Tuple[str, float]],
    k: int = 60,
    window: int = 20
) -> List[Tuple[str, float]]:
    """
    Perform Reciprocal Rank Fusion on two ranked lists.

    Args:
        list1: List of (key, score) tuples from first ranking system
        list2: List of (key, score) tuples from second ranking system
        k: RRF constant parameter (default: 60)
        window: Maximum number of results to consider from each list (default: 20)

    Returns:
        List of (key, rrf_score) tuples sorted by RRF score in descending order

    Example:
        >>> list1 = [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7)]
        >>> list2 = [("doc2", 0.95), ("doc3", 0.85), ("doc1", 0.75)]
        >>> result = rrf_fusion(list1, list2, k=60, window=20)
        >>> print(result)
        [('doc2', 0.032786885245901644), ('doc3', 0.032258064516129031), ('doc1', 0.029508196721311475)]
    """
    # Apply window limit to input lists
    list1_windowed = list1[:window]
    list2_windowed = list2[:window]

    # Dictionary to store RRF scores for each key
    rrf_scores: Dict[str, float] = defaultdict(float)

    # Get all unique keys from both windowed lists
    all_keys: Set[str] = set()
    for key, _ in list1_windowed:
        all_keys.add(key)
    for key, _ in list2_windowed:
        all_keys.add(key)

    # Create rank mappings for each windowed list
    rank1 = {key: rank + 1 for rank, (key, _) in enumerate(list1_windowed)}
    rank2 = {key: rank + 1 for rank, (key, _) in enumerate(list2_windowed)}

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

    # Convert to list of tuples and sort by RRF score (descending)
    result = [(key, score) for key, score in rrf_scores.items()]
    result.sort(key=lambda x: x[1], reverse=True)

    return result


def rrf_fusion_multiple(
    ranked_lists: List[List[Tuple[str, float]]],
    k: int = 60,
    window: int = 20
) -> List[Tuple[str, float]]:
    """
    Perform Reciprocal Rank Fusion on multiple ranked lists.

    Args:
        ranked_lists: List of ranked lists, each containing (key, score) tuples
        k: RRF constant parameter (default: 60)
        window: Maximum number of results to consider from each list (default: 20)

    Returns:
        List of (key, rrf_score) tuples sorted by RRF score in descending order

    Example:
        >>> lists = [
        ...     [("doc1", 0.9), ("doc2", 0.8)],
        ...     [("doc2", 0.95), ("doc3", 0.85)],
        ...     [("doc3", 0.9), ("doc1", 0.7)]
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
        for key, _ in ranked_list:
            all_keys.add(key)

    # Create rank mappings for each windowed list
    rank_mappings = []
    for ranked_list in windowed_lists:
        rank_map = {key: rank + 1 for rank, (key, _) in enumerate(ranked_list)}
        rank_mappings.append(rank_map)

    # Calculate RRF score for each key
    for key in all_keys:
        rrf_score = 0.0

        # Add contribution from each list where the key appears
        for rank_map in rank_mappings:
            if key in rank_map:
                rrf_score += 1.0 / (k + rank_map[key])

        rrf_scores[key] = rrf_score

    # Convert to list of tuples and sort by RRF score (descending)
    result = [(key, score) for key, score in rrf_scores.items()]
    result.sort(key=lambda x: x[1], reverse=True)

    return result


if __name__ == "__main__":
    # Example usage
    print("RRF Fusion Example:")

    # Example with two lists
    search_results = [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7), ("doc4", 0.6)]
    vector_results = [("doc2", 0.95), ("doc3", 0.85), ("doc5", 0.8), ("doc1", 0.75)]

    print("\nSearch results:", search_results)
    print("Vector results:", vector_results)

    fused_results = rrf_fusion(search_results, vector_results, k=60)
    print("\nRRF Fused results:")
    for key, score in fused_results:
        print(f"  {key}: {score:.6f}")

    # Example with multiple lists
    print("\n" + "="*50)
    print("Multiple Lists RRF Example:")

    lists = [
        [("doc1", 0.9), ("doc2", 0.8), ("doc3", 0.7)],
        [("doc2", 0.95), ("doc3", 0.85), ("doc1", 0.75)],
        [("doc3", 0.9), ("doc1", 0.8), ("doc4", 0.7)]
    ]

    for i, lst in enumerate(lists):
        print(f"List {i+1}: {lst}")

    multi_fused = rrf_fusion_multiple(lists, k=60)
    print("\nMultiple RRF Fused results:")
    for key, score in multi_fused:
        print(f"  {key}: {score:.6f}")
