import re
from RLTest import Env
from itertools import groupby
from utils.rrf import rrf, Result
from common import getConnectionByEnv, create_np_array_typed
from typing import List, Tuple, Optional


# Constant string used in create_comparison_table() to indicate missing value
# or missing ranking info
MISSING_VALUE = "---"


def _sort_adjacent_same_scores(results: List[Result]) -> None:
    """
    Sort adjacent results with the same score by key for deterministic tests.

    Only sorts consecutive results with identical scores. Preserves score ordering.
    Does NOT sort non-adjacent results with the same score.

    Example: [Result('c', 0.5), Result('b', 1.0), Result('a', 1.0)] -> [Result('c', 0.5), Result('a', 1.0), Result('b', 1.0)]
    """
    grouped = []
    for _, group in groupby(results, key=lambda x: x.score):
        group_list = list(group)
        if len(group_list) > 1:
            group_list.sort(key=lambda x: x.key)
        grouped.extend(group_list)
    results[:] = grouped


def _validate_results(env, actual_results: List[Result], expected_results: List[Result], comparison_table: str) -> None:
    """Compare actual vs expected results, allowing for small score variations"""

    # Every test case should return at least one result
    env.assertGreater(len(actual_results), 0, message=comparison_table)

    # We assume the number of actual result is correct
    env.assertLessEqual(len(actual_results), len(expected_results), message=comparison_table)
    for i in range(len(actual_results)):
        if actual_results[i].score == actual_results[-1].score:
            # in this case, we cannot know which subset of the results is included in the response, so we just validate inclusion
            expected_results_with_last_score = [result.key for result in expected_results if abs(result.score - actual_results[i].score) < 1e-10]
            actual_results_with_last_score = [result.key for result in actual_results if abs(result.score - actual_results[i].score) < 1e-10]
            env.assertTrue(set(actual_results_with_last_score).issubset(set(expected_results_with_last_score)),
                message=f'Keys in actual results with last score ({actual_results[i].score}) not found in expected results with same score: {set(actual_results_with_last_score) - set(expected_results_with_last_score)}. {comparison_table}')
            break
        else:
            env.assertEqual(
                actual_results[i].key, expected_results[i].key,
                message=f'key mismatch at index {i}: actual: {actual_results[i].key}, expected: {expected_results[i].key}')
            env.assertAlmostEqual(
                actual_results[i].score, expected_results[i].score, delta=1e-10,
                message=f'score mismatch at index {i}: actual: {actual_results[i].score}, expected: {expected_results[i].score}')

def _process_search_response(search_results):
    """
    Process search response into list of Result objects

    Args:
        search_results: Raw Redis search response like:
                       [349, b'25669', b'10.94315946939261', b'64068', b'10.822403974287118', ...]

    Returns:
        list: [Result(key=doc_id_str, score=score_float), ...] objects
    """
    if not search_results or len(search_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = search_results[1:]

    # Pack into Result objects
    processed = []
    for i in range(0, len(results_data), 2):
        if i + 1 < len(results_data):
            doc_id = results_data[i].decode('utf-8') if isinstance(results_data[i], bytes) else str(results_data[i])
            score = float(results_data[i + 1].decode('utf-8') if isinstance(results_data[i + 1], bytes) else results_data[i + 1])
            processed.append(Result(key=doc_id, score=score))
    return processed


def _process_aggregate_response(aggregate_results):
    """
    Process aggregate response into list of Result objects

    Args:
        aggregate_results: Raw Redis aggregate response like:
        [30,
            ['__score', '1.69230771347', '__key', 'vector_10'],
            ['__score', '1.69230771347', '__key', 'vector_09'],...

    Returns:
        list: [Result(key=doc_id_str, score=score_float), ...] objects
    """
    if not aggregate_results or len(aggregate_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = aggregate_results[1:]

    # Pack into Result objects
    processed = []
    score = [float(row[row.index('__score') + 1] if '__score' in row else '0') for row in results_data]
    doc_id = [row[row.index('__key') + 1] for row in results_data]
    for i in range(0, len(score)):
        processed.append(Result(key=doc_id[i], score=score[i]))

    return processed


def _process_hybrid_response(hybrid_results, expected_results: Optional[List[Result]] = None) -> Tuple[List[Result], dict]:
    """
    Process hybrid response into list of Result objects and ranking info

    Args:
        hybrid_results: Raw Redis hybrid response like:
             ['format', 'STRING', 'results', ['attributes', ['__key', 'both_02', 'SEARCH_RANK', '2', 'VECTOR_RANK', '5', '__score', '0.0312805474096']], ...]
        expected_results: Optional list of expected Result objects for comparison

    Returns:
        tuple: ([Result(key=doc_id_str, score=score_float), ...], ranking_info_dict)

    Note: ranking_info_dict contains search_ranks and vector_ranks for each document
    """
    if not hybrid_results or len(hybrid_results) < 4:
        return [], {}

    # Extract the results array from index 3
    # Structure: ['format', 'STRING', 'results', [result_items...]]
    results_data = hybrid_results[3]
    if not results_data:
        return [], {}

    processed = []
    ranking_info = {'search_ranks': {}, 'vector_ranks': {}}

    for result_item in results_data:
        attrs = dict(zip(result_item[::2], result_item[1::2]))

        # Extract doc_id and score if both exist
        if '__key' in attrs and '__score' in attrs:
            try:
                score = float(attrs['__score'])
                doc_id = attrs['__key']

                # Extract ranking information
                search_rank = attrs.get('SEARCH_RANK', '-')
                vector_rank = attrs.get('VECTOR_RANK', '-')

                # Store ranking info (convert to int if not '-')
                if search_rank != '-':
                    try:
                        ranking_info['search_ranks'][doc_id] = int(search_rank)
                    except ValueError:
                        pass

                if vector_rank != '-':
                    try:
                        ranking_info['vector_ranks'][doc_id] = int(vector_rank)
                    except ValueError:
                        pass

                processed.append(Result(key=doc_id, score=score))
            except (ValueError, TypeError):
                pass  # Skip invalid scores

    return processed, ranking_info


def _create_comparison_table(actual_results: List[Result], expected_results: List[Result],
                           ranking_info: dict = None, original_search_results: List[Result] = None,
                           original_vector_results: List[Result] = None) -> str:
    """Create side-by-side comparison table of actual vs expected results with search/vector rankings"""
    lines = []
    lines.append("="*200)
    lines.append(f"{'RANK':<6} {'ACTUAL DOC_ID':<20} {'ACTUAL SCORE':<15} {'A_SEARCH':<10} {'A_VECTOR':<10} {'|':<3} {'EXPECTED DOC_ID':<20} {'EXPECTED SCORE':<15} {'E_SEARCH':<10} {'E_VECTOR':<10} {'MATCH':<8}")
    lines.append("-"*200)

    # Get ranking maps from hybrid results (for actual results)
    actual_search_rank_map = ranking_info.get('search_ranks', {}) if ranking_info else {}
    actual_vector_rank_map = ranking_info.get('vector_ranks', {}) if ranking_info else {}

    # Create ranking maps from original search and vector results (for expected results)
    expected_search_rank_map = {}
    expected_vector_rank_map = {}

    if original_search_results:
        for rank, result in enumerate(original_search_results, 1):
            expected_search_rank_map[result.key] = rank

    if original_vector_results:
        for rank, result in enumerate(original_vector_results, 1):
            expected_vector_rank_map[result.key] = rank

    max_len = max(len(actual_results), len(expected_results))

    for i in range(max_len):
        # Get actual result
        if i < len(actual_results):
            actual_result = actual_results[i]
            actual_doc_str = actual_result.key[:19]  # Truncate if too long
            actual_score_str = f"{actual_result.score:.10f}"

            # Get search and vector rankings for actual doc (from hybrid results)
            actual_search_rank = actual_search_rank_map.get(actual_result.key, MISSING_VALUE)
            actual_vector_rank = actual_vector_rank_map.get(actual_result.key, MISSING_VALUE)
            actual_search_str = str(actual_search_rank) if actual_search_rank != MISSING_VALUE else MISSING_VALUE
            actual_vector_str = str(actual_vector_rank) if actual_vector_rank != MISSING_VALUE else MISSING_VALUE
        else:
            actual_doc_str = MISSING_VALUE
            actual_score_str = MISSING_VALUE
            actual_search_str = MISSING_VALUE
            actual_vector_str = MISSING_VALUE

        # Get expected result
        if i < len(expected_results):
            expected_result = expected_results[i]
            expected_doc_str = expected_result.key[:19]  # Truncate if too long
            expected_score_str = f"{expected_result.score:.10f}"

            # Get search and vector rankings for expected doc (from original results)
            expected_search_rank = expected_search_rank_map.get(expected_result.key, MISSING_VALUE)
            expected_vector_rank = expected_vector_rank_map.get(expected_result.key, MISSING_VALUE)
            expected_search_str = str(expected_search_rank) if expected_search_rank != MISSING_VALUE else MISSING_VALUE
            expected_vector_str = str(expected_vector_rank) if expected_vector_rank != MISSING_VALUE else MISSING_VALUE
        else:
            expected_doc_str = MISSING_VALUE
            expected_score_str = MISSING_VALUE
            expected_search_str = MISSING_VALUE
            expected_vector_str = MISSING_VALUE

        # Check if they match
        if (i < len(actual_results) and i < len(expected_results) and
            actual_results[i].key == expected_results[i].key):
            match_str = "✓"
        else:
            match_str = "✗"

        lines.append(f"{i+1:<6} {actual_doc_str:<20} {actual_score_str:<15} {actual_search_str:<10} {actual_vector_str:<10} {'|':<3} {expected_doc_str:<20} {expected_score_str:<15} {expected_search_str:<10} {expected_vector_str:<10} {match_str:<8}")

    lines.append("="*200)
    return "\n" + "\n".join(lines) + "\n"

def _process_vector_response(vector_results):
    """
    Process vector response into list of Result objects

    Args:
        vector_results: Raw Redis vector response like:
                       [10, b'45767', [b'score', b'0.961071372032'], b'16617', [b'score', b'0.956172764301'], ...]

    Returns:
        list: [Result(key=doc_id_str, score=score_float), ...] objects
    """
    if not vector_results or len(vector_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = vector_results[1:]

    # Pack into Result objects
    processed = []
    for i in range(0, len(results_data), 2):
        if i + 1 < len(results_data):
            doc_id = results_data[i].decode('utf-8') if isinstance(results_data[i], bytes) else str(results_data[i])

            # Extract score from nested array [b'score', b'0.961071372032']
            score_data = results_data[i + 1]
            if isinstance(score_data, list) and len(score_data) >= 2:
                score_value = score_data[1]
                score = float(score_value.decode('utf-8') if isinstance(score_value, bytes) else score_value)
            else:
                score = 0.0  # fallback

            processed.append(Result(key=doc_id, score=score))
    return processed

# =============================================================================
# QUERY TRANSLATION LAYER
# =============================================================================

def translate_vector_query(vector_query, vector_blob, index_name, cmd_suffix):
    """
    Translate simple vector query notation to working Redis command

    Args:
        simple_query: Simple notation like "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        vector_blob: Vector data as bytes
        index_name: Redis index name

    Returns:
        list: Command parts for redis_client.execute_command
    """
    command_parts = [
        'FT.SEARCH', index_name, vector_query,
        'PARAMS', '2', 'BLOB', vector_blob,
        'RETURN', '1', 'vector_distance', 'SORTBY', 'vector_distance',
        'DIALECT', '2',
    ]
    if cmd_suffix:
        command_parts.extend(cmd_suffix.split(' '))
    return command_parts


def translate_search_query(search_query, index_name):
    """
    Translate simple search query to Redis command

    Args:
        simple_query: Like "FT.SEARCH idx number"
        index_name: Redis index name

    Returns:
        list: Command parts for redis_client.execute_command
    """

    # Split into command parts
    command_parts = [
        'FT.SEARCH', index_name, search_query, 'WITHSCORES', 'NOCONTENT',
        'DIALECT', '2', 'LIMIT', '0', '20'
    ]
    return command_parts

def translate_hybrid_query(hybrid_query, vector_blob, index_name):
    """
    Translate simple hybrid query notation to working Redis command

    Args:
        simple_query: Simple notation like "SEARCH hello VSIM @vector $BLOB"
        vector_blob: Vector data as bytes
        index_name: Redis index name

    Returns:
        list: Command parts for redis_client.execute_command
    """
    cmd = f'FT.HYBRID {index_name} {hybrid_query}'
    # Split into command parts, keeping single quoted strings together
    command_parts = [p for p in re.split(r" (?=(?:[^']*'[^']*')*[^']*$)", cmd) if p]
    # Remove single quotes from command parts
    command_parts = [p.replace("'", "") for p in command_parts]
    # Add PARAMS section with the vector blob
    command_parts.extend(['PARAMS', '2', 'BLOB', vector_blob])
    return command_parts

# =============================================================================
# TEST EXECUTION
# =============================================================================

def run_test_scenario(env, index_name, scenario, vector_blob):
    """
    Run a test scenario from dict

    Args:
        scenario: Dict with test scenario
        index_name: Redis index name
        vector_blob: Vector data as bytes
    """

    conn = getConnectionByEnv(env)

    # Execute search query
    search_cmd = translate_search_query(scenario['search_equivalent'], index_name)
    search_results_raw = conn.execute_command(*search_cmd)

    # Process search results
    search_results = _process_search_response(search_results_raw)

    # Execute vector query using translation
    vector_cmd = translate_vector_query(
                    scenario['vector_equivalent'], vector_blob,
                    index_name, scenario.get('vector_suffix', ''))
    vector_results_raw = conn.execute_command(*vector_cmd)

    # Process vector results
    vector_results = _process_vector_response(vector_results_raw)

    rrf_constant = scenario.get('rrf_constant', 60)
    expected_rrf = rrf(search_results, vector_results, k=rrf_constant)
    _sort_adjacent_same_scores(expected_rrf)

    hybrid_cmd = translate_hybrid_query(
                scenario['hybrid_query'], vector_blob, index_name)
    # Use env.cmd for FT.HYBRID to handle cluster routing properly
    hybrid_results_raw = env.cmd(*hybrid_cmd)

    hybrid_results, ranking_info = _process_hybrid_response(hybrid_results_raw)
    _sort_adjacent_same_scores(hybrid_results)

    # Create comparison table for debugging
    comparison_table = _create_comparison_table(hybrid_results, expected_rrf, ranking_info, search_results, vector_results)
    # print(comparison_table)

    # Assert with detailed comparison table on failure
    _validate_results(env, hybrid_results, expected_rrf, comparison_table)
    return True
