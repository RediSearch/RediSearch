import numpy as np
from typing import List, Tuple, Optional
from RLTest import Env
from common import getConnectionByEnv, to_dict, debug_cmd, create_np_array_typed
from utils.rrf import rrf_fusion, Result


def _sort_adjacent_same_scores(results: List[Result]) -> None:
    """
    Sort adjacent results with the same score by key for deterministic tests.

    Only sorts consecutive results with identical scores. Preserves score ordering.
    Does NOT sort non-adjacent results with the same score.

    Example: [Result('c', 0.5), Result('b', 1.0), Result('a', 1.0)] -> [Result('c', 0.5), Result('a', 1.0), Result('b', 1.0)]
    """
    i = 0
    while i < len(results):
        # Find the end of the group with the same score
        j = i
        while j < len(results) and results[j].score == results[i].score:
            j += 1
        # Sort the group by key if it has more than one element
        if j - i > 1:
            results[i:j] = sorted(results[i:j], key=lambda x: x.key)
        i = j

# =============================================================================
# HYBRID SEARCH TESTS CLASS
# =============================================================================

class testHybridSearch:
    '''
    Run all hybrid search tests on a single env without taking
    env down between tests. The test data is created once in __init__.
    '''
    def __init__(self):
        self.env = Env()
        self.index_name = self._generate_hybrid_test_data()

    def _generate_hybrid_test_data(self):
        """
        Generate sample data for hybrid search tests.
        This runs once when the class is instantiated.

        Returns:
            str: index_name
        """
        index_name = "idx"
        dim = 128
        num_vectors = 10
        data_type = "FLOAT32"

        # Create index with vector, text, numeric and tag fields
        try:
            self.env.cmd('FT.DROPINDEX', index_name)
        except:
            pass  # Index doesn't exist, which is fine
        self.env.expect(
            'FT.CREATE', index_name, 'SCHEMA',
            'vector', 'VECTOR', 'FLAT', '6', 'TYPE', data_type, 'DIM', dim,
            'DISTANCE_METRIC', 'L2',
            'text', 'TEXT',
            'number', 'NUMERIC',
            'tag', 'TAG').ok()

        # Generate and load data
        np.random.seed(42)  # For reproducibility
        conn = getConnectionByEnv(self.env)
        p = conn.pipeline(transaction=False)

        words = ["zero", "one", "two", "three", "four", "five", "six", "seven",
                 "eight", "nine"]

        for i in range(1, num_vectors + 1):
            # Generate field values

            if i % 2 == 0:
                tag_value = "even"
            else:
                tag_value = "odd"

            text_value = f"Only text number {words[i % len(words)]} {tag_value}"

            # Create documents with only text
            p.execute_command('HSET', f'only_text_{i:02d}',
                              'text', text_value, 'number', i, 'tag', tag_value)

            # Create documents with only vector
            p.execute_command('HSET', f'only_vector_{i:02d}',
                              'vector', np.random.rand(dim).astype(np.float32).tobytes(),
                              'number', i, 'tag', tag_value)

            # Create documents with both vector and text data
            p.execute_command('HSET', f'both_{i:02d}',
                              'vector', np.random.rand(dim).astype(np.float32).tobytes(),
                              'text', text_value, 'number', i, 'tag', tag_value)

        p.execute()
        return index_name

    def test_knn_single_token_search(self):
        """Test hybrid search using KNN + single token search scenario"""
        scenario = {
            "test_name": "Single token text search",
            "hybrid_query": "FT.HYBRID idx SEARCH two VSIM @vector $BLOB LIMIT 0 11",
            "search_equivalent": "two",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_wildcard_search(self):
        """Test hybrid search using KNN + wildcard search scenario"""
        scenario = {
            "test_name": "Wildcard text search",
            "hybrid_query": "FT.HYBRID idx SEARCH * VSIM @vector $BLOB",
            "search_equivalent": "*",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        # TODO: Why the search_equivalent query returns 'only_vector_' docs with higher scores than the ones from 'both_' docs?
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_custom_k(self):
        """Test hybrid search using KNN with custom k scenario"""
        scenario = {
            "test_name": "KNN with custom k",
            "hybrid_query": "FT.HYBRID idx SEARCH even VSIM @vector $BLOB KNN 2 K 5",
            "search_equivalent": "even",
            "vector_equivalent": "*=>[KNN 5 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_with_parameters(self):
        """Test hybrid search using KNN + EF_RUNTIME parameter"""
        scenario = {
            "test_name": "KNN query with parameters",
            "hybrid_query": "FT.HYBRID idx SEARCH even VECTOR @vector $BLOB KNN 4 EF_RUNTIME 100 YIELD_DISTANCE_AS vector_distance",
            "search_equivalent": "even",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB EF_RUNTIME 100 AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_text_vector_prefilter(self):
        """Test hybrid search using KNN + text prefilter"""
        scenario = {
            "test_name": "KNN with text prefilter",
            "hybrid_query": "FT.HYBRID idx SEARCH @text:(even) VSIM @vector $BLOB FILTER @text:(six|four)",
            "search_equivalent": "@text:(even)",
            "vector_equivalent": "(@text:(six|four))=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_numeric_vector_prefilter(self):
        """Test hybrid search using KNN + numeric prefilter"""
        scenario = {
            "test_name": "KNN with numeric prefilter",
            "hybrid_query": f"SEARCH @text:even VSIM @vector $BLOB FILTER @number:[2 5]",
            "search_equivalent": "@text:even",
            "vector_equivalent": "(@number:[2 5])=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_tag_vector_prefilter(self):
        """Test hybrid search using KNN + tag prefilter"""
        scenario = {
            "test_name": "KNN with tag prefilter",
            "hybrid_query": "FT.HYBRID idx SEARCH @text:even VSIM @vector $BLOB FILTER @tag:{odd}",
            "search_equivalent": "@text:even",
            "vector_equivalent": "(@tag:{odd})=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_no_vector_results(self):
        """Test hybrid search using KNN + vector prefilter that returns zero results"""
        scenario = {
            "test_name": "KNN with vector prefilter that returns zero results",
            "hybrid_query": "FT.HYBRID idx SEARCH @text:even VSIM @vector $BLOB FILTER @tag:{invalid_tag}",
            "search_equivalent": "@text:even",
            "vector_equivalent": "(@tag:{invalid_tag})=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_no_text_results(self):
        """Test hybrid search using KNN + text prefilter that returns zero results"""
        scenario = {
            "test_name": "KNN with vector prefilter that returns zero results",
            "hybrid_query": "FT.HYBRID idx SEARCH @text:(invalid_text) VSIM @vector $BLOB",
            "search_equivalent": "@text:(invalid_text)",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_range_basic(self):
        """Test hybrid search using range query scenario"""
        scenario = {
            "test_name": "Range query",
            "hybrid_query": "FT.HYBRID idx SEARCH @text:(four|even) VSIM @vector $BLOB RANGE 2 RADIUS 5",
            "search_equivalent": "@text:(four|even)",
            "vector_equivalent": "@vector:[VECTOR_RANGE 5 $BLOB]=>{$YIELD_DISTANCE_AS: vector_distance}"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_range_with_parameters(self):
        """Test hybrid search using range with parameters"""
        scenario = {
            "test_name": "Range query",
            "hybrid_query": "FT.HYBRID idx SEARCH @text:(four|even) VSIM @vector $BLOB RANGE 6 RADIUS 5 EPSILON 0.5 YIELD_DISTANCE_AS vector_distance",
            "search_equivalent": "@text:(four|even)",
            "vector_equivalent": "@vector:[VECTOR_RANGE 5 $BLOB]=>{$EPSILON:0.5; $YIELD_DISTANCE_AS: vector_distance}"
        }
        run_test_scenario(self.env, self.index_name, scenario)


# =============================================================================
# QUERY TRANSLATION LAYER
# =============================================================================

def process_search_response(search_results):
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
    # _sort_adjacent_same_scores(processed)
    return processed


def process_aggregate_response(aggregate_results):
    """
    Process aggregate response into list of Result objects

    Args:
        aggregate_results: Raw Redis aggregate response like:
        [30,
            ['__score', '1.69230771347', '__key', 'only_vector_10'],
            ['__score', '1.69230771347', '__key', 'only_vector_09'],...

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

    # printprocessed)

    return processed

def process_hybrid_response(hybrid_results, expected_results: Optional[List[Result]] = None) -> Tuple[List[Result], dict]:
    """
    Process hybrid response into list of Result objects and ranking info

    Args:
        hybrid_results: Raw Redis hybrid response like:
             ['format', 'STRING', 'results', [['attributes', [['__key', 'both_02', 'SEARCH_RANK', '2', 'VECTOR_RANK', '5', '__score', '0.0312805474096']]], ...]]
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
        # Each result_item is: ['attributes', [['__key', doc_id, 'SEARCH_RANK', '2', 'VECTOR_RANK', '5', '__score', score_str]]]
        if (len(result_item) >= 2 and
            result_item[0] == 'attributes' and
            result_item[1] and
            isinstance(result_item[1][0], list)):

            # Convert flat key-value list to dict using zip with slicing
            attr_list = result_item[1][0]
            attrs = dict(zip(attr_list[::2], attr_list[1::2]))

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


def create_comparison_table(actual_results: List[Result], expected_results: List[Result],
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
            actual_search_rank = actual_search_rank_map.get(actual_result.key, "---")
            actual_vector_rank = actual_vector_rank_map.get(actual_result.key, "---")
            actual_search_str = str(actual_search_rank) if actual_search_rank != "---" else "---"
            actual_vector_str = str(actual_vector_rank) if actual_vector_rank != "---" else "---"
        else:
            actual_doc_str = "---"
            actual_score_str = "---"
            actual_search_str = "---"
            actual_vector_str = "---"

        # Get expected result
        if i < len(expected_results):
            expected_result = expected_results[i]
            expected_doc_str = expected_result.key[:19]  # Truncate if too long
            expected_score_str = f"{expected_result.score:.10f}"

            # Get search and vector rankings for expected doc (from original results)
            expected_search_rank = expected_search_rank_map.get(expected_result.key, "---")
            expected_vector_rank = expected_vector_rank_map.get(expected_result.key, "---")
            expected_search_str = str(expected_search_rank) if expected_search_rank != "---" else "---"
            expected_vector_str = str(expected_vector_rank) if expected_vector_rank != "---" else "---"
        else:
            expected_doc_str = "---"
            expected_score_str = "---"
            expected_search_str = "---"
            expected_vector_str = "---"

        # Check if they match
        if (i < len(actual_results) and i < len(expected_results) and
            actual_results[i].key == expected_results[i].key):
            match_str = "✓"
        else:
            match_str = "✗"

        lines.append(f"{i+1:<6} {actual_doc_str:<20} {actual_score_str:<15} {actual_search_str:<10} {actual_vector_str:<10} {'|':<3} {expected_doc_str:<20} {expected_score_str:<15} {expected_search_str:<10} {expected_vector_str:<10} {match_str:<8}")

    lines.append("="*200)
    return "\n" + "\n".join(lines) + "\n"

def process_vector_response(vector_results):
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
    # _sort_adjacent_same_scores(processed)
    return processed


def translate_vector_query(vector_query, vector_blob, index_name):
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
        'DIALECT', '2'
    ]
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
        'DIALECT', '2'
    ]
    return command_parts


# =============================================================================
# TEST EXECUTION
# =============================================================================

def run_test_scenario(env, index_name, scenario):
    """
    Run a test scenario from JSON file

    Args:
        scenario_file: Path to JSON scenario file
    """

    conn = getConnectionByEnv(env)

    # Create test vector (zero vector for now)
    dim = 128
    test_vector = create_np_array_typed([3.1415] * dim)
    vector_blob = test_vector.tobytes()

    # printf"Running test: {scenario['test_name']}")
    # printf"Using index: {index_name}")

    # Execute search query
    search_cmd = translate_search_query(scenario['search_equivalent'], index_name)
    # printf"Search command: {search_cmd}")
    # # printf"Search command: {' '.join(search_cmd)}")
    search_results_raw = conn.execute_command(*search_cmd)
    # printf"Search results raw: {search_results_raw}")

    # Process search results
    search_results = process_search_response(search_results_raw)
    # printf"Search results count: {len(search_results)}")
    # # printf"search results: {search_results}")
    search_results_docs = [result.key for result in search_results]
    # printf"search results docs: {search_results_docs}")


    # Execute vector query using translation
    vector_cmd = translate_vector_query(scenario['vector_equivalent'], vector_blob, index_name)
    # printf"Vector command: {' '.join(str(x) for x in vector_cmd[:3])} ... [with vector blob]")
    vector_results_raw = conn.execute_command(*vector_cmd)

    # Process vector results
    vector_results = process_vector_response(vector_results_raw)
    # printf"Vector results count: {len(vector_results)}")
    # # printf"vector results: {vector_results}")
    vector_results_docs = [result.key for result in vector_results]
    # printf"vector results docs: {vector_results_docs}")

    # printf"Search results for RRF: {search_results}")
    # printf"Vector results for RRF: {vector_results}")

    expected_rrf = rrf_fusion(search_results, vector_results)
    _sort_adjacent_same_scores(expected_rrf)
    # printf"Expected RRF results: {expected_rrf}")
    expected_rrf_docs = [result.key for result in expected_rrf]
    # printf"Expected RRF results docs: {expected_rrf_docs}")

    hybrid_results_raw = conn.execute_command(scenario['hybrid_query'].replace('$BLOB', vector_blob.decode('utf-8')))
    hybrid_results, ranking_info = process_hybrid_response(hybrid_results_raw)
    _sort_adjacent_same_scores(hybrid_results)

    # Create comparison table for debugging
    comparison_table = create_comparison_table(hybrid_results, expected_rrf, ranking_info, search_results, vector_results)
    # comparison_table = ''

    # # printcomparison_table)

    # Assert with detailed comparison table on failure
    env.assertEqual(hybrid_results, expected_rrf[:10], message=comparison_table)
    return True
