import numpy as np
from RLTest import Env
from common import getConnectionByEnv, to_dict, debug_cmd, create_np_array_typed
from utils.rrf import rrf_fusion

# =============================================================================
# DATA GENERATION
# =============================================================================

def generate_hybrid_test_data(env, index_name="idx", dim=128, num_vectors=10, data_type="FLOAT32"):
    """
    Generate sample data for hybrid search tests.

    Args:
        env: RLTest environment
        index_name: Name of the index to create
        dim: Vector dimension
        num_vectors: Number of vectors to generate
        data_type: Vector data type (FLOAT32 or FLOAT64)v

    Returns:
        A tuple containing (index_name, vector_field_name, text_field_name)
    """
    vector_field = "vector"
    text_field = "text"

    # Create index with vector and text fields
    env.expect('FT.CREATE', index_name, 'SCHEMA',
               vector_field, 'VECTOR', 'HNSW', '6', 'TYPE', data_type,
               'DIM', dim, 'DISTANCE_METRIC', 'L2',
               text_field, 'TEXT').ok()

    # Generate and load data
    np.random.seed(42)  # For reproducibility
    conn = getConnectionByEnv(env)
    p = conn.pipeline(transaction=False)

    words = ["zero", "one", "two", "three", "four", "five", "six", "seven",
             "eight", "nine"]

    # Create documents with only text
    for i in range(1, num_vectors + 1):
        text_value = f"Only text number {words[i % len(words)]}"
        if i % 2 == 0:
            text_value += " even"
        else:
            text_value += " odd"

        p.execute_command('HSET', f'only_text_{i:02d}', text_field, text_value)

    p.execute()

    # Create documents with only vector
    for i in range(1, num_vectors + 1):
        vector = create_np_array_typed([i] * dim)
        # vector = np.random.rand(dim).astype(np.float32 if data_type == "FLOAT32" else np.float64)
        p.execute_command('HSET', f'only_vector_{i:02d}', vector_field, vector.tobytes())

    p.execute()

    # Create documents with both vector and text data
    for i in range(1, num_vectors + 1):
        # Create a random vector
        vector = create_np_array_typed([i] * dim)
        # vector = np.random.rand(dim).astype(np.float32 if data_type == "FLOAT32" else np.float64)

        # Assign text based on vector properties
        # Documents with even IDs contain "even" for our test case
        text_value = f"Number {words[i % len(words)]}"
        if i % 2 == 0:
            text_value += " even"

        p.execute_command('HSET', f'both_{i:02d}', vector_field, vector.tobytes(), text_field, text_value)

    p.execute()

    return index_name, vector_field, text_field

# =============================================================================
# QUERY TRANSLATION LAYER
# =============================================================================

def process_search_response(search_results):
    """
    Process search response into list of (doc_id, score) tuples

    Args:
        search_results: Raw Redis search response like:
                       [349, b'25669', b'10.94315946939261', b'64068', b'10.822403974287118', ...]

    Returns:
        list: [(doc_id_str, score_float), ...] tuples
    """
    if not search_results or len(search_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = search_results[1:]

    # Pack into tuples: (doc_id, score)
    processed = []
    for i in range(0, len(results_data), 2):
        if i + 1 < len(results_data):
            doc_id = results_data[i].decode('utf-8') if isinstance(results_data[i], bytes) else str(results_data[i])
            score = float(results_data[i + 1].decode('utf-8') if isinstance(results_data[i + 1], bytes) else results_data[i + 1])
            processed.append((doc_id, score))

    return processed


def process_aggregate_response(aggregate_results):
    """
    Process aggregate response into list of (doc_id, score) tuples

    Args:
        aggregate_results: Raw Redis aggregate response like:
        [30,
            ['__score', '1.69230771347', '__key', 'only_vector_10'],
            ['__score', '1.69230771347', '__key', 'only_vector_09'],...

    Returns:
        list: [(doc_id_str, score_float), ...] tuples
    """
    if not aggregate_results or len(aggregate_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = aggregate_results[1:]

    # Pack into tuples: (doc_id, score)
    processed = []
    score = [float(row[row.index('__score') + 1] if '__score' in row else '0') for row in results_data]
    doc_id = [row[row.index('__key') + 1] for row in results_data]
    for i in range(0, len(score)):
        processed.append((doc_id[i], score[i]))

    print(processed)

    return processed


def process_vector_response(vector_results):
    """
    Process vector response into list of (doc_id, score) tuples

    Args:
        vector_results: Raw Redis vector response like:
                       [10, b'45767', [b'score', b'0.961071372032'], b'16617', [b'score', b'0.956172764301'], ...]

    Returns:
        list: [(doc_id_str, score_float), ...] tuples
    """
    if not vector_results or len(vector_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = vector_results[1:]

    # Pack into tuples: (doc_id, [score_field, score_value])
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

            processed.append((doc_id, score))

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

def translate_aggregate_query(aggregate_query, index_name):
    """
    Translate simple aggregate query to Redis command

    Args:
        simple_query: Like "FT.AGGREGATE idx number"
        index_name: Redis index name

    Returns:
        list: Command parts for redis_client.execute_command
    """

    # Split into command parts
    command_parts = [
        'FT.AGGREGATE', index_name, aggregate_query, 'ADDSCORES',
        'LOAD', '2', '@__key', '@__score',
        'SORTBY', '4', '@__score', 'DESC', '@__key', 'ASC',
        'DIALECT', '2', 'LIMIT', '0', '10'
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

    print(f"Running test: {scenario['test_name']}")
    print(f"Using index: {index_name}")

    # Execute search query
    search_cmd = translate_search_query(scenario['search_equivalent'], index_name)
    print(f"Search command: {search_cmd}")
    # print(f"Search command: {' '.join(search_cmd)}")
    search_results_raw = conn.execute_command(*search_cmd)
    print(f"Search results raw: {search_results_raw}")

    # Process search results
    search_results = process_search_response(search_results_raw)
    print(f"Search results count: {len(search_results)}")
    # print(f"search results: {search_results}")
    search_results_docs = [k for k, _ in search_results]
    print(f"search results docs: {search_results_docs}")

    # TODO: Add aggregate query execution, which could be used instead of search
    # # Execute aggregate query
    # aggregate_cmd = translate_aggregate_query(scenario['search_equivalent'], index_name)
    # print(f"Aggregate command: {aggregate_cmd}")
    # aggregate_results_raw = conn.execute_command(*aggregate_cmd)
    # print(f"Aggregate results raw: {aggregate_results_raw}")

    # # Process aggregate results
    # aggregate_results = process_aggregate_response(aggregate_results_raw)
    # print(f"Aggregate results count: {len(aggregate_results)}")
    # print(f"aggregate results: {aggregate_results}")
    # aggregate_results_docs = [k for k, _ in aggregate_results]
    # print(f"aggregate results docs: {aggregate_results_docs}")

    # Execute vector query using translation
    vector_cmd = translate_vector_query(scenario['vector_equivalent'], vector_blob, index_name)
    print(f"Vector command: {' '.join(str(x) for x in vector_cmd[:3])} ... [with vector blob]")
    vector_results_raw = conn.execute_command(*vector_cmd)

    # Process vector results
    vector_results = process_vector_response(vector_results_raw)
    print(f"Vector results count: {len(vector_results)}")
    # print(f"vector results: {vector_results}")
    vector_results_docs = [k for k, _ in vector_results]
    print(f"vector results docs: {vector_results_docs}")

    expected_rrf = rrf_fusion(search_results, vector_results)
    # print(f"Expected RRF results: {expected_rrf}")
    expected_rrf_docs = [k for k, _ in expected_rrf]
    print(f"Expected RRF results docs: {expected_rrf_docs}")

    # TODO: Add hybrid query execution and RRF comparison
    # hybrid_results = execute_hybrid_query(scenario['hybrid_query'], vector_blob, index_name)
    # env.assertEqual(hybrid_results, expected_rrf)
    return True

def test_knn_single_token_search(env):
    """Test hybrid search using KNN + single token search scenario"""
    index_name, vector_field, text_field = generate_hybrid_test_data(env)
    scenario = {
        "test_name": "Single token text search",
        "hybrid_query": "FT.HYBRID idx SEARCH two VSIM @vector $BLOB",
        "search_equivalent": "two",
        "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
    }
    run_test_scenario(env, index_name, scenario)

def test_knn_wildcard_search(env):
    """Test hybrid search using KNN + wildcard search scenario"""

    index_name, vector_field, text_field = generate_hybrid_test_data(env)
    scenario = {
        "test_name": "Wildcard text search",
        "hybrid_query": "FT.HYBRID idx SEARCH * VSIM @vector $vector",
        "search_equivalent": "*",
        "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
    }
    # TODO: Why the search query returns 'only_vector_' docs with higher scores
    # than the ones from 'both_' docs?
    run_test_scenario(env, index_name, scenario)

def test_knn_custom_k(env):
    """Test hybrid search using KNN with custom k scenario"""

    index_name, vector_field, text_field = generate_hybrid_test_data(env)
    scenario = {
        "test_name": "KNN with custom k",
        "hybrid_query": "FT.HYBRID idx SEARCH even VSIM @vector $vector KNN 2 K 5",
        "search_equivalent": "even",
        "vector_equivalent": "*=>[KNN 5 @vector $BLOB AS vector_distance]"
    }
    run_test_scenario(env, index_name, scenario)

def test_hybrid_range_basic(env):
    """Test hybrid search using range query scenario"""

    index_name, vector_field, text_field = generate_hybrid_test_data(env)
    scenario = {
        "test_name": "Range query",
        "hybrid_query": "FT.HYBRID idx SEARCH @text_field:(four|even) VSIM @vector $vector",
        "search_equivalent": f"@{text_field}:(four|even)",
        "vector_equivalent": "@vector:[VECTOR_RANGE 5 $BLOB]=>{$EPSILON:0.5; $YIELD_DISTANCE_AS: vector_distance}"
    }
    run_test_scenario(env, index_name, scenario)

