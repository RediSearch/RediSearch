from common import *
import numpy as np

def extract_docs_and_scores(result):
    """Extract document keys and scores from a search result."""
    if len(result) < 4 or result[0] != 'total_results':
        return []

    # Find the 'results' array in the result
    results_list = None
    for i in range(len(result)):
        if result[i] == 'results' and i + 1 < len(result):
            results_list = result[i + 1]
            break

    if results_list is None:
        return []

    docs = []

    # Results are structured as: [['__key', 'doc:112', '__score', '0.0294372294372'], ...]
    for doc_result in results_list:
        doc_key = None
        score = None

        # Each doc_result is a list like ['__key', 'doc:112', '__score', '0.0294372294372']
        for i in range(0, len(doc_result), 2):
            if i + 1 < len(doc_result):
                if doc_result[i] == '__key':
                    doc_key = doc_result[i + 1]
                elif doc_result[i] == '__score':
                    score = float(doc_result[i + 1])

        if doc_key and score is not None:
            docs.append((doc_key, score))

    return docs



def test_hybrid_sanity(env: Env):
    n_docs = 2**14

    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT',
               'tag', 'TAG',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3', 'DISTANCE_METRIC', 'L2').ok()

    # Set random seed for reproducible vectors
    np.random.seed(42)

    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            # Generate a simple vector based on the document number for predictable results
            vector = np.array([float(i % 10), float((i * 2) % 10), float((i * 3) % 10)], dtype=np.float32)

            # Create text content based on document number
            text_content = f"document {i} content"
            tag_value = "even" if i % 2 == 0 else "odd"

            con.execute_command('HSET', f'doc:{i}',
                              'n', i,
                              'text', text_content,
                              'tag', tag_value,
                              'vector', vector.tobytes())

    query_vector = np.array([5.0, 5.0, 5.0], dtype=np.float32).tobytes()
    query = ('FT.HYBRID', 'idx',
            'SEARCH', '@n:[69 1420]',
            'VSIM', '@vector', query_vector, 'KNN', '2', 'K', str(n_docs),
            'COMBINE', 'RRF', '2', 'CONSTANT', '60')
    expected = env.cmd(*query)
    expected_docs = extract_docs_and_scores(expected)
    print(f'Expected docs: {expected_docs}')

    shards = env.getOSSMasterNodesConnectionList()
    shard_results = []
    for i, shard in enumerate(shards):
        res = shard.execute_command(*query)
        expected_docs_from_shard = extract_docs_and_scores(res)
        print(f"Shard {i} docs: {expected_docs_from_shard}")
        env.assertEqual(expected_docs_from_shard, expected_docs, message=f"shard {i} returned unexpected results", depth=1)


def test_sortby_abstract():
    """Test FT.AGGREGATE and FT.SEARCH with SORTBY on @abstract field"""

    # Create environment with WORKERS 2 to ensure RPSafeLoader_New is used
    env = Env(moduleArgs='WORKERS 2')

    # Create index with abstract field
    env.expect('FT.CREATE', 'enwiki_abstract', 'SCHEMA',
               'title', 'TEXT',
               'abstract', 'TEXT', 'SORTABLE').ok()

    # Sample documents with abstracts for sorting
    documents = [
        {
            'key': 'doc:1',
            'title': 'Apple',
            'abstract': 'Apple is a fruit that grows on trees and is commonly eaten fresh.'
        },
        {
            'key': 'doc:2',
            'title': 'Zebra',
            'abstract': 'Zebra is a mammal known for its distinctive black and white stripes.'
        },
        {
            'key': 'doc:3',
            'title': 'Computer',
            'abstract': 'Computer is an electronic device that processes data and performs calculations.'
        },
        {
            'key': 'doc:4',
            'title': 'Database',
            'abstract': 'Database is a structured collection of data stored electronically.'
        },
        {
            'key': 'doc:5',
            'title': 'Elephant',
            'abstract': 'Elephant is the largest land mammal with a distinctive trunk.'
        }
    ]

    # Insert documents
    with env.getClusterConnectionIfNeeded() as con:
        for doc in documents:
            con.execute_command('HSET', doc['key'],
                              'title', doc['title'],
                              'abstract', doc['abstract'])

    # Test FT.AGGREGATE with SORTBY @abstract ASC
    print("\n=== FT.AGGREGATE Results ===")
    aggregate_result = env.cmd('FT.AGGREGATE', 'enwiki_abstract', '*',
                              'SORTBY', '2', '@abstract', 'ASC',
                              'LOAD', '*',
                              'LIMIT', '0', '100')
    print(f"FT.AGGREGATE result: {aggregate_result}")

    # Test FT.SEARCH with SORTBY abstract ASC
    print("\n=== FT.SEARCH Results ===")
    search_result = env.cmd('FT.SEARCH', 'enwiki_abstract', '*',
                           'SORTBY', 'abstract', 'ASC',
                           'LIMIT', '0', '100')
    print(f"FT.SEARCH result: {search_result}")

    # Verify both commands return results
    env.assertGreater(len(aggregate_result), 0)
    env.assertGreater(len(search_result), 0)

    # Verify search returns expected number of documents (should be 5 + metadata)
    env.assertEqual(search_result[0], 5)  # total_results should be 5
