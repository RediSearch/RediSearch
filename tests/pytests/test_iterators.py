from common import *

class TestIteratorsRevalidate:
    """
    Test class for the new iterators "Revalidate" mechanism.
    Tests different combinations of terms intersection, union, not (-) and optional (~) operations
    with cursor reads, document deletions, and GC operations.
    """

    def __init__(self):
        skipTest(cluster=True)
        self.env = Env(moduleArgs='FORK_GC_CLEAN_THRESHOLD 1 FORK_GC_RUN_INTERVAL 99999999999999999')

    def setUp(self):
        """Create index and add 10 documents for testing"""
        # Create index with text fields
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

        # Add 10 documents with various combinations of terms
        docs = [
            {'key': 'doc:1',  'text': 'apple banana'},
            {'key': 'doc:2',  'text': 'banana cherry'},
            {'key': 'doc:3',  'text': 'apple cherry'},
            {'key': 'doc:4',  'text': 'dog cat'},
            {'key': 'doc:5',  'text': 'cat dog'},
            {'key': 'doc:6',  'text': 'apple mixed'},
            {'key': 'doc:7',  'text': 'banana mixed'},
            {'key': 'doc:8',  'text': 'cherry mixed'},
            {'key': 'doc:9',  'text': 'apple banana cherry'},
            {'key': 'doc:10', 'text': 'dog cat bird'}
        ]

        with self.env.getClusterConnectionIfNeeded() as conn:
            for doc in docs:
                conn.execute_command('HSET', doc['key'], 'text', doc['text'])

    def tearDown(self):
        """Clean up the index and documents"""
        self.env.flush()

    def initiate_cursor(self, query):
        """Helper to initiate a cursor for a given query"""
        res, cursor = self.env.cmd('FT.AGGREGATE', 'idx', query, 'LOAD', '1', '@__key', 'WITHCURSOR', 'COUNT', '1')
        return to_dict(res[1])['__key'], cursor

    def read_all_cursor_results(self, cursor):
        """Helper to read all remaining results from cursor until cursor = 0"""
        all_results = []
        while cursor != 0:
            res, cursor = self.env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            # Ignore first value (always 1), get actual results from rest of list
            all_results.extend([doc for _, doc in res[1:]])
        return all_results

    def delete(self, *doc_keys):
        """Helper to delete documents by keys"""
        with self.env.getClusterConnectionIfNeeded() as conn:
            for key in doc_keys:
                res = conn.execute_command('DEL', key)
                self.env.assertEqual(res, 1, message=f"Failed to delete document {key}", depth=1)

    def test_intersection_delete_last_returned(self):
        """Test intersection query - delete the last document returned from first read"""
        query = 'apple banana'  # Should match doc:1, doc:9

        # Start aggregate with cursor, read 1 result and load the document key
        doc_key, cursor = self.initiate_cursor(query)

        # Assert the first document returned (deterministic order)
        self.env.assertEqual(doc_key, 'doc:1')
        self.delete('doc:1')

        # Run GC
        forceInvokeGC(self.env)

        # Read all remaining results from cursor
        remaining_docs = self.read_all_cursor_results(cursor)

        # Assert exactly the remaining document
        self.env.assertEqual(remaining_docs, ['doc:9'])

    def test_intersection_delete_next_to_return(self):
        """Test intersection query - delete the next document that should be returned"""
        query = 'apple cherry'  # Should match doc:3, doc:9

        # Start aggregate with cursor, read 1 result
        first_doc, cursor = self.initiate_cursor(query)
        self.env.assertEqual(first_doc, 'doc:3')

        # Delete the other document that matches this query (doc:9)
        self.delete('doc:9')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results - should skip the deleted document
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have 0 remaining docs since we deleted the only other match
        self.env.assertEqual(remaining_docs, [])

    def test_union_delete_last_returned(self):
        """Test union query - delete the last document returned from first read"""
        query = 'apple|dog'  # Should match doc:1, doc:3, doc:4, doc:5, doc:6, doc:9, doc:10

        # Start aggregate with cursor, read 1 result
        doc_key, cursor = self.initiate_cursor(query)

        self.env.assertEqual(doc_key, 'doc:1')
        self.delete('doc:1')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have the remaining 6 docs (all except the deleted doc:1)
        self.env.assertEqual(remaining_docs, ['doc:3', 'doc:4', 'doc:5', 'doc:6', 'doc:9', 'doc:10'])

    def test_union_delete_a_few_next_to_return(self):
        """Test union query - delete a few documents next to the first returned"""
        query = 'apple|dog'  # Should match doc:1, doc:3, doc:4, doc:5, doc:6, doc:9, doc:10

        # Start aggregate with cursor, read 1 result
        doc_key, cursor = self.initiate_cursor(query)
        self.env.assertEqual(doc_key, 'doc:1')

        # Delete doc:3 (apple cherry) and doc:4 (dog cat) which should be in the remaining results
        self.delete('doc:1', 'doc:3', 'doc:4')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have the remaining 4 docs (all except the deleted doc:3 and doc:4)
        self.env.assertEqual(remaining_docs, ['doc:5', 'doc:6', 'doc:9', 'doc:10'])

    def test_union_delete_next_to_return(self):
        """Test union query - delete the next document that should be returned"""
        query = 'banana|cat'  # Should match doc:1, doc:2, doc:4, doc:5, doc:7, doc:9, doc:10

        # Start aggregate with cursor, read 1 result
        first_doc, cursor = self.initiate_cursor(query)
        self.env.assertEqual(first_doc, 'doc:1')

        # Delete doc:2 (banana cherry) which should be in the remaining results
        self.delete('doc:2')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have the remaining docs except doc:1 (first) and doc:2 (deleted)
        self.env.assertEqual(remaining_docs, ['doc:4', 'doc:5', 'doc:7', 'doc:9', 'doc:10'])

    def test_not_query_delete_last_returned(self):
        """Test NOT query - delete the last document returned from first read"""
        query = 'apple -cherry'  # Should match doc:1, doc:6 (apple but not cherry)

        # Start aggregate with cursor, read 1 result
        doc_key, cursor = self.initiate_cursor(query)
        self.env.assertEqual(doc_key, 'doc:1')
        self.delete('doc:1', 'doc:3', 'doc:9')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results from cursor
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have exactly the remaining document
        self.env.assertEqual(remaining_docs, ['doc:6'])

    def test_not_query_delete_next_to_return(self):
        """Test NOT query - delete the next document that should be returned"""
        query = 'apple -cherry'  # Should match doc:1, doc:6 (apple but not cherry)

        # Start aggregate with cursor, read 1 result
        first_doc, cursor = self.initiate_cursor(query)
        self.env.assertEqual(first_doc, 'doc:1')

        # Delete the other document that matches this query (doc:6)
        self.delete('doc:6')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results from cursor
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have 0 remaining docs since we deleted the only other match
        self.env.assertEqual(remaining_docs, [])

    def test_optional_query_delete_last_returned(self):
        """Test optional query - delete the last document returned from first read"""
        query = 'dog ~bird'  # Should match doc:4, doc:5, doc:10 (dog required, bird optional)

        # Start aggregate with cursor, read 1 result
        doc_key, cursor = self.initiate_cursor(query)
        self.env.assertEqual(doc_key, 'doc:4')
        self.delete('doc:4')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results from cursor
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have exactly 2 remaining documents
        self.env.assertEqual(remaining_docs, ['doc:5', 'doc:10'])

    def test_optional_query_delete_next_to_return(self):
        """Test optional query - delete the next document that should be returned"""
        query = 'cat ~cherry'  # Should match doc:4, doc:5, doc:10 (cat required, cherry optional)

        # Start aggregate with cursor, read 1 result
        first_doc, cursor = self.initiate_cursor(query)
        self.env.assertEqual(first_doc, 'doc:4')

        # Delete doc:5 which should be in the remaining results
        self.delete('doc:5')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results from cursor
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have the remaining docs except doc:4 (first) and doc:5 (deleted)
        self.env.assertEqual(remaining_docs, ['doc:10'])

    def test_complex_query_multiple_deletions(self):
        """Test complex query with multiple deletions between cursor reads"""
        query = '(apple|banana) -dog'  # Should match doc:1, doc:2, doc:3, doc:6, doc:7, doc:9

        # Start aggregate with cursor, read 1 result
        doc_key, cursor = self.initiate_cursor(query)
        self.env.assertEqual(doc_key, 'doc:1')

        # Delete additional specific documents that match the query
        self.delete('doc:1', 'doc:2', 'doc:9')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results from cursor
        remaining_docs = self.read_all_cursor_results(cursor)

        # Should have the remaining docs except deleted ones
        self.env.assertEqual(remaining_docs, ['doc:3', 'doc:6', 'doc:7'])

    def test_edge_case_delete_all_remaining(self):
        """Test edge case where all remaining documents are deleted"""
        query = 'mixed'  # Should match doc:6, doc:7, doc:8

        # Start aggregate with cursor, read 1 result
        first_doc, cursor = self.initiate_cursor(query)
        self.env.assertEqual(first_doc, 'doc:6')

        # Delete all documents that match the query (including the one already returned)
        self.delete('doc:6', 'doc:7', 'doc:8')

        # Run GC
        forceInvokeGC(self.env)

        # Read remaining results from cursor - should be empty
        remaining_docs = self.read_all_cursor_results(cursor)

        # No remaining results since we deleted all matching documents
        self.env.assertEqual(remaining_docs, [])
