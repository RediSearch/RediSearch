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


class TestIteratorsRevalidateTimeout:
    """
    A query whose deadline expires while the iterator tree is being revalidated must be reported as
    timed out, not as a query that ran to completion.

    Revalidation only happens when a query resumes after releasing the spec lock, so the deadline
    has to expire in that narrow window. `MOCK_REVALIDATE_TIMEOUT` stands in for it: the timeout
    sources a test can drive directly are the same flags the result processor checks immediately
    after revalidating, which hides the very case under test.
    """

    def __init__(self):
        skipTest(cluster=True)
        self.env = Env(protocol=3,
                       moduleArgs='FORK_GC_CLEAN_THRESHOLD 1 FORK_GC_RUN_INTERVAL 99999999999999999')

    def setUp(self):
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()
        with self.env.getClusterConnectionIfNeeded() as conn:
            for i in range(1, 6):
                conn.execute_command('HSET', f'doc:{i}', 'text', 'apple')
        self.prev_policy = self.env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]

    def tearDown(self):
        self.env.cmd(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'disable')
        self.env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, self.prev_policy)
        self.env.flush()

    def open_cursor(self, on_timeout):
        """Open a cursor under `on_timeout`, then stage a revalidation that will time out.

        The policy is frozen onto the cursor when it is created, so it has to be set first.

        The delete and the GC cycle are what make revalidation do real work in production - the
        iterators re-seek an index whose blocks moved under them, which is where the deadline would
        actually expire. The mock reports the timeout before any of that runs, so this setup does
        not affect the assertions; it is here to keep the sequence recognisable as the production
        one, not to drive the timeout.
        """
        self.env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout).ok()
        res, cursor = self.env.cmd('FT.AGGREGATE', 'idx', 'apple', 'LOAD', '1', '@__key',
                                   'WITHCURSOR', 'COUNT', '1')
        self.env.assertEqual(len(res['results']), 1)
        self.env.assertNotEqual(cursor, 0, message="the cursor must still have results to read")

        with self.env.getClusterConnectionIfNeeded() as conn:
            self.env.assertEqual(conn.execute_command('DEL', 'doc:2'), 1)
        forceInvokeGC(self.env)

        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'enable').ok()
        return cursor

    def test_status_reports_whether_the_switch_is_on(self):
        """`status` makes a server left with the switch on diagnosable rather than baffling."""
        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'status').equal(
            'Mock revalidation timeout: disabled')

        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'enable').ok()
        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'status').equal(
            'Mock revalidation timeout: enabled')

        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'disable').ok()
        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'status').equal(
            'Mock revalidation timeout: disabled')

        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'bogus').error().contains(
            'Use: enable, disable, or status')

    def test_fail_policy_errors_on_revalidation_timeout(self):
        """Under ON_TIMEOUT FAIL the read is an error, not a silently truncated result set."""
        cursor = self.open_cursor('fail')

        self.env.expect('FT.CURSOR', 'READ', 'idx', cursor).error().contains(
            'Timeout limit was reached')

    def test_return_policy_warns_and_drops_the_tree(self):
        """Under ON_TIMEOUT RETURN the read warns instead of ending cleanly, and the tree is gone.

        Both halves matter. The warning is the fix: the client is told the results are partial
        rather than complete. The empty tail is the cost that comes with it - a timed-out
        revalidation leaves the iterators indeterminate, so they are freed exactly as an aborted
        revalidation frees them, and the cursor has nothing left to serve even once the deadline is
        out of the way.
        """
        cursor = self.open_cursor('return')

        res, cursor = self.env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        self.env.assertNotEqual(cursor, 0,
                                message="RETURN keeps the cursor alive across a timeout; a cursor "
                                        "depleted here means the read ended as if the index were "
                                        "exhausted")
        self.env.assertEqual(res['results'], [],
                             message="the timeout lands before any result is read")
        VerifyTimeoutWarningResp3(self.env, res,
                                  message="a revalidation timeout must reach the client")

        self.env.expect(debug_cmd(), 'MOCK_REVALIDATE_TIMEOUT', 'disable').ok()
        while cursor != 0:
            res, cursor = self.env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            self.env.assertEqual(res['results'], [],
                                 message="the tree was freed, so no further results can arrive")
