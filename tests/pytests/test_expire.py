import copy
import time
from itertools import chain

from common import *

@skip(cluster=True)
def testExpireIndex(env):
    # temporary indexes
    env.cmd('ft.create', 'idx', 'TEMPORARY', '4', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    ttl = env.cmd(debug_cmd(), 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    with TimeLimit(10):
        while env.cmd(debug_cmd(), 'TTL', 'idx') > 2:
            time.sleep(0.1)
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'this is a simple test')
    ttl = env.cmd(debug_cmd(), 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    with TimeLimit(10):
        while env.cmd(debug_cmd(), 'TTL', 'idx') > 2:
            time.sleep(0.1)
    env.cmd('ft.search', 'idx', 'simple')
    ttl = env.cmd(debug_cmd(), 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    with TimeLimit(10):
        while env.cmd(debug_cmd(), 'TTL', 'idx') > 2:
            time.sleep(0.1)
    env.cmd('ft.aggregate', 'idx', 'simple', 'LOAD', '1', '@test')
    ttl = env.cmd(debug_cmd(), 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    with TimeLimit(10):
        try:
            while True:
                env.cmd(debug_cmd(), 'TTL', 'idx')
                time.sleep(0.1)
        except Exception as e:
            # `assertContains` expects (expected_substring, actual_string)
            env.assertContains('SEARCH_INDEX_NOT_FOUND Index not found', str(e))

@skip(cluster=True, redis_less_than="7.4")
def test_MOD_14800_persist_clears_expiration_metadata(env: Env):
    # Regression for MOD-14800:
    # Verify that persisting a hash key or an indexed hash field clears the
    # corresponding expiration metadata from the index, so the document remains
    # searchable after the original expiration deadline would have passed.
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('HSET', 'doc:1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello').equal([1, 'doc:1', ['t', 'hello']])

    env.expect('PEXPIRE', 'doc:1', '100').equal(1)
    env.expect('PERSIST', 'doc:1').equal(1)

    time.sleep(0.2)

    env.expect('EXISTS', 'doc:1').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello').equal([1, 'doc:1', ['t', 'hello']])

    env.expect('HPEXPIRE', 'doc:1', '100', 'FIELDS', '1', 't').equal([1])
    env.expect('HPERSIST', 'doc:1', 'FIELDS', '1', 't').equal([1])

    time.sleep(0.2)

    env.expect('HGET', 'doc:1', 't').equal('hello')
    env.expect('FT.SEARCH', 'idx', 'hello').equal([1, 'doc:1', ['t', 'hello']])

@skip(cluster=True, redis_less_than='8.0')
def test_doc_expiration_preserves_field_expiration(env: Env):
    # Regression: PEXPIRE/PERSIST on an indexed hash that also has HEXPIRE-
    # managed field TTLs must leave the per-field TTL state intact.
    #
    # The keyspace-notification fast path Indexes_UpdateMatchingDocExpiration
    # only needs to refresh dmd->expirationTimeNs. A previous implementation
    # delegated through DocTable_UpdateExpiration -> DocTable_UpdateFieldExpiration
    # with a NULL field array, which silently cleared the spec's TTL-table
    # entry for the doc and "resurrected" fields that should remain expired.
    conn = getConnectionByEnv(env)
    # Pin the field's apparent expiration to the HFE table only; without this
    # Redis would also remove the field server-side, masking the bug.
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'SCHEMA', 'x', 'TEXT', 'y', 'TEXT').ok()
    # Both flags must be on: 'documents' so the EXPIRE/PERSIST path enters the
    # spec at all, 'fields' so the HPEXPIRE below populates the TTL table.
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'documents', 'fields')

    conn.execute_command('HSET', 'doc:1', 'x', 'hello', 'y', 'world')
    conn.execute_command('HSET', 'doc:2', 'x', 'hello', 'y', 'world')

    # Mark doc:1.x as expired via the HFE table. Active expiration is off, so
    # the field value stays in the hash; only the spec's TTL table filters it.
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'x')
    time.sleep(0.05)

    # Sanity baseline: doc:1 is filtered out of @x queries, @y still hits both.
    env.expect('FT.SEARCH', 'idx', '@x:hello', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@y:world', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])

    # Drive Indexes_UpdateMatchingDocExpiration with a non-zero TTL. With the
    # bug, this removes doc:1's HFE entry and @x:hello starts matching doc:1.
    conn.execute_command('PEXPIRE', 'doc:1', '60000')
    env.expect('FT.SEARCH', 'idx', '@x:hello', 'NOCONTENT').equal([1, 'doc:2'])

    # Same fast path, now with ttl == 0. Must also leave HFE state untouched.
    conn.execute_command('PERSIST', 'doc:1')
    env.expect('FT.SEARCH', 'idx', '@x:hello', 'NOCONTENT').equal([1, 'doc:2'])

    # And the unrelated field is unaffected throughout.
    env.expect('FT.SEARCH', 'idx', '@y:world', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])

res_score_and_explanation = ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                    ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]]
both_docs_no_sortby = "both_docs_no_sortby"
both_docs_sortby = "both_docs_sortby"
doc2_is_lazy_expired = "doc2_is_lazy_expired"
doc2_is_lazy_expired_sortby = "doc2_is_lazy_expired_sortby"
doc2_is_lazy_expired_sortby_sorted = "doc2_is_lazy_expired_sortby_sorted"
only_doc1_sortby = "only_doc1_sortby"
only_doc1_no_sortby = "only_doc1_no_sortby"

def add_explain_to_results(results):
    results = results.copy()
    for offset, i in enumerate(range(2, len(results), 2)):
        results.insert(i+offset, res_score_and_explanation)
    return results

def buildExpireDocsResults(isJson):
    results = {}
    doc1 = ['doc1', ['t', 'bar'] if not isJson else ['$', '{"t":"bar"}']]
    doc2 = ['doc2', ['t', 'arr'] if not isJson else ['$', '{"t":"arr"}']]
    doc1_with_sort_key = ['doc1', ['t', 'bar']] if not isJson else ['doc1', ['t', 'bar', '$', '{"t":"bar"}']]
    doc2_with_sort_key = ['doc2', ['t', 'arr']] if not isJson else ['doc2', ['t', 'arr', '$', '{"t":"arr"}']]
    # When calling FT.SEARCH with SORTBY on json index, the sortby field is loaded into the result together with the json document
    results[both_docs_no_sortby] = [2, *doc1, *doc2]
    results[both_docs_sortby] = [2, *doc2_with_sort_key, *doc1_with_sort_key]

    results[doc2_is_lazy_expired] = [2, *doc1, *doc2]
    results[doc2_is_lazy_expired_sortby] = [2, *doc2_with_sort_key, *doc1_with_sort_key]
    results[doc2_is_lazy_expired_sortby_sorted] = [2, *doc2_with_sort_key, *doc1_with_sort_key]

    # on Json we also return the sortby field value.
    results[only_doc1_sortby] = [1, *doc1_with_sort_key]
    results[only_doc1_no_sortby] = [1, *doc1]

    return results

# Refer to expireDocs for details on why this test is skipped for Redis versions below 7.2
@skip(cluster=True, redis_less_than="7.2")
def testExpireDocsHash(env):
    # Without SORTABLE - since the fields are not SORTABLE, we need to load the results from Redis Keyspace
    expireDocs(env, False, False)

# Refer to expireDocs for details on why this test is skipped for Redis versions below 7.2
@skip(cluster=True, redis_less_than="7.2", no_json=True)
def testExpireDocsJson(env):
    # Without SORTABLE - since the fields are not SORTABLE, we need to load the results from Redis Keyspace
    expireDocs(env, False, True)

# Refer to expireDocs for details on why this test is skipped for Redis versions below 7.2
@skip(cluster=True, redis_less_than="7.2")
def testExpireDocsSortableHash(env):
    expireDocs(env, True, False)
            # With SORTABLE -
            # The documents data exists in the index.
            # Since we are not trying to load the document in the sorter, it is not discarded from the results,
            # but it is marked as deleted and we reply with None.

# Refer to expireDocs for details on why this test is skipped for Redis versions below 7.2
@skip(cluster=True, redis_less_than="7.2", no_json=True)
def testExpireDocsSortableJSON(env):
    expireDocs(env, True, True)
            # With SORTABLE -
            # The documents data exists in the index.
            # Since we are not trying to load the document in the sorter, it is not discarded from the results,
            # but it is marked as deleted and we reply with None.


#TODO: DvirDu: I think this test should be broken down to smaller tests, due to the complexity of the test and the number of cases it covers it is hard to debug
# Skip this test for Redis versions below 7.2 due to a bug in PEXPIRE.
# In older versions, a bug involving multiple time samplings during PEXPIRE execution
# can cause keys to prematurely expire, triggering a "del" notification and eliminating them from the index,
# thus missing from search results.
# This impacts the test as the key should be included in the search results but return NULL upon access
# (i.e lazy expiration).
# The bug was resolved in Redis 7.2, ensuring the test's stability.
def expireDocs(env, isSortable, isJson):
    '''
    This test creates an index and two documents
    We disable active expiration
    One of the documents is lazily expired. We should succeed to open the key in Redis keyspace since we use REDISMODULE_OPEN_KEY_ACCESS_EXPIRED flag.
    The test checks the expected output.
    The value of the lazily expired key should be valid, regardless of whether the field is sortable or not.
    If the field is SORTABLE, the order of the results is determined by its value. Else, the expired doc should be last.
    The document will be loaded in the loader, which should use the expiration flags to ensure doc2 will be loaded successfully

    When isSortable is True the index is created with `SORTABLE` arg

    expected results table (doc2 value > doc1 value)
    | Case          | SORTBY            | No SORTBY |
    |---------------|-------------------|-----------|
    | SORTABLE      | doc2, arr, ['$']  | doc2, arr |
    |               | doc1, bar, ['$']  | doc1, bar |
    |---------------|-------------------|-----------|
    | Not SORTABLE  | doc1, bar, ['$']  | doc1, bar |
    |               | doc2, arr, ['$']  | doc2, arr |
    '''
    conn = env.getConnection()
    expected_results = buildExpireDocsResults(isJson)

    # i = 2 -> without sortby, i = 1 -> with sortby
    for sortby in [False, True]:
        # Use "lazy" expire (expire only when key is accessed)
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
        sortby_cmd = [] if not sortby else ['SORTBY', 't']
        sortable_arg = [] if not isSortable else ['SORTABLE']
        if isJson:
            conn.execute_command(
                'FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TEXT', *sortable_arg)
            conn.execute_command('JSON.SET', 'doc1', '$', '{"t":"bar"}')
            conn.execute_command('JSON.SET', 'doc2', '$', '{"t":"arr"}')
        else:
            conn.execute_command(
                'FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', *sortable_arg)
            conn.execute_command('HSET', 'doc1', 't', 'bar')
            conn.execute_command('HSET', 'doc2', 't', 'arr')
        conn.execute_command(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'not-documents')
        # Both docs exist.
        res = conn.execute_command('FT.SEARCH', 'idx', '*')
        env.assertEqual(res, expected_results[both_docs_no_sortby], message='both docs exist')

        # MOD-6781 Prior to the fix, if a field was SORTABLE, the expired document content depended on the result order.
        # This was due to the lazy construction of the rlookup when there was no explicit return,
        # causing the values of fields included in former resulted to be looked up in the sorting vector.
        # Expiring 'doc2' instead of 'doc1' ensures the 't' column exists in the rlookup.
        # Without the fix, when 't' is SORTABLE the result of doc2 contains the expiring document's values, demonstrating
        # the inconsistency the fix resolved.
        conn.execute_command('PEXPIRE', 'doc2', 1)
        # ensure expiration before search
        time.sleep(0.01)

        msg = '{}{} sortby'.format(
            'SORTABLE ' if isSortable else '', 'without' if not sortby else 'with')
        # First iteration
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        if isSortable:
            expected_res = expected_results[doc2_is_lazy_expired_sortby_sorted if sortby else doc2_is_lazy_expired]
        else:
            expected_res = expected_results[doc2_is_lazy_expired_sortby if sortby else doc2_is_lazy_expired]
        env.assertEqual(res, expected_res, message=msg)

        # Cancel lazy expire to allow the deletion of the key
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        # Second iteration - only 1 doc is left
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, expected_results[only_doc1_sortby if sortby else only_doc1_no_sortby], message=msg)


        # test with WITHSCORES and EXPLAINSCORE - make sure all memory is released
        # we need to re-write the documents since in case of score tie they will be returned by internal id order. This will break once we will ignore stale updates to documents
        if isJson:
            conn.execute_command('JSON.SET', 'doc1', '$', '{"t":"bar"}')
            conn.execute_command('JSON.SET', 'doc2', '$', '{"t":"arr"}')
        else:
            conn.execute_command('HSET', 'doc1', 't', 'bar')
            conn.execute_command('HSET', 'doc2', 't', 'arr')

        # both docs exist
        expected_res = add_explain_to_results(expected_results[both_docs_no_sortby])

        res = conn.execute_command('FT.SEARCH', 'idx', '*', 'SCORER', 'TFIDF', 'WITHSCORES', 'EXPLAINSCORE')
        env.assertEqual(res, expected_res)

        # Activate lazy expire again to ensure the key is not expired before we run the query
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

        # expire doc2
        conn.execute_command('PEXPIRE', 'doc2', 1)
        # ensure expiration before search
        time.sleep(0.01)

        res = conn.execute_command('FT.SEARCH', 'idx', '*', 'SCORER', 'TFIDF', 'WITHSCORES', 'EXPLAINSCORE', *sortby_cmd)

        if isSortable:
            env.assertEqual(res, add_explain_to_results(expected_results[doc2_is_lazy_expired_sortby_sorted if sortby else doc2_is_lazy_expired]), message=(msg + ' WITHSCORES, EXPLAINSCORE'))
        else:
            env.assertEqual(res, add_explain_to_results(expected_results[doc2_is_lazy_expired_sortby if sortby else doc2_is_lazy_expired]), message=(msg + ' WITHSCORES, EXPLAINSCORE'))

        # Cancel lazy expire to allow the deletion of the key
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        # only 1 doc is left
        res = add_explain_to_results(expected_results[only_doc1_no_sortby])
        env.expect('FT.SEARCH', 'idx', '*', 'SCORER', 'TFIDF', 'WITHSCORES', 'EXPLAINSCORE').equal(res)

        conn.execute_command('FLUSHALL')

@skip(cluster=True, redis_less_than="7.2")
def test_expire_aggregate(env):
    conn = env.getConnection()
    # Use "lazy" expire (expire only when key is accessed)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'not-documents')

    conn.execute_command('HSET', 'doc1', 't', 'bar')
    conn.execute_command('HSET', 'doc2', 't', 'arr')

    # expire doc1
    conn.execute_command('PEXPIRE', 'doc1', 1)
    # ensure expiration before search
    time.sleep(0.01)
    # In some pipelines we can reuse the search result by clearing it before populating it with a new result.
    # If not cleared, it might affect subsequent results.
    # This test ensures that the flag indicating expiration is cleared and the search result struct is ready to be reused.
    res = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t')
    # The result count is not accurate in aggregation, because WITHOUTCOUNT is the default
    # Accept both orders, since docID did not advance
    env.assertEqual([res[0], sorted(res[1:])], [1, sorted([['t', 'arr'], ['t', 'bar']])])
    # Test using WITHCOUNT
    res = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@t')
    env.assertEqual([res[0], sorted(res[1:])], [2, sorted([['t', 'arr'], ['t', 'bar']])])



def expire_ft_hybrid_test(protocol):
    env = Env(protocol=protocol)
        # Use "lazy" expire (expire only when key is accessed) on all shards
    env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Create index with text, vector, and numeric fields
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

    # Create test vectors (2-dimensional float32)
    import numpy as np
    query_vector = np.array([0.5, 0.5]).astype(np.float32).tobytes()

    # Use cluster-aware connection for data insertion
    with env.getClusterConnectionIfNeeded() as conn:
        # Create 1000 documents
        for i in range(1000):
            # Create a unique vector for each document
            vector = np.array([float(i % 100) / 100.0, float((i + 1) % 100) / 100.0]).astype(np.float32).tobytes()
            doc_key = f'doc{i}'
            text_value = f'text{i}'
            numeric_value = str(i)

            conn.execute_command('HSET', doc_key, 't', text_value, 'n', numeric_value, 'v', vector)

            # Expire the first 990 documents (doc0 to doc989)
            if i < 990:
                conn.execute_command('PEXPIRE', doc_key, 1)

    # Ensure expiration before query
    time.sleep(0.01)

    # Test FT.HYBRID requesting 1000 results but expecting only 10 (non-expired documents)
    hybrid_query = ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB' , 'LIMIT', '0', '1000', 'COMBINE', 'RRF', '2', 'CONSTANT', '60', 'LOAD', '4', '@__key', '@__score', '@t', '@n', 'PARAMS', '2', 'BLOB', query_vector]

    # Execute query using cluster-aware command to get expected results
    actual_res = env.cmd(*hybrid_query)
    from common import get_results_from_hybrid_response
    actual_results_dict, actual_total_results = get_results_from_hybrid_response(actual_res)

    # Validate that only 10 documents are returned (doc990 to doc999)
    env.assertEqual(actual_total_results, 10)

    # Verify that only non-expired documents are present
    expected_doc_keys = {f'doc{i}' for i in range(990, 1000)}
    actual_doc_keys = set(actual_results_dict.keys())
    env.assertEqual(actual_doc_keys, expected_doc_keys)

    # Verify that each returned document has the correct attributes
    for doc_key in actual_results_dict:
        doc_num = int(doc_key[3:])  # Extract number from 'docXXX'
        env.assertTrue('__key' in actual_results_dict[doc_key])
        env.assertTrue('__score' in actual_results_dict[doc_key])
        env.assertTrue('t' in actual_results_dict[doc_key])
        env.assertTrue('n' in actual_results_dict[doc_key])
        env.assertEqual(actual_results_dict[doc_key]['__key'], doc_key)
        env.assertEqual(actual_results_dict[doc_key]['t'], f'text{doc_num}')
        env.assertEqual(actual_results_dict[doc_key]['n'], str(doc_num))
        env.assertTrue(float(actual_results_dict[doc_key]['__score']) >= 0)

def test_expire_ft_hybrid_resp2():
    expire_ft_hybrid_test(protocol=2)

def test_expire_ft_hybrid_resp3():
    expire_ft_hybrid_test(protocol=3)

def createTextualSchema(field_to_additional_schema_keywords):
    schema = []
    for field, additional_schema_words in field_to_additional_schema_keywords.items():
        schema.append(field)
        schema.append('TEXT')
        for keyword in additional_schema_words:
            schema.append(keyword)
    return schema


def sort_document_names(document_list):
    if len(document_list) == 0:
        return {}

    num_docs = document_list[0]
    names = document_list[1:]
    names.sort()
    return [num_docs, *names]


def transform_document_list_to_dict(document_list):
    if len(document_list) == 0:
        return {}

    result = {}
    num_docs = document_list[0]
    for i in range(1, num_docs * 2, 2):
        values_dict = result[document_list[i]] = {}
        field_and_value_pairs = document_list[i+1]
        for j in range(0, len(field_and_value_pairs), 2):
            values_dict[field_and_value_pairs[j]] = field_and_value_pairs[j+1]
    return result

# The test creates an index, then documents based on document_name_to_expire
# Each document will hold the fields based on fields argument
# If the field is marked to expire and the document is marked to expire, the field will be expired
# The field_to_schema_list allows specifying additional schema keywords for the field
# e.g SORTABLE, this can affect the expected results
# The value for each field will be 't' if the field is marked to expire and the document is marked to expire,
# otherwise 'f'
def commonFieldExpiration(env, schema, fields, expiration_interval_to_fields, document_name_to_expire):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', *schema)

    def create_documents():
        field_and_value_dict = {field_name: field_name for field_name in fields}
        field_and_value_list = list(chain.from_iterable(field_and_value_dict.items()))
        documents = {}
        for document_name in document_name_to_expire.keys():
            documents[document_name] = copy.deepcopy(field_and_value_dict)
            conn.execute_command('HSET', document_name, *field_and_value_list)
        return documents

    def setup_field_expiration(current_documents):
        for doc_name, should_expire in document_name_to_expire.items():
            if not should_expire:
                continue
            for expiration_interval, field_list in expiration_interval_to_fields.items():
                conn.execute_command('HPEXPIRE', doc_name, str(expiration_interval), 'FIELDS', str(len(field_list)),
                                     *field_list)
                for field in field_list:
                    del current_documents[doc_name][field]
                if len(current_documents[doc_name]) == 0:
                    del current_documents[doc_name]
        return current_documents

    def build_inverted_index_dict_for_documents(current_documents):
        inverted_index = {}
        for document_name, fields in current_documents.items():
            for field_value in fields.values():
                if field_value not in inverted_index:
                    inverted_index[field_value] = []
                inverted_index[field_value].append(document_name)
        return inverted_index

    expected_results = create_documents()
    env.expect('FT.SEARCH', 'idx', '*').apply(transform_document_list_to_dict).equal(expected_results)
    expected_results = setup_field_expiration(expected_results)
    expected_inverted_index = build_inverted_index_dict_for_documents(expected_results)
    # now allow active expiration to delete the expired fields
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
    time.sleep(0.5)
    env.expect('FT.SEARCH', 'idx', '*').apply(transform_document_list_to_dict).equal(expected_results)
    for field_name_and_value, expected_docs in expected_inverted_index.items():
        (env.expect('FT.SEARCH', 'idx', f'@{field_name_and_value}:{field_name_and_value}', 'NOCONTENT')
         .apply(sort_document_names).equal([len(expected_docs), *expected_docs]))
    conn.execute_command('FLUSHALL')


# Aims to expire a single field in a document and make sure that document expires as well
@skip(redis_less_than='7.3')
def testSingleExpireField(env):
    field_to_additional_schema_keywords = {'x': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})

# Aims to test that the expiration of a single field will not affect the search results
@skip(redis_less_than='7.3')
def testTwoFieldsOneOfThemWillExpire(env):
    field_to_additional_schema_keywords = {'x': [], 'y': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['y']},
                          document_name_to_expire={'doc1': True, 'doc2': False})

# Aims to test that the expiration of a single sortable field will cause the document to expire
@skip(redis_less_than='7.3')
def testSingleSortableFieldWithExpiration(env):
    field_to_additional_schema_keywords = {'x': ['SORTABLE']}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})

# Aims to test that the expiration of a single sortable field will not affect the search results
@skip(redis_less_than='7.3')
def testSortableFieldWithExpirationAndRegularField(env):
    field_to_additional_schema_keywords = {'x': ['SORTABLE'], 'y': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})


# Aims to test that the expiration of a single non sortable field will not affect the search results
@skip(redis_less_than='7.3')
def testFieldWithExpirationAndSortableField(env):
    field_to_additional_schema_keywords = {'x': [], 'y': ['SORTABLE']}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})


# Aims to test that two fields with different expiration times will eventually cause the key itself to expire
@skip(redis_less_than='7.3')
def testExpireMultipleFields(env):
    field_to_additional_schema_keywords = {'x': [], 'y': [], 'z': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x'], 3: ['y', 'z']},
                          document_name_to_expire={'doc1': True, 'doc2': False})


# Aims to test that the expectation that for 2 fields with the same expiration time we will get a single notification
@skip(redis_less_than='7.3')
def testExpireMultipleFieldsWhereOneIsSortable(env):
    field_to_additional_schema_keywords = {'x': ['SORTABLE'], 'y': [], 'z': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x'], 3: ['y', 'z']},
                          document_name_to_expire={'doc1': True, 'doc2': False})

@skip(cluster=True, redis_less_than='8.0')
def testLazyTextFieldExpiration(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    # We added not_text_field to make sure that the expandFieldMask function hits the continue clause
    # Meaning that at least one field ftid during the expiration check will be RS_INVALID_FIELD_ID
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'not_text_field', 'NUMERIC', 'x', 'TEXT', 'INDEXMISSING', 'y', 'TEXT')
    # Enable monitoring on hash field expiration. TODO: have this on default once we fix the call to HPEXPIRE
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')  # use shard connection for _FT.DEBUG
    conn.execute_command('HSET', 'doc:1', 'x', 'hello', 'y', '57')
    conn.execute_command('HSET', 'doc:2', 'x', 'hello', 'y', '57')
    conn.execute_command('HSET', 'doc:3', 'x', 'hello', 'y', '57')
    conn.execute_command('HSET', 'doc:4', 'x', 'hello', 'y', 'hello')
    conn.execute_command('HPEXPIRE', 'doc:4', '1', 'FIELDS', '1', 'x')
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015)
    # there shouldn't be an active expiration for field x in doc:1
    # but due to the ttl table we should not return doc:4 when searching for x
    env.expect('FT.SEARCH', 'idx', '@x:hello', 'NOCONTENT').equal([3, 'doc:1', 'doc:2', 'doc:3'])
    # also we expect that the ismissing inverted index to contain document 4 since it had an active expiration
    env.expect('FT.SEARCH', 'idx', 'ismissing(@x)', 'NOCONTENT', 'DIALECT', '3').equal([1, 'doc:4'])
    # Test the field mask element, hello term should have a bit mask of 2 fields
    # For doc:1 the mask should have two bits for its two fields
    # since the field y is still valid we should still get doc:1 in the results
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').apply(sort_document_names).equal([4, 'doc:1', 'doc:2', 'doc:3', 'doc:4'])


@skip(redis_less_than='8.0')
def testLazyGeoshapeFieldExpiration(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT', 'INDEXMISSING').ok()
    # Enable monitoring on hash field expiration. TODO: have this on default once we fix the call to HPEXPIRE
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')  # use shard connection for _FT.DEBUG
    first = 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))'
    second = 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))'
    conn.execute_command('HSET', 'doc:1', 'txt', 'hello', 'geom', first)
    conn.execute_command('HSET', 'doc:2', 'txt', 'world', 'geom', second)
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'geom')
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015)
    query = 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))'
    env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', query, 'NOCONTENT', 'DIALECT', 3).equal([1, 'doc:2'])
    # also we expect that the ismissing inverted index to contain document 1 since it had an active expiration
    env.expect('FT.SEARCH', 'idx', 'ismissing(@geom)', 'NOCONTENT', 'DIALECT', '3').equal([1, 'doc:1'])


@skip(redis_less_than='8.0')
def testLazyVectorFieldExpiration(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE idx SCHEMA v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 INDEXMISSING t TEXT n NUMERIC').ok()
    # Enable monitoring on hash field expiration. TODO: have this on default once we fix the call to HPEXPIRE
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')  # use shard connection for _FT.DEBUG
    conn.execute_command('hset', 'doc:1', 'v', 'bababaca', 't', "hello", 'n', 1)
    conn.execute_command('hset', 'doc:2', 'v', 'babababa', 't', "hello", 'n', 2)
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'v')
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015)
    env.expect('FT.SEARCH', 'idx', '@n:[1, 4]=>[KNN 3 @v $vec]', 'PARAMS', 2, 'vec', 'aaaaaaaa', 'NOCONTENT', 'DIALECT', 3).equal([1, 'doc:2'])
    # also we expect that the ismissing inverted index to contain document 1 since it had an active expiration
    env.expect('FT.SEARCH', 'idx', 'ismissing(@v)', 'NOCONTENT', 'DIALECT', '3').equal([1, 'doc:1'])

def _setup_non_matching_indexes(env):
    # Shared setup for the EXPIRE/PEXPIRE/PERSIST scenarios. Three distinct
    # ways an index can fail to match the key are exercised:
    #   - idx_other_prefix: PREFIX excludes the key entirely, so the spec is
    #                       not returned by FindMatchingSchemaRules.
    #   - idx_filter_skip:  PREFIX matches but the FILTER excludes the doc, so
    #                       FindMatchingSchemaRules tags the op as SpecOp_Del.
    #   - idx_no_monitor:   PREFIX matches but monitorDocumentExpiration is
    #                       disabled, so the spec is skipped on the fast path.
    # Only idx_match should reflect TTL changes via DocTable_UpdateExpiration.
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    env.expect('FT.CREATE', 'idx_match', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'SCHEMA', 'x', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx_other_prefix', 'ON', 'HASH',
               'PREFIX', '1', 'other:',
               'SCHEMA', 'x', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx_filter_skip', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'FILTER', '@n == 100',
               'SCHEMA', 'x', 'TEXT', 'n', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx_no_monitor', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'SCHEMA', 'x', 'TEXT').ok()

    # Disable doc-level expiration tracking on idx_no_monitor; the other specs
    # keep the default monitorDocumentExpiration=true.
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx_no_monitor', 'not-documents')

    # n=1 fails the FILTER on idx_filter_skip but the PREFIX still selects
    # these hashes for the keyspace-notification path.
    conn.execute_command('HSET', 'doc:1', 'x', 'hello', 'n', '1')
    conn.execute_command('HSET', 'doc:2', 'x', 'hello', 'n', '1')
    # other:1 belongs only to idx_other_prefix.
    conn.execute_command('HSET', 'other:1', 'x', 'hello')

    # Sanity check: every index returns the docs it is supposed to before
    # any TTL command fires.
    env.expect('FT.SEARCH', 'idx_match', '@x:hello', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])
    env.expect('FT.SEARCH', 'idx_no_monitor', '@x:hello', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])
    env.expect('FT.SEARCH', 'idx_other_prefix', '@x:hello', 'NOCONTENT') \
        .equal([1, 'other:1'])
    env.expect('FT.SEARCH', 'idx_filter_skip', '*', 'NOCONTENT').equal([0])

    return conn


def _assert_non_matching_indexes_unchanged(env):
    # Indexes that did not match doc:1 must keep their pre-TTL view of the
    # data regardless of which TTL command fired.
    #
    # idx_no_monitor: spec opted out, so the fast path skipped it and the
    # DocTable entry for doc:1 was never tagged. Active expiration is off,
    # so the underlying hash is still present and doc:1 remains visible.
    env.expect('FT.SEARCH', 'idx_no_monitor', '@x:hello', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])
    # idx_other_prefix: doc:1 was never selected by this index's PREFIX, and
    # other:1 has no TTL of its own.
    env.expect('FT.SEARCH', 'idx_other_prefix', '@x:hello', 'NOCONTENT') \
        .equal([1, 'other:1'])
    # idx_filter_skip: the FILTER excludes every doc, so the index stays
    # empty and the SpecOp_Del branch in the fast path is exercised.
    env.expect('FT.SEARCH', 'idx_filter_skip', '*', 'NOCONTENT').equal([0])


def _run_key_expiration_with_non_matching_indexes(env, expire_cmd, ttl_arg, sleep_secs):
    # Cover the full-key TTL case when several indexes coexist but only
    # idx_match is affected by an EXPIRE/PEXPIRE notification.
    conn = _setup_non_matching_indexes(env)

    conn.execute_command(expire_cmd, 'doc:1', ttl_arg)
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(sleep_secs)

    # idx_match: monitorDocumentExpiration=true, so DocTable_UpdateExpiration
    # ran for doc:1 and DocTable_IsDocExpired now filters it out at search.
    env.expect('FT.SEARCH', 'idx_match', '@x:hello', 'NOCONTENT').equal([1, 'doc:2'])

    _assert_non_matching_indexes_unchanged(env)


@skip(cluster=True, redis_less_than='8.0')
def testKeyExpirationWithNonMatchingIndexes(env):
    # PEXPIRE granularity: TTL of 1ms, expires almost immediately.
    _run_key_expiration_with_non_matching_indexes(env, 'PEXPIRE', '1', 0.015)


@skip(cluster=True, redis_less_than='8.0')
def testKeyExpirationWithNonMatchingIndexesExpire(env):
    # EXPIRE (seconds) granularity: same scenario, but the keyspace
    # notification arrives via the seconds-based path.
    _run_key_expiration_with_non_matching_indexes(env, 'EXPIRE', '1', 1.05)


@skip(cluster=True, redis_less_than='8.0')
def testKeyPersistWithNonMatchingIndexes(env):
    # PERSIST shares the expire_cmd branch in src/notifications.c, so it
    # also flows through Indexes_UpdateMatchingDocExpiration. Verify that
    # clearing the TTL on doc:1 propagates to idx_match's DocTable entry
    # so the doc remains visible past the original PEXPIRE deadline, while
    # the non-matching indexes keep their pre-TTL view.
    conn = _setup_non_matching_indexes(env)

    # Use the same 100 ms / 200 ms timing as test_MOD_14800_persist_clears_expiration_metadata.
    env.expect('PEXPIRE', 'doc:1', '100').equal(1)
    env.expect('PERSIST', 'doc:1').equal(1)
    # Sleep past the original PEXPIRE deadline: if PERSIST had failed to clear
    # dmd->expirationTimeNs, DocTable_IsDocExpired would now filter doc:1 out.
    time.sleep(0.2)

    # idx_match: PERSIST cleared the DocTable TTL, so DocTable_IsDocExpired
    # returns false and doc:1 is still visible.
    env.expect('FT.SEARCH', 'idx_match', '@x:hello', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])

    _assert_non_matching_indexes_unchanged(env)


# Same scenario as testLazyVectorFieldExpiration but without INDEXMISSING, so the
# HPEXPIRE notification routes through the TTL-table fast path rather than the
# full reindex fallback. Verifies the fast path keeps KNN results consistent.
@skip(redis_less_than='8.0')
def testFastPathVectorFieldExpiration(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE idx SCHEMA v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 t TEXT n NUMERIC').ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')
    conn.execute_command('hset', 'doc:1', 'v', 'bababaca', 't', "hello", 'n', 1)
    conn.execute_command('hset', 'doc:2', 'v', 'babababa', 't', "hello", 'n', 2)
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'v')
    time.sleep(0.015)
    env.expect('FT.SEARCH', 'idx', '@n:[1, 4]=>[KNN 3 @v $vec]', 'PARAMS', 2, 'vec', 'aaaaaaaa', 'NOCONTENT', 'DIALECT', 3).equal([1, 'doc:2'])


# Same scenario as testLazyGeoshapeFieldExpiration but without INDEXMISSING, so
# the HPEXPIRE notification routes through the TTL-table fast path rather than
# the full reindex fallback. Verifies the fast path keeps geoshape results
# consistent.
@skip(redis_less_than='8.0')
def testFastPathGeoshapeFieldExpiration(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT').ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')
    first = 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))'
    second = 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))'
    conn.execute_command('HSET', 'doc:1', 'txt', 'hello', 'geom', first)
    conn.execute_command('HSET', 'doc:2', 'txt', 'world', 'geom', second)
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'geom')
    time.sleep(0.015)
    query = 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))'
    env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', query, 'NOCONTENT', 'DIALECT', 3).equal([1, 'doc:2'])


# Regression for the GEOSHAPE query iterator: read_single() applies the
# field-expiration predicate (DocTable_CheckFieldExpirationPredicate),
# but skip_to() historically did not.
# A bare `@geom:[within ...]` query only ever drives the iterator via Read, so
# the gap is invisible there. To reach skip_to(), the geoshape must be the
# NON-leading child of an intersection: an intersection sorts children by
# estimated result count and leads with the smallest via Read, driving the rest
# via SkipTo. Here `@txt:hello` matches only 2 docs while the geoshape matches
# all 12, so the text term leads and the geoshape is skipped to each candidate.
# doc:1's geom field is expired, so it must be filtered out. Before the fix
# skip_to() skipped the expiration check and doc:1 leaked into the results.
@skip(cluster=True, redis_less_than='8.0')
def testGeoshapeFieldExpirationSkipToPath(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'geom', 'GEOSHAPE', 'FLAT').ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')

    shape = 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))'
    # doc:1 and doc:2 carry the 'hello' term (2 estimated results) so the text
    # term leads the intersection; doc:1's geom field is the one that expires.
    conn.execute_command('HSET', 'doc:1', 'txt', 'hello', 'geom', shape)
    conn.execute_command('HSET', 'doc:2', 'txt', 'hello', 'geom', shape)
    # Filler docs inflate the geoshape iterator's estimate well above the text
    # term so the geoshape is the non-leading child driven via SkipTo.
    for i in range(3, 13):
        conn.execute_command('HSET', f'doc:{i}', 'txt', 'world', 'geom', shape)
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'geom')
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015)

    query = 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))'
    # The text term 'hello' leads and drives the geoshape via SkipTo. doc:1's
    # geom field is expired, so only doc:2 must survive the intersection.
    env.expect('FT.SEARCH', 'idx', '@txt:hello @geom:[within $poly]',
               'PARAMS', 2, 'poly', query, 'NOCONTENT').equal([1, 'doc:2'])


@skip(cluster=True, redis_less_than='8.0')
def testHashFieldExpirationWithNonMatchingIndexes(env):
    # Cover Indexes_UpdateMatchingHashFieldExpiration (src/spec.c) when several
    # indexes coexist but only some are affected by an HPEXPIRE notification.
    # Three distinct ways an index can fail to match the hash are exercised:
    #   - idx_other_prefix: PREFIX excludes the doc key entirely, so the spec
    #                       is not even returned by FindMatchingSchemaRules.
    #   - idx_other_field:  PREFIX matches but the schema does not reference
    #                       the expiring field, so the per-field rebuild walks
    #                       only schema fields with no new TTL and produces an
    #                       unchanged view.
    #   - idx_filter_skip:  PREFIX matches but the FILTER excludes the doc, so
    #                       FindMatchingSchemaRules tags the op as SpecOp_Del.
    # Only idx_match should reflect the field expiration; the others must keep
    # serving their pre-expiration view of the data.
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    env.expect('FT.CREATE', 'idx_match', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'SCHEMA', 'x', 'TEXT', 'y', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx_other_prefix', 'ON', 'HASH',
               'PREFIX', '1', 'other:',
               'SCHEMA', 'x', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx_other_field', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'SCHEMA', 'y', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx_filter_skip', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'FILTER', '@n == 100',
               'SCHEMA', 'x', 'TEXT', 'n', 'NUMERIC').ok()

    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx_match', 'fields')
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx_other_prefix', 'fields')
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx_other_field', 'fields')
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx_filter_skip', 'fields')

    # n=1 fails the FILTER on idx_filter_skip but the PREFIX still selects
    # these hashes for the keyspace-notification path.
    conn.execute_command('HSET', 'doc:1', 'x', 'hello', 'y', 'world', 'n', '1')
    conn.execute_command('HSET', 'doc:2', 'x', 'hello', 'y', 'world', 'n', '1')
    # other:1 belongs only to idx_other_prefix.
    conn.execute_command('HSET', 'other:1', 'x', 'hello')

    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'x')
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015)

    # idx_match: doc:1's x is expired, doc:2 still matches; y is untouched.
    env.expect('FT.SEARCH', 'idx_match', '@x:hello', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx_match', '@y:world', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])

    # idx_other_field: schema only references 'y', which has no TTL, so the
    # per-field rebuild yields an unchanged view and doc:1 stays searchable.
    env.expect('FT.SEARCH', 'idx_other_field', '@y:world', 'NOCONTENT') \
        .apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])

    # idx_other_prefix: doc:1 was never selected by this index's PREFIX.
    env.expect('FT.SEARCH', 'idx_other_prefix', '@x:hello', 'NOCONTENT') \
        .equal([1, 'other:1'])

    # idx_filter_skip: the FILTER excludes every doc, so the index stays empty
    # and the SpecOp_Del branch in the fast path is exercised.
    env.expect('FT.SEARCH', 'idx_filter_skip', '*', 'NOCONTENT').equal([0])


@skip(cluster=True, redis_less_than='8.0')
def testHashFieldExpirationFilterSkipWithIndexMissing(env):
    # Regression for the HPEXPIRE fast path in Indexes_UpdateMatchingHashFieldExpiration:
    # when a spec has any INDEXMISSING field, the fast path falls back to
    # IndexSpec_UpdateDoc(). FindMatchingSchemaRules is invoked with
    # runFilters=false, so the FILTER must still be honored on the fallback —
    # otherwise an HPEXPIRE on a hash whose PREFIX matches but whose FILTER
    # rejects it could incorrectly add the doc to the index.
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'PREFIX', '1', 'doc:',
               'FILTER', '@n == 100',
               'SCHEMA', 'x', 'TEXT', 'INDEXMISSING', 'n', 'NUMERIC').ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')

    # n=1 fails the FILTER, so doc:1 is never indexed. The PREFIX still
    # selects it for the keyspace-notification fast path.
    conn.execute_command('HSET', 'doc:1', 'x', 'hello', 'n', '1')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', 'ismissing(@x)', 'NOCONTENT', 'DIALECT', '3').equal([0])

    # HPEXPIRE on the FILTER-rejected doc must not add it to the index, even
    # though the spec has an INDEXMISSING field that routes through the
    # IndexSpec_UpdateDoc fallback.
    conn.execute_command('HPEXPIRE', 'doc:1', '60000', 'FIELDS', '1', 'x')

    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', 'ismissing(@x)', 'NOCONTENT', 'DIALECT', '3').equal([0])

    # Sanity check: a hash that passes the FILTER is indexed normally, and an
    # HPEXPIRE on its INDEXMISSING field flips it into the ismissing posting.
    conn.execute_command('HSET', 'doc:2', 'x', 'hello', 'n', '100')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([1, 'doc:2'])
    conn.execute_command('HPEXPIRE', 'doc:2', '1', 'FIELDS', '1', 'x')
    time.sleep(0.015)
    env.expect('FT.SEARCH', 'idx', 'ismissing(@x)', 'NOCONTENT', 'DIALECT', '3').equal([1, 'doc:2'])


@skip(redis_less_than='7.3')
def testLastFieldNoExpiration(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'x', 'TEXT', 'y', 'TEXT')
    conn.execute_command('HSET', 'doc:1', 'x', 'hello', 'y', 'hello')
    conn.execute_command('HSET', 'doc:2', 'x', 'hello', 'y', '57')
    conn.execute_command('HEXPIRE', 'doc:1', '300', 'FIELDS', '1', 'y')
    # We want to hit this line:
    # } else if (fieldIndexToCheck < fieldExpiration->index) {
    #   ++runningIndex;
    # for that we need a field with a high index that is set for expiration
    # we use a free text search that will return both documents
    # the mask for doc:1 will be for both x and y
    # doc:1 will see it has fields set for expiration
    # it will check if all of the fields are expired
    # this should lead to the line being hit
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])

def testDocWithLongExpiration(env):
    # We want to cover this snippet of code:
    # if (ttlEntry->fieldExpirations == NULL || array_len(ttlEntry->fieldExpirations) == 0) {
    #   // the document has no fields with expiration times, there exists at least one valid field
    #   return true;
    # }
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'x', 'TEXT', 'y', 'TEXT')
    conn.execute_command('HSET', 'doc:1', 'x', 'hello', 'y', 'hello')
    conn.execute_command('HSET', 'doc:2', 'x', 'hello', 'y', '57')
    # Set an expiration that will take a long time to expire
    conn.execute_command('EXPIRE', 'doc:1', '30000')
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').apply(sort_document_names).equal([2, 'doc:1', 'doc:2'])

def testSeekToExpirationChecks(env):
    # We want to cover the IndexReader_ReadWithSeeker function
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'x', 'TEXT', 'y', 'TEXT')
    conn.execute_command('HSET', 'doc:0', 'x', 'hello', 'y', 'foo') # doc:expire internal id is 1001
    # inverted index state
    # 'hello': [1]
    # 'foo': [1]
    for i in range(1, 1001):
        conn.execute_command('HSET', f'doc:{i}', 'x', 'hello', 'y', 'world')
        # important we expire now since that assigns a new doc id for the document
        # doc:{i} internal should now be (2 * i)
        conn.execute_command('HPEXPIRE', f'doc:{i}', '1', 'FIELDS', '1', 'y')
    conn.execute_command('HSET', 'doc:1001', 'x', 'hello', 'y', 'world')
    # inverted index state
    # 'hello': ['doc:0', 'doc:1', , ..., 'doc:1000', 'doc:1001']
    # 'world': ['doc:1', , ..., 'doc:1000', 'doc:1001']
    # 'foo': ['doc:0']

    # expected flow
    # - hello reader starts with doc:0
    # - world reader starts with doc:1
    # - intersect iterator reads doc:0 and tries to skip to it in world reader
    # - world reader should skip to doc:1001 since all the other docs will be expired
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015) # we want to sleep enough so we filter out the expired documents at the iterator phase
    # doc:0 up to doc:1000 should not be returned:
    # - doc:0 because y != world
    # - doc:1 up to doc:1000 y field should be expired
    # Due to the nature of intersection iterator we expect SkipTo to be called at least once
    # since text fields have a seeker we expect IndexReader_ReadWithSeeker to be called
    # that should provide coverage for IndexReader_ReadWithSeeker.
    env.expect('FT.SEARCH', 'idx', '@x:(hello) @y:(world)', 'NOCONTENT').equal([1, 'doc:1001'])


# Verify that background indexing does not cause lazy expiration of expired documents.
@skip(cluster=True)
def test_background_index_no_lazy_expiration(env):
    env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('HSET', 'doc:1', 't', 'bar').equal(1)
    env.expect('HSET', 'doc:2', 't', 'arr').equal(1)
    env.expect('PEXPIRE', 'doc:1', '1').equal(1)
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015)
    # Expect background indexing to take place after doc:1 has expired.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
    waitForIndex(env, 'idx')

    # Validate that doc:1 has expired but not evicted.
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'doc:2', ['t', 'arr']])
    env.expect('DBSIZE').equal(2)

    # Accessing doc:1 directly should cause lazy expire and its removal from the DB.
    env.expect('HGET', 'doc:1', 't').equal(None)
    env.expect('DBSIZE').equal(1)


# Same test as the above but for JSON documents.
@skip(cluster=True, no_json=True)
def test_background_index_no_lazy_expiration_json(env):
    env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('JSON.SET', 'doc:1', "$", r'{"t":"bar"}').ok()
    env.expect('JSON.SET', 'doc:2', "$", r'{"t":"arr"}').ok()
    env.expect('PEXPIRE', 'doc:1', '1').equal(1)
    # https://www.kernel.org/doc/html/latest/core-api/timekeeping.html (CLOCK_REALTIME_COARSE may be off by 10ms)
    time.sleep(0.015)
    # Expect background indexing to take place after doc:1 has expired.
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', 't', 'TEXT').equal('OK')
    waitForIndex(env, 'idx')

    # Validate that doc:1 has expired but not evicted.
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'doc:2', ['$', '{"t":"arr"}']])
    env.expect('DBSIZE').equal(2)

    # Accessing doc:1 directly should cause lazy expire and its removal from the DB.
    env.expect('JSON.GET', 'doc:1', "$").equal(None)
    env.expect('DBSIZE').equal(1)


@skip(cluster=True, redis_less_than='7.4')
def test_ttl_table_collision_chain():
    # Regression for the direct-modulo TTL table: seed the index with far more
    # HEXPIRE-covered docs than the TTL bucket cap, so every slot must carry
    # a collision chain. Then query for fresh and expired entries and verify
    # the chain walk returns the right ones.
    env = Env(moduleArgs='MAXDOCTABLESIZE 4')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'n', 'NUMERIC', 't', 'TAG').ok()

    # Docs 1..odd are long-lived; docs 1..even get a fast HPEXPIRE so they
    # will be reported as expired at query time.
    N = 64  # > 16x the bucket cap => many collisions per slot
    for d in range(1, N + 1):
        env.expect('HSET', f'doc:{d}', 'n', str(d), 't', 'tag').equal(2)
        if d % 2 == 0:
            env.expect('HPEXPIRE', f'doc:{d}', '1', 'FIELDS', '2', 'n', 't').equal([1, 1])

    time.sleep(0.050)

    # Tag filter should still find every odd doc; evens are all expired.
    res = env.cmd('FT.SEARCH', 'idx', '@t:{tag}', 'NOCONTENT', 'LIMIT', '0', str(N))
    returned = sorted(int(d.split(':')[1]) for d in res[1:])
    expected = list(range(1, N + 1, 2))
    env.assertEqual(returned, expected)

    # Numeric filter over the full range: same expectation.
    res = env.cmd('FT.SEARCH', 'idx', f'@n:[1 {N}]', 'NOCONTENT', 'LIMIT', '0', str(N))
    returned = sorted(int(d.split(':')[1]) for d in res[1:])
    env.assertEqual(returned, expected)

@skip(cluster=True, redis_less_than='7.4')
def test_wide_schema_field_expiration(env):
    # Indexes with >32 text fields are auto-promoted to wide schema encoding.
    # We use also field index >= 64 to trigger high half loop iteration
    N_FIELDS = 70

    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    schema = list(chain.from_iterable((f'f{i}', 'TEXT') for i in range(N_FIELDS)))
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', *schema)

    hello_kv = list(chain.from_iterable((f'f{i}', 'hello') for i in range(N_FIELDS)))
    conn.execute_command('HSET', 'doc:plain', *hello_kv)
    conn.execute_command('HSET', 'doc:short', *hello_kv)
    conn.execute_command('HPEXPIRE', 'doc:short', '1', 'FIELDS', '1', 'f5')

    kv_scan = list(chain.from_iterable(
        (f'f{i}', 'needle' if i in (3, 67) else 'filler') for i in range(N_FIELDS)))
    conn.execute_command('HSET', 'doc:scan', *kv_scan)
    conn.execute_command('HPEXPIRE', 'doc:scan', '1', 'FIELDS', '2', 'f3', 'f67')

    conn.execute_command('HSET', 'doc:docexp', *hello_kv)
    conn.execute_command('EXPIRE', 'doc:docexp', '30000')

    kv_lowexp = list(chain.from_iterable(
        (f'f{i}', 'tophalf' if i == 67 else 'other') for i in range(N_FIELDS)))
    conn.execute_command('HSET', 'doc:lowexp', *kv_lowexp)
    conn.execute_command('HPEXPIRE', 'doc:lowexp', '1', 'FIELDS', '1', 'f5')

    kv_below = list(chain.from_iterable(
        (f'f{i}', 'below' if i == 3 else 'other') for i in range(N_FIELDS)))
    conn.execute_command('HSET', 'doc:below', *kv_below)
    conn.execute_command('HPEXPIRE', 'doc:below', '1', 'FIELDS', '1', 'f50')

    kv_live = list(chain.from_iterable(
        (f'f{i}', 'alive' if i == 3 else 'other') for i in range(N_FIELDS)))
    conn.execute_command('HSET', 'doc:live', *kv_live)
    conn.execute_command('HEXPIRE', 'doc:live', '30000', 'FIELDS', '1', 'f3')

    waitForIndex(env, 'idx')

    time.sleep(0.050)

    # Match because:
    # - "doc:plain" has no expiration
    # - "doc:docexp" has a long doc expiration time
    # - "doc:short" has f5 expired but not the others
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').apply(sort_document_names) \
        .equal([3, 'doc:docexp', 'doc:plain', 'doc:short'])
    # Not match because:
    # - "doc:scan" f3 and f67 were expired
    env.expect('FT.SEARCH', 'idx', 'needle', 'NOCONTENT').equal([0])
    # Match because:
    # - "doc:lowexp" f67 matches so the f5 expiration is not considered
    env.expect('FT.SEARCH', 'idx', 'tophalf', 'NOCONTENT').equal([1, 'doc:lowexp'])
    # Match because:
    # - "doc:below" f3 matches so the f50 expiration is not considered
    env.expect('FT.SEARCH', 'idx', 'below', 'NOCONTENT').equal([1, 'doc:below'])
    # Match because:
    # - "doc:live" f3 matches and it is not expired yet
    env.expect('FT.SEARCH', 'idx', 'alive', 'NOCONTENT').equal([1, 'doc:live'])

@skip(cluster=True)
def test_expire_past_timestamp_removes_doc(env):
    env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('HSET', 'doc:1', 't', 'hello').equal(1)
    env.expect('HSET', 'doc:2', 't', 'world').equal(1)
    waitForIndex(env, 'idx')

    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').apply(sort_document_names).equal(
        [2, 'doc:1', 'doc:2'])

    env.expect('EXPIREAT', 'doc:1', '1').equal(1)

    env.expect('EXISTS', 'doc:1').equal(0)
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([1, 'doc:2'])


@skip(redis_less_than='7.3')
def test_inline_field_expiration_bit_reindex_on_hexpire(env):
    # Each inverted-index entry carries a per-field "this field has a TTL" bit,
    # set at index time, that expiration-aware iterators use to skip the
    # TTL-table lookup when it is clear. A document indexed WITHOUT any field TTL
    # therefore has the bit cleared on all of its term/tag/numeric postings. A
    # later HEXPIRE that gives a field its first TTL (the field's 0->1 transition)
    # must reindex the document so that field's postings get the bit set and the
    # now-expiring field is actually filtered out; otherwise the iterators would
    # keep skipping the check and wrongly return the expired field.
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 't', 'TEXT', 'tg', 'TAG', 'n', 'NUMERIC').ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')

    # Indexed without any field TTL: every posting's expiration bit is 0.
    conn.execute_command('HSET', 'doc:1', 't', 'hello', 'tg', 'red', 'n', '42')
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@tg:{red}', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@n:[42 42]', 'NOCONTENT').equal([1, 'doc:1'])

    # First field TTL on the matched fields (0->1) must trigger a reindex so the
    # bit flips to 1; once the TTL lapses, all three field types filter the doc.
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '3', 't', 'tg', 'n')
    time.sleep(0.02)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '@tg:{red}', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '@n:[42 42]', 'NOCONTENT').equal([0])


@skip(redis_less_than='7.3')
def test_inline_field_expiration_bit_reindex_when_sibling_field_already_expiring(env):
    # Field-granular regression: a document is indexed with field `b` already
    # carrying a TTL but field `a` not. So `a`'s postings have the bit cleared
    # even though the document is already in the TTL table (because of `b`). When
    # `a` later gets its first TTL, the per-field 0->1 transition must still
    # reindex — a doc-level "has any TTL" trigger would miss it (the doc already
    # had `b`'s TTL), leaving `a`'s postings with a stale 0 bit and wrongly
    # returning the expired `a`.
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'a', 'TEXT', 'b', 'TEXT').ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')

    conn.execute_command('HSET', 'doc:1', 'a', 'hello', 'b', 'world')
    # Give only `b` a (long) TTL: the document enters the TTL table, but `a`'s
    # postings keep bit=0.
    conn.execute_command('HEXPIRE', 'doc:1', '10000', 'FIELDS', '1', 'b')
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc:1'])

    # Now `a` gets its first TTL (per-field 0->1 even though the doc already had
    # `b`'s TTL): must reindex so `a`'s postings flip to 1 and `a` is filtered.
    conn.execute_command('HPEXPIRE', 'doc:1', '1', 'FIELDS', '1', 'a')
    time.sleep(0.02)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([0])
    # `b` is still valid (long TTL), so a query on `b` still returns the doc.
    env.expect('FT.SEARCH', 'idx', 'world', 'NOCONTENT').equal([1, 'doc:1'])
