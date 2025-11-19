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
            env.assertEqual(str(e), 'Unknown index name')

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
    env.assertEqual(res, [2, ['t', 'arr'], ['t', 'bar']])

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
