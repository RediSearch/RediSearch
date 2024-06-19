import copy
from itertools import chain

from common import *

@skip(cluster=True)
def testExpireIndex(env):
    # temporary indexes
    env.cmd('ft.create', 'idx', 'TEMPORARY', '4', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'TTL', 'idx')
        time.sleep(1)
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'this is a simple test')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'TTL', 'idx')
        time.sleep(1)
    env.cmd('ft.search', 'idx', 'simple')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'TTL', 'idx')
        time.sleep(1)
    env.cmd('ft.aggregate', 'idx', 'simple', 'LOAD', '1', '@test')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    try:
        while True:
            ttl = env.cmd('ft.debug', 'TTL', 'idx')
            time.sleep(1)
    except Exception as e:
        env.assertEqual(str(e), 'Unknown index name')

res_score_and_explanation = ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                    ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]]
both_docs_no_sortby = "both_docs_no_sortby"
both_docs_sortby = "both_docs_sortby"
doc1_is_empty = "doc1_is_empty"
doc1_is_empty_sortby = "doc1_is_empty_sortby"
doc1_is_partial = "doc1_is_partial"
doc1_is_partial_sortby = "doc1_is_partial_sortby"
doc1_is_empty_last = "doc1_is_empty_last"
doc1_is_empty_last_sortby = "doc1_is_empty_last_sortby"
doc1_is_partial_last = "doc1_is_partial_last"
doc1_is_partial_last_sortby = "doc1_is_partial_last_sortby"
only_doc2_sortby = "only_doc2_sortby"
only_doc2_no_sortby = "only_doc2_no_sortby"

def add_explain_to_results(results):
    results = results.copy()
    for offset, i in enumerate(range(2, len(results), 2)):
        results.insert(i+offset, res_score_and_explanation)
    return results

def buildExpireDocsResults(isSortable, isJson):
    results = {}
    # When calling FT.SEARCH with SORTBY on json index, the sortby field is loaded into the result together with the json document
    results[both_docs_no_sortby] = [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', ['$', '{"t":"bar"}'], 'doc2', ['$', '{"t":"foo"}']]
    results[both_docs_sortby] = [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', ['t', 'bar','$', '{"t":"bar"}'], 'doc2', ['t', 'foo', '$', '{"t":"foo"}']]

    results[doc1_is_empty] = [2, 'doc1', [], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', [], 'doc2', ['$', '{"t":"foo"}']]
    results[doc1_is_empty_sortby] = [2, 'doc2', ['t', 'foo'], 'doc1', []] if not isJson else [2, 'doc2', ['t', 'foo', '$', '{"t":"foo"}'], 'doc1', []]

    results[doc1_is_partial] = [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo', '$', '{"t":"foo"}']]
    results[doc1_is_partial_sortby] = [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo', '$', '{"t":"foo"}']]

    results[doc1_is_empty_last] = [2, 'doc2', ['t', 'foo'], 'doc1', []] if not isJson else [2, 'doc2', ['t', 'foo', '$', '{"t":"foo"}'], 'doc1', []]
    results[doc1_is_empty_last_sortby] = [2, 'doc2', ['t', 'foo'], 'doc1', []] if not isJson else [2, 'doc2', ['t', 'foo', '$', '{"t":"foo"}'], 'doc1', []]

    results[doc1_is_partial_last] = [2, 'doc2', ['t', 'foo'], 'doc1', ['t', 'bar']] if not isJson else [2, 'doc2', ['$', '{"t":"foo"}'], 'doc1', ['t', 'bar']]
    results[doc1_is_partial_last_sortby] = [2, 'doc2', ['t', 'foo'], 'doc1', ['t', 'bar']] if not isJson else [2, 'doc2', ['t', 'foo', '$', '{"t":"foo"}'], 'doc1', ['t', 'bar', '$', '{"t":"bar"}']]

    results[only_doc2_sortby] = [1, 'doc2', ['t', 'foo']] if not isJson else [1, 'doc2', ['t', 'foo', '$', '{"t":"foo"}']]
    results[only_doc2_no_sortby] = [1, 'doc2', ['t', 'foo']] if not isJson else [1, 'doc2', ['$', '{"t":"foo"}']]


    return results

    # return [both_docs_no_sortby, # both docs exist
    #         res_doc1_is_empty_last if not isSortable else res_doc1_is_partial, # With sortby - sorter compares a missing value (doc1) to an existing value (doc2) and prefers the existing value
    #         res_doc1_is_empty, # Without sortby -  both docs exist but we failed to load doc1 since it was expired lazily
    #         # WITHSCORES, EXPLAINSCORE
    #         empty_with_scores_and_explain_last if not isSortable else partial_with_scores_and_explain,
    #         empty_with_scores_and_explain_last if not isSortable else partial_with_scores_and_explain_last]

# Refer to expireDocs for details on why this test is skipped for Redis versions below 7.2
@skip(cluster=True, redis_less_than="7.2")
def testExpireDocs(env):

    for isJson in [False, True]:
        expected_results = buildExpireDocsResults(False, isJson)
        expireDocs(env, False, # Without SORTABLE - since the fields are not SORTABLE, we need to load the results from Redis Keyspace
                expected_results, isJson)

# Refer to expireDocs for details on why this test is skipped for Redis versions below 7.2
@skip(cluster=True, redis_less_than="7.2")
def testExpireDocsSortable(env):
    '''
    Same as test `testExpireDocs` only with SORTABLE
    '''

    for isJson in [False, True]:
        expected_results = buildExpireDocsResults(True, isJson)
        expireDocs(env, True,  # With SORTABLE -
               # The documents data exists in the index.
               # Since we are not trying to load the document in the sorter, it is not discarded from the results,
               # but it is marked as deleted and we reply with None.
               expected_results, isJson)


#TODO: DvirDu: I think this test should be broken down to smaller tests, due to the complexity of the test and the number of cases it covers it is hard to debug
# Skip this test for Redis versions below 7.2 due to a bug in PEXPIRE.
# In older versions, a bug involving multiple time samplings during PEXPIRE execution
# can cause keys to prematurely expire, triggering a "del" notification and eliminating them from the index,
# thus missing from search results.
# This impacts the test as the key should be included in the search results but return NULL upon access
# (i.e lazy expiration).
# The bug was resolved in Redis 7.2, ensuring the test's stability.
def expireDocs(env, isSortable, expected_results, isJson):
    '''
    This test creates an index and two documents
    We disable active expiration
    One of the documents is expired. As a result we fail to open the key in Redis keyspace.
    The test checks the expected output, which depends on wether the field is sortable and if the query should be sorted by this field.
    The document will be loaded in the loader, which doesn't discard results in case that the load fails, but no data is stored in the look up table,
    hence we return 'None'.

    When isSortable is True the index is created with `SORTABLE` arg
    '''
    conn = env.getConnection()

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
            conn.execute_command('JSON.SET', 'doc2', '$', '{"t":"foo"}')
        else:
            conn.execute_command(
                'FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', *sortable_arg)
            conn.execute_command('HSET', 'doc1', 't', 'bar')
            conn.execute_command('HSET', 'doc2', 't', 'foo')

        # Both docs exist.
        res = conn.execute_command('FT.SEARCH', 'idx', '*')
        env.assertEqual(res, expected_results[both_docs_no_sortby], message='both docs exist')

        conn.execute_command('PEXPIRE', 'doc1', 1)
        # ensure expiration before search
        time.sleep(0.01)

        msg = '{}{} sortby'.format(
            'SORTABLE ' if isSortable else '', 'without' if not sortby else 'with')
        # First iteration
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        if isSortable:
            expected_res = expected_results[doc1_is_partial_sortby if sortby else doc1_is_empty] # We don't load the field to the rlookup when it is sortable on sortby
        else:
            expected_res = expected_results[doc1_is_empty_sortby if sortby else doc1_is_empty]
        env.assertEqual(res, expected_res, message=msg)

        # Cancel lazy expire to allow the deletion of the key
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        # Second iteration - only 1 doc is left
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, expected_results[only_doc2_sortby if sortby else only_doc2_no_sortby], message=msg)


        # test with WITHSCORES and EXPLAINSCORE - make sure all memory is released
        # we need to re-write the documents since in case of score tie they will be returned by internal id order. This will break once we will ignore stale updates to documents
        if isJson:
            conn.execute_command('JSON.SET', 'doc1', '$', '{"t":"bar"}')
            conn.execute_command('JSON.SET', 'doc2', '$', '{"t":"foo"}')
        else:
            conn.execute_command('HSET', 'doc1', 't', 'bar')
            conn.execute_command('HSET', 'doc2', 't', 'foo')

        # both docs exist
        expected_res = add_explain_to_results(expected_results[both_docs_no_sortby])

        res = conn.execute_command('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE')
        env.assertEqual(res, expected_res)

        # Activate lazy expire again to ensure the key is not expired before we run the query
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

        # expire doc1
        conn.execute_command('PEXPIRE', 'doc1', 1)
        # ensure expiration before search
        time.sleep(0.01)

        res = conn.execute_command('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE', *sortby_cmd)

        if isSortable:
            env.assertEqual(res, add_explain_to_results(expected_results[doc1_is_partial if sortby else doc1_is_empty]), message=(msg + ' WITHSCORES, EXPLAINSCORE'))
        else:
            env.assertEqual(res, add_explain_to_results(expected_results[doc1_is_empty_last if sortby else doc1_is_empty]), message=(msg + ' WITHSCORES, EXPLAINSCORE'))

        # Cancel lazy expire to allow the deletion of the key
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        # only 1 doc is left
        res = add_explain_to_results(expected_results[only_doc2_no_sortby])
        env.expect('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE').equal(res)

        conn.execute_command('FLUSHALL')


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
            for field_name, field_value in fields.items():
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
@skip(redis_less_than='7.4')
def testSingleExpireField(env):
    field_to_additional_schema_keywords = {'x': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})

# Aims to test that the expiration of a single field will not affect the search results
@skip(redis_less_than='7.4')
def testTwoFieldsOneOfThemWillExpire(env):
    field_to_additional_schema_keywords = {'x': [], 'y': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['y']},
                          document_name_to_expire={'doc1': True, 'doc2': False})

# Aims to test that the expiration of a single sortable field will cause the document to expire
@skip(redis_less_than='7.4')
def testSingleSortableFieldWithExpiration(env):
    field_to_additional_schema_keywords = {'x': ['SORTABLE']}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})

# Aims to test that the expiration of a single sortable field will not affect the search results
@skip(redis_less_than='7.4')
def testSortableFieldWithExpirationAndRegularField(env):
    field_to_additional_schema_keywords = {'x': ['SORTABLE'], 'y': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})


# Aims to test that the expiration of a single non sortable field will not affect the search results
@skip(redis_less_than='7.4')
def testFieldWithExpirationAndSortableField(env):
    field_to_additional_schema_keywords = {'x': [], 'y': ['SORTABLE']}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x']},
                          document_name_to_expire={'doc1': True, 'doc2': False})


# Aims to test that two fields with different expiration times will eventually cause the key itself to expire
@skip(redis_less_than='7.4')
def testExpireMultipleFields(env):
    field_to_additional_schema_keywords = {'x': [], 'y': [], 'z': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x'], 3: ['y', 'z']},
                          document_name_to_expire={'doc1': True, 'doc2': False})


# Aims to test that the expectation that for 2 fields with the same expiration time we will get a single notification
@skip(redis_less_than='7.4')
def testExpireMultipleFieldsWhereOneIsSortable(env):
    field_to_additional_schema_keywords = {'x': ['SORTABLE'], 'y': [], 'z': []}
    schema = createTextualSchema(field_to_additional_schema_keywords)
    commonFieldExpiration(env, schema, field_to_additional_schema_keywords.keys(),
                          expiration_interval_to_fields={1: ['x'], 3: ['y', 'z']},
                          document_name_to_expire={'doc1': True, 'doc2': False})