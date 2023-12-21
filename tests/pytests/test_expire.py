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

@skip(cluster=True)
def testExpireDocs(env):

    for isJson in [False, True]:
        expected_results = buildExpireDocsResults(False, isJson)
        expireDocs(env, False, # Without SORTABLE - since the fields are not SORTABLE, we need to load the results from Redis Keyspace
                expected_results, isJson)

@skip(cluster=True)
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

        # Active lazy expire again to ensure the key is not expired before we run the query
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
