from common import *


def testExpireIndex(env):
    # temporary indexes
    if env.isCluster():
        env.skip()
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
                                    
def buildExpireDocsResults(isSortable, isJson):
    both_docs = [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', ['$', '{"t":"bar"}'], 'doc2', ['$', '{"t":"foo"}']]
    res_doc1_is_empty = [2, 'doc1', [], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', [], 'doc2', ['$', '{"t":"foo"}']]
    res_doc1_is_empty_last = [2, 'doc2', ['t', 'foo'], 'doc1', []] if not isJson else [2, 'doc2', ['t', 'foo', '$', '{"t":"foo"}'], 'doc1', []]
    res_doc1_is_partial = [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']] if not isJson else [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo', '$', '{"t":"foo"}']]
    res_doc1_is_partial_last = [2, 'doc2', ['t', 'foo'], 'doc1', ['t', 'bar']] if not isJson else [2, 'doc2', ['$', '{"t":"foo"}'], 'doc1', []]

    empty_with_scores_and_explain_last = res_doc1_is_empty_last.copy()
    for offset, i in enumerate(range(2, len(res_doc1_is_partial), 2)):
        empty_with_scores_and_explain_last.insert(i + offset, res_score_and_explanation)
    
    partial_with_scores_and_explain = res_doc1_is_partial.copy()
    partial_with_scores_and_explain_last = res_doc1_is_partial_last.copy()
    for offset, i in enumerate(range(2, len(res_doc1_is_partial), 2)):
        partial_with_scores_and_explain.insert(i + offset, res_score_and_explanation)
        partial_with_scores_and_explain_last.insert(i + offset, res_score_and_explanation)

    return [both_docs, # both docs exist
            res_doc1_is_empty_last if not isSortable else res_doc1_is_partial, # With sortby - sorter compares a missing value (doc1) to an existing value (doc2) and prefers the existing value
            res_doc1_is_empty, # Without sortby -  both docs exist but we failed to load doc1 since it was expired lazily
            # WITHSCORES, EXPLAINSCORE
            empty_with_scores_and_explain_last if not isSortable else partial_with_scores_and_explain,
            empty_with_scores_and_explain_last if not isSortable else partial_with_scores_and_explain_last]

def testExpireDocs(env):

    for isJson in [False, True]:
        expected_results = buildExpireDocsResults(False, isJson)
        expireDocs(env, False, # Without SORTABLE - since the fields are not SORTABLE, we need to load the results from Redis Keyspace
                expected_results, isJson)


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
    env.skipOnCluster()
    conn = env.getConnection()

    # i = 2 -> without sortby, i = 1 -> with sortby
    for i in [1, 2]:
        sortby = i==1
        print ('sortby = {}'.format(sortby), "isSortable = {}".format(isSortable), "isJson = {}".format(isJson))
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
        env.assertEqual(res, expected_results[0])

        conn.execute_command('PEXPIRE', 'doc1', 1)
        # ensure expiration before search
        time.sleep(0.01)

        msg = '{}{} sortby'.format(
            'SORTABLE ' if isSortable else '', 'without' if not sortby else 'with')
        print("before expire")
        # First iteration
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, expected_results[i], message=msg)

        # Cancel lazy expire to allow the deletion of the key
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        print("before expire")
        # Second iteration - only 1 doc is left
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, [1, 'doc2', ['t', 'foo']] if not isJson else ([1, 'doc2', ['t', 'foo', '$', '{"t":"foo"}'] if sortby else ['$', '{"t":"foo"}']]), message=msg)


        # test with WITHSCORES and EXPLAINSCORE - make sure all memory is released
        if isJson:
            conn.execute_command('JSON.SET', 'doc1', '$', '{"t":"bar"}')
        else: 
            conn.execute_command('HSET', 'doc1', 't', 'bar')

        # both docs exist
        expected_res = [2, 'doc2', res_score_and_explanation, ['t', 'foo'] if not isJson else ['$', '{"t":"foo"}'],
                           'doc1', res_score_and_explanation, ['t', 'bar'] if not isJson else ['$', '{"t":"bar"}']]

        res = conn.execute_command('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE')
        env.assertEqual(res, expected_res)

        # Active lazy expire again to ensure the key is not expired before we run the query
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

        # expire doc1
        conn.execute_command('PEXPIRE', 'doc1', 1)
        # ensure expiration before search
        time.sleep(0.01)

        res = conn.execute_command('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE', *sortby_cmd)
        env.assertEqual(res, expected_results[i + 2], message=(msg + ' WITHSCORES, EXPLAINSCORE'))

        # Cancel lazy expire to allow the deletion of the key
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        # only 1 doc is left
        res = [1, 'doc2', res_score_and_explanation, ['t', 'foo'] if not isJson else ['$', '{"t":"foo"}']]
        env.expect('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE').equal(res)

        conn.execute_command('FLUSHALL')
