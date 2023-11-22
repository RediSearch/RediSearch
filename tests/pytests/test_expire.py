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

res_doc1_is_empty = [2, 'doc1', [], 'doc2', ['t', 'foo']]
res_doc1_is_empty_last = [2, 'doc2', ['t', 'foo'], 'doc1', []]
res_doc1_is_partial = [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']]
res_doc1_is_partial_last = [2, 'doc2', ['t', 'foo'], 'doc1', ['t', 'bar']]

res_score_and_explanation = ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                    ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]]

def testExpireDocs(env):
    empty_with_scores_and_explain_last = res_doc1_is_empty_last.copy()
    for offset, i in enumerate(range(2, len(res_doc1_is_partial), 2)):
        empty_with_scores_and_explain_last.insert(i + offset, res_score_and_explanation)

    expected_results = [res_doc1_is_empty, # Without sortby -  both docs exist but we failed to load doc1 since it was expired lazily
                        res_doc1_is_empty_last, # With sortby - sorter compares a missing value (doc1) to an existing value (doc2) and prefers the existing value
                        # WITHSCORES, EXPLAINSCORE
                        empty_with_scores_and_explain_last, #  without sortby
                        empty_with_scores_and_explain_last] #  with sortby

    expireDocs(env, False, # Without SORTABLE - since the fields are not SORTABLE, we need to load the results from Redis Keyspace
                expected_results)


def testExpireDocsSortable(env):
    '''
    Same as test `testExpireDocs` only with SORTABLE
    '''
    partial_with_scores_and_explain = res_doc1_is_partial.copy()
    partial_with_scores_and_explain_last = res_doc1_is_partial_last.copy()
    for offset, i in enumerate(range(2, len(res_doc1_is_partial), 2)):
        partial_with_scores_and_explain.insert(i + offset, res_score_and_explanation)
        partial_with_scores_and_explain_last.insert(i + offset, res_score_and_explanation)

    expected_results = [res_doc1_is_empty,   # without sortby
                        res_doc1_is_partial, # With sortby
                        # WITHSCORES, EXPLAINSCORE
                        partial_with_scores_and_explain_last,  # without sortby
                        partial_with_scores_and_explain]       # with sortby

    expireDocs(env, True,  # With SORTABLE -
               # The documents data exists in the index.
               # Since we are not trying to load the document in the sorter, it is not discarded from the results,
               # but it is marked as deleted and we reply with None.
               expected_results)

@skip(cluster=True)
def expireDocs(env, isSortable, expected_results):
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

    # i = 0 -> without sortby, i = 1 -> with sortby
    for i in range(2):
        # Use "lazy" expire (expire only when key is accessed)
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
        sortby_cmd = [] if i == 0 else ['SORTBY', 't']
        sortable_arg = [] if not isSortable else ['SORTABLE']
        conn.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', *sortable_arg)
        conn.execute_command('HSET', 'doc1', 't', 'bar')
        conn.execute_command('HSET', 'doc2', 't', 'foo')

        # Both docs exist.
        res = conn.execute_command('FT.SEARCH', 'idx', '*')
        env.assertEqual(res, [2, 'doc1', ['t', 'bar'], 'doc2', ['t', 'foo']])

        conn.execute_command('PEXPIRE', 'doc1', 1)
        # ensure expiration before search
        time.sleep(0.01)

        msg = '{}{} sortby'.format(
            'SORTABLE ' if isSortable else '', 'without' if i == 0 else 'with')
        # First iteration
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, expected_results[i], message=msg)

        # Cancel lazy expire to allow the deletion of the key
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        # ensure expiration before search
        time.sleep(0.5)

        # Second iteration - only 1 doc is left
        res = conn.execute_command('FT.SEARCH', 'idx', '*', *sortby_cmd)
        env.assertEqual(res, [1, 'doc2', ['t', 'foo']],
                        message=msg)


        # test with WITHSCORES and EXPLAINSCORE - make sure all memory is released
        conn.execute_command('HSET', 'doc1', 't', 'bar')

        # both docs exist
        expected_res = [2, 'doc2', res_score_and_explanation, ['t', 'foo'],
                           'doc1', res_score_and_explanation, ['t', 'bar']]

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
        res = [1, 'doc2', res_score_and_explanation, ['t', 'foo']]
        env.expect('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE').equal(res)

        conn.execute_command('FLUSHALL')
