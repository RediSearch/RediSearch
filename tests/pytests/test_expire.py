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

res_doc1_is_None = [2, 'doc1', None, 'doc2', ['t', 'foo']]
res_doc1_is_discarded = [1, 'doc2', ['t', 'foo']]

res_score_and_explanation = ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                    ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]]

def testExpireDocs(env):
    expected_results = [res_doc1_is_None, # Without sortby -  both docs exist but we failed to load doc1 since it was expired lazily
                        res_doc1_is_discarded, # With sortby - when the sorter fails to do that, it discards the result.
                        # WITHSCORES, EXPLAINSCORE
                        [2, 'doc2', res_score_and_explanation, ['t', 'foo'], #  without sortby
                        'doc1', res_score_and_explanation, None],
                        [1, 'doc2', res_score_and_explanation, ['t', 'foo']]] #  with sortby

    expireDocs(env, False, # Without SORTABLE - since the fields are not SORTABLE, we need to load the results from Redis Keyspace
                expected_results)


def testExpireDocsSortable(env):
    '''
    Same as test `testExpireDocs` only with SORTABLE
    '''

    expected_results = [res_doc1_is_None, # without sortby
                        res_doc1_is_None, # With sortby
                        # WITHSCORES, EXPLAINSCORE
                        [2, 'doc2', res_score_and_explanation, ['t', 'foo'], #  without sortby
                        'doc1', res_score_and_explanation, None],
                        [2, 'doc2', res_score_and_explanation, ['t', 'foo'], #  with sortby
                        'doc1', res_score_and_explanation, None]]

    expireDocs(env, True,  # With SORTABLE -
               # The documents data exists in the index.
               # Since we are not trying to load the document in the sorter, it is not discarded from the results,
               # but it is marked as deleted and we reply with None.
               expected_results)

def expireDocs(env, isSortable, expected_results):
    '''
    This test creates an index and two documents
    We disable active expiration
    One of the documents is expired. As a result we fail to open the key in Redis keyspace.
    The test checks the expected output, which depends on wether the field is sortable and if the query should be sorted by this field.
    If the field is not SORTABLE and we sortby it, the sorter fails to load the field's value and discards the result.
    In all other combinations (SORTABLE + sortby, SORTABLE + no sortyby, not SORTABLE + no sortby)
    the document will be loaded in the loader, which doesn't discard results in case that the load fails, but no data is stored in the look up table,
    hence we return 'None'.

    When isSortable is True the index is created with `SORTABLE` arg
    '''
    env.skipOnCluster()
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


        # test with SCOREEXPLAIN and EXPLAINSCORE - make sure all memory is released
        conn.execute_command('HSET', 'doc1', 't', 'zoo')

        # both docs exist
        expected_res = [2, 'doc2', res_score_and_explanation, ['t', 'foo'],
                'doc1', res_score_and_explanation, ['t', 'zoo']]

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


