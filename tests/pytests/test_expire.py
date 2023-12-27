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

# Skip this test for Redis versions below 7.2 due to a bug in PEXPIRE.
# In older versions, a bug involving multiple time samplings during PEXPIRE execution
# can cause keys to prematurely expire, triggering a "del" notification and eliminating them from the index,
# thus missing from search results.
# This impacts the test as the key should be included in the search results but return NULL upon access
# (i.e lazy expiration).
# The bug was resolved in Redis 7.2, ensuring the test's stability.
@skip(cluster=True, redis_less_than="7.2")
def testExpireDocs(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    conn.execute_command('FT.CREATE idx SCHEMA t TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'bar')

    # both docs exist
    env.expect('FT.SEARCH', 'idx', '*').equal([2, 'doc1', ['t', 'foo'], 'doc2', ['t', 'bar']])

    conn.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.01)

    # both docs exist but doc1 fail to load field since they were expired passively
    env.expect('FT.SEARCH', 'idx', '*').equal([2, 'doc1', None, 'doc2', ['t', 'bar']])

    # only 1 doc is left
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'doc2', ['t', 'bar']])


    # test with SCOREEXPLAIN and EXPLAINSCORE - make sure all memory is released
    conn.execute_command('HSET', 'doc1', 't', 'foo')

    # both docs exist
    res = [2, 'doc2', ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]], ['t', 'bar'],
              'doc1', ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]], ['t', 'foo']]
    env.expect('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE').equal(res)

    conn.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.01)

    # both docs exist but doc1 fail to load field since they were expired passively
    res = [2, 'doc2', ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]], ['t', 'bar'],
              'doc1', ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]], None]
    env.expect('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE').equal(res)

    # only 1 doc is left
    res = [1, 'doc2', ['1', ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                                ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]], ['t', 'bar']]
    env.expect('FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE').equal(res)
