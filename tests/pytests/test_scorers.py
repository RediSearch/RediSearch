import math
from includes import *
from common import getConnectionByEnv, waitForIndex, check_server_version


def testHammingScorer(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score', 'schema', 'title', 'text').ok()
    waitForIndex(env, 'idx')

    for i in range(16):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1,
                   'payload', ('%x' % i) * 8,
                   'fields', 'title', 'hello world').ok()
    for i in range(16):
        res = env.cmd('ft.search', 'idx', '*', 'PAYLOAD', ('%x' % i) * 8,
                      'SCORER', 'HAMMING', 'WITHSCORES', 'WITHPAYLOADS')
        env.assertEqual(res[1], 'doc%d' % i)
        env.assertEqual(res[2], '1')
        # test with payload of different length
        res = env.cmd('ft.search', 'idx', '*', 'PAYLOAD', ('%x' % i) * 7,
                      'SCORER', 'HAMMING', 'WITHSCORES', 'WITHPAYLOADS')
        env.assertEqual(res[2], '0')
        # test with no payload
        res = env.cmd('ft.search', 'idx', '*',
                      'SCORER', 'HAMMING', 'WITHSCORES', 'WITHPAYLOADS')
        env.assertEqual(res[2], '0')

def testScoreTagIndex(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    N = 25
    for n in range(N):

        sc = math.sqrt(float(N - n + 10) / float(N + 10))
        # print n, sc

        env.expect('ft.add', 'idx', 'doc%d' % n, sc, 'fields',
                   'title', 'hello world ' * n, 'body', 'lorem ipsum ' * n).ok()
    results_single = [
        [24L, 'doc1', 1.97, 'doc2', 1.94, 'doc3',
            1.91, 'doc4', 1.88, 'doc5', 1.85],
        [24L, 'doc1', 0.9, 'doc2', 0.59, 'doc3',
            0.43, 'doc4', 0.34, 'doc5', 0.28],
        [24L, 'doc4', 1.75, 'doc5', 1.75, 'doc3',
            1.74, 'doc6', 1.74, 'doc7', 1.72],
        [24L, 'doc24', 480.0, 'doc23', 460.0, 'doc22',
            440.0, 'doc21', 420.0, 'doc20', 400.0],
        [24L, 'doc1', 0.99, 'doc2', 0.97, 'doc3',
            0.96, 'doc4', 0.94, 'doc5', 0.93]
    ]
    results_cluster = [
        [24L, 'doc1', 1.97, 'doc2', 1.94, 'doc3',
            1.91, 'doc4', 1.88, 'doc5', 1.85],
        [24L, 'doc1', 0.9, 'doc2', 0.59, 'doc3',
            0.43, 'doc4', 0.34, 'doc5', 0.28],
        [24L, 'doc4', 1.76, 'doc5', 1.75, 'doc3',
            1.74, 'doc6', 1.73, 'doc7', 1.72],
        [24L, 'doc24', 480.0, 'doc23', 460.0, 'doc22',
            440.0, 'doc21', 420.0, 'doc20', 400.0],
        [24L, 'doc1', 0.99, 'doc2', 0.97, 'doc3',
            0.96, 'doc4', 0.94, 'doc5', 0.93],
    ]

    scorers = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'DISMAX', 'DOCSCORE']
    expected_results = results_cluster if env.is_cluster() else results_single

    for _ in env.reloading_iterator():
        waitForIndex(env, 'idx')
        for i, scorer in enumerate(scorers):
            res = env.cmd('ft.search', 'idx', 'hello world', 'scorer',
                              scorer, 'nocontent', 'withscores', 'limit', 0, 5)
            res = [round(float(x), 2) if j > 0 and (j - 1) %
                   2 == 1 else x for j, x in enumerate(res)]
            #print res
            env.assertListEqual(expected_results[i], res)

def testDocscoreScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem').ok()
    env.expect('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem').ok()
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DOCSCORE')
    env.assertEqual(res[0], 3L)
    env.assertEqual(res[2][1], "Document's score is 1.00")
    env.assertEqual(res[5][1], "Document's score is 0.50")
    env.assertEqual(res[8][1], "Document's score is 0.10")

def testTFIDFScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem').ok()
    env.expect('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem').ok()
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE')
    env.assertEqual(res[0], 3L)
    env.assertEqual(res[2][1], ['Final TFIDF : words TFIDF 20.00 * document score 0.50 / norm 10 / slop 1',
                                [['(Weight 1.00 * total children TFIDF 20.00)',
                                ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]])
    env.assertEqual(res[5][1],['Final TFIDF : words TFIDF 20.00 * document score 1.00 / norm 10 / slop 2',
                                [['(Weight 1.00 * total children TFIDF 20.00)',
                                ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]])
    env.assertEqual(res[8][1], ['Final TFIDF : words TFIDF 20.00 * document score 0.10 / norm 10 / slop 3',
                                [['(Weight 1.00 * total children TFIDF 20.00)',
                                ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]])

    # test depth limit
    res = env.cmd('ft.search', 'idx', 'hello(world(world))', 'withscores', 'EXPLAINSCORE', 'limit', 0, 1)
    env.assertEqual(res[2][1], ['Final TFIDF : words TFIDF 30.00 * document score 0.50 / norm 10 / slop 1',
                                [['(Weight 1.00 * total children TFIDF 30.00)',
                                    ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                        ['(Weight 1.00 * total children TFIDF 20.00)',
                                            ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                            '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]]])

    res1 = ['Final TFIDF : words TFIDF 40.00 * document score 1.00 / norm 10 / slop 1',
                [['(Weight 1.00 * total children TFIDF 40.00)',
                    ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                        ['(Weight 1.00 * total children TFIDF 30.00)',
                            ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                ['(Weight 1.00 * total children TFIDF 20.00)',
                                    ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                     '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]]]]]
    res2 = ['Final TFIDF : words TFIDF 40.00 * document score 1.00 / norm 10 / slop 1',
                [['(Weight 1.00 * total children TFIDF 40.00)',
                    ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                        ['(Weight 1.00 * total children TFIDF 30.00)',
                            ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                             '(Weight 1.00 * total children TFIDF 20.00)']]]]]]


    actual_res = env.cmd('ft.search', 'idx', 'hello(world(world(hello)))', 'withscores', 'EXPLAINSCORE', 'limit', 0, 1)
    # on older versions we trim the reply to remain under the 7-layer limitation.
    res = res1 if check_server_version(env, "6.2.0") else res2
    env.assertEqual(actual_res[2][1], res)

def testBM25ScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem').ok()
    env.expect('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem').ok()
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25')
    env.assertEqual(res[0], 3L)
    if env.isCluster():
        env.assertContains('Final BM25', res[2][1][0])
        env.assertContains('Final BM25', res[5][1][0])
        env.assertContains('Final BM25', res[8][1][0])
    else:
        env.assertEqual(res[2][1], ['Final BM25 : words BM25 1.56 * document score 0.50 / slop 1',
                            [['(Weight 1.00 * children BM25 1.56)',
                            ['(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))',
                            '(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))']]]])
        env.assertEqual(res[5][1], ['Final BM25 : words BM25 1.56 * document score 1.00 / slop 2',
                            [['(Weight 1.00 * children BM25 1.56)',
                            ['(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))',
                            '(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))']]]])
        env.assertEqual(res[8][1], ['Final BM25 : words BM25 1.56 * document score 0.10 / slop 3',
                            [['(Weight 1.00 * children BM25 1.56)',
                            ['(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))',
                            '(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))']]]])


def testDisMaxScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem').ok()
    env.expect('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem').ok()
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DISMAX')
    env.assertEqual(res[0], 3L)
    env.assertEqual(res[2][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])
    env.assertEqual(res[5][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])
    env.assertEqual(res[8][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])

def testScoreReplace(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create idx ON HASH schema f text').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'doc1', 'f', 'redisearch')
    conn.execute_command('HSET', 'doc1', 'f', 'redisearch')
    env.expect('FT.SEARCH idx redisearch withscores nocontent').equal([1L, 'doc1', '1'])
    conn.execute_command('HSET', 'doc1', 'f', 'redisearch')
    env.expect('FT.SEARCH idx redisearch withscores nocontent').equal([1L, 'doc1', '0'])
    if not env.isCluster:
        env.expect('ft.config set FORK_GC_CLEAN_THRESHOLD 0').ok()
        env.expect('ft.debug GC_FORCEINVOKE idx').equal('DONE')
        env.expect('FT.SEARCH idx redisearch withscores nocontent').equal([1L, 'doc1', '1'])

def testScoreDecimal(env):
    env.expect('ft.create idx ON HASH schema title text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add idx doc1 0.01 fields title hello').ok()
    res = env.cmd('ft.search idx hello withscores nocontent')
    env.assertLess(float(res[2]), 1)

def testScoreError(env):
    env.skipOnCluster()
    env.expect('ft.create idx ON HASH schema title text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add idx doc1 0.01 fields title hello').ok()
    env.expect('ft.search idx hello EXPLAINSCORE').error().contains('EXPLAINSCORE must be accompanied with WITHSCORES')
