import math
from time import sleep

from includes import *
from common import getConnectionByEnv, waitForIndex, server_version_at_least, skip


def testHammingScorer(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'PAYLOAD_FIELD', 'PAYLOAD', 'SCORE_FIELD', '__score', 'schema', 'title', 'text').ok()
    waitForIndex(env, 'idx')

    for i in range(16):
        res = conn.execute_command('hset', 'doc%d' % i, 'PAYLOAD', ('%x' % i) * 8, 'title', 'hello world')
        env.assertEqual(res, 2)

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

def testScoreIndex(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')

    N = 25
    for n in range(1, N):
        sc = math.sqrt(float(N - n + 10) / float(N + 10))
        conn.execute_command('HSET', 'doc%d' % n, '__score', sc, 'title', 'hello world '*n, 'body', 'lorem ipsum '*n)
    results_single = [
        [24, 'doc1', 1.97, 'doc2', 1.94, 'doc3', 1.91, 'doc4', 1.88, 'doc5', 1.85],
        [24, 'doc1', 0.9, 'doc2', 0.88, 'doc3', 0.87, 'doc4', 0.86, 'doc5', 0.84],
        [24, 'doc17', 0.73, 'doc18', 0.73, 'doc16', 0.72, 'doc19', 0.72, 'doc15', 0.72],
        [24, 'doc2', 0.08, 'doc3', 0.08, 'doc1', 0.08, 'doc4', 0.08, 'doc5', 0.08],
        [24, 'doc24', 480.0, 'doc23', 460.0, 'doc22', 440.0, 'doc21', 420.0, 'doc20', 400.0],
        [24, 'doc1', 0.99, 'doc2', 0.97, 'doc3', 0.96, 'doc4', 0.94, 'doc5', 0.93]
    ]
    # BM25 score computation is effected by the document's length and the average document length *in the local shard*.
    # Hence, we see differences in the score between single shard mode and cluster mode.
    results_cluster = [
        [24, 'doc1', 1.97, 'doc2', 1.94, 'doc3', 1.91, 'doc4', 1.88, 'doc5', 1.85],
        [24, 'doc1', 0.9, 'doc2', 0.88, 'doc3', 0.87, 'doc4', 0.86, 'doc5', 0.84],
        [24, 'doc17', 0.77, 'doc16', 0.77, 'doc13', 0.75, 'doc12', 0.73, 'doc18', 0.73],
        [24, 'doc4', 0.26, 'doc8', 0.25, 'doc11', 0.23, 'doc1', 0.23, 'doc5', 0.23],
        [24, 'doc24', 480.0, 'doc23', 460.0, 'doc22', 440.0, 'doc21', 420.0, 'doc20', 400.0],
        [24, 'doc1', 0.99, 'doc2', 0.97, 'doc3', 0.96, 'doc4', 0.94, 'doc5', 0.93],
    ]
    scorers = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'BM25STD', 'DISMAX', 'DOCSCORE']
    expected_results = results_cluster if env.shardsCount > 1 else results_single
    for _ in env.reloading_iterator():
        waitForIndex(env, 'idx')
        for i, scorer in enumerate(scorers):
            res = env.cmd('ft.search', 'idx', 'hello world', 'scorer', scorer, 'nocontent', 'withscores', 'limit', 0, 5)
            res = [round(float(x), 2) if j > 0 and (j - 1) %
                   2 == 1 else x for j, x in enumerate(res)]
            env.assertListEqual(expected_results[i], res)


def testDocscoreScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem').ok()
    env.expect('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem').ok()
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DOCSCORE')
    env.assertEqual(res[0], 3)
    env.assertEqual(res[2][1], "Document's score is 1.00")
    env.assertEqual(res[5][1], "Document's score is 0.50")
    env.assertEqual(res[8][1], "Document's score is 0.10")

def testTFIDFScorerExplanation(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')

    env.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum')
    env.execute_command('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem')
    env.execute_command('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem')

    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE')
    env.assertEqual(res[0], 3)
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
    res = res1 if server_version_at_least(env, "6.2.0") else res2
    env.assertEqual(actual_res[2][1], res)

def testBM25ScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem').ok()
    env.expect('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem').ok()
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25')
    env.assertEqual(res[0], 3)
    if env.isCluster():
        env.assertContains('Final BM25', res[2][1][0])
        env.assertContains('Final BM25', res[5][1][0])
        env.assertContains('Final BM25', res[8][1][0])
    else:
        env.assertEqual(res[2][1], ['Final BM25 : words BM25 0.70 * document score 0.50 / slop 1',
                            [['(Weight 1.00 * children BM25 0.70)',
                            ['(0.35 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                            '(0.35 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])
        env.assertEqual(res[5][1], ['Final BM25 : words BM25 0.70 * document score 1.00 / slop 2',
                            [['(Weight 1.00 * children BM25 0.70)',
                            ['(0.35 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                            '(0.35 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])
        env.assertEqual(res[8][1], ['Final BM25 : words BM25 0.70 * document score 0.10 / slop 3',
                            [['(Weight 1.00 * children BM25 0.70)',
                            ['(0.35 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                            '(0.35 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])


def testBM25STDScorerExplanation(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'doc1', 'title', 'hello world', 'body', 'lorem ist ipsum')
    conn.execute_command('HSET', 'doc2', 'title', 'hello space world', 'body', 'lorem ist ipsum lorem lorem')
    conn.execute_command('HSET', 'doc3', 'title', 'hello more space world', 'body', 'lorem ist ipsum lorem lorem')
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD')
    env.assertEqual(res[0], 3)
    if env.isCluster():
        env.assertEqual(res[2][1],  ['Final BM25 : words BM25 1.13 * document score 1.00',
                                     [['(Weight 1.00 * children BM25 1.13)',
                                       ['hello: (0.57 = IDF 0.29 * (F 10.00 * (k1 1.2 + 1)) /'
                                        ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 23 / Average Doc Len 23.00)))',
                                        'world: (0.57 = IDF 0.29 * (F 10.00 * (k1 1.2 + 1)) / '
                                        '(F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 23 / Average Doc Len 23.00)))']]]])
        env.assertEqual(res[5][1], ['Final BM25 : words BM25 0.72 * document score 1.00',
                                    [['(Weight 1.00 * children BM25 0.72)',
                                      ['hello: (0.36 = IDF 0.18 * (F 10.00 * (k1 1.2 + 1)) /'
                                       ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 35 / Average Doc Len 40.00)))',
                                       'world: (0.36 = IDF 0.18 * (F 10.00 * (k1 1.2 + 1)) /'
                                       ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 35 / Average Doc Len 40.00)))']]]])
        env.assertEqual(res[8][1],  ['Final BM25 : words BM25 0.71 * document score 1.00',
                                     [['(Weight 1.00 * children BM25 0.71)',
                                       ['hello: (0.36 = IDF 0.18 * (F 10.00 * (k1 1.2 + 1)) /'
                                        ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 45 / Average Doc Len 40.00)))',
                                        'world: (0.36 = IDF 0.18 * (F 10.00 * (k1 1.2 + 1)) /'
                                        ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 45 / Average Doc Len 40.00)))']]]])
    else:
        env.assertEqual(res[2][1], ['Final BM25 : words BM25 0.53 * document score 1.00',
                                   [['(Weight 1.00 * children BM25 0.53)',
                                     ['hello: (0.27 = IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                      ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 23 / Average Doc Len 34.33)))',
                                      'world: (0.27 = IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                      ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 23 / Average Doc Len 34.33)))']]]])
        env.assertEqual(res[5][1],  ['Final BM25 : words BM25 0.52 * document score 1.00',
                                     [['(Weight 1.00 * children BM25 0.52)',
                                       ['hello: (0.26 = IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                        ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 35 / Average Doc Len 34.33)))',
                                        'world: (0.26 = IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                        ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 35 / Average Doc Len 34.33)))']]]] )
        env.assertEqual(res[8][1],  ['Final BM25 : words BM25 0.52 * document score 1.00',
                                     [['(Weight 1.00 * children BM25 0.52)',
                                       ['hello: (0.26 = IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                        ' (F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 45 / Average Doc Len 34.33)))',
                                        'world: (0.26 = IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / '
                                        '(F 10.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 45 / Average Doc Len 34.33)))']]]])


@skip(cluster=True)
def testOptionalAndWildcardScoring(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'schema', 'title', 'text').ok()

    conn.execute_command('HSET', 'doc1', 'title', 'some text here')
    conn.execute_command('HSET', 'doc2', 'title', 'some text more words here')

    expected_res = [2, 'doc2', '0.8195877903737075', 'doc1', '0.19566220141314736']

    # Validate that optional term contributes the scoring only in documents in which it appears.
    res = conn.execute_command('ft.search', 'idx', 'text ~words', 'withscores', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)
    res = conn.execute_command('ft.search', 'idx', 'text | ~words', 'withscores', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)

    expected_res = [2, 'doc1', ['1.073170733125631',
                                ['Final BM25 : words BM25 1.07 * document score 1.00',
                                 ['*: (1.07 = IDF 1.00 * (F 1.00 * (k1 1.2 + 1)) /'
                                  ' (F 1.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 3 / Average Doc Len 4.00)))']]],
                       'doc2', ['0.936170211686652',
                                ['Final BM25 : words BM25 0.94 * document score 1.00',
                                 ['*: (0.94 = IDF 1.00 * (F 1.00 * (k1 1.2 + 1)) /'
                                  ' (F 1.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 5 / Average Doc Len 4.00)))']]]]
    res = conn.execute_command('ft.search', 'idx', '*', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)

    expected_res = [1, 'doc2', ['0.6489037427548099',
                                ['Final BM25 : words BM25 0.65 * document score 1.00',
                                 [['(Weight 1.00 * children BM25 0.65)',
                                   ['words: (0.65 = IDF 0.69 * (F 1.00 * (k1 1.2 + 1)) '
                                    '/ (F 1.00 + k1 1.2 * (1 - b 0.5 + b 0.5 * Doc Len 5 / Average Doc Len 4.00)))']]]]]]
    res = conn.execute_command('ft.search', 'idx', '*ds', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)


def testDisMaxScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum').ok()
    env.expect('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem').ok()
    env.expect('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem').ok()
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DISMAX')
    env.assertEqual(res[0], 3)
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
    env.expect('FT.SEARCH idx redisearch withscores nocontent').equal([1, 'doc1', '1'])
    conn.execute_command('HSET', 'doc1', 'f', 'redisearch')
    env.expect('FT.SEARCH idx redisearch withscores nocontent').equal([1, 'doc1', '0'])
    if not env.isCluster:
        env.expect('ft.config set FORK_GC_CLEAN_THRESHOLD 0').ok()
        env.expect('ft.debug GC_FORCEINVOKE idx').equal('DONE')
        env.expect('FT.SEARCH idx redisearch withscores nocontent').equal([1, 'doc1', '1'])

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
