import math
from includes import *


def testHammingScorer(env):
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 'title', 'text'))

    for i in range(16):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1,
                                        'payload', ('%x' % i) * 8,
                                        'fields', 'title', 'hello world'))
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
    env.assertOk(env.cmd(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10, 'body', 'text'))
    N = 25
    for n in range(N):

        sc = math.sqrt(float(N - n + 10) / float(N + 10))
        # print n, sc

        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % n, sc, 'fields',
                               'title', 'hello world ' * n, 'body', 'lorem ipsum ' * n))
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
        [24L, 'doc5', 1.76, 'doc4', 1.75, 'doc6',
            1.74, 'doc3', 1.74, 'doc7', 1.72],
        [24L, 'doc24', 480.0, 'doc23', 460.0, 'doc22',
            440.0, 'doc21', 420.0, 'doc20', 400.0],
        [24L, 'doc1', 0.99, 'doc2', 0.97, 'doc3',
            0.96, 'doc4', 0.94, 'doc5', 0.93],
    ]

    scorers = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'DISMAX', 'DOCSCORE']
    expected_results = results_cluster if env.is_cluster() else results_single

    for _ in env.reloading_iterator():
        for i, scorer in enumerate(scorers):
            res = env.cmd('ft.search', 'idx', 'hello world', 'scorer',
                              scorer, 'nocontent', 'withscores', 'limit', 0, 5)
            res = [round(float(x), 2) if j > 0 and (j - 1) %
                   2 == 1 else x for j, x in enumerate(res)]
            #print res
            env.assertListEqual(expected_results[i], res)

def testDocscoreScorerExplanation(env):
    env.assertOk(env.cmd(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10, 'body', 'text'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem'))    
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DOCSCORE')
    env.assertEqual(res[0], 3L)
    env.assertEqual(res[2][1], "Document's score is 1.00")
    env.assertEqual(res[5][1], "Document's score is 0.50")
    env.assertEqual(res[8][1], "Document's score is 0.10")

def testTFIDFScorerExplanation(env):
    env.assertOk(env.cmd(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10, 'body', 'text'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem'))    
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE')
    env.assertEqual(res[0], 3L)
    env.assertEqual(res[2][1],['Final TFIDF : words TFIDF 20.00 * document score 1.00 / norm 10 / slop 2',
                                [['(Weight 1.00 * total children TFIDF 20.00)',
                                ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]])
    env.assertEqual(res[5][1], ['Final TFIDF : words TFIDF 20.00 * document score 0.50 / norm 10 / slop 1',
                                [['(Weight 1.00 * total children TFIDF 20.00)',
                                ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]])
    env.assertEqual(res[8][1], ['Final TFIDF : words TFIDF 20.00 * document score 0.10 / norm 10 / slop 3',
                                [['(Weight 1.00 * total children TFIDF 20.00)',
                                ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]])

def testBM25ScorerExplanation(env):
    env.assertOk(env.cmd(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10, 'body', 'text'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem'))    
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25')
    env.assertEqual(res[0], 3L)
    if env.isCluster():
        env.assertContains('Final BM25', res[2][1][0])
        env.assertContains('Final BM25', res[5][1][0])
        env.assertContains('Final BM25', res[8][1][0])
    else:
        env.assertEqual(res[2][1], ['Final BM25 : words BM25 1.56 * document score 1.00 / slop 2',
                            [['(Weight 1.00 * children BM25 1.56)',
                            ['(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))',
                            '(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))']]]])
        env.assertEqual(res[5][1], ['Final BM25 : words BM25 1.56 * document score 0.50 / slop 1',
                            [['(Weight 1.00 * children BM25 1.56)',
                            ['(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))',
                            '(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))']]]])
        env.assertEqual(res[8][1], ['Final BM25 : words BM25 1.56 * document score 0.10 / slop 3',
                            [['(Weight 1.00 * children BM25 1.56)',
                            ['(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))',
                            '(0.78 = IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 3.67)))']]]])


def testDisMaxScorerExplanation(env):
    env.assertOk(env.cmd(
        'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10, 'body', 'text'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem'))
    env.assertOk(env.cmd(
        'ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem'))    
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DISMAX')
    env.assertEqual(res[0], 3L)
    env.assertEqual(res[2][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])
    env.assertEqual(res[5][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])
    env.assertEqual(res[8][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])
