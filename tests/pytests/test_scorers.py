import math
from time import sleep
from includes import *
from common import *

DEFAULT_SCORE_NORM_STRETCH_FACTOR = 4

def testHammingScorer(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'PAYLOAD_FIELD', 'PAYLOAD', 'SCORE_FIELD', '__score', 'schema', 'title', 'text').ok()
    waitForIndex(env, 'idx')

    for i in range(16):
        res = conn.execute_command('hset', 'doc%d' % i, 'PAYLOAD', (f'{i:x}') * 8, 'title', 'hello world')
        env.assertEqual(res, 2)

    for i in range(16):
        res = env.cmd('ft.search', 'idx', '*', 'PAYLOAD', (f'{i:x}') * 8,
                      'SCORER', 'HAMMING', 'WITHSCORES', 'WITHPAYLOADS')
        env.assertEqual(res[1], 'doc%d' % i)
        env.assertEqual(res[2], '1')
        # test with payload of different length
        res = env.cmd('ft.search', 'idx', '*', 'PAYLOAD', (f'{i:x}') * 7,
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
        [24, 'doc1', 0.08, 'doc2', 0.08, 'doc3', 0.08, 'doc4', 0.08, 'doc5', 0.08],
        [24, 'doc1', 0.02, 'doc2', 0.02, 'doc3', 0.02, 'doc4', 0.02, 'doc5', 0.02],
        [24, 'doc24', 480.0, 'doc23', 460.0, 'doc22', 440.0, 'doc21', 420.0, 'doc20', 400.0],
        [24, 'doc1', 0.99, 'doc2', 0.97, 'doc3', 0.96, 'doc4', 0.94, 'doc5', 0.93]
    ]
    # BM25 score computation is effected by the document's length and the average document length *in the local shard*.
    # Hence, we see differences in the score between single shard mode and cluster mode.
    results_cluster = [
        [24, 'doc1', 1.97, 'doc2', 1.94, 'doc3', 1.91, 'doc4', 1.88, 'doc5', 1.85],
        [24, 'doc1', 0.9, 'doc2', 0.88, 'doc3', 0.87, 'doc4', 0.86, 'doc5', 0.84],
        [24, 'doc17', 0.77, 'doc16', 0.77, 'doc13', 0.75, 'doc12', 0.73, 'doc18', 0.73],
        [24, 'doc4', 0.26, 'doc8', 0.25, 'doc1', 0.24, 'doc11', 0.23, 'doc5', 0.23],
        [24, 'doc4', 0.07, 'doc8', 0.06, 'doc1', 0.06, 'doc11', 0.06, 'doc5', 0.06],
        [24, 'doc24', 480.0, 'doc23', 460.0, 'doc22', 440.0, 'doc21', 420.0, 'doc20', 400.0],
        [24, 'doc1', 0.99, 'doc2', 0.97, 'doc3', 0.96, 'doc4', 0.94, 'doc5', 0.93],
    ]
    scorers = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'BM25STD', 'BM25STD.TANH', 'DISMAX', 'DOCSCORE']
    expected_results = results_cluster if env.shardsCount > 1 else results_single
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        for i, scorer in enumerate(scorers):
            res = env.cmd('ft.search', 'idx', 'hello world', 'scorer', scorer, 'nocontent', 'withscores', 'limit', 0, 5)
            res = [round(float(x), 2) if j > 0 and (j - 1) %
                   2 == 1 else x for j, x in enumerate(res)]
            env.assertEqual(expected_results[i], res, message=scorers[i])

def testDocscoreScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem'))
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DOCSCORE')
    env.assertEqual(res[0], 3)
    env.assertEqual(res[2][1], "Document's score is 1.00")
    env.assertEqual(res[5][1], "Document's score is 0.50")
    env.assertEqual(res[8][1], "Document's score is 0.10")

def testTFIDFScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    dialect = int(env.cmd(config_cmd(), 'GET', 'DEFAULT_DIALECT')[0][1])

    con = env.getClusterConnectionIfNeeded()
    con.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum')
    con.execute_command('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem')
    con.execute_command('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem')

    res = env.cmd('ft.search', 'idx', 'hello world', 'SCORER', 'TFIDF', 'WITHSCORES', 'EXPLAINSCORE')
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

    res = env.cmd('ft.search', 'idx', 'hello(world(world))', 'SCORER', 'TFIDF', 'WITHSCORES', 'EXPLAINSCORE', 'LIMIT', 0, 1)
    dialect1 = ['Final TFIDF : words TFIDF 30.00 * document score 0.50 / norm 10 / slop 1',
                                [['(Weight 1.00 * total children TFIDF 30.00)',
                                  [['(Weight 1.00 * total children TFIDF 20.00)',
                                    ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                     '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']],
                                   '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]
    dialect1_coord =['Final TFIDF : words TFIDF 30.00 * document score 0.50 / norm 10 / slop 1', [[
                                '(Weight 1.00 * total children TFIDF 30.00)', [
                                  '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)', [
                                    '(Weight 1.00 * total children TFIDF 20.00)', [
                                      '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                      '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]]]
    dialect2 = ['Final TFIDF : words TFIDF 30.00 * document score 0.50 / norm 10 / slop 1',
                                [['(Weight 1.00 * total children TFIDF 30.00)', [
                                    '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                    '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                                    '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]
    env.assertEqual(res[2][1], dialect2 if dialect != 1 else dialect1_coord if env.isCluster() else dialect1)

    res1 = ['Final TFIDF : words TFIDF 40.00 * document score 1.00 / norm 10 / slop 1',
            [['(Weight 1.00 * total children TFIDF 40.00)',
              [['(Weight 1.00 * total children TFIDF 30.00)',
                [['(Weight 1.00 * total children TFIDF 20.00)',
                  ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
                   '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']],
                 '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']],
               '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]
    res2 = ['Final TFIDF : words TFIDF 40.00 * document score 1.00 / norm 10 / slop 1',
            [['(Weight 1.00 * total children TFIDF 40.00)',
              [['(Weight 1.00 * total children TFIDF 30.00)',
                ['(Weight 1.00 * total children TFIDF 20.00)',
                 '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']],
               '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]
    res3 = ['Final TFIDF : words TFIDF 40.00 * document score 0.50 / norm 10 / slop 1',
            [['(Weight 1.00 * total children TFIDF 40.00)',
              ['(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
               '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
               '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)',
               '(TFIDF 10.00 = Weight 1.00 * TF 10 * IDF 1.00)']]]]

    actual_res = env.cmd('ft.search', 'idx', 'hello(world(world(hello)))', 'SCORER', 'TFIDF', 'withscores', 'EXPLAINSCORE', 'limit', 0, 1)
    # on older versions we trim the reply to remain under the 7-layer limitation.
    res = res3 if dialect != 1 else res1 if server_version_at_least(env, "6.2.0") else res2
    env.assertEqual(actual_res[2][1], res)

def testBM25ScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1{hash_tag}', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc2{hash_tag}', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc3{hash_tag}', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem'))
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25')
    env.assertEqual(res[0], 3)

    env.assertEqual(res[2][1], ['Final BM25 : words BM25 0.70 * document score 0.50 / slop 1',
                        [['(Weight 1.00 * children BM25 0.70)',
                        ['(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                        '(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])
    env.assertEqual(res[5][1], ['Final BM25 : words BM25 0.70 * document score 1.00 / slop 2',
                        [['(Weight 1.00 * children BM25 0.70)',
                        ['(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                        '(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])
    env.assertEqual(res[8][1], ['Final BM25 : words BM25 0.70 * document score 0.10 / slop 3',
                        [['(Weight 1.00 * children BM25 0.70)',
                        ['(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                        '(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])

    res = env.cmd('ft.search', 'idx', '((@title:(hello => {$weight: 0.5;}|world) => {$weight: 0.7;}) => {$weight: 0.3;})', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25', 'nocontent')

    env.assertEqual(res[2][1],['Final BM25 : words BM25 0.16 * document score 0.50 / slop 1',
                                [['(Weight 0.30 * children BM25 0.52)',
                                ['(0.17 = Weight 0.50 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                                    '(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])
    env.assertEqual(res[4][1],['Final BM25 : words BM25 0.16 * document score 1.00 / slop 2',
                                [['(Weight 0.30 * children BM25 0.52)',
                                ['(0.17 = Weight 0.50 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                                    '(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])
    env.assertEqual(res[6][1], ['Final BM25 : words BM25 0.16 * document score 0.10 / slop 3',
                                [['(Weight 0.30 * children BM25 0.52)',
                                ['(0.17 = Weight 0.50 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))',
                                    '(0.35 = Weight 1.00 * IDF 1.00 * F 10 / (F 10 + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len 30.00)))']]]])

def testBM25STDScorerExplanation(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'doc1{hash_tag}', 'title', 'hello world', 'body', 'lorem ist ipsum')
    conn.execute_command('HSET', 'doc2{hash_tag}', 'title', 'hello space world', 'body', 'lorem ist ipsum lorem lorem')
    conn.execute_command('HSET', 'doc3{hash_tag}', 'title', 'hello more space world', 'body', 'lorem ist ipsum lorem lorem')
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD')
    env.assertEqual(res[0], 3)

    env.assertEqual(res[2][1], ['Final BM25 : words BM25 0.54 * document score 1.00',
                                [['(Weight 1.00 * children BM25 0.54)',
                                    ['hello: (0.27 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))',
                                    'world: (0.27 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))']]]])
    env.assertEqual(res[5][1],  ['Final BM25 : words BM25 0.52 * document score 1.00',
                                    [['(Weight 1.00 * children BM25 0.52)',
                                    ['hello: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))']]]] )
    env.assertEqual(res[8][1],  ['Final BM25 : words BM25 0.51 * document score 1.00',
                                    [['(Weight 1.00 * children BM25 0.51)',
                                    ['hello: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / '
                                    '(F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))']]]])

    res = env.cmd('ft.search', 'idx', '((@title:(hello => {$weight: 0.5;}|world) => {$weight: 0.7;}) => {$weight: 0.3;})', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD', 'nocontent')


    env.assertEqual(res[2][1],['Final BM25 : words BM25 0.12 * document score 1.00',
                                [['(Weight 0.30 * children BM25 0.40)',
                                    ['hello: (0.13 = Weight 0.50 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))',
                                    'world: (0.27 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))']]]])

    env.assertEqual(res[4][1],['Final BM25 : words BM25 0.12 * document score 1.00',
                                [['(Weight 0.30 * children BM25 0.39)',
                                ['hello: (0.13 = Weight 0.50 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))']]]])
    env.assertEqual(res[6][1], ['Final BM25 : words BM25 0.12 * document score 1.00',
                                [['(Weight 0.30 * children BM25 0.38)',
                                ['hello: (0.13 = Weight 0.50 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))']]]])

def testBM25STDNORMScorerExplanation(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'doc1{hash_tag}', 'title', 'hello world', 'body', 'lorem ist ipsum')
    conn.execute_command('HSET', 'doc2{hash_tag}', 'title', 'hello space world', 'body', 'lorem ist ipsum lorem lorem')
    conn.execute_command('HSET', 'doc3{hash_tag}', 'title', 'hello more space world', 'body', 'lorem ist ipsum lorem lorem')
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD.NORM')
    env.assertEqual(res[0], 3)

    env.assertEqual(res[2][1], ['Final BM25STD.NORM: 1.00 = Original Score: 0.54 / Max Score: 0.54',
                                [['(Weight 1.00 * children BM25 0.54)',
                                    ['hello: (0.27 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))',
                                    'world: (0.27 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))']]]])
    env.assertEqual(res[5][1],  ['Final BM25STD.NORM: 0.97 = Original Score: 0.52 / Max Score: 0.54',
                                    [['(Weight 1.00 * children BM25 0.52)',
                                    ['hello: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))']]]] )
    env.assertEqual(res[8][1],  ['Final BM25STD.NORM: 0.95 = Original Score: 0.51 / Max Score: 0.54',
                                    [['(Weight 1.00 * children BM25 0.51)',
                                    ['hello: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) /'
                                    ' (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / '
                                    '(F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))']]]])

    res = env.cmd('ft.search', 'idx', '((@title:(hello => {$weight: 0.5;}|world) => {$weight: 0.7;}) => {$weight: 0.3;})', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD.NORM', 'nocontent')

    # The result of 0.12 divided by 0.12 can be a little different due to number accuracy limits
    env.assertEqual(res[2][1],['Final BM25STD.NORM: 1.00 = Original Score: 0.12 / Max Score: 0.12',
                                [['(Weight 0.30 * children BM25 0.40)',
                                    ['hello: (0.13 = Weight 0.50 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))',
                                    'world: (0.27 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 23 / Average Doc Len 34.33)))']]]])

    env.assertEqual(res[4][1],['Final BM25STD.NORM: 0.97 = Original Score: 0.12 / Max Score: 0.12',
                                [['(Weight 0.30 * children BM25 0.39)',
                                ['hello: (0.13 = Weight 0.50 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 35 / Average Doc Len 34.33)))']]]])
    env.assertEqual(res[6][1], ['Final BM25STD.NORM: 0.95 = Original Score: 0.12 / Max Score: 0.12',
                                [['(Weight 0.30 * children BM25 0.38)',
                                ['hello: (0.13 = Weight 0.50 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))',
                                    'world: (0.26 = Weight 1.00 * IDF 0.13 * (F 10.00 * (k1 1.2 + 1)) / (F 10.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 45 / Average Doc Len 34.33)))']]]])



@skip(cluster=True)
def testOptionalAndWildcardScoring(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 'schema', 'title', 'text').ok()
    env.expect('ft.create', 'idx_opt', 'INDEXALL', 'ENABLE', 'schema', 'title', 'text').ok()

    conn.execute_command('HSET', 'doc1', 'title', 'some text here')
    conn.execute_command('HSET', 'doc2', 'title', 'some text more words here')

    expected_res = [2, 'doc2', '0.7942396779178669', 'doc1', '0.203092367479523']

    # Validate that optional term contributes the scoring only in documents in which it appears.
    res = conn.execute_command('ft.search', 'idx', 'text ~more', 'withscores', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)
    res = conn.execute_command('ft.search', 'idx', 'text | ~more', 'withscores', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)

    # Check the same for the optimized version
    res = conn.execute_command('ft.search', 'idx_opt', 'text ~more', 'withscores', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)
    res = conn.execute_command('ft.search', 'idx_opt', 'text | ~more', 'withscores', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)

    expected_res = [2, 'doc1', ['1.1139240529250303',
                                ['Final BM25 : words BM25 1.11 * document score 1.00',
                                 ['*: (1.11 = Weight 1.00 * IDF 1.00 * (F 1.00 * (k1 1.2 + 1)) /'
                                  ' (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 4.00)))']]],
                       'doc2', ['0.9072164933249958',
                                ['Final BM25 : words BM25 0.91 * document score 1.00',
                                 ['*: (0.91 = Weight 1.00 * IDF 1.00 * (F 1.00 * (k1 1.2 + 1)) /'
                                  ' (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 5 / Average Doc Len 4.00)))']]]]
    res = conn.execute_command('ft.search', 'idx', '*', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)

    expected_res = [1, 'doc2', ['0.6288345545057012',
                                ['Final BM25 : words BM25 0.63 * document score 1.00',
                                 ['words: (0.63 = Weight 1.00 * IDF 0.69 * (F 1.00 * (k1 1.2 + 1)) '
                                    '/ (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 5 / Average Doc Len 4.00)))']]]]
    res = conn.execute_command('ft.search', 'idx', '*ds', 'withscores', 'EXPLAINSCORE', 'scorer', 'BM25STD', 'nocontent')
    env.assertEqual(res, expected_res)

def testDisMaxScorerExplanation(env):
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCORE_FIELD', '__score',
               'schema', 'title', 'text', 'weight', 10, 'body', 'text').ok()
    waitForIndex(env, 'idx')
    con = env.getClusterConnectionIfNeeded()
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields', 'title', 'hello world',' body', 'lorem ist ipsum'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc2', 1, 'fields', 'title', 'hello another world',' body', 'lorem ist ipsum lorem lorem'))
    env.assertOk(con.execute_command('ft.add', 'idx', 'doc3', 0.1, 'fields', 'title', 'hello yet another world',' body', 'lorem ist ipsum lorem lorem'))
    res = env.cmd('ft.search', 'idx', 'hello world', 'withscores', 'EXPLAINSCORE', 'scorer', 'DISMAX')
    env.assertEqual(res[0], 3)
    env.assertEqual(res[2][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])
    env.assertEqual(res[5][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])
    env.assertEqual(res[8][1], ['20.00 = Weight 1.00 * children DISMAX 20.00',
            ['DISMAX 10.00 = Weight 1.00 * Frequency 10', 'DISMAX 10.00 = Weight 1.00 * Frequency 10']])

def testScoreDecimal(env):
    env.expect('ft.create idx ON HASH schema title text').ok()
    waitForIndex(env, 'idx')
    env.assertOk(env.getClusterConnectionIfNeeded().execute_command('ft.add', 'idx', 'doc1', '0.01', 'fields', 'title', 'hello'))
    res = env.cmd('ft.search idx hello withscores nocontent')
    env.assertLess(float(res[2]), 1)

@skip(cluster=True)
def testScoreError(env):
    env.expect('ft.create idx ON HASH schema title text').ok()
    waitForIndex(env, 'idx')
    env.expect('ft.add idx doc1 0.01 fields title hello').ok()
    env.expect('ft.search idx hello EXPLAINSCORE').error().contains('EXPLAINSCORE must be accompanied with WITHSCORES')

def _test_expose_score(env, idx):
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc1', 'title', 'hello')

    # MOD-8060 - `SCORER` should propagate to the shards on `FT.AGGREGATE` (cluster mode)
    # Test with default scorer (BM25STD)
    expected = [1, ['__score', '0.287682072452']]
    env.expect('FT.AGGREGATE', idx, '~hello', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC').equal(expected)
    # Test with explicit BM25STD scorer
    env.expect('FT.AGGREGATE', idx, '~hello', 'SCORER', 'BM25STD', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC').equal(expected)
    # Test with explicit TFIDF scorer
    expected = [1, ['__score', str(1)]] # TFIDF score (different from BM25STD)
    env.expect('FT.AGGREGATE', idx, '~hello', 'SCORER', 'TFIDF', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC').equal(expected)

    conn.execute_command('HSET', 'doc2', 'title', 'world')

    doc1_score = 0.287682072452 if env.isCluster() else 0.69314718056

    expected = [2, ['__score', str(doc1_score)], ['__score', '0']]
    env.expect('FT.AGGREGATE', idx, '~hello', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC').equal(expected)

    expected = [1, ['count', '1']]
    env.expect('FT.AGGREGATE', idx, '~hello', 'ADDSCORES', 'FILTER', '@__score > 0', 'GROUPBY', 0, 'REDUCE', 'COUNT', '0', 'AS', 'count').equal(expected)

    env.expect('FT.SEARCH', idx, '~hello', 'ADDSCORES').error().equal('SEARCH_PARSE_ARGS ADDSCORES is not supported on FT.SEARCH')

def testExposeScore(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    _test_expose_score(env, 'idx')

def testExposeScoreOptimized(env: Env):
    env.expect('FT.CREATE', 'idxOpt', 'INDEXALL', 'ENABLE', 'SCHEMA', 'title', 'TEXT').ok()
    _test_expose_score(env, 'idxOpt')

def _prepare_index(env, idx):
    """Prepares the index for the score tests"""

    conn = env.getClusterConnectionIfNeeded()

    # Create an index
    env.expect('FT.CREATE', idx, 'SCHEMA', 'title', 'TEXT').ok()

    # We are going to search against
    # We currently use a hash-tag such that all docs will reside on the same shard
    # such that we will not get a score difference between the standalone and cluster modes
    conn.execute_command('HSET', 'doc1{tag}', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2{tag}', 'title', 'hello space world')
    conn.execute_command('HSET', 'doc3{tag}', 'title', 'hello more space world')

def testNormalizedBM25Tanh():
    """
    Tests that the normalized BM25 scorer works as expected.
    We apply the stretched tanh function to the BM25 score, reaching a normalized
    value between 0 and 1.
    We also test that we maintain differentiability between the scores for all
    possible stretch factors.
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # Prepare the index
    _prepare_index(env, 'idx')

    def testNormScore(env, stretch_factor, query_param=False):
        """
        Tests the normalized BM25 scorer with the given stretch factor.
        """
        # Search for `hello world` and get the scores using the BM25STD scorer
        search_cmd = ['FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD']
        res = env.cmd(*search_cmd)

        # Search for the same query and get the scores using the BM25STD.TANH scorer
        norm_search_cmd = ['FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.TANH']
        # Add the query param if needed
        if query_param:
            norm_search_cmd += ['BM25STD_TANH_FACTOR', str(stretch_factor)]
        norm_res_search = env.cmd(*norm_search_cmd)
        env.assertEqual(len(res), len(norm_res_search), message=str(stretch_factor))
        norm_scores_raw = []
        norm_scores = []
        for i in range(1, len(res), 2):
            # The same order should be kept
            env.assertEqual(res[i], norm_res_search[i], message=str(stretch_factor))
            # The score should be normalized using the stretched tanh function
            env.assertEqual(round(float(norm_res_search[i+1]), 5), round(math.tanh(float(res[i+1]) * (1/stretch_factor)), 5), message=str(stretch_factor))
            # Save the score to make sure the aggregate command returns the same results
            norm_scores.append(round(float(norm_res_search[i+1]), 5))
            norm_scores_raw.append(float(norm_res_search[i+1]))

        agg_cmd = ['FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD.TANH', 'SORTBY', '2', '@__score', 'DESC']
        if query_param:
            agg_cmd += ['BM25STD_TANH_FACTOR', str(stretch_factor)]
        norm_res_aggregate = env.cmd(*agg_cmd)
        for i, single_res in enumerate(norm_res_aggregate[1:]):
            # Check that the order and the scores are the same
            env.assertEqual(round(float(single_res[1]), 5), norm_scores[i], message=str(stretch_factor))

        # The scores of the different documents should be different
        env.assertEqual(len(set(norm_scores_raw)), 3, message=str(stretch_factor))

    testNormScore(env, DEFAULT_SCORE_NORM_STRETCH_FACTOR)

    # Do the same with a different stretch factor
    stretch_factor = 20
    env.assertEqual(
        run_command_on_all_shards(env, "config", "set", "search-bm25std-tanh-factor", str(stretch_factor)),
        ["OK"] * env.shardsCount,
        message=str(stretch_factor)
    )
    testNormScore(env, stretch_factor)

    # And with a very large stretch factor (the largest we currently allow), given
    # as a query parameter
    stretch_factor = 10000
    testNormScore(env, stretch_factor, query_param=True)


class TestBM25NormMax:
    """
    Scores are normalized by dividing each score by the maximum score in the result set.
    This result processor calculates the maximum score by accumulating all of its upstream results.
    This means that other result processors such as LIMIT, or cursor usage do not affect the score normalization.
    """
    def create_and_fill_index(self, use_key_tags=False):
        # Prepare the index with documents having different scores
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'body', 'TEXT').ok()
        conn = self.env.getClusterConnectionIfNeeded()
        # Add documents with varying content to get different scores
        tag = '{tag}' if use_key_tags else ''
        conn.execute_command('HSET', f'doc:5{tag}', 'title', 'hello world orange yellow blue')
        conn.execute_command('HSET', f'doc:4{tag}', 'title', 'hello world orange yellow')
        conn.execute_command('HSET', f'doc:3{tag}', 'title', 'hello world orange')
        conn.execute_command('HSET', f'doc:2{tag}', 'title', 'hello world')
        conn.execute_command('HSET', f'doc:1{tag}', 'title', 'hello')

    def setUp(self):
        self.env = Env(moduleArgs='DEFAULT_DIALECT 2')
        self.create_and_fill_index(use_key_tags=self.env.isCluster())

    def tearDown(self): # cleanup after each test
        self.env.flush()

    def test_bm25std_normalization_correctness(self):
        # Run both SEARCH and AGGREGATE with BM25STD and BM25STD.NORM
        res_std = self.env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD')
        res_norm = self.env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.NORM')

        keys_std = [res_std[i] for i in range(1, len(res_std), 2)]
        keys_norm = [res_norm[i] for i in range(1, len(res_norm), 2)]
        self.env.assertEqual(keys_std, keys_norm)

        max_std = max(float(res_std[i]) for i in range(2, len(res_std), 2))
        max_norm = max(float(res_norm[i]) for i in range(2, len(res_norm), 2))
        # Since no documents are excluded and the query does not have any 'not' clause, maximum score should be 1.0
        self.env.assertAlmostEqual(max_norm, 1.0, 0.00001)

        for i in range(2, len(res_norm), 2):
            expected = float(res_std[i]) / max_std
            actual = float(res_norm[i])
            self.env.assertAlmostEqual(actual, expected, 0.00001)
            self.env.assertGreaterEqual(actual, 0.0)
            self.env.assertLessEqual(actual, 1.0)

        # AGGREGATE version
        agg_std = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD',
                          'LOAD', '2', '@__key', '@__score')
        agg_norm = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD.NORM',
                           'LOAD', '2', '@__key', '@__score')

        key_index = agg_std[1].index('__key') + 1
        score_index = agg_std[1].index('__score') + 1
        scores_std = {row[key_index]: float(row[score_index]) for row in agg_std[1:]}
        max_score = max(scores_std.values())
        for row in agg_norm[1:]:
            norm_score = float(row[score_index])
            self.env.assertAlmostEqual(norm_score, scores_std[row[key_index]] / max_score, 0.00001)

    def test_limit_behavior(self):
        # Retrieve the second and third results from a query with four results, and verify their scores match in both full and restricted queries
        offset = 1
        limit = 2
        full = self.env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'SCORER', 'BM25STD.NORM', 'NOCONTENT')
        limited = self.env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'SCORER', 'BM25STD.NORM',
                          'NOCONTENT', 'LIMIT', offset, limit)

        num_results = lambda response: (len(response) - 1) / 2
        self.env.assertEqual(limited[0], 4)
        self.env.assertEqual(num_results(limited), 2)
        self.env.assertEqual(full[0], 4)

        for i in range(1, len(limited), 2):
            self.env.assertAlmostEqual(float(limited[i+1]), float(full[2*offset +i+1]), 0.00001)

        # AGGREGATE version
        agg_full = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD.NORM',
                           'LOAD', '2', '@__key', '@__score', 'SORTBY', '2', '@__score', 'DESC')
        agg_limited = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD.NORM',
                           'LOAD', '2', '@__key', '@__score', 'SORTBY', '2', '@__score', 'DESC', 'LIMIT', offset, limit)

        key_index = agg_full[1].index('__key') + 1
        score_index = agg_full[1].index('__score') + 1

        for i in range(1, len(agg_limited)):
            self.env.assertLessEqual(float(agg_limited[i][score_index]), float(agg_full[i+offset][score_index]), 0.00001)

    def test_single_result_normalization(self):
        res = self.env.cmd('FT.SEARCH', 'idx', 'blue', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.NORM')
        self.env.assertEqual(res[0], 1)
        self.env.assertAlmostEqual(float(res[2]), 1.0, 0.00001)

        agg = self.env.cmd('FT.AGGREGATE', 'idx', 'blue', 'ADDSCORES', 'SCORER', 'BM25STD.NORM')
        self.env.assertAlmostEqual(float(res[2]), 1.0, 0.00001)

    def test_identical_scores_same_shard(self):
        conn = self.env.getClusterConnectionIfNeeded()

        # Add identical documents with tag to ensure same shard
        for i in range(6, 9):
            conn.execute_command('HSET', f'doc{i}{{tag}}', 'title', 'identical content', 'body', 'identical body text')

        res = self.env.cmd('FT.SEARCH', 'idx', 'identical', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.NORM')
        for i in range(2, len(res), 2):
            self.env.assertAlmostEqual(float(res[i]), 1.0, 0.00001)

        agg = self.env.cmd('FT.AGGREGATE', 'idx', 'identical', 'ADDSCORES', 'SCORER', 'BM25STD.NORM')
        for row in agg[1:]:
            self.env.assertAlmostEqual(float(row[1]), 1.0, 0.00001)

    def test_no_results(self):
        query = 'no match term'
        res = self.env.cmd('FT.SEARCH', 'idx', query, 'WITHSCORES', 'SCORER', 'BM25STD.NORM', 'NOCONTENT')
        self.env.assertEqual(res[0], 0)

        agg = self.env.cmd('FT.AGGREGATE', 'idx', query, 'ADDSCORES', 'SCORER', 'BM25STD.NORM')
        self.env.assertEqual(agg[0], 0)

    def test_cursor(self):
        agg_norm = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD.NORM',
                        'LOAD', '2', '@__key', '@__score')

        key_index = agg_norm[1].index('__key') + 1
        score_index = agg_norm[1].index('__score') + 1
        scores_norm = {row[key_index]: float(row[score_index]) for row in agg_norm[1:]}

        results, cursor = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD.NORM',
                           'LOAD', '2', '@__key', '@__score', 'WITHCURSOR', 'COUNT', 2)
        while cursor != 0:
            for result in results[1:]:
                self.env.assertAlmostEqual(scores_norm[result[key_index]], float(result[score_index]), 0.00001)
            res, cursor = self.env.cmd('FT.CURSOR', 'READ', 'idx', cursor)

        results, cursor = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'ADDSCORES', 'SCORER', 'BM25STD.NORM',
                           'LOAD', '2', '@__key', '@__score', 'SORTBY', 2, '@__score', 'DESC', 'WITHCURSOR', 'COUNT', 2)
        while cursor != 0:
            for result in results[1:]:
                self.env.assertAlmostEqual(scores_norm[result[key_index]], float(result[score_index]), 0.00001)
            res, cursor = self.env.cmd('FT.CURSOR', 'READ', 'idx', cursor)


def testNormalizedBM25ScorerExplanation():
    """
    Tests that the normalized BM25STD scorer explanation is correct
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # Prepare the index
    _prepare_index(env, 'idx')

    # Search for the same query and get the scores using the BM25STD.TANH scorer
    norm_res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD.TANH')

    env.assertEqual(
        norm_res[2][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/4 * Final BM25 0.31)',
         [['Final BM25 : words BM25 0.31 * document score 1.00',
           [['(Weight 1.00 * children BM25 0.31)',
             ['hello: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))',
             'world: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))'
             ]
           ]]
        ]]]
    )

    env.assertEqual(
        norm_res[4][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/4 * Final BM25 0.27)',
         [['Final BM25 : words BM25 0.27 * document score 1.00',
           [['(Weight 1.00 * children BM25 0.27)',
             ['hello: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))',
              'world: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))'
              ]
            ]]
        ]]]
    )

    env.assertEqual(
        norm_res[6][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/4 * Final BM25 0.24)',
         [['Final BM25 : words BM25 0.24 * document score 1.00',
           [['(Weight 1.00 * children BM25 0.24)',
             ['hello: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))',
              'world: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))'
              ]
            ]]
        ]]]
    )

    # Test using weights
    norm_res = env.cmd('FT.SEARCH', 'idx', '(hello world) => {$weight: 0.25}', 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD.TANH')

    env.assertEqual(
        norm_res[2][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/4 * Final BM25 0.08)',
         [['Final BM25 : words BM25 0.08 * document score 1.00',
           [['(Weight 0.25 * children BM25 0.31)',
             ['hello: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))',
             'world: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))'
             ]
           ]]
        ]]]
    )

    env.assertEqual(
        norm_res[4][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/4 * Final BM25 0.07)',
         [['Final BM25 : words BM25 0.07 * document score 1.00',
           [['(Weight 0.25 * children BM25 0.27)',
             ['hello: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))',
              'world: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))'
              ]
            ]]
        ]]]
    )

    env.assertEqual(
        norm_res[6][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/4 * Final BM25 0.06)',
         [['Final BM25 : words BM25 0.06 * document score 1.00',
           [['(Weight 0.25 * children BM25 0.24)',
             ['hello: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))',
              'world: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))'
              ]
            ]]
        ]]]
    )

    # Normalize the score with a non-default stretch factor
    stretch_factor = 20
    env.assertEqual(
        run_command_on_all_shards(env, "config", "set", "search-bm25std-tanh-factor", str(stretch_factor)),
        ["OK"] * env.shardsCount
    )

    # Search for the same query and get the scores using the BM25STD.TANH scorer
    norm_res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD.TANH')
    env.assertEqual(
        norm_res[2][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/20 * Final BM25 0.31)',
            [['Final BM25 : words BM25 0.31 * document score 1.00',
            [['(Weight 1.00 * children BM25 0.31)',
            ['hello: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))',
            'world: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))'
            ]
            ]]
        ]]]
    )
    env.assertEqual(
        norm_res[4][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/20 * Final BM25 0.27)',
            [['Final BM25 : words BM25 0.27 * document score 1.00',
            [['(Weight 1.00 * children BM25 0.27)',
            ['hello: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))',
            'world: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))'
            ]
            ]]
        ]]]
    )
    env.assertEqual(
        norm_res[6][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/20 * Final BM25 0.24)',
            [['Final BM25 : words BM25 0.24 * document score 1.00',
            [['(Weight 1.00 * children BM25 0.24)',
            ['hello: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))',
            'world: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))'
            ]
            ]]
        ]]]
    )

    # Normalize the score in the query
    stretch_factor = 15
    norm_res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD.TANH', 'BM25STD_TANH_FACTOR', str(stretch_factor))
    env.assertEqual(
        norm_res[2][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/15 * Final BM25 0.31)',
            [['Final BM25 : words BM25 0.31 * document score 1.00',
            [['(Weight 1.00 * children BM25 0.31)',
            ['hello: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))',
            'world: (0.15 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 2 / Average Doc Len 3.00)))'
            ]
            ]]
        ]]]
    )
    env.assertEqual(
        norm_res[4][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/15 * Final BM25 0.27)',
            [['Final BM25 : words BM25 0.27 * document score 1.00',
            [['(Weight 1.00 * children BM25 0.27)',
            ['hello: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))',
            'world: (0.13 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 3 / Average Doc Len 3.00)))'
            ]
            ]]
        ]]]
    )
    env.assertEqual(
        norm_res[6][1],
        ['Final Normalized BM25 : tanh(stretch factor 1/15 * Final BM25 0.24)',
            [['Final BM25 : words BM25 0.24 * document score 1.00',
            [['(Weight 1.00 * children BM25 0.24)',
            ['hello: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))',
            'world: (0.12 = Weight 1.00 * IDF 0.13 * (F 1.00 * (k1 1.2 + 1)) / (F 1.00 + k1 1.2 * (1 - b 0.75 + b 0.75 * Doc Len 4 / Average Doc Len 3.00)))'
            ]
            ]]
        ]]]
    )

def testNormalizedBM25TanhValidations():
    """
    Tests the validations of the stretch factor of the BM25STD.TANH normalized
    scorer.
    Validations:
    - stretch factor must be a uint
    - stretch factor must be in the range [0, 10000]
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # Float
    env.expect("CONFIG", "SET", "search-bm25std-tanh-factor", "1.5").error().contains("argument couldn't be parsed into an integer")
    env.expect(config_cmd(), "SET", "BM25STD_TANH_FACTOR", "1.5").error().contains("SEARCH_PARSE_ARGS Could not convert argument to expected type")

    # Below minimum value
    env.expect("CONFIG", "SET", "search-bm25std-tanh-factor", "-1").error().contains("argument must be between 1 and 10000 inclusive")
    env.expect(config_cmd(), "SET", "BM25STD_TANH_FACTOR", "-1").error().contains("Value is outside acceptable bounds")

    # Above max value
    env.expect("CONFIG", "SET", "search-bm25std-tanh-factor", "10001").error().contains("argument must be between 1 and 10000 inclusive")
    env.expect(config_cmd(), "SET", "BM25STD_TANH_FACTOR", "10001").error().contains("BM25STD_TANH_FACTOR must be between 1 and 10000 inclusive")

    # Valid value
    env.expect("CONFIG", "SET", "search-bm25std-tanh-factor", "25").ok()
    env.expect(config_cmd(), "SET", "BM25STD_TANH_FACTOR", "25").ok()

def testNormalizedBM25TanhScoreField():
    """
    Tests that the normalized BM25 scorer works as expected when using a score
    field for the score.
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    conn = env.getClusterConnectionIfNeeded()

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCORE_FIELD', 'my_score_field', 'SCHEMA', 'title', 'TEXT').ok()

    # We currently use a hash-tag such that all docs will reside on the same
    # shard such that we will not get a score difference between the standalone
    # and cluster modes
    conn.execute_command('HSET', 'doc1{tag}', 'title', 'hello world', 'my_score_field', 10)
    conn.execute_command('HSET', 'doc2{tag}', 'title', 'hello space world', 'my_score_field', 100)
    conn.execute_command('HSET', 'doc3{tag}', 'title', 'hello more space world', 'my_score_field', 10000)

    # Search for `hello world` and get the scores using the BM25STD scorer
    res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD')
    # Order of results
    env.assertEqual(res[1::2], ['doc3{tag}', 'doc2{tag}', 'doc1{tag}'])
    # Scores
    expected_scores = [2350.1525, 26.7063, 3.0923]
    env.assertEqual([round(float(x), 4) for x in res[2::2]], expected_scores)

    # Search for the same query and get the scores using the BM25STD.TANH scorer
    norm_res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.TANH')
    # Order of results
    env.assertEqual(norm_res[1::2], ['doc3{tag}', 'doc2{tag}', 'doc1{tag}'])
    # Scores
    norm_expected_scores = [round(math.tanh(x * (1/DEFAULT_SCORE_NORM_STRETCH_FACTOR)), 5) for x in expected_scores]
    env.assertEqual([round(float(x), 5) for x in norm_res[2::2]], norm_expected_scores)

def scorer_with_weight_test(env, scorer):
    # Test that the scorer is applied correctly when using the `weight` attribute
    conn = getConnectionByEnv(env)
    env.expect('ft.create idx ON HASH schema title text').ok()
    conn.execute_command('HSET', 'doc1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2', 'title', 'hello world cat dog')

    def get_scores(env, query):
        res = env.cmd('ft.search', 'idx', query, 'withscores', 'scorer', scorer, 'nocontent')
        return [float(res[2]), float(res[4])]

    default_query = '@title: hello'
    weighted_query = '((@title:hello) => {$weight: 0.5;})'

    scores = get_scores(env, default_query)
    weighted_scores = get_scores(env, weighted_query)
    # Assert that weighted_scores are half of the default scores, since the weight is 0.5
    max_difference = max(abs(w - 0.5*s) for w, s in zip(weighted_scores, scores))
    env.assertAlmostEqual(max_difference, 0, 1E-6)

def testBM25STDScoreWithWeight(env: Env):
    scorer_with_weight_test(env, 'BM25STD')

def testBM25ScoreWithWeight(env: Env):
    scorer_with_weight_test(env, 'BM25')

@skip(cluster=True)
def testBM25STDUnderflow(env: Env):
    """
    Tests that we do not underflow when calculating the BM25STD score.
    Before the fix, we had an underflow when calculating the IDF, which caused
    the score to be jump rapidly in case of specific update/delete flows (MOD-12223).
    This test also shows the scoring behavior currently in RediSearch, in which
    for the same database image by the user, the score can change until the GC
    runs.
    """

    # Set the scorer to `BM25STD` (we had this issue only there)
    env.expect(config_cmd(), 'SET', 'DEFAULT_SCORER', 'BM25STD').ok()

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()

    # Turn off the GC, to model the scenario without interference
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()

    # Add a document with a single term
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc0', 'title', 'hello')

    # Get the score for `hello`
    res = env.cmd('ft.search', 'idx', 'hello', 'withscores', 'nocontent')
    score_before = float(res[2])

    # Update doc0, such that it will be deleted and re-added to the index
    conn.execute_command('HSET', 'doc0', 'title', 'hello')
    # Now, we have 1 document in the index, but the inverted-index of `hello`
    # contains 2 entries, until the GC cleans it up

    # After the fix, when we search for the term, the score should not jump, but
    # rather be slightly smaller, since the idf will be smaller
    # See https://en.wikipedia.org/wiki/Okapi_BM25 for more details
    res = env.cmd('ft.search', 'idx', 'hello', 'withscores', 'nocontent')
    score_after_update = float(res[2])

    env.assertGreater(score_before, score_after_update)

    # Reschedule the gc - add a job to the queue
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')

@skip(cluster=True)
def testBM25DocLen(env: Env):
    """
    Tests that the total document length is calculated correctly (MOD-122234).
    Relevant for the BM25 and BM25STD scorers.
    This test currently tests updates only, i.e., for an existing doc.
    """

    def get_avg_doc_len(response: str, std: bool = True):
        score_exp = response[2][1][1][0]
        split_by = 'Average Doc Len ' if std else 'Average Len'
        avg_doc_len = float(score_exp.split(split_by)[1].split(')')[0])
        return avg_doc_len

    def validate_avg_doc_len(env, query: str, expected_avg_len: float):
        for std in [True, False]:
            res = env.cmd('FT.SEARCH', 'idx', query, 'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT', 'SCORER', 'BM25STD' if std else 'BM25')
            env.assertEqual(get_avg_doc_len(res, std), expected_avg_len)

    # --------------------------------- Update ---------------------------------

    # Create an index with a TEXT field
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()

    # Add the first document
    env.cmd('HSET', 'doc0', 'title', 'hello world')

    # The average doc length should be 2
    validate_avg_doc_len(env, 'hello', 2)

    # Add the same document -> we re-index, and the avg doc length should be
    # updated on the fly to the new doc length
    env.cmd('HSET', 'doc0', 'title', 'hello world baby')

    validate_avg_doc_len(env, 'hello', 3)

    # -------------------------------- Deletion --------------------------------

    # Add a document with 5 terms (not stopwords)
    env.cmd('HSET', 'doc1', 'title', 'hello world baby cat mouse')

    # The average doc length should be 4
    validate_avg_doc_len(env, 'hello', 4)

    # Delete the document with 5 terms, the average doc length should be updated
    # on the fly back to 3
    env.cmd('DEL', 'doc1')

    validate_avg_doc_len(env, 'hello', 3)


def calculate_idf(total_docs, term_docs):
    """Calculate IDF using logb - same as C CalculateIDF function"""
    import math
    value = 1.0 + total_docs / (term_docs if term_docs else 1)
    # logb returns the exponent of the floating-point representation
    # which is floor(log2(|x|)) for positive x
    return math.floor(math.log2(abs(value))) if value != 0 else float('-inf')

def calculate_bm25_idf(total_docs, term_docs):
    """Calculate BM25 IDF using natural log - same as C CalculateIDF_BM25 function"""
    total_docs = max(total_docs, term_docs)
    return math.log(1.0 + (total_docs - term_docs + 0.5) / (term_docs + 0.5))


@skip(cluster=True)
def test_test_num_docs_scorer():
    """
    Test the TEST_NUM_DOCS scorer which returns the number of documents in the index.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Register the test scorers using the debug command
    env.expect(debug_cmd(), 'REGISTER_TEST_SCORERS').ok()

    # Create an index with a text field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    # Add 3 documents
    conn.execute_command('HSET', 'doc1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2', 'title', 'hello world again')
    conn.execute_command('HSET', 'doc3', 'title', 'hello world again more')

    # Expected score = numDocs = 3
    expected_score = 3.0

    # Search with the TEST_NUM_DOCS scorer
    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'TEST_NUM_DOCS', 'WITHSCORES', 'NOCONTENT')

    # Verify the score
    env.assertEqual(res[0], 3)  # 3 results
    actual_score = float(res[2])
    env.assertAlmostEqual(actual_score, expected_score, delta=0.0001)


@skip(cluster=True)
def test_test_num_terms_scorer():
    """
    Test the TEST_NUM_TERMS scorer which returns the number of unique terms in the index.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Register the test scorers using the debug command
    env.expect(debug_cmd(), 'REGISTER_TEST_SCORERS').ok()

    # Create an index with a text field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    # Add documents with 4 unique terms: hello, world, again, more
    conn.execute_command('HSET', 'doc1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2', 'title', 'hello world again')
    conn.execute_command('HSET', 'doc3', 'title', 'hello world again more')

    # Expected score = numTerms = 4
    expected_score = 4.0

    # Search with the TEST_NUM_TERMS scorer
    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'TEST_NUM_TERMS', 'WITHSCORES', 'NOCONTENT')

    # Verify the score
    env.assertEqual(res[0], 3)  # 3 results
    actual_score = float(res[2])
    env.assertAlmostEqual(actual_score, expected_score, delta=0.0001)


@skip(cluster=True)
def test_test_avg_doc_len_scorer():
    """
    Test the TEST_AVG_DOC_LEN scorer which returns the average document length.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Register the test scorers using the debug command
    env.expect(debug_cmd(), 'REGISTER_TEST_SCORERS').ok()

    # Create an index with a text field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    # Add documents: 2 tokens, 3 tokens, 4 tokens => avg = (2+3+4)/3 = 3.0
    conn.execute_command('HSET', 'doc1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2', 'title', 'hello world again')
    conn.execute_command('HSET', 'doc3', 'title', 'hello world again more')

    # Expected score = avgDocLen = 3.0
    expected_score = 3.0

    # Search with the TEST_AVG_DOC_LEN scorer
    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'TEST_AVG_DOC_LEN', 'WITHSCORES', 'NOCONTENT')

    # Verify the score
    env.assertEqual(res[0], 3)  # 3 results
    actual_score = float(res[2])
    env.assertAlmostEqual(actual_score, expected_score, delta=0.0001)


@skip(cluster=True)
def test_test_sum_idf_scorer():
    """
    Test the TEST_SUM_IDF scorer which returns the sum of IDF values from all terms.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Register the test scorers using the debug command
    env.expect(debug_cmd(), 'REGISTER_TEST_SCORERS').ok()

    # Create an index with a text field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    conn.execute_command('HSET', 'doc1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2', 'title', 'hello world again')
    conn.execute_command('HSET', 'doc3', 'title', 'hello world again more')

    # Searching for "hello" which appears in all 3 documents
    total_docs = 3
    term_docs = 3
    expected_score = calculate_idf(total_docs, term_docs)

    # Search with the TEST_SUM_IDF scorer
    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'TEST_SUM_IDF', 'WITHSCORES', 'NOCONTENT')

    # Verify the score
    env.assertEqual(res[0], 3)  # 3 results
    actual_score = float(res[2])
    env.assertAlmostEqual(actual_score, expected_score, delta=0.0001)


@skip(cluster=True)
def test_test_sum_bm25_idf_scorer():
    """
    Test the TEST_SUM_BM25_IDF scorer which returns the sum of BM25 IDF values from all terms.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Register the test scorers using the debug command
    env.expect(debug_cmd(), 'REGISTER_TEST_SCORERS').ok()

    # Create an index with a text field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    conn.execute_command('HSET', 'doc1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2', 'title', 'hello world again')
    conn.execute_command('HSET', 'doc3', 'title', 'hello world again more')

    # Searching for "hello" which appears in all 3 documents
    total_docs = 3
    term_docs = 3
    expected_score = calculate_bm25_idf(total_docs, term_docs)

    # Search with the TEST_SUM_BM25_IDF scorer
    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'TEST_SUM_BM25_IDF', 'WITHSCORES', 'NOCONTENT')

    # Verify the score
    env.assertEqual(res[0], 3)  # 3 results
    actual_score = float(res[2])
    env.assertAlmostEqual(actual_score, expected_score, delta=0.0001)


@skip(cluster=True)
def test_test_scorers_with_aggregate():
    """
    Test the test scorers with FT.AGGREGATE using ADDSCORES.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Register the test scorers using the debug command
    env.expect(debug_cmd(), 'REGISTER_TEST_SCORERS').ok()

    # Create an index with a text field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    # Add documents with controlled content
    conn.execute_command('HSET', 'doc1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc2', 'title', 'hello world again')

    # Test TEST_NUM_DOCS scorer with aggregate
    res = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'SCORER', 'TEST_NUM_DOCS',
                  'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    env.assertEqual(res[0], 2)
    env.assertAlmostEqual(float(res[1][1]), 2.0, delta=0.0001)  # numDocs = 2


@skip(cluster=True)
def test_test_scorers_with_explainscore():
    """
    Test the test scorers with EXPLAINSCORE to verify the explanations.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Register the test scorers using the debug command
    env.expect(debug_cmd(), 'REGISTER_TEST_SCORERS').ok()

    # Create an index with a text field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    # Add a single document
    conn.execute_command('HSET', 'doc1', 'title', 'hello world')

    # Test each scorer with EXPLAINSCORE
    scorers_and_expected = [
        ('TEST_NUM_DOCS', 1.0, 'TEST_NUM_DOCS'),
        ('TEST_NUM_TERMS', 2.0, 'TEST_NUM_TERMS'),
        ('TEST_AVG_DOC_LEN', 2.0, 'TEST_AVG_DOC_LEN'),
    ]

    for scorer_name, expected_score, expected_in_explanation in scorers_and_expected:
        res = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', scorer_name,
                      'WITHSCORES', 'EXPLAINSCORE', 'NOCONTENT')

        env.assertEqual(res[0], 1)
        score_info = res[2]
        actual_score = float(score_info[0])
        env.assertAlmostEqual(actual_score, expected_score, delta=0.0001)
        env.assertContains(expected_in_explanation, score_info[1])
