import math
import unittest
from rmtest import ModuleTestCase


class ScorersTestCase(ModuleTestCase('../redisearch.so')):

    def search(self, r, *args):
        return r.execute_command('ft.search', *args)

    def testTagIndex(self):
        with self.redis() as r:
            r.flushdb()

            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10, 'body', 'text'))
            N = 25
            for n in range(N):

                sc = math.sqrt(float(N - n + 10) / float(N + 10))
                # print n, sc

                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % n, sc, 'fields',
                                                'title', 'hello world ' * n, 'body', 'lorem ipsum ' * n))
            results = [
                [24L, 'doc1', 1.97, 'doc2', 1.94, 'doc3',
                    1.91, 'doc4', 1.88, 'doc5', 1.85],
                [24L, 'doc1', 0.9, 'doc2', 0.59, 'doc3',
                    0.43, 'doc4', 0.34, 'doc5', 0.28],
                [24L, 'doc4', 1.75, 'doc5', 1.75, 'doc3',
                    1.74, 'doc6', 1.74, 'doc7', 1.72],
                [24L, 'doc24', 480.0, 'doc23', 460.0, 'doc22',
                    440.0, 'doc21', 420.0, 'doc20', 400.0],
                [24L, 'doc1', 0.99, 'doc2', 0.97, 'doc3',
                    0.96, 'doc4', 0.94, 'doc5', 0.93],
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

            scorers = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'DISMAX', 'DOCSCORE']
            for _ in r.retry_with_rdb_reload():
                for i, scorer in enumerate(scorers):
                    res = self.search(r, 'idx', 'hello world', 'scorer',
                                      scorer, 'nocontent', 'withscores', 'limit', 0, 5)
                    res = [round(float(x), 2) if j > 0 and (j - 1) %
                           2 == 1 else x for j, x in enumerate(res)]
                    #print res
                    self.assertListEqual(results[i], res)

if __name__ == '__main__':

    unittest.main()
