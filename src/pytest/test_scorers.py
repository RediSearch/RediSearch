from rmtest import ModuleTestCase
import redis
import unittest
import platform
import math


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
                #print n, sc

                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % n, sc, 'fields',
                                                'title', 'hello world ' * n, 'body', 'lorem ipsum ' * n))
            results = [
                [24L, 'doc1', 1.9712215662002563, 'doc2', 1.9420166015625, 'doc3',
                    1.9123657941818237, 'doc4', 1.8822479248046875, 'doc5', 1.851640224456787],
                [24L, 'doc1', 0.8960098028182983, 'doc2', 0.5884898792613636, 'doc3',
                    0.43462858958677814, 'doc4', 0.3422268954190341, 'doc5', 0.28055154916011926],
                [24L, 'doc4', 1.7548460933639696, 'doc5', 1.7500002083188189, 'doc3',
                    1.743586604189589, 'doc6', 1.7364727331891703, 'doc7', 1.717598554845132],
                [24L, 'doc24', 480.0, 'doc23', 460.0, 'doc22',
                    440.0, 'doc21', 420.0, 'doc20', 400.0],
                [24L, 'doc1', 0.9856107831001282, 'doc2', 0.97100830078125, 'doc3',
                    0.9561828970909119, 'doc4', 0.9411239624023438, 'doc5', 0.9258201122283936],
            ]

            scorers = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'DISMAX', 'DOCSCORE']

            for i, scorer in enumerate(scorers):
                res = self.search(r, 'idx', 'hello world', 'scorer',
                                  scorer, 'nocontent', 'withscores', 'limit', 0, 5)
                res = [float(x) if j > 0 and (j - 1) %
                       2 == 1 else x for j, x in enumerate(res)]
                #print res
                self.assertListEqual(results[i], res)

if __name__ == '__main__':

    unittest.main()
