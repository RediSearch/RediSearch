from rmtest import ModuleTestCase
import redis
import unittest
import random
import time


class SearchTestCase(ModuleTestCase('../redisearch.so')):

    _tokens = {}
    _docs = {}
    _docId = 1
    _vocab_size = 10000

    def _random_token(self):

        return 'tok%d' % random.randrange(1, self._vocab_size)

    def generate_random_doc(self, num_tokens=100):

        random.seed(time.time())

        tokens = []

        for i in range(num_tokens):
            tokens.append(self._random_token())

        for tok in tokens:
            self._tokens.setdefault(tok, set()).add(self._docId)

        self._docs[self._docId] = tokens
        self._docId += 1

        return self._docId - 1, tokens

    def createIndex(self, r):

        r.flushdb()
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'txt', 'text'))

        for i in xrange(1000):
            did, tokens = self.generate_random_doc()

            r.execute_command('ft.add', 'idx', did,
                              1.0, 'fields', 'txt', ' '.join(tokens))

        # print r.execute_command('ft.info', 'idx')

    def compareResults(self, r, num_unions=2, toks_per_union=7):

        # generate N unions  of M tokens
        unions = [[self._random_token() for _ in range(toks_per_union)]
                  for _ in range(num_unions)]

        # get the documents for each union
        union_docs = [reduce(lambda x, y: x.union(y), [self._tokens.get(t, set()) for t in u], set())
                      for u in unions]
        # intersect the result to get the actual search result for an
        # intersection of all unions
        result = reduce(lambda x, y: x.intersection(y), union_docs)

        # format the equivalent search query for the same tokens
        q = ''.join(('(%s)' % '|'.join(toks) for toks in unions))
        args = ['ft.search', 'idx', q, 'nocontent', 'limit', 0, 100]
        #print args

        qr = set((int(x) for x in r.execute_command('ft.search', 'idx',
                                                    q, 'nocontent', 'limit', 0, 100)[1:]))

        #print sorted(result), '<=>', sorted(qr)
        return result.difference(qr)

    def testFuzzy(self):

        # print self._tokens
        with self.redis() as r:
            self.createIndex(r)
            self.assertTrue(True)

            for x in range(100):
                self.assertFalse(self.compareResults(r, 5, 40))


if __name__ == '__main__':

    unittest.main()
