import random
import time
from includes import *


_tokens = {}
_docs = {}
_docId = 1
_vocab_size = 10000

def _random_token(env):

    return 'tok%d' % random.randrange(1, _vocab_size)

def generate_random_doc(env, num_tokens=100):
    global _docId

    random.seed(time.time())

    tokens = []

    for i in range(num_tokens):
        tokens.append(_random_token(env))

    for tok in tokens:
        _tokens.setdefault(tok, set()).add(_docId)

    _docs[_docId] = tokens
    _docId += 1

    return _docId - 1, tokens

def createIndex(env, r):
    env.assertOk(r.execute_command(
        'ft.create', 'idx', 'schema', 'txt', 'text'))

    for i in xrange(1000):
        did, tokens = generate_random_doc(env)

        r.execute_command('ft.add', 'idx', did,
                          1.0, 'fields', 'txt', ' '.join(tokens))

    # print r.execute_command('ft.info', 'idx')

def compareResults(env, r, num_unions=2, toks_per_union=7):

    # generate N unions  of M tokens
    unions = [[_random_token(env) for _ in range(toks_per_union)]
              for _ in range(num_unions)]

    # get the documents for each union
    union_docs = [reduce(lambda x, y: x.union(y), [_tokens.get(t, set()) for t in u], set())
                  for u in unions]
    # intersect the result to get the actual search result for an
    # intersection of all unions
    result = reduce(lambda x, y: x.intersection(y), union_docs)

    # format the equivalent search query for the same tokens
    q = ''.join(('(%s)' % '|'.join(toks) for toks in unions))
    args = ['ft.search', 'idx', q, 'nocontent', 'limit', 0, 100]
    # print args

    qr = set((int(x) for x in r.execute_command('ft.search', 'idx',
                                                q, 'nocontent', 'limit', 0, 100)[1:]))

    # print sorted(result), '<=>', sorted(qr)
    return result.difference(qr)

def testFuzzy(env):

    # print env._tokens
    r = env
    createIndex(env, r)
    env.assertTrue(True)

    for x in range(100):
        env.assertIsNotNone(compareResults(env, r, 5, 40))
