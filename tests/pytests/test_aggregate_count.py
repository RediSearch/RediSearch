from common import *
import bz2
import json


GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')

def _setup_index_and_data(env, number_of_iterations=1):
    env.cmd('FT.CREATE', 'games', 'ON', 'HASH',
                        'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                        'brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                        'description', 'TEXT', 'price', 'NUMERIC',
                        'categories', 'TAG')
    con = env.getClusterConnectionIfNeeded()

    # Use pipeline for much faster bulk HSET operations
    pipeline = con.pipeline(transaction=False)
    batch_size = 10000  # Execute pipeline every 10000 docs to avoid memory issues

    j = 0
    for i in range(number_of_iterations):
        fp = bz2.BZ2File(GAMES_JSON, 'r')
        for line in fp:
            obj = json.loads(line)
            id = obj['asin'] + (str(i) if i > 0 else '')
            del obj['asin']
            obj['price'] = obj.get('price') or 0
            obj['categories'] = ','.join(obj['categories'])
            cmd = ['HSET', id] + \
                [str(x) if x is not None else '' for x in itertools.chain(
                    *obj.items())]
            pipeline.execute_command(*cmd)
            j += 1
            # Execute pipeline every batch_size docs to avoid memory issues
            if j % batch_size == 0:
                pipeline.execute()
        fp.close()
    pipeline.execute()


def _get_total_results(res):
    if isinstance(res, dict):
        return res['total_results']
    else:
        return res[0]


def _assert_equal_wrapper(env, actual, expected, **kwargs):
    env.assertEqual(actual, expected, **kwargs, depth=2)


def _assert_less_wrapper(env, actual, expected, **kwargs):
    env.assertLess(actual, expected, **kwargs, depth=2)


def _test_query_results(env, queries, assertion_func, docs):
    for dialect in [1, 2, 3, 4]:
        env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
        for query in queries:
            res = env.cmd(*query)
            protocol_str = 'RESP3' if env.protocol == 3 else 'RESP2'
            err_message = f"{protocol_str}, dialect: {dialect}, {' '.join(str(x) for x in query)}"
            assertion_func(env, _get_total_results(res), docs, message=err_message)


queries_withcount = [
    # WITHCOUNT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT'],
    # WITHCOUNT + LOAD
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'LOAD', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'LOAD', '1', '@price'],
    # WITHCOUNT + LIMIT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'LIMIT', '0', '5'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'LIMIT', '0', '50'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'LIMIT', '0', '1010'],
    # WITHCOUNT + LIMIT 0 0 - only count
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'LIMIT', '0', '0'],
    # WITHCOUNT + SORTBY
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '1', '@price'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'],
]

queries_withoutcount = [
    # implicit WITHOUTCOUNT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'LOAD', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'LOAD', '1', '@price'],
    # explicit WITHOUTCOUNT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LOAD', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LOAD', '1', '@price'],
]

queries_withoutcount_limit = [
    # WITHOUTCOUNT + LIMIT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LIMIT', '0', '5'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LIMIT', '0', '50'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LIMIT', '10', '50'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LIMIT', '0', '1010'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LIMIT', '500', '1010'],
]

queries_withoutcount_limit_high = [
    # WITHOUTCOUNT + LIMIT, limit > number of docs
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LIMIT', '0', '10000'],
]

queries_withoutcount_limit00 = [
    # WITHOUTCOUNT + LIMIT 0 0 - only count
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'LIMIT', '0', '0'],
]

queries_withoutcount_sortby = [
    # WITHOUTCOUNT + SORTBY
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'SORTBY', '1', '@price'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'SORTBY', '2', '@title', 'ASC'],
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT', 'WITHOUTCOUNT', 'SORTBY', '2', '@price', 'ASC'],
]

def test_resp2():
    protocol = 2
    env = Env(protocol=protocol)
    add_values_iterations = 2
    _setup_index_and_data(env, number_of_iterations=add_values_iterations)
    indexed_docs = 2265 * add_values_iterations

    # For WITHCOUNT total_results is always accurate
    _test_query_results(env, queries_withcount,
                        _assert_equal_wrapper, indexed_docs)

    # For WITHOUTCOUNT + SORTBY total_results is always accurate
    _test_query_results(env, queries_withoutcount_sortby,
                        _assert_equal_wrapper, indexed_docs)

    # For WITHOUTCOUNT total_results is less than the accurate count
    _test_query_results(env, queries_withoutcount,
                        _assert_less_wrapper, indexed_docs)
    _test_query_results(env, queries_withoutcount_limit,
                        _assert_less_wrapper, indexed_docs)
    _test_query_results(env, queries_withoutcount_limit_high,
                        _assert_less_wrapper, indexed_docs)

    # For WITHOUTCOUNT + LIMIT 0 0
    if CLUSTER:
        # total_results is less than the accurate count
        # because we send total_results before receiving results from all shards
        _test_query_results(env, queries_withoutcount_limit00,
                            _assert_less_wrapper, indexed_docs)
    else: # STANDALONE
        # total_results is accurate
        _test_query_results(env, queries_withoutcount_limit00,
                            _assert_equal_wrapper, indexed_docs)

def test_resp3():
    protocol = 3
    env = Env(protocol=protocol)
    add_values_iterations = 2
    _setup_index_and_data(env, number_of_iterations=add_values_iterations)
    indexed_docs = 2265 * add_values_iterations
    # For WITHCOUNT total_results is always accurate
    _test_query_results(env, queries_withcount, _assert_equal_wrapper, indexed_docs)

    # For WITHOUTCOUNT + SORTBY total_results is always accurate
    _test_query_results(env, queries_withoutcount_sortby,
                        _assert_equal_wrapper, indexed_docs)

    # For WITHOUTCOUNT, in RESP3 total_results is accurate because
    # we send total_results after receiving results from all shards
    _test_query_results(env, queries_withoutcount,
                        _assert_equal_wrapper, indexed_docs)

    # For WITHOUTCOUNT + LIMIT total_results is less than the accurate count
    _test_query_results(env, queries_withoutcount_limit,
                        _assert_less_wrapper, indexed_docs)
    # For WITHOUTCOUNT + LIMIT, limit > number of docs,
    # in RESP3 total_results is accurate
    _test_query_results(env, queries_withoutcount_limit_high,
                        _assert_equal_wrapper, indexed_docs)

    # For WITHOUTCOUNT + LIMIT 0 0, in RESP3 total_results is accurate
    _test_query_results(env, queries_withoutcount_limit00,
                        _assert_equal_wrapper, indexed_docs)




