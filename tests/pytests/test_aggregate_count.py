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


queries_withoutcount = [
    # implicit WITHOUTCOUNT
    ['FT.AGGREGATE', 'games', '*', 'NOCONTENT'],
    ['FT.AGGREGATE', 'games', '*', 'LOAD', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'LOAD', '1', '@price'],
    # explicit WITHOUTCOUNT
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LOAD', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LOAD', '1', '@price'],
]

queries_withoutcount_limit = [
    # WITHOUTCOUNT + LIMIT
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', '0', '5'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', '0', '50'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', '10', '50'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', '0', '1010'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', '500', '1010'],
]

queries_withoutcount_limit_high = [
    # WITHOUTCOUNT + LIMIT, limit > number of docs
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', '0', '10000'],
]

queries_withoutcount_limit00 = [
    # WITHOUTCOUNT + LIMIT 0 0 - only count
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', '0', '0'],
]

queries_withoutcount_sortby = [
    # WITHOUTCOUNT + SORTBY
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@price'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@title', 'ASC'],
    ['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@price', 'ASC'],
]

def test_resp2():
    protocol = 2
    env = Env(protocol=protocol)
    add_values_iterations = 2
    _setup_index_and_data(env, number_of_iterations=add_values_iterations)
    indexed_docs = 2265 * add_values_iterations

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

def _test_limit00(protocol):
    env = Env(protocol=protocol)
    add_values_iterations = 1
    _setup_index_and_data(env, number_of_iterations=add_values_iterations)
    docs = 2265 * add_values_iterations

    queries_and_results = [
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, 0], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 0], ANY),
        (['FT.AGGREGATE', 'games', '*', 'LIMIT', 0, 0], ANY),
    ]

    for query, expected_results in queries_and_results:
        res = env.cmd(*query)
        cmd=' '.join(str(x) for x in query)
        print(cmd)
        print(res)
        for dialect in [2]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            if protocol == 3:
                print('total_results:', res['total_results'])
                print('len(results):', len(res['results']))
                env.assertEqual(res['total_results'], expected_results,
                                message=f'{cmd}: total_results != expected')
                env.assertEqual(len(res['results']), 0,
                                message=f'{cmd}: len(results) != 1')
            else:
                print('total_results:', res[0])
                print('len(results):', len(res[1:]))
                env.assertEqual(res[0], expected_results,
                                message=f'{cmd}: total_results != expected')
                env.assertEqual(len(res[1:]), 0,
                                message=f'{cmd}: len(results) != 1')


def _test_count(protocol):
    env = Env(protocol=protocol)
    add_values_iterations = 1
    _setup_index_and_data(env, number_of_iterations=add_values_iterations)
    docs = 2265 * add_values_iterations

    queries_and_results = [
        # WITHCOUNT
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT'], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, docs*4], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 10, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 100, docs], docs - 100),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', docs, docs*2], 0),
        # WITHCOUNT + LOAD
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LOAD', '1', '@title'], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LOAD', '1', '@price'], docs),
        # WITHCOUNT + SORTBY
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'], 2265), # 10?
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@price'], 2265), # 10?
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'], 2265), # 10?
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'], 2265), # 10?
        # WITHCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs),
    ]

    if(CLUSTER):
        message = 'CLUSTER'
    else:
        message = 'STANDALONE'
    print(message, 'protocol:', protocol)
    for query, expected_results in queries_and_results:
        res = env.cmd(*query)
        cmd=' '.join(str(x) for x in query)
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            print(cmd)
            # print(res)
            if protocol == 3:
                print('total_results:', res['total_results'])
                print('len(results):', len(res['results']))
                env.assertEqual(res['total_results'], expected_results,
                                message=f'{cmd}: total_results != expected')
                env.assertEqual(res['total_results'], len(res['results']),
                                message=f'{cmd}: total_results != len(results)')
            else:
                print('total_results:', res[0])
                print('len(results):', len(res[1:]))
                env.assertEqual(res[0], expected_results,
                                message=f'{cmd}: total_results != expected')
                env.assertEqual(len(res[1:]), expected_results,
                                message=f'{cmd}: total_results != len(results)')

def test3():
    _test_count(3)

def test2():
    _test_count(2)

def test23():
    _test_count(2)
    _test_count(3)

def test_limit00_resp3():
    _test_limit00(3)

def test_limit00_resp2():
    _test_limit00(2)
