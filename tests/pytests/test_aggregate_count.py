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

def _get_results(res):
    if isinstance(res, dict):
        return res['results']
    else:
        return res[1:]


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
        cmd=' '.join(str(x) for x in query)
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            res = env.cmd(*query)
            total_results = _get_total_results(res)
            results = _get_results(res)

            # Verify results
            env.assertEqual(total_results, expected_results,
                            message=f'{cmd}: total_results != expected')
            env.assertEqual(len(results), 0,
                            message=f'{cmd}: len(results) != 0')


def test_limit00_resp3():
    _test_limit00(3)


def test_limit00_resp2():
    _test_limit00(2)


def _test_withcount(protocol):
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
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'], 2265),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@price'], 2265),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'], 2265),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'], 2265),
        # WITHCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs),
    ]

    for query, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            res = env.cmd(*query)
            total_results = _get_total_results(res)
            results = _get_results(res)

            # Verify results
            env.assertEqual(total_results, expected_results,
                            message=f'{cmd}: total_results != expected')
            env.assertEqual(len(results), expected_results,
                            message=f'{cmd}: len(results) != expected')


def test_withcount_resp3():
    _test_withcount(3)


def test_withcount_resp2():
    _test_withcount(2)


def _test_withoutcount(protocol):
    env = Env(protocol=protocol)
    add_values_iterations = 1
    _setup_index_and_data(env, number_of_iterations=add_values_iterations)
    docs = 2265 * add_values_iterations


    queries_and_results = [
        # WITHOUTCOUNT
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT'], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, docs*4], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 10, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 100, docs], docs - 100),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', docs, docs*2], 0),
        # WITHOUTCOUNT + LOAD
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LOAD', '1', '@title'], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LOAD', '1', '@price'], docs),
        # WITHOUTCOUNT + SORTBY - backwards compatible, returns only 10 results
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'], 10),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@price'], 10),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@title', 'ASC'], 10),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@price', 'ASC'], 10),
        # WITHOUTCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs),
    ]

    for query, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)

        # query without WITHOUTCOUNT
        query_default = query.copy()
        query_default.remove('WITHOUTCOUNT')
        cmd_default=' '.join(str(x) for x in query_default)
        res_default = env.cmd(*query_default)

        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            res = env.cmd(*query)

            results = _get_results(res)
            results_default = _get_results(res_default)

            # Verify only the length of results, don't verify total_results
            env.assertEqual(
                len(results), expected_results,
                message=f'{cmd}: len(results) != expected - dialect: {dialect}')

            # Compare with the query without WITHOUTCOUNT
            env.assertEqual(
                len(results_default), expected_results,
                message=f'{cmd_default}: len(results) != results_default - dialect: {dialect}')


def test_withoutcount_resp3():
    _test_withoutcount(3)

def test_withoutcount_resp2():
    _test_withoutcount(2)
