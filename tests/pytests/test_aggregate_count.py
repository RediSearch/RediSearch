from common import *
import bz2
import json


GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')

def _setup_index_and_data(env, docs):
    env.cmd('FT.CREATE', 'games', 'ON', 'HASH',
                        'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                        'brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                        'description', 'TEXT', 'price', 'NUMERIC',
                        'categories', 'TAG')
    conn = env.getClusterConnectionIfNeeded()

    for i in range(docs):
        title = f'Game {i}'
        brand = f'Brand {i % 10}'
        description = f'Description for game {i}'
        price = i * 1.5
        category = f'Category {i % 5}'
        conn.execute_command(
            'HSET', f'doc_{i}', 'title', title, 'brand', brand,
            'description', description, 'price', price, 'categories', category)


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
    docs = 2265
    _setup_index_and_data(env, docs)

    queries_and_results = [
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, 0], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 0], ANY),
        (['FT.AGGREGATE', 'games', '*', 'LIMIT', 0, 0], ANY),
    ]

    env.expect('CONFIG', 'SET', 'search-on-timeout', 'return').ok()
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
    docs = 2265
    _setup_index_and_data(env, docs)

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
        # WITHCOUNT + SORTBY 0
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '0'], docs),
        # WITHCOUNT + SORTBY
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@price'], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'], docs),
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
    docs = 2265
    _setup_index_and_data(env, docs)

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
        # WITHOUTCOUNT + SORTBY 0
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '0'], docs),
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


def test_profile_resp2():
    env = Env(protocol=2)
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'text')
    conn.execute_command('HSET', '1', 't', 'hello')
    conn.execute_command('HSET', '2', 't', 'world')

    if env.isCluster():
        rp0 = ['Type', 'Network', 'Time', ANY, 'Results processed', ANY]
    else:
        rp0 = ['Type', 'Index', 'Time', ANY, 'Results processed', ANY]
    rp1 = ['Type', 'Depleter', 'Time', ANY, 'Results processed', ANY]

    env.expect('CONFIG', 'SET', 'search-on-timeout', 'return').ok()
    # No strict mode + WITHOUTCOUNT doesn't add a depleter
    queries = [
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',],
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHOUTCOUNT'],
    ]
    for query in queries:
        profile = env.cmd(*query)
        if env.isCluster():
            RP_profile = profile[1][3][11]
        else:
            RP_profile = profile[1][1][0][13]
        env.assertEqual(len(RP_profile), 1,
                        message=f'query: {query} profile: {RP_profile}')
        env.assertEqual(RP_profile[0], rp0)

    # Non-strict mode + WITHCOUNT adds a depleter
    profile = env.cmd(
        'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHCOUNT')
    if env.isCluster():
        RP_profile = profile[1][3][11]
    else:
        RP_profile = profile[1][1][0][13]
    env.assertEqual(len(RP_profile), 2,
                    message=f'query: {query} profile: {RP_profile}')
    env.assertEqual(RP_profile[0], rp0)
    env.assertEqual(RP_profile[1], rp1)

    # Strict mode always adds a depleter
    env.expect('CONFIG', 'SET', 'search-on-timeout', 'fail').ok()
    queries = [
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*'],
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHOUTCOUNT'],
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHCOUNT'],
    ]
    for query in queries:
        profile = env.cmd(*query)
        if env.isCluster():
            RP_coord = profile[1][3][11]
        else:
            RP_coord = profile[1][1][0][13]

        env.assertEqual(len(RP_coord), 2,
                        message=f'query: {query} profile: {RP_coord}')
        env.assertEqual(RP_coord[0], rp0)
        env.assertEqual(RP_coord[1], rp1)


def test_profile_resp3():
    env = Env(protocol=3)
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'text')
    conn.execute_command('HSET', '1', 't', 'hello')
    conn.execute_command('HSET', '2', 't', 'world')

    if env.isCluster():
        rp0 = {'Type': 'Network', 'Time': ANY, 'Results processed': ANY}
    else:
        rp0 = {'Type': 'Index', 'Time': ANY, 'Results processed': ANY}

    queries = [
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',],
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHOUTCOUNT'],
        ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHCOUNT'],
    ]

    # In RESP3, we don't have a depleter, independent of the timeout policy
    for on_timeout_policy in ['return', 'fail']:
        env.expect('CONFIG', 'SET', 'search-on-timeout', on_timeout_policy).ok()
        for query in queries:
            profile = env.cmd(*query)
            if env.isCluster():
                RP_profile = profile['Profile']['Coordinator']['Result processors profile']
            else:
                RP_profile = profile['Profile']['Shards'][0]['Result processors profile']

            env.assertEqual(len(RP_profile), 1,
                            message=f'query: {query} profile: {RP_profile}')
            env.assertEqual(RP_profile[0], rp0)
