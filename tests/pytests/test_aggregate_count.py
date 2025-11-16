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
        price = i
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

    for on_timeout_policy in ['return', 'fail']:
        env.expect('CONFIG', 'SET', 'search-on-timeout', on_timeout_policy).ok()
        queries_and_results = [
            (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, 0], docs),
            (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 0], ANY),
        ]

        for query, expected_results in queries_and_results:
            cmd=' '.join(str(x) for x in query)
            for dialect in [1, 2, 3, 4]:
                env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
                res = env.cmd(*query)
                total_results = _get_total_results(res)
                results = _get_results(res)

                # Verify results
                env.assertEqual(
                    total_results, expected_results,
                    message=f'{cmd}: total_results != expected dialect: {dialect}')
                env.assertEqual(
                    len(results), 0,
                    message=f'{cmd}: len(results) != 0 dialect: {dialect}')

        # If no WITHCOUNT/WITHOUTCOUNT is specified, the behavior depends on the dialect.
        query =['FT.AGGREGATE', 'games', '*', 'LIMIT', 0, 0]
        cmd=' '.join(str(x) for x in query)
        for dialect in [1, 2, 3, 4]:
            if dialect < 4:
                expected_results = docs
            else:
                expected_results = ANY

            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            res = env.cmd(*query)
            total_results = _get_total_results(res)
            results = _get_results(res)

            # Verify results
            env.assertEqual(
                total_results, expected_results,
                message=f'{cmd}: total_results != expected dialect: {dialect} on_timeout_policy: {on_timeout_policy}')
            env.assertEqual(
                len(results), 0,
                message=f'{cmd}: len(results) != 0 dialect: {dialect} on_timeout_policy: {on_timeout_policy}')


def test_limit00_resp3():
    _test_limit00(3)


def test_limit00_resp2():
    _test_limit00(2)


def _test_withcount(protocol):
    env = Env(protocol=protocol)
    docs = 2265
    _setup_index_and_data(env, docs)

    queries_and_results = [
        # query, total_results, length of results

        # WITHCOUNT
        # No sorter, no limit, returns all results
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT'], docs, docs),

        # WITHCOUNT + LIMIT
        # No sorter, limit results
        # total_results = number of documents matching the query up to the LIMIT
        # length of results = min(total_results, LIMIT)
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, 50], 50, 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, int(docs/2)], int(docs/2), int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 0, docs*4], docs, docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 10, 50], 50, 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', 100, docs], docs - 100, docs - 100),
        (['FT.AGGREGATE', 'games', '@price:[1, 100]', 'WITHCOUNT', 'LIMIT', 0, docs], 100, 100),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'LIMIT', docs, docs*2], 0, 0),

        # WITHCOUNT + SORTBY 0
        # No sorter, no limit, returns all results
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '0'], docs, docs),

        # WITHCOUNT + SORTBY
        # Sorter, limit results to DEFAULT_LIMIT
        # total_results = docs, length of results = DEFAULT_LIMIT
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'], docs, 10),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '1', '@price'], docs, 10),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'], docs, 10),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'], docs, 10),

        # WITHCOUNT + SORTBY + LIMIT
        # total_results = number of documents matching the query up to the LIMIT
        # length of results = min(total_results, LIMIT)
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], 50, 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@price', 'LIMIT', 0, 50], 50, 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], int(docs/2), int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs, docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 10, 50], 50, 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 100, docs], docs - 100, docs - 100),
        (['FT.AGGREGATE', 'games', '@price:[1, 100]', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs], 100, 100),
    ]

    for query, expected_total_results, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            res = env.cmd(*query)
            total_results = _get_total_results(res)
            results = _get_results(res)

            # Verify results
            env.assertEqual(
                total_results, expected_total_results,
                message=f'{cmd}: total_results != expected dialect: {dialect}')
            env.assertEqual(
                len(results), expected_results,
                message=f'{cmd}: len(results) != expected dialect: {dialect}')


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

        # WITHOUTCOUNT + LIMIT
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 0, docs*4], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 10, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', 100, docs], docs - 100),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'LIMIT', docs, docs*2], 0),

        # WITHOUTCOUNT + SORTBY 0
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '0'], docs),

        # WITHOUTCOUNT + SORTBY - backwards compatible, returns only 10 results
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'], 10),
        # (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@price'], 10), # crash
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@title', 'ASC'], 10),
        # (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@price', 'ASC'], 10), # crash

        # # WITHOUTCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@price', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 10, 50], 50),
        (['FT.AGGREGATE', 'games', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 100, docs], docs - 100),
        (['FT.AGGREGATE', 'games', '@price:[1, 100]', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs], 100),
    ]

    for query, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)

        # create a query without WITHOUTCOUNT to test the default behavior
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
    # Test when the depleter is added for FT.AGGREGATE RESP2
    env = Env(protocol=2)
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'text')
    conn.execute_command('HSET', '1', 't', 'hello')
    conn.execute_command('HSET', '2', 't', 'world')

    # Setup expected result processor profiles
    if env.isCluster():
        rp0 = ['Type', 'Network', 'Time', ANY, 'Results processed', ANY]
    else:
        rp0 = ['Type', 'Index', 'Time', ANY, 'Results processed', ANY]
    rp1 = ['Type', 'Depleter', 'Time', ANY, 'Results processed', ANY]

    for on_timeout_policy in ['return', 'fail']:
        env.expect('CONFIG', 'SET', 'search-on-timeout', on_timeout_policy).ok()

        # WITHOUTCOUNT doesn't add a depleter
        queries = [
            ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHOUTCOUNT'],
        ]
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            for query in queries:
                profile = env.cmd(*query)
                if env.isCluster():
                    RP_profile = profile[1][3][11]
                else:
                    RP_profile = profile[1][1][0][13]
                env.assertEqual(len(RP_profile), 1,
                                message=f'query: {query} profile: {RP_profile}, dialect: {dialect}')
                env.assertEqual(RP_profile[0], rp0)

        # WITHCOUNT adds a depleter
        queries = [
            ('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'WITHCOUNT'),
        ]
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            for query in queries:
                profile = env.cmd(*query)
                if env.isCluster():
                    RP_profile = profile[1][3][11]
                else:
                    RP_profile = profile[1][1][0][13]
                message=f'query: {query} profile: {RP_profile} dialect: {dialect}'
                env.assertEqual(len(RP_profile), 2, message=message)
                env.assertEqual(RP_profile[0], rp0)
                env.assertEqual(RP_profile[1], rp1)

        # Queries without explicit WITHCOUNT/WITHOUTCOUNT the behavior
        # depends on the dialect:
        # - Dialect 1-3 adds a depleter    (WITHCOUNT by default)
        # - Dialect 4 doesn't add depleter (WITHOUTCOUNT by default)
        queries = [
            ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*'],
        ]
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            for query in queries:
                profile = env.cmd(*query)
                if env.isCluster():
                    RP_profile = profile[1][3][11]
                else:
                    RP_profile = profile[1][1][0][13]

                if dialect < 4:
                    env.assertEqual(len(RP_profile), 2,
                                    message=f'query: {query} profile: {RP_profile}, dialect: {dialect}')
                    env.assertEqual(RP_profile[0], rp0)
                    env.assertEqual(RP_profile[1], rp1)
                else:
                    env.assertEqual(len(RP_profile), 1,
                                    message=f'query: {query} profile: {RP_profile}, dialect: {dialect}')
                    env.assertEqual(RP_profile[0], rp0)


def test_profile_resp3():
    # Test when the depleter is added for FT.AGGREGATE RESP3
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

    # In RESP3, we never have a depleter
    for on_timeout_policy in ['return', 'fail']:
        env.expect('CONFIG', 'SET', 'search-on-timeout', on_timeout_policy).ok()
        for dialect in [1, 2, 3, 4]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            for query in queries:
                profile = env.cmd(*query)
                if env.isCluster():
                    RP_profile = profile['Profile']['Coordinator']['Result processors profile']
                else:
                    RP_profile = profile['Profile']['Shards'][0]['Result processors profile']

                message = f'query: {query} profile: {RP_profile} on_timeout_policy: {on_timeout_policy}'
                env.assertEqual(len(RP_profile), 1,
                                message=message)
                env.assertEqual(RP_profile[0], rp0)
