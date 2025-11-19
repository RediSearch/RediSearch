from common import *
import bz2
import json

def _setup_index_and_data(env, docs):
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
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


def _get_total_results(res) -> int:
    # Extract the total_results from the query response
    if isinstance(res, dict):
        return res['total_results']
    else:
        return res[0]


def _get_results(res):
    # Extract the results from the query response
    if isinstance(res, dict):
        return res['results']
    else:
        return res[1:]


def _get_cluster_RP_profile(env, res) -> list:
    # Extract the RP types from the profile response
    if isinstance(res, dict):
        shard = res['Profile']['Shards'][0]['Result processors profile']
        coord = res['Profile']['Coordinator']['Result processors profile']
        shard_RP = [item['Type'] for item in shard]
        coord_RP = [item['Type'] for item in coord]
        return [shard_RP, coord_RP]
    else:
        shard = res[1][1][0][13]
        coord = res[1][3][11]
        shard_RP = [item[1] for item in shard]
        coord_RP = [item[1] for item in coord]
        return [shard_RP, coord_RP]


def _get_standalone_RP_profile(env, res) -> list:
    if isinstance(res, dict):
        profile = res['Profile']['Shards'][0]['Result processors profile']
        return [item['Type'] for item in profile]
    else:
        profile = res[1][1][0][13]
        return [item[1] for item in profile]


def _translate_query_to_profile_query(query) -> list:
    profile = ['FT.PROFILE']
    profile.append(query[1])        # index name
    profile.append(query[0][3:])    # command
    profile.append('QUERY')
    profile.extend(query[2:])       # query
    return profile


def _test_limit00(protocol):
    env = Env(protocol=protocol)
    docs = 2265
    _setup_index_and_data(env, docs)

    for on_timeout_policy in ['return', 'fail']:
        env.expect('CONFIG', 'SET', 'search-on-timeout', on_timeout_policy).ok()
        queries_and_results = [
            (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 0], docs),
            (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 0], ANY),
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
        query =['FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 0]
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
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT'], docs, docs),

        # WITHCOUNT + LIMIT
        # No sorter, limit results
        # total_results = number of documents matching the query up to the LIMIT
        # length of results = min(total_results, LIMIT)
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, int(docs/2)], docs, int(docs/2)),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs*4], docs, docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 10, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 100, docs], docs, docs - 100),
        (['FT.AGGREGATE', 'idx', '@price:[1, 100]', 'WITHCOUNT', 'LIMIT', 0, docs], 100, 100),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', docs, docs*2], docs, 0),

        # WITHCOUNT + SORTBY 0
        # No sorter, no limit, returns all results
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0'], docs, docs),

        # WITHCOUNT + SORTBY
        # Sorter, limit results to DEFAULT_LIMIT
        # total_results = docs, length of results = DEFAULT_LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'], docs, 10),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@price'], docs, 10),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'], docs, 10),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'], docs, 10),

        # WITHCOUNT + SORTBY + LIMIT
        # total_results = number of documents matching the query up to the LIMIT
        # length of results = min(total_results, LIMIT)
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@price', 'LIMIT', 0, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], docs, int(docs/2)),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs, docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 10, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 100, docs], docs, docs - 100),
        (['FT.AGGREGATE', 'idx', '@price:[1, 100]', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs], 100, 100),

        # WITHCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title'], docs, docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price'], docs, docs),
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


def _test_withcount_withcursor(protocol):
    env = Env(protocol=protocol)
    docs = 2265
    _setup_index_and_data(env, docs)
    cursor_count = 10
    default_cursor_count = 1000
    default_limit = 10

    queries_and_results = [
        # WITHCOUNT + WITHCURSOR
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR'], docs, default_cursor_count),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR', 'COUNT', cursor_count], docs, cursor_count),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR', 'COUNT', cursor_count*2], docs, cursor_count*2),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR', 'COUNT', cursor_count*4], docs, cursor_count*4),

        # WITHCOUNT + SORTBY + WITHCURSOR
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'WITHCURSOR'], docs, default_limit),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'WITHCURSOR', 'COUNT', cursor_count], docs, cursor_count),

        # WITHCOUNT + LIMIT + WITHCURSOR - default cursor count
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs, 'WITHCURSOR'], docs, default_cursor_count),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs * 2, 'WITHCURSOR'], docs, default_cursor_count),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs // 2, 'WITHCURSOR'], docs, default_cursor_count),

        # WITHCOUNT + LIMIT + WITHCURSOR - limit > cursor count
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs, 'WITHCURSOR', 'COUNT', cursor_count], docs, cursor_count),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs * 2, 'WITHCURSOR', 'COUNT', cursor_count], docs, cursor_count),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs // 2, 'WITHCURSOR', 'COUNT', cursor_count], docs, cursor_count),

        # WITHCOUNT + LIMIT + WITHCURSOR - limit == docs
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs, 'WITHCURSOR', 'COUNT', docs], docs, docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs, 'WITHCURSOR', 'COUNT', docs * 2], docs, docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, docs, 'WITHCURSOR', 'COUNT', docs // 2], docs, docs // 2),
    ]

    for query, expected_total_results, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)
        for dialect in [2]:
            env.expect('CONFIG', 'SET', 'search-default-dialect', dialect).ok()
            res, cursor = env.cmd(*query)
            total_results = _get_total_results(res)
            results = _get_results(res)

            # Verify results
            env.assertEqual(
                total_results, expected_total_results,
                message=f'{cmd}: total_results != expected dialect: {dialect}')
            env.assertEqual(
                len(results), expected_results,
                message=f'{cmd}: len(results) != expected dialect: {dialect}')


def test_withcount_withcursor_resp2():
    _test_withcount_withcursor(2)


def test_withcount_withcursor_resp3():
    _test_withcount_withcursor(3)


def _test_withoutcount(protocol):
    env = Env(protocol=protocol)
    docs = 2265
    _setup_index_and_data(env, docs)

    queries_and_results = [
        # WITHOUTCOUNT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT'], docs),

        # WITHOUTCOUNT + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, docs*4], docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 10, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 100, docs], docs - 100),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', docs, docs*2], 0),

        # WITHOUTCOUNT + SORTBY 0
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0'], docs),

        # WITHOUTCOUNT + SORTBY - backwards compatible, returns only 10 results
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'], 10),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@price'], 10), # crash
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@title', 'ASC'], 10),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@price', 'ASC'], 10), # crash

        # # WITHOUTCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@price', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 10, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 100, docs], docs - 100),
        (['FT.AGGREGATE', 'idx', '@price:[1, 100]', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs], 100),
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


def _test_profile(protocol):
    env = Env(protocol=protocol)
    docs = 5
    _setup_index_and_data(env, docs)

    queries_and_profiles = [
        # query,
        # RESP2 Standalone,
        # RESP3 standalone,
        # RESP2 [shard[0], coordinator]
        # RESP3 [shard[0], coordinator]

        # WITHCOUNT
        # No sorter, no limit, returns all results
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT'],
         ['Index', 'Depleter'],
         ['Index'],
         [['Index', 'Depleter'], ['Network', 'Depleter']],
         [['Index'], ['Network']]),

        # WITHCOUNT + LIMIT 0 0
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 0],
         ['Index', 'Counter'],
         ['Index', 'Counter'],
         [['Index', 'Depleter'], ['Network', 'Counter']],
         [['Index'], ['Network', 'Counter']]),

        # WITHCOUNT + LIMIT
        # No sorter, limit results
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 50],
         ['Index', 'Depleter', 'Pager/Limiter'],
         ['Index', 'Depleter', 'Pager/Limiter'],
         [['Index', 'Depleter'], ['Network', 'Depleter', 'Pager/Limiter']],
         [['Index', 'Depleter'], ['Network', 'Depleter', 'Pager/Limiter']]),

        # WITHCOUNT + SORTBY 0
        # No sorter, no limit, returns all results
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0'],
         ['Index', 'Depleter'],
         ['Index'],
         [['Index', 'Depleter'], ['Network', 'Depleter']],
         [['Index'], ['Network']]),

        # WITHCOUNT + SORTBY
        # Sorter, limit results to DEFAULT_LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'],
         ['Index', 'Sorter', 'Pager/Limiter'],
         ['Index', 'Sorter', 'Pager/Limiter'],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter', 'Pager/Limiter']],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter', 'Pager/Limiter']]),

        # WITHCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50],
         ['Index', 'Depleter', 'Sorter', 'Pager/Limiter'],
         ['Index', 'Depleter', 'Sorter', 'Pager/Limiter'],
         [['Index', 'Depleter', 'Sorter', 'Loader'], ['Network', 'Depleter', 'Sorter', 'Pager/Limiter']],
         [['Index', 'Depleter', 'Sorter', 'Loader'], ['Network', 'Depleter', 'Sorter', 'Pager/Limiter']]),

        # WITHCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title'],
         ['Index', 'Loader', 'Depleter'],
         ['Index', 'Loader'],
         [['Index', 'Loader', 'Depleter'], ['Network', 'Depleter']],
         [['Index', 'Loader'], ['Network']]),

        # ----------------------------------------------------------------------
        # WITHOUTCOUNT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT'],
         ['Index'],
         ['Index'],
         [['Index'], ['Network']],
         [['Index'], ['Network']]),

        # WITHOUTCOUNT + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 50],
         ['Index', 'Pager/Limiter'],
         ['Index', 'Pager/Limiter'],
         [['Index', 'Pager/Limiter'], ['Network', 'Pager/Limiter']],
         [['Index', 'Pager/Limiter'], ['Network', 'Pager/Limiter']]),

        # WITHOUTCOUNT + SORTBY 0
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0'],
         ['Index'],
         ['Index'],
         [['Index'], ['Network']],
         [['Index'], ['Network']]),

        # WITHOUTCOUNT + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'],
         ['Index', 'Sorter'],
         ['Index', 'Sorter'],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']]),

        # WITHOUTCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50],
         ['Index', 'Sorter', 'Pager/Limiter'],
         ['Index', 'Sorter', 'Pager/Limiter'],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter', 'Pager/Limiter']],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter', 'Pager/Limiter']]),
    ]

    for (query, resp2_standalone, resp3_standalone, resp2_cluster,
         resp3_cluster) in queries_and_profiles:
        cmd=' '.join(str(x) for x in query)
        ftprofile = _translate_query_to_profile_query(query)
        res = env.cmd(*ftprofile)

        if env.isCluster():
            message = f'{cmd}: RP_list != expected: RESP{env.protocol}, Cluster'
            cluster_RP_list = _get_cluster_RP_profile(env, res)
            if env.protocol == 2:
                env.assertEqual(cluster_RP_list, resp2_cluster,
                                message=message)
            else:
                env.assertEqual(cluster_RP_list, resp3_cluster,
                                message=message)
        else:
            message = f'{cmd}: RP_list != expected: RESP{env.protocol}, Standalone'
            standalone_RP_list = _get_standalone_RP_profile(env, res)
            if env.protocol == 2:
                env.assertEqual(standalone_RP_list, resp2_standalone,
                                message=message)
            else:
                env.assertEqual(standalone_RP_list, resp3_standalone,
                                message=message)


def test_profile_resp2():
    _test_profile(2)

def test_profile_resp3():
    _test_profile(3)

