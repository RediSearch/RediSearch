from common import *

DEFAULT_LIMIT = 10

def _setup_index_and_data(env, docs):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
                        'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                        'brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                        'description', 'TEXT', 'price', 'NUMERIC',
                        'categories', 'TAG').ok()
    conn = env.getClusterConnectionIfNeeded()

    for i in range(docs):
        title = f'Game {i}'
        brand = f'Brand {i % 25}'
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
    shard_RP_and_count = []
    if isinstance(res, dict):
        shard = res['Shards']['Shard #1']['Result processors profile']
        shard_RP_and_count.append([(item['Type'], item['Results processed']) for item in shard])

        # Extract the RP types from the coordinator
        coord = res['Coordinator']['Result processors profile']['profile']['Result processors profile']
        coord_RP_and_count = [(item['Type'], item['Results processed']) for item in coord]
        return [shard_RP_and_count, coord_RP_and_count]

    else:
        shard = res[2][6][1:]
        shard_RP_and_count.append([(item[1], item[5]) for item in shard])

        # Extract the RP types from the coordinator
        coord = res[4][1][0][4][1:]
        coord_RP_and_count = [(item[1], item[5]) for item in coord]
        return [shard_RP_and_count, coord_RP_and_count]


def _get_standalone_RP_profile(env, res) -> list:
    if isinstance(res, dict):
        profile = res['profile']['Result processors profile']
        RP_and_count = [(item['Type'], item['Results processed']) for item in profile]
        return RP_and_count
    else:
        profile = res[1][5][1:]
        RP_and_count = [(item[1], item[5]) for item in profile]
        return RP_and_count


def _translate_query_to_profile_query(query) -> list:
    profile = ['FT.PROFILE']
    profile.append(query[1])        # index name
    profile.append(query[0][3:])    # command
    profile.append('QUERY')
    profile.extend(query[2:])       # query
    return profile


def _test_limit00(protocol):
    env = Env(protocol=protocol)
    enable_unstable_features(env)
    docs = 2265
    _setup_index_and_data(env, docs)

    for on_timeout_policy in ['return', 'fail']:
        config_command = [config_cmd(), 'SET', 'ON_TIMEOUT', on_timeout_policy]
        verify_command_OK_on_all_shards(env, *config_command)
        queries_and_results = [
            (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 0], docs),
            (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 0], ANY),
            # WITHOUTCOUNT is implied by default
            (['FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 0], ANY),
        ]

        for query, expected_results in queries_and_results:
            cmd=' '.join(str(x) for x in query)
            for dialect in [1, 2, 3, 4]:
                config_command = [config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect]
                verify_command_OK_on_all_shards(env, *config_command)
                res = env.cmd(*query)
                total_results = _get_total_results(res)
                results = _get_results(res)

                # Verify results
                env.assertEqual(
                    total_results, expected_results,
                    message=f'{cmd}: total_results != expected. Dialect: {dialect}')
                env.assertEqual(
                    len(results), 0,
                    message=f'{cmd}: len(results) != 0. Dialect: {dialect}')


def test_limit00_resp3():
    _test_limit00(3)


def test_limit00_resp2():
    _test_limit00(2)


def _test_withcount(protocol):
    env = Env(protocol=protocol)
    enable_unstable_features(env)
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
        # Sorter without keys, no sorter, no limiter
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0'], docs, docs),

        # WITHCOUNT + SORTBY 0 + MAX
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0', 'MAX', 3], docs, 3),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0', 'MAX', 30], docs, 30),

        # WITHCOUNT + SORTBY
        # Sorter, limit results to DEFAULT_LIMIT
        # total_results = docs, length of results = DEFAULT_LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'], docs, DEFAULT_LIMIT),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@price'], docs, DEFAULT_LIMIT),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '2', '@title', 'ASC'], docs, DEFAULT_LIMIT),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '2', '@price', 'ASC'], docs, DEFAULT_LIMIT),

        # WITHCOUNT + SORTBY + MAX
        # total_results = docs, length of results = MAX
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title', 'MAX', 3], docs, 3),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@price', 'MAX', 4], docs, 4),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title', 'MAX', 30], docs, 30),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@price', 'MAX', 40], docs, 40),

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

        # WITHCOUNT + SORTBY + MAX + LIMIT
        # total_results = docs, length of results = LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title', 'MAX', 3, 'LIMIT', 0, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title', 'MAX', docs*2, 'LIMIT', 0, 50], docs, 50),

        # WITHCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title'], docs, docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price'], docs, docs),

        # WITHCOUNT + LOAD + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title', 'LIMIT', 0, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title', 'LIMIT', 100, docs], docs, docs - 100),

        # WITHCOUNT + GROUPBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand'], 25, 25),

        # WITHCOUNT + GROUPBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 12], 25, 12),

        # WITHCOUNT + GROUPBY + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 11], 25, 11),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 50], 25, 25),

        # WITHCOUNT + ADDSCORES
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES'], docs, docs),

        # WITHCOUNT + ADDSCORES + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'SORTBY', 1, '@title'], docs, DEFAULT_LIMIT),

        # WITHCOUNT + ADDSCORES + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'LIMIT', 0, 50], docs, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'LIMIT', 10, 50], docs, 50),

        # Enable FILTER test after backporting #6880
        # WITHCOUNT + FILTER
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200'], 200, 200),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price >= 0'], docs, docs),

        # Enable FILTER test after backporting #6880
        # WITHCOUNT + FILTER + LIMIT
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 0, 50], 200, 50),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 20, 50], 200, 50),
    ]

    for query, expected_total_results, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)
        for dialect in [1, 2, 3, 4]:
            config_command = [config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect]
            verify_command_OK_on_all_shards(env, *config_command)
            res = env.cmd(*query)
            total_results = _get_total_results(res)
            results = _get_results(res)

            # Verify results
            env.assertEqual(
                total_results, expected_total_results,
                message=f'{cmd}: total_results != expected. Dialect: {dialect}')
            env.assertEqual(
                len(results), expected_results,
                message=f'{cmd}: len(results) != expected. Dialect: {dialect}')


def test_withcount_resp3():
    _test_withcount(3)


def test_withcount_resp2():
    _test_withcount(2)


def _test_withoutcount(protocol):
    env = Env(protocol=protocol)
    enable_unstable_features(env)
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

        # WITHOUTCOUNT + SORTBY 0 + MAX
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0', 'MAX', 3], 3),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0', 'MAX', 30], 30),

        # WITHOUTCOUNT + SORTBY - backwards compatible, returns only 10 results
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'], DEFAULT_LIMIT),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@price'], DEFAULT_LIMIT), # crash
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@title', 'ASC'], DEFAULT_LIMIT),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '2', '@price', 'ASC'], DEFAULT_LIMIT), # crash

        # WITHOUTCOUNT + SORTBY + MAX
        # total_results = docs, length of results = MAX
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title', 'MAX', 3], 3),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@price', 'MAX', 4], 4),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title', 'MAX', 30], 30),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@price', 'MAX', 40], 40),

        # WITHOUTCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@price', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, int(docs/2)], int(docs/2)),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs*4], docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 10, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 100, docs], docs - 100),
        (['FT.AGGREGATE', 'idx', '@price:[1, 100]', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, docs], 100),

        # WITHOUTCOUNT + SORTBY + MAX + LIMIT
        # total_results = docs, length of results = LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title', 'MAX', 3, 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title', 'MAX', docs*2, 'LIMIT', 0, 50], 50),

        # WITHOUTCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@title'], docs),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@price'], docs),

        # WITHOUTCOUNT + LOAD + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@title', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@title', 'LIMIT', 100, docs], docs - 100),

        # WITHOUTCOUNT + GROUPBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand'], 25),

        # WITHOUTCOUNT + GROUPBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 12], 12),

        # WITHOUTCOUNT + GROUPBY + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 11], 11),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 50], 25),

        # WITHOUTCOUNT + ADDSCORES
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES'], docs),

        # WITHOUTCOUNT + ADDSCORES + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES', 'SORTBY', 1, '@title'], 10),

        # WITHOUTCOUNT + ADDSCORES + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES', 'LIMIT', 0, 50], 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES', 'LIMIT', 10, 50], 50),
    ]

    for query, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)

        # create a query without WITHOUTCOUNT to test the default behavior
        query_default = query.copy()
        query_default.remove('WITHOUTCOUNT')
        cmd_default=' '.join(str(x) for x in query_default)
        res_default = env.cmd(*query_default)

        for dialect in [1, 2, 3, 4]:
            config_command = [config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect]
            verify_command_OK_on_all_shards(env, *config_command)
            res = env.cmd(*query)

            results = _get_results(res)
            results_default = _get_results(res_default)

            # Verify only the length of results, don't verify total_results
            env.assertEqual(
                len(results), expected_results,
                message=f'{cmd}: len(results) != expected. Dialect: {dialect}')

            # Compare with the query without WITHOUTCOUNT
            env.assertEqual(
                len(results_default), expected_results,
                message=f'{cmd_default}: len(results) != results_default. Dialect: {dialect}')


def test_withoutcount_resp3():
    _test_withoutcount(3)


def test_withoutcount_resp2():
    _test_withoutcount(2)


def _test_profile(protocol):
    env = Env(protocol=protocol)
    enable_unstable_features(env)
    docs = 3100
    _setup_index_and_data(env, docs)

    queries_and_profiles = [
        # query,
        # RESP2/RESP3 Standalone,
        # RESP2/RESP3 [[shard[0], shard[1], shard[2]], coordinator]

        # WITHCOUNT
        # No sorter, no limit, returns all results
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT'],
         [('Index', 3100), ('Depleter', 3100)],
         [[[('Index', 1027), ('Depleter', 1027)],
           [('Index', 1032), ('Depleter', 1032)],
           [('Index', 1041), ('Depleter', 1041)]],
           [('Network', 3100), ('Depleter', 3100)]]),

        # WITHCOUNT + LIMIT 0 0
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 0],
         [('Index', 3100), ('Counter', 1)],
         [[[('Index', 1027), ('Depleter', 1027)],
           [('Index', 1032), ('Depleter', 1032)],
           [('Index', 1041), ('Depleter', 1041)]],
           [('Network', 3100), ('Counter', 1)]]),

        # WITHCOUNT + LIMIT
        # No sorter, limit results
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 50],
         [('Index', 3100), ('Depleter', 49), ('Pager/Limiter', 50)],
         [[[('Index', 1027), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1032), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1041), ('Depleter', 49), ('Pager/Limiter', 50)]],
           [('Network', 150), ('Depleter', 49), ('Pager/Limiter', 50)]]),

        # WITHCOUNT + SORTBY 0
        # Sorter without keys, default limit
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0'],
         [('Index', 3100), ('Depleter', 3100)],
         [[[('Index', 1027), ('Depleter', 1027)],
           [('Index', 1032), ('Depleter', 1032)],
           [('Index', 1041), ('Depleter', 1041)]],
           [('Network', 3100), ('Depleter', 3100)]]),

        # WITHCOUNT + SORTBY 0 + MAX
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0', 'MAX', 3],
         [('Index', 3100), ('Depleter', 2), ('Pager/Limiter', 3)],
         [[[('Index', 1027), ('Depleter', 2), ('Pager/Limiter', 3)],
           [('Index', 1032), ('Depleter', 2), ('Pager/Limiter', 3)],
           [('Index', 1041), ('Depleter', 2), ('Pager/Limiter', 3)]],
           [('Network', 9), ('Depleter', 2), ('Pager/Limiter', 3)]]),

        # WITHCOUNT + SORTBY 0 + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0', 'LIMIT', 0, 50],
         [('Index', 3100), ('Depleter', 49), ('Pager/Limiter', 50)],
         [[[('Index', 1027), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1032), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1041), ('Depleter', 49), ('Pager/Limiter', 50)]],
           [('Network', 150), ('Depleter', 49), ('Pager/Limiter', 50)]]),

        # WITHCOUNT + SORTBY
        # Sorter, limit results to DEFAULT_LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'],
         [('Index', 3100), ('Sorter', 10)],
         [[[('Index', 1027), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1032), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1041), ('Sorter', 10), ('Loader', 10)]],
           [('Network', 30), ('Sorter', 10)]]),

        # WITHCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50],
         [('Index', 3100), ('Sorter', 50)],
         [[[('Index', 1027), ('Sorter', 50), ('Loader', 50)],
         [('Index', 1032), ('Sorter', 50), ('Loader', 50)],
         [('Index', 1041), ('Sorter', 50), ('Loader', 50)]],
         [('Network', 150), ('Sorter', 50)]]),

        # WITHCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title'],
         [('Index', 3100), ('Loader', 3100), ('Depleter', 3100)],
         [[[('Index', 1027), ('Loader', 1027), ('Depleter', 1027)],
           [('Index', 1032), ('Loader', 1032), ('Depleter', 1032)],
           [('Index', 1041), ('Loader', 1041), ('Depleter', 1041)]],
           [('Network', 3100), ('Depleter', 3100)]]),

        # WITHCOUNT + LOAD + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title', 'LIMIT', 0, 50],
         [('Index', 3100), ('Loader', 3100), ('Depleter', 49), ('Pager/Limiter', 50)],
         [[[('Index', 1027), ('Loader', 1027), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1032), ('Loader', 1032), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1041), ('Loader', 1041), ('Depleter', 49), ('Pager/Limiter', 50)]],
           [('Network', 150), ('Depleter', 49), ('Pager/Limiter', 50)]]),

        # WITHCOUNT + GROUPBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand'],
         [('Index', 3100), ('Grouper', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25)]]),

        # WITHCOUNT + GROUPBY + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand'],
         [('Index', 3100), ('Grouper', 25), ('Sorter', 10)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25), ('Sorter', 10)]]),

        # WITHCOUNT + GROUPBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 50],
         [('Index', 3100), ('Grouper', 25), ('Pager/Limiter', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25), ('Pager/Limiter', 25)]]),

        # WITHCOUNT + GROUPBY + LIMIT (stop calling before EOF)
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 25],
         [('Index', 3100), ('Grouper', 24), ('Pager/Limiter', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 24), ('Pager/Limiter', 25)]]),

        # WITHCOUNT + GROUPBY + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 50],
         [('Index', 3100), ('Grouper', 25), ('Sorter', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25), ('Sorter', 25)]]),

        # WITHCOUNT + ADDSCORES
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES'],
         [('Index', 3100), ('Scorer', 3100), ('Depleter', 3100)],
         [[[('Index', 1027), ('Scorer', 1027), ('Depleter', 1027)],
           [('Index', 1032), ('Scorer', 1032), ('Depleter', 1032)],
           [('Index', 1041), ('Scorer', 1041), ('Depleter', 1041)]],
           [('Network', 3100), ('Depleter', 3100)]]),

        # WITHCOUNT + ADDSCORES + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'SORTBY', 1, '@title'],
         [('Index', 3100), ('Scorer', 3100), ('Sorter', 10)],
         [[[('Index', 1027), ('Scorer', 1027), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1032), ('Scorer', 1032), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1041), ('Scorer', 1041), ('Sorter', 10), ('Loader', 10)]],
           [('Network', 30), ('Sorter', 10)]]),

        # WITHCOUNT + ADDSCORES + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'LIMIT', 0, 50],
         [('Index', 3100), ('Scorer', 3100), ('Depleter', 49), ('Pager/Limiter', 50)],
         [[[('Index', 1027), ('Scorer', 1027), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1032), ('Scorer', 1032), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1041), ('Scorer', 1041), ('Depleter', 49), ('Pager/Limiter', 50)]],
           [('Network', 150), ('Depleter', 49), ('Pager/Limiter', 50)]]),

        # WITHCOUNT + FILTER
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200'],
         [('Index', 3100), ('Loader', 3100), ('Filter - Predicate <', 200), ('Depleter', 200)],
         [[[('Index', 1027), ('Loader', 1027), ('Filter - Predicate <', 64), ('Depleter', 64)],
           [('Index', 1032), ('Loader', 1032), ('Filter - Predicate <', 68), ('Depleter', 68)],
           [('Index', 1041), ('Loader', 1041), ('Filter - Predicate <', 68), ('Depleter', 68)]],
           [('Network', 200), ('Depleter', 200)]]),

        # WITHCOUNT + FILTER + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 0, 50],
         [('Index', 3100), ('Loader', 3100), ('Filter - Predicate <', 200), ('Depleter', 49), ('Pager/Limiter', 50)],
         [[[('Index', 1027), ('Loader', 1027), ('Filter - Predicate <', 64), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1032), ('Loader', 1032), ('Filter - Predicate <', 68), ('Depleter', 49), ('Pager/Limiter', 50)],
           [('Index', 1041), ('Loader', 1041), ('Filter - Predicate <', 68), ('Depleter', 49), ('Pager/Limiter', 50)]],
           [('Network', 150), ('Depleter', 49), ('Pager/Limiter', 50)]]),

        # ----------------------------------------------------------------------
        # WITHOUTCOUNT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT'],
         [('Index', 3100)],
         [[[('Index', 1027)],
           [('Index', 1032)],
           [('Index', 1041)]],
           [('Network', 3100)]]),

        # WITHOUTCOUNT implicit (by default)
        (['FT.AGGREGATE', 'idx', '*'],
         [('Index', 3100)],
         [[[('Index', 1027)],
           [('Index', 1032)],
           [('Index', 1041)]],
           [('Network', 3100)]]),

        # WITHOUTCOUNT + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 50],
         [('Index', 49), ('Pager/Limiter', 50)],
         [[[('Index', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Pager/Limiter', 50)]],
           [('Network', 49), ('Pager/Limiter', 50)]]),

         # WITHOUTCOUNT (implicit) + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 50],
         [('Index', 49), ('Pager/Limiter', 50)],
         [[[('Index', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Pager/Limiter', 50)]],
           [('Network', 49), ('Pager/Limiter', 50)]]),

        # WITHOUTCOUNT + SORTBY 0
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0'],
         [('Index', 3100)],
         [[[('Index', 1027)],
           [('Index', 1032)],
           [('Index', 1041)]],
           [('Network', 3100)]]),

        # WITHOUTCOUNT + SORTBY 0 + MAX
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0', 'MAX', 3],
         [('Index', 2), ('Pager/Limiter', 3)],
         [[[('Index', 2), ('Pager/Limiter', 3)],
           [('Index', 2), ('Pager/Limiter', 3)],
           [('Index', 2), ('Pager/Limiter', 3)]],
           [('Network', 2), ('Pager/Limiter', 3)]]),

        # WITHOUTCOUNT + SORTBY 0 + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0', 'LIMIT', 0, 50],
         [('Index', 49), ('Pager/Limiter', 50)],
         [[[('Index', 49), ('Pager/Limiter', 50)], [('Index', 49), ('Pager/Limiter', 50)], [('Index', 49), ('Pager/Limiter', 50)]], [('Network', 49), ('Pager/Limiter', 50)]]),

        # WITHOUTCOUNT + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'],
         [('Index', 3100), ('Sorter', 10)],
         [[[('Index', 1027), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1032), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1041), ('Sorter', 10), ('Loader', 10)]],
           [('Network', 30), ('Sorter', 10)]]),

        # WITHOUTCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50],
         [('Index', 3100), ('Sorter', 50)],
         [[[('Index', 1027), ('Sorter', 50), ('Loader', 50)],
           [('Index', 1032), ('Sorter', 50), ('Loader', 50)],
           [('Index', 1041), ('Sorter', 50), ('Loader', 50)]],
           [('Network', 150), ('Sorter', 50)]]),

        # WITHOUTCOUNT + GROUPBY + LIMIT (stop calling before EOF)
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 25],
         [('Index', 3100), ('Grouper', 24), ('Pager/Limiter', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 24), ('Pager/Limiter', 25)]]),

         # WITHOUTCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@title'],
         [('Index', 3100), ('Loader', 3100)],
         [[[('Index', 1027), ('Loader', 1027)],
           [('Index', 1032), ('Loader', 1032)],
           [('Index', 1041), ('Loader', 1041)]],
           [('Network', 3100)]]),

         # WITHOUTCOUNT + GROUPBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand'],
         [('Index', 3100), ('Grouper', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25)]]),

        # WITHOUTCOUNT + GROUPBY + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand'],
         [('Index', 3100), ('Grouper', 25), ('Sorter', 10)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25), ('Sorter', 10)]]),

        # WITHOUTCOUNT + GROUPBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 50],
         [('Index', 3100), ('Grouper', 25), ('Pager/Limiter', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25), ('Pager/Limiter', 25)]]),

        # WITHOUTCOUNT + GROUPBY + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 50],
         [('Index', 3100), ('Grouper', 25), ('Sorter', 25)],
         [[[('Index', 1027), ('Grouper', 25)],
           [('Index', 1032), ('Grouper', 25)],
           [('Index', 1041), ('Grouper', 25)]],
           [('Network', 75), ('Grouper', 25), ('Sorter', 25)]]),

        # WITHOUTCOUNT + ADDSCORES
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES'],
         [('Index', 3100), ('Scorer', 3100)],
         [[[('Index', 1027), ('Scorer', 1027)],
           [('Index', 1032), ('Scorer', 1032)],
           [('Index', 1041), ('Scorer', 1041)]],
           [('Network', 3100)]]),

        # WITHOUTCOUNT + ADDSCORES + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES', 'SORTBY', 1, '@title'],
         [('Index', 3100), ('Scorer', 3100), ('Sorter', 10)],
         [[[('Index', 1027), ('Scorer', 1027), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1032), ('Scorer', 1032), ('Sorter', 10), ('Loader', 10)],
           [('Index', 1041), ('Scorer', 1041), ('Sorter', 10), ('Loader', 10)]],
           [('Network', 30), ('Sorter', 10)]]),

        # WITHOUTCOUNT + ADDSCORES + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES', 'LIMIT', 0, 50],
         [('Index', 49), ('Scorer', 49), ('Pager/Limiter', 50)],
         [[[('Index', 49), ('Scorer', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Scorer', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Scorer', 49), ('Pager/Limiter', 50)]],
           [('Network', 49), ('Pager/Limiter', 50)]]),

        # WITHOUTCOUNT + FILTER
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200'],
         [('Index', 3100), ('Loader', 3100), ('Filter - Predicate <', 200)],
         [[[('Index', 1027), ('Loader', 1027), ('Filter - Predicate <', 64)],
           [('Index', 1032), ('Loader', 1032), ('Filter - Predicate <', 68)],
           [('Index', 1041), ('Loader', 1041), ('Filter - Predicate <', 68)]],
           [('Network', 200)]]),

        # WITHOUTCOUNT + FILTER + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 0, 50],
         [('Index', 49), ('Loader', 49), ('Filter - Predicate <', 49), ('Pager/Limiter', 50)],
         [[[('Index', 49), ('Loader', 49), ('Filter - Predicate <', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Loader', 49), ('Filter - Predicate <', 49), ('Pager/Limiter', 50)],
           [('Index', 49), ('Loader', 49), ('Filter - Predicate <', 49), ('Pager/Limiter', 50)]],
           [('Network', 49), ('Pager/Limiter', 50)]]),
    ]

    for (query, standalone, cluster) in queries_and_profiles:
        cmd=' '.join(str(x) for x in query)
        ftprofile = _translate_query_to_profile_query(query)
        res = env.cmd(*ftprofile)

        if env.isCluster():
            message = f'{cmd}: RP_list != expected: RESP{env.protocol}, Cluster'
            cluster_RP_list = _get_cluster_RP_profile(env, res)
            # Only one shard is returned in the profile, so we need to check if
            # the expected shard is in the list
            expected_shard = cluster_RP_list[0][0]
            found = False
            for shard in cluster_RP_list[0]:
                if shard == expected_shard:
                    found = True
                    break
            env.assertTrue(found, message=message)

            expected_coord = cluster_RP_list[1]
            env.assertEqual(cluster_RP_list[1], expected_coord,
                            message=message)
        else:
            message = f'{cmd}: RP_list != expected: RESP{env.protocol}, Standalone'
            standalone_RP_list = _get_standalone_RP_profile(env, res)
            env.assertEqual(standalone_RP_list, standalone,
                            message=message)


def test_profile_resp2():
    _test_profile(2)

def test_profile_resp3():
    _test_profile(3)

def test_withcursor(env):
    env = Env()
    enable_unstable_features(env)
    docs = 5
    _setup_index_and_data(env, docs)

    invalid_queries = [
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR', 'COUNT', 10],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 10, 'WITHCOUNT'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'WITHCOUNT'],
    ]
    error_message = 'FT.AGGREGATE does not support using WITHCOUNT and WITHCURSOR together'
    for query in invalid_queries:
        env.expect(*query).error().contains(error_message)

    valid_queries = [
        ['FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 10],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCURSOR'],
    ]
    for query in valid_queries:
        env.expect(*query).notContains(error_message)

def _test_pagers(protocol):
    env = Env(protocol=protocol)
    enable_unstable_features(env)
    docs = 10
    _setup_index_and_data(env, docs)

    queries = [
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title', 'SORTBY', 1, '@title'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title', 'GROUPBY', 1, '@brand'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'LOAD', 1, '@title'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'LOAD', 1, '@title', 'SORTBY', 2, '@price', 'DESC'],
    ]
    for query in queries:
        limit = 6
        offset = 2
        query1 = query + ['LIMIT', 0, limit]
        query2 = query + ['LIMIT', offset, limit]
        res1 = env.cmd(*query1)
        res2 = env.cmd(*query2)

        # Compare total_results
        total_results1 = _get_total_results(res1)
        total_results2 = _get_total_results(res2)
        env.assertEqual(total_results1, total_results2)

        # Compare length of results
        results1 = _get_results(res1)
        results2 = _get_results(res2)
        env.assertEqual(len(results1), len(results2))

        # Compare common part of the results
        if any(x in query for x in ('SORTBY', 'GROUPBY')):
            env.assertEqual(results1[offset:limit + offset + 1],
                            results2[0:limit - offset], message=query)


def test_pagers_resp2():
    _test_pagers(2)


def test_pagers_resp3():
    _test_pagers(3)


def test_unstable_features_guard(env):
    """Test that unstable features are disabled by default"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    _setup_index_and_data(env, 1)
    env.expect('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT').error()\
        .contains('FT.AGGREGATE + WITHCOUNT is not available when `ENABLE_UNSTABLE_FEATURES` is off')
