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
    if isinstance(res, dict):
        shard = res['Profile']['Shards'][0]['Result processors profile']
        coord = res['Profile']['Coordinator']['Result processors profile']
        shard_RP = [item['Type'] for item in shard]
        coord_RP = [item['Type'] for item in coord]

        for i in range(3):
            shard = res['Profile']['Shards'][i]['Result processors profile']
            shard_RP_and_count = [(item['Type'], item['Results processed']) for item in shard]
            print(f"shard[{i}]: {shard_RP_and_count}")
        coord_RP_and_count = [(item['Type'], item['Results processed']) for item in coord]
        print(f"coord: {coord_RP_and_count}")
        return [shard_RP, coord_RP]
    else:
        shard = res[1][1][0][13]
        coord = res[1][3][11]
        shard_RP = [item[1] for item in shard]
        coord_RP = [item[1] for item in coord]

        for i in range(3):
            shard = res[1][1][i][13]
            shard_RP_and_count = [(item[1], item[5]) for item in shard]
            print(f"shard[{i}]: {shard_RP_and_count}")
        coord_RP_and_count = [(item[1], item[5]) for item in coord]

        print(f"coord_RP_and_count: {coord_RP_and_count}")
        return [shard_RP, coord_RP]


def _get_standalone_RP_profile(env, res) -> list:
    if isinstance(res, dict):
        profile = res['Profile']['Shards'][0]['Result processors profile']
        RP_and_count = [(item['Type'], item['Results processed']) for item in profile]
        print(f"RP_and_count: {RP_and_count}")
        return [item['Type'] for item in profile]
    else:
        profile = res[1][1][0][13]
        RP_and_count = [(item[1], item[5]) for item in profile]
        print(f"RP_and_count: {RP_and_count}")
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
        config_cmd = ['CONFIG', 'SET', 'search-on-timeout', on_timeout_policy]
        verify_command_OK_on_all_shards(env, *config_cmd)
        queries_and_results = [
            (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 0], docs),
            (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', 0, 0], ANY),
            # WITHOUTCOUNT is implied by default
            (['FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 0], ANY),
        ]

        for query, expected_results in queries_and_results:
            cmd=' '.join(str(x) for x in query)
            for dialect in [1, 2, 3, 4]:
                config_cmd = ['CONFIG', 'SET', 'search-default-dialect', dialect]
                verify_command_OK_on_all_shards(env, *config_cmd)
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
        # Sorter without keys, default limit
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0'], docs, DEFAULT_LIMIT),

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
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50], docs, 50), # Sanitizer error
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

        # WITHCOUNT + FILTER
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200'], 200, 200),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price >= 0'], docs, docs),

        # WITHCOUNT + FILTER + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 0, 50], 200, 50),
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 20, 50], 200, 50),
    ]

    for query, expected_total_results, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)
        for dialect in [1, 2, 3, 4]:
            config_cmd = ['CONFIG', 'SET', 'search-default-dialect', dialect]
            verify_command_OK_on_all_shards(env, *config_cmd)
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
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0'], DEFAULT_LIMIT),

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
            config_cmd = ['CONFIG', 'SET', 'search-default-dialect', dialect]
            verify_command_OK_on_all_shards(env, *config_cmd)
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
         ['Index', 'Sync Depleter'],
         ['Index', 'Sync Depleter'],
         [['Index', 'Sync Depleter'], ['Network', 'Sync Depleter']],
         [['Index', 'Sync Depleter'], ['Network', 'Sync Depleter']]),

        # WITHCOUNT + LIMIT 0 0
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 0],
         ['Index', 'Counter'],
         ['Index', 'Counter'],
         [['Index', 'Sync Depleter'], ['Network', 'Counter']],
         [['Index', 'Sync Depleter'], ['Network', 'Counter']]),

        # WITHCOUNT + LIMIT
        # No sorter, limit results
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 50],
         ['Index', 'Sync Depleter', 'Pager/Limiter'],
         ['Index', 'Sync Depleter', 'Pager/Limiter'],
         [['Index', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']],
         [['Index', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']]),

        # WITHCOUNT + SORTBY 0
        # Sorter without keys, default limit
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '0'],
         ['Index', 'Sorter'],
         ['Index', 'Sorter'],
         [['Index', 'Sorter'], ['Network', 'Sorter']],
         [['Index', 'Sorter'], ['Network', 'Sorter']]),

        # WITHCOUNT + SORTBY
        # Sorter, limit results to DEFAULT_LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', '1', '@title'],
         ['Index', 'Sorter'],
         ['Index', 'Sorter'],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']]),

        # WITHCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50],
         ['Index', 'Sorter'],
         ['Index', 'Sorter'],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']]),

        # WITHCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title'],
         ['Index', 'Loader', 'Sync Depleter'],
         ['Index', 'Loader', 'Sync Depleter'],
         [['Index', 'Loader', 'Sync Depleter'], ['Network', 'Sync Depleter']],
         [['Index', 'Loader', 'Sync Depleter'], ['Network', 'Sync Depleter']]),

        # WITHCOUNT + LOAD + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title', 'LIMIT', 0, 50],
         ['Index', 'Loader', 'Sync Depleter', 'Pager/Limiter'],
         ['Index', 'Loader', 'Sync Depleter', 'Pager/Limiter'],
         [['Index', 'Loader', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']],
         [['Index', 'Loader', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']]),

        # WITHCOUNT + GROUPBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand'],
         ['Index', 'Grouper'],
         ['Index', 'Grouper'],
         [['Index', 'Grouper'], ['Network', 'Grouper']],
         [['Index', 'Grouper'], ['Network', 'Grouper']]),

        # WITHCOUNT + GROUPBY + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand'],
         ['Index', 'Grouper', 'Sorter'],
         ['Index', 'Grouper', 'Sorter'],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']]),

        # WITHCOUNT + GROUPBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 50],
         ['Index', 'Grouper', 'Sync Depleter', 'Pager/Limiter'],
         ['Index', 'Grouper', 'Sync Depleter', 'Pager/Limiter'],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sync Depleter', 'Pager/Limiter']],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sync Depleter', 'Pager/Limiter']]),

        # WITHCOUNT + GROUPBY + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 50],
         ['Index', 'Grouper', 'Sorter'],
         ['Index', 'Grouper', 'Sorter'],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']]),

        # WITHCOUNT + ADDSCORES
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES'],
         ['Index', 'Scorer', 'Sync Depleter'],
         ['Index', 'Scorer', 'Sync Depleter'],
         [['Index', 'Scorer', 'Sync Depleter'], ['Network', 'Sync Depleter']],
         [['Index', 'Scorer', 'Sync Depleter'], ['Network', 'Sync Depleter']]),

        # WITHCOUNT + ADDSCORES + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'SORTBY', 1, '@title'],
         ['Index', 'Scorer', 'Sorter'],
         ['Index', 'Scorer', 'Sorter'],
         [['Index', 'Scorer', 'Sorter', 'Loader'], ['Network', 'Sorter']],
         [['Index', 'Scorer', 'Sorter', 'Loader'], ['Network', 'Sorter']]),

        # WITHCOUNT + ADDSCORES + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'LIMIT', 0, 50],
         ['Index', 'Scorer', 'Sync Depleter', 'Pager/Limiter'],
         ['Index', 'Scorer', 'Sync Depleter', 'Pager/Limiter'],
         [['Index', 'Scorer', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']],
         [['Index', 'Scorer', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']]),

        # WITHCOUNT + FILTER
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200'],
         ['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter'],
         ['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter'],
         [['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter'], ['Network', 'Sync Depleter']],
         [['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter'], ['Network', 'Sync Depleter']]),

        # WITHCOUNT + FILTER + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 0, 50],
         ['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter', 'Pager/Limiter'],
         ['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter', 'Pager/Limiter'],
         [['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']],
         [['Index', 'Loader', 'Filter - Predicate <', 'Sync Depleter', 'Pager/Limiter'], ['Network', 'Sync Depleter', 'Pager/Limiter']]),

        # ----------------------------------------------------------------------
        # WITHOUTCOUNT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT'],
         ['Index'],
         ['Index'],
         [['Index'], ['Network']],
         [['Index'], ['Network']]),

        # WITHOUTCOUNT implicit (by default)
        (['FT.AGGREGATE', 'idx', '*'],
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

         # WITHOUTCOUNT (implicit) + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 50],
         ['Index', 'Pager/Limiter'],
         ['Index', 'Pager/Limiter'],
         [['Index', 'Pager/Limiter'], ['Network', 'Pager/Limiter']],
         [['Index', 'Pager/Limiter'], ['Network', 'Pager/Limiter']]),

        # WITHOUTCOUNT + SORTBY 0
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0'],
         ['Index', 'Pager/Limiter'],        # Bug no related to WITHCOUNT change
         ['Index', 'Pager/Limiter'],        # Bug no related to WITHCOUNT change
         [['Index', 'Sorter'], ['Network', 'Sorter']],
         [['Index', 'Sorter'], ['Network', 'Sorter']]),

        # WITHOUTCOUNT + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'],
         ['Index', 'Pager/Limiter'],        # Bug no related to WITHCOUNT change
         ['Index', 'Pager/Limiter'],        # Bug no related to WITHCOUNT change
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']]),

        # WITHOUTCOUNT + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 50],
         ['Index', 'Pager/Limiter'],
         ['Index', 'Pager/Limiter'],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']],
         [['Index', 'Sorter', 'Loader'], ['Network', 'Sorter']]),

         # WITHOUTCOUNT + LOAD
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@title'],
         ['Index', 'Loader',],
         ['Index', 'Loader'],
         [['Index', 'Loader',], ['Network']],
         [['Index', 'Loader'], ['Network']]),

         # WITHOUTCOUNT + GROUPBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand'],
         ['Index', 'Grouper'],
         ['Index', 'Grouper'],
         [['Index', 'Grouper'], ['Network', 'Grouper']],
         [['Index', 'Grouper'], ['Network', 'Grouper']]),

        # WITHOUTCOUNT + GROUPBY + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand'],
         ['Index', 'Grouper'],
         ['Index', 'Grouper'],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']]),

        # WITHOUTCOUNT + GROUPBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 50],
         ['Index', 'Grouper', 'Pager/Limiter'],
         ['Index', 'Grouper', 'Pager/Limiter'],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Pager/Limiter']],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Pager/Limiter']]),

        # WITHOUTCOUNT + GROUPBY + SORTBY + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 50],
         ['Index', 'Grouper', 'Pager/Limiter'],
         ['Index', 'Grouper', 'Pager/Limiter'],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']],
         [['Index', 'Grouper'], ['Network', 'Grouper', 'Sorter']]),

        # WITHOUTCOUNT + ADDSCORES
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES'],
         ['Index', 'Scorer'],
         ['Index', 'Scorer'],
         [['Index', 'Scorer'], ['Network']],
         [['Index', 'Scorer'], ['Network']]),

        # WITHOUTCOUNT + ADDSCORES + SORTBY
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES', 'SORTBY', 1, '@title'],
         ['Index', 'Scorer'],
         ['Index', 'Scorer'],
         [['Index', 'Scorer', 'Sorter', 'Loader'], ['Network', 'Sorter']],
         [['Index', 'Scorer', 'Sorter', 'Loader'], ['Network', 'Sorter']]),

        # WITHOUTCOUNT + ADDSCORES + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'ADDSCORES', 'LIMIT', 0, 50],
         ['Index', 'Scorer', 'Pager/Limiter'],
         ['Index', 'Scorer', 'Pager/Limiter'],
         [['Index', 'Scorer', 'Pager/Limiter'], ['Network', 'Pager/Limiter']],
         [['Index', 'Scorer', 'Pager/Limiter'], ['Network', 'Pager/Limiter']]),

        # WITHOUTCOUNT + FILTER
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200'],
         ['Index', 'Loader', 'Filter - Predicate <'],
         ['Index', 'Loader', 'Filter - Predicate <'],
         [['Index', 'Loader', 'Filter - Predicate <'], ['Network']],
         [['Index', 'Loader', 'Filter - Predicate <'], ['Network']]),

        # WITHOUTCOUNT + FILTER + LIMIT
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 0, 50],
         ['Index', 'Loader', 'Filter - Predicate <', 'Pager/Limiter'],
         ['Index', 'Loader', 'Filter - Predicate <', 'Pager/Limiter'],
         [['Index', 'Loader', 'Filter - Predicate <', 'Pager/Limiter'], ['Network', 'Pager/Limiter']],
         [['Index', 'Loader', 'Filter - Predicate <', 'Pager/Limiter'], ['Network', 'Pager/Limiter']]),
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

def test_withcursor(env):
    env = Env()
    docs = 5
    _setup_index_and_data(env, docs)

    invalid_queries = [
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR', 'COUNT', 10],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'WITHCURSOR'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 10, 'WITHCOUNT'],
        ['FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'WITHCOUNT'],
    ]
    error_message = 'WITHCURSOR is not supported when using FT.AGGREGATE and WITHCOUNT'
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

def _test_max_aggregate_results(env):
    env = Env()
    docs = 1000
    _setup_index_and_data(env, docs)
    query = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title']
    for dialect in [1, 2, 3, 4]:
        config_cmd = ['CONFIG', 'SET', 'search-default-dialect', dialect]
        verify_command_OK_on_all_shards(env, *config_cmd)
        config_cmd = ['CONFIG', 'SET', 'search-max-aggregate-results', 10]
        verify_command_OK_on_all_shards(env, *config_cmd)
        res = env.cmd(*query)
        print(res)
        results = _get_results(res)
        env.assertEqual(len(results), 1000)
        env.assertEqual(_get_total_results(res), 1000)

@skip()
def test_max_aggregate_results_resp2():
    _test_max_aggregate_results(2)

@skip()
def test_max_aggregate_results_resp3():
    _test_max_aggregate_results(3)

def _test_query(protocol):
    env = Env(protocol=protocol)
    docs = 15000
    _setup_index_and_data(env, docs)

    queries_and_results = [
        # query, total_results, length of results
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT'], docs, docs),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 60], docs, 60),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 10, 65], docs, 65),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 0], docs, 0,),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 0], docs, DEFAULT_LIMIT),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'MAX', 3], docs, 3),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'MAX', 3, 'LIMIT', 0, 50], docs, 50),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'SORTBY', 1, '@title', 'LIMIT', 0, 75], docs, 75),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title'], docs, docs),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@title', 'LIMIT', 0, 50], docs, 50),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand'], 25, 25),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'LIMIT', 0, 12], 25, 12),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'GROUPBY', 1, '@brand', 'SORTBY', 1, '@brand', 'LIMIT', 0, 11], 25, 11),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES'], docs, docs),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'SORTBY', 1, '@title'], docs, DEFAULT_LIMIT),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'ADDSCORES', 'LIMIT', 0, 50], docs, 50),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200'], 200, 200),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@price', 'FILTER', '@price < 200', 'LIMIT', 0, 50], 200, 50),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0'], 1, DEFAULT_LIMIT),
        # (['FT.AGGREGATE', 'idx', '*', 'SORTBY', '0'], docs, DEFAULT_LIMIT),
        (['FT.AGGREGATE', 'idx', '*', 'SORTBY', '1', '@title'], 1, DEFAULT_LIMIT),
        (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '1', '@title'], 1, DEFAULT_LIMIT),
        # (['FT.AGGREGATE', 'idx', '*', 'SORTBY', '0'], docs, DEFAULT_LIMIT),
        # (['FT.AGGREGATE', 'idx', '*', 'WITHOUTCOUNT', 'SORTBY', '0'], docs, DEFAULT_LIMIT),
    ]

    for query, expected_total_results, expected_results in queries_and_results:
        cmd=' '.join(str(x) for x in query)
        print("---------------------------------")
        print(cmd)
        for dialect in [2]:
            # config_cmd = ['CONFIG', 'SET', 'search-max-aggregate-results', 500000]
            # verify_command_OK_on_all_shards(env, *config_cmd)
            config_cmd = ['CONFIG', 'SET', 'search-default-dialect', dialect]
            verify_command_OK_on_all_shards(env, *config_cmd)
            res = env.cmd(*query)
            # print(res)
            total_results = _get_total_results(res)
            results = _get_results(res)
            print(f"total_results: {total_results}, len(results): {len(results)}")

            # Verify results
            env.assertEqual(
                total_results, expected_total_results,
                message=f'{cmd}: total_results != expected. Dialect: {dialect}')
            env.assertEqual(
                len(results), expected_results,
                message=f'{cmd}: len(results) != expected. Dialect: {dialect}')

        # print profile
        ftprofile = _translate_query_to_profile_query(query)
        res = env.cmd(*ftprofile)
        # print(res)

        if env.isCluster():
            cluster_RP_list = _get_cluster_RP_profile(env, res)
            print(cluster_RP_list)
        else:
            standalone_RP_list = _get_standalone_RP_profile(env, res)
            print(standalone_RP_list)

def test_query_resp2():
    _test_query(2)

def test_query_resp3():
    _test_query(3)

