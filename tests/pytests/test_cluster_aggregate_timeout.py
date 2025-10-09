"""
Test for FT.AGGREGATE in cluster mode with random shard timeouts.
This test only runs in cluster mode and simulates random timeouts on shards
during aggregate operations.
"""

import random
from common import *

def configure_shards_with_different_timeout_policies(env):
    """Configure shards with different timeout policies"""
    #env.cmd(debug_cmd(), 'ALLOW_ZERO_TIMEOUT', 'true')
    env.cmd('CONFIG', 'SET', 'search-timeout', '1')
    res = env.cmd('CONFIG', 'get', 'search-timeout')
    print(f"JOAN res of CONFIG set search-on-timeout: {res}")
    for shard_id in range(1, env.shardsCount + 1):
        conn = env.getConnection(shard_id)
        print(f'Conn {shard_id} port is {conn.connection_pool.connection_kwargs["port"]}')

        if shard_id == 1:
            # First shard uses RETURN policy
            conn.execute_command('CONFIG', 'SET', 'search-on-timeout', 'return')
        else:
            # All other shards use FAIL policy
            conn.execute_command('CONFIG', 'SET', 'search-on-timeout', 'fail')

@skip(cluster=False)
def test_cluster_aggregate_random_timeout(env):
    """
    Test FT.AGGREGATE in cluster mode where one of the shards randomly times out
    during private command processing.
    """
    if not env.isCluster():
        env.skip()

    configure_shards_with_different_timeout_policies(env)

    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n JOAAAAN HERE AFTER CONFIGURATION \n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n" )
    time.sleep(10)

    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'title', 'TEXT', 'SORTABLE',
               'price', 'NUMERIC', 'SORTABLE',
               'category', 'TAG', 'SORTABLE',
               'brand', 'TEXT', 'NOSTEM',
               'rating', 'NUMERIC',
               'stock', 'NUMERIC',
               'tags', 'TAG',
               'created_date', 'NUMERIC').ok()

    conn = getConnectionByEnv(env)
    num_docs = 100000

    brands = ['Apple', 'Samsung', 'Sony', 'LG', 'Dell', 'HP', 'Nike', 'Adidas', 'Zara', 'H&M']

    for i in range(num_docs):
        doc_key = f'doc:{i}'
        title = f'Product {i} title with keywords'
        price = random.randint(10, 1000)
        category = random.choice(['electronics', 'books', 'clothing', 'home'])
        brand = random.choice(brands)
        rating = round(random.uniform(1.0, 5.0), 1)
        stock = random.randint(0, 100)
        tags = ','.join(random.sample(['new', 'sale', 'premium', 'bestseller', 'limited'], k=random.randint(1, 3)))
        created_date = random.randint(1640995200, 1672531200)  # 2022-2023 timestamps

        conn.execute_command('HSET', doc_key,
                           'title', title,
                           'price', price,
                           'category', category,
                           'brand', brand,
                           'rating', rating,
                           'stock', stock,
                           'tags', tags,
                           'created_date', created_date)

    waitForIndex(env, 'idx')

    info = index_info(env, 'idx')
    env.assertGreater(int(info['num_docs']), 0)

    for _ in range(10):
      # Make sure it checks all shards as coord
      result = env.cmd('FT.AGGREGATE', 'idx', '*',
                      'GROUPBY', '1', '@category',
                      'REDUCE', 'COUNT', '0', 'AS', 'count',
                      'REDUCE', 'AVG', '1', '@price', 'AS', 'avg_price')


      print(result)

      env.assertGreater(len(result), 1)  # Should have results

      # Complex aggregate with multiple grouping, filtering, and calculations
      result = env.cmd('FT.AGGREGATE', 'idx', '@category:{electronics|clothing}',
                      'FILTER', '@price >= 50 && @rating >= 3.0',
                      'APPLY', 'floor(@price/100)*100', 'AS', 'price_range',
                      'APPLY', 'if(@stock > 50, "high", if(@stock > 20, "medium", "low"))', 'AS', 'stock_level',
                      'GROUPBY', '3', '@category', '@brand', '@stock_level',
                      'REDUCE', 'COUNT', '0', 'AS', 'product_count',
                      'REDUCE', 'AVG', '1', '@price', 'AS', 'avg_price',
                      'REDUCE', 'AVG', '1', '@rating', 'AS', 'avg_rating',
                      'REDUCE', 'SUM', '1', '@stock', 'AS', 'total_stock',
                      'REDUCE', 'MIN', '1', '@price', 'AS', 'min_price',
                      'REDUCE', 'MAX', '1', '@price', 'AS', 'max_price',
                      'FILTER', '@product_count >= 10',
                      'APPLY', 'format("%.2f", @avg_price)', 'AS', 'formatted_avg_price',
                      'SORTBY', '4', '@avg_rating', 'DESC', '@product_count', 'DESC',
                      'LIMIT', '0', '20')

      print(result)

      env.assertGreater(len(result), 1)  # Should have results
