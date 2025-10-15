import random
from common import *

def configure_shards_with_different_timeout_policies(env):
    for shard_id in range(1, env.shardsCount + 1):
        conn = env.getConnection(shard_id)

        if shard_id == 1:
            # First shard uses RETURN policy
            conn.execute_command(config_cmd(), 'set', 'ON_TIMEOUT', 'RETURN')
        else:
            # All other shards use FAIL policy
            conn.execute_command(config_cmd(), 'set', 'ON_TIMEOUT', 'FAIL')
            conn.execute_command(config_cmd(), 'set', 'TIMEOUT', '1')

@skip(cluster=False)
def test_cluster_aggregate_with_shards_timeout(env):
    """
    This test tries to reproduce an issue MOD-10774, which crashed when the coordinator shard had a different TimeOur Return Policy
    than the other shards. (The coordinator had On TimeOut return and the other On Timeout Fail). When the shard actually timed out,
    the coordinator would crash.
    """
    configure_shards_with_different_timeout_policies(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'title', 'TEXT', 'SORTABLE',
               'price', 'NUMERIC', 'SORTABLE',
               'category', 'TAG', 'SORTABLE',
               'brand', 'TEXT', 'NOSTEM',
               'rating', 'NUMERIC',
               'stock', 'NUMERIC',
               'tags', 'TAG',
               'created_date', 'NUMERIC').ok()

    waitForIndex(env, 'idx')

    conn = getConnectionByEnv(env)
    num_docs = 20000

    brands = ['Apple', 'Samsung', 'Sony', 'LG', 'Dell', 'HP', 'Nike', 'Adidas', 'Zara', 'H&M']

    # Use pipeline for much faster bulk HSET operations
    pipeline = conn.pipeline(transaction=False)
    batch_size = 10000  # Execute pipeline every 1000 docs to avoid memory issues

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

        pipeline.execute_command('HSET', doc_key,
                                'title', title,
                                'price', price,
                                'category', category,
                                'brand', brand,
                                'rating', rating,
                                'stock', stock,
                                'tags', tags,
                                'created_date', created_date)

        # Execute pipeline every batch_size docs to avoid memory issues
        if (i + 1) % batch_size == 0:
            pipeline.execute()

    # Execute remaining docs
    pipeline.execute()
    conn_shard_1 = env.getConnection(1)
    result = conn_shard_1.execute_command('FT.AGGREGATE', 'idx', '*',
                                          'GROUPBY', '1', '@category',
                                          'REDUCE', 'COUNT', '0', 'AS', 'count',
                                          'REDUCE', 'AVG', '1', '@price', 'AS', 'avg_price')

    env.assertGreater(len(result), 1, message=result)  # Should have results, at least the coordinator does not timeout
