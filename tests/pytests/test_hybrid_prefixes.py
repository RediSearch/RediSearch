from common import *

@skip(cluster=False, min_shards=2)
def test_hybrid_incompatibleIndex(env):
    """Tests that we get an error if we try to query an index with a different
    schema than the one used in the query"""

    # Connect to two shards
    first_conn = env.getConnection(0)
    second_conn = env.getConnection(1)

    # Create an index
    index_name = 'idx'
    env.expect(f'FT.CREATE {index_name} PREFIX 1 h: SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    def modify_index(conn, index_name, prefixes):
        # Promote the connection to an internal one, such that we can execute internal (shard-local) commands
        conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        # Connect to a shard, and create an index with a different schema, but
        # the same name
        res = conn.execute_command('_FT.DROPINDEX', index_name)
        env.assertEqual(res, 'OK')
        res = conn.execute_command('_FT.CREATE', index_name, 'PREFIX', len(prefixes), *prefixes, 'SCHEMA', 'description', 'TEXT', 'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2')
        env.assertEqual(res, 'OK')

    modify_index(first_conn, index_name, ['k:'])

    # Query via the cluster connection, such that we will get the mismatch error
    commands = [
        ['FT.HYBRID', index_name, 'SEARCH', 'text', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', '0']
    ]

    # Run commands on second shard (different index prefixes -> error)
    for command in commands:
        try:
            second_conn.execute_command(*command)
            env.assertTrue(False)
        except Exception as e:
            env.assertContains("Index mismatch: Shard index is different than queried index", str(e))

    # Also for an index with a different amount of prefixes
    modify_index(first_conn, index_name, ['h:', 'k:'])
    # Run commands on second shard (different index prefixes -> error)
    for command in commands:
        try:
            second_conn.execute_command(*command)
            env.assertTrue(False)
        except Exception as e:
            env.assertContains("Index mismatch: Shard index is different than queried index", str(e))
