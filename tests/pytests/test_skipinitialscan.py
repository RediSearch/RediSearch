from common import *

def test_skipinitialscan_text_search():
    """Test FT.CREATE with SKIPINITIALSCAN and FT.SEARCH on TEXT field"""
    env = Env()
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN',
            'SCHEMA', 'title', 'TEXT').ok()

    # Insert 3 documents BEFORE creating the index
    conn.execute_command('HSET', 'doc0', 'title', 'value-index1-0')
    conn.execute_command('HSET', 'doc1', 'title', 'value-index1-1')
    conn.execute_command('HSET', 'doc2', 'title', 'value-index1-2')

    # Run FT.EXPLAIN to see the query plan/iterators
    res = env.cmd('FT.EXPLAIN', 'idx', 'value-index1-0')
    print(f"FT.EXPLAIN result: {res}")

    # Run FT.SEARCH to see the query plan/iterators
    res = env.cmd('FT.SEARCH', 'idx', 'value-index1-0')
    print(f"FT.SEARCH result: {res}")
