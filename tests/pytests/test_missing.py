from common import *

def testFilterOnMissingValues():
    """Tests the missing values indexing feature with the `exists` operator"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    # Create an index with a TAG field that indexes missing values
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'foo', 'TAG', 'goo', 'NUMERIC').ok()

    # Add some documents, with\without the indexed fields.
    conn.execute_command('HSET', 'doc1', 'foo', 'val')
    conn.execute_command('HSET', 'doc2', 'foo', 'val', 'goo', '3')

    # Search for the documents with the indexed fields (sanity)
    env.expect('FT.SEARCH', 'idx', '@foo:{val}', 'FILTER', 'goo', '0', '10').equal([1, 'doc2', ['foo', 'val', 'goo', '3']])
