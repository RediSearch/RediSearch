from common import *
import faker

def test_existing_argument(env):
    """Tests that:
        * we accept only the wanted arguments for the 'existing' keyword.
        * We default to 'OFF' if the argument is not given.
    """

    # Create an index with a bad option for the 'existing' keyword
    env.expect('FT.CREATE', 'idx', 'INDEXALL', 'GULU_GULU', 'SCHEMA' , 't', 'TEXT').error().contains('Invalid argument for `INDEXALL`, use ENABLE/DISABLE')

    # Now let's try a valid one (ON case)
    env.expect('FT.CREATE', 'idx', 'INDEXALL', 'ENABLE', 'SCHEMA' , 't', 'TEXT').ok()

    # Now let's try a valid one (OFF case)
    env.expect('FT.CREATE', 'explicit', 'INDEXALL', 'DISABLE', 'SCHEMA' , 't', 'TEXT').ok()
    env.expect('FT.CREATE', 'implicit', 'SCHEMA', 'title', 'TEXT').ok()

    explicit_res = env.cmd('FT.INFO', 'explicit')
    implicit_res = env.cmd('FT.INFO', 'implicit')
    env.assertEqual(explicit_res[explicit_res.index('index_definition')], implicit_res[implicit_res.index('index_definition')])

@skip(cluster=True)
def test_existing_GC():
    """Tests the GC functionality on the existing docs inverted index."""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'INDEXALL', 'ENABLE', 'SCHEMA', 't', 'TEXT').ok()
    n_docs = 1005       # 5 more than the amount of entries in an index block
    fake = faker.Faker()
    for i in range(2 * n_docs):
        conn.execute_command('HSET', f'doc{2 *i}', 't', fake.name())
        conn.execute_command('HSET', f'doc{2 * i + 1}', 't2', fake.name())

    # Set the GC clean threshold to 0, and stop its periodic execution
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()

    # Delete docs with 'missing values'
    for i in range(2 * n_docs):
        conn.execute_command('DEL', f'doc{2 * i + 1}')

    # Run GC, and wait for it to finish
    env.expect(debug_cmd(), 'GC_FORCEINVOKE', 'idx').equal('DONE')

    # Make sure we have updated the index, by searching for the docs, and
    # verifying that `bytes_collected` > 0
    res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0')
    env.assertEqual(res, [2010])

    res = env.cmd('FT.INFO', 'idx')
    gc_sec = res[res.index('gc_stats') + 1]
    bytes_collected = gc_sec[gc_sec.index('bytes_collected') + 1]
    env.assertTrue(int(bytes_collected) > 0)

    # Reschedule the gc - add a job to the queue
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')

@skip(cluster=True)
def testOptimized():
    """
    Basic test for the optimized versions of the iterators that exploit the
    existing-index.
    """

    env = Env(moduleArgs="DEFAULT_DIALECT 2 FORK_GC_CLEAN_THRESHOLD 0")

    env.expect('FT.CREATE', 'idx', 'INDEXALL', 'ENABLE', 'SCHEMA', 't', 'TEXT').ok()
    # Stop GC from running periodically
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()

    # Add some docs
    n_docs = 10
    for i in range(1, n_docs + 1):
        env.cmd('HSET', f'doc{i}', 't', f'hello')

    # Remove some docs
    for i in range(1, n_docs + 1):       # Doc{i} matches doc with id = i
        if i % 2 == 0:
            env.cmd('DEL', f'doc{i}')

    # Sanity check
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), [i for i in range(1, 11)])

    # Apply the GC, to clean the deleted docs from the inverted indexes
    env.expect(debug_cmd(), 'GC_FORCEINVOKE', 'idx').equal('DONE')

    # Make sure the inverted index is updated
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), [i for i in range(1, 11, 2)])

    # Test the optimized wildcard iterator
    res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0')
    env.assertEqual(res, [n_docs / 2])

    # Add a doc with a different value for the 't' field
    env.cmd('HSET', 'doc11', 't', 'world')

    # Test the optimized version of the optional iterator
    exp_score = env.cmd('FT.SEARCH', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT', 'LIMIT', '0', '1')[2]
    expected = [n_docs / 2 + 1]
    for i in range(1, n_docs + 2, 2):
        # Only doc11 should have score 0
        expected.extend([f'doc{i}', exp_score if i != 11 else '0'])

    env.expect('FT.SEARCH', 'idx', '~@t:hello*', 'WITHSCORES', 'NOCONTENT').equal(expected)

    # Test the optimized version of the NOT iterator
    env.expect('FT.SEARCH', 'idx', '-@t:world', 'NOCONTENT').equal(
        [5, 'doc1', 'doc3', 'doc5', 'doc7', 'doc9'])

    # Reschedule the gc - add a job to the queue
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')
