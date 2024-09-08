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
