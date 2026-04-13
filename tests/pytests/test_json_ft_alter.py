from common import *

def test_alter_realloc_crash(env):
    if env.isCluster():
        env.skip()

    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA', '$.tag', 'AS', 'tag', 'TAG')

    # Add a document
    env.cmd('JSON.SET', 'doc:1', '$', '{"tag": "foo"}')
    env.cmd('JSON.SET', 'doc:2', '$', '{"tag": "foo"}')

    # Create a cursor that evaluates the TAG node and suspends.
    res = env.cmd('FT.AGGREGATE', 'idx', '@tag:{foo}', 'LOAD', 1, '@tag', 'WITHCURSOR', 'COUNT', 1)

    cursor_id = res[1]

    # Trigger reallocation of sp->fields
    for i in range(100):
        schema_args = []
        for j in range(100):
            schema_args.extend([f'$.new{i}_{j}', 'AS', f'new{i}_{j}', 'TAG'])
        env.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *schema_args)

    # Resume the cursor, triggering the use-after-free
    res = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)

    # We expect to get the second document's result
    # If the array was reallocated and read from garbage, this might fail or return garbage
    print("Cursor result:", res)
    env.assertEqual(len(res[0]), 1)  # one row
    env.assertTrue(True)



def test_alter_json_filter_no_results(env):
    if env.isCluster():
        env.skip()

    env.cmd('FT.CREATE', 'idx2', 'ON', 'JSON', 'PREFIX', 1, 'doc:',
            'FILTER', '@age1 > 10 || @age2 > 10',
            'SCHEMA', '$.name', 'AS', 'name', 'TEXT', '$.age1', 'AS', 'age1', 'NUMERIC')

    # Add a document that passes the filter
    env.cmd('JSON.SET', 'doc:1', '$', '{"name": "foo", "age1": 20, "age2": 0}')

    res = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(res[0], 1)

    # Alter the index by adding the field that was used in the filter
    env.cmd('FT.ALTER', 'idx2', 'SCHEMA', 'ADD', '$.age2', 'AS', 'age2', 'NUMERIC')

    # Add a NEW document that passes the filter (because of age2)
    env.cmd('JSON.SET', 'doc:2', '$', '{"name": "bar", "age1": 0, "age2": 30}')
    waitForIndex(env, 'idx2')

    # This document WILL NOT BE INDEXED if the filter evaluation is corrupted!
    res = env.cmd('FT.SEARCH', 'idx2', '*')
    print(res)
    env.assertEqual(res[0], 2)
