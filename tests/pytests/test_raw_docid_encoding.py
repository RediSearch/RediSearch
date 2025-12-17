"""
Tests for RAW_DOCID_ENCODING configuration.

These tests verify that the raw DocID encoding (4-byte fixed) works correctly
with TAG indexes and GC.
"""

from RLTest import Env
from common import skip, config_cmd, forceInvokeGC


@skip(cluster=True)
def testTagIndexWithRawDocIdEncoding():
    """Test TAG index operations with RAW_DOCID_ENCODING enabled"""
    env = Env(moduleArgs='RAW_DOCID_ENCODING true')
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'TAG').ok()

    # Add documents
    for i in range(100):
        env.expect('hset', f'doc{i}', 't', 'value').equal(1)

    # Verify search works
    res = env.cmd('ft.search', 'idx', '@t:{value}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 100, message=res)

    # Delete half the documents
    for i in range(0, 100, 2):
        env.expect('del', f'doc{i}').equal(1)

    # Verify remaining documents
    res = env.cmd('ft.search', 'idx', '@t:{value}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 50, message=res)


@skip(cluster=True)
def testTagIndexGCWithRawDocIdEncoding():
    """Test TAG index with GC operations using RAW_DOCID_ENCODING.

    This test verifies that the RawDocIdsOnly encoding works correctly
    with garbage collection, which is the main purpose of this test file.
    TAG fields use DocIdsOnly encoding, which becomes RawDocIdsOnly when
    RAW_DOCID_ENCODING=true.
    """
    env = Env(moduleArgs='RAW_DOCID_ENCODING true')
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'TAG').ok()

    # Add enough documents to span multiple blocks (RECOMMENDED_BLOCK_ENTRIES is 1000)
    num_docs = 3200
    for i in range(num_docs):
        env.expect('hset', f'doc{i}', 't', 'testvalue').equal(1)

    # Verify all documents are indexed
    res = env.cmd('ft.search', 'idx', '@t:{testvalue}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], num_docs, message=res)

    # Delete first 2000 documents
    for i in range(2000):
        env.expect('del', f'doc{i}').equal(1)

    # Force GC to clean up deleted entries
    forceInvokeGC(env, 'idx')

    # Verify remaining documents after GC
    res = env.cmd('ft.search', 'idx', '@t:{testvalue}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 1200, message=res)

    # Delete more documents and run GC again
    for i in range(2000, 3000):
        env.expect('del', f'doc{i}').equal(1)

    forceInvokeGC(env, 'idx')

    # Verify remaining documents
    res = env.cmd('ft.search', 'idx', '@t:{testvalue}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 200, message=res)


@skip(cluster=True)
def testMultipleTagValuesWithRawDocIdEncoding():
    """Test multiple TAG values with RAW_DOCID_ENCODING"""
    env = Env(moduleArgs='RAW_DOCID_ENCODING true')
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'TAG').ok()

    # Add documents with different tag values
    for i in range(100):
        tag_value = f'tag{i % 10}'  # 10 different tag values
        env.expect('hset', f'doc{i}', 't', tag_value).equal(1)

    # Verify each tag value
    for tag_num in range(10):
        res = env.cmd('ft.search', 'idx', f'@t:{{tag{tag_num}}}', 'NOCONTENT', 'LIMIT', 0, 0)
        env.assertEqual(res[0], 10, message=res)

    # Delete documents with even tag numbers
    for i in range(100):
        if (i % 10) % 2 == 0:  # tag0, tag2, tag4, tag6, tag8
            env.expect('del', f'doc{i}').equal(1)

    forceInvokeGC(env, 'idx')

    # Verify odd tag values still have 10 docs each
    for tag_num in [1, 3, 5, 7, 9]:
        res = env.cmd('ft.search', 'idx', f'@t:{{tag{tag_num}}}', 'NOCONTENT', 'LIMIT', 0, 0)
        env.assertEqual(res[0], 10, message=res)

    # Verify even tag values have 0 docs
    for tag_num in [0, 2, 4, 6, 8]:
        res = env.cmd('ft.search', 'idx', f'@t:{{tag{tag_num}}}', 'NOCONTENT', 'LIMIT', 0, 0)
        env.assertEqual(res[0], 0, message=res)


@skip(cluster=True)
def testTagIntersectionWithRawDocIdEncoding():
    """Test TAG intersection queries with RAW_DOCID_ENCODING.

    This test verifies that the skip_to (seek) functionality works correctly
    with raw DocID encoding. Intersection queries require seeking to find
    matching documents across multiple posting lists.
    """
    env = Env(moduleArgs='RAW_DOCID_ENCODING true')
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 't1', 'TAG', 't2', 'TAG').ok()

    # Add documents with two tag fields
    # t1 gets values: A for even docs, B for odd docs
    # t2 gets values: X for docs divisible by 3, Y for docs divisible by 5, Z otherwise
    for i in range(1000):
        t1_val = 'A' if i % 2 == 0 else 'B'
        if i % 3 == 0:
            t2_val = 'X'
        elif i % 5 == 0:
            t2_val = 'Y'
        else:
            t2_val = 'Z'
        env.expect('hset', f'doc{i}', 't1', t1_val, 't2', t2_val).equal(2)

    # Test intersection: A AND X (even docs divisible by 3)
    # These are: 0, 6, 12, 18, ... (multiples of 6)
    res = env.cmd('ft.search', 'idx', '@t1:{A} @t2:{X}', 'NOCONTENT', 'LIMIT', 0, 0)
    expected = len([i for i in range(1000) if i % 2 == 0 and i % 3 == 0])
    env.assertEqual(res[0], expected, message=res)

    # Test intersection: B AND Y (odd docs divisible by 5)
    # These are: 5, 25, 35, 55, ... (odd multiples of 5, not divisible by 3)
    res = env.cmd('ft.search', 'idx', '@t1:{B} @t2:{Y}', 'NOCONTENT', 'LIMIT', 0, 0)
    expected = len([i for i in range(1000) if i % 2 == 1 and i % 5 == 0 and i % 3 != 0])
    env.assertEqual(res[0], expected, message=res)

    # Delete some documents and run GC
    for i in range(0, 500, 2):
        env.expect('del', f'doc{i}').equal(1)

    forceInvokeGC(env, 'idx')

    # Test intersection after GC: A AND X should only include docs >= 500
    res = env.cmd('ft.search', 'idx', '@t1:{A} @t2:{X}', 'NOCONTENT', 'LIMIT', 0, 0)
    expected = len([i for i in range(500, 1000) if i % 2 == 0 and i % 3 == 0])
    env.assertEqual(res[0], expected, message=res)

    # Test intersection: B AND Z (odd docs not divisible by 3 or 5)
    res = env.cmd('ft.search', 'idx', '@t1:{B} @t2:{Z}', 'NOCONTENT', 'LIMIT', 0, 0)
    expected = len([i for i in range(1000) if i % 2 == 1 and i % 3 != 0 and i % 5 != 0])
    env.assertEqual(res[0], expected, message=res)
