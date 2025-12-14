"""
Tests for RAW_DOCID_ENCODING configuration.

These tests should be run with RAW_DOCID_ENCODING=true module argument to verify
that the raw DocID encoding (4-byte fixed) works correctly with TAG indexes and GC.
"""

from common import *


@skip(cluster=True)
def testRawDocIdEncodingConfig(env):
    """Verify RAW_DOCID_ENCODING is enabled"""
    raw_encoding = env.cmd(config_cmd(), 'GET', 'RAW_DOCID_ENCODING')
    # Config returns [['RAW_DOCID_ENCODING', 'true']] format
    env.assertEqual(raw_encoding[0][1], 'true')


@skip(cluster=True)
def testTagIndexWithRawDocIdEncoding(env):
    """Test TAG index operations with RAW_DOCID_ENCODING enabled"""
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'TAG').ok()
    waitForIndex(env, 'idx')

    # Add documents
    for i in range(100):
        env.expect('hset', f'doc{i}', 't', 'value').equal(1)

    # Verify search works
    res = env.cmd('ft.search', 'idx', '@t:{value}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 100)

    # Delete half the documents
    for i in range(0, 100, 2):
        env.expect('del', f'doc{i}').equal(1)

    # Verify remaining documents
    res = env.cmd('ft.search', 'idx', '@t:{value}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 50)


@skip(cluster=True)
def testTagIndexGCWithRawDocIdEncoding(env):
    """Test TAG index with GC operations using RAW_DOCID_ENCODING.
    
    This test verifies that the RawDocIdsOnly encoding works correctly
    with garbage collection, which is the main purpose of this test file.
    TAG fields use DocIdsOnly encoding, which becomes RawDocIdsOnly when
    RAW_DOCID_ENCODING=true.
    """
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'TAG').ok()
    waitForIndex(env, 'idx')

    # Add enough documents to span multiple blocks (RECOMMENDED_BLOCK_ENTRIES is 1000)
    num_docs = 3200
    for i in range(num_docs):
        env.expect('hset', f'doc{i}', 't', 'testvalue').equal(1)

    # Verify all documents are indexed
    res = env.cmd('ft.search', 'idx', '@t:{testvalue}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], num_docs)

    # Delete first 2000 documents
    for i in range(2000):
        env.expect('del', f'doc{i}').equal(1)

    # Force GC to clean up deleted entries
    forceInvokeGC(env, 'idx')

    # Verify remaining documents after GC
    res = env.cmd('ft.search', 'idx', '@t:{testvalue}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 1200)

    # Delete more documents and run GC again
    for i in range(2000, 3000):
        env.expect('del', f'doc{i}').equal(1)

    forceInvokeGC(env, 'idx')

    # Verify remaining documents
    res = env.cmd('ft.search', 'idx', '@t:{testvalue}', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[0], 200)


@skip(cluster=True)
def testMultipleTagValuesWithRawDocIdEncoding(env):
    """Test multiple TAG values with RAW_DOCID_ENCODING"""
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'TAG').ok()
    waitForIndex(env, 'idx')

    # Add documents with different tag values
    for i in range(100):
        tag_value = f'tag{i % 10}'  # 10 different tag values
        env.expect('hset', f'doc{i}', 't', tag_value).equal(1)

    # Verify each tag value
    for tag_num in range(10):
        res = env.cmd('ft.search', 'idx', f'@t:{{tag{tag_num}}}', 'NOCONTENT', 'LIMIT', 0, 0)
        env.assertEqual(res[0], 10)

    # Delete documents with even tag numbers
    for i in range(100):
        if (i % 10) % 2 == 0:  # tag0, tag2, tag4, tag6, tag8
            env.expect('del', f'doc{i}').equal(1)

    forceInvokeGC(env, 'idx')

    # Verify odd tag values still have 10 docs each
    for tag_num in [1, 3, 5, 7, 9]:
        res = env.cmd('ft.search', 'idx', f'@t:{{tag{tag_num}}}', 'NOCONTENT', 'LIMIT', 0, 0)
        env.assertEqual(res[0], 10)

    # Verify even tag values have 0 docs
    for tag_num in [0, 2, 4, 6, 8]:
        res = env.cmd('ft.search', 'idx', f'@t:{{tag{tag_num}}}', 'NOCONTENT', 'LIMIT', 0, 0)
        env.assertEqual(res[0], 0)

