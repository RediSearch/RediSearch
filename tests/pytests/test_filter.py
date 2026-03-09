from includes import *
from common import *
from RLTest import Env

def testFilter1(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['name', 'foo']])

def testFilter2(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'stuff', 'ON', 'HASH',
            'FILTER', 'startswith(@__key, "stuff:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    conn.execute_command('hset', 'object:jojo', 'name', 'vivi')
    conn.execute_command('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['name', 'foo', 'age', '42']])

def testIdxField(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx1',
            'ON', 'HASH',
            'PREFIX', 1, 'doc',
            'FILTER', '@indexName=="idx1"',
            'SCHEMA', 'name', 'text', 'indexName', 'text')
    env.cmd('ft.create', 'idx2',
            'ON', 'HASH',
            'FILTER', '@indexName=="idx2"',
            'SCHEMA', 'name', 'text', 'indexName', 'text')

    conn.execute_command('hset', 'doc1', 'name', 'foo', 'indexName', 'idx1')
    conn.execute_command('hset', 'doc2', 'name', 'bar', 'indexName', 'idx2')

    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx1', '*')), toSortedFlatList([1, 'doc1', ['name', 'foo', 'indexName', 'idx1']]))
    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx2', '*')), toSortedFlatList([1, 'doc2', ['name', 'bar', 'indexName', 'idx2']]))

def testMultiFilters1(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'test', 'ON', 'HASH',
               'PREFIX', '2', 'student:', 'pupil:',
               'FILTER', 'startswith(@__key, "student:")',
               'SCHEMA', 'first', 'TEXT', 'last', 'TEXT', 'age', 'NUMERIC').ok()
    conn.execute_command('HSET', 'student:yes1', 'first', 'yes1', 'last', 'yes1', 'age', '17')
    conn.execute_command('HSET', 'student:yes2', 'first', 'yes2', 'last', 'yes2', 'age', '15')
    conn.execute_command('HSET', 'pupil:no1', 'first', 'no1', 'last', 'no1', 'age', '17')
    conn.execute_command('HSET', 'pupil:no2', 'first', 'no2', 'last', 'no2', 'age', '15')
    res1 = [2, 'student:yes2', ['first', 'yes2', 'last', 'yes2', 'age', '15'],
                'student:yes1', ['first', 'yes1', 'last', 'yes1', 'age', '17']]
    res = env.cmd('ft.search test *')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(res1))

def testMultiFilters2(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'test', 'ON', 'HASH',
               'PREFIX', '2', 'student:', 'pupil:',
               'FILTER', '@age > 16',
               'SCHEMA', 'first', 'TEXT', 'last', 'TEXT', 'age', 'NUMERIC').ok()
    conn.execute_command('HSET', 'student:yes1', 'first', 'yes1', 'last', 'yes1', 'age', '17')
    conn.execute_command('HSET', 'student:no1', 'first', 'no1', 'last', 'no1', 'age', '15')
    conn.execute_command('HSET', 'pupil:yes2', 'first', 'yes2', 'last', 'yes2', 'age', '17')
    conn.execute_command('HSET', 'pupil:no2', 'first', 'no2', 'last', 'no2', 'age', '15')
    res1 = [2, 'pupil:yes2', ['first', 'yes2', 'last', 'yes2', 'age', '17'],
                'student:yes1', ['first', 'yes1', 'last', 'yes1', 'age', '17']]
    res = env.cmd('ft.search test *')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(res1))

def testCountry(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx1',
            'PREFIX', 1, 'address:',
            'FILTER', '@country=="usa"',
            'SCHEMA', 'business', 'text', 'country', 'text')

    conn.execute_command('hset', 'address:1', 'business', 'foo', 'country', 'usa')
    conn.execute_command('hset', 'address:2', 'business', 'bar', 'country', 'israel')

    res = env.cmd('ft.search', 'idx1', '*')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'address:1', ['business', 'foo', 'country', 'usa']]))

def testIssue1571(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx',
            'FILTER', '@index=="yes"',
            'SCHEMA', 't', 'TEXT')

    conn.execute_command('hset', 'doc1', 't', 'foo1', 'index', 'yes')

    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx', 'foo*')), toSortedFlatList([1, 'doc1', ['t', 'foo1', 'index', 'yes']]))

    conn.execute_command('hset', 'doc1', 'index', 'no')

    env.expect('ft.search', 'idx', 'foo*').equal([0])

    conn.execute_command('hset', 'doc1', 't', 'foo2')

    env.expect('ft.search', 'idx', 'foo*').equal([0])

    conn.execute_command('hset', 'doc1', 'index', 'yes')

    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx', 'foo*')), toSortedFlatList([1, 'doc1', ['t', 'foo2', 'index', 'yes']]))

def testIssue1571WithRename(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx1',
            'PREFIX', '1', 'idx1',
            'FILTER', '@index=="yes"',
            'SCHEMA', 't', 'TEXT')
    env.cmd('ft.create', 'idx2',
            'PREFIX', '1', 'idx2',
            'FILTER', '@index=="yes"',
            'SCHEMA', 't', 'TEXT')

    conn.execute_command('hset', 'idx1:{doc}1', 't', 'foo1', 'index', 'yes')

    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx1', 'foo*')), toSortedFlatList([1, 'idx1:{doc}1', ['t', 'foo1', 'index', 'yes']]))
    env.expect('ft.search', 'idx2', 'foo*').equal([0])

    conn.execute_command('rename', 'idx1:{doc}1', 'idx2:{doc}1')

    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx2', 'foo*')), toSortedFlatList([1, 'idx2:{doc}1', ['t', 'foo1', 'index', 'yes']]))
    env.expect('ft.search', 'idx1', 'foo*').equal([0])

    conn.execute_command('hset', 'idx2:{doc}1', 'index', 'no')

    env.expect('ft.search', 'idx1', 'foo*').equal([0])
    env.expect('ft.search', 'idx2', 'foo*').equal([0])

    conn.execute_command('rename', 'idx2:{doc}1', 'idx1:{doc}1')

    env.expect('ft.search', 'idx1', 'foo*').equal([0])
    env.expect('ft.search', 'idx2', 'foo*').equal([0])

    conn.execute_command('hset', 'idx1:{doc}1', 'index', 'yes')

    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx1', 'foo*')), toSortedFlatList([1, 'idx1:{doc}1', ['t', 'foo1', 'index', 'yes']]))
    env.expect('ft.search', 'idx2', 'foo*').equal([0])

@skip(cluster=True)
def testRenameWithFilterUsingFieldValueBetweenIndexes(env):
    """
    Test RENAME between different indexes where both have FILTER expressions
    that read field values. This tests that filters are correctly evaluated
    using the data from the new key location.
    """
    conn = getConnectionByEnv(env)

    # Create two indexes with different prefixes but same filter expression
    env.cmd('ft.create', 'idx1',
            'PREFIX', '1', 'prefix1:',
            'FILTER', '@category=="books"',
            'SCHEMA', 'title', 'TEXT', 'category', 'TAG')

    env.cmd('ft.create', 'idx2',
            'PREFIX', '1', 'prefix2:',
            'FILTER', '@category=="books"',
            'SCHEMA', 'title', 'TEXT', 'category', 'TAG')

    # Add a document that matches idx1's prefix and filter
    conn.execute_command('hset', 'prefix1:item', 'title', 'mybook', 'category', 'books')

    # Verify it's in idx1 and not in idx2
    env.expect('ft.search', 'idx1', 'mybook').equal([1, 'prefix1:item', ['title', 'mybook', 'category', 'books']])
    env.expect('ft.search', 'idx2', 'mybook').equal([0])

    # Rename to idx2's prefix - the filter should still pass because
    # we read the field value from the new key location
    env.expect('RENAME prefix1:item prefix2:item').ok()

    # Verify it moved to idx2
    env.expect('ft.search', 'idx1', 'mybook').equal([0])
    env.expect('ft.search', 'idx2', 'mybook').equal([1, 'prefix2:item', ['title', 'mybook', 'category', 'books']])

@skip(cluster=True)
def testRenameWithFilterExcludingDocument(env):
    """
    Test RENAME where the target index's filter would exclude the document.
    The document should not be indexed in the target index.
    """
    conn = getConnectionByEnv(env)

    # Create an index with a filter that checks field value
    env.cmd('ft.create', 'idx1',
            'PREFIX', '1', 'prefix1:',
            'FILTER', '@type=="allowed"',
            'SCHEMA', 'data', 'TEXT', 'type', 'TAG')

    env.cmd('ft.create', 'idx2',
            'PREFIX', '1', 'prefix2:',
            'FILTER', '@type=="special"',
            'SCHEMA', 'data', 'TEXT', 'type', 'TAG')

    # Add a document that matches idx1's filter but NOT idx2's filter
    conn.execute_command('hset', 'prefix1:doc', 'data', 'hello', 'type', 'allowed')

    # Verify it's in idx1
    env.expect('ft.search', 'idx1', 'hello').equal([1, 'prefix1:doc', ['data', 'hello', 'type', 'allowed']])
    env.expect('ft.search', 'idx2', 'hello').equal([0])

    # Rename to idx2's prefix - but the filter should NOT pass
    # because type != "special"
    env.expect('RENAME prefix1:doc prefix2:doc').ok()

    # Document should be removed from idx1 and NOT added to idx2
    env.expect('ft.search', 'idx1', 'hello').equal([0])
    env.expect('ft.search', 'idx2', 'hello').equal([0])

@skip(cluster=True)
def testRenameToSameName(env):
    """
    Test RENAME to the same name (e.g., RENAME prefix1:doc prefix1:doc).
    This should be a no-op and the document should remain in the index.
    """
    conn = getConnectionByEnv(env)

    # Create an index with a filter
    env.cmd('ft.create', 'idx1',
            'PREFIX', '1', 'prefix1:',
            'FILTER', '@type=="allowed"',
            'SCHEMA', 'data', 'TEXT', 'type', 'TAG')

    # Add a document
    conn.execute_command('hset', 'prefix1:doc', 'data', 'hello', 'type', 'allowed')

    # Verify it's in idx1
    env.expect('ft.search', 'idx1', 'hello').equal([1, 'prefix1:doc', ['data', 'hello', 'type', 'allowed']])

    # Rename to same name - should be a no-op
    env.expect('RENAME prefix1:doc prefix1:doc').ok()

    # Document should still be in idx1
    env.expect('ft.search', 'idx1', 'hello').equal([1, 'prefix1:doc', ['data', 'hello', 'type', 'allowed']])

@skip(no_json=True)
def testIdxFieldJson(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx1',
            'ON', 'JSON',
            'PREFIX', 1, 'doc',
            'FILTER', '@indexName=="idx1"',
            'SCHEMA', '$.name', 'AS', 'name', 'text', '$.indexName', 'AS', 'indexName', 'text')
    env.cmd('ft.create', 'idx2',
            'ON', 'JSON',
            'FILTER', '@indexName=="idx2"',
            'SCHEMA', '$.name', 'AS', 'name', 'text', '$.indexName', 'AS', 'indexName', 'text')

    conn.execute_command('JSON.SET', 'doc:1', '$', r'{"name":"foo", "indexName":"idx1"}')
    conn.execute_command('JSON.SET', 'doc:2', '$', r'{"name":"bar", "indexName":"idx2"}')

    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx1', '*')), toSortedFlatList([1, '$', 'doc:1', '{"name":"foo","indexName":"idx1"}']))
    env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx2', '*')), toSortedFlatList([1, '$', 'doc:2', '{"name":"bar","indexName":"idx2"}']))

@skip(no_json=True)
def testFilterStartWith(env):
    conn = getConnectionByEnv(env)

    env.cmd('ft.create', 'things',
            'ON', 'JSON',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', '$.name', 'AS', 'name', 'text')

    conn.execute_command('JSON.SET', 'thing:bar', '$', r'{"name":"foo", "indexName":"idx1"}')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['$', '{"name":"foo","indexName":"idx1"}']])

@skip(no_json=True)
def testFilterWithOperator(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things',
            'ON', 'JSON',
            'FILTER', '@num > (0 + 0)',
            'SCHEMA', '$.name', 'AS', 'name', 'text', '$.num', 'AS', 'num', 'numeric')

    conn.execute_command('JSON.SET', 'thing:foo', '$', r'{"name":"foo", "num":5}')
    conn.execute_command('JSON.SET', 'thing:bar', '$', r'{"name":"foo", "num":-5}')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:foo', ['$', '{"name":"foo","num":5}']])

@skip(no_json=True)
def testFilterWithNot(env):
    conn = getConnectionByEnv(env)
    # check NOT on a non existing value return 1 result
    env.cmd('ft.create', 'things',
            'ON', 'JSON',
            'FILTER', '!(@name == "bar")',
            'SCHEMA', '$.name', 'AS', 'name', 'text')

    conn.execute_command('JSON.SET', 'thing:bar', '$', r'{"name":"foo", "indexName":"idx1"}')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1, 'thing:bar', ['$', '{"name":"foo","indexName":"idx1"}']])


    env.cmd('FT.DROPINDEX', 'things', 'DD')

    # check NOT on an existing value return 0 results
    env.cmd('ft.create', 'things',
            'ON', 'JSON',
            'FILTER', '!(@name == "foo")',
            'SCHEMA', '$.name', 'AS', 'name', 'text')

    conn.execute_command('JSON.SET', 'thing:bar', '$', r'{"name":"foo", "indexName":"idx1"}')

    env.expect('ft.search', 'things', 'foo').equal([0])

@skip(cluster=True)
def testFilterWithAliasedFieldsHash(env):
    """
    Test that FILTER expressions work correctly when multiple indexes use
    the same alias name but map to different actual hash fields.
    This tests that RLookup state is properly cleaned up between filter
    evaluations for different indexes.
    """
    conn = getConnectionByEnv(env)

    # Create two indexes with the same alias 'name' but different source fields
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH',
            'FILTER', '@name=="John"',
            'SCHEMA', 'name1', 'AS', 'name', 'TEXT')

    env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH',
            'FILTER', '@name=="John"',
            'SCHEMA', 'name2', 'AS', 'name', 'TEXT')

    # doc1: name1=Jeff, name2=John
    # Should be indexed in idx2 (name2=John matches) but NOT in idx1 (name1=Jeff doesn't match)
    conn.execute_command('HSET', 'doc1', 'name1', 'Jeff', 'name2', 'John')

    # doc2: name1=John, name2=Bill
    # Should be indexed in idx1 (name1=John matches) but NOT in idx2 (name2=Bill doesn't match)
    conn.execute_command('HSET', 'doc2', 'name1', 'John', 'name2', 'Bill')

    # doc3: name1=John, name2=John
    # Should be indexed in BOTH indexes
    conn.execute_command('HSET', 'doc3', 'name1', 'John', 'name2', 'John')

    # doc4: name1=Bill, name2=Jeff
    # Should NOT be indexed in either index
    conn.execute_command('HSET', 'doc4', 'name1', 'Bill', 'name2', 'Jeff')

    # Verify idx1 contains doc2 and doc3 (where name1=John)
    res = env.cmd('FT.SEARCH', 'idx1', '*', 'NOCONTENT')
    env.assertEqual(res, [2, 'doc2', 'doc3'])

    # Verify idx2 contains doc1 and doc3 (where name2=John)
    res = env.cmd('FT.SEARCH', 'idx2', '*', 'NOCONTENT')
    env.assertEqual(res, [2, 'doc1', 'doc3'])

@skip(cluster=True, no_json=True)
def testFilterWithAliasedFieldsJson(env):
    """
    Test that FILTER expressions work correctly when multiple JSON indexes use
    the same alias name but map to different JSON paths.
    This tests that RLookup state is properly cleaned up between filter
    evaluations for different indexes.
    """
    conn = getConnectionByEnv(env)

    # Create two JSON indexes with the same alias 'name' but different JSON paths
    env.cmd('FT.CREATE', 'idx1', 'ON', 'JSON',
            'FILTER', '@name=="John"',
            'SCHEMA', '$.name1', 'AS', 'name', 'TEXT')

    env.cmd('FT.CREATE', 'idx2', 'ON', 'JSON',
            'FILTER', '@name=="John"',
            'SCHEMA', '$.name2', 'AS', 'name', 'TEXT')

    # doc1: name1=Jeff, name2=John
    # Should be indexed in idx2 (name2=John matches) but NOT in idx1 (name1=Jeff doesn't match)
    conn.execute_command('JSON.SET', 'doc1', '$', '{"name1": "Jeff", "name2": "John"}')

    # doc2: name1=John, name2=Bill
    # Should be indexed in idx1 (name1=John matches) but NOT in idx2 (name2=Bill doesn't match)
    conn.execute_command('JSON.SET', 'doc2', '$', '{"name1": "John", "name2": "Bill"}')

    # doc3: name1=John, name2=John
    # Should be indexed in BOTH indexes
    conn.execute_command('JSON.SET', 'doc3', '$', '{"name1": "John", "name2": "John"}')

    # doc4: name1=Bill, name2=Jeff
    # Should NOT be indexed in either index
    conn.execute_command('JSON.SET', 'doc4', '$', '{"name1": "Bill", "name2": "Jeff"}')

    # Verify idx1 contains doc2 and doc3 (where $.name1=John)
    res = env.cmd('FT.SEARCH', 'idx1', '*', 'NOCONTENT')
    env.assertEqual(res, [2, 'doc2', 'doc3'])

    # Verify idx2 contains doc1 and doc3 (where $.name2=John)
    res = env.cmd('FT.SEARCH', 'idx2', '*', 'NOCONTENT')
    env.assertEqual(res, [2, 'doc1', 'doc3'])

@skip(cluster=True, no_json=True)
def testFilterWithAliasedFieldsMixedTypes(env):
    """
    Test that FILTER expressions with aliased fields work correctly when
    both HASH and JSON indexes coexist with the same alias names.
    The indexes should not be affected by each other.
    """
    conn = getConnectionByEnv(env)

    # Create JSON index with same alias but different path
    env.expect('FT.CREATE', 'json_idx', 'ON', 'JSON',
            'FILTER', '@status=="active"',
            'SCHEMA', '$.state', 'AS', 'status', 'TAG').ok()

    # Create HASH index with alias
    env.expect('FT.CREATE', 'hash_idx', 'ON', 'HASH',
            'FILTER', '@status=="active"',
            'SCHEMA', 'stat', 'AS', 'status', 'TAG').ok()

    # Create HASH document with stat=active
    env.expect('HSET', 'hash1', 'stat', 'active', 'data', 'hello').equal(2)

    # Verify HASH document is in hash_idx, i.e., there was no interference from
    # json_idx
    res = env.cmd('FT.SEARCH', 'hash_idx', '*', 'NOCONTENT')
    env.assertEqual(res, [1, 'hash1'])

def testFilterWithMissingFields(env):
    """
    Test that documents are not indexed when the filter expression evaluation
    fails due to missing fields. This is a regression test for a bug where
    documents added after index creation would be indexed even when the filter
    expression could not be evaluated (e.g., due to missing fields).
    """
    conn = getConnectionByEnv(env)

    # Create a document BEFORE the index exists
    conn.execute_command('HSET', 'h1', 'd2', '1')

    # Create an index with a filter that references fields d1 and d2
    # The filter requires both @d1==0 AND @d2==0
    env.cmd('FT.CREATE', 'idx', 'FILTER', '@d1==0 && @d2==0',
            'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC')

    waitForIndex(env, 'idx')

    # h1 should not be indexed because:
    # - d1 is missing (filter evaluation should fail or return false)
    # - d2=1 (doesn't match @d2==0 anyway)
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Create a document AFTER the index exists with only d2 field
    # This document should NOT be indexed because d1 is missing
    conn.execute_command('HSET', 'h2', 'd2', '1')

    # h2 should not be indexed - the filter expression references @d1 which is missing
    # Filter evaluation should fail, meaning the document should NOT be indexed
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Create a document that actually matches the filter
    conn.execute_command('HSET', 'h3', 'd1', '0', 'd2', '0')

    # h3 should be indexed because it matches the filter
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([1, 'h3'])

    # Update h2 to have d1=0, but d2 is still 1, so it shouldn't match
    conn.execute_command('HSET', 'h2', 'd1', '0')

    # h2 still shouldn't be indexed (d2=1 != 0)
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([1, 'h3'])

    # Update h2 to match the filter completely
    conn.execute_command('HSET', 'h2', 'd2', '0')

    # Now h2 should be indexed
    res = env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res[0], 2)
    env.assertContains('h2', res)
    env.assertContains('h3', res)

@skip(cluster=True)
def test_filter_and_missing_field(env):
    """Test that AND filter expressions work consistently when fields are missing"""
    conn = getConnectionByEnv(env)

    # --- Documents first (before index creation) ---
    # Add document with only d2=1 (d1 is missing) BEFORE creating indexes
    conn.execute_command('HSET', 'h1', 'd2', '1')

    # Create index with filter: @d1==0 && @d2==0
    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 && @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Create index with filter in reverse order: @d2==0 && @d1==0
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==0 && @d1==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Both indexes should have 0 documents (d2=1, d1 missing)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result1, [0])
    env.assertEqual(result2, [0])

    # --- Documents after index creation ---
    # Add document with d2=0 but d1 still missing - should NOT be indexed
    conn.execute_command('HSET', 'h2', 'd2', '0')

    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result1, [0], message="h2 should NOT be indexed - d1 is missing")
    env.assertEqual(result2, [0], message="h2 should NOT be indexed - d1 is missing")

    # Add document with both fields matching - should be indexed (success case)
    conn.execute_command('HSET', 'h3', 'd1', '0', 'd2', '0')

    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result1[0], 1, message="h3 should be indexed - both fields match")
    env.assertEqual(result1[1], 'h3')
    env.assertEqual(result2[0], 1, message="h3 should be indexed - both fields match")
    env.assertEqual(result2[1], 'h3')
    env.assertEqual(result1, result2)

@skip(cluster=True)
def test_filter_or_missing_field(env):
    """Test that OR filter expressions work consistently when fields are missing"""
    conn = getConnectionByEnv(env)

    # --- Documents first (before index creation) ---
    # Add document with only d2=1 (d1 is missing) BEFORE creating indexes
    conn.execute_command('HSET', 'h1', 'd2', '1')

    # Create index with filter: @d1==0 || @d2==0
    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 || @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Create index with filter in reverse order: @d2==0 || @d1==0
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==0 || @d1==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    waitForIndex(env, 'idx1')
    waitForIndex(env, 'idx2')

    # Both indexes should have 0 documents (d2=1 doesn't match, d1 missing)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result1[0], 0)
    env.assertEqual(result2[0], 0)

    # --- Documents after index creation ---
    # Add document with d2=0 (d1 missing) - should be indexed because d2==0 is true
    conn.execute_command('HSET', 'h2', 'd2', '0')

    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result1[0], 1, message="h2 should be indexed - d2==0 is true")
    env.assertEqual(result1[1], 'h2')
    env.assertEqual(result2[0], 1, message="h2 should be indexed - d2==0 is true")
    env.assertEqual(result2[1], 'h2')
    env.assertEqual(result1, result2)

@skip(cluster=True)
def test_filter_both_fields_missing(env):
    """Test AND and OR filters when both fields are missing"""
    conn = getConnectionByEnv(env)

    # --- Documents first (before index creation) ---
    # Add document with neither d1 nor d2 BEFORE creating index
    conn.execute_command('HSET', 'h1', 'other', 'value')

    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 && @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d1==1 || @d2==1',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Should have 0 documents because both fields are missing (treated as false)
    result = env.cmd('FT.SEARCH', 'idx1', '*')
    env.assertEqual(result, [0])
    result = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result, [0])

    # --- Documents after index creation ---
    # Add another document with neither d1 nor d2
    conn.execute_command('HSET', 'h2', 'other', 'data')

    # Still should have 0 documents
    result = env.cmd('FT.SEARCH', 'idx1', '*')
    env.assertEqual(result, [0])
    result = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result, [0])


# Tests for type mismatches in filter expressions

@skip(cluster=True)
def test_filter_type_mismatch_numeric_comparison(env):
    """Test that type mismatches in numeric comparisons are handled correctly"""
    conn = getConnectionByEnv(env)

    # Create index with numeric comparison filter
    env.expect('FT.CREATE', 'idx', 'FILTER', '@price > 100',
               'SCHEMA', 'price', 'TEXT', 'name', 'TEXT').ok()

    # Add document with a non-numeric string in price field
    conn.execute_command('HSET', 'h1', 'price', 'hello', 'name', 'test')

    # Document should NOT be indexed because 'hello' > 100 should fail/return false
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result, [0], message="'hello' > 100 should fail, doc not indexed")

    # Add document with valid numeric price
    conn.execute_command('HSET', 'h2', 'price', '150', 'name', 'test2')

    # This document SHOULD be indexed
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="150 > 100 should pass")
    env.assertEqual(result[1], 'h2', message="Only h2 should be indexed")

    # Add document with price below threshold
    conn.execute_command('HSET', 'h3', 'price', '50', 'name', 'test3')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="50 > 100 is false, only h2 indexed")
    env.assertEqual(result[1], 'h2')


@skip(cluster=True)
def test_filter_comparison_operators_with_missing_fields(env):
    """Test various comparison operators with missing fields"""
    conn = getConnectionByEnv(env)

    # Test less than
    env.expect('FT.CREATE', 'idx_lt', 'FILTER', '@val < 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test greater than
    env.expect('FT.CREATE', 'idx_gt', 'FILTER', '@val > 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test less than or equal
    env.expect('FT.CREATE', 'idx_le', 'FILTER', '@val <= 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test greater than or equal
    env.expect('FT.CREATE', 'idx_ge', 'FILTER', '@val >= 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test not equal
    env.expect('FT.CREATE', 'idx_ne', 'FILTER', '@val != 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test equal
    env.expect('FT.CREATE', 'idx_eq', 'FILTER', '@val == 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Add document without 'val' field
    conn.execute_command('HSET', 'h1', 'other', 'data')

    # All indexes should have 0 documents (missing field treated as comparison failure)
    for idx in ['idx_lt', 'idx_gt', 'idx_le', 'idx_ge', 'idx_ne', 'idx_eq']:
        result = env.cmd('FT.SEARCH', idx, '*')
        env.assertEqual(result, [0], message=f"Index {idx} should have 0 docs when field is missing")

    # Now add documents with actual values to verify the filters work correctly
    conn.execute_command('HSET', 'h_lt', 'val', '5')
    conn.execute_command('HSET', 'h_gt', 'val', '15')
    conn.execute_command('HSET', 'h_eq', 'val', '10')

    # Verify each index now has the correct document
    result = env.cmd('FT.SEARCH', 'idx_lt', '*')
    env.assertEqual(result[0], 1, message="idx_lt should have 1 doc (5 < 10)")
    env.assertEqual(result[1], 'h_lt')

    result = env.cmd('FT.SEARCH', 'idx_gt', '*')
    env.assertEqual(result[0], 1, message="idx_gt should have 1 doc (15 > 10)")
    env.assertEqual(result[1], 'h_gt')

    result = env.cmd('FT.SEARCH', 'idx_le', '*')
    env.assertEqual(result[0], 2, message="idx_le should have 2 docs (5 <= 10, 10 <= 10)")
    env.assertContains('h_lt', result)
    env.assertContains('h_eq', result)

    result = env.cmd('FT.SEARCH', 'idx_ge', '*')
    env.assertEqual(result[0], 2, message="idx_ge should have 2 docs (15 >= 10, 10 >= 10)")
    env.assertContains('h_gt', result)
    env.assertContains('h_eq', result)

    result = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result[0], 2, message="idx_ne should have 2 docs (5 != 10, 15 != 10)")
    env.assertContains('h_lt', result)
    env.assertContains('h_gt', result)

    result = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result[0], 1, message="idx_eq should have 1 doc (10 == 10)")
    env.assertEqual(result[1], 'h_eq')


@skip(cluster=True)
def test_filter_nested_expressions_with_missing_fields(env):
    """Test nested AND/OR expressions with some fields missing"""
    conn = getConnectionByEnv(env)

    # Complex filter: (@a==1 && @b==2) || (@c==3 && @d==4)
    env.expect('FT.CREATE', 'idx', 'FILTER', '(@a==1 && @b==2) || (@c==3 && @d==4)',
               'SCHEMA', 'a', 'NUMERIC', 'b', 'NUMERIC', 'c', 'NUMERIC', 'd', 'NUMERIC').ok()

    # Document with only a=1, b=2 (c and d missing) - should match first clause
    conn.execute_command('HSET', 'h1', 'a', '1', 'b', '2')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="h1 should match (@a==1 && @b==2)")
    env.assertEqual(result[1], 'h1')

    # Document with only c=3, d=4 (a and b missing) - should match second clause
    conn.execute_command('HSET', 'h2', 'c', '3', 'd', '4')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 2, message="h2 should match (@c==3 && @d==4)")
    env.assertContains('h1', result)
    env.assertContains('h2', result)

    # Document with only a=1 (b, c, d missing) - should NOT match
    conn.execute_command('HSET', 'h3', 'a', '1')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 2, message="h3 should NOT match - b is missing so first clause fails")
    env.assertContains('h1', result)
    env.assertContains('h2', result)

    # Document with all fields but wrong values - should NOT match
    conn.execute_command('HSET', 'h4', 'a', '9', 'b', '9', 'c', '9', 'd', '9')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 2, message="h4 should NOT match - wrong values")
    env.assertContains('h1', result)
    env.assertContains('h2', result)

    # Document matching both clauses - should match
    conn.execute_command('HSET', 'h5', 'a', '1', 'b', '2', 'c', '3', 'd', '4')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 3, message="h5 should match - both clauses true")
    env.assertContains('h1', result)
    env.assertContains('h2', result)
    env.assertContains('h5', result)


@skip(cluster=True)
def test_filter_comparing_fields_with_missing_fields(env):
    """Test that comparing two missing fields returns false (not true like NULL == NULL)"""
    conn = getConnectionByEnv(env)

    # Filter that compares two fields directly
    env.expect('FT.CREATE', 'idx_eq', 'FILTER', '@field1 == @field2',
               'SCHEMA', 'field1', 'NUMERIC', 'field2', 'NUMERIC').ok()

    env.expect('FT.CREATE', 'idx_ne', 'FILTER', '@field1 != @field2',
               'SCHEMA', 'field1', 'NUMERIC', 'field2', 'NUMERIC').ok()

    # Add document where both fields are missing
    conn.execute_command('HSET', 'h1', 'other', 'data')

    # Both comparisons should return false when both fields are missing
    # (missing property NULL is different from literal NULL)
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq, [0], message="@field1 == @field2 should be false when both are missing")

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne, [0], message="@field1 != @field2 should be false when both are missing")

    # Add document where both fields exist and are equal
    conn.execute_command('HSET', 'h2', 'field1', '5', 'field2', '5')
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq[0], 1, message="@field1 == @field2 should be true when both are 5")
    env.assertEqual(result_eq[1], 'h2')

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne, [0], message="@field1 != @field2 should still be false (5 != 5 is false)")

    # Add document where both fields exist and are not equal
    conn.execute_command('HSET', 'h3', 'field1', '5', 'field2', '10')
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq[0], 1, message="@field1 == @field2 still only h2 (5 == 10 is false)")
    env.assertEqual(result_eq[1], 'h2')

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne[0], 1, message="@field1 != @field2 should be true for h3 (5 != 10)")
    env.assertEqual(result_ne[1], 'h3')

    # Add document where only one field exists
    conn.execute_command('HSET', 'h4', 'field1', '5')
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq[0], 1, message="Missing field2 - comparison should fail")
    env.assertEqual(result_eq[1], 'h2')

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne[0], 1, message="Missing field2 - comparison should fail")
    env.assertEqual(result_ne[1], 'h3')
