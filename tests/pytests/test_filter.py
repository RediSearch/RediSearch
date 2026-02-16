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
