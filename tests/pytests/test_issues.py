# -*- coding: utf-8 -*-

from common import *
from RLTest import Env

def test_1282(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'txt1', 'TEXT').ok()
  env.assertEqual(conn.execute_command('hset', 'doc1', 'txt1', 'foo'), 1)

  # optional search for new word would crash server
  env.expect('FT.SEARCH idx', '~foo').equal([1, 'doc1', ['txt1', 'foo']])
  env.expect('FT.SEARCH idx', '~bar ~foo').equal([1, 'doc1', ['txt1', 'foo']])

def test_1304(env):
  env.expect('FT.CREATE idx SCHEMA txt1 TEXT').equal('OK')
  env.expect('FT.EXPLAIN idx -20*').equal('PREFIX{-20*}\n')
  env.expect('FT.EXPLAIN idx -\\20*').equal('NOT{\n  PREFIX{20*}\n}\n')

def test_1414(env):
  env.skipOnCluster()
  env.expect('FT.CREATE idx SCHEMA txt1 TEXT').equal('OK')
  env.cmd('hset', 'doc', 'foo', 'hello', 'bar', 'world')
  env.expect('ft.search', 'idx', '*', 'limit', '0', '1234567').error().contains('LIMIT exceeds maximum of 1000000')
  env.expect('FT.CONFIG', 'set', 'MAXSEARCHRESULTS', '-1').ok()
  env.assertEqual(toSortedFlatList(env.cmd('ft.search', 'idx', '*', 'limit', '0', '1234567')),
                  toSortedFlatList([1, 'doc', ['foo', 'hello', 'bar', 'world']]))
  env.expect('FT.CONFIG', 'set', 'MAXSEARCHRESULTS', '1000000').ok()

def test_1502(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('HSET', 'a', 'bar', 'hello')

  env.expect('FT.CREATE idx1 SKIPINITIALSCAN SCHEMA foo TEXT').ok()
  env.expect('FT.CREATE idx2 SKIPINITIALSCAN SCHEMA foo TEXT').ok()

  env.expect('ft.search idx1 *').equal([0])
  env.expect('ft.search idx2 *').equal([0])

  env.expect('FT.ALTER idx1 SKIPINITIALSCAN SCHEMA ADD bar TEXT').ok()
  env.expect('FT.ALTER idx2 SCHEMA ADD bar TEXT').ok()
  waitForIndex(env, 'idx2')

  env.expect('ft.search idx1 *').equal([0])
  env.expect('ft.search idx2 *').equal([1, 'a', ['bar', 'hello']])

def test_1601(env):
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx:movie', 'SCHEMA', 'title', 'TEXT')
  conn.execute_command('HSET', 'movie:1', 'title', 'Star Wars: Episode I - The Phantom Menace')
  conn.execute_command('HSET', 'movie:2', 'title', 'Star Wars: Episodes II - Attack of the Clones')
  conn.execute_command('HSET', 'movie:3', 'title', 'Star Wars: Episode III - Revenge of the Sith')
  res = env.cmd('ft.search idx:movie @title:(episode) withscores nocontent')
  env.assertEqual(res[0], 3)

def testMultiSortby(env):
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'SORTABLE', 't2', 'TEXT', 'SORTABLE', 't3', 'TEXT', 'SORTABLE')
  conn.execute_command('hset', '1', 't1', 'foo', 't2', 'bar', 't3', 'baz')
  conn.execute_command('hset', '2', 't1', 'bar', 't2', 'foo', 't3', 'baz')
  sortby_t1 = [2, '2', '1']
  sortby_t2 = [2, '1', '2']
  env.expect('ft.search idx foo nocontent sortby t1 asc').equal(sortby_t1)
  env.expect('ft.search idx foo nocontent sortby t2 asc').equal(sortby_t2)
  env.expect('ft.search idx foo nocontent sortby t1 sortby t3').error()\
    .contains('Multiple SORTBY steps are not allowed')
  env.expect('ft.aggregate idx foo nocontent sortby 2 @t1 asc sortby 2 @t3 desc').error()\
    .contains('Multiple SORTBY steps are not allowed. Sort multiple fields in a single step')
  #TODO: allow multiple sortby steps
  #env.expect('ft.search idx foo nocontent sortby t1 sortby t3').equal(sortby_t1)
  #env.expect('ft.search idx foo nocontent sortby t2 sortby t3').equal(sortby_t2)

def test_1667(env):
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'text', 'TEXT')
  env.expect('ft.search idx @tag:{a}').equal([0])
  env.expect('ft.search idx @tag:{b}').equal([0])

  conn.execute_command('HSET', 'doc', 'tag', 'a,b')
  conn.execute_command('HSET', 'doc1', 'tag', 'abc')

  # test single stopword
  env.expect('ft.search idx @tag:{a}').equal([1, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{b}').equal([1, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c}').equal([0])

  # test stopword in list
  env.expect('ft.search idx @tag:{a|c}').equal([1, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c|a}').equal([1, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c|a|c}').equal([1, 'doc', ['tag', 'a,b']])

  # test stopword with prefix
  env.expect('ft.search idx @tag:{ab*}').equal([1, 'doc1', ['tag', 'abc']])
  env.expect('ft.search idx @tag:{abc*}').equal([1, 'doc1', ['tag', 'abc']])

  # ensure regular text field
  conn.execute_command('HSET', 'doc_a', 'text', 'a')
  conn.execute_command('HSET', 'doc_b', 'text', 'b')
  env.expect('ft.search idx a').equal([0])
  env.expect('ft.search idx b').equal([1, 'doc_b', ['text', 'b']])

def test_MOD_865(env):
  conn = getConnectionByEnv(env)
  args_list = ['FT.CREATE', 'idx', 'SCHEMA']
  for i in range(1025):
    args_list.extend([i, 'NUMERIC', 'SORTABLE'])
  env.expect(*args_list).error().contains('Schema is limited to 1024 fields')
  env.expect('FT.DROPINDEX', 'idx')

  args_list = ['FT.CREATE', 'idx', 'SCHEMA']
  for i in range(129):
    args_list.extend([i, 'TEXT'])
  env.expect(*args_list).error().contains('Schema is limited to {} TEXT fields'.format(arch_int_bits()))
  env.expect('FT.DROPINDEX', 'idx')

  args_list = ['FT.CREATE', 'idx', 'SCHEMA']
  for i in range(2):
    args_list.extend(['txt', 'TEXT'])
  env.expect(*args_list).error().contains('Duplicate field in schema - txt')
  env.expect('FT.DROPINDEX', 'idx')

def test_issue1826(env):
  # Stopword query is case sensitive.
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
  conn.execute_command('HSET', 'doc', 't', 'boy with glasses')

  env.expect('FT.SEARCH', 'idx', 'boy with glasses').equal([1, 'doc', ['t', 'boy with glasses']])
  env.expect('FT.SEARCH', 'idx', 'boy With glasses').equal([1, 'doc', ['t', 'boy with glasses']])

def test_issue1834(env):
  # Stopword query is case sensitive.
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
  conn.execute_command('HSET', 'doc', 't', 'hell hello')

  env.expect('FT.SEARCH', 'idx', 'hell|hello', 'HIGHLIGHT').equal([1, 'doc', ['t', '<b>hell</b> <b>hello</b>']])

def test_issue1880(env):
  # order of iterator in intersect is optimized by function
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
  conn.execute_command('HSET', 'doc1', 't', 'hello world')
  conn.execute_command('HSET', 'doc2', 't', 'hello')

  excepted_res = ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'world', 'Counter', 1, 'Size', 1],
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 2]]
  res1 = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'hello world')
  res2 = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'world hello')
  # both queries return `world` iterator before `hello`
  env.assertEqual(res1[1][3][1], excepted_res)
  env.assertEqual(res2[1][3][1], excepted_res)

  # test with a term which does not exist
  excepted_res = ['Type', 'INTERSECT', 'Counter', 0, 'Child iterators',
                    None,
                    ['Type', 'TEXT', 'Term', 'world', 'Counter', 0, 'Size', 1],
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 0, 'Size', 2]]
  res3 = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'hello new world')

  env.assertEqual(res3[1][3][1], excepted_res)

def test_issue1932(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    env.expect('FT.AGGREGATE', 'idx', '*', 'LIMIT', '100000000000000000', '100000000000', 'SORTBY', '1', '@t').error() \
      .contains('OFFSET exceeds maximum of 1000000')

def test_issue1988(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    env.expect('FT.SEARCH', 'idx', 'foo').equal([1, 'doc1', ['t', 'foo']])
    env.expect('FT.SEARCH', 'idx', 'foo', 'WITHSCORES').equal([1, 'doc1', '1', ['t', 'foo']])
    env.expect('FT.SEARCH', 'idx', 'foo', 'SORTBY' , 't').equal([1, 'doc1', ['t', 'foo']])
    env.expect('FT.SEARCH', 'idx', 'foo', 'WITHSCORES', 'SORTBY' , 't').equal([1, 'doc1', '1', ['t', 'foo']])

@no_msan
def testIssue2104(env):
  # 'AS' attribute does not work in functions
  conn = getConnectionByEnv(env)

  # hash
  env.execute_command('FT.CREATE', 'hash_idx', 'SCHEMA', 'name', 'TEXT', 'SORTABLE', 'subj1', 'NUMERIC', 'SORTABLE')
  conn.execute_command('hset', 'data1','name', 'abc', 'subj1', '20')
  # load a single field
  env.expect('FT.AGGREGATE', 'hash_idx', '*', 'LOAD', '1', '@subj1') \
      .equal([1, ['subj1', '20']])
  # load a field with an attribute
  env.expect('FT.AGGREGATE', 'hash_idx', '*', 'LOAD', '3', '@subj1', 'AS', 'a') \
      .equal([1, ['a', '20']])
  # load field and use `APPLY`
  env.expect('FT.AGGREGATE', 'hash_idx', '*', 'LOAD', '3', '@subj1', 'AS', 'a', 'APPLY', '(@a+@a)/2', 'AS', 'avg') \
      .equal([1, ['a', '20', 'avg', '20']])
  # load a field implicitly with `APPLY`
  res = env.cmd('FT.AGGREGATE', 'hash_idx', '*', 'APPLY', '(@subj1+@subj1)/2', 'AS', 'avg')
  env.assertEqual(toSortedFlatList([1, ['subj1', '20', 'avg', '20']]), toSortedFlatList(res))

  res = env.cmd('FT.AGGREGATE', 'hash_idx', '*', 'LOAD', '3', '@subj1', 'AS', 'a', 'APPLY', '(@subj1+@subj1)/2', 'AS', 'avg')
  env.assertEqual(toSortedFlatList([1, ['a', '20', 'subj1', '20', 'avg', '20']]), toSortedFlatList(res))

  # json
  env.execute_command('FT.CREATE', 'json_idx', 'ON', 'JSON', 'SCHEMA', '$.name', 'AS', 'name', 'TEXT', 'SORTABLE',
                                                                        '$.subj1', 'AS', 'subj2', 'NUMERIC', 'SORTABLE')
  env.execute_command('JSON.SET', 'doc:1', '$', r'{"name":"Redis", "subj1":3.14}')
  env.expect('json.get', 'doc:1', '$').equal('[{"name":"Redis","subj1":3.14}]')
  # load a single field
  env.expect('FT.AGGREGATE', 'json_idx', '*', 'LOAD', '1', '@subj2') \
      .equal([1, ['subj2', '3.14']])
  # load a field with an attribute
  env.expect('FT.AGGREGATE', 'json_idx', '*', 'LOAD', '3', '@subj2', 'AS', 'a') \
      .equal([1, ['a', '3.14']])
  # load field and use `APPLY`
  env.expect('FT.AGGREGATE', 'json_idx', '*', 'LOAD', '3', '@subj2', 'AS', 'a', 'APPLY', '(@a+@a)/2', 'AS', 'avg') \
      .equal([1, ['a', '3.14', 'avg', '3.14']])
  # load a field implicitly with `APPLY`
  res = env.cmd('FT.AGGREGATE', 'json_idx', '*', 'APPLY', '(@subj2+@subj2)/2', 'AS', 'avg')
  env.assertEqual(toSortedFlatList([1, ['subj2', '3.14', 'avg', '3.14']]), toSortedFlatList(res))

  # load a field with an attribute
  env.expect('FT.AGGREGATE', 'json_idx', '*', 'LOAD', '3', '@$.subj1', 'AS', 'a') \
      .equal([1, ['a', '3.14']])
  # In this example we get both `a` and `subj1` since
  env.expect('FT.AGGREGATE', 'json_idx', '*', 'LOAD', '3', '@$.subj1', 'AS', 'a', 'APPLY', '(@a+@a)/2', 'AS', 'avg') \
      .equal([1, ['a', '3.14', 'avg', '3.14']])

@no_msan
def test_MOD1266(env):
  # Test parsing failure
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'n1', 'NUMERIC', 'SORTABLE', 'n2', 'NUMERIC', 'SORTABLE')
  conn.execute_command('HSET', 'doc1', 'n1', '1', 'n2', '1')
  conn.execute_command('HSET', 'doc2', 'n1', '2', 'n2', '2')
  conn.execute_command('HSET', 'doc2', 'n1', 'foo', 'n2', '-999')
  conn.execute_command('HSET', 'doc3', 'n1', '3', 'n2', '3')

  env.expect('FT.SEARCH', 'idx', '*', 'sortby', 'n2', 'DESC', 'RETURN', '1', 'n2') \
    .equal([2, 'doc3', ['n2', '3'], 'doc1', ['n2', '1']])

  assertInfoField(env, 'idx', 'num_docs', '2')

  # Test fetching failure. An object cannot be indexed
  env.execute_command('FT.CREATE', 'jsonidx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
  conn.execute_command('JSON.SET', '1', '$', r'{"t":"Redis"}')
  env.expect('FT.SEARCH', 'jsonidx', '*').equal([1, '1', ['$', '{"t":"Redis"}']])
  env.expect('FT.SEARCH', 'jsonidx', 'redis').equal([1, '1', ['$', '{"t":"Redis"}']])
  conn.execute_command('JSON.SET', '1', '$.t', r'{"inner_t":"Redis"}')
  env.expect('FT.SEARCH', 'jsonidx', '*').equal([0])

def testMemAllocated(env):
  conn = getConnectionByEnv(env)
  # sanity
  env.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 't', 'TEXT')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '0')
  conn.execute_command('HSET', 'doc1', 't', 'foo bar baz')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '2.765655517578125e-05', delta=0.01)
  conn.execute_command('HSET', 'doc2', 't', 'hello world')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '8.296966552734375e-05', delta=0.01)
  conn.execute_command('HSET', 'd3', 't', 'help')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '0.00013828277587890625', delta=0.01)

  conn.execute_command('DEL', 'd3')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '8.296966552734375e-05', delta=0.01)
  conn.execute_command('DEL', 'doc1')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '2.765655517578125e-05', delta=0.01)
  conn.execute_command('DEL', 'doc2')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '0')

  # mass
  env.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 't', 'TEXT')
  for i in range(1000):
    conn.execute_command('HSET', f'doc{i}', 't', f'text{i}')
  assertInfoField(env, 'idx2', 'key_table_size_mb', '0.027684211730957031', delta=0.01)

  for i in range(1000):
    conn.execute_command('DEL', f'doc{i}')
  assertInfoField(env, 'idx2', 'key_table_size_mb', '0')

def testUNF(env):
  conn = getConnectionByEnv(env)

  env.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                       'txt', 'TEXT', 'SORTABLE',
                       'txt_unf', 'TEXT', 'SORTABLE', 'UNF',
                       'tag', 'TAG', 'SORTABLE',
                       'tag_unf', 'TAG', 'SORTABLE', 'UNF')
  conn.execute_command('HSET', 'doc1', 'txt', 'FOO', 'txt_unf', 'FOO',
                                       'tag', 'FOO', 'tag_unf', 'FOO')

  # test `FOO`
  env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '4', '@txt', '@txt_unf', '@tag', '@tag_unf') \
    .equal([1, ['txt', 'foo', 'txt_unf', 'FOO', 'tag', 'foo', 'tag_unf', 'FOO']])

  # test `Maße`
  conn.execute_command('HSET', 'doc1', 'txt', u'Maße', 'txt_unf', u'Maße',
                                       'tag', u'Maße', 'tag_unf', u'Maße')
  env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '4', '@txt', '@txt_unf', '@tag', '@tag_unf') \
    .equal([1, ['txt', 'masse', 'txt_unf', u'Maße', 'tag', 'masse', 'tag_unf', u'Maße']])

  # test `Maße` with LOAD
  conn.execute_command('HSET', 'doc1', 'txt', 'Maße', 'txt_unf', u'Maße',
                                       'tag', 'Maße', 'tag_unf', u'Maße')
  env.expect('FT.AGGREGATE', 'idx', '*', \
             'LOAD',    '4', '@txt', '@txt_unf', '@tag', '@tag_unf', \
             'GROUPBY', '4', '@txt', '@txt_unf', '@tag', '@tag_unf') \
     .equal([1, ['txt', u'Maße', 'txt_unf', u'Maße', 'tag', u'Maße', 'tag_unf', 'Maße']])

def test_MOD_1517(env):
  conn = getConnectionByEnv(env)

  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'field1', 'TAG', 'SORTABLE',
                                                    'field2', 'TAG', 'SORTABLE')
  # both fields exist
  conn.execute_command('HSET', 'doc1', 'field1', 'val1', 'field2', 'val2', 'amount1', '1', 'amount2', '1')
  # first tag is nil
  conn.execute_command('HSET', 'doc2', 'field2', 'val2', 'amount1', '1', 'amount2', '1')
  # second tag is nil
  conn.execute_command('HSET', 'doc3', 'field1', 'val1', 'amount1', '1', 'amount2', '1')
  # both tags are nil
  conn.execute_command('HSET', 'doc4', 'amount1', '1', 'amount2', '1')

  res = [4, ['field1', None, 'field2', None, 'amount1Sum', '1', 'amount2Sum', '1'],
             ['field1', 'val1', 'field2', 'val2', 'amount1Sum', '1', 'amount2Sum', '1'],
             ['field1', None, 'field2', 'val2', 'amount1Sum', '1', 'amount2Sum', '1'],
             ['field1', 'val1', 'field2', None, 'amount1Sum', '1', 'amount2Sum', '1']]

  env.expect('FT.AGGREGATE', 'idx', '*',
             'LOAD', '2', '@amount1', '@amount2',
             'GROUPBY', '2', '@field1', '@field2',
             'REDUCE', 'SUM', '1', '@amount1', 'AS', 'amount1Sum',
             'REDUCE', 'SUM', '1', '@amount2', 'as', 'amount2Sum').equal(res)

@no_msan
def test_MOD1544(env):
  # Test parsing failure
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.name', 'AS', 'name', 'TEXT')
  conn.execute_command('JSON.SET', '1', '.', '{"name": "John Smith"}')
  res = [1, '1', ['name', '<b>John</b> Smith']]
  env.expect('FT.SEARCH', 'idx', '@name:(John)', 'RETURN', '1', 'name', 'HIGHLIGHT').equal(res)
  env.expect('FT.SEARCH', 'idx', '@name:(John)', 'RETURN', '1', 'name', 'HIGHLIGHT', 'FIELDS', '1', 'name').equal(res)

def test_MOD_1808(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
  conn.execute_command('hset', 'doc0', 't', 'world0')
  conn.execute_command('hset', 'doc1', 't', 'world1')
  conn.execute_command('hset', 'doc2', 't', 'world2')
  conn.execute_command('hset', 'doc3', 't', 'world3')
  res = env.cmd('FT.SEARCH', 'idx', '(~@t:world2) (~@t:world1) (~@fawdfa:wada)', 'SUMMARIZE', 'FRAGS', '1', 'LEN', '25', 'HIGHLIGHT', 'TAGS', "<span style='background-color:yellow'>", '</span>')
  env.assertEqual(toSortedFlatList(res), toSortedFlatList([4, 'doc2', ['t', "<span style='background-color:yellow'>world2</span>... "], 'doc1', ['t', "<span style='background-color:yellow'>world1</span>... "], 'doc0', ['t', 'world0'], 'doc3', ['t', 'world3']]))

def test_2370(env):
  # Test limit offset great than number of results
  conn = getConnectionByEnv(env)
  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 't2', 'TEXT')
  conn.execute_command('HSET', 'doc1', 't1', 'foo', 't2', 'bar')
  conn.execute_command('HSET', 'doc2', 't1', 'baz')

  # number of results is lower than LIMIT
  env.expect('FT.SEARCH', 'idx', '*', 'LIMIT', '10', '10').equal([2])
  # missing fields
  env.expect('FT.SEARCH', 'idx', '*').equal([2, 'doc1', ['t1', 'foo', 't2', 'bar'], 'doc2', ['t1', 'baz']])

def test_MOD1907(env):
  # Test FT.CREATE w/o fields parameters
  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA').error().contains('Fields arguments are missing')
  env.expect('FT.CREATE', 'idx', 'STOPWORDS', 0, 'SCHEMA').error().contains('Fields arguments are missing')

def test_SkipFieldWithNoMatch(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 't2', 'TEXT')
  conn.execute_command('HSET', 'doc1', 't1', 'foo', 't2', 'bar')

  excepted_res = ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'world', 'Counter', 1, 'Size', 1],
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 2]]


  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@t1:foo')
  env.assertEqual(res[1][3][1], ['Type', 'TEXT', 'Term', 'foo', 'Counter', 1, 'Size', 1])
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'foo')
  env.assertEqual(res[1][3][1], ['Type', 'TEXT', 'Term', 'foo', 'Counter', 1, 'Size', 1])
  # bar exists in `t2` only
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@t1:bar')
  env.assertEqual(res[1][3][1], ['Type', 'EMPTY', 'Counter', 0])
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'bar')
  env.assertEqual(res[1][3][1], ['Type', 'TEXT', 'Term', 'bar', 'Counter', 1, 'Size', 1] )

  # Check with NOFIELDS flag
  env.execute_command('FT.CREATE', 'idx_nomask', 'NOFIELDS', 'SCHEMA', 't1', 'TEXT', 't2', 'TEXT')
  waitForIndex(env, 'idx_nomask')

  res = env.cmd('FT.PROFILE', 'idx_nomask', 'SEARCH', 'QUERY', '@t1:foo')
  env.assertEqual(res[1][3][1], ['Type', 'TEXT', 'Term', 'foo', 'Counter', 1, 'Size', 1])
  res = env.cmd('FT.PROFILE', 'idx_nomask', 'SEARCH', 'QUERY', 'foo')
  env.assertEqual(res[1][3][1], ['Type', 'TEXT', 'Term', 'foo', 'Counter', 1, 'Size', 1])

  res = env.cmd('FT.PROFILE', 'idx_nomask', 'SEARCH', 'QUERY', '@t1:bar')
  env.assertEqual(res[1][3][1], ['Type', 'TEXT', 'Term', 'bar', 'Counter', 1, 'Size', 1])
  res = env.cmd('FT.PROFILE', 'idx_nomask', 'SEARCH', 'QUERY', 'bar')
  env.assertEqual(res[1][3][1], ['Type', 'TEXT', 'Term', 'bar', 'Counter', 1, 'Size', 1])

def test_update_num_terms(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0')

  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
  conn.execute_command('HSET', 'doc1', 't', 'foo')
  conn.execute_command('HSET', 'doc1', 't', 'bar')
  assertInfoField(env, 'idx', 'num_terms', '2')
  forceInvokeGC(env, 'idx')
  assertInfoField(env, 'idx', 'num_terms', '1')

def testOverMaxResults():
  env = Env(moduleArgs='MAXSEARCHRESULTS 20')
  env.skipOnCluster()
  conn = getConnectionByEnv(env)

  commands = [
    ['FT.CONFIG', 'SET', 'MAXAGGREGATERESULTS', '25'],
    ['FT.CONFIG', 'SET', 'MAXAGGREGATERESULTS', '20'],
    ['FT.CONFIG', 'SET', 'MAXAGGREGATERESULTS', '15'],
  ]

  for c in commands:
    env.cmd(*c)

    env.cmd('flushall')

    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')

    # test with number of documents lesser than MAXSEARCHRESULTS
    for i in range(10):
      conn.execute_command('HSET', i, 't', i)

    res = [10, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal(res)
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '0', '10').equal(res)
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '1', '20').equal([res[0], *res[2:]])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '5', '10').equal([res[0], *res[6:11]])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '10', '10').equal([10])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '20', '10').equal([10])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '30', '10').equal('OFFSET exceeds maximum of 20')

    # test with number of documents equal to MAXSEARCHRESULTS
    for i in range(10,20):
      conn.execute_command('HSET', i, 't', i)

    res = [20, '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '10', '10').equal(res)
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '1', '20').equal([res[0], *[str(i) for i in range(1, 20, 1)]])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '15', '10').equal([20, *res[6:11]])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '20', '10').equal([20])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '30', '10').equal('OFFSET exceeds maximum of 20')

    # test with number of documents greater than MAXSEARCHRESULTS
    for i in range(20,30):
      conn.execute_command('HSET', i, 't', i)

    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '1', '20').equal([30, *[str(i) for i in range(1, 20, 1)]])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '10', '10').equal([30, *res[1:11]])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '15', '10').equal([30, *res[6:11]])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '20', '10').equal([30])
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '25', '10').equal('OFFSET exceeds maximum of 20')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', '30', '10').equal('OFFSET exceeds maximum of 20')


def test_MOD_3372(env):
  #env.skipOnCluster()
  conn = getConnectionByEnv(env)

  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')

  env.expect('FT.EXPLAIN').error().contains('wrong number of arguments')
  env.expect('FT.EXPLAIN', 'idx').error().contains('wrong number of arguments')
  env.expect('FT.EXPLAIN', 'idx', 'foo').equal('UNION {\n  foo\n  +foo(expanded)\n}\n')
  env.expect('FT.EXPLAIN', 'idx', 'foo', 'verbatim').equal('foo\n')
  env.expect('FT.EXPLAIN', 'non-exist', 'foo').error().equal('non-exist: no such index')

  if not env.isCluster():
    # FT.EXPLAINCLI is not supported by the coordinator
    env.expect('FT.EXPLAINCLI').error().contains('wrong number of arguments')
    env.expect('FT.EXPLAINCLI', 'idx').error().contains('wrong number of arguments')
    env.expect('FT.EXPLAINCLI', 'idx', 'foo').equal(['UNION {', '  foo', '  +foo(expanded)', '}', ''])
    env.expect('FT.EXPLAINCLI', 'idx', 'foo', 'verbatim').equal(['foo', ''])
    env.expect('FT.EXPLAINCLI', 'non-exist', 'foo').error().equal('non-exist: no such index')

def test_MOD_3540(env):
  # disable SORTBY MAX for FT.SEARCH
  conn = getConnectionByEnv(env)

  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
  for i in range(10):
    conn.execute_command('HSET', i, 't', i)

  env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'DESC', 'MAX', '1').error()  \
                  .contains('SORTBY MAX is not supported by FT.SEARCH')

  env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '2', '@t', 'DESC', 'MAX', '1', 'LOAD', '*')  \
                  .equal([10, ['t', '9']])

  # SORTBY MAX followed by LIMIT
  env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '2', '@t', 'DESC', 'MAX', '1', 'LIMIT', '0', '2', 'LOAD', '*')  \
                  .equal([10, ['t', '9'], ['t', '8']])
  env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '2', '@t', 'DESC', 'MAX', '2', 'LIMIT', '0', '1', 'LOAD', '*')  \
                  .equal([10, ['t', '9']])
  env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '2', '@t', 'DESC', 'MAX', '1', 'LIMIT', '0', '0', 'LOAD', '*')  \
                  .equal([10])
  env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '2', '@t', 'DESC', 'MAX', '0', 'LIMIT', '0', '1', 'LOAD', '*')  \
                  .equal([10, ['t', '9']])

  # LIMIT followed by SORTBY MAX
  env.expect('FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', '2', 'SORTBY', '2', '@t', 'DESC', 'MAX', '1', 'LOAD', '*')  \
                  .equal([10, ['t', '9']])
  env.expect('FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', '1', 'SORTBY', '2', '@t', 'DESC', 'MAX', '2', 'LOAD', '*')  \
                  .equal([10, ['t', '9'], ['t', '8']])
  env.expect('FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', '1', 'SORTBY', '2', '@t', 'DESC', 'MAX', '0', 'LOAD', '*')  \
                  .equal([10, ['t', '9'], ['t', '8'], ['t', '7'], ['t', '6'], ['t', '5'], ['t', '4'], ['t', '3'], ['t', '2'], ['t', '1'], ['t', '0']])
  env.expect('FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', '0', 'SORTBY', '2', '@t', 'DESC', 'MAX', '1', 'LOAD', '*')  \
                  .equal([10])

def test_sortby_Noexist(env):
  conn = getConnectionByEnv(env)

  env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
  conn.execute_command('HSET', 'doc1', 't', '1')
  conn.execute_command('HSET', 'doc2', 'somethingelse', '2')
  conn.execute_command('HSET', 'doc3', 't', '3')
  conn.execute_command('HSET', 'doc4', 'somethingelse', '4')

  env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'ASC', 'LIMIT', '0', '2').equal([4, 'doc1', ['t', '1'], 'doc3', ['t', '3']])
  env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'DESC', 'LIMIT', '0', '2').equal([4, 'doc3', ['t', '3'], 'doc1', ['t', '1']])

  # receive a result w/o sortby field at the end.
  # remove in test to support test on cluster
  res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'ASC', 'LIMIT', '0', '3')
  env.assertEqual(res[0:5], [4, 'doc1', ['t', '1'], 'doc3', ['t', '3']])

  res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'DESC', 'LIMIT', '0', '3')
  env.assertEqual(res[0:5], [4, 'doc3', ['t', '3'], 'doc1', ['t', '1']])

  if not env.isCluster():
    env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'ASC', 'LIMIT', '0', '3').equal([4, 'doc1', ['t', '1'], 'doc3', ['t', '3'], 'doc2', ['somethingelse', '2']])
    env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'DESC', 'LIMIT', '0', '3').equal([4, 'doc3', ['t', '3'], 'doc1', ['t', '1'], 'doc4', ['somethingelse', '4']])

def test_sortby_Noexist_Sortables(env):
  ''' issue 3457 '''

  conn = getConnectionByEnv(env)
  sortable_options = [[True,True], [True,False], [False,True], [False,False]]

  for count, args in enumerate(sortable_options):
    sortable1 = ['SORTABLE'] if args[0] else []
    sortable2 = ['SORTABLE'] if args[1] else []
    conn.execute_command('FT.CREATE', 'idx{}'.format(count), 'SCHEMA', 'numval', 'NUMERIC' , *sortable1,
                                                'text', 'TEXT', *sortable2)

  for count, args in enumerate(sortable_options):
    # Use cluster {hashtag} to handle which keys are on the same shard (same cluster slot)
    conn.execute_command('HSET', '{key1}1', 'numval', '110')
    conn.execute_command('HSET', '{key1}2', 'numval', '108')
    conn.execute_command('HSET', '{key2}1', 'text', 'Meow')
    conn.execute_command('HSET', '{key2}2', 'text', 'Chirp')

    msg = 'sortable1: {}, sortable2: {}'.format(sortable1, sortable2)

    # Check ordering of docs:
    #   In cluster: Docs without sortby field are ordered by key name
    #   In non-cluster: Docs without sortby field are ordered by doc id (order of insertion/update)

    res = conn.execute_command('FT.SEARCH', 'idx{}'.format(count), '*', 'sortby', 'numval', 'ASC')
    env.assertEqual(res, [4,
        '{key1}2', ['numval', '108'], '{key1}1', ['numval', '110'],
        '{key2}1', ['text', 'Meow'], '{key2}2', ['text', 'Chirp'],
      ], message=msg)

    res = conn.execute_command('FT.SEARCH', 'idx{}'.format(count), '*', 'sortby', 'numval', 'DESC')
    env.assertEqual(res, [4,
        '{key1}1', ['numval', '110'], '{key1}2', ['numval', '108'],
        '{key2}2', ['text', 'Chirp'], '{key2}1', ['text', 'Meow'],
      ], message=msg)

  # Add more keys
  conn.execute_command('HSET', '{key1}3', 'text', 'Bark')
  conn.execute_command('HSET', '{key1}4', 'text', 'Quack')
  conn.execute_command('HSET', '{key2}3', 'numval', '109')
  conn.execute_command('HSET', '{key2}4', 'numval', '111')
  conn.execute_command('HSET', '{key2}5', 'numval', '108')
  conn.execute_command('HSET', '{key2}6', 'text', 'Squeak')

  for count, args in enumerate(sortable_options):
    res = conn.execute_command('FT.SEARCH', 'idx{}'.format(count), '*', 'sortby', 'numval', 'ASC')
    if env.isCluster():
      env.assertEqual(res, [10,
          '{key1}2', ['numval', '108'],
          '{key2}5', ['numval', '108'],
          '{key2}3', ['numval', '109'],
          '{key1}1', ['numval', '110'],
          '{key2}4', ['numval', '111'],
          '{key1}3', ['text', 'Bark'],
          '{key1}4', ['text', 'Quack'],
          '{key2}1', ['text', 'Meow'],
          '{key2}2', ['text', 'Chirp'],
          '{key2}6', ['text', 'Squeak'],
        ], message=msg)
    else:
      env.assertEqual(res, [10,
          '{key1}2', ['numval', '108'],
          '{key2}5', ['numval', '108'],
          '{key2}3', ['numval', '109'],
          '{key1}1', ['numval', '110'],
          '{key2}4', ['numval', '111'],
          '{key2}1', ['text', 'Meow'],
          '{key2}2', ['text', 'Chirp'],
          '{key1}3', ['text', 'Bark'],
          '{key1}4', ['text', 'Quack'],
          '{key2}6', ['text', 'Squeak'],
        ], message=msg)

    res = conn.execute_command('FT.SEARCH', 'idx{}'.format(count), '*', 'sortby', 'numval', 'DESC')
    if env.isCluster():
      env.assertEqual(res, [10,
          '{key2}4', ['numval', '111'],
          '{key1}1', ['numval', '110'],
          '{key2}3', ['numval', '109'],
          '{key2}5', ['numval', '108'],
          '{key1}2', ['numval', '108'],
          '{key2}6', ['text', 'Squeak'],
          '{key2}2', ['text', 'Chirp'],
          '{key2}1', ['text', 'Meow'],
          '{key1}4', ['text', 'Quack'],
          '{key1}3', ['text', 'Bark'],
        ], message=msg)
    else:
      env.assertEqual(res, [10,
          '{key2}4', ['numval', '111'],
          '{key1}1', ['numval', '110'],
          '{key2}3', ['numval', '109'],
          '{key2}5', ['numval', '108'],
          '{key1}2', ['numval', '108'],
          '{key2}6', ['text', 'Squeak'],
          '{key1}4', ['text', 'Quack'],
          '{key1}3', ['text', 'Bark'],
          '{key2}2', ['text', 'Chirp'],
          '{key2}1', ['text', 'Meow'],
        ], message=msg)


def testDeleteIndexes(env):
  # test cleaning of all specs from a prefix
  conn = getConnectionByEnv(env)
  for i in range(10):
    env.execute_command('FT.CREATE', i, 'PREFIX', '1', i / 2, 'SCHEMA', 't', 'TEXT')
    env.execute_command('FT.DROPINDEX', i)

  # create an additional index
  env.execute_command('FT.CREATE', i, 'PREFIX', '1', i / 2, 'SCHEMA', 't', 'TEXT')

def test_mod_4207(env):
  conn = getConnectionByEnv(env)

  env.cmd('FT.CREATE', 'idx1', 'FILTER', 'EXISTS(@country)', 'SCHEMA', 'business', 'TEXT', 'country', 'TEXT')
  env.cmd('FT.CREATE', 'idx2', 'FILTER', 'EXISTS(@business)', 'SCHEMA', 'business', 'TEXT', 'country', 'TEXT')
  conn.execute_command('HSET', 'address:1', 'business', 'foo', 'country', 'USA')
  conn.execute_command('HSET', 'address:2', 'business', 'bar', 'country', 'Israel')
  conn.execute_command('HSET', 'address:3', 'business', 'foo')
  conn.execute_command('HSET', 'address:4', 'country', 'Israel')

  env.expect('FT.SEARCH', 'idx1', '*', 'NOCONTENT').equal([3, 'address:1', 'address:2', 'address:4'])
  env.expect('FT.SEARCH', 'idx2', '*', 'NOCONTENT').equal([3, 'address:1', 'address:2', 'address:3'])

def test_mod_4255(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)

  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT').equal('OK')

  conn.execute_command('HSET', 'doc1', 'test', '1')
  conn.execute_command('HSET', 'doc2', 'test', '2')

  # test normal case
  # get first result
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@test', 'WITHCURSOR', 'COUNT', '1')
  env.assertEqual(res[0] ,[1, ['test', '1']])
  cursor = res[1]
  env.assertNotEqual(cursor ,0)
  # get second result
  res = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
  env.assertEqual(res[0] ,[1, ['test', '2']])
  cursor = res[1]
  env.assertNotEqual(cursor ,0)
  # get empty results after cursor was exhausted
  env.expect('FT.CURSOR', 'READ', 'idx', cursor).equal([[0], 0])


  # Test cursor after data structure that has changed due to insert
  res = env.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@test', 'WITHCURSOR', 'COUNT', '1')
  cursor = res[1]
  for i in range(3, 1001, 1):
      conn.execute_command('HSET', f'doc{i}', 'test', str(i))
  res = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
  env.assertEqual(res[0] ,[1, ['test', '2']])
  env.assertNotEqual(cursor ,0)

  # Test cursor after data structure that has changed due to insert
  res = env.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@test', 'WITHCURSOR', 'COUNT', '1')
  env.assertEqual(res[0] ,[1, ['test', '1']])
  cursor = res[1]
  env.assertNotEqual(cursor ,0)
  for i in range(3, 1001, 1):
    conn.execute_command('DEL', f'doc{i}', 'test', str(i))
  forceInvokeGC(env, 'idx')

  res = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
  env.assertEqual(res[0] ,[1, ['test', '2']])
  cursor = res[1]
  env.assertNotEqual(cursor ,0)
  res = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
  cursor = res[1]
  env.assertEqual(cursor ,0)

def test_as_startswith_as(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.attr1', 'AS', 'asa', 'TEXT').equal('OK')
    conn.execute_command('JSON.SET', 'doc2', '$', '{"attr1": "foo", "attr2": "bar"}')

    env.expect('FT.SEARCH', 'idx', '@asa:(foo)', 'RETURN', 1, 'asa').equal([1, 'doc2', ['asa', 'foo']])
    env.expect('FT.SEARCH', 'idx', '@asa:(foo)', 'RETURN', 3, 'asa', 'AS', 'asa').equal([1, 'doc2', ['asa', 'foo']])
    env.expect('FT.SEARCH', 'idx', '@asa:(foo)', 'RETURN', 3, '$.attr1', 'AS', 'asa').equal([1, 'doc2', ['asa', 'foo']])
    env.expect('FT.SEARCH', 'idx', '@asa:(foo)', 'RETURN', 3, '$.attr1', 'AS', '$.attr2').equal([1, 'doc2', ['$.attr2', 'foo']])
    env.expect('FT.SEARCH', 'idx', '@asa:(foo)', 'RETURN', 3, 'asa', 'AS', '$.attr2').equal([1, 'doc2', ['$.attr2', 'foo']])

    env.expect('FT.AGGREGATE', 'idx', '@asa:(foo)', 'LOAD', 1, 'asa').equal([1, ['asa', 'foo']])
    env.expect('FT.AGGREGATE', 'idx', '@asa:(foo)', 'LOAD', 3, 'asa', 'AS', 'asa').equal([1, ['asa', 'foo']])
    env.expect('FT.AGGREGATE', 'idx', '@asa:(foo)', 'LOAD', 3, '$.attr1', 'AS', 'asa').equal([1, ['asa', 'foo']])
    env.expect('FT.AGGREGATE', 'idx', '@asa:(foo)', 'LOAD', 3, '$.attr1', 'AS', '$.attr2').equal([1, ['$.attr2', 'foo']])
    env.expect('FT.AGGREGATE', 'idx', '@asa:(foo)', 'LOAD', 3, 'asa', 'AS', '$.attr2').equal([1, ['$.attr2', 'foo']])

def test_mod4296_badexpr(env):
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').equal('OK')
  env.expect('HSET', 'doc', 't', 'foo').equal(1)
  env.expect('FT.AGGREGATE', 'idx', 'foo', 'LOAD', 1, '@t', 'APPLY', '1%0', 'as', 'foo').equal([1, ['t', 'foo', 'foo', 'nan']])
  env.expect('FT.AGGREGATE', 'idx', 'foo', 'LOAD', 1, '@t', 'APPLY', '1/0', 'as', 'foo').equal([1, ['t', 'foo', 'foo', 'nan']])

def test_mod5062(env):
  env.skipOnCluster()
  env.expect('FT.CONFIG', 'SET', 'MAXSEARCHRESULTS', '0').ok()
  env.expect('FT.CONFIG', 'SET', 'MAXAGGREGATERESULTS', '0').ok()
  n = 100

  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()

  for i in range(n):
    env.expect('HSET', i, 't', 'hello world').equal(1)

  # verify no crash
  env.expect('FT.SEARCH', 'idx', 'hello').equal([n])

  # verify using counter instead of sorter
  search_profile = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'hello')
  env.assertEquals('Counter', search_profile[1][4][3][1])

  # verify no crash
  env.expect('FT.AGGREGATE', 'idx', 'hello').noError()
  env.expect('FT.AGGREGATE', 'idx', 'hello', 'LIMIT', 0, 0).equal([n])

  # verify using counter instead of sorter, even with explicit sort
  aggregate_profile = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', 'hello', 'SORTBY', '1', '@t')
  env.assertEquals('Counter', aggregate_profile[1][4][2][1])

def test_mod5252(env):
  # Create an index and add a document
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC').equal('OK')
  env.expect('HSET', 'doc', 't', 'Hello', 'n', '1').equal(2)

  # Test that the document is returned with the key name on a search command
  res = env.cmd('FT.SEARCH', 'idx', '*', 'RETURN', '1', '__key')
  env.assertEqual(res, [1, 'doc', ['__key', 'doc']])

  # Test that the document is returned with the key name WITH ALIAS on a search command
  res = env.cmd('FT.SEARCH', 'idx', '*', 'RETURN', '3', '__key', 'AS', 'key_name')
  env.assertEqual(res, [1, 'doc', ['key_name', 'doc']])

  # Test that the document is returned with the key name on an aggregate command
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@__key', 'SORTBY', '1', '@__key')
  env.assertEqual(res, [1, ['__key', 'doc']])

  # Test that the document is returned with the key name WITH ALIAS on an aggregate command
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '3', '@__key', 'AS', 'key_name', 'SORTBY', '1', '@key_name')
  env.assertEqual(res, [1, ['key_name', 'doc']])


def test_mod5791(env):
    con = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'L2',
               'DIM', 2).equal('OK')
    env.assertEqual(2, con.execute_command('HSET', 'doc1', 't', 'Hello world', 'v', 'abcdefgh'))
    env.assertEqual(2, con.execute_command('HSET', 'doc2', 't', 'Hello world', 'v', 'abcdefgi'))

    # The RSIndexResult object should be contructed as following:
    # UNION:
    #   INTERSECTION:
    #       metric
    #       term
    #   metric
    # While computing the scores, RSIndexResult_IterateOffsets is called. Validate that there is no corruption when
    # iterating the metric RSIndexResult (before, we treated it as "default" - which is the aggregate type, and we might
    # try access non-existing fields).
    res = env.cmd('FT.SEARCH', 'idx', '(@v:[VECTOR_RANGE 0.8 $blob] @t:hello) | @v:[VECTOR_RANGE 0.8 $blob]',
                  'WITHSCORES', 'DIALECT', '2', 'params', '2', 'blob', 'abcdefgh')
    env.assertEqual(res[:2], [1, 'doc1'])
