# -*- coding: utf-8 -*-

from common import *

def test_1282(env):
  env.expect('FT.CREATE idx ON HASH SCHEMA txt1 TEXT').equal('OK')
  env.expect('FT.ADD idx doc1 1.0 FIELDS txt1 foo').equal('OK')

  # optional search for new word would crash server
  env.expect('FT.SEARCH idx', '~foo').equal([1L, 'doc1', ['txt1', 'foo']])
  env.expect('FT.SEARCH idx', '~bar ~foo').equal([1L, 'doc1', ['txt1', 'foo']])

def test_1304(env):
  env.expect('FT.CREATE idx SCHEMA txt1 TEXT').equal('OK')
  env.expect('FT.EXPLAIN idx -20*').equal('PREFIX{-20*}\n')
  env.expect('FT.EXPLAIN idx -\\20*').equal('NOT{\n  PREFIX{20*}\n}\n')

def test_1414(env):
  env.skipOnCluster()
  env.expect('FT.CREATE idx SCHEMA txt1 TEXT').equal('OK')
  env.expect('ft.add idx doc 1 fields foo hello bar world').ok()
  env.expect('ft.search idx * limit 0 1234567').error().contains('LIMIT exceeds maximum of 1000000')
  env.expect('FT.CONFIG set MAXSEARCHRESULTS -1').equal('OK')
  env.assertEqual(toSortedFlatList(env.cmd('ft.search idx * limit 0 1234567')), toSortedFlatList([1L, 'doc', ['foo', 'hello', 'bar', 'world']]))

def test_1502(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('HSET', 'a', 'bar', 'hello')

  env.expect('FT.CREATE idx1 SKIPINITIALSCAN SCHEMA foo TEXT').ok()
  env.expect('FT.CREATE idx2 SKIPINITIALSCAN SCHEMA foo TEXT').ok()

  env.expect('ft.search idx1 *').equal([0L])
  env.expect('ft.search idx2 *').equal([0L])

  env.expect('FT.ALTER idx1 SKIPINITIALSCAN SCHEMA ADD bar TEXT').ok()
  env.expect('FT.ALTER idx2 SCHEMA ADD bar TEXT').ok()
  waitForIndex(env, 'idx2')

  env.expect('ft.search idx1 *').equal([0L])
  env.expect('ft.search idx2 *').equal([1L, 'a', ['bar', 'hello']])

def test_1601(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx:movie', 'SCHEMA', 'title', 'TEXT')
  conn.execute_command('HSET', 'movie:1', 'title', 'Star Wars: Episode I - The Phantom Menace')
  conn.execute_command('HSET', 'movie:2', 'title', 'Star Wars: Episodes II - Attack of the Clones')
  conn.execute_command('HSET', 'movie:3', 'title', 'Star Wars: Episode III - Revenge of the Sith')
  res = env.cmd('ft.search idx:movie @title:(episode) withscores nocontent')
  env.assertEqual(res[0], 3L)

def testMultiSortby(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'SORTABLE', 't2', 'TEXT', 'SORTABLE', 't3', 'TEXT', 'SORTABLE')
  conn.execute_command('FT.ADD', 'idx', '1', '1', 'FIELDS', 't1', 'foo', 't2', 'bar', 't3', 'baz')
  conn.execute_command('FT.ADD', 'idx', '2', '1', 'FIELDS', 't1', 'bar', 't2', 'foo', 't3', 'baz')
  sortby_t1 = [2L, '2', '1']
  sortby_t2 = [2L, '1', '2']
  env.expect('ft.search idx foo nocontent sortby t1 asc').equal(sortby_t1)
  env.expect('ft.search idx foo nocontent sortby t2 asc').equal(sortby_t2)
  env.expect('ft.search idx foo nocontent sortby t1 sortby t3').error()\
    .contains('Multiple SORTBY steps are not allowed. Sort multiple fields in a single step')
  #TODO: allow multiple sortby steps
  #env.expect('ft.search idx foo nocontent sortby t1 sortby t3').equal(sortby_t1)
  #env.expect('ft.search idx foo nocontent sortby t2 sortby t3').equal(sortby_t2)

def test_1667(env):
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'text', 'TEXT')
  env.expect('ft.search idx @tag:{a}').equal([0L])
  env.expect('ft.search idx @tag:{b}').equal([0L])

  conn.execute_command('HSET', 'doc', 'tag', 'a,b')
  conn.execute_command('HSET', 'doc1', 'tag', 'abc')

  # test single stopword
  env.expect('ft.search idx @tag:{a}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{b}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c}').equal([0L])

  # test stopword in list
  env.expect('ft.search idx @tag:{a|c}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c|a}').equal([1L, 'doc', ['tag', 'a,b']])
  env.expect('ft.search idx @tag:{c|a|c}').equal([1L, 'doc', ['tag', 'a,b']])

  # test stopword with prefix
  env.expect('ft.search idx @tag:{ab*}').equal([1L, 'doc1', ['tag', 'abc']])
  env.expect('ft.search idx @tag:{abc*}').equal([1L, 'doc1', ['tag', 'abc']])

  # ensure regular text field
  conn.execute_command('HSET', 'doc_a', 'text', 'a')
  conn.execute_command('HSET', 'doc_b', 'text', 'b')
  env.expect('ft.search idx a').equal([0L])
  env.expect('ft.search idx b').equal([1L, 'doc_b', ['text', 'b']])

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
  env.expect(*args_list).error().contains('Schema is limited to 128 TEXT fields')
  env.expect('FT.DROPINDEX', 'idx')

  args_list = ['FT.CREATE', 'idx', 'SCHEMA']
  for i in range(2):
    args_list.extend(['txt', 'TEXT'])
  env.expect(*args_list).error().contains('Duplicate field in schema - txt')
  env.expect('FT.DROPINDEX', 'idx')

def test_issue1834(env):
  # Stopword query is case sensitive.
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
  conn.execute_command('HSET', 'doc', 't', 'hell hello')

  env.expect('FT.SEARCH', 'idx', 'hell|hello', 'HIGHLIGHT').equal([1L, 'doc', ['t', '<b>hell</b> <b>hello</b>']])

def test_issue1932(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    env.expect('FT.AGGREGATE', 'idx', '*', 'LIMIT', '100000000000000000', '100000000000', 'SORTBY', '1', '@t').error() \
      .contains('OFFSET exceeds maximum of 1000000')

def test_issue1988(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    env.expect('FT.SEARCH', 'idx', 'foo').equal([1L, 'doc1', ['t', 'foo']])
    env.expect('FT.SEARCH', 'idx', 'foo', 'WITHSCORES').equal([1L, 'doc1', '1', ['t', 'foo']])
    env.expect('FT.SEARCH', 'idx', 'foo', 'SORTBY' , 't').equal([1L, 'doc1', ['t', 'foo']])
    env.expect('FT.SEARCH', 'idx', 'foo', 'WITHSCORES', 'SORTBY' , 't').equal([1L, 'doc1', '1', ['t', 'foo']])

def test_MOD1266(env):
  # Test parsing failure
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'n1', 'NUMERIC', 'SORTABLE', 'n2', 'NUMERIC', 'SORTABLE')
  conn.execute_command('HSET', 'doc1', 'n1', '1', 'n2', '1')
  conn.execute_command('HSET', 'doc2', 'n1', '2', 'n2', '2')
  conn.execute_command('HSET', 'doc2', 'n1', 'foo', 'n2', '-999')
  conn.execute_command('HSET', 'doc3', 'n1', '3', 'n2', '3')

  env.expect('FT.SEARCH', 'idx', '*', 'sortby', 'n2', 'DESC', 'RETURN', '1', 'n2')  \
    .equal([2L, 'doc3', ['n2', '3'], 'doc1', ['n2', '1']])

def testMemAllocated(env):
  conn = getConnectionByEnv(env)
  # sanity
  conn.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 't', 'TEXT')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '0')
  conn.execute_command('HSET', 'doc1', 't', 'foo bar baz')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '2.765655517578125e-05')
  conn.execute_command('HSET', 'doc2', 't', 'hello world')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '8.296966552734375e-05')
  conn.execute_command('HSET', 'd3', 't', 'help')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '0.00013828277587890625')

  conn.execute_command('DEL', 'd3')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '8.296966552734375e-05')
  conn.execute_command('DEL', 'doc1')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '2.765655517578125e-05')
  conn.execute_command('DEL', 'doc2')
  assertInfoField(env, 'idx1', 'key_table_size_mb', '0')

  # mass
  conn.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 't', 'TEXT')
  for i in range(1000):
    conn.execute_command('HSET', 'doc%d' % i, 't', 'text%d' % i)
  assertInfoField(env, 'idx2', 'key_table_size_mb', '0.027684211730957031')

  for i in range(1000):
    conn.execute_command('DEL', 'doc%d' % i)
  assertInfoField(env, 'idx2', 'key_table_size_mb', '0')

def testUNF(env):
  conn = getConnectionByEnv(env)

  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'SORTABLE',
                                                      'txt_unf', 'TEXT', 'SORTABLE', 'UNF',
                                                      'tag', 'TAG', 'SORTABLE',
                                                      'tag_unf', 'TAG', 'SORTABLE', 'UNF')
  conn.execute_command('HSET', 'doc1', 'txt', 'FOO', 'txt_unf', 'FOO',
                                       'tag', 'FOO', 'tag_unf', 'FOO')

  # test `FOO`
  env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '4', '@txt', '@txt_unf', '@tag', '@tag_unf')  \
    .equal([1L, ['txt', 'foo', 'txt_unf', 'FOO', 'tag', 'foo', 'tag_unf', 'FOO']])

  # test `Maße`
  conn.execute_command('HSET', 'doc1', 'txt', 'Maße', 'txt_unf', 'Maße',
                                       'tag', 'Maße', 'tag_unf', 'Maße')
  env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '4', '@txt', '@txt_unf', '@tag', '@tag_unf')  \
    .equal([1L, ['txt', 'masse', 'txt_unf', 'Ma\xc3\x9fe', 'tag', 'masse', 'tag_unf', 'Ma\xc3\x9fe']])

  # test `Maße` with LOAD
  conn.execute_command('HSET', 'doc1', 'txt', 'Maße', 'txt_unf', 'Maße',
                                       'tag', 'Maße', 'tag_unf', 'Maße')
  env.expect('FT.AGGREGATE', 'idx', '*',                              \
             'LOAD',    '4', '@txt', '@txt_unf', '@tag', '@tag_unf',  \
             'GROUPBY', '4', '@txt', '@txt_unf', '@tag', '@tag_unf')  \
    .equal([1L, ['txt', 'Ma\xc3\x9fe', 'txt_unf', 'Ma\xc3\x9fe', 'tag', 'Ma\xc3\x9fe', 'tag_unf', 'Ma\xc3\x9fe']])

def test_MOD_1517(env):
  conn = getConnectionByEnv(env)

  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'field1', 'TAG', 'SORTABLE',
                                                     'field2', 'TAG', 'SORTABLE')
  # both fields exist
  conn.execute_command('HSET', 'doc1', 'field1', 'val1', 'field2', 'val2', 'amount1', '1', 'amount2', '1')
  # first tag is nil
  conn.execute_command('HSET', 'doc2', 'field2', 'val2', 'amount1', '1', 'amount2', '1')
  # second tag is nil
  conn.execute_command('HSET', 'doc3', 'field1', 'val1', 'amount1', '1', 'amount2', '1')
  # both tags are nil
  conn.execute_command('HSET', 'doc4', 'amount1', '1', 'amount2', '1')

  res = [4L, ['field1', None, 'field2', None, 'amount1Sum', '1', 'amount2Sum', '1'],
             ['field1', 'val1', 'field2', 'val2', 'amount1Sum', '1', 'amount2Sum', '1'],
             ['field1', None, 'field2', 'val2', 'amount1Sum', '1', 'amount2Sum', '1'],
             ['field1', 'val1', 'field2', None, 'amount1Sum', '1', 'amount2Sum', '1']]

  env.expect('FT.AGGREGATE', 'idx', '*',
             'LOAD', '2', '@amount1', '@amount2',
             'GROUPBY', '2', '@field1', '@field2',
             'REDUCE', 'SUM', '1', '@amount1', 'AS', 'amount1Sum',
             'REDUCE', 'SUM', '1', '@amount2', 'as', 'amount2Sum').equal(res)

def test_MOD_1808(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
  conn.execute_command('hset', 'doc0', 't', 'world0')
  conn.execute_command('hset', 'doc1', 't', 'world1')
  conn.execute_command('hset', 'doc2', 't', 'world2')
  conn.execute_command('hset', 'doc3', 't', 'world3')
  env.expect('FT.SEARCH', 'idx', '(~@t:world2) (~@t:world1) (~@fawdfa:wada)', 'SUMMARIZE', 'FRAGS', '1', 'LEN', '25', 'HIGHLIGHT', 'TAGS', "<span style='background-color:yellow'>", '</span>')\
  .equal([4L, 'doc2', ['t', "<span style='background-color:yellow'>world2</span>... "], 'doc1', ['t', 'world1'], 'doc0', ['t', 'world0'], 'doc3', ['t', 'world3']])

