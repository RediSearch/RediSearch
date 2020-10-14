from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList

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
  env.expect('ft.search idx * limit 0 1234567').equal([1L, 'doc', ['foo', 'hello', 'bar', 'world']]) 
  
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
  env.assertEqual(toSortedFlatList(res), toSortedFlatList([3L, 'movie:3', '2', 'movie:1', '2', 'movie:2', '1']))

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

