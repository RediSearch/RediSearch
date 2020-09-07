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
  

def testIssue1497(env):
  env.skipOnCluster()

  count = 110
  divide_by = 1000000   # ensure limits of geo are not exceeded 
  number_of_fields = 4  # one of every type

  env.execute_command('FLUSHALL')
  env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO').ok()

  res = env.execute_command('ft.info', 'idx')
  d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
  env.assertEqual(d['inverted_sz_mb'], '0')
  env.assertEqual(d['num_records'], '0')
  for i in range(count):
    geo = '1.23456,' + str(float(i) / divide_by)
    env.expect('HSET', 'doc%d' % i, 't', 'hello%d' % i, 'tg', 'world%d' % i, 'n', i, 'g', geo)
  res = env.cmd('FT.SEARCH idx *')
  env.assertEqual(res[0], count)

  res = env.execute_command('ft.info', 'idx')
  d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
  env.assertGreater(d['inverted_sz_mb'], '0')
  env.assertEqual(d['num_records'], str(count * number_of_fields))
  for i in range(count):
    env.expect('DEL', 'doc%d' % i)
    env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

  res = env.execute_command('ft.info', 'idx')
  d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
  env.assertEqual(d['inverted_sz_mb'], '0')
  env.assertEqual(d['num_records'], '0')
  #res = env.execute_command('CLIENT', 'PAUSE', 10000)
