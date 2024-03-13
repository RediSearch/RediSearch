from common import waitForIndex, forceInvokeGC, skip, to_list, index_info, Env

# The output for this test can be used for recreating documentation for `FT.INFO`
@skip()
def testInfo(env):
  count = 345678
  conn = env.getConnection()
  pl = conn.pipeline()

  idx = 'wikipedia'

  for i in range(count):
    geo = '1.23456,1.' + str(i / float(count))
    pl.execute_command('HSET', 'doc%d' % i, 'title', 'hello%d' % i,
                                            'body', '%dhello%dworld%dhow%dare%dyou%dtoday%d' % (i, i, i, i, i, i, i),
                                            'n', i / 17.0,
                                            'geo', geo)
    if i % 10000 == 0:
      pl.execute()
  pl.execute()

  env.expect('FT.CREATE', idx, 'STOPWORDS', 3, 'TLV', 'summer', '2020',
                               'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                                         'body', 'TEXT',
                                         'id', 'NUMERIC',
                                         'subject location', 'GEO').ok()

  waitForIndex(env, idx)

  for i in range(count):
    pl.execute_command('DEL', 'doc%d' % i)
    if i % 10000 == 0:
      pl.execute()
      forceInvokeGC(env, idx)
  pl.execute()

  #  GC stats
  for i in range(25):
    forceInvokeGC(env, idx)

  # cursor stats
  #query = ['FT.AGGREGATE', idx, '*', 'WITHCURSOR']
  #res = env.cmd(*query)
  #env.cmd('FT.CURSOR', 'READ', idx, str(res[1]))

  #print info

def test_vecsim_info():
  env = Env(protocol=3)
  dim = 2

  for alg in ["HNSW", "FLAT"]:
    info_expected = {"identifier": "vec", "attribute": "vec", "type": "VECTOR", "algorithm": alg,
                     "dim": dim, "flags": []}
    additional_params = {"M": 12, "ef_construction": 100} if alg == "HNSW" else {}
    info_expected.update(additional_params)
    # for each data type
    for type in ["FLOAT32", "FLOAT64"]:
      info_expected["data_type"] = type
      # for each metric
      for metric in ["L2", "IP", "COSINE"]:
        info_expected["distance_metric"] = metric
        # create index
        params = ["TYPE", type, "DIM", dim,
                  "DISTANCE_METRIC", metric, *to_list(additional_params)]

        env.expect('FT.CREATE', "idx", 'SCHEMA', "vec", 'VECTOR', alg, len(params), *params).ok()

        # check info
        info = env.executeCommand('ft.info', 'idx')
        env.assertEqual(info["attributes"][0], info_expected,
                        message=f"info for ({alg, type, metric})")

        # drop index
        env.expect('FT.DROPINDEX', 'idx').ok()

def test_numeric_info(env):

  env.cmd('ft.create', 'idx1', 'SCHEMA', 'n', 'numeric')
  env.cmd('ft.create', 'idx2', 'SCHEMA', 'n', 'numeric', 'SORTABLE')
  env.cmd('ft.create', 'idx3', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'UNF')
  env.cmd('ft.create', 'idx4', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'NOINDEX')
  env.cmd('ft.create', 'idx5', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'UNF', 'NOINDEX')

  res1 = index_info(env, 'idx1')['attributes']
  res2 = index_info(env, 'idx2')['attributes']
  res3 = index_info(env, 'idx3')['attributes']
  res4 = index_info(env, 'idx4')['attributes']
  res5 = index_info(env, 'idx5')['attributes']

  exp1 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC']]
  exp2 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC', 'SORTABLE', 'UNF']]
  exp3 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC', 'SORTABLE', 'UNF', 'NOINDEX']]

  env.assertEqual(res1, exp1)  # Nothing special about the numeric field
  env.assertEqual(res2, exp2)  # Numeric field is sortable, and automatically UNF
  env.assertEqual(res3, exp2)  # Numeric field is sortable, and explicitly UNF
  env.assertEqual(res4, exp3)  # Numeric field is sortable, explicitly NOINDEX, and automatically UNF
  env.assertEqual(res5, exp3)  # Numeric field is sortable, explicitly NOINDEX, and explicitly UNF
