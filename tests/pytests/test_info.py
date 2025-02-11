from common import *
import time

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

@skip(cluster=True)
def test_info_text_tag_overhead(env):
  """Tests that the text and tag overhead fields report logic values (non-zero
  when there are docs, and 0 when there aren't, and the GC has worked)"""

  conn = getConnectionByEnv(env)

  # Create an index with a text and a tag field
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'tag1', 'TAG', 'text1', 'TEXT')

  # No docs, no GC --> no overhead
  res = index_info(env, 'idx')
  env.assertEqual(float(res['tag_overhead_sz_mb']), 0)
  env.assertEqual(float(res['text_overhead_sz_mb']), 0)

  # Add some docs (enough to enable GC, and for Trie/TrieMap splitting deletion
  # of multiple nodes)
  n_docs = 10000
  for i in range(n_docs):
    conn.execute_command('HSET', f'doc{i}', 'tag1', f'tag{i}', 'text1', f'text{i}')

  # Overhead > 0
  res = index_info(env, 'idx')
  tag_overhead = float(res['tag_overhead_sz_mb'])
  text_overhead = float(res['text_overhead_sz_mb'])
  total = float(res['total_index_memory_sz_mb'])
  env.assertGreater(tag_overhead, 0)
  env.assertGreater(text_overhead, 0)
  env.assertGreater(total, 0)

  # Delete half of the docs
  for i in range(int(n_docs / 2)):
    conn.execute_command('DEL', f'doc{i}')

  # Run GC
  forceInvokeGC(env, 'idx')
  time.sleep(1)

  # Overhead > 0, but smaller than before
  res = index_info(env, 'idx')
  env.assertGreater(tag_overhead, float(res['tag_overhead_sz_mb']))
  env.assertGreater(text_overhead, float(res['text_overhead_sz_mb']))
  env.assertGreater(total, float(res['total_index_memory_sz_mb']), 0)

  # Delete the rest of the docs
  for i in range(int(n_docs / 2), n_docs):
    conn.execute_command('DEL', f'doc{i}')

  # Run GC
  forceInvokeGC(env, 'idx')
  time.sleep(1)

  # Overhead = 0
  res = index_info(env, 'idx')
  env.assertEqual(float(res['tag_overhead_sz_mb']), 0)
  env.assertEqual(float(res['text_overhead_sz_mb']), 0)

def test_vecsim_info_stats_memory(env):
  env = Env(protocol=3)
  vec_size = 6
  data_type = 'FLOAT16'
  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'vector', 'VECTOR', 'FLAT', 6, 'DIM', 6, 'TYPE', 'float16', 'DISTANCE_METRIC', 'L2').ok()
  load_vectors_to_redis(env, 1000, 0, vec_size, data_type)
  def get_memory(info): return info[info.index('MEMORY') + 1]
  total_memory = 0
  for shard_conn in shardsConnections(env):
    total_memory += get_memory(shard_conn.execute_command(debug_cmd(), 'VECSIM_INFO', 'idx', 'vector'))
  info = shard_conn.execute_command('ft.info', 'idx')
  env.assertTrue("field statistics" in info)
  env.assertAlmostEqual(total_memory, info["field statistics"][0]["memory"], delta=env.shardsCount)

def test_vecsim_info_stats_marked_deleted(env):
  env = Env(protocol=3, moduleArgs='WORKERS 1 FORK_GC_RUN_INTERVAL 50000')
  conn = env.getClusterConnectionIfNeeded()
  vec_size = 6
  data_type = 'FLOAT16'
  conn.execute_command('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', 6, 'DIM', 6, 'TYPE', 'float16', 'DISTANCE_METRIC', 'L2')
  load_vectors_to_redis(env, 1000, 0, vec_size, data_type)
  env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok() # wait for HNSW graph construction to finish
  env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok() # pause to prevent repair jobs on the graph
  docs_to_delete = 100
  for i in range(1, 1 + docs_to_delete):
    conn.execute_command('DEL', f'{i}')
  info = conn.execute_command('ft.info', 'idx')
  env.assertTrue("field statistics" in info)
  env.assertEqual(info["field statistics"][0]["marked_deleted"], docs_to_delete)
  env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
  # Wait for all repair jobs to be finish, then run GC to remove the deleted vectors.
  env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
  env.expect(debug_cmd(), 'GC_FORCEINVOKE', 'idx').equal('DONE')
  info = conn.execute_command('ft.info', 'idx')
  env.assertEqual(info["field statistics"][0]["marked_deleted"], 0)
