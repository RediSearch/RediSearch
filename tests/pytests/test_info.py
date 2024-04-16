from common import *
from RLTest import Env
from time import sleep


def ft_info_to_dict(env, idx):
  res = env.execute_command('ft.info', idx)
  return {res[i]: res[i + 1] for i in range(0, len(res), 2)}

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
  sleep(1)

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
  sleep(1)

  # Overhead = 0
  res = index_info(env, 'idx')
  env.assertEqual(float(res['tag_overhead_sz_mb']), 0)
  env.assertEqual(float(res['text_overhead_sz_mb']), 0)
