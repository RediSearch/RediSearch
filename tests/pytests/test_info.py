from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList, forceInvokeGC, skip
from RLTest import Env
from time import sleep


def ft_info_to_dict(env, idx):
  res = env.cmd('ft.info', idx)
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


def test_numeric_info(env):

  env.cmd('ft.create', 'idx1', 'SCHEMA', 'n', 'numeric')
  env.cmd('ft.create', 'idx2', 'SCHEMA', 'n', 'numeric', 'SORTABLE')
  env.cmd('ft.create', 'idx3', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'UNF')
  env.cmd('ft.create', 'idx4', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'NOINDEX')
  env.cmd('ft.create', 'idx5', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'UNF', 'NOINDEX')

  res1 = ft_info_to_dict(env, 'idx1')['attributes']
  res2 = ft_info_to_dict(env, 'idx2')['attributes']
  res3 = ft_info_to_dict(env, 'idx3')['attributes']
  res4 = ft_info_to_dict(env, 'idx4')['attributes']
  res5 = ft_info_to_dict(env, 'idx5')['attributes']

  exp1 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC']]
  exp2 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC', 'SORTABLE', 'UNF']]
  exp3 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC', 'SORTABLE', 'UNF', 'NOINDEX']]

  env.assertEqual(res1, exp1)  # Nothing special about the numeric field
  env.assertEqual(res2, exp2)  # Numeric field is sortable, and automatically UNF
  env.assertEqual(res3, exp2)  # Numeric field is sortable, and explicitly UNF
  env.assertEqual(res4, exp3)  # Numeric field is sortable, explicitly NOINDEX, and automatically UNF
  env.assertEqual(res5, exp3)  # Numeric field is sortable, explicitly NOINDEX, and explicitly UNF
