# -*- coding: utf-8 -*-

from cmath import inf
from email import message
from includes import *
from common import *
from RLTest import Env


def testEmptyBuffer(env):
    if not POWER_TO_THE_WORKERS:
        env.skip()
    env = Env(moduleArgs='WORKER_THREADS 1 ENABLE_THREADS TRUE')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    env.expect('ft.search', 'idx', '*', 'sortby', 'n').equal([0])

def CreateAndSearchSortBy(env, docs_count):
    if not POWER_TO_THE_WORKERS:
        env.skip()
    env = Env(moduleArgs='WORKER_THREADS 1 ENABLE_THREADS TRUE')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')
    conn = getConnectionByEnv(env)

    for n in range (1, docs_count + 1):
        doc_name = f'doc{n}'
        conn.execute_command('HSET', doc_name, 'n', n)
    output = conn.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'n')

    # The first element in the results array is the number of docs.
    env.assertEqual(output[0], docs_count)

    # The results are sorted according to n
    result_len = 2
    n = 1
    for i in range(1, len(output) - result_len, result_len):
        result = output[i: i + result_len]
        # docs id starts from 1
        # each result should contain the doc name, the field name and its value
        expected = [f'doc{n}', ['n', f'{n}']]
        env.assertEqual(result, expected)
        n += 1

def testSimpleBuffer(env):
    CreateAndSearchSortBy(env, docs_count = 10)

# In this test we have more than BlockSize docs to buffer, we want to make sure there are no leaks
# caused by the buffer memory management.
def testMultipleBlocksBuffer(env):
    CreateAndSearchSortBy(env, docs_count = 2500)

'''
Test pipeline:
Sorter with no access to redis:
case 1: no loader (sortby SORTABLE NOCONTENT/sortby a (SORTABLE UNF) return a, no sortby NOCONTENT)
expected pipeline: root(<-scorer)<-sorter
case 2: with loader (sortby SORTABLE (loadall)/ sortby SORTABLE a return b, no sortby (loadall) )
expected pipeline: root<-(<-scorer)sorter<-buffer-locker<-loader<-unlocker
case 3: with pager, no loader
expected pipeline: root<-sorter<-pager
case 4: with pager, with loader
expected pipeline: root<-sorter<-pager<-buffer-locker<-loader<-unlocker

Sorter with access to redis:
case 1: no loader (sortby NOTSORTABLE NOCONTENT/sortby a (NOTSORTABLE) return a)
expected pipeline: root<-buffer-locker<-sorter<-unlocker
case 2: with loader (sortby NOTSORTABLE (loadall)/ sortby NOTSORTABLE a return b)
expected pipeline: root<-buffer-locker<-sorter<-loader<-unlocker
case 3: with pager, no loader
expected pipeline: root<-buffer-locker<-sorter<-pager<-unlocker
case 4: with pager, with loader
expected pipeline: root<-buffer-locker<-sorter<-pager<-loader<-unlocker

Additional rp:
case 1: highlighter (last to access redis is not endProc)
expected pipeline: root<-sorter<-buffer-locker<-loader<-unlocker<-highlighter
case 2: metric
expected pipeline: root<-metric<-sorter<-buffer-locker<-loader<-unlocker
'''

def get_pipeline(profile_res):
    for entry in profile_res[1]:
        if (entry[0] == 'Result processors profile'):
            return entry

def test_pipeline(env):
    if not POWER_TO_THE_WORKERS:
        env.skip()
    env = Env(moduleArgs='WORKER_THREADS 1 ENABLE_THREADS TRUE')
    env.skipOnCluster()
    env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

    docs_count = 3
    # if we use LIMIT
    reply_length = 1

    root = ['Type', 'Index', 'Counter', docs_count]
    scorer = ['Type', 'Scorer', 'Counter', docs_count]
    def sorter(paged= False):
        return ['Type', 'Sorter', 'Counter', docs_count if not paged else reply_length]
    def buffer_locker(paged= False):
        return ['Type', 'Buffer and Locker', 'Counter', docs_count if not paged else reply_length]
    def unlocker(paged= False):
        return ['Type', 'Unlocker', 'Counter', docs_count if not paged else reply_length]
    def loader(paged= False):
        return ['Type', 'Loader', 'Counter', docs_count if not paged else reply_length]
    def apply(paged= False):
        return ['Type', 'Projector - Operator ^', 'Counter', docs_count if not paged else reply_length]
    pager = ['Type', 'Pager/Limiter', 'Counter', reply_length]
    highlighter = ['Type', 'Highlighter', 'Counter', docs_count]

    sortable_UNF_field_name = 'n_sortable'
    nonsortable_field_name = 'n'
    notindexed_field_name = 'x'


    env.cmd('FT.CREATE', 'idx', 'SCHEMA', nonsortable_field_name, 'NUMERIC', sortable_UNF_field_name, 'NUMERIC', 'SORTABLE', 'UNF')
    conn = getConnectionByEnv(env)

    env.assertEqual(conn.execute_command('HSET', 'doc1', nonsortable_field_name, '102', sortable_UNF_field_name, '202'), 2)
    env.assertEqual(conn.execute_command('HSET', 'doc2', nonsortable_field_name, '101', sortable_UNF_field_name, '201'), 2)
    env.assertEqual(conn.execute_command('HSET', 'doc3', nonsortable_field_name, '100', sortable_UNF_field_name, '200'), 2)

    ft_profile_search_cmd = ['FT.PROFILE', 'idx', 'SEARCH', 'LIMITED', 'QUERY']
    ft_profile_aggregate_cmd = ft_profile_search_cmd.copy()
    ft_profile_aggregate_cmd[2] = 'AGGREGATE'
    search_UNF_sortable = '@n_sortable:[200 202]'
    sortby_UNF_sortable_s = ['SORTBY', sortable_UNF_field_name]
    sortby_not_sortable_s = ['SORTBY', nonsortable_field_name]
    sortby_UNF_sortable_a = ['SORTBY', 1, '@' + sortable_UNF_field_name]
    sortby_not_sortable_a = ['SORTBY', 1, '@' + nonsortable_field_name]

    ############################### Sorter WITH NO access to redis ###############################

    ''' case 1: no loader (sortby SORTABLE NOCONTENT/sortby a (SORTABLE UNF) return a, no sortby NOCONTENT)
        expected pipeline: root(<-scorer)<-sorter  '''
    expected_pipeline = ['Result processors profile', root, sorter()]
    #sortby SORTABLE NOCONTENT
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_UNF_sortable_s, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #sortby a (SORTABLE UNF) return a
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_UNF_sortable_s, 'RETURN', 1, sortable_UNF_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #no sortby NOCONTENT
    expected_pipeline = ['Result processors profile', root, scorer, sorter()]
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ''' case 2: with loader (sortby SORTABLE (loadall)/ sortby SORTABLE a return b, no sortby (loadall) )
        expected pipeline: root<-(<-scorer)sorter<-buffer-locker<-loader<-unlocker  '''
    expected_pipeline = ['Result processors profile', root, sorter(), buffer_locker(), loader(), unlocker()]
    #sortby SORTABLE (loadall)
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_UNF_sortable_s)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #sortby a (SORTABLE UNF) return b
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_UNF_sortable_s, 'RETURN', 1, notindexed_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #no sortby (loadall)
    expected_pipeline = ['Result processors profile', root, scorer, sorter(), buffer_locker(), loader(), unlocker()]
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 3: with pager, no loader (sortby a (SORTABLE UNF) LIMIT 1 1 NOCONTENT)
        expected pipeline: root<-sorter<-pager '''
    paged = True
    expected_pipeline = ['Result processors profile', root, sorter(paged), pager]
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_UNF_sortable_s, 'LIMIT', 1, 1, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 4: with pager, with loader (sortby a (SORTABLE UNF) LIMIT 1 1 (loadall))
        expected pipeline: root<-sorter<-pager<-buffer-locker<-loader<-unlocker'''
    expected_pipeline = ['Result processors profile', root, sorter(paged), pager, buffer_locker(paged), loader(paged), unlocker(paged)]
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_UNF_sortable_s, 'LIMIT', 1, 1)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ############################### Sorter WITH access to redis: ###############################

    ''' case 1: no loader
        expected pipeline: root<-buffer-locker<-loader<-unlocker<-sorter '''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker(), sorter()]
    #sortby NOT SORTABLE NOCONTENT
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_not_sortable_s, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #sortby a (NOTSORTABLE) return a
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_not_sortable_s, 'RETURN', 1, nonsortable_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ''' case 2: with loader
        expected pipeline: root<-buffer-locker<-loader<-sorter<-loader<-unlocker'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), sorter(), loader(), unlocker()]
    #sortby NOT SORTABLE (loadall)
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_not_sortable_s)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    # sortby NOTSORTABLE a return b
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_not_sortable_s, 'RETURN', 1, notindexed_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 3: with pager, no loader
        expected pipeline: root<-buffer-locker<-loader<-unlocker<-sorter<-pager'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker(), sorter(paged), pager]
    #(sortby NOTSORTABLE LIMIT 1 1 NOCONTENT)
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_not_sortable_s, 'LIMIT', 1, 1, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 4: with pager, with loader
        expected pipeline: root<-buffer-locker<-loader<-sorter<-pager<-loader<-unlocker '''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), sorter(paged), pager, loader(paged), unlocker(paged)]
    # (sortby NOTSORTABLE LIMIT 1 1 (loadall))
    res = conn.execute_command(*ft_profile_search_cmd, search_UNF_sortable, *sortby_not_sortable_s, 'LIMIT', 1, 1)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    # AGGREGATE pipeline
    ############################### Sorter WITH NO access to redis ###############################

    ''' case 1: no loader (sortby SORTABLE NOCONTENT/sortby a (SORTABLE UNF) return a, no sortby NOCONTENT)
        expected pipeline: root<-sorter  '''
    expected_pipeline = ['Result processors profile', root, sorter()]
    #sortby SORTABLE NOCONTENT
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #sortby a (SORTABLE UNF) return a
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'LOAD', 1, sortable_UNF_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #no sortby NOCONTENT
    expected_pipeline = ['Result processors profile', root]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ''' case 2: with loader (sortby SORTABLE loadall / sortby SORTABLE a return b, no sortby loadall )
        expected pipeline: root<-sorter<-buffer-locker<-loader<-unlocker  '''
    expected_pipeline = ['Result processors profile', root, sorter(), buffer_locker(), loader(), unlocker()]
    #sortby SORTABLE (loadall)
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'LOAD', '*')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #sortby a (SORTABLE UNF) return b
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'LOAD', 1, notindexed_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #no sortby
    expected_pipeline = ['Result processors profile', root]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    #no sortby (loadall)
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker()]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, 'LOAD', '*')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 3: with pager, no loader (sortby a (SORTABLE UNF) LIMIT 1 1 NOCONTENT)
        expected pipeline: root<-sorter<-pager '''
    paged = True
    expected_pipeline = ['Result processors profile', root, sorter(paged), pager]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'LIMIT', 1, 1, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 4: with pager, with no loader (sortby a (SORTABLE UNF) LIMIT 1 1)
        expected pipeline: root<-sorter<-pager'''
    expected_pipeline = ['Result processors profile', root, sorter(paged), pager]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'LIMIT', 1, 1)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 5: with pager, with loader (sortby a (SORTABLE UNF) LIMIT 1 1 loadall)
        expected pipeline: root<-sorter<-pager<-buffer-locker<-loader<-unlocker'''
    expected_pipeline = ['Result processors profile', root, sorter(paged), pager, buffer_locker(paged), loader(paged), unlocker(paged)]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'LIMIT', 1, 1, 'LOAD', '*')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 6: with loader and applier (sortby a (SORTABLE UNF) LOAD 1 @n apply @n^2)
        expected pipeline: root<-sorter<-buffer-locker<-loader<-apply<-unlocker'''
    expected_pipeline = ['Result processors profile', root, sorter(), buffer_locker(), loader(), apply(), unlocker()]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_UNF_sortable_a, 'LOAD', 1, nonsortable_field_name, 'APPLY', '@' + nonsortable_field_name + '^2')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 7: with applier, sorter and loader (sortby n apply @n^2)
        expected pipeline: root<-buffer-locker<-loader<-unlocker<-sorter<-apply'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker(), sorter(), apply()]
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'APPLY', '@' + nonsortable_field_name + '^2')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ############################### Sorter WITH access to redis: ###############################

    ''' case 1: no loader
        expected pipeline: root<-buffer-locker<-loader<-unlocker<-sorter '''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker(), sorter()]
    #sortby NOT SORTABLE NOCONTENT
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    #sortby a (NOTSORTABLE) return a
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'LOAD', 1, nonsortable_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ''' case 2: with loader
        expected pipeline: root<-buffer-locker<-loader<-unlocker<-sorter'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker(), sorter()]
    #sortby NOT SORTABLE
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ''' case 2.1: with loader
        expected pipeline: root<-buffer-locker<-loader<-sorter<-loader<-unlocker'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), sorter(), loader(), unlocker()]
    #sortby NOT SORTABLE loadall
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'LOAD', '*')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    # sortby NOTSORTABLE a return b
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'LOAD', 1, notindexed_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 3: with pager, no loader
        expected pipeline: root<-buffer-locker<-loader<-unlocker<-sorter<-pager'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker(), sorter(paged), pager]
    #(sortby NOTSORTABLE LIMIT 1 1 NOCONTENT)
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'LIMIT', 1, 1, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 4: with pager, with loader
        expected pipeline: root<-buffer-locker<-loader<-unlocker<-sorter<-pager '''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), unlocker(), sorter(paged), pager]
    # (sortby NOTSORTABLE LIMIT 1 1)
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'LIMIT', 1, 1)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    '''case 4.1: with pager, with loader
        expected pipeline: root<-buffer-locker<-loader<-sorter<-pager<-loader<-unlocker '''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), loader(), sorter(paged), pager, loader(paged), unlocker(paged)]
    # (sortby NOTSORTABLE LIMIT 1 1 loadall)
    res = conn.execute_command(*ft_profile_aggregate_cmd, search_UNF_sortable, *sortby_not_sortable_a, 'LIMIT', 1, 1, 'LOAD', '*')
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ############################### Additional rp ###############################

    ''' case 1: highlighter (last to access redis is not endProc)
        expected pipeline: root<-sorter<-buffer-locker<-loader<-unlocker<-highlighter '''
    expected_pipeline = ['Result processors profile', root, sorter(), buffer_locker(), loader(), unlocker(), highlighter]
    res = conn.execute_command(*ft_profile_search_cmd, '*', *sortby_UNF_sortable_s, 'HIGHLIGHT')
    env.assertEqual(get_pipeline(res), expected_pipeline)


    '''case 2: metric
    expected pipeline: root<-metric<-sorter<-buffer-locker<-loader<-unlocker '''
    env.cmd('flushall')

    env.cmd('FT.CONFIG', 'SET', 'DEFAULT_DIALECT', '2')

    env.expect('FT.CREATE idx SCHEMA v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 t TEXT').ok()
    conn.execute_command('hset', '1', 'v', 'bababaca', 't', "hello")
    conn.execute_command('hset', '2', 'v', 'babababa', 't', "hello")
    conn.execute_command('hset', '3', 'v', 'aabbaabb', 't', "hello")

    metric = ['Type', 'Metrics Applier', 'Counter', docs_count]

    res = conn.execute_command(*ft_profile_search_cmd, '*=>[KNN 3 @v $vec]',
                               'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa')

    expected_pipeline = ['Result processors profile', root, metric, sorter(), buffer_locker(), loader(), unlocker()]
    #sortby NOT SORTABLE NOCONTENT
    env.assertEqual(get_pipeline(res), expected_pipeline)
