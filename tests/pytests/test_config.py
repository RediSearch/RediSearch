from RLTest import Env
from includes import *
from common import skip, config_cmd

not_modifiable = 'Not modifiable at runtime'

@skip(cluster=True)
def testConfig(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.expect(config_cmd(), 'help', 'idx').equal([])
    env.expect(config_cmd(), 'set', 'MINPREFIX', 1).equal('OK')

@skip(cluster=True)
def testConfigErrors(env):
    env.expect(config_cmd(), 'set', 'MINPREFIX', 1, 2).equal('EXCESSARGS')
    env.expect(config_cmd(), 'no_such_command', 'idx').equal('No such configuration action')
    env.expect(config_cmd(), 'idx').error().contains('wrong number of arguments')
    env.expect(config_cmd(), 'set', '_NUMERIC_RANGES_PARENTS', 3) \
        .equal('Max depth for range cannot be higher than max depth for balance')
    env.expect(config_cmd(), 'set', 'MINSTEMLEN', 1).error()\
        .contains('Minimum stem length cannot be lower than')
    if MT_BUILD:
        env.expect(config_cmd(), 'set', 'WORKERS', 1_000_000).error()\
            .contains('Number of worker threads cannot exceed')

@skip(cluster=True)
def testGetConfigOptions(env):
    def check_config(conf):
        env.expect(config_cmd(), 'get', conf).noError().apply(lambda x: x[0][0]).equal(conf)

    check_config('EXTLOAD')
    check_config('NOGC')
    check_config('MINPREFIX')
    check_config('FORKGC_SLEEP_BEFORE_EXIT')
    check_config('MAXDOCTABLESIZE')
    check_config('MAXEXPANSIONS')
    check_config('MAXPREFIXEXPANSIONS')
    check_config('TIMEOUT')
    if MT_BUILD:
        check_config('WORKERS')
        check_config('MIN_OPERATION_WORKERS')
        check_config('WORKER_THREADS')
        check_config('MT_MODE')
        check_config('TIERED_HNSW_BUFFER_LIMIT')
        check_config('PRIVILEGED_THREADS_NUM')
        check_config('WORKERS_PRIORITY_BIAS_THRESHOLD')
    check_config('FRISOINI')
    check_config('MAXSEARCHRESULTS')
    check_config('MAXAGGREGATERESULTS')
    check_config('ON_TIMEOUT')
    check_config('GCSCANSIZE')
    check_config('MIN_PHONETIC_TERM_LEN')
    check_config('GC_POLICY')
    check_config('FORK_GC_RUN_INTERVAL')
    check_config('FORK_GC_CLEAN_THRESHOLD')
    check_config('FORK_GC_RETRY_INTERVAL')
    check_config('PARTIAL_INDEXED_DOCS')
    check_config('UNION_ITERATOR_HEAP')
    check_config('_NUMERIC_COMPRESS')
    check_config('_NUMERIC_RANGES_PARENTS')
    check_config('RAW_DOCID_ENCODING')
    check_config('FORK_GC_CLEAN_NUMERIC_EMPTY_NODES')
    check_config('_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES')
    check_config('_FREE_RESOURCE_ON_THREAD')
    check_config('BG_INDEX_SLEEP_GAP')
    check_config('_PRIORITIZE_INTERSECT_UNION_CHILDREN')
    check_config('MINSTEMLEN')


@skip(cluster=True)
def testSetConfigOptions(env):

    env.expect(config_cmd(), 'set', 'MINPREFIX', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'EXTLOAD', 1).equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'NOGC', 1).equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'MINPREFIX', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'MAXDOCTABLESIZE', 1).equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'TIMEOUT', 1).equal('OK')
    if MT_BUILD:
        env.expect(config_cmd(), 'set', 'WORKERS', 1).equal('OK')
        env.expect(config_cmd(), 'set', 'MIN_OPERATION_WORKERS', 1).equal('OK')
        env.expect(config_cmd(), 'set', 'WORKER_THREADS', 1).equal('OK') # deprecated
        env.expect(config_cmd(), 'set', 'MT_MODE', 1).equal('OK') # deprecated
    env.expect(config_cmd(), 'set', 'FRISOINI', 1).equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 1).equal('Invalid ON_TIMEOUT value')
    env.expect(config_cmd(), 'set', 'GCSCANSIZE', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'MIN_PHONETIC_TERM_LEN', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'GC_POLICY', 1).equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'FORK_GC_RUN_INTERVAL', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'FORK_GC_RETRY_INTERVAL', 1).equal('OK')

@skip(cluster=True)
def testSetConfigOptionsErrors(env):
    env.expect(config_cmd(), 'set', 'MAXDOCTABLESIZE', 'str').equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'TIMEOUT', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Could not convert argument to expected type')
    if MT_BUILD:
        env.expect(config_cmd(), 'set', 'WORKERS',  2 ** 13 + 1).contains('Number of worker threads cannot exceed')
        env.expect(config_cmd(), 'set', 'MIN_OPERATION_WORKERS', 2 ** 13 + 1).contains('Number of worker threads cannot exceed')


@skip(cluster=True)
def testAllConfig(env):
    ## on existing env the pre tests might change the config
    ## so no point of testing it
    if env.env == 'existing-env':
        env.skip()
    res_list = env.cmd(config_cmd() + ' get *')
    res_dict = {d[0]: d[1:] for d in res_list}
    env.assertEqual(res_dict['EXTLOAD'][0], None)
    env.assertEqual(res_dict['NOGC'][0], 'false')
    env.assertEqual(res_dict['MINPREFIX'][0], '2')
    env.assertEqual(res_dict['FORKGC_SLEEP_BEFORE_EXIT'][0], '0')
    env.assertEqual(res_dict['MAXDOCTABLESIZE'][0], '1000000')
    env.assertEqual(res_dict['MAXSEARCHRESULTS'][0], '1000000')
    env.assertEqual(res_dict['MAXAGGREGATERESULTS'][0], 'unlimited')
    env.assertEqual(res_dict['MAXEXPANSIONS'][0], '200')
    env.assertEqual(res_dict['MAXPREFIXEXPANSIONS'][0], '200')
    env.assertContains(res_dict['TIMEOUT'][0], ['500', '0'])
    if MT_BUILD:
        env.assertEqual(res_dict['WORKERS'][0], '0')
        env.assertEqual(res_dict['MIN_OPERATION_WORKERS'][0], '4')
        env.assertEqual(res_dict['TIERED_HNSW_BUFFER_LIMIT'][0], '1024')
        env.assertEqual(res_dict['PRIVILEGED_THREADS_NUM'][0], '1')
        env.assertEqual(res_dict['WORKERS_PRIORITY_BIAS_THRESHOLD'][0], '1')
    env.assertEqual(res_dict['FRISOINI'][0], None)
    env.assertEqual(res_dict['ON_TIMEOUT'][0], 'return')
    env.assertEqual(res_dict['GCSCANSIZE'][0], '100')
    env.assertEqual(res_dict['MIN_PHONETIC_TERM_LEN'][0], '3')
    env.assertEqual(res_dict['FORK_GC_RUN_INTERVAL'][0], '30')
    env.assertEqual(res_dict['FORK_GC_CLEAN_THRESHOLD'][0], '100')
    env.assertEqual(res_dict['FORK_GC_RETRY_INTERVAL'][0], '5')
    env.assertEqual(res_dict['CURSOR_MAX_IDLE'][0], '300000')
    env.assertEqual(res_dict['NO_MEM_POOLS'][0], 'false')
    env.assertEqual(res_dict['PARTIAL_INDEXED_DOCS'][0], 'false')
    env.assertEqual(res_dict['_NUMERIC_COMPRESS'][0], 'false')
    env.assertEqual(res_dict['_NUMERIC_RANGES_PARENTS'][0], '0')
    env.assertEqual(res_dict['FORK_GC_CLEAN_NUMERIC_EMPTY_NODES'][0], 'true')
    env.assertEqual(res_dict['_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES'][0], 'true')
    env.assertEqual(res_dict['_PRIORITIZE_INTERSECT_UNION_CHILDREN'][0], 'false')
    env.assertEqual(res_dict['_FREE_RESOURCE_ON_THREAD'][0], 'true')
    env.assertEqual(res_dict['BG_INDEX_SLEEP_GAP'][0], '100')
    env.assertEqual(res_dict['GC_POLICY'][0], 'fork')
    env.assertEqual(res_dict['UNION_ITERATOR_HEAP'][0], '20')

@skip(cluster=True)
def testInitConfig():
    # Numeric arguments

    def test_arg_num(arg_name, arg_value):
        env = Env(moduleArgs=f'{arg_name} {arg_value}', noDefaultModuleArgs=True)
        if env.env == 'existing-env':
            env.skip()
        env.expect(config_cmd(), 'get', arg_name).equal([[arg_name, str(arg_value)]])
        env.stop()

    test_arg_num('MAXDOCTABLESIZE', 123456)
    test_arg_num('TIMEOUT', 0)
    test_arg_num('MINPREFIX', 3)
    test_arg_num('FORKGC_SLEEP_BEFORE_EXIT', 5)
    test_arg_num('MAXEXPANSIONS', 5)
    test_arg_num('MAXPREFIXEXPANSIONS', 5)
    if MT_BUILD:
        test_arg_num('WORKERS', 3)
        test_arg_num('MIN_OPERATION_WORKERS', 3)
        test_arg_num('TIERED_HNSW_BUFFER_LIMIT', 50000)
        test_arg_num('PRIVILEGED_THREADS_NUM', 4)
        test_arg_num('WORKERS_PRIORITY_BIAS_THRESHOLD', 4)
    test_arg_num('GCSCANSIZE', 3)
    test_arg_num('MIN_PHONETIC_TERM_LEN', 3)
    test_arg_num('FORK_GC_RUN_INTERVAL', 3)
    test_arg_num('FORK_GC_CLEAN_THRESHOLD', 3)
    test_arg_num('FORK_GC_RETRY_INTERVAL', 3)
    test_arg_num('UNION_ITERATOR_HEAP', 20)
    test_arg_num('_NUMERIC_RANGES_PARENTS', 1)
    test_arg_num('BG_INDEX_SLEEP_GAP', 15)
    test_arg_num('MINSTEMLEN', 3)

# True/False arguments
    def test_arg_true_false(arg_name, res):
        env = Env(moduleArgs=arg_name, noDefaultModuleArgs=True)
        if env.env == 'existing-env':
            env.skip()
        env.expect(config_cmd(), 'get', arg_name).equal([[arg_name, res]])
        env.stop()

    test_arg_true_false('NOGC', 'true')
    test_arg_true_false('NO_MEM_POOLS', 'true')
    test_arg_true_false('FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'true')

    # String arguments
    def test_arg_str(arg_name, arg_value, ret_value=None):
        if ret_value == None:
            ret_value = arg_value
        env = Env(moduleArgs=arg_name + ' ' + arg_value, noDefaultModuleArgs=True)
        if env.env == 'existing-env':
            env.skip()
        env.expect(config_cmd(), 'get', arg_name).equal([[arg_name, ret_value]])
        env.stop()

    test_arg_str('GC_POLICY', 'fork')
    test_arg_str('GC_POLICY', 'default', 'fork')
    test_arg_str('ON_TIMEOUT', 'fail')
    test_arg_str('TIMEOUT', '0', '0')
    test_arg_str('PARTIAL_INDEXED_DOCS', '0', 'false')
    test_arg_str('PARTIAL_INDEXED_DOCS', '1', 'true')
    test_arg_str('MAXSEARCHRESULTS', '100', '100')
    test_arg_str('MAXSEARCHRESULTS', '-1', 'unlimited')
    test_arg_str('MAXAGGREGATERESULTS', '100', '100')
    test_arg_str('MAXAGGREGATERESULTS', '-1', 'unlimited')
    test_arg_str('RAW_DOCID_ENCODING', 'false', 'false')
    test_arg_str('RAW_DOCID_ENCODING', 'true', 'true')
    test_arg_str('_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'false', 'false')
    test_arg_str('_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'true', 'true')
    test_arg_str('_FREE_RESOURCE_ON_THREAD', 'false', 'false')
    test_arg_str('_FREE_RESOURCE_ON_THREAD', 'true', 'true')
    test_arg_str('_PRIORITIZE_INTERSECT_UNION_CHILDREN', 'true', 'true')
    test_arg_str('_PRIORITIZE_INTERSECT_UNION_CHILDREN', 'false', 'false')

@skip(cluster=True)
def testImmutable(env):

    env.expect(config_cmd(), 'set', 'EXTLOAD').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'NOGC').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'MAXDOCTABLESIZE').error().contains(not_modifiable)
    if MT_BUILD:
        env.expect(config_cmd(), 'set', 'TIERED_HNSW_BUFFER_LIMIT').error().contains(not_modifiable)
        env.expect(config_cmd(), 'set', 'PRIVILEGED_THREADS_NUM').error().contains(not_modifiable) # deprecated
        env.expect(config_cmd(), 'set', 'WORKERS_PRIORITY_BIAS_THRESHOLD').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'FRISOINI').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'GC_POLICY').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'NO_MEM_POOLS').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'PARTIAL_INDEXED_DOCS').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'UPGRADE_INDEX').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'RAW_DOCID_ENCODING').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'BG_INDEX_SLEEP_GAP').error().contains(not_modifiable)

###############################################################################
# TODO: rewrite following tests properly for all coordinator's config options #
###############################################################################

@skip(cluster=False)
def testConfigCoord(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.expect(config_cmd(), 'help', 'idx').equal([])

@skip(cluster=False)
def testConfigErrorsCoord(env):
    env.expect(config_cmd(), 'set', 'SEARCH_THREADS', 'banana').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'SEARCH_THREADS', '-1').error().contains(not_modifiable)

@skip(cluster=False)
def testGetConfigOptionsCoord(env):
    def check_config(conf):
        env.expect(config_cmd(), 'get', conf).noError().apply(lambda x: x[0][0]).equal(conf)

    check_config('SEARCH_THREADS')

@skip(cluster=CLUSTER) # Change to `skip(cluster=False)`
def testAllConfigCoord(env):
    pass

@skip(cluster=False)
def testInitConfigCoord():
    def test_arg_num(arg_name, arg_value):
        env = Env(moduleArgs=f'{arg_name} {arg_value}', noDefaultModuleArgs=True)
        env.expect(config_cmd(), 'get', arg_name).equal([[arg_name, str(arg_value)]])
        env.stop()

    test_arg_num('SEARCH_THREADS', 3)
    test_arg_num('CONN_PER_SHARD', 3)

@skip(cluster=False)
def testImmutableCoord(env):
    env.expect(config_cmd(), 'set', 'SEARCH_THREADS').error().contains(not_modifiable)
