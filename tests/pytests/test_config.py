from RLTest import Env
from includes import *
from common import skip, config_cmd
import math

not_modifiable = 'Not modifiable at runtime'

@skip(cluster=True)
def testConfig(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.expect('ft.config', 'help', 'idx').equal([])
    env.expect('ft.config', 'set', 'MINPREFIX', 1).equal('OK')

@skip(cluster=True)
def testConfigErrors(env):
    env.expect('ft.config', 'set', 'MINPREFIX', 1, 2).equal('EXCESSARGS')
    env.expect('ft.config', 'no_such_command', 'idx').equal('No such configuration action')
    env.expect('ft.config', 'idx').error().contains('wrong number of arguments')
    env.expect('ft.config', 'set', '_NUMERIC_RANGES_PARENTS', 3) \
        .equal('Max depth for range cannot be higher than max depth for balance')
    env.expect('ft.config', 'set', 'MINSTEMLEN', 1).error()\
        .contains('Minimum stem length cannot be lower than')
    if MT_BUILD:
        env.expect('ft.config', 'set', 'WORKERS', 1_000_000).error()\
            .contains('Number of worker threads cannot exceed')

@skip(cluster=True)
def testGetConfigOptions(env):
    def check_config(conf):
        env.expect('ft.config', 'get', conf).noError().apply(lambda x: x[0][0]).equal(conf)

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
    check_config('INDEX_CURSOR_LIMIT')
    check_config('ENABLE_UNSTABLE_FEATURES')
    check_config('BM25STD_TANH_FACTOR')
    check_config('INDEXER_YIELD_EVERY_OPS')

@skip(cluster=True)
def testSetConfigOptions(env):

    env.expect('ft.config', 'set', 'MINPREFIX', 'str').equal('Could not convert argument to expected type')
    env.expect('ft.config', 'set', 'EXTLOAD', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'NOGC', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'MINPREFIX', 1).equal('OK')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 1).equal('OK')
    env.expect('ft.config', 'set', 'MAXDOCTABLESIZE', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 1).equal('OK')
    env.expect('ft.config', 'set', 'TIMEOUT', 1).equal('OK')
    if MT_BUILD:
        env.expect(config_cmd(), 'set', 'WORKERS', 1).equal('OK')
        env.expect(config_cmd(), 'set', 'MIN_OPERATION_WORKERS', 1).equal('OK')
        env.expect(config_cmd(), 'set', 'WORKER_THREADS', 1).equal(not_modifiable) # deprecated
        env.expect(config_cmd(), 'set', 'MT_MODE', 1).equal(not_modifiable) # deprecated
    env.expect(config_cmd(), 'set', 'FRISOINI', 1).equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 1).equal('Invalid ON_TIMEOUT value')
    env.expect(config_cmd(), 'set', 'GCSCANSIZE', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'MIN_PHONETIC_TERM_LEN', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'GC_POLICY', 1).equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'FORK_GC_RUN_INTERVAL', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'FORK_GC_RETRY_INTERVAL', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'INDEX_CURSOR_LIMIT', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'ENABLE_UNSTABLE_FEATURES', 'true').equal('OK')
    env.expect(config_cmd(), 'set', 'BM25STD_TANH_FACTOR', 1).equal('OK')
    env.expect(config_cmd(), 'set', 'INDEXER_YIELD_EVERY_OPS', 1).equal('OK')

@skip(cluster=True)
def testSetConfigOptionsErrors(env):
    env.expect('ft.config', 'set', 'MAXDOCTABLESIZE', 'str').equal(not_modifiable)
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 'str').equal('Could not convert argument to expected type')
    env.expect('ft.config', 'set', 'TIMEOUT', 'str').equal('Could not convert argument to expected type')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Could not convert argument to expected type')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Could not convert argument to expected type')
    if MT_BUILD:
        env.expect('ft.config', 'set', 'WORKERS',  2 ** 13 + 1).contains('Number of worker threads cannot exceed')
        env.expect('ft.config', 'set', 'MIN_OPERATION_WORKERS', 2 ** 13 + 1).contains('Number of worker threads cannot exceed')
    env.expect(config_cmd(), 'set', 'INDEX_CURSOR_LIMIT', -1).contains('Value is outside acceptable bounds')
    env.expect(config_cmd(), 'set', 'BM25STD_TANH_FACTOR', -1).contains('Value is outside acceptable bounds')
    env.expect(config_cmd(), 'set', 'BM25STD_TANH_FACTOR', 10001).contains('BM25STD_TANH_FACTOR must be between 1 and 10000')

@skip(cluster=True)
def testAllConfig(env):
    ## on existing env the pre tests might change the config
    ## so no point of testing it
    if env.env == 'existing-env':
        env.skip()
    res_list = env.cmd('ft.config get *')
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
    env.assertEqual(res_dict['INDEX_CURSOR_LIMIT'][0], '128')
    env.assertEqual(res_dict['ENABLE_UNSTABLE_FEATURES'][0], 'false')
    env.assertEqual(res_dict['BM25STD_TANH_FACTOR'][0], '4')
    env.assertEqual(res_dict['INDEXER_YIELD_EVERY_OPS'][0], '1000')

@skip(cluster=True)
def testInitConfig():
    # Numeric arguments

    def test_arg_num(arg_name, arg_value):
        env = Env(moduleArgs=f'{arg_name} {arg_value}', noDefaultModuleArgs=True)
        if env.env == 'existing-env':
            env.skip()
        env.expect('ft.config', 'get', arg_name).equal([[arg_name, str(arg_value)]])
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
    test_arg_num('INDEX_CURSOR_LIMIT', 128)
    test_arg_num('BM25STD_TANH_FACTOR', 8)

# True/False arguments
    def test_arg_true_false(arg_name, res):
        env = Env(moduleArgs=arg_name, noDefaultModuleArgs=True)
        if env.env == 'existing-env':
            env.skip()
        env.expect('ft.config', 'get', arg_name).equal([[arg_name, res]])
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
        env.expect('ft.config', 'get', arg_name).equal([[arg_name, ret_value]])
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

    env.expect('ft.config', 'set', 'EXTLOAD').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'NOGC').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'MAXDOCTABLESIZE').error().contains(not_modifiable)
    if MT_BUILD:
        env.expect('ft.config', 'set', 'TIERED_HNSW_BUFFER_LIMIT').error().contains(not_modifiable)
        env.expect('ft.config', 'set', 'PRIVILEGED_THREADS_NUM').error().contains(not_modifiable) # deprecated
        env.expect('ft.config', 'set', 'WORKERS_PRIORITY_BIAS_THRESHOLD').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'FRISOINI').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'GC_POLICY').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'NO_MEM_POOLS').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'PARTIAL_INDEXED_DOCS').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'UPGRADE_INDEX').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'RAW_DOCID_ENCODING').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'BG_INDEX_SLEEP_GAP').error().contains(not_modifiable)


############################ TEST DEPRECATED MT CONFIGS ############################

workers_default = 0
min_operation_workers_default = 4

@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_full():
    workers = '3'
    env = Env(moduleArgs=f'WORKER_THREADS {workers} MT_MODE MT_MODE_FULL')
    # Check old config values
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', workers]])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_FULL']])
    # Check new config values
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', workers]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])

@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_operations():
    workers = '3'
    env = Env(moduleArgs=f'WORKER_THREADS {workers} MT_MODE MT_MODE_ONLY_ON_OPERATIONS')
    # Check old config values
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', workers]])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_ONLY_ON_OPERATIONS']])
    # Check new config values
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', workers]])

@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_off():
    env = Env(moduleArgs='WORKER_THREADS 0 MT_MODE MT_MODE_OFF')
    # Check old config values
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '0']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_OFF']])
    # Check new config values. Both are 0 due to explicit configuration
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '0']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '0']])

# Check invalid combination
@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_full_with_0():
    env = Env(moduleArgs='MT_MODE MT_MODE_FULL WORKER_THREADS 0')
    env.assertTrue(env.isUp())
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])
@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_operations_with_0():
    env = Env(moduleArgs='MT_MODE MT_MODE_ONLY_ON_OPERATIONS WORKER_THREADS 0')
    env.assertTrue(env.isUp())
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])
@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_off_with_non_0():
    env = Env(moduleArgs='MT_MODE MT_MODE_OFF WORKER_THREADS 3')
    env.assertTrue(env.isUp())
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])

@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_ignore_full():
    # Check deprecated configs are ignored when new configs are set
    env = Env(moduleArgs='WORKER_THREADS 3 MT_MODE MT_MODE_FULL WORKERS 5 MIN_OPERATION_WORKERS 6')
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '5']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '6']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_FULL']])
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '5']]) # follows WORKERS

@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_ignore_operations():
    # Check deprecated configs are ignored when new configs are set
    env = Env(moduleArgs='WORKER_THREADS 3 MT_MODE MT_MODE_ONLY_ON_OPERATIONS WORKERS 5 MIN_OPERATION_WORKERS 6')
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '5']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '6']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_ONLY_ON_OPERATIONS']])
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '6']]) # follows MIN_OPERATION_WORKERS

@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_address_combination_full():
    # Check allowed combination of deprecated and new configs
    env = Env(moduleArgs='WORKER_THREADS 3 MT_MODE MT_MODE_FULL MIN_OPERATION_WORKERS 6')
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '3']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '6']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_FULL']])
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '3']]) # follows WORKERS

@skip(cluster=True, noWorkers=True)
def testDeprecatedMTConfig_address_combination_operations():
    # Check allowed combination of deprecated and new configs
    env = Env(moduleArgs='WORKER_THREADS 3 MT_MODE MT_MODE_ONLY_ON_OPERATIONS WORKERS 5')
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '5']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '3']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_ONLY_ON_OPERATIONS']])
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '3']]) # follows MIN_OPERATION_WORKERS

########################## TEST DEPRECATED MT CONFIGS END ##########################

###############################################################################
# TODO: rewrite following tests properly for all coordinator's config options #
###############################################################################

@skip(cluster=False)
def testConfigCoord(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.expect('_ft.config', 'help', 'idx').equal([])

@skip(cluster=False)
def testConfigErrorsCoord(env):
    env.expect('_ft.config', 'set', 'SEARCH_THREADS', 'banana').error().contains(not_modifiable)
    env.expect('_ft.config', 'set', 'SEARCH_THREADS', '-1').error().contains(not_modifiable)

@skip(cluster=False)
def testGetConfigOptionsCoord(env):
    def check_config(conf):
        env.expect('_ft.config', 'get', conf).noError().apply(lambda x: x[0][0]).equal(conf)

    check_config('SEARCH_THREADS')

@skip(cluster=COORD) # Change to `skip(cluster=False)`
def testAllConfigCoord(env):
    pass

@skip(cluster=False)
def testInitConfigCoord():
    def test_arg_num(arg_name, arg_value):
        env = Env(moduleArgs=f'{arg_name} {arg_value}', noDefaultModuleArgs=True)
        env.expect('_ft.config', 'get', arg_name).equal([[arg_name, str(arg_value)]])
        env.stop()

    test_arg_num('SEARCH_THREADS', 3)
    test_arg_num('CONN_PER_SHARD', 3)

@skip(cluster=False)
def testImmutableCoord(env):
    env.expect('_ft.config', 'set', 'SEARCH_THREADS').error().contains(not_modifiable)

@skip(cluster=True)
def testUnstableFeaturesOffByDefault():
    """Tests that the unstable features are off by default."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    reset_unstable_config = lambda: env.cmd(config_cmd(), 'SET', 'ENABLE_UNSTABLE_FEATURES', 'false')

    # Prepare the index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()

    # Add a document
    env.cmd('HSET', 'doc1', 'title', 'hello world')

    # -------------------- BM25STD.TANH scorer --------------------
    env.expect('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.TANH') \
        .error().contains('Scorer BM25STD.TANH is unavailable when `ENABLE_UNSTABLE_FEATURES` is off')

    env.cmd(config_cmd(), 'SET', 'ENABLE_UNSTABLE_FEATURES', 'true')

    res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.TANH')

    # Check the score (only 1 doc..)
    env.assertEqual(round(float(res[2]), 5), 0.14286)

    # -------------------- BM25STD_TANH_FACTOR --------------------
    reset_unstable_config()

    factor = 2
    env.expect('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'BM25STD_TANH_FACTOR', str(factor)) \
        .error().contains('BM25STD_TANH_FACTOR is not available when `ENABLE_UNSTABLE_FEATURES` is off')

    env.cmd(config_cmd(), 'SET', 'ENABLE_UNSTABLE_FEATURES', 'true')

    unnormalized_res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD')
    normalized_res = env.cmd('FT.SEARCH', 'idx', 'hello world', 'WITHSCORES', 'NOCONTENT', 'SCORER', 'BM25STD.TANH', 'BM25STD_TANH_FACTOR', str(factor))

    # Check the score (only 1 doc..)
    env.assertEqual(round(math.tanh(float(unnormalized_res[2]) / factor), 5), round(float(normalized_res[2]), 5))
