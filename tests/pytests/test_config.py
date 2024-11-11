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


@skip(cluster=True)
def testSetConfigOptionsErrors(env):
    env.expect(config_cmd(), 'set', 'MAXDOCTABLESIZE', 'str').equal(not_modifiable)
    env.expect(config_cmd(), 'set', 'MAXEXPANSIONS', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'TIMEOUT', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Could not convert argument to expected type')
    env.expect(config_cmd(), 'set', 'WORKERS',  2 ** 13 + 1).contains('Number of worker threads cannot exceed')
    env.expect(config_cmd(), 'set', 'MIN_OPERATION_WORKERS', 2 ** 13 + 1).contains('Number of worker threads cannot exceed')
    env.expect(config_cmd(), 'set', 'INDEX_CURSOR_LIMIT', -1).contains('Value is outside acceptable bounds')


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
def test_command_name(env: Env):
    if config_cmd() == '_FT.CONFIG':
        # if the binaries are not standalone only, the command name is _FT.CONFIG
        env.expect('_FT.CONFIG', 'GET', 'TIMEOUT').noError()
    # Expect the `FT.CONFIG` command to be available anyway
    env.expect('FT.CONFIG', 'GET', 'TIMEOUT').noError()

@skip(cluster=True)
def testImmutable(env):

    env.expect(config_cmd(), 'set', 'EXTLOAD').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'NOGC').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'MAXDOCTABLESIZE').error().contains(not_modifiable)
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


############################ TEST DEPRECATED MT CONFIGS ############################

workers_default = 0
min_operation_workers_default = 4

@skip(cluster=True)
def testDeprecatedMTConfig_full():
    workers = '3'
    env = Env(moduleArgs=f'WORKER_THREADS {workers} MT_MODE MT_MODE_FULL')
    # Check old config values
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', workers]])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_FULL']])
    # Check new config values
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', workers]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])

@skip(cluster=True)
def testDeprecatedMTConfig_operations():
    workers = '3'
    env = Env(moduleArgs=f'WORKER_THREADS {workers} MT_MODE MT_MODE_ONLY_ON_OPERATIONS')
    # Check old config values
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', workers]])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_ONLY_ON_OPERATIONS']])
    # Check new config values
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', workers]])

@skip(cluster=True)
def testDeprecatedMTConfig_off():
    env = Env(moduleArgs='WORKER_THREADS 0 MT_MODE MT_MODE_OFF')
    # Check old config values
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '0']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_OFF']])
    # Check new config values. Both are 0 due to explicit configuration
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '0']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '0']])

# Check invalid combination
@skip(cluster=True)
def testDeprecatedMTConfig_full_with_0():
    env = Env(moduleArgs='MT_MODE MT_MODE_FULL WORKER_THREADS 0')
    env.assertTrue(env.isUp())
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])
@skip(cluster=True)
def testDeprecatedMTConfig_operations_with_0():
    env = Env(moduleArgs='MT_MODE MT_MODE_ONLY_ON_OPERATIONS WORKER_THREADS 0')
    env.assertTrue(env.isUp())
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])
@skip(cluster=True)
def testDeprecatedMTConfig_off_with_non_0():
    env = Env(moduleArgs='MT_MODE MT_MODE_OFF WORKER_THREADS 3')
    env.assertTrue(env.isUp())
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', str(workers_default)]])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', str(min_operation_workers_default)]])

@skip(cluster=True)
def testDeprecatedMTConfig_ignore_full():
    # Check deprecated configs are ignored when new configs are set
    env = Env(moduleArgs='WORKER_THREADS 3 MT_MODE MT_MODE_FULL WORKERS 5 MIN_OPERATION_WORKERS 6')
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '5']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '6']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_FULL']])
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '5']]) # follows WORKERS

@skip(cluster=True)
def testDeprecatedMTConfig_ignore_operations():
    # Check deprecated configs are ignored when new configs are set
    env = Env(moduleArgs='WORKER_THREADS 3 MT_MODE MT_MODE_ONLY_ON_OPERATIONS WORKERS 5 MIN_OPERATION_WORKERS 6')
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '5']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '6']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_ONLY_ON_OPERATIONS']])
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '6']]) # follows MIN_OPERATION_WORKERS

@skip(cluster=True)
def testDeprecatedMTConfig_address_combination_full():
    # Check allowed combination of deprecated and new configs
    env = Env(moduleArgs='WORKER_THREADS 3 MT_MODE MT_MODE_FULL MIN_OPERATION_WORKERS 6')
    env.expect(config_cmd(), 'get', 'WORKERS').equal([['WORKERS', '3']])
    env.expect(config_cmd(), 'get', 'MIN_OPERATION_WORKERS').equal([['MIN_OPERATION_WORKERS', '6']])
    env.expect(config_cmd(), 'get', 'MT_MODE').equal([['MT_MODE', 'MT_MODE_FULL']])
    env.expect(config_cmd(), 'get', 'WORKER_THREADS').equal([['WORKER_THREADS', '3']]) # follows WORKERS

@skip(cluster=True)
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

################################################################################
# Test CONFIG SET/GET numeric parameters
################################################################################
numericConfigs = [
    # configName, ftConfigName, defaultValue, minValue, maxValue, immutable
    ('search._numeric-ranges-parents', '_NUMERIC_RANGES_PARENTS', 0, 0, 2, False),
    ('search.bg-index-sleep-gap', 'BG_INDEX_SLEEP_GAP', 100, 1, 999999999, True),
    ('search.cursor-max-idle', 'CURSOR_MAX_IDLE', 300000, 1, 999999999, False),
    ('search.default-dialect', 'DEFAULT_DIALECT', 1, 1, 4, False),
    ('search.fork-gc-clean-threshold', 'FORK_GC_CLEAN_THRESHOLD', 100, 1, 999999999, False),
    ('search.fork-gc-retry-interval', 'FORK_GC_RETRY_INTERVAL', 5, 1, 999999999, False),
    ('search.fork-gc-run-interval', 'FORK_GC_RUN_INTERVAL', 30, 1, 999999999, False),
    ('search.fork-gc-sleep-before-exit', 'FORKGC_SLEEP_BEFORE_EXIT', 0, 0, 999999999, False),
    ('search.gc-scan-size', 'GCSCANSIZE', 100, 1, 999999999, False),
    ('search.index-cursor-limit', 'INDEX_CURSOR_LIMIT', 128, 0, 999999999, False),
    ('search.max-aggregate-results', 'MAXAGGREGATERESULTS', 'unlimited', 1, 999999999, False),
    ('search.max-doctablesize', 'MAXDOCTABLESIZE', 1_000_000, 1, 100_000_000, True),
    ('search.max-prefix-expansions', 'MAXPREFIXEXPANSIONS', 200, 1, 999999999, False),
    ('search.min-operation-workers', 'MIN_OPERATION_WORKERS', 4, 1, 16, False),
    ('search.min-phonetic-term-len', 'MIN_PHONETIC_TERM_LEN', 3, 1, 999999999, False),
    ('search.min-prefix', 'MINPREFIX', 2, 1, 999999999, False),
    ('search.min-stem-len', 'MINSTEMLEN', 4, 2, 999999999, False),
    ('search.multi-text-slop', 'MULTI_TEXT_SLOP', 100, 1, 999999999, True),
    ('search.tiered-hnsw-buffer-limit', 'TIERED_HNSW_BUFFER_LIMIT', 1024, 0, 999999999, True),
    ('search.timeout', 'TIMEOUT', 500, 1, 999999999, False),
    ('search.union-iterator-heap', 'UNION_ITERATOR_HEAP', 20, 1, 999999999, False),
    ('search.vss-max-resize', 'VSS_MAX_RESIZE', 0, 0, 999999999, False),
    ('search.workers', 'WORKERS', 0, 0, 8192, False),
    ('search.workers-priority-bias-threshold', 'WORKERS_PRIORITY_BIAS_THRESHOLD', 1, 0, 999999999, True),
]

def testConfigAPIRunTimeNumericParams():
    env = Env(noDefaultModuleArgs=True)

    def _testNumericConfig(env, configName, ftConfigName, default, min, max):
        # Check default value
        env.expect('CONFIG', 'GET', configName).equal([configName, str(default)])

        # write using CONFIG SET, read using CONFIG GET/FT.CONFIG GET
        env.expect('CONFIG', 'SET', configName, max).equal('OK')
        env.expect('CONFIG', 'GET', configName).equal([configName, str(max)])
        env.expect(config_cmd(), 'GET', ftConfigName)\
            .equal([[ftConfigName, str(max)]])

        # Write using FT.CONFIG SET, read using CONFIG GET/FT.CONFIG GET
        env.expect(config_cmd(), 'SET', ftConfigName, min).ok()
        env.expect('CONFIG', 'GET', configName).equal([configName, str(min)])
        env.expect(config_cmd(), 'GET', ftConfigName)\
            .equal([[ftConfigName, str(min)]])

        # test invalid values
        env.expect('CONFIG', 'SET', configName, 'invalid_numeric').error()\
            .contains('CONFIG SET failed')
        env.expect('CONFIG', 'SET', configName, str(min - 1)).error()\
            .contains('CONFIG SET failed')
        env.expect('CONFIG', 'SET', configName, str(max + 1)).error()\
            .contains('CONFIG SET failed')

        # test valid range limits
        env.expect('CONFIG', 'SET', configName, str(min)).equal('OK')
        env.expect('CONFIG', 'GET', configName).equal([configName, str(min)])
        env.expect('CONFIG', 'SET', configName, str(max)).equal('OK')
        env.expect('CONFIG', 'GET', configName).equal([configName, str(max)])

    def _testImmutableNumericConfig(env, configName, ftConfigName, default):
        # Check default value
        env.expect('CONFIG', 'GET', configName).equal([configName, str(default)])
        env.expect(config_cmd(), 'GET', ftConfigName).\
            equal([[ftConfigName, str(default)]])

        # Check that the value is immutable
        env.expect('CONFIG', 'SET', configName, str(default)).error()\
            .contains('CONFIG SET failed')

    # Test numeric parameters
    for configName, ftConfigName, default, min, max, immutable in numericConfigs:
        # TODO: These tests are not working as expected. Need to fix them.
        if configName in ['search.max-aggregate-results', 'search.workers']:
            continue

        if immutable:
            _testImmutableNumericConfig(env, configName, ftConfigName, default)
        else:
            _testNumericConfig(env, configName, ftConfigName, default, min, max)

    if env.isCluster():
        _testImmutableNumericConfig(env, 'search.search-threads',
                                    'SEARCH_THREADS', 20)
        _testNumericConfig(env, 'search.topology-validation-timeout',
                           'TOPOLOGY_VALIDATION_TIMEOUT', 30000, 0, 999999999)

@skip(cluster=True)
def testModuleLoadexNumericParams():
    env = Env(noDefaultModuleArgs=True)

    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()
    os.unlink(rdbFilePath)
    
    # Remove modules and args
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    redisearch_module_path = env.envRunner.modulePath[0]
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')

    for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
        # TODO: These tests are not working as expected. Need to fix them.
        if configName in ['search.max-aggregate-results', 'search.workers']:
            continue

        if (minValue != default):
            configValue = str(minValue)
            argValue = str(minValue + 1)
        else:
            configValue = str(minValue + 1)
            argValue = str(minValue + 2)
        
        env.assertNotEqual(configValue, str(default))

        # Load module using module arguments
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                      'ARGS', argName, argValue
        )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, argValue]])
        env.expect('CONFIG', 'GET', configName).equal([configName, argValue])
        env.stop()
        os.unlink(rdbFilePath)

        # Load module using CONFIG
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                      'CONFIG', configName, configValue
        )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, configValue]])
        env.expect('CONFIG', 'GET', configName).equal([configName, configValue])
        env.stop()
        os.unlink(rdbFilePath)

        # Load module using CONFIG and module arguments
        # the CONFIG values should take precedence
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                      'CONFIG', configName, configValue,
                      'ARGS', argName, argValue
        )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, configValue]])
        env.expect('CONFIG', 'GET', configName).equal([configName, configValue])
        env.stop()
        os.unlink(rdbFilePath)

################################################################################
# Test CONFIG SET/GET enum parameters
################################################################################
def testConfigAPIRunTimeEnumParams():
    env = Env(noDefaultModuleArgs=True)

    # Test default value
    env.expect('CONFIG', 'GET', 'search.on-timeout')\
        .equal(['search.on-timeout', 'return'])

    # Test search.on-timeout - valid values
    env.expect('CONFIG', 'SET', 'search.on-timeout', 'fail').equal('OK')
    env.expect('CONFIG', 'GET', 'search.on-timeout')\
        .equal(['search.on-timeout', 'fail'])

    env.expect('CONFIG', 'SET', 'search.on-timeout', 'return').equal('OK')
    env.expect('CONFIG', 'GET', 'search.on-timeout')\
        .equal(['search.on-timeout', 'return'])

    # Test search.on-timeout - invalid values
    env.expect('CONFIG', 'SET', 'search.on-timeout', 'invalid_value').error()\
            .contains('CONFIG SET failed')

@skip(cluster=True)
def testModuleLoadexEnumParams():
    env = Env(noDefaultModuleArgs=True)

    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()
    os.unlink(rdbFilePath)
    
    # Remove modules and args
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    redisearch_module_path = env.envRunner.modulePath[0]
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')

    configName = 'search.on-timeout'
    argName = 'ON_TIMEOUT'
    testValue = 'fail'
    defaultValue = 'return'

    # Test setting the parameter using CONFIG
    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, [])
    res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                'CONFIG', configName, testValue
    )
    env.expect(config_cmd(), 'GET', argName).equal([[argName, testValue]])
    env.expect('CONFIG', 'GET', configName).equal([configName, testValue])
    env.stop()
    os.unlink(rdbFilePath)

    # Test setting the parameter using ARGS
    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, [])
    res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                'ARGS', argName, testValue
    )
    env.expect(config_cmd(), 'GET', argName).equal([[argName, testValue]])
    env.expect('CONFIG', 'GET', configName).equal([configName, testValue])
    env.stop()
    os.unlink(rdbFilePath)

    # Load module using CONFIG and module arguments, the CONFIG values should
    # take precedence
    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, [])
    res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                'CONFIG', configName, testValue,
                'ARGS', argName, defaultValue
    )
    env.expect(config_cmd(), 'GET', argName).equal([[argName, testValue]])
    env.expect('CONFIG', 'GET', configName).equal([configName, testValue])
    env.stop()
    os.unlink(rdbFilePath)

################################################################################
# Test CONFIG SET/GET string parameters
################################################################################
stringConfigs = [
    # configName, ftConfigName, ftDefault, testValue
    ('search.ext-load', 'EXTLOAD', None,
     'example_extension/libexample_extension.so'),
    ('search.friso-ini', 'FRISOINI', None, 'deps/cndict/friso.ini'),
]

def testConfigAPIRunTimeStringParams():
    env = Env(noDefaultModuleArgs=True)

    def _testImmutableStringConfig(env, configName, ftConfigName, ftDefault,
                                   testValue):
        # Check default value
        if ftDefault == None:
            default = ''
        env.expect('CONFIG', 'GET', configName).\
            equal([configName, default])
        env.expect(config_cmd(), 'GET', ftConfigName).\
            equal([[ftConfigName, ftDefault]])

        # Check that the value is immutable
        env.expect('CONFIG', 'SET', configName, testValue).error()\
            .contains('CONFIG SET failed')

    # String parameters
    for configName, ftConfigName, ftDefault, testValue in stringConfigs:
        _testImmutableStringConfig(env, configName, ftConfigName, ftDefault,
                                   testValue)

@skip(cluster=True)
def testModuleLoadexStringParams():
    env = Env(noDefaultModuleArgs=True)

    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()
    os.unlink(rdbFilePath)
    
    # Remove modules and args
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    redisearch_module_path = env.envRunner.modulePath[0]
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')

    for configName, argName, ftDefault, testValue in stringConfigs:
        if configName == 'search.ext-load':
            modpath = env.module[0]
            testValue = os.path.abspath(os.path.join(os.path.dirname(modpath), testValue))

        # Test setting the parameter using CONFIG
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                    'CONFIG', configName, testValue
        )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, testValue]])
        env.expect('CONFIG', 'GET', configName).equal([configName, testValue])
        env.stop()
        os.unlink(rdbFilePath)

        # Test setting the parameter using ARGS
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                    'ARGS', argName, testValue
        )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, testValue]])
        env.expect('CONFIG', 'GET', configName).equal([configName, testValue])
        env.stop()
        os.unlink(rdbFilePath)

        # Load module using CONFIG and module arguments, the CONFIG values should
        # take precedence
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                    'CONFIG', configName, testValue,
                    'ARGS', argName, 'invalid_value'
        )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, testValue]])
        env.expect('CONFIG', 'GET', configName).equal([configName, testValue])
        env.stop()
        os.unlink(rdbFilePath)

################################################################################
# Test CONFIG SET/GET boolean parameters
################################################################################
booleanConfigs = [
    # configName, ftConfigName, defaultValue, immutable, isFlag
    ('search._free-resource-on-thread', '_FREE_RESOURCE_ON_THREAD', 'yes', False, False),
    ('search._numeric-compress', '_NUMERIC_COMPRESS', 'no', False, False),
    ('search._print-profile-clock', '_PRINT_PROFILE_CLOCK', 'yes', False, False),
    ('search.no-gc', 'NOGC', 'no', True, True),
    ('search.no-mem-pools', 'NO_MEM_POOLS', 'no', True, True),
    ('search.partial-indexed-docs', 'PARTIAL_INDEXED_DOCS', 'no', True, False),
    ('search._prioritize-intersect-union-children', '_PRIORITIZE_INTERSECT_UNION_CHILDREN', 'no', False, False),
    ('search.raw-docid-encoding', 'RAW_DOCID_ENCODING', 'no', True, False),
    # # TODO: Confirm if we need to test search._fork-gc-clean-numeric-empty-nodes, 
    # # because it will be deprecated in  8.0
    # ('search._fork-gc-clean-numeric-empty-nodes', '_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'yes', False)
]

def testConfigAPIRunTimeBooleanParams():
    env = Env(noDefaultModuleArgs=True)

    def _testBooleanConfig(env, configName, ftConfigName, default):
        # Check default value
        env.expect('CONFIG', 'GET', configName).equal([configName, default])

        for val in ['yes', 'no']:
            old_val = 'true' if val == 'yes' else 'false'

            # write using CONFIG SET, read using CONFIG GET/FT.CONFIG GET
            env.expect('CONFIG', 'SET', configName, val).equal('OK')
            env.expect('CONFIG', 'GET', configName).equal([configName, val])
            env.expect(config_cmd(), 'GET', ftConfigName)\
                .equal([[ftConfigName, old_val]])

            # Write using FT.CONFIG SET, read using CONFIG GET/FT.CONFIG GET
            env.expect(config_cmd(), 'SET', ftConfigName, old_val).ok()
            env.expect('CONFIG', 'GET', configName).equal([configName, val])
            env.expect(config_cmd(), 'GET', ftConfigName)\
                .equal([[ftConfigName, old_val]])

        # Test invalid values
        env.expect('CONFIG', 'SET', configName, 'invalid_boolean').error()\
            .contains('CONFIG SET failed')

    def _testImmutableBooleanConfig(env, configName, ftConfigName, default):
        # Check default value
        env.expect('CONFIG', 'GET', configName).equal([configName, str(default)])

        old_val = 'true' if default == 'yes' else 'false'
        env.expect(config_cmd(), 'GET', ftConfigName).\
            equal([[ftConfigName, str(old_val)]])

        # Check that the value is immutable
        env.expect('CONFIG', 'SET', configName, str(default)).error()\
            .contains('CONFIG SET failed')

    # Test boolean parameters
    for configName, ftConfigName, defaultValue, immutable, isFlag in booleanConfigs:
        if immutable:
            _testImmutableBooleanConfig(env, configName, ftConfigName, defaultValue)
        else:
            _testBooleanConfig(env, configName, ftConfigName, defaultValue)


@skip(cluster=True)
def testModuleLoadexBooleanParams():
    env = Env(noDefaultModuleArgs=True)

    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()
    os.unlink(rdbFilePath)
    
    # Remove modules and args
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    redisearch_module_path = env.envRunner.modulePath[0]
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.modulePath.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')

    for configName, argName, defaultValue, immutable, isFlag in booleanConfigs:
        # Load module using CONFIG
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        # use non-default value as config value
        configValue = 'yes' if defaultValue == 'no' else 'yes'
        expected = 'true' if configValue == 'yes' else 'false'
        res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                    'CONFIG', configName, configValue
        )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, expected]])
        env.expect('CONFIG', 'GET', configName).equal([configName, configValue])
        env.stop()
        os.unlink(rdbFilePath)

        # `search.partial-indexed-docs` is tested later because 
        # `PARTIAL_INDEXED_DOCS` is set using a number but returns a boolean
        if configName == 'search.partial-indexed-docs':
            continue

        # Load module using module arguments
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        # use non-default value as argument value
        argValue = 'true' if defaultValue == 'no' else 'false'
        expected = 'yes' if argValue == 'true' else 'no'

        if not isFlag:
            res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                        'ARGS', argName, argValue
            )
        else:
            res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                        'ARGS', argName
            )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, argValue]])
        env.expect('CONFIG', 'GET', configName).equal([configName, expected])
        env.stop()
        os.unlink(rdbFilePath)

        # Load module using CONFIG and module arguments
        # the CONFIG values should take precedence
        env.start()
        res = env.cmd('MODULE', 'LIST')
        env.assertEqual(res, [])
        # use non-default value as config value
        configValue = 'yes' if defaultValue == 'no' else 'yes'
        # use default value as argument value
        argValue = 'true' if defaultValue == 'yes' else 'false'
        # expected value should be equivalent to the config value
        expected = 'true' if configValue == 'yes' else 'false'
        if not isFlag:
            res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                        'CONFIG', configName, configValue,
                        'ARGS', argName, argValue
            )
        else:
            res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                        'CONFIG', configName, configValue,
                        'ARGS', argName
            )
        env.expect(config_cmd(), 'GET', argName).equal([[argName, expected]])
        env.expect('CONFIG', 'GET', configName).equal([configName, configValue])
        env.stop()
        os.unlink(rdbFilePath)

    # test `search.partial-indexed-docs`
    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, [])
    # use non-default value as argument value
    res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                'ARGS', 'PARTIAL_INDEXED_DOCS', '1'
    )
    env.expect(config_cmd(), 'GET', 'PARTIAL_INDEXED_DOCS')\
        .equal([['PARTIAL_INDEXED_DOCS', 'true']])
    env.expect('CONFIG', 'GET', 'search.partial-indexed-docs')\
        .equal(['search.partial-indexed-docs', 'yes'])
    env.stop()
    os.unlink(rdbFilePath)

    # Load module using CONFIG and module arguments, the CONFIG values should
    # take precedence
    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, [])
    res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                'CONFIG', 'search.partial-indexed-docs', 'yes',
                'ARGS', 'PARTIAL_INDEXED_DOCS', '0'
    )
    env.expect(config_cmd(), 'GET', 'PARTIAL_INDEXED_DOCS')\
        .equal([['PARTIAL_INDEXED_DOCS', 'true']])
    env.expect('CONFIG', 'GET', 'search.partial-indexed-docs')\
        .equal(['search.partial-indexed-docs', 'yes'])
    env.stop()
    os.unlink(rdbFilePath)
