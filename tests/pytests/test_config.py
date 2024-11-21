from RLTest import Env
from includes import *
from common import *

not_modifiable = 'Not modifiable at runtime'

def _test_config_str(arg_name, arg_value, ret_value=None):
    if ret_value == None:
        ret_value = arg_value
    env = Env(moduleArgs=arg_name + ' ' + arg_value, noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect(config_cmd(), 'get', arg_name).equal([[arg_name, ret_value]])
    env.stop()

def _test_config_num(arg_name, arg_value):
    env = Env(moduleArgs=f'{arg_name} {arg_value}', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect(config_cmd(), 'get', arg_name).equal([[arg_name, str(arg_value)]])
    env.stop()

def _test_config_true_false(arg_name, res):
    env = Env(moduleArgs=arg_name, noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect(config_cmd(), 'get', arg_name).equal([[arg_name, res]])
    env.stop()

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
    check_config('OSS_GLOBAL_PASSWORD')
    check_config('OSS_ACL_USERNAME')
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

def testInitConfig():

    # Numeric arguments
    _test_config_num('MAXDOCTABLESIZE', 123456)
    _test_config_num('TIMEOUT', 0)
    _test_config_num('MINPREFIX', 3)
    _test_config_num('FORKGC_SLEEP_BEFORE_EXIT', 5)
    _test_config_num('MAXEXPANSIONS', 5)
    _test_config_num('MAXPREFIXEXPANSIONS', 5)
    _test_config_num('WORKERS', 3)
    _test_config_num('MIN_OPERATION_WORKERS', 3)
    _test_config_num('TIERED_HNSW_BUFFER_LIMIT', 50000)
    _test_config_num('PRIVILEGED_THREADS_NUM', 4)
    _test_config_num('WORKERS_PRIORITY_BIAS_THRESHOLD', 4)
    _test_config_num('GCSCANSIZE', 3)
    _test_config_num('MIN_PHONETIC_TERM_LEN', 3)
    _test_config_num('FORK_GC_RUN_INTERVAL', 3)
    _test_config_num('FORK_GC_CLEAN_THRESHOLD', 3)
    _test_config_num('FORK_GC_RETRY_INTERVAL', 3)
    _test_config_num('UNION_ITERATOR_HEAP', 20)
    _test_config_num('_NUMERIC_RANGES_PARENTS', 1)
    _test_config_num('BG_INDEX_SLEEP_GAP', 15)
    _test_config_num('MINSTEMLEN', 3)
    _test_config_num('INDEX_CURSOR_LIMIT', 128)

# True/False arguments
    _test_config_true_false('NOGC', 'true')
    _test_config_true_false('NO_MEM_POOLS', 'true')
    _test_config_true_false('FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'true')

    _test_config_str('GC_POLICY', 'fork')
    _test_config_str('GC_POLICY', 'default', 'fork')
    _test_config_str('ON_TIMEOUT', 'fail')
    _test_config_str('TIMEOUT', '0', '0')
    _test_config_str('PARTIAL_INDEXED_DOCS', '0', 'false')
    _test_config_str('PARTIAL_INDEXED_DOCS', '1', 'true')
    _test_config_str('MAXSEARCHRESULTS', '100', '100')
    _test_config_str('MAXSEARCHRESULTS', '-1', 'unlimited')
    _test_config_str('MAXAGGREGATERESULTS', '100', '100')
    _test_config_str('MAXAGGREGATERESULTS', '-1', 'unlimited')
    _test_config_str('RAW_DOCID_ENCODING', 'false', 'false')
    _test_config_str('RAW_DOCID_ENCODING', 'true', 'true')
    _test_config_str('_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'false', 'false')
    _test_config_str('_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'true', 'true')
    _test_config_str('_FREE_RESOURCE_ON_THREAD', 'false', 'false')
    _test_config_str('_FREE_RESOURCE_ON_THREAD', 'true', 'true')
    _test_config_str('_PRIORITIZE_INTERSECT_UNION_CHILDREN', 'true', 'true')
    _test_config_str('_PRIORITIZE_INTERSECT_UNION_CHILDREN', 'false', 'false')

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

    _test_config_num('SEARCH_THREADS', 3)
    _test_config_num('CONN_PER_SHARD', 3)

    def _testOSSGlobalPasswordConfig():
        env = Env(moduleArgs='OSS_GLOBAL_PASSWORD 123456', noDefaultModuleArgs=True)
        if env.env == 'existing-env':
            env.skip()
        env.expect(config_cmd(), 'get', 'OSS_GLOBAL_PASSWORD').equal([['OSS_GLOBAL_PASSWORD', 'Password: *******']])
        env.stop()

    # We test `OSS_GLOBAL_PASSWORD` manually since the getter obfuscates the value
    _testOSSGlobalPasswordConfig()

    _test_config_str('OSS_ACL_USERNAME', 'default')

@skip(cluster=False)
def testImmutableCoord(env):
    env.expect(config_cmd(), 'set', 'SEARCH_THREADS').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'OSS_GLOBAL_PASSWORD').error().contains(not_modifiable)
    env.expect(config_cmd(), 'set', 'OSS_ACL_USERNAME').error().contains(not_modifiable)

@skip(cluster=False)
def testSetACLUsername():
    """Tests that the OSS_ACL_USERNAME configuration is set correctly on module
    load
    we also test that the client hangs when trying to authenticate with a
    non-existing user. This is a BUG that should be fixed - see MOD-8071.
    """

    # Setting the `OSS_ACL_USERNAME` configuration without the `OSS_GLOBAL_PASSWORD`
    # the configuration should not do anything since we don't try to authenticate.
    _test_config_str('OSS_ACL_USERNAME', 'test')

    # Set both the username and password. This should fail since we have no such
    # user.
    env = Env(moduleArgs='OSS_ACL_USERNAME test_user OSS_GLOBAL_PASSWORD 123456', noDefaultModuleArgs=True)

    timeout = 3 # 3 seconds, more than enough for the an env to be up normally
    try:
        with TimeLimit(timeout):
            env.cmd('FT.SEARCH', 'idx', '*')
            # Client hangs.
            env.assertTrue(False)
    except Exception as e:
        env.assertEqual(str(e), 'Timeout: operation timeout exceeded')

################################################################################
# Test CONFIG SET/GET numeric parameters
################################################################################
LLONG_MAX = (1 << 63) - 1
UINT64_MAX = (1 << 64) - 1
UINT32_MAX = (1 << 32) - 1

numericConfigs = [
    # configName, ftConfigName, defaultValue, minValue, maxValue, immutable
    ('search-_numeric-ranges-parents', '_NUMERIC_RANGES_PARENTS', 0, 0, 2, False),
    ('search-bg-index-sleep-gap', 'BG_INDEX_SLEEP_GAP', 100, 1, UINT32_MAX, True),
    ('search-cursor-max-idle', 'CURSOR_MAX_IDLE', 300000, 1, LLONG_MAX, False),
    ('search-default-dialect', 'DEFAULT_DIALECT', 1, 1, 4, False),
    ('search-fork-gc-clean-threshold', 'FORK_GC_CLEAN_THRESHOLD', 100, 1, LLONG_MAX, False),
    ('search-fork-gc-retry-interval', 'FORK_GC_RETRY_INTERVAL', 5, 1, LLONG_MAX, False),
    ('search-fork-gc-run-interval', 'FORK_GC_RUN_INTERVAL', 30, 1, LLONG_MAX, False),
    ('search-fork-gc-sleep-before-exit', 'FORKGC_SLEEP_BEFORE_EXIT', 0, 0, LLONG_MAX, False),
    ('search-gc-scan-size', 'GCSCANSIZE', 100, 1, LLONG_MAX, False),
    ('search-index-cursor-limit', 'INDEX_CURSOR_LIMIT', 128, 0, LLONG_MAX, False),
    ('search-max-aggregate-results', 'MAXAGGREGATERESULTS', -1, 0, LLONG_MAX, False),
    ('search-max-doctablesize', 'MAXDOCTABLESIZE', 1_000_000, 1, 100_000_000, True),
    ('search-max-prefix-expansions', 'MAXPREFIXEXPANSIONS', 200, 1, LLONG_MAX, False),
    ('search-max-search-results', 'MAXSEARCHRESULTS', 1_000_000, 0, LLONG_MAX, False),
    ('search-min-operation-workers', 'MIN_OPERATION_WORKERS', 4, 1, 16, False),
    ('search-min-phonetic-term-len', 'MIN_PHONETIC_TERM_LEN', 3, 1, LLONG_MAX, False),
    ('search-min-prefix', 'MINPREFIX', 2, 1, LLONG_MAX, False),
    ('search-min-stem-len', 'MINSTEMLEN', 4, 2, UINT32_MAX, False),
    ('search-multi-text-slop', 'MULTI_TEXT_SLOP', 100, 1, UINT32_MAX, True),
    ('search-tiered-hnsw-buffer-limit', 'TIERED_HNSW_BUFFER_LIMIT', 1024, 0, LLONG_MAX, True),
    ('search-timeout', 'TIMEOUT', 500, 1, LLONG_MAX, False),
    ('search-union-iterator-heap', 'UNION_ITERATOR_HEAP', 20, 1, LLONG_MAX, False),
    ('search-vss-max-resize', 'VSS_MAX_RESIZE', 0, 0, UINT32_MAX, False),
    ('search-workers', 'WORKERS', 0, 0, 16, False),
    ('search-workers-priority-bias-threshold', 'WORKERS_PRIORITY_BIAS_THRESHOLD', 1, 0, LLONG_MAX, True),
    # Cluster parameters
    ('search-threads', 'SEARCH_THREADS', 20, 1, LLONG_MAX, True),
    ('search-topology-validation-timeout', 'TOPOLOGY_VALIDATION_TIMEOUT', 30_000, 0, LLONG_MAX, False),
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
        if configName in ['search-threads',
                          'search-topology-validation-timeout']:
            if not env.isCluster():
                continue

        if immutable:
            _testImmutableNumericConfig(env, configName, ftConfigName, default)
        else:
            _testNumericConfig(env, configName, ftConfigName, default, min, max)

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
        if configName in ['search-threads',
                          'search-topology-validation-timeout']:
            if not env.isCluster():
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

        # For immutable parameters, we need to test that the limits are enforced
        # using MODULE LOADEX
        if immutable:
            env.start()
            res = env.cmd('MODULE', 'LIST')
            env.assertEqual(res, [])
            env.expect('MODULE', 'LOADEX', redisearch_module_path,
                       'CONFIG', configName, str(minValue - 1)).error()\
                        .contains('Error loading the extension')
            env.assertTrue(env.isUp())
            env.stop()
            os.unlink(rdbFilePath)

            env.start()
            res = env.cmd('MODULE', 'LIST')
            env.assertEqual(res, [])
            env.expect('MODULE', 'LOADEX', redisearch_module_path,
                       'CONFIG', configName, str(maxValue + 1)).error()\
                        .contains('Error loading the extension')
            env.assertTrue(env.isUp())
            env.stop()
            os.unlink(rdbFilePath)

@skip(cluster=True)
def testConfigFileNumericParams():
    # Test using only redis config file
    redisConfigFile = '/tmp/testConfigFileNumericParams.conf'

    # create redis.conf file in /tmp
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
            # Skip cluster parameters
            if configName in ['search-threads',
                              'search-topology-validation-timeout']:
                continue

            f.write(f'{configName} {minValue}\n')

    # Start the server using the conf file and check each value
    env = Env(noDefaultModuleArgs=True, redisConfigFile=redisConfigFile)
    for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
        # Skip cluster parameters
        if configName in ['search-threads',
                          'search-topology-validation-timeout']:
            if not env.isCluster():
                continue

        res = env.cmd('CONFIG', 'GET', configName)
        env.assertEqual(res, [configName, str(minValue)])
        res = env.cmd(config_cmd(), 'GET', argName)
        env.assertEqual(res, [[argName, str(minValue)]])

@skip(cluster=False)
def testClusterConfigFileNumericParams():
    # Test using only redis config file
    redisConfigFile = '/tmp/testClusterConfigFileNumericParams.conf'

    # create redis.conf file in /tmp
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
            f.write(f'{configName} {minValue}\n')

    # Start the server using the conf file and check each value
    env = Env(noDefaultModuleArgs=True, redisConfigFile=redisConfigFile)
    for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
        res = env.cmd('CONFIG', 'GET', configName)
        env.assertEqual(res, [configName, str(minValue)])
        res = env.cmd(config_cmd(), 'GET', argName)
        env.assertEqual(res, [[argName, str(minValue)]])

@skip(cluster=True)
def testConfigFileAndArgsNumericParams():
    # Test using redis config file and module arguments
    redisConfigFile = '/tmp/testConfigFileAndArgsNumericParams.conf'
    # create redis.conf file in /tmp and add all the boolean parameters
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
            # Skip cluster parameters
            if configName in ['search-threads',
                              'search-topology-validation-timeout']:
                continue
            f.write(f'{configName} {minValue}\n')

    moduleArgs = ''
    for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
        moduleArgs += f'{argName} {minValue} '

    env = Env(noDefaultModuleArgs=True, moduleArgs=moduleArgs, redisConfigFile=redisConfigFile)
    for configName, argName, default, minValue, maxValue, immutable in numericConfigs:
        # Skip cluster parameters
        if configName in ['search-threads',
                          'search-topology-validation-timeout']:
            continue

        res = env.cmd('CONFIG', 'GET', configName)
        env.assertEqual(res, [configName, str(minValue)])
        res = env.cmd(config_cmd(), 'GET', argName)
        env.assertEqual(res, [[argName, str(minValue)]])

################################################################################
# Test CONFIG SET/GET enum parameters
################################################################################
def testConfigAPIRunTimeEnumParams():
    env = Env(noDefaultModuleArgs=True)

    # Test default value
    env.expect('CONFIG', 'GET', 'search-on-timeout')\
        .equal(['search-on-timeout', 'return'])

    # Test search-on-timeout - valid values
    env.expect('CONFIG', 'SET', 'search-on-timeout', 'fail').equal('OK')
    env.expect('CONFIG', 'GET', 'search-on-timeout')\
        .equal(['search-on-timeout', 'fail'])

    env.expect('CONFIG', 'SET', 'search-on-timeout', 'return').equal('OK')
    env.expect('CONFIG', 'GET', 'search-on-timeout')\
        .equal(['search-on-timeout', 'return'])

    # Test search-on-timeout - invalid values
    env.expect('CONFIG', 'SET', 'search-on-timeout', 'invalid_value').error()\
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

    # Test search-on-timeout
    configName = 'search-on-timeout'
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

def testConfigFileEnumParams():
    # Test using only redis config file
    redisConfigFile = '/tmp/testConfigFileEnumParams.conf'

    # Test search-on-timeout
    configName = 'search-on-timeout'
    argName = 'ON_TIMEOUT'
    testValue = 'fail'

    # create redis.conf file in /tmp
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        f.write(f'{configName} {testValue}\n')

    # Start the server using the conf file and check each value
    env = Env(noDefaultModuleArgs=True, redisConfigFile=redisConfigFile)
    res = env.cmd('CONFIG', 'GET', configName)
    env.assertEqual(res, [configName, testValue])
    res = env.cmd(config_cmd(), 'GET', argName)
    env.assertEqual(res, [[argName, testValue]])

def testConfigFileAndArgsEnumParams():
    # Test using redis config file and module arguments
    redisConfigFile = '/tmp/testConfigFileAndArgsEnumParams.conf'

    # Test search-on-timeout
    configName = 'search-on-timeout'
    argName = 'ON_TIMEOUT'
    testValue = 'fail'
    moduleArgs = 'ON_TIMEOUT return'

    # create redis.conf file in /tmp
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        f.write(f'{configName} {testValue}\n')

    # Start the server using the conf file and check each value
    env = Env(noDefaultModuleArgs=True, moduleArgs=moduleArgs, redisConfigFile=redisConfigFile)
    res = env.cmd('CONFIG', 'GET', configName)
    env.assertEqual(res, [configName, testValue])
    res = env.cmd(config_cmd(), 'GET', argName)
    env.assertEqual(res, [[argName, testValue]])

################################################################################
# Test CONFIG SET/GET string parameters
################################################################################
stringConfigs = [
    # configName, ftConfigName, ftDefault, testValue
    ('search-ext-load', 'EXTLOAD', None,
     'example_extension/libexample_extension.so'),
    ('search-friso-ini', 'FRISOINI', None, 'deps/cndict/friso.ini'),
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
        if configName == 'search-ext-load':
            basedir = os.path.dirname(redisearch_module_path)
            testValue = os.path.abspath(os.path.join(basedir, testValue))

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

def testConfigFileStringParams():
    # Test using only redis config file
    redisConfigFile = '/tmp/testConfigFileStringParams.conf'
    with open(redisConfigFile, 'w') as f:
        pass  # Do nothing, just create the file
    env = Env(noDefaultModuleArgs=True, redisConfigFile=redisConfigFile)

    # stop the server and remove the rdb file
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()
    os.unlink(rdbFilePath)

    # get module path
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    redisearch_module_path = env.envRunner.modulePath[0]
    # env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')

    # create redis.conf file in /tmp and add all the boolean parameters
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        for configName, argName, ftDefault, testValue in stringConfigs:
            if configName == 'search-ext-load':
                basedir = os.path.dirname(redisearch_module_path)
                testValue = os.path.abspath(os.path.join(basedir, testValue))
            f.write(f'{configName} {testValue}\n')

    # Restart the server using the conf file and check each value
    env.start()
    for configName, argName, ftDefault, testValue in stringConfigs:
        if configName == 'search-ext-load':
                basedir = os.path.dirname(redisearch_module_path)
                testValue = os.path.abspath(os.path.join(basedir, testValue))
        res = env.cmd('CONFIG', 'GET', configName)
        env.assertEqual(res, [configName, testValue])
        res = env.cmd(config_cmd(), 'GET', argName)
        env.assertEqual(res, [[argName, testValue]])

@skip(cluster=True)
def testConfigFileAndArgsStringParams():
    # Test using redis config file and module arguments
    redisConfigFile = '/tmp/testConfigFileAndArgsStringParams.conf'
    with open(redisConfigFile, 'w') as f:
        pass  # Do nothing, just create the file
    env = Env(redisConfigFile=redisConfigFile)

    # stop the server and remove the rdb file
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()
    os.unlink(rdbFilePath)

    # get module path
    env.assertEqual(len(env.envRunner.modulePath), 2)
    env.assertEqual(len(env.envRunner.moduleArgs), 2)
    redisearch_module_path = env.envRunner.modulePath[0]

    # create redis configuration file
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        for configName, argName, ftDefault, testValue in stringConfigs:
            if configName == 'search-ext-load':
                basedir = os.path.dirname(redisearch_module_path)
                testValue = os.path.abspath(os.path.join(basedir, testValue))
            f.write(f'{configName} {testValue}\n')

    # create module arguments
    env.envRunner.moduleArgs.pop()
    env.envRunner.moduleArgs.pop()
    env.envRunner.moduleArgs.append(['EXTLOAD unusedValue FRISOINI unusedValue'])
    env.envRunner.moduleArgs.append([])
    env.envRunner.masterCmdArgs = env.envRunner.createCmdArgs('master')

    # Restart the server using the conf file and check each value
    env.start()
    for configName, argName, ftDefault, testValue in stringConfigs:
        if configName == 'search-ext-load':
                basedir = os.path.dirname(redisearch_module_path)
                testValue = os.path.abspath(os.path.join(basedir, testValue))
        res = env.cmd('CONFIG', 'GET', configName)
        env.assertEqual(res, [configName, testValue])
        res = env.cmd(config_cmd(), 'GET', argName)
        env.assertEqual(res, [[argName, testValue]])

################################################################################
# Test CONFIG SET/GET boolean parameters
################################################################################
booleanConfigs = [
    # configName, ftConfigName, defaultValue, immutable, isFlag
    ('search-_free-resource-on-thread', '_FREE_RESOURCE_ON_THREAD', 'yes', False, False),
    ('search-_numeric-compress', '_NUMERIC_COMPRESS', 'no', False, False),
    ('search-_print-profile-clock', '_PRINT_PROFILE_CLOCK', 'yes', False, False),
    ('search-no-gc', 'NOGC', 'no', True, True),
    ('search-no-mem-pools', 'NO_MEM_POOLS', 'no', True, True),
    ('search-partial-indexed-docs', 'PARTIAL_INDEXED_DOCS', 'no', True, False),
    ('search-_prioritize-intersect-union-children', '_PRIORITIZE_INTERSECT_UNION_CHILDREN', 'no', False, False),
    ('search-raw-docid-encoding', 'RAW_DOCID_ENCODING', 'no', True, False),
    # # TODO: Confirm if we need to test search-_fork-gc-clean-numeric-empty-nodes,
    # # because it will be deprecated in  8.0
    # ('search-_fork-gc-clean-numeric-empty-nodes', '_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES', 'yes', False)
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

        # `search-partial-indexed-docs` is tested later because
        # `PARTIAL_INDEXED_DOCS` is set using a number but returns a boolean
        if configName == 'search-partial-indexed-docs':
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

    # test `search-partial-indexed-docs`
    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, [])
    # use non-default value as argument value
    res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                'ARGS', 'PARTIAL_INDEXED_DOCS', '1'
    )
    env.expect(config_cmd(), 'GET', 'PARTIAL_INDEXED_DOCS')\
        .equal([['PARTIAL_INDEXED_DOCS', 'true']])
    env.expect('CONFIG', 'GET', 'search-partial-indexed-docs')\
        .equal(['search-partial-indexed-docs', 'yes'])
    env.stop()
    os.unlink(rdbFilePath)

    # Load module using CONFIG and module arguments, the CONFIG values should
    # take precedence
    env.start()
    res = env.cmd('MODULE', 'LIST')
    env.assertEqual(res, [])
    res = env.cmd('MODULE', 'LOADEX', redisearch_module_path,
                'CONFIG', 'search-partial-indexed-docs', 'yes',
                'ARGS', 'PARTIAL_INDEXED_DOCS', '0'
    )
    env.expect(config_cmd(), 'GET', 'PARTIAL_INDEXED_DOCS')\
        .equal([['PARTIAL_INDEXED_DOCS', 'true']])
    env.expect('CONFIG', 'GET', 'search-partial-indexed-docs')\
        .equal(['search-partial-indexed-docs', 'yes'])
    env.stop()
    os.unlink(rdbFilePath)

def testConfigFileBooleanParams():
    # Test using only redis config file
    redisConfigFile = '/tmp/testConfigFileBooleanParams.conf'

    # create redis.conf file in /tmp and add all the boolean parameters
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        for configName, argName, defaultValue, immutable, isFlag in booleanConfigs:
            # use non-default value as config value
            configValue = 'yes' if defaultValue == 'no' else 'no'
            f.write(f'{configName} {configValue}\n')

    # Start the server using the conf file and check each value
    env = Env(noDefaultModuleArgs=True, redisConfigFile=redisConfigFile)
    for configName, argName, defaultValue, immutable, isFlag in booleanConfigs:
        # the expected value is the opposite of the default value
        configValue = 'yes' if defaultValue == 'no' else 'no'
        ftExpectedValue = 'true' if defaultValue == 'no' else 'false'
        res = env.cmd('CONFIG', 'GET', configName)
        env.assertEqual(res, [configName, configValue])
        res = env.cmd(config_cmd(), 'GET', argName)
        env.assertEqual(res, [[argName, ftExpectedValue]])

def testConfigFileAndArgsBooleanParams():
    # Test using redis config file and module arguments
    redisConfigFile = '/tmp/testConfigFileAndArgsBooleanParams.conf'
    # create redis.conf file in /tmp and add all the boolean parameters
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        for configName, argName, defaultValue, immutable, isFlag in booleanConfigs:
            # use non-default value as config value
            configValue = 'yes' if defaultValue == 'no' else 'no'
            f.write(f'{configName} {configValue}\n')

    moduleArgs = ''
    for configName, argName, defaultValue, immutable, isFlag in booleanConfigs:
        ftDefaultValue = 'true' if defaultValue == 'yes' else 'false'
        if isFlag:
            moduleArgs += f'{argName} '
        else:
            moduleArgs += f'{argName} {ftDefaultValue} '

    env = Env(noDefaultModuleArgs=True, moduleArgs=moduleArgs, redisConfigFile=redisConfigFile)
    for configName, argName, defaultValue, immutable, isFlag in booleanConfigs:
        # the expected value is the opposite of the default value
        configValue = 'yes' if defaultValue == 'no' else 'no'
        ftExpectedValue = 'true' if defaultValue == 'no' else 'false'
        res = env.cmd('CONFIG', 'GET', configName)
        env.assertEqual(res, [configName, configValue])
        res = env.cmd(config_cmd(), 'GET', argName)
        env.assertEqual(res, [[argName, ftExpectedValue]])
