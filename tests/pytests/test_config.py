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

@skip(cluster=True)
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
            env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
            # Client hangs.
            env.assertTrue(False)
    except Exception as e:
        env.assertEqual(str(e), 'Timeout: operation timeout exceeded')
