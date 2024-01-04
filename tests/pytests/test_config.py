from RLTest import Env
from includes import *
from common import skip

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
        check_config('WORKER_THREADS')
        check_config('MT_MODE')
        check_config('TIERED_HNSW_BUFFER_LIMIT')
        check_config('PRIVILEGED_THREADS_NUM')
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

'''

Config options test. TODO : Fix 'Success (not an error)' parsing wrong error.

def testSetConfigOptions(env):

    env.expect('ft.config', 'set', 'MINPREFIX', 'str').equal('Success (not an error)')  ## TODO incorrect code
    env.expect('ft.config', 'set', 'EXTLOAD', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'NOGC', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'MINPREFIX', 1).equal('OK')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 1).equal('OK')
    env.expect('ft.config', 'set', 'MAXDOCTABLESIZE', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 1).equal('OK')
    env.expect('ft.config', 'set', 'TIMEOUT', 1).equal('OK')
    if MT_BUILD:
        env.expect('ft.config', 'set', 'WORKER_THREADS', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'FRISOINI', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'ON_TIMEOUT', 1).equal('Success (not an error)')
    env.expect('ft.config', 'set', 'GCSCANSIZE', 1).equal('OK')
    env.expect('ft.config', 'set', 'MIN_PHONETIC_TERM_LEN', 1).equal('OK')
    env.expect('ft.config', 'set', 'GC_POLICY', 1).equal(not_modifiable)
    env.expect('ft.config', 'set', 'FORK_GC_RUN_INTERVAL', 1).equal('OK')
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 1).equal('OK')
    env.expect('ft.config', 'set', 'FORK_GC_RETRY_INTERVAL', 1).equal('OK')

def testSetConfigOptionsErrors(env):
    env.expect('ft.config', 'set', 'MAXDOCTABLESIZE', 'str').equal(not_modifiable)
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 'str').equal('Success (not an error)')
    env.expect('ft.config', 'set', 'TIMEOUT', 'str').equal('Success (not an error)')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Success (not an error)')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Success (not an error)')
'''

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
        env.assertEqual(res_dict['WORKER_THREADS'][0], '0')
        env.assertEqual(res_dict['MT_MODE'][0], 'MT_MODE_OFF')
        env.assertEqual(res_dict['TIERED_HNSW_BUFFER_LIMIT'][0], '1024')
        env.assertEqual(res_dict['PRIVILEGED_THREADS_NUM'][0], '1')
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

# skip ctest configured tests
    #env.assertEqual(res_dict['GC_POLICY'][0], 'fork')
    #env.assertEqual(res_dict['UNION_ITERATOR_HEAP'][0], '20')

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
        test_arg_num('WORKER_THREADS', 3)
        test_arg_num('TIERED_HNSW_BUFFER_LIMIT', 50000)
        test_arg_num('PRIVILEGED_THREADS_NUM', 4)
    test_arg_num('GCSCANSIZE', 3)
    test_arg_num('MIN_PHONETIC_TERM_LEN', 3)
    test_arg_num('FORK_GC_RUN_INTERVAL', 3)
    test_arg_num('FORK_GC_CLEAN_THRESHOLD', 3)
    test_arg_num('FORK_GC_RETRY_INTERVAL', 3)
    test_arg_num('UNION_ITERATOR_HEAP', 20)
    test_arg_num('_NUMERIC_RANGES_PARENTS', 1)
    test_arg_num('BG_INDEX_SLEEP_GAP', 15)

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
        env.expect('ft.config', 'set', 'MT_MODE').error().contains(not_modifiable)
        env.expect('ft.config', 'set', 'WORKER_THREADS').error().contains(not_modifiable)
        env.expect('ft.config', 'set', 'TIERED_HNSW_BUFFER_LIMIT').error().contains(not_modifiable)
        env.expect('ft.config', 'set', 'PRIVILEGED_THREADS_NUM').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'FRISOINI').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'GC_POLICY').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'NO_MEM_POOLS').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'PARTIAL_INDEXED_DOCS').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'UPGRADE_INDEX').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'RAW_DOCID_ENCODING').error().contains(not_modifiable)
    env.expect('ft.config', 'set', 'BG_INDEX_SLEEP_GAP').error().contains(not_modifiable)

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

@skip(cluster=False)
def testImmutableCoord(env):
    env.expect('_ft.config', 'set', 'SEARCH_THREADS').error().contains(not_modifiable)
