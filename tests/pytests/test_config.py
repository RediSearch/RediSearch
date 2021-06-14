from RLTest import Env

def testConfig(env):
    env.skipOnCluster()
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.expect('ft.config', 'help', 'idx').equal([])
    env.expect('ft.config', 'set', 'MINPREFIX', 1).equal('OK')

def testConfigErrors(env):
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MINPREFIX', 1, 2).equal('EXCESSARGS')
    env.expect('ft.config', 'no_such_command', 'idx').equal('No such configuration action')
    env.expect('ft.config', 'idx').error().contains("wrong number of arguments for 'ft.config' command")
    env.expect('ft.config', 'set', '_NUMERIC_RANGES_PARENTS', 3) \
        .equal('Max depth for range cannot be higher than max depth for balance')

def testGetConfigOptions(env):
    env.skipOnCluster()
    assert env.expect('ft.config', 'get', 'EXTLOAD').res[0][0] == 'EXTLOAD'
    assert env.expect('ft.config', 'get', 'SAFEMODE').res[0][0] == 'SAFEMODE'
    assert env.expect('ft.config', 'get', 'NOGC').res[0][0] == 'NOGC'
    assert env.expect('ft.config', 'get', 'MINPREFIX').res[0][0] == 'MINPREFIX'
    assert env.expect('ft.config', 'get', 'FORKGC_SLEEP_BEFORE_EXIT').res[0][0] =='FORKGC_SLEEP_BEFORE_EXIT'
    assert env.expect('ft.config', 'get', 'MAXDOCTABLESIZE').res[0][0] =='MAXDOCTABLESIZE'
    assert env.expect('ft.config', 'get', 'MAXEXPANSIONS').res[0][0] =='MAXEXPANSIONS'
    assert env.expect('ft.config', 'get', 'MAXPREFIXEXPANSIONS').res[0][0] =='MAXPREFIXEXPANSIONS'
    assert env.expect('ft.config', 'get', 'TIMEOUT').res[0][0] =='TIMEOUT'
    assert env.expect('ft.config', 'get', 'INDEX_THREADS').res[0][0] =='INDEX_THREADS'
    assert env.expect('ft.config', 'get', 'SEARCH_THREADS').res[0][0] =='SEARCH_THREADS'
    assert env.expect('ft.config', 'get', 'FRISOINI').res[0][0] =='FRISOINI'
    assert env.expect('ft.config', 'get', 'MAXSEARCHRESULTS').res[0][0] =='MAXSEARCHRESULTS'
    assert env.expect('ft.config', 'get', 'MAXAGGREGATERESULTS').res[0][0] =='MAXAGGREGATERESULTS'
    assert env.expect('ft.config', 'get', 'ON_TIMEOUT').res[0][0] == 'ON_TIMEOUT'
    assert env.expect('ft.config', 'get', 'GCSCANSIZE').res[0][0] =='GCSCANSIZE'
    assert env.expect('ft.config', 'get', 'MIN_PHONETIC_TERM_LEN').res[0][0] =='MIN_PHONETIC_TERM_LEN'
    assert env.expect('ft.config', 'get', 'GC_POLICY').res[0][0] =='GC_POLICY'
    assert env.expect('ft.config', 'get', 'FORK_GC_RUN_INTERVAL').res[0][0] =='FORK_GC_RUN_INTERVAL'
    assert env.expect('ft.config', 'get', 'FORK_GC_CLEAN_THRESHOLD').res[0][0] =='FORK_GC_CLEAN_THRESHOLD'
    assert env.expect('ft.config', 'get', 'FORK_GC_RETRY_INTERVAL').res[0][0] =='FORK_GC_RETRY_INTERVAL'
    assert env.expect('ft.config', 'get', '_MAX_RESULTS_TO_UNSORTED_MODE').res[0][0] =='_MAX_RESULTS_TO_UNSORTED_MODE'
    assert env.expect('ft.config', 'get', 'PARTIAL_INDEXED_DOCS').res[0][0] =='PARTIAL_INDEXED_DOCS'
    assert env.expect('ft.config', 'get', 'UNION_ITERATOR_HEAP').res[0][0] =='UNION_ITERATOR_HEAP'
    assert env.expect('ft.config', 'get', '_NUMERIC_COMPRESS').res[0][0] =='_NUMERIC_COMPRESS'
    assert env.expect('ft.config', 'get', '_NUMERIC_RANGES_PARENTS').res[0][0] =='_NUMERIC_RANGES_PARENTS'
'''

Config options test. TODO : Fix 'Success (not an error)' parsing wrong error.

def testSetConfigOptions(env):

    env.expect('ft.config', 'set', 'MINPREFIX', 'str').equal('Success (not an error)')  ## TODO incorrect code
    env.expect('ft.config', 'set', 'EXTLOAD', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'SAFEMODE', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'NOGC', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'MINPREFIX', 1).equal('OK')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 1).equal('OK')
    env.expect('ft.config', 'set', 'MAXDOCTABLESIZE', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 1).equal('OK')
    env.expect('ft.config', 'set', 'TIMEOUT', 1).equal('OK')
    env.expect('ft.config', 'set', 'INDEX_THREADS', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'SEARCH_THREADS', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'FRISOINI', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'ON_TIMEOUT', 1).equal('Success (not an error)')
    env.expect('ft.config', 'set', 'GCSCANSIZE', 1).equal('OK')
    env.expect('ft.config', 'set', 'MIN_PHONETIC_TERM_LEN', 1).equal('OK')
    env.expect('ft.config', 'set', 'GC_POLICY', 1).equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'FORK_GC_RUN_INTERVAL', 1).equal('OK')
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 1).equal('OK')
    env.expect('ft.config', 'set', 'FORK_GC_RETRY_INTERVAL', 1).equal('OK')
    env.expect('ft.config', 'set', '_MAX_RESULTS_TO_UNSORTED_MODE', 1).equal('OK')

def testSetConfigOptionsErrors(env):
    env.expect('ft.config', 'set', 'MAXDOCTABLESIZE', 'str').equal('Not modifiable at runtime')
    env.expect('ft.config', 'set', 'MAXEXPANSIONS', 'str').equal('Success (not an error)')
    env.expect('ft.config', 'set', 'TIMEOUT', 'str').equal('Success (not an error)')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Success (not an error)')
    env.expect('ft.config', 'set', 'FORKGC_SLEEP_BEFORE_EXIT', 'str').equal('Success (not an error)')
'''

def testAllConfig(env):
    env.skipOnCluster()
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
    env.assertIn(res_dict['TIMEOUT'][0], ['500', '0'])
    env.assertEqual(res_dict['INDEX_THREADS'][0], '8')
    env.assertEqual(res_dict['SEARCH_THREADS'][0], '20')
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

    # skip ctest configured tests
    #env.assertEqual(res_dict['GC_POLICY'][0], 'fork')
    #env.assertEqual(res_dict['_MAX_RESULTS_TO_UNSORTED_MODE'][0], '1000')
    #env.assertEqual(res_dict['SAFEMODE'][0], 'true')
    #env.assertEqual(res_dict['UNION_ITERATOR_HEAP'][0], '20')

def testInitConfig(env):
    # Numeric arguments
    env.skipOnCluster()

    def test_arg_num(arg_name, arg_value):
        env = Env(moduleArgs=arg_name + ' ' + '%d' % arg_value)
        if env.env == 'existing-env':
            env.skip()
        assert env.expect('ft.config', 'get', arg_name).equal([[arg_name, '%d' % arg_value]])
        env.stop()

    test_arg_num('MAXDOCTABLESIZE', 123456)
    test_arg_num('TIMEOUT', 0)
    test_arg_num('MINPREFIX', 3)
    test_arg_num('FORKGC_SLEEP_BEFORE_EXIT', 5)
    test_arg_num('MAXEXPANSIONS', 5)
    test_arg_num('MAXPREFIXEXPANSIONS', 5)
    test_arg_num('INDEX_THREADS', 3)
    test_arg_num('SEARCH_THREADS', 3)
    test_arg_num('GCSCANSIZE', 3)
    test_arg_num('MIN_PHONETIC_TERM_LEN', 3)
    test_arg_num('FORK_GC_RUN_INTERVAL', 3)
    test_arg_num('FORK_GC_CLEAN_THRESHOLD', 3)
    test_arg_num('FORK_GC_RETRY_INTERVAL', 3)
    test_arg_num('_MAX_RESULTS_TO_UNSORTED_MODE', 3)
    test_arg_num('UNION_ITERATOR_HEAP', 20)
    test_arg_num('_NUMERIC_RANGES_PARENTS', 1)

    # True/False arguments
    def test_arg_true(arg_name):
        env = Env(moduleArgs=arg_name)
        if env.env == 'existing-env':
            env.skip()
        assert env.expect('ft.config', 'get', arg_name).equal([[arg_name, 'true']])
        env.stop()

    test_arg_true('NOGC')
    test_arg_true('SAFEMODE')
    test_arg_true('CONCURRENT_WRITE_MODE')
    test_arg_true('NO_MEM_POOLS')

    # String arguments
    def test_arg_str(arg_name, arg_value, ret_value=None):
        if ret_value == None:
            ret_value = arg_value
        env = Env(moduleArgs=arg_name + ' ' + arg_value)
        if env.env == 'existing-env':
            env.skip()
        assert env.expect('ft.config', 'get', arg_name).equal([[arg_name, ret_value]])
        env.stop()

    test_arg_str('GC_POLICY', 'fork')
    test_arg_str('GC_POLICY', 'default', 'fork')
    test_arg_str('GC_POLICY', 'legacy', 'sync')
    test_arg_str('ON_TIMEOUT', 'fail')
    test_arg_str('TIMEOUT', '0', '0')
    test_arg_str('PARTIAL_INDEXED_DOCS', '0', 'false')
    test_arg_str('PARTIAL_INDEXED_DOCS', '1', 'true')
    test_arg_str('MAXSEARCHRESULTS', '100', '100')
    test_arg_str('MAXSEARCHRESULTS', '-1', 'unlimited')
    test_arg_str('MAXAGGREGATERESULTS', '100', '100')
    test_arg_str('MAXAGGREGATERESULTS', '-1', 'unlimited')
