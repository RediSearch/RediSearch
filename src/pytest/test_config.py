def testConfig(env):
    env.skipOnCluster()
    env.cmd('ft.create', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.expect('ft.config', 'help', 'idx').equal([])
    env.expect('ft.config', 'set', 'MINPREFIX', 1).equal('OK')    

def testConfigErrors(env):
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MINPREFIX', 1, 2).equal('EXCESSARGS')    
    env.expect('ft.config', 'no_such_command', 'idx').equal('No such configuration action')
    env.expect('ft.config', 'idx').error().contains("wrong number of arguments for 'ft.config' command")

def testGetConfigOptions(env):
    env.skipOnCluster()
    assert env.expect('ft.config', 'get', 'EXTLOAD').res[0][0] == 'EXTLOAD'    
    assert env.expect('ft.config', 'get', 'SAFEMODE').res[0][0] == 'SAFEMODE'  
    assert env.expect('ft.config', 'get', 'NOGC').res[0][0] == 'NOGC'  
    assert env.expect('ft.config', 'get', 'MINPREFIX').res[0][0] == 'MINPREFIX'   
    assert env.expect('ft.config', 'get', 'FORKGC_SLEEP_BEFORE_EXIT').res[0][0] =='FORKGC_SLEEP_BEFORE_EXIT'   
    assert env.expect('ft.config', 'get', 'MAXDOCTABLESIZE').res[0][0] =='MAXDOCTABLESIZE'   
    assert env.expect('ft.config', 'get', 'MAXEXPANSIONS').res[0][0] =='MAXEXPANSIONS'   
    assert env.expect('ft.config', 'get', 'TIMEOUT').res[0][0] =='TIMEOUT'   
    assert env.expect('ft.config', 'get', 'INDEX_THREADS').res[0][0] =='INDEX_THREADS'   
    assert env.expect('ft.config', 'get', 'SEARCH_THREADS').res[0][0] =='SEARCH_THREADS'    
    assert env.expect('ft.config', 'get', 'FRISOINI').res[0][0] =='FRISOINI'    
    assert env.expect('ft.config', 'get', 'ON_TIMEOUT').res[0][0] == 'ON_TIMEOUT'
    assert env.expect('ft.config', 'get', 'GCSCANSIZE').res[0][0] =='GCSCANSIZE'    
    assert env.expect('ft.config', 'get', 'MIN_PHONETIC_TERM_LEN').res[0][0] =='MIN_PHONETIC_TERM_LEN'    
    assert env.expect('ft.config', 'get', 'GC_POLICY').res[0][0] =='GC_POLICY' 
    assert env.expect('ft.config', 'get', 'FORK_GC_RUN_INTERVAL').res[0][0] =='FORK_GC_RUN_INTERVAL'   
    assert env.expect('ft.config', 'get', 'FORK_GC_CLEAN_THRESHOLD').res[0][0] =='FORK_GC_CLEAN_THRESHOLD'  
    assert env.expect('ft.config', 'get', 'FORK_GC_RETRY_INTERVAL').res[0][0] =='FORK_GC_RETRY_INTERVAL'  
    assert env.expect('ft.config', 'get', '_MAX_RESULTS_TO_UNSORTED_MODE').res[0][0] =='_MAX_RESULTS_TO_UNSORTED_MODE'

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