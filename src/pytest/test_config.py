def testConfig(env):
    env.cmd('ft.create', 'idx', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    env.expect('ft.config', 'help', 'idx').equal([])
    env.expect('ft.config', 'set', 'MINPREFIX', 1).equal('OK')    
    env.expect('ft.config', 'set', 'MINPREFIX', 1, 2).equal('EXCESSARGS')    
    env.expect('ft.config', 'set', 'MINPREFIX', 'str').equal('Success (not an error)')  
    env.expect('ft.config', 'no_such_command', 'idx').equal('No such configuration action')
    env.expect('ft.config', 'idx').error().contains("wrong number of arguments for 'ft.config' command")
