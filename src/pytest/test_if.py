from RLTest import Env


def testIfQueries(env):
    env.cmd('FT.CREATE idx SCHEMA txt TEXT num NUMERIC empty TEXT')
    env.cmd('FT.ADD idx doc1 1.0 FIELDS txt word num 10')
    env.expect('FT.GET idx doc1').equal(['txt', 'word', 'num', '10'])

    # test single field
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@txt) FIELDS txt word').contains('to_number: cannot convert string')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_number(@txt) FIELDS txt word').contains('to_number: cannot convert string')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @num FIELDS num 10').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@num FIELDS num 10').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_str(@num) FIELDS num 10').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_str(@num) FIELDS num 10').equal('NOADD')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@empty) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_number(@empty) FIELDS txt word').error()
   #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_str(@empty) FIELDS num 10').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_str(@empty) FIELDS num 10').equal('NOADD')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @noexist FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@noexist FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@noexist) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_number(@noexist) FIELDS txt word').error()
   #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_str(@noexist) FIELDS num 10').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_str(@noexist) FIELDS num 10').equal('NOADD')

    # test multiple fields
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt==@txt FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty==@empty FIELDS txt word').equal('OK')

    # comaprison filled to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt==@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt!=@empty FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt>@empty FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt>=@empty FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt<@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt<=@empty FIELDS txt word').equal('NOADD')
    # negative comparison filled to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt==@empty FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt!=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt>@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt>=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt<@empty FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt<=@empty FIELDS txt word').equal('OK')

    # comaprison empty to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty==@empty FIELDS txt word').equal('OK')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty!=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty>@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty>=@empty FIELDS txt word').equal('OK')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty<@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty<=@empty FIELDS txt word').equal('OK')          # 1.4 OK

    # negative comparison empty to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty==@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty!=@empty FIELDS txt word').equal('OK')         # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty>@empty FIELDS txt word').equal('OK')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty>=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty<@empty FIELDS txt word').equal('OK')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty<=@empty FIELDS txt word').equal('NOADD')

    # Or
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt||@txt FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt||@empty FIELDS txt word').equal('OK')                           
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty||@txt FIELDS txt word').equal('OK')       # 1.6 NOADD
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty||@empty FIELDS txt word').equal('NOADD')  # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty||!@empty FIELDS txt word').equal('OK')    # 1.6 NOADD
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty||@empty FIELDS txt word').equal('OK')    # 
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty||!@empty FIELDS txt word').equal('OK')   # 

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt||@empty=="word" FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"||@txt FIELDS txt word').equal('OK')               # ?? # 1.6 NOADD 
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"||@empty=="word" FIELDS txt word').equal('NOADD')  # ??

    # And
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt&&@txt FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt&&@empty FIELDS txt word').equal('NOADD')            # 1.4 OK                  
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty&&@txt FIELDS txt word').equal('NOADD')            # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty&&@empty FIELDS txt word').equal('NOADD')          # 1.4 OK

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt&&@empty=="word" FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"&&@txt FIELDS txt word').equal('NOADD')            # ??
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"&&@empty=="word" FIELDS txt word').equal('NOADD')  # ??

    #check only 1st tested
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number("redis") FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt||to_number("redis") FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt||to_number("redis") FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty||to_number("redis") FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty||to_number("redis") FIELDS txt word').equal('OK')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt&&to_number("redis") FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty&&to_number("redis") FIELDS txt word').equal('NOADD')
