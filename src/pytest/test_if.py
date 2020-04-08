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

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty FIELDS txt word').equal('NOADD')                  # 1.4 returns OK
    #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty FIELDS txt word').equal('OK')                    # 1.6 & 1.4 returns NOADD
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(@empty) FIELDS txt word').equal('NOADD')   #?? # 1.4 error
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_number(@empty) FIELDS txt word').equal('NOADD')  #?? # 1.4 error
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_str(@empty) FIELDS num 10').equal('NOADD')        #?? # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !to_str(@empty) FIELDS num 10').equal('NOADD')       #??

    # test multiple fields
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt==@txt FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty==@empty FIELDS txt word').equal('NOADD')          # 1.4 OK

    # comaprison filled to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt==@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt!=@empty FIELDS txt word').equal('NOADD')            # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt>@empty FIELDS txt word').equal('NOADD')             # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt>=@empty FIELDS txt word').equal('NOADD')            # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt<@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt<=@empty FIELDS txt word').equal('NOADD')
    # negative comparison filled to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt==@empty FIELDS txt word').equal('NOADD')           # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt!=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt>@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt>=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt<@empty FIELDS txt word').equal('NOADD')            # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@txt<=@empty FIELDS txt word').equal('NOADD')           # 1.4 OK

    # comaprison empty to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty==@empty FIELDS txt word').equal('NOADD')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty!=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty>@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty>=@empty FIELDS txt word').equal('NOADD')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty<@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty<=@empty FIELDS txt word').equal('NOADD')          # 1.4 OK

    # negative comparison empty to empty
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty==@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty!=@empty FIELDS txt word').equal('NOADD')         # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty>@empty FIELDS txt word').equal('NOADD')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty>=@empty FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty<@empty FIELDS txt word').equal('NOADD')          # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty<=@empty FIELDS txt word').equal('NOADD')

    # Or
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt||@txt FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt||@empty FIELDS txt word').equal('OK')                           
    #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty||@txt FIELDS txt word').equal('OK')       # 1.6 NOADD
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty||@empty FIELDS txt word').equal('NOADD')  # 1.4 OK
    #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty||!@empty FIELDS txt word').equal('OK')    # 1.6 NOADD
    #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty||@empty FIELDS txt word').equal('OK')    # 
    #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !@empty||!@empty FIELDS txt word').equal('OK')   # 

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt||@empty=="word" FIELDS txt word').equal('OK')
    #env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"||@txt FIELDS txt word').equal('OK')               # ?? # 1.6 NOADD 
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"||@empty=="word" FIELDS txt word').equal('NOADD')  # ??

    # And
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt&&@txt FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt&&@empty FIELDS txt word').equal('NOADD')            # 1.4 OK                  
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty&&@txt FIELDS txt word').equal('NOADD')            # 1.4 OK
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty&&@empty FIELDS txt word').equal('NOADD')          # 1.4 OK

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @txt&&@empty=="word" FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"&&@txt FIELDS txt word').equal('NOADD')            # ??
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if @empty=="word"&&@empty=="word" FIELDS txt word').equal('NOADD')  # ??

def testIsnull(env):
    env.cmd('FT.CREATE idx SCHEMA txt TEXT num NUMERIC empty TEXT')
    env.cmd('FT.ADD idx doc1 1.0 FIELDS txt word num 10')
    env.expect('FT.GET idx doc1').equal(['txt', 'word', 'num', '10'])

    # test single field
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@txt) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@num) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@empty) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@noexist) FIELDS txt word').contains('`noexist` not loaded')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !isnull(@txt) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !isnull(@num) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !isnull(@empty) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !isnull(@noexist) FIELDS txt word').contains('`noexist` not loaded')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@txt)||isnull(@txt) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@txt)||isnull(@empty) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@empty)||isnull(@txt) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@empty)||isnull(@empty) FIELDS txt word').equal('OK')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@txt)&&isnull(@txt) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@txt)&&isnull(@empty) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@empty)&&isnull(@txt) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(@empty)&&isnull(@empty) FIELDS txt word').equal('OK')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if lower(isnull(@empty)) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if upper(isnull(@empty)) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if substr(isnull(@empty)) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if format(isnull(@empty)) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if split(isnull(@empty)) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if matched_terms(isnull(@empty)) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(isnull(@empty)) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_str(isnull(@empty)) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if isnull(isnull(@empty)) FIELDS txt word').equal('NOADD')