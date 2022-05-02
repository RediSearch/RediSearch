from RLTest import Env
from includes import *


def testIfQueries(env):
    env.cmd('FT.CREATE idx ON HASH SCHEMA txt TEXT num NUMERIC empty TEXT')
    env.cmd('FT.ADD idx doc1 1.0 FIELDS txt word num 10')
    res = env.cmd('FT.GET idx doc1')
    env.assertEqual(set(res), set(['txt', 'word', 'num', '10']))

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

def testExists(env):
    env.cmd('FT.CREATE idx ON HASH SCHEMA txt TEXT num NUMERIC empty TEXT')
    env.cmd('FT.ADD idx doc1 1.0 FIELDS txt word num 10')
    res = env.cmd('FT.GET idx doc1')
    env.assertEqual(set(res), set(['txt', 'word', 'num', '10']))

    # test single field
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@txt) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@num) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@empty) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@noexist) FIELDS txt word').contains('`noexist` not loaded')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !exists(@txt) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !exists(@num) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !exists(@empty) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if !exists(@noexist) FIELDS txt word').contains('`noexist` not loaded')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@txt)||exists(@txt) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@txt)||exists(@empty) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@empty)||exists(@txt) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@empty)||exists(@empty) FIELDS txt word').equal('NOADD')

    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@txt)&&exists(@txt) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@txt)&&exists(@empty) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@empty)&&exists(@txt) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(@empty)&&exists(@empty) FIELDS txt word').equal('NOADD')

    # check no crash
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if lower(exists(@empty)) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if upper(exists(@empty)) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if substr(exists(@empty)) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if format(exists(@empty)) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if split(exists(@empty)) FIELDS txt word').error()
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if matched_terms(exists(@empty)) FIELDS txt word').equal('NOADD')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_number(exists(@empty)) FIELDS txt word').equal('NOADD') # ??
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if to_str(exists(@empty)) FIELDS txt word').equal('OK')
    env.expect('FT.ADD idx doc1 1.0 REPLACE PARTIAL if exists(exists(@empty)) FIELDS txt word').equal('OK')  # ??

