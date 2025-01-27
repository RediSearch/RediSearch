# -*- coding: utf-8 -*-

from RLTest import Env
from includes import *
from common import *

def search(env, r, *args):
    return r.expect('ft.search', *args)

def setup_index(env):
    env.expect("FT.CREATE idx SCHEMA t1 TEXT t2 TAG").ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'h1', 't1', 'James Brown', 't2', 'NYC')
    conn.execute_command('HSET', 'h2', 't1', 'James Lore', 't2', 'MIA')
    conn.execute_command('HSET', 'h3', 't1', r'James\!\* Exclaim', 't2', 'PHX')

def test_wildcard(env):
    setup_index(env)
    expected = [2, 'h1', 'h2']
    all = [3, 'h3', 'h1', 'h2']
    search(env, env, 'idx', '@t1:\'James*\'', 'NOCONTENT', 'DIALECT', 2).equal(expected)
    search(env, env, 'idx', '@t1:"James*"', 'NOCONTENT', 'DIALECT', 2).equal(expected)
    search(env, env, 'idx', '@t1:(James*)', 'NOCONTENT', 'DIALECT', 2).equal(all)

def test_no_wildcard(env):
    setup_index(env)
    expected = [2, 'h1', 'h2']
    search(env, env, 'idx', '@t1:\'James\'', 'NOCONTENT', 'DIALECT', 2).equal(expected)
    search(env, env, 'idx', '@t1:"James"', 'NOCONTENT', 'DIALECT', 2).equal(expected)
    search(env, env, 'idx', '@t1:(James)', 'NOCONTENT', 'DIALECT', 2).equal(expected)

def test_tags(env):
    setup_index(env)
    expected = [1, 'h1']
    search(env, env, 'idx', '@t2:{NYC}', 'NOCONTENT', 'DIALECT', 2).equal(expected)
    search(env, env, 'idx', '@t2:{"NYC"}', 'NOCONTENT', 'DIALECT', 2).equal(expected)
    search(env, env, 'idx', '@t2:{\'NYC\'}', 'NOCONTENT', 'DIALECT', 2).equal(expected)

def test_verbatim_escaping(env):
    setup_index(env)
    expected = [1, 'h3']
    search(env, env, 'idx', r'@t1:("James\!\*")', 'NOCONTENT', 'DIALECT', 2).equal(expected)
    search(env, env, 'idx', r'@t1:(\'James\!\*\')', 'NOCONTENT', 'DIALECT', 2).equal(expected)