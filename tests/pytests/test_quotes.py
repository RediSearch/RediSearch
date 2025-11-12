# -*- coding: utf-8 -*-

from RLTest import Env
from includes import *
from common import *

def search(env, *args):
    return env.expect('ft.search', *args)

def sort_document_names(document_list):
    if len(document_list) == 0:
        return {}

    num_docs = document_list[0]
    names = document_list[1:]
    names.sort()
    return [num_docs, *names]

def setup_index(env):
    env.expect("FT.CREATE idx SCHEMA t1 TEXT NOSTEM t2 TAG").ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'h1', 't1', 'James Brown', 't2', 'NYC')
    conn.execute_command('HSET', 'h2', 't1', 'James Lore', 't2', 'MIA')
    conn.execute_command('HSET', 'h3', 't1', 'James\\!\\* Exclaim', 't2', 'PHX')

def test_wildcard(env):
    setup_index(env)
    expected = [2, 'h1', 'h2']
    all = [3, 'h1', 'h2', 'h3']
    search(env, 'idx', '@t1:\'James*\'', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)
    search(env, 'idx', '@t1:"James*"', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)
    search(env, 'idx', '@t1:(James*)', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(all)

def test_no_wildcard(env):
    setup_index(env)
    expected = [2, 'h1', 'h2']
    search(env, 'idx', '@t1:\'James\'', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)
    search(env, 'idx', '@t1:"James"', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)
    search(env, 'idx', '@t1:(James)', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)

def test_tags(env):
    setup_index(env)
    expected = [1, 'h1']
    search(env, 'idx', '@t2:{NYC}', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)
    search(env, 'idx', '@t2:{"NYC"}', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)
    search(env, 'idx', '@t2:{\'NYC\'}', 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)
    search(env, 'idx', "@t2:{\"NYC\"}", 'NOCONTENT', 'DIALECT', 2).apply(sort_document_names).equal(expected)

def test_verbatim_escaping(env):
    setup_index(env)
    expected = [1, 'h3']
    expected_explain = '@t1:EXACT {\n  @t1:james!*\n}\n'
    dquote = '@t1:("James\\!\\*")'
    # Need to extra escape due to redis bug with single quote escaping:
    # https://github.com/redis/redis/issues/6928
    # https://github.com/redis/redis/issues/8672
    squote = "@t1:('James\\!\\*')"
    env.expect(debug_cmd(), 'DUMP_TERMS', 'idx').contains('james!*')
    env.expect('FT.EXPLAIN', 'idx', dquote, 'DIALECT', 2).equal(expected_explain)
    search(env, 'idx', dquote, 'NOCONTENT', 'DIALECT', 2).equal(expected)
    env.expect('FT.EXPLAIN', 'idx', squote, 'DIALECT', 2).equal(expected_explain)
    search(env, 'idx', squote, 'NOCONTENT', 'DIALECT', 2).equal(expected)
