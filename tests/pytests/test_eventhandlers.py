from common import *

def testFlushDefaultDatabase(env):
    conn = getConnectionByEnv(env)

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    env.cmd('select', '0')

    conn.execute_command('FLUSHDB')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo').equal('things: no such index')

def testFlushNotDefaultDatabase(env):
    conn = getConnectionByEnv(env)

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')            

    # Switch to DB 1...
    env.cmd('select', '1')

    # Call FLUSHDB from DB 1, which should leave the search index and hash intact. 
    conn.execute_command('FLUSHDB')

    env.expect('ft.search', 'things', 'foo').equal([1, '1', ['name', 'foo']])   