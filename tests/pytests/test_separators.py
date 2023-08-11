# -*- coding: utf-8 -*-

import unittest
from common import *
from RLTest import Env

def test01_Separators(env):
    # Index with custom separators with escaped characters
    env.expect(
        'FT.CREATE', 'idx1', 'ON', 'HASH',
        'SEPARATORS', ' \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~',
        'SCHEMA', 'foo', 'text').ok()
    assertInfoField(env, 'idx1', 'separators', [' \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~'])
    env.execute_command('FT.DROPINDEX', 'idx1')

    # Index with custom separators
    env.expect(
        'FT.CREATE', 'idx1', 'ON', 'HASH',
        'SEPARATORS', ';*',
        'SEPARATORS', ';*,#@',
        'SCHEMA', 'foo', 'text').ok()
    assertInfoField(env, 'idx1', 'separators', [';*,#@'])
    env.execute_command('FT.DROPINDEX', 'idx1')

    # If SEPARATORS exceeds MAX_SEPARATORSTRING_SIZE = 64 it will be truncated
    long_sep = ' \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~ \t!\"#$%&\'()*+,-./:;<=>?@\
[]^`{|}~ \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~ \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~'
    env.expect(
        'FT.CREATE', 'idx1', 'ON', 'HASH',
        'SEPARATORS', long_sep,
        'SCHEMA', 'foo', 'text').ok()
    assertInfoField(env, 'idx1', 'separators', [long_sep[:64]])
    env.execute_command('FT.DROPINDEX', 'idx1')

def test02_IndexOnHashWithCustomSeparator(env):
    conn = getConnectionByEnv(env)

    # Create sample data
    conn.execute_command(
        'HSET', 'customer:1', 'code', '101;111', 'email', 'c01@rx.com',
        'name', 'Kyle')
    conn.execute_command(
        'HSET', 'customer:2', 'code', '101;222', 'email', 'c02@rx.com',
        'name', 'Sarah')
    conn.execute_command(
        'HSET', 'customer:3', 'code', '101;333', 'email', 'c03@rx.com',
        'name', 'Ginger')

    # Index with a single separator: semicolon (;)
    env.expect(
        'FT.CREATE idx1 ON hash SEPARATORS ; PREFIX 1 customer: \
        SCHEMA code TEXT SORTABLE email TEXT SORTABLE \
        name TEXT SORTABLE').equal('OK')
    waitForIndex(env, 'idx1')
    assertInfoField(env, 'idx1', 'separators', [';'])

    res = env.execute_command('FT.SEARCH idx1 @name:Sarah RETURN 1 code')
    expected_result = [1, 'customer:2', ['code', '101;222']]
    env.assertEqual(res, expected_result)

    # Searching "@code:101" should return 3 results, because (;) is a separator
    res = env.execute_command('FT.SEARCH idx1 @code:101 RETURN 1 code LIMIT 0 0')
    env.assertEqual(res, [3])

    # Searching "@code:101;222" should not return results
    res = env.execute_command('FT.SEARCH idx1 @code:101\\;222 RETURN 1 code')
    env.assertEqual(res, [0])

    env.execute_command('FT.DROPINDEX', 'idx1')

    # Index with custom separators: semicolon (;) was removed
    env.expect(
        'FT.CREATE', 'idx2', 'ON', 'HASH',
        'SEPARATORS', ' \t!\"#$%&\'()*+,-./:<=>?@[]^`{|}~',
        'PREFIX', '1', 'customer:', 'SCHEMA',
        'code', 'TEXT', 'SORTABLE',
        'email', 'TEXT', 'SORTABLE',
        'name', 'TEXT', 'SORTABLE').equal('OK')
    waitForIndex(env, 'idx2')
    env.expect('FT.INFO idx2')
    assertInfoField(env, 'idx2', 'separators', [' \t!\"#$%&\'()*+,-./:<=>?@[]^`{|}~'])

    # Searching "@code:101" should return 0 results, because 101 is not a token
    res = env.execute_command('FT.SEARCH idx2 @code:101')
    env.assertEqual(res, [0])

    # Searching "@code=101;222" should return 1 result
    res = env.execute_command('FT.SEARCH idx2 @code:101\\;222 RETURN 1 code')
    expected_result = [1, 'customer:2', ['code', '101;222']]
    env.assertEqual(res, expected_result)

    env.execute_command('FT.DROPINDEX', 'idx2')

def test03_IndexOnJSONWithCustomSeparator(env):
    conn = getConnectionByEnv(env)
    # Create sample data
    conn.execute_command(
        'JSON.SET', 'login:1', '$', '{"app":"a1","dev_id":"1b-e0:0f"}')
    conn.execute_command(
        'JSON.SET', 'login:2', '$', '{"app":"a2","dev_id":"1b-4a:70"}')
    conn.execute_command(
        'JSON.SET', 'login:3', '$', '{"app":"a3","dev_id":"1b-a3:0f"}')

    # Index with custom separators: hyphen (-) was removed
    env.expect(
        'FT.CREATE', 'idx_login', 'ON', 'JSON',
        'SEPARATORS', ' \t!\"#$%&\'()*+,./:;<=>?@[]^`{|}~',
        'PREFIX', '1', 'login:', 'SCHEMA',
        '$.app', 'AS', 'app', 'text',
        '$.dev_id', 'AS', 'dev_id', 'TEXT', 'NOSTEM').equal('OK')
    waitForIndex(env, 'idx_login')

    # Searching "@dev_id:1b" should return 0 results, because 1b is not a token
    res = env.execute_command('FT.SEARCH idx_login @dev_id:1b')
    env.assertEqual(res, [0])

    # Searching "@dev_id:0f" should return 2 results, because 0f is a token
    res = env.execute_command('FT.SEARCH idx_login @dev_id:0f LIMIT 0 0')
    env.assertEqual(res, [2])

    # Search filtering two fields
    res = env.execute_command(
        'FT.SEARCH', 'idx_login', '@dev_id:0f @app:a1', 'LIMIT', '0', '0')
    env.assertEqual(res, [1])

    # Searching using tokens
    expected_result = [1, 'login:2', ['$', '{"app":"a2","dev_id":"1b-4a:70"}']]
    res = env.execute_command('FT.SEARCH idx_login @dev_id:1b\\-4a')
    env.assertEqual(res, expected_result)
    res = env.execute_command('FT.SEARCH idx_login @dev_id:70')
    env.assertEqual(res, expected_result)

    env.execute_command('FT.DROPINDEX', 'idx_login')

def test04_SummarizeCustomSeparator(env):
    # Index with custom separators: hyphen (-) was removed
    env.expect(
        'FT.CREATE', 'idx', 'ON', 'HASH',
        'SEPARATORS', ' \t!\"#$%&\'()*+,./:;<=>?@[]^`{|}~',
        'SCHEMA', 'txt', 'TEXT').equal('OK')
    waitForIndex(env, 'idx')

    env.expect(
        'FT.ADD', 'idx', 'text1', '1.0',
        'FIELDS', 'txt', 'This is self-guided tour available for everyone.'
    ).equal('OK')

    res = env.execute_command(
        'FT.SEARCH', 'idx', 'self\-guided',
        'SUMMARIZE', 'FIELDS', '1', 'txt', 'LEN', '3',
        'HIGHLIGHT', 'FIELDS', '1', 'txt', 'TAGS', '<b>', '</b>')
    expected_result = [1, 'text1', ['txt', 'is <b>self-guided</b> tour... ']]
    env.assertEqual(res, expected_result)

    env.execute_command('FT.DROPINDEX', 'idx')
