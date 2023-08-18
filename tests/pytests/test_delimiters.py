# -*- coding: utf-8 -*-

from common import assertInfoField, getConnectionByEnv, waitForIndex
from RLTest import Env

_defaultDelimiters = '\t !\"#$%&\'()*+,-./:;<=>?@[]^`{|}~'

def test01_IndexDelimiters(env):
    # Index with custom delimiters with escaped characters
    # the delimiters characters are printed ordered by its ASCII code
    env.expect(
        'FT.CREATE', 'idx1', 'ON', 'HASH',
        'DELIMITERS', ' \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~',
        'SCHEMA', 'foo', 'text').ok()
    assertInfoField(env, 'idx1', 'delimiters', _defaultDelimiters)
    env.execute_command('FT.DROPINDEX', 'idx1')

    # Index with custom delimiters
    env.expect(
        'FT.CREATE', 'idx1', 'ON', 'HASH',
        'DELIMITERS', ';*',
        'DELIMITERS', ';*,#@',
        'SCHEMA', 'foo', 'text').ok()
    assertInfoField(env, 'idx1', 'delimiters', '#*,;@')
    env.execute_command('FT.DROPINDEX', 'idx1')

    # If DELIMITERS exceeds MAX_DELIMITERSTRING_SIZE = 64 it will be truncated
    # and the duplicated values will be removed
    long_sep = ' \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~ \t!\"#$%&\'()*+,-./:;<=>?@\
[]^`{|}~ \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~ \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~'
    env.expect(
        'FT.CREATE', 'idx1', 'ON', 'HASH',
        'DELIMITERS', long_sep,
        'SCHEMA', 'foo', 'text').ok()
    assertInfoField(env, 'idx1', 'delimiters', '\t !\"#$%&\'()*+,-./:;<=>?@[]^`{|}~')
    env.execute_command('FT.DROPINDEX', 'idx1')

    # # TODO:
    # # Multiple rules for index level delimiters
    # env.expect(
    #     'FT.CREATE', 'idx1', 'ON', 'HASH',
    #     'DELIMITERS', '', 'DELIMITERS+', 'a', 'DELIMITERS+', 'b',
    #     'PREFIX', '1', 'customer:',
    #     'SCHEMA',
    #     'field1', 'TEXT').equal('OK')
    # waitForIndex(env, 'idx1')
    # assertInfoField(env, 'idx1', 'delimiters', 'ab')
    # env.execute_command('FT.DROPINDEX', 'idx1')

    # env.expect(
    #     'FT.CREATE', 'idx1', 'ON', 'HASH',
    #     'DELIMITERS', 'abcd', 'DELIMITERS-', 'a', 'DELIMITERS-', 'b',
    #     'PREFIX', '1', 'customer:',
    #     'SCHEMA',
    #     'field1', 'TEXT').equal('OK')
    # waitForIndex(env, 'idx1')
    # assertInfoField(env, 'idx1', 'delimiters', 'cd')


def test02_IndexOnHashWithCustomDelimiter(env):
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

    # Index with a single delimiter: semicolon (;)
    env.expect(
        'FT.CREATE idx1 ON hash DELIMITERS ; PREFIX 1 customer: \
        SCHEMA code TEXT SORTABLE email TEXT SORTABLE \
        name TEXT SORTABLE').equal('OK')
    waitForIndex(env, 'idx1')
    assertInfoField(env, 'idx1', 'delimiters', ';')

    res = env.execute_command('FT.SEARCH idx1 @name:Sarah RETURN 1 code')
    expected_result = [1, 'customer:2', ['code', '101;222']]
    env.assertEqual(res, expected_result)

    # Searching "@code:101" should return 3 results, because (;) is a delimiter
    res = env.execute_command('FT.SEARCH idx1 @code:101 RETURN 1 code LIMIT 0 0')
    env.assertEqual(res, [3])

    # Searching "@code:101;222" should not return results
    res = env.execute_command('FT.SEARCH idx1 @code:101\\;222 RETURN 1 code')
    env.assertEqual(res, [0])

    env.execute_command('FT.DROPINDEX', 'idx1')

    # Index with custom delimiters: semicolon (;) was removed
    env.expect(
        'FT.CREATE', 'idx2', 'ON', 'HASH',
        'DELIMITERS', ' \t!\"#$%&\'()*+,-./:<=>?@[]^`{|}~',
        'PREFIX', '1', 'customer:', 'SCHEMA',
        'code', 'TEXT', 'SORTABLE',
        'email', 'TEXT', 'SORTABLE',
        'name', 'TEXT', 'SORTABLE').equal('OK')
    waitForIndex(env, 'idx2')

    assertInfoField(env, 'idx2', 'delimiters', '\t !\"#$%&\'()*+,-./:<=>?@[]^`{|}~')

    # Searching "@code:101" should return 0 results, because 101 is not a token
    res = env.execute_command('FT.SEARCH idx2 @code:101')
    env.assertEqual(res, [0])

    # Searching "@code=101;222" should return 1 result
    res = env.execute_command('FT.SEARCH idx2 @code:101\\;222 RETURN 1 code')
    expected_result = [1, 'customer:2', ['code', '101;222']]
    env.assertEqual(res, expected_result)

    env.execute_command('FT.DROPINDEX', 'idx2')

def test03_IndexOnJSONWithCustomDelimiter(env):
    conn = getConnectionByEnv(env)
    # Create sample data
    conn.execute_command(
        'JSON.SET', 'login:1', '$', '{"app":"a1","dev_id":"1b-e0:0f"}')
    conn.execute_command(
        'JSON.SET', 'login:2', '$', '{"app":"a2","dev_id":"1b-4a:70"}')
    conn.execute_command(
        'JSON.SET', 'login:3', '$', '{"app":"a3","dev_id":"1b-a3:0f"}')

    # Index with custom delimiters: hyphen (-) was removed
    env.expect(
        'FT.CREATE', 'idx_login', 'ON', 'JSON',
        'DELIMITERS', ' \t!\"#$%&\'()*+,./:;<=>?@[]^`{|}~',
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

def test04_SummarizeCustomDelimiter(env):
    # Index with custom delimiters: hyphen (-) was removed
    env.expect(
        'FT.CREATE', 'idx', 'ON', 'HASH',
        'DELIMITERS', ' \t!\"#$%&\'()*+,./:;<=>?@[]^`{|}~',
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

def test05_IndexOnHashCustomDelimiterByFieldDataFirst(env):
    conn = getConnectionByEnv(env)

    # Create sample data
    conn.execute_command(
        'HSET', 'customer:1', 'code', '101;111@0f', 'email', 'c01@rx.com',
        'name', 'Kyle')
    conn.execute_command(
        'HSET', 'customer:2', 'code', '101;222@70', 'email', 'c02@rx.com',
        'name', 'Sarah')
    conn.execute_command(
        'HSET', 'customer:3', 'code', '101;333@0f', 'email', 'c03@rx.com',
        'name', 'Ginger')

    # Index with custom delimiters by field
    # the delimiters of field 'code' is equal to @
    # the delimiters of field 'email' does not contain: at (@), dot (.)
    env.expect(
        'FT.CREATE', 'idx2', 'ON', 'HASH',
        'DELIMITERS', ' \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~',
        'PREFIX', '1', 'customer:',
        'SCHEMA',
        'code', 'TEXT', 'DELIMITERS', '@',
        'SORTABLE',
        'email', 'TEXT', 'DELIMITERS', ' \t!\"#$%^&\'()*+,-/:;<=>?[]^`{|}~',
        'SORTABLE',
        'name', 'TEXT', 'SORTABLE').equal('OK')
    waitForIndex(env, 'idx2')

    # Create sample data
    conn.execute_command(
        'HSET', 'customer:1', 'code', '101;111@0f', 'email', 'c01@rx.com',
        'name', 'Kyle')
    conn.execute_command(
        'HSET', 'customer:2', 'code', '101;222@70', 'email', 'c02@rx.com',
        'name', 'Sarah')
    conn.execute_command(
        'HSET', 'customer:3', 'code', '101;333@0f', 'email', 'c03@rx.com',
        'name', 'Ginger')

    # Custom delimiters
    # Search by @code:101 should return 0, because (;) is not a delimiter
    res = env.execute_command(
            'FT.SEARCH', 'idx2', '@code:101', 'LIMIT', '0', '0')
    env.assertEqual(res, [0])

    # Search by @code:0f returns 2 results because 0f is a token
    res = env.execute_command('FT.SEARCH idx2 @code:0f LIMIT 0 0')
    env.assertEqual(res, [2])

    # Search by a complete @email should return result
    res = env.execute_command(
        'FT.SEARCH', 'idx2', '@email:c03\\@rx\\.com',
        'RETURN', '2', 'code', 'email')
    expected_result = [1, 'customer:3',
                       ['code', '101;333@0f', 'email', 'c03@rx.com']]
    env.assertEqual(res, expected_result)

    # Search by @email:c03 does not return results because it is not token
    res = env.execute_command(
        'FT.SEARCH', 'idx2', '@email:c03',
        'RETURN', '2', 'code', 'email')
    env.assertEqual(res, [0])

    # Search using two fields
    res = env.execute_command(
            'FT.SEARCH', 'idx2', '@code:0f @email:c03\\@rx\\.com',
            'RETURN', '2', 'code', 'email')
    expected_result = [1, 'customer:3',
                       ['code', '101;333@0f', 'email', 'c03@rx.com']]
    env.assertEqual(res, expected_result)

    env.cmd('FT.DROPINDEX', 'idx2')


def test06_IndexOnHashCustomDelimiterByFieldIndexFirst(env):
    conn = getConnectionByEnv(env)

    # Index with custom delimiters by field
    # the delimiters of field 'code' is equal to @
    # the delimiters of field 'email' does not contain: at (@), dot (.)
    env.expect(
        'FT.CREATE', 'idx2', 'ON', 'HASH',
        'DELIMITERS', ' \t!\"#$%&\'()*+,-./:;<=>?@[]^`{|}~',
        'PREFIX', '1', 'customer:',
        'SCHEMA',
        'code', 'TEXT', 'DELIMITERS', '@',
        'SORTABLE',
        'email', 'TEXT', 'DELIMITERS-', '@.',
        'SORTABLE',
        'name', 'TEXT', 'SORTABLE').equal('OK')
    waitForIndex(env, 'idx2')

    # Create sample data
    conn.execute_command(
        'HSET', 'customer:1', 'code', '101;111@0f', 'email', 'c01@rx.com',
        'name', 'Kyle')
    conn.execute_command(
        'HSET', 'customer:2', 'code', '101;222@70', 'email', 'c02@rx.com',
        'name', 'Sarah')
    conn.execute_command(
        'HSET', 'customer:3', 'code', '101;333@0f', 'email', 'c03@rx.com',
        'name', 'Ginger')

    # Custom delimiters
    # Search by @code:101 should return 0, because (;) is not a delimiter
    res = env.execute_command(
            'FT.SEARCH', 'idx2', '@code:101', 'LIMIT', '0', '0')
    env.assertEqual(res, [0])

    # Search by @code:0f returns 2 results because 0f is a token
    res = env.execute_command('FT.SEARCH idx2 @code:0f LIMIT 0 0')
    env.assertEqual(res, [2])

    # Search by a complete @email should return result
    res = env.execute_command(
        'FT.SEARCH', 'idx2', '@email:c03\\@rx\\.com',
        'RETURN', '2', 'code', 'email')
    expected_result = [1, 'customer:3',
                       ['code', '101;333@0f', 'email', 'c03@rx.com']]
    env.assertEqual(res, expected_result)

    # Search by @email:c03 does not return results because it is not token
    res = env.execute_command(
        'FT.SEARCH', 'idx2', '@email:c03',
        'RETURN', '2', 'code', 'email')
    env.assertEqual(res, [0])

    # Search using two fields
    res = env.execute_command(
            'FT.SEARCH', 'idx2', '@code:0f @email:c03\\@rx\\.com',
            'RETURN', '2', 'code', 'email')
    expected_result = [1, 'customer:3',
                       ['code', '101;333@0f', 'email', 'c03@rx.com']]
    env.assertEqual(res, expected_result)

    env.cmd('FT.DROPINDEX', 'idx2')

def test07_SummarizeCustomDelimiterByField(env):
    # Index with custom delimiters by field: hyphen (-) was removed
    env.expect(
        'FT.CREATE', 'idx', 'ON', 'HASH',
        'SCHEMA', 'txt', 'TEXT',
        'DELIMITERS', ' \t!\"#$%&\'()*+,./:;<=>?@[]^`{|}~').equal('OK')
    waitForIndex(env, 'idx')

    env.expect(
        'FT.ADD', 'idx', 'text1', '1.0',
        'FIELDS', 'txt', 'This is self-guided tour available for everyone.'
    ).equal('OK')

    res = env.execute_command(
        'FT.SEARCH', 'idx', 'self\\-guided',
        'SUMMARIZE', 'FIELDS', '1', 'txt', 'LEN', '3',
        'HIGHLIGHT', 'FIELDS', '1', 'txt', 'TAGS', '<b>', '</b>')
    expected_result = [1, 'text1', ['txt', 'is <b>self-guided</b> tour... ']]
    env.assertEqual(res, expected_result)

    env.execute_command('FT.DROPINDEX', 'idx')

def test08_FieldDelimitersConfig(env):

    # Index with custom delimiters by field, single rule by field
    env.expect(
        'FT.CREATE', 'idx', 'ON', 'HASH',
        'PREFIX', '1', 'customer:',
        'SCHEMA',
        'code', 'TEXT', 'DELIMITERS+', 'abc',
        'SORTABLE',
        'email', 'TEXT', 'DELIMITERS-', '@.',
        'SORTABLE',
        'name', 'TEXT', 'DELIMITERS+', '',
        'SORTABLE').equal('OK')
    waitForIndex(env, 'idx')

    # TODO: Should we print always the delimiters?
    # assertInfoField(env, 'idx', 'delimiters', _defaultDelimiters)

    if not env.isCluster():
        res = env.execute_command('FT.INFO', 'idx')

        # code delimiters
        env.assertEqual(res[7][0][1], 'code')
        env.assertEqual(res[7][0][8], 'delimiters')
        env.assertEqual(res[7][0][9], '\t !"#$%&\'()*+,-./:;<=>?@[]^`abc{|}~')
        # email delimiters
        env.assertEqual(res[7][1][1], 'email')
        env.assertEqual(res[7][1][8], 'delimiters')
        env.assertEqual(res[7][1][9], '\t !\"#$%&\'()*+,-/:;<=>?[]^`{|}~')
        # name delimiters
        env.assertEqual(res[7][2][1], 'name')
        env.assertEqual(res[7][2][8], 'delimiters')
        env.assertEqual(res[7][2][9], _defaultDelimiters)

def test09_FieldDelimitersConfigMultipleRules(env):

    # Index with custom delimiters by field, multiple rules by field
    env.expect(
        'FT.CREATE', 'idx2', 'ON', 'HASH',
        'DELIMITERS', '',
        'PREFIX', '1', 'customer:',
        'SCHEMA',
        'field1', 'TEXT', 'DELIMITERS', '@-!', 'DELIMITERS+', '? \t',
        'SORTABLE',
        'field2', 'TEXT', 'DELIMITERS-', 'abc',
        'SORTABLE',
        'field3', 'TEXT', 'DELIMITERS+', '??z??', 'DELIMITERS-', 'zxy',
        'SORTABLE',
        'field4', 'TEXT', 'DELIMITERS+', '',
        'SORTABLE',
        'field5', 'TEXT', 'DELIMITERS+', '??z??', 'DELIMITERS+', 'zxy',
        'SORTABLE',
        'field6', 'TEXT', 'DELIMITERS', 'abcd*', 'DELIMITERS-', 'b',
        'DELIMITERS-', '*', 'SORTABLE',
        ).equal('OK')
    waitForIndex(env, 'idx2')

    assertInfoField(env, 'idx2', 'delimiters', '')

    if not env.isCluster():
        res = env.execute_command('FT.INFO', 'idx2')

        # field1 delimiters
        env.assertEqual(res[7][0][1], 'field1')
        env.assertEqual(res[7][0][8], 'delimiters')
        env.assertEqual(res[7][0][9], '\t !-?@')
        # field2 delimiters
        env.assertEqual(res[7][1][1], 'field2')
        env.assertEqual(res[7][1][8], 'delimiters')
        env.assertEqual(res[7][1][9], '')
        # field3 delimiters
        env.assertEqual(res[7][2][1], 'field3')
        env.assertEqual(res[7][2][8], 'delimiters')
        env.assertEqual(res[7][2][9], '?')
        # field4 delimiters
        env.assertEqual(res[7][3][1], 'field4')
        env.assertEqual(res[7][3][8], 'delimiters')
        env.assertEqual(res[7][3][9], '')
        # field5 delimiters
        env.assertEqual(res[7][4][1], 'field5')
        env.assertEqual(res[7][4][8], 'delimiters')
        env.assertEqual(res[7][4][9], '?xyz')
        # field6 delimiters
        env.assertEqual(res[7][5][1], 'field6')
        env.assertEqual(res[7][5][8], 'delimiters')
        env.assertEqual(res[7][5][9], 'acd')

def test10_IndexOnHashTagDelimiters(env):
    conn = getConnectionByEnv(env)

    conn.execute_command(
        'HSET', 'traveller:1', 'cities', 'New York, Barcelona, San Francisco',
        'name', 'Tara Holden')
    conn.execute_command(
        'HSET', 'traveller:2', 'cities', 'San Francisco',
        'name', 'Kate Connor')

    # Create index without DELIMITER in TAG field should use the SEPARATOR char
    # by default (backward comptible)
    env.expect(
        'FT.CREATE', 'tagIndex1', 'ON', 'HASH',
        'PREFIX', '1', 'traveller:',
        'SCHEMA', 'name', 'TEXT', 'cities', 'TAG').equal('OK')
    waitForIndex(env, 'tagIndex1')

    res = env.execute_command('FT.TAGVALS', 'tagIndex1', 'cities')
    expected_result = ['barcelona', 'new york', 'san francisco']
    env.assertEqual(res, expected_result)

    res = env.execute_command(
        'FT.SEARCH', 'tagIndex1', '@cities:{san francisco}',
        'RETURN', '1', 'name')
    env.assertEqual(res[0], 2)

    # TODO: Check if this is the expected behavior
    # Create index using DELIMITERS ','
    env.expect(
        'FT.CREATE', 'tagIndex2', 'ON', 'HASH',
        'PREFIX', '1', 'traveller:',
        'SCHEMA',
        'name', 'TEXT',
        'cities', 'TAG', 'DELIMITERS', ',').equal('OK')
    waitForIndex(env, 'tagIndex2')

    res = env.execute_command('FT.TAGVALS', 'tagIndex2', 'cities')
    expected_result =  [' barcelona', ' san francisco', 'new york', 'san francisco']
    env.assertEqual(res, expected_result)

def test11_IndexOnJsonTagDelimiters(env):
    conn = getConnectionByEnv(env)

    # Create sample data
    conn.execute_command(
        'JSON.SET', 'page:1', '$',
        '{"title":"p1", "url":"https://redis.io/"}')
    conn.execute_command(
        'JSON.SET', 'page:2', '$',
        '{"title":"p2", "url":"https://www.youtube.com/watch?v=infTV4ifNZY"}')
    conn.execute_command(
        'JSON.SET', 'page:3', '$',
        '{"title":"p3", "url":"https://www.youtube.com/watch?v=abc324XXXX"}')

    # Create index using DELIMITERS '='
    env.expect(
        'FT.CREATE', 'idx', 'ON', 'JSON',
        'PREFIX', '1', 'page:',
        'SCHEMA',
        '$..title', 'AS', 'title', 'TEXT', 'SORTABLE',
        '$..url', 'AS', 'url', 'TAG', 'DELIMITERS', '=', 'SORTABLE').equal('OK')
    waitForIndex(env, 'idx')

    res = env.execute_command('FT.TAGVALS', 'idx', 'url')
    expected_result = ['abc324xxxx', 'https://redis.io/',
                        'https://www.youtube.com/watch?v', 'inftv4ifnzy']
    env.assertEqual(res, expected_result)
