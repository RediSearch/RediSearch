# -*- coding: utf-8 -*-

from common import *
from RLTest import Env

def testMultibyteText(env):
    '''Test that multibyte characters are correctly converted to lowercase and
    that queries are case-insensitive using TEXT fields'''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'RUSSIAN', 'SCHEMA', 't', 'TEXT')

    conn.execute_command('HSET', 'test:1', 't', 'abcabc')
    conn.execute_command('HSET', 'test:2', 't', 'ABCABC')
    conn.execute_command('HSET', 'test:upper', 't', 'БЪЛГА123') # uppercase
    conn.execute_command('HSET', 'test:lower', 't', 'бълга123') # lowercase
    conn.execute_command('HSET', 'test:mixed', 't', 'БЪлга123') # mixed case
    conn.execute_command('HSET', 'doc:eszett_1', 't', 'GRÜẞEN')
    conn.execute_command('HSET', 'doc:eszett_2', 't', 'grüßen')

    if not env.isCluster():
        # only 2 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 3)
        env.assertEqual(res, ['abcabc', 'grüßen', 'бълга123'])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # Search term without multibyte chars
        expected = [2, 'test:2', 'test:1']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:abcabc', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:ABCABC', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        expected = [3, 'test:upper', 'test:mixed', 'test:lower']
        # Search uppercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪЛГА123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search lowercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:бълга123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search mixed case term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪлга123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with lowercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:бълга*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with uppercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪЛГА*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with lowercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ълга123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with uppercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ЪЛГА123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with lowercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ълга*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with uppercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ЪЛГА*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search for term with eszett
        expected = [2, 'doc:eszett_1', 'doc:eszett_2']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(GRÜẞEN)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(grüßen)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(grüß*)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # res = conn.execute_command(
        #     'FT.SEARCH', 'idx', '@t:(GRÜß*)', 'NOCONTENT', 'SORTBY', 't')
        # env.assertEqual(res, expected)

        # res = conn.execute_command(
        #     'FT.SEARCH', 'idx', '@t:(*üßen)', 'NOCONTENT', 'SORTBY', 't')
        # env.assertEqual(res, expected)

        # res = conn.execute_command(
        #     'FT.SEARCH', 'idx', '@t:(*ÜßEN)', 'NOCONTENT', 'SORTBY', 't')
        # env.assertEqual(res, expected)


def testJsonMultibyteText(env):
    '''Test that multibyte characters are correctly converted to lowercase and
    that queries are case-insensitive using TEXT fields on JSON index'''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON',
            'LANGUAGE', 'RUSSIAN', 'SCHEMA', '$.t', 'AS', 't', 'TEXT')

    conn.execute_command('JSON.SET', 'test:1', '$', r'{"t": "abcabc"}')
    conn.execute_command('JSON.SET', 'test:2', '$', r'{"t": "ABCABC"}')
    conn.execute_command('JSON.SET', 'test:upper', '$', r'{"t": "БЪЛГА123"}')
    conn.execute_command('JSON.SET', 'test:lower', '$', r'{"t": "бълга123"}')
    conn.execute_command('JSON.SET', 'test:mixed', '$', r'{"t": "БЪлга123"}')
    conn.execute_command('JSON.SET', 'doc:eszett_1', '$', r'{"t": "GRÜẞEN"}')
    conn.execute_command('JSON.SET', 'doc:eszett_2', '$', r'{"t": "grüßen"}')

    if not env.isCluster():
        # only 2 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 3)
        env.assertEqual(res, ['abcabc', 'grüßen', 'бълга123'])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # Search term without multibyte chars
        expected = [2, 'test:2', 'test:1']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:abcabc', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:ABCABC', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        expected = [3, 'test:upper', 'test:mixed', 'test:lower']
        # Search uppercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪЛГА123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search lowercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:бълга123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search mixed case term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪлга123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with lowercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:бълга*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with uppercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪЛГА*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with lowercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ълга123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with uppercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ЪЛГА123', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with lowercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ълга*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search with uppercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ЪЛГА*', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search for term with eszett
        expected = [2, 'doc:eszett_1', 'doc:eszett_2']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(GRÜẞEN)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(grüßen)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

def testRussianAlphabet(env):
    '''Test that the russian alphabet is correctly indexed and searched.'''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'RUSSIAN', 'SCHEMA', 't', 'TEXT', 'NOSTEM')
    conn.execute_command('HSET', 'test:consonantsU', 't', 'БВГДЗКЛМНПРСТФХ')
    conn.execute_command('HSET', 'test:consonantsL', 't', 'бвгдзклмнпрстфх')
    conn.execute_command('HSET', 'test:softConsonantsU', 't', 'ЙЧЩ')
    conn.execute_command('HSET', 'test:softConsonantsL', 't', 'йчщ')
    conn.execute_command('HSET', 'test:hardConsonantsU', 't', 'ЖШЦ')
    conn.execute_command('HSET', 'test:hardConsonantsL', 't', 'жшц')
    conn.execute_command('HSET', 'test:hardVowelsU', 't', 'АЭЫОУ')
    conn.execute_command('HSET', 'test:hardVowelsL', 't', 'аэыоу')
    conn.execute_command('HSET', 'test:softVowelsU', 't', 'ЯЕИЁЮ')
    conn.execute_command('HSET', 'test:softVowelsL', 't', 'яеиёю')

    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 5)

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # Search consonants
        expected = [2, 'test:consonantsU', 'test:consonantsL']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БВГДЗКЛМНПРСТФХ', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search soft consonants
        expected = [2, 'test:softConsonantsU', 'test:softConsonantsL']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:ЙЧЩ', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search hard consonants
        expected = [2, 'test:hardConsonantsU', 'test:hardConsonantsL']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:ЖШЦ', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search hard vowels
        expected = [2, 'test:hardVowelsU', 'test:hardVowelsL']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:АЭЫОУ', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search soft vowels
        expected = [2, 'test:softVowelsU', 'test:softVowelsL']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:ЯЕИЁЮ', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

def testDiacritics(env):
    ''' Test that caracters with diacritics are converted to lowercase, but the
    diacritics are not removed.
    '''
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'SPANISH', 'SCHEMA', 't', 'TEXT', 'NOSTEM')

    conn.execute_command('HSET', 'test:1', 't', 'éèêë')
    conn.execute_command('HSET', 'test:2', 't', 'ÉÈÊË')
    conn.execute_command('HSET', 'test:3', 't', 'àâä')
    conn.execute_command('HSET', 'test:4', 't', 'ÀÂÄ')
    conn.execute_command('HSET', 'test:5', 't', 'ç')
    conn.execute_command('HSET', 'test:6', 't', 'Ç')
    conn.execute_command('HSET', 'test:7', 't', 'œ')
    conn.execute_command('HSET', 'test:8', 't', 'Œ')
    conn.execute_command('HSET', 'test:9', 't', 'ùûü')
    conn.execute_command('HSET', 'test:10', 't', 'ÙÛÜ')
    conn.execute_command('HSET', 'test:11', 't', 'îï')
    conn.execute_command('HSET', 'test:12', 't', 'ÎÏ')
    conn.execute_command('HSET', 'test:13', 't', 'ôö')
    conn.execute_command('HSET', 'test:14', 't', 'ÔÖ')
    conn.execute_command('HSET', 'test:15', 't', 'ÿ')
    conn.execute_command('HSET', 'test:16', 't', 'Ÿ')
    conn.execute_command('HSET', 'test:17', 't', 'æ')
    conn.execute_command('HSET', 'test:18', 't', 'Æ')

    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        # only 9 terms are indexed, the lowercase representation of the terms
        # with diacritics, but the diacritis are not removed.
        env.assertEqual(len(res), 9)
        env.assertEqual(res, ['àâä', 'æ', 'ç', 'éèêë', 'îï', 'ôö', 'ùûü', 'ÿ', 'œ'])

def testDiacriticLimitation(env):
    ''' Test that the diacritics are not removed, so the terms with diacritics
    are not found when searching for terms without diacritics, and vice versa.
    This limitation should be removed in the future, see MOD-5366.
    '''
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'FRENCH', 'SCHEMA', 't', 'TEXT')

    conn.execute_command('HSET', 'mot:1', 't', 'etude')
    conn.execute_command('HSET', 'mot:2', 't', 'étude')
    conn.execute_command('HSET', 'mot:3', 't', 'etudes')
    conn.execute_command('HSET', 'mot:4', 't', 'études')

    # the diacritics are not removed, so we got 6 different terms
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        expected = ['+etud', '+étud', 'etude', 'etudes', 'étude', 'études']
        env.assertEqual(res, expected)

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # search term without diacritics
        # the diacritics are not removed, so the terms WITH diacritics are
        # not found
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:etude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [2, 'mot:1', 'mot:3'], message=f'Dialect: {dialect}')
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:Etude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [2, 'mot:1', 'mot:3'], message=f'Dialect: {dialect}')

        # search term with diacritics
        # the diacritics are not removed, so the terms WITHOUT diacritics are
        # not found
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:étude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [2, 'mot:2', 'mot:4'])
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:Étude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [2, 'mot:2', 'mot:4'])

@skip(cluster=True)
def testStopWords(env):
    '''Test that stopwords are not indexed, but for multibyte characters they
    are not converted to lowercase correctly. This is a limitation that will be
    fixed by MOD-8443'''

    conn = getConnectionByEnv(env)
    # test with russian lowercase stopwords
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'STOPWORDS', 3, 'и', 'не', 'от',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')

    conn.execute_command('HSET', 'doc:1', 't', 'не ясно') # 1 term
    conn.execute_command('HSET', 'doc:2', 't', 'Мужчины и женщины') # 2 terms
    conn.execute_command('HSET', 'doc:3', 't', 'от одного до десяти') # 3 terms
    # create the same text with different case
    conn.execute_command('HSET', 'doc:4', 't', 'НЕ ЯСНО')
    conn.execute_command('HSET', 'doc:5', 't', 'МУЖЧИНЫ И ЖЕНЩИНЫ')
    conn.execute_command('HSET', 'doc:6', 't', 'ОТ ОДНОГО ДО ДЕСЯТИ')
    # only 6 terms are indexed, the stopwords are not indexed
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx1')
    env.assertEqual(len(res), 6)
    env.assertEqual(res, ['десяти', 'до', 'женщины', 'мужчины', 'одного', 'ясно'])

    # check the stopwords list - lowercase
    res = index_info(env, 'idx1')['stopwords_list']
    env.assertEqual(res, ['и', 'не', 'от'])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)
        # search for a stopword term should return 0 results
        res = conn.execute_command('FT.SEARCH', 'idx1', '@t:(не | от | и)')
        env.assertEqual(res, [0])
        res = conn.execute_command('FT.SEARCH', 'idx1', '@t:(НЕ | ОТ | И)')
        env.assertEqual(res, [0])


    # test with russian uppercase stopwords.
    env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'STOPWORDS', 3, 'И', 'НЕ', 'ОТ',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')
    waitForIndex(env, 'idx2')
    # This fails, there are 9 terms because the stopwords are not converted
    # to lowercase correctly
    # Ticket created to fix this: MOD-8443
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx2')
    env.assertEqual(len(res), 9)
    env.assertEqual(res, ['десяти', 'до', 'женщины', 'и', 'мужчины', 'не',
                          'одного', 'от', 'ясно'])

    # check the stopwords list - uppercase
    res = index_info(env, 'idx2')['stopwords_list']
    env.assertEqual(res, ['И', 'НЕ', 'ОТ'])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)
        # In idx2, the stopwords were created with uppercase, and currently they
        # are not converted to lowercase.
        # So the search for the stopwords in lowercase returns all the docs.
        expected = [6, 'doc:5', 'doc:2', 'doc:4', 'doc:6', 'doc:1', 'doc:3']
        res = conn.execute_command('FT.SEARCH', 'idx2', '@t:(не | от | и)',
                                   'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search for the stopwords in uppercase should return 0 results, because
        # they were not indexed.
        res = conn.execute_command('FT.SEARCH', 'idx2', '@t:(НЕ | ОТ | И)',
                                   'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0], message=f'Dialect: {dialect}')

def testInvalidMultiByteSequence(env):
    '''Test that invalid multi-byte sequences are ignored when indexing terms.
    '''
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'SCHEMA', 't', 'TEXT')
    conn = getConnectionByEnv(env)

    # Valid strings for comparison
    conn.execute_command('HSET', 'test:1', 't', 'abcabc')
    conn.execute_command('HSET', 'test:2', 't', 'ABCABC')

    # Invalid multi-byte sequences
    invalid_str1 = b'\xC3'         # Incomplete UTF-8 sequence
    invalid_str2 = b'\xC3\x28'     # Invalid UTF-8 sequence
    invalid_str3 = b'\xC0\xAF'     # Overlong encoding
    invalid_str4 = b'\xE2\x28\xA1' # Invalid UTF-8 sequence

    # Store invalid strings in Redis
    conn.execute_command('HSET', 'test:3', 't', invalid_str1.decode('utf-8', 'ignore'))
    conn.execute_command('HSET', 'test:4', 't', invalid_str2.decode('utf-8', 'ignore'))
    conn.execute_command('HSET', 'test:5', 't', invalid_str3.decode('utf-8', 'ignore'))
    conn.execute_command('HSET', 'test:6', 't', invalid_str4.decode('utf-8', 'ignore'))

    # Check the terms in the index
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        # Only the valid terms are indexed
        env.assertEqual(res, ['abcabc'])

    res = conn.execute_command('FT.SEARCH', 'idx', '@t:abcabc', 'NOCONTENT')
    env.assertEqual(res, [2, 'test:1', 'test:2'])

def testGermanEszett(env):
    '''Test that the german eszett is correctly indexed and searched.
    The eszett is a special case, because the uppercase unicode character
    occupies 3 bytes, and the lowercase unicode character occupies 2 bytes.
    '''
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'LANGUAGE', 'GERMAN',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'test:1', 't', 'GRÜẞEN') # term: grüßen
    conn.execute_command('HSET', 'test:2', 't', 'grüßen') # term: grüßen
    # Some times the 'ẞ' (eszett) is written as 'ss', but this will result in a
    # different term
    conn.execute_command('HSET', 'test:3', 't', 'GRÜSSEN')
    conn.execute_command('HSET', 'test:4', 't', 'grüssen')

    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 2)
        env.assertEqual(res, ['grüssen', 'grüßen'])

    # Query for terms with 'ẞ'
    expected = [2, 'test:1', 'test:2']
    res = conn.execute_command('FT.SEARCH', 'idx', '@t:GRÜẞEN', 'NOCONTENT')
    env.assertEqual(res, expected)
    res = conn.execute_command('FT.SEARCH', 'idx', '@t:grüßen', 'NOCONTENT')
    env.assertEqual(res, expected)
    res = conn.execute_command('FT.SEARCH', 'idx', '@t:GrüßeN', 'NOCONTENT')
    env.assertEqual(res, expected)

    # Query for terms with 'ss'
    expected = [2, 'test:3', 'test:4']
    res = conn.execute_command('FT.SEARCH', 'idx', '@t:GRÜSSEN', 'NOCONTENT')
    env.assertEqual(res, expected)
    res = conn.execute_command('FT.SEARCH', 'idx', '@t:grüssen', 'NOCONTENT')
    env.assertEqual(res, expected)

def testLongTerms(env):
    '''Test that long terms are correctly indexed.
    This tests the case where unicode_tolower() uses heap memory allocation
    '''
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')
    conn = getConnectionByEnv(env)

    # lowercase
    long_term_lower = 'частнопредпринимательский' * 6;
    conn.execute_command('HSET', 'w1', 't', long_term_lower)
    # uppercase
    long_term_upper = 'ЧАСТНОПРЕДПРИНИМАТЕЛЬСКИЙ' * 6;
    conn.execute_command('HSET', 'w2', 't', long_term_lower)

    # A single term should be generated in lower case.
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx1')
        env.assertEqual(res, [long_term_lower])

    # For index with STEMMING enabled, two terms are expected
    env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'SCHEMA', 't', 'TEXT')
    waitForIndex(env, 'idx2')
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx2')
        env.assertEqual(res, [f'+{long_term_lower[:148]}', long_term_lower])

def testMultibyteTag(env):
    '''Test that multibyte characters are correctly converted to lowercase and
    that queries are case-insensitive using TAG fields'''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'ENGLISH', 'SCHEMA', 't', 'TAG', 'id', 'NUMERIC')

    conn.execute_command('HSET', 'doc:1', 't', 'abcabc', 'id', 1)
    conn.execute_command('HSET', 'doc:2', 't', 'ABCABC', 'id', 2)
    conn.execute_command('HSET', 'doc:upper', 't', 'БЪЛГА123', 'id', 3)
    conn.execute_command('HSET', 'doc:lower', 't', 'бълга123', 'id', 4)
    conn.execute_command('HSET', 'doc:mixed', 't', 'БЪлга123', 'id', 5)
    conn.execute_command('HSET', 'doc:eszett_1', 't', 'GRÜẞEN', 'id', 6)
    conn.execute_command('HSET', 'doc:eszett_2', 't', 'grüßen', 'id', 7)
    conn.execute_command('HSET', 'doc:eszeet_3', 't', 'FUẞBALL STRAẞE', 'id', 8)

    if not env.isCluster():
        # only 3 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
        env.assertEqual(res, [['abcabc', [1, 2]], ['fußball straße', [8]],
                              ['grüßen', [6, 7]], ['бълга123', [3, 4, 5]]])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # Search term without multibyte chars
        # ANY because for dialect 4 the count can be different
        expected = [ANY, 'doc:1', 'doc:2']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{abcabc}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{ABCABC}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        expected = [ANY, 'doc:upper', 'doc:lower', 'doc:mixed']
        # Search uppercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪЛГА123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search lowercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{бълга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search mixed case term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪлга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with lowercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{бълга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with uppercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪЛГА*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search with lowercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ълга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with uppercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ЪЛГА123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with lowercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ълга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with uppercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ЪЛГА*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search for term with Eszett (ẞ).
        # The Eszett is a special case, because the uppercase unicode character
        # occupies 3 bytes, and the lowercase unicode character occupies 2 bytes
        expected = [2, 'doc:eszett_1', 'doc:eszett_2']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{GRÜẞEN}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{grüßen}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{grüß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{GRÜß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*üßen}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ÜßEN}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*üß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*Üß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Test wildcard search
        # Tag + wildcard search is not supported by dialect 1
        if dialect > 1:
            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:{w'*ÜßEN'}", 'NOCONTENT', 'SORTBY', 'id')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:{w'GRÜ*'}", 'NOCONTENT', 'SORTBY', 'id')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:{w'GRÜßEN'}", 'NOCONTENT', 'SORTBY', 'id')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Test phrase search
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:{FUẞBALL STRAẞE}", 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:eszeet_3'], message=f'Dialect: {dialect}')

        # Tag + fuzzy search is not supported
        env.expect(
            'FT.SEARCH', 'idx', "@t:{%GRÜßET%}", 'NOCONTENT', 'SORTBY', 'id')\
                .error().contains('Syntax error')

def testJsonMultibyteTag(env):
    '''Test that multibyte characters are correctly converted to lowercase and
    that queries are case-insensitive using TAG fields on JSON index'''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON', 'LANGUAGE', 'ENGLISH', 'SCHEMA',
            '$.t', 'AS', 't', 'TAG', '$.id', 'AS', 'id', 'NUMERIC')

    conn.execute_command('JSON.SET', 'doc:1', '$', r'{"t": "abcabc", "id": 1}')
    conn.execute_command('JSON.SET', 'doc:2', '$', r'{"t": "ABCABC", "id": 2}')
    conn.execute_command('JSON.SET', 'doc:upper', '$', r'{"t": "БЪЛГА123", "id": 3}')
    conn.execute_command('JSON.SET', 'doc:lower', '$', r'{"t": "бълга123", "id": 4}')
    conn.execute_command('JSON.SET', 'doc:mixed', '$', r'{"t": "БЪлга123", "id": 5}')
    conn.execute_command('JSON.SET', 'doc:eszett_1', '$', r'{"t": "GRÜẞEN", "id": 6}')
    conn.execute_command('JSON.SET', 'doc:eszett_2', '$', r'{"t": "grüßen", "id": 7}')
    conn.execute_command('JSON.SET', 'doc:eszeet_3', '$', r'{"t": "FUẞBALL STRAẞE", "id": 8}')

    if not env.isCluster():
        # only 3 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
        env.assertEqual(res, [['abcabc', [1, 2]], ['fußball straße', [8]],
                              ['grüßen', [6, 7]], ['бълга123', [3, 4, 5]]])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # Search term without multibyte chars
        # ANY because for dialect 4 the count can be different
        expected = [ANY, 'doc:1', 'doc:2']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{abcabc}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{ABCABC}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        expected = [ANY, 'doc:upper', 'doc:lower', 'doc:mixed']
        # Search uppercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪЛГА123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search lowercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{бълга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Search mixed case term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪлга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with lowercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{бълга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with uppercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪЛГА*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with lowercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ълга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with uppercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ЪЛГА123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with lowercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ълга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search with uppercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ЪЛГА*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Search for term with eszett
        # The Eszett is a special case, because the uppercase unicode character
        # occupies 3 bytes, and the lowercase unicode character occupies 2 bytes
        expected = [2, 'doc:eszett_1', 'doc:eszett_2']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{GRÜẞEN}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{grüßen}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{grüß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{GRÜß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*üßen}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ÜßEN}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*üß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*Üß*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, expected)

        # Test wildcard search
        # Tag + wildcard search is not supported by dialect 1
        if dialect > 1:
            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:{w'*ÜßEN'}", 'NOCONTENT', 'SORTBY', 'id')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:{w'GRÜ*'}", 'NOCONTENT', 'SORTBY', 'id')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:{w'GRÜßEN'}", 'NOCONTENT', 'SORTBY', 'id')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Test phrase search
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:{FUẞBALL STRAẞE}", 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:eszeet_3'], message=f'Dialect: {dialect}')

        # Tag + fuzzy search is not supported
        env.expect(
            'FT.SEARCH', 'idx', "@t:{%GRÜßET%}", 'NOCONTENT', 'SORTBY', 'id')\
                .error().contains('Syntax error')

def testMultibyteTagCaseSensitive(env):
    '''Test multibyte characters with TAG fields with CASESENSITIVE option.
    The terms are not converted to lowercase, and the search is case-sensitive.
    '''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'ENGLISH',
            'SCHEMA', 't', 'TAG', 'CASESENSITIVE', 'id', 'NUMERIC')

    conn.execute_command('HSET', 'doc:1', 't', 'abcabc', 'id', 1)
    conn.execute_command('HSET', 'doc:2', 't', 'ABCABC', 'id', 2)
    conn.execute_command('HSET', 'doc:upper', 't', 'БЪЛГА123', 'id', 3)
    conn.execute_command('HSET', 'doc:lower', 't', 'бълга123', 'id', 4)
    conn.execute_command('HSET', 'doc:mixed', 't', 'БЪлга123', 'id', 5)
    conn.execute_command('HSET', 'doc:eszett_1', 't', 'GRÜẞEN', 'id', 6)
    conn.execute_command('HSET', 'doc:eszett_2', 't', 'grüßen', 'id', 7)

    if not env.isCluster():
        # 7 terms are indexed because the TAG field is CASESENSITIVE
        res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
        env.assertEqual(res, [['ABCABC', [2]], ['GRÜẞEN', [6]], ['abcabc', [1]],
                              ['grüßen', [7]], ['БЪЛГА123', [3]],
                              ['БЪлга123', [5]], ['бълга123', [4]]])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # Search lowercase term without multibyte chars
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{abcabc}', 'NOCONTENT')
        env.assertEqual(res, [1, 'doc:1'], message=f'Dialect: {dialect}')

        # Search uppercase term without multibyte chars
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{ABCABC}', 'NOCONTENT')
        env.assertEqual(res, [1, 'doc:2'])

        # Search uppercase term with multibyte chars
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪЛГА123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:upper'])

        # Search lowercase term with multibyte chars
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{бълга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:lower'])

        # Search mixed case term with multibyte chars
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪлга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:mixed'])

        # Search with lowercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{бълга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:lower'])

        # Search with uppercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{БЪЛГА*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:upper'])

        # Search with lowercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ълга123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:lower'])

        # Search with uppercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ЪЛГА123}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:upper'])

        # Search with lowercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ълга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:lower'])

        # Search with mixedcase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*Ълга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:mixed'])

        # Search with uppercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ЪЛГА*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [1, 'doc:upper'])

def testMultibyteBasicSynonymsUseCase(env):
    '''Test multi-byte synonyms with upper and lower case terms.'''
    conn = getConnectionByEnv(env)
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH',
        'schema', 'title', 'text', 'body', 'text').ok()
    # Create synonyms for 'fußball' using uppercase letters
    conn.execute_command('ft.synupdate', 'idx', 'id1', 'FUẞBALL', 'Football')
    conn.execute_command('HSET', 'doc1', 'title', 'Football ist gut')

    # Search for 'fußball' using lowercase letters
    res = conn.execute_command(
        'FT.SEARCH', 'idx', 'fußball', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [1, 'doc1', ['title', 'Football ist gut']])
