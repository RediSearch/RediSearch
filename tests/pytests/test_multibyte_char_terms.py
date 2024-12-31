# -*- coding: utf-8 -*-

from common import *
from RLTest import Env
import locale


def is_locale_available(locale_name):
    try:
        locale.setlocale(locale.LC_ALL, locale_name)
        return True
    except locale.Error:
        return False

def testMultibyteChars(env):
    ''' Test that multibyte characters are correctly converted to lowercase and
    that queries are case-insensitive.'''
    if not is_locale_available('en_US.UTF-8'):
        env.skip()

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'RUSSIAN', 'SCHEMA', 't', 'TEXT')
    
    conn.execute_command('HSET', 'test:1', 't', 'abcabc')
    conn.execute_command('HSET', 'test:2', 't', 'ABCABC')
    conn.execute_command('HSET', 'test:upper', 't', 'БЪЛГА123') # uppercase
    conn.execute_command('HSET', 'test:lower', 't', 'бълга123') # lowercase
    conn.execute_command('HSET', 'test:mixed', 't', 'БЪлга123') # mixed case

    if not env.isCluster():
        # only 2 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 2)
        env.assertEqual(res, ['abcabc', 'бълга123'])

    for dialect in range(1, 5):
        env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)

        # Search term without multibyte chars
        expected = [2, 'test:1', 'test:2']
        res = conn.execute_command('FT.SEARCH', 'idx', '@t:abcabc', 'NOCONTENT')
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', 'idx', '@t:ABCABC', 'NOCONTENT')
        env.assertEqual(res, expected)

        expected = [3, 'test:upper', 'test:lower', 'test:mixed']
        # Search uppercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪЛГА123', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search lowercase term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:бълга123', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search mixed case term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪлга123', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search with lowercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:бълга*', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search with uppercase prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪЛГА*', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search with lowercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ълга123', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search with uppercase suffix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ЪЛГА123', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search with lowercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ълга*', 'NOCONTENT')
        env.assertEqual(res, expected)

        # Search with uppercase contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:*ЪЛГА*', 'NOCONTENT')
        env.assertEqual(res, expected)

def testRussianAlphabet(env):
    '''Test that the russian alphabet is correctly indexed and searched.'''
    if not is_locale_available('en_US.UTF-8'):
        env.skip()

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
    if not is_locale_available('en_US.UTF-8'):
        env.skip()

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'ENGLISH', 'SCHEMA', 't', 'TEXT')

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
    
def testDiacriticLimitation(env):
    ''' Test that the diacritics are not removed, so the terms with diacritics
    are not found when searching for terms without diacritics, and vice versa.'''
    if not is_locale_available('en_US.UTF-8'):
        env.skip()

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
        env.assertEqual(res, [2, 'mot:1', 'mot:3'])
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:Etude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [2, 'mot:1', 'mot:3'])

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
    are not converted to lowercase correctly.'''
    if not is_locale_available('en_US.UTF-8'):
        env.skip()
    
    conn = getConnectionByEnv(env)
    # test with russian lowercase stopwords
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'STOPWORDS', 3, 'от', 'и', 'не',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')

    conn.execute_command('HSET', 'test:1', 't', 'не ясно') # 1 term
    conn.execute_command('HSET', 'test:2', 't', 'Мужчины и женщины') # 2 terms
    conn.execute_command('HSET', 'test:3', 't', 'от одного до десяти') # 3 terms
    # create the same text with different case
    conn.execute_command('HSET', 'test:4', 't', 'НЕ ЯСНО')
    conn.execute_command('HSET', 'test:5', 't', 'МУЖЧИНЫ И ЖЕНЩИНЫ')
    conn.execute_command('HSET', 'test:6', 't', 'ОТ ОДНОГО ДО ДЕСЯТИ')
    # only 6 terms are indexed, the stopwords are not indexed
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx1')
    env.assertEqual(len(res), 6)
    env.assertEqual(res, ['десяти', 'до', 'женщины', 'мужчины', 'одного', 'ясно'])  

    # test with russian uppercase stopwords.
    env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'STOPWORDS', 3, 'ОТ', 'И', 'НЕ',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')
    waitForIndex(env, 'idx2')
    # This fails, there are 9 terms because the stopwords are not converted
    # to lowercase correctly
    # Ticket created to fix this: MOD-8443
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx2')
    env.assertEqual(len(res), 9)
    env.assertEqual(res, ['десяти', 'до', 'женщины', 'и', 'мужчины', 'не',
                          'одного', 'от', 'ясно'])

def testInvalidMultiByteSequence(env):
    ''' Test that invalid multi-byte sequences are ignored when indexing terms.'''

    if not is_locale_available('en_US.UTF-8'):
        env.skip()
    
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
