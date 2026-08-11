# -*- coding: utf-8 -*-

from common import *
from RLTest import Env

# These are the unescaped and escaped versions of a long term with multibyte
# characters where the length of the converted term to lowercase is greater
# than its original length.
unescaped_long_term = 'E-Ticaret Yöneticisi / Yönetmeni - XXİX DANIŞMANLIK VE ELEKTRONİK ÇÖZÜMLER İTHALAT İHRACAT LİMİTED ŞİRKETİ - İstanbul'
escaped_long_term = 'E\\-Ticaret\\ Yöneticisi\\ \\/\\ Yönetmeni\\ \\-\\ XXİX\\ DANIŞMANLIK\\ VE\\ ELEKTRONİK\\ ÇÖZÜMLER\\ İTHALAT\\ İHRACAT\\ LİMİTED\\ ŞİRKETİ\\ \\-\\ İstanbul'


# These are the unescaped and escaped versions of a long term with multibyte
# characters where the length of the converted term to lowercase is greater
# than its original length.
unescaped_long_term = 'E-Ticaret Yöneticisi / Yönetmeni - XXİX DANIŞMANLIK VE ELEKTRONİK ÇÖZÜMLER İTHALAT İHRACAT LİMİTED ŞİRKETİ - İstanbul'
escaped_long_term = 'E\\-Ticaret\\ Yöneticisi\\ \\/\\ Yönetmeni\\ \\-\\ XXİX\\ DANIŞMANLIK\\ VE\\ ELEKTRONİK\\ ÇÖZÜMLER\\ İTHALAT\\ İHRACAT\\ LİMİTED\\ ŞİRKETİ\\ \\-\\ İstanbul'


def testMultibyteText(env):
    '''Test that multibyte characters are correctly converted to lowercase and
    that queries are case-insensitive using TEXT fields'''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'NOSTEM')

    conn.execute_command('HSET', 'test:1', 't', 'abcabc')
    conn.execute_command('HSET', 'test:2', 't', 'ABCABC')
    conn.execute_command('HSET', 'test:upper', 't', 'БЪЛГА123') # uppercase
    conn.execute_command('HSET', 'test:lower', 't', 'бълга123') # lowercase
    conn.execute_command('HSET', 'test:mixed', 't', 'БЪлга123') # mixed case
    conn.execute_command('HSET', 'doc:eszett_1', 't', 'GRÜẞEN')
    conn.execute_command('HSET', 'doc:eszett_2', 't', 'grüßen')
    conn.execute_command('HSET', 'doc:eszett_3', 't', 'FUẞBALL STRAẞE')

    if not env.isCluster():
        # only 5 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 5)
        env.assertEqual(res, ['abcabc', 'fußball', 'grüßen', 'straße',
                              'бълга123'])

    for dialect in range(1, 5):
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

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

        # Search using mixed uppercase and lowercase, different from the text
        # in the documents
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:бЪЛГА123', 'NOCONTENT', 'SORTBY', 't')
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

        # Test prefix search
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(grüß*)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(GRÜß*)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Test suffix search
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(*ßen)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(*üßen)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(*ÜßEN)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(*Üßen)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Test suffix search replacing ẞ by SS.
        # 0 results because ß is folded as a single S
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:(GRÜss*)', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0])

        # Test wildcard search
        # Text + wildcard search is not supported by dialect 1
        if dialect > 1:
            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:(w'*ÜßEN')", 'NOCONTENT', 'SORTBY', 't')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:(w'GRÜ*')", 'NOCONTENT', 'SORTBY', 't')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:(w'GRÜßEN')", 'NOCONTENT', 'SORTBY', 't')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:(w'GR?sseN')", 'NOCONTENT', 'SORTBY', 't')
            env.assertEqual(res, [0], message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:(w'gRÜ??eN')", 'NOCONTENT', 'SORTBY', 't')
            env.assertEqual(res, [0], message=f'Dialect: {dialect}')

            # ß is a single character, so this search should return results
            res = conn.execute_command(
                'FT.SEARCH', 'idx', "@t:(w'gRÜ?eN')", 'NOCONTENT', 'SORTBY', 't')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # Test phrase search
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(FUẞBALL STRAẞE)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [1, 'doc:eszett_3'], message=f'Dialect: {dialect}')

        # Test phrase search replacing ẞ by SS
        # 0 results because ß is not folded as 'ss'
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(FUSSBALL STRAssE)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0], message=f'Dialect: {dialect}')

        # Test fuzzy search
        expected = [2, 'doc:eszett_1', 'doc:eszett_2']
        # Max distance 1
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(%GRÜßET%)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message = f'Dialect: {dialect}')

        # 0 results because ß is folded as a single S
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(%GRÜSSET%)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0], message = f'Dialect: {dialect}')

        # Max distance 1
        # No changes.
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(%grüßen%)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Max distance 2
        # G was replaced by C, N was replaced by T
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(%%CRÜßET%%)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(%%crüßet%%)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Distance is 1, ß was replaced by X
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(%grüXen%)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Distance is 2, ß was replaced by X and n was replaced by L
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(%%grüXeL%%)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected)

        # Search using parameters
        if dialect > 1:
            expected = [3, 'test:upper', 'test:mixed', 'test:lower']
            res = conn.execute_command(
                'FT.SEARCH', 'idx', '@t:$p', 'NOCONTENT', 'PARAMS', 2, 'p',
                'БЪЛГА123', 'SORTBY', 't')
            env.assertEqual(res, expected)

            res = conn.execute_command(
                'FT.SEARCH', 'idx', '@t:$p*', 'NOCONTENT', 'PARAMS', 2, 'p',
                'БЪЛ', 'SORTBY', 't')
            env.assertEqual(res, expected)

            expected = [2, 'doc:eszett_1', 'doc:eszett_2']
            res = conn.execute_command(
                'FT.SEARCH', 'idx', '@t:($p)', 'NOCONTENT', 'PARAMS', 2, 'p',
                'GRÜẞEN', 'SORTBY', 't')
            env.assertEqual(res, expected)


def testJsonMultibyteText(env):
    '''Test that multibyte characters are correctly converted to lowercase and
    that queries are case-insensitive using TEXT fields on JSON index'''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON',
            'SCHEMA', '$.t', 'AS', 't', 'TEXT', 'NOSTEM')

    conn.execute_command('JSON.SET', 'test:1', '$', r'{"t": "abcabc"}')
    conn.execute_command('JSON.SET', 'test:2', '$', r'{"t": "ABCABC"}')
    conn.execute_command('JSON.SET', 'test:upper', '$', r'{"t": "БЪЛГА123"}')
    conn.execute_command('JSON.SET', 'test:lower', '$', r'{"t": "бълга123"}')
    conn.execute_command('JSON.SET', 'test:mixed', '$', r'{"t": "БЪлга123"}')
    conn.execute_command('JSON.SET', 'doc:eszett_1', '$', r'{"t": "GRÜẞEN"}')
    conn.execute_command('JSON.SET', 'doc:eszett_2', '$', r'{"t": "grüßen"}')
    conn.execute_command('JSON.SET', 'doc:eszett_3', '$', r'{"t": "FUẞBALL STRAẞE"}')
    conn.execute_command('JSON.SET', 'doc:I_lower', '$', r'{"t": "i̇stanbul"}')
    conn.execute_command('JSON.SET', 'doc:I_upper', '$', r'{"t": "İSTANBUL"}')

    if not env.isCluster():
        # only 5 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 6)
        env.assertEqual(res, ['abcabc', 'fußball', 'grüßen', 'i̇stanbul', 'straße',
                              'бълга123'])

    for dialect in range(1, 5):
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

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

        # Test phrase search
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(FUẞBALL STRAẞE)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [1, 'doc:eszett_3'], message=f'Dialect: {dialect}')

        # Test phrase search, replacing ẞ by S.
        # 0 results because ß was transformed to lowercase, not to S
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(FUSBALL STRAsE)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0], message=f'Dialect: {dialect}')

        # Test phrase search, replacing ẞ by SS
        # 0 results because ß is transformed to lowercase, not to SS
        res = conn.execute_command(
            'FT.SEARCH', 'idx', "@t:(FUSSBALL STRAssE)", 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0], message=f'Dialect: {dialect}')

def testRussianAlphabet(env):
    '''Test that the russian alphabet is correctly indexed and searched.'''

    conn = getConnectionByEnv(env)
    # We don't need to set the language to RUSSIAN, because the normalization
    # does not depend on the language, but on the unicode character.
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'NOSTEM')
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
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

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
    ''' Test that characters with diacritics are converted to lowercase, but the
    diacritics are not removed.
    '''
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'NOSTEM')

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
    # In this test set the index language to FRENCH, because we want to
    # search using stemmed words in french.
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'LANGUAGE', 'FRENCH', 'SCHEMA', 't', 'TEXT')

    conn.execute_command('HSET', 'mot:1', 't', 'etude')
    conn.execute_command('HSET', 'mot:2', 't', 'étude')
    conn.execute_command('HSET', 'mot:3', 't', 'etudes')
    conn.execute_command('HSET', 'mot:4', 't', 'études')

    # the diacritics are not removed, so we got 6 different terms:
    # the 4 original terms from the documents, and 2 stemmed terms.
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        expected = ['+etud', '+étud', 'etude', 'etudes', 'étude', 'études']
        env.assertEqual(res, expected)

    for dialect in range(1, 5):
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

        # search term without diacritics
        # the diacritics are not removed, so the terms WITH diacritics are
        # not found
        expected = [2, 'mot:1', 'mot:3']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:etude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:Etude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        # search term with diacritics
        # the diacritics are not removed, so the terms WITHOUT diacritics are
        # not found
        expected = [2, 'mot:2', 'mot:4']
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:étude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:Étude', 'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

@skip(cluster=True)
def testStopWords(env):
    '''Test that stopwords using multibyte characters are converted to lowercase
    correctly
    '''

    conn = getConnectionByEnv(env)
    # test with multi-byte lowercase stopwords
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'STOPWORDS', 5, 'и', 'не', 'от', 'fußball', 'şi̇rketi̇',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')

    conn.execute_command('HSET', 'doc:1', 't', 'не ясно fußball şi̇rketi̇') # 1 term
    conn.execute_command('HSET', 'doc:2', 't', 'Мужчины и женщины') # 2 terms
    conn.execute_command('HSET', 'doc:3', 't', 'от одного до десяти') # 3 terms
    # create the same text with different case
    conn.execute_command('HSET', 'doc:4', 't', 'НЕ ЯСНО FUßBALL ŞİRKETİ')
    conn.execute_command('HSET', 'doc:5', 't', 'МУЖЧИНЫ И ЖЕНЩИНЫ')
    conn.execute_command('HSET', 'doc:6', 't', 'ОТ ОДНОГО ДО ДЕСЯТИ')

    # only 6 terms are indexed, the stopwords are not indexed
    expected_terms = ['десяти', 'до', 'женщины', 'мужчины', 'одного', 'ясно']
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx1')
    env.assertEqual(len(res), 6)
    env.assertEqual(res, expected_terms)

    # check the stopwords list - lowercase
    expected_stopwords = ['fußball', 'şi̇rketi̇', 'и', 'не', 'от']
    res = index_info(env, 'idx1')['stopwords_list']
    env.assertEqual(res, expected_stopwords)

    for dialect in range(1, 5):
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

        # search for a stopword term should return 0 results
        res = conn.execute_command(
            'FT.SEARCH', 'idx1', '@t:(не | от | и | fußball | şi̇rketi̇)')
        env.assertEqual(res, [0])
        res = conn.execute_command(
            'FT.SEARCH', 'idx1', '@t:(НЕ | ОТ | И | FUßBALL | ŞİRKETİ)')
        env.assertEqual(res, [0])


    # test with multi-byte uppercase stopwords.
    env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'STOPWORDS', 5, 'И', 'НЕ', 'ОТ', 'FUßBALL', 'ŞİRKETİ',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')
    waitForIndex(env, 'idx2')
    # only 6 terms are indexed, the stopwords are not indexed, the same terms
    # as idx1
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx2')
    env.assertEqual(len(res), 6)
    env.assertEqual(res, expected_terms)

    # check the stopwords list - uppercase
    res = index_info(env, 'idx2')['stopwords_list']
    # the stopwords are converted to lowercase
    env.assertEqual(res, expected_stopwords)

    for dialect in range(1, 5):
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)
        # In idx2, the stopwords were created with uppercase, but they are
        # converted to lowercase.
        # So the search for the stopwords in lowercase returns 0 docs.
        res = conn.execute_command('FT.SEARCH', 'idx2', '@t:(не | от | и | fußball | şi̇rketi̇)',
                                   'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0])

        # Search for the stopwords in uppercase should return 0 results, because
        # they were not indexed.
        res = conn.execute_command('FT.SEARCH', 'idx2', '@t:(НЕ | ОТ | И | FÜßBALL | ŞİRKETİ)',
                                   'NOCONTENT', 'SORTBY', 't')
        env.assertEqual(res, [0])

def testInvalidMultiByteSequence(env):
    '''Test that invalid multi-byte sequences are ignored when indexing terms.
    '''
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
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
    # We don't need to set the language to GERMAN, because the normalization
    # does not depend on the language, but on the unicode character.
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'NOSTEM')
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'test:1', 't', 'GRÜẞEN') # term: grüssen
    conn.execute_command('HSET', 'test:2', 't', 'grüßen') # term: grüssen
    # Some times the 'ẞ' (eszett) is written as 'ss', but during normalization
    # we are converting to lowercase, not folding it to 'ss'.
    # So the words 'grüssen' and 'grüßen' would be indexed as different terms.
    conn.execute_command('HSET', 'test:3', 't', 'GRÜSSEN')
    conn.execute_command('HSET', 'test:4', 't', 'grüssen')

    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 2)
        env.assertEqual(res, ['grüssen', 'grüßen'])

    expected = [2, 'test:1', 'test:2']
    # Query for terms with 'ẞ'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:GRÜẞEN', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:grüßen', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:GrüßeN', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    # Query for terms with 'ss'
    expected = [2, 'test:3', 'test:4']
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:GRÜSSEN', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:grüssen', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

def testGreekSigma(env):
    '''Test that the greek sigma is correctly indexed and searched.
    The Greek letter "Σ" (U+03A3) is the uppercase form of the letter sigma.
    In Greek, the lowercase form of sigma can be either "σ" (U+03C3) or
    "ς" (U+03C2), depending on its position in the word.
    Since we are not folding it to "σ", we'll have different terms.'''

    # We don't need to set the language to GREEK, because the normalization
    # does not depend on the language, but on the unicode character.
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'NOSTEM')
    conn = getConnectionByEnv(env)

    # term 1: 'σίγμα' Sigma at the beginning of the word
    conn.execute_command('HSET', 'su@b:upper', 't', 'ΣΊΓΜΑ')
    conn.execute_command('HSET', 'su@b:mixed', 't', 'Σίγμα')
    conn.execute_command('HSET', 'su@b:lower', 't', 'σίγμα')

    # term 2: 'νεανίασ' Uppercase sigma at the end of the word
    conn.execute_command('HSET', 'su@e:upper', 't', 'ΝΕΑΝΊΑΣ')
    conn.execute_command('HSET', 'su@e:mixed', 't', 'νεανΊΑΣ')

    # term 3: 'νεανίας'  Lowercase sigma 'ς' at the end of the word
    # this is an invalid ingested term, because the lowercase sigma should be 'σ'
    conn.execute_command('HSET', 'sl@e:mixed', 't', 'Νεανίας')
    conn.execute_command('HSET', 'sl@e:lower', 't', 'νεανίας')

    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(len(res), 3)
        env.assertEqual(res, ['νεανίας', 'νεανίασ', 'σίγμα'])

    # Test with sigma at the beginning of the word
    expected = [3, 'su@b:upper', 'su@b:mixed', 'su@b:lower']
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:ΣΊΓΜΑ', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:Σίγμα', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:σίγμα', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:σίγ*', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    # Test with uppercase sigma at the end of the word
    expected = [2, 'su@e:upper', 'su@e:mixed']
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:ΝΕΑΝΊΑΣ', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    # Test with lowercase sigma at the end of the word
    expected = [2, 'sl@e:mixed', 'sl@e:lower']
    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:Νεανίας', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:νεανίας', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx', '@t:*νίας', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)


def testLongTerms(env):
    '''Test that long terms are correctly indexed.
    This tests the case where unicode_tolower() uses heap memory allocation
    '''
    env.cmd('FT.CREATE', 'idx1', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')
    conn = getConnectionByEnv(env)

    # lowercase
    long_term_lower = 'частнопредпринимательский' * 6
    conn.execute_command('HSET', 'w1', 't', long_term_lower)
    # uppercase
    long_term_upper = 'ЧАСТНОПРЕДПРИНИМАТЕЛЬСКИЙ' * 6
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

@skip(cluster=True)
def testLongTermWildcardQuery(env):
    '''Test that a wildcard (contains) query with >128 runes exercises the heap
    allocation path in strToLowerRunes.'''
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 't', 'TEXT', 'NOSTEM', 'WITHSUFFIXTRIE').ok()
    conn = getConnectionByEnv(env)

    # 'б' is a single Cyrillic rune; 130 repetitions > SSO_MAX_LENGTH (128)
    long_term = 'б' * 130
    conn.execute_command('HSET', 'doc:1', 't', long_term)

    # Contains query (*term*) calls strToLowerRunes at query time
    res = env.cmd('FT.SEARCH', 'idx', f'@t:*{long_term}*', 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res, [1, 'doc:1'])

    # Uppercase query — verifies case folding on the heap path
    res = env.cmd('FT.SEARCH', 'idx', f'@t:*{long_term.upper()}*', 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res, [1, 'doc:1'])

@skip(cluster=True)
def testSingleRuneMultibyteSuffixTrie(env):
    '''Test that a single multi-byte rune (rlen=1, len=3) is correctly
    inserted and deleted from the suffix trie without leaking memory.
    addSuffixTrie inserts the full-word entry unconditionally, so
    deleteSuffixTrie must handle it even when rlen < MIN_SUFFIX.'''
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 't', 'TEXT', 'NOSTEM', 'WITHSUFFIXTRIE').ok()
    conn = getConnectionByEnv(env)

    # '中' is a single CJK rune (3 bytes UTF-8, 1 rune)
    conn.execute_command('HSET', 'doc:1', 't', '中')

    res = env.cmd('FT.SEARCH', 'idx', '中', 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res, [1, 'doc:1'])

    # Delete the document; the suffix trie entry must be cleaned up
    conn.execute_command('DEL', 'doc:1')

    # Trigger GC to exercise deleteSuffixTrie on the single-rune term
    forceInvokeGC(env, 'idx')

    res = env.cmd('FT.SEARCH', 'idx', '中', 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res, [0])


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
        # only 4 terms are indexed, the lowercase representation of the terms
        res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
        env.assertEqual(res, [['abcabc', [1, 2]], ['fußball straße', [8]],
                              ['grüßen', [6, 7]], ['бълга123', [3, 4, 5]]])

    for dialect in range(1, 5):
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

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

        # Search using parameters
        if dialect > 1:
            expected = [3, 'doc:upper', 'doc:mixed', 'doc:lower']

            res = conn.execute_command(
                'FT.SEARCH', 'idx', '@t:{$p}', 'NOCONTENT', 'PARAMS', 2, 'p',
                'БЪЛГА123', 'SORTBY', 't')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            res = conn.execute_command(
                'FT.SEARCH', 'idx', '@t:{$p*}', 'NOCONTENT', 'PARAMS', 2, 'p',
                'БЪЛ', 'SORTBY', 't')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

            expected = [ANY, 'doc:eszett_1', 'doc:eszett_2']
            res = conn.execute_command(
                'FT.SEARCH', 'idx', '@t:{$p}', 'NOCONTENT', 'PARAMS', 2, 'p',
                'GRÜẞEN', 'SORTBY', 't')
            env.assertEqual(res, expected, message=f'Dialect: {dialect}')

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
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

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
        run_command_on_all_shards(env, config_cmd(),
                                  'SET', 'DEFAULT_DIALECT', dialect)

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

        # Search with an unexisting uppercase/lowercase combination contains
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:{*ЪЛга*}', 'NOCONTENT', 'SORTBY', 'id')
        env.assertEqual(res, [0])

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

def testMultibyteMemoryAllocationForSynonyms(env):
    '''Test multi-byte synonyms with upper and lower case terms which require
    memory reallocation.'''
    conn = getConnectionByEnv(env)
    env.expect(
        'ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text').ok()
    # Create synonyms for 'İSTANBUL' using uppercase letters
    conn.execute_command('ft.synupdate', 'idx', 'id1', 'İSTANBUL', 'ESTAMBUL')
    conn.execute_command('HSET', 'doc1', 'title', 'İstanbul capital of Turkey')

    # Search for 'estambul' using lowercase letters
    res = conn.execute_command(
        'FT.SEARCH', 'idx', 'estambul', 'EXPANDER', 'SYNONYM')
    env.assertEqual(res, [1, 'doc1', ['title', 'İstanbul capital of Turkey']])

def testSuggestions(env):
    '''Test suggestion dictionary with multi-byte characters.
    For the suggestions, the suggestions are saved in the dictionary in its
    original form, and during FT.SUGGET they are filtered using folding.'''
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.SUGADD', 'sug', 'синий', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'СИНИЙ красный', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'heiß', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'HEIẞ', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'heiss', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'HEISS', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'Dreißig', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'Dreissig', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'abcde', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'abcdef', 1)
    # Suggestions with greek sigma at the beginning of the word
    conn.execute_command('FT.SUGADD', 'sug', 'ΣΊΓΜΑ', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'Σίγμα', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'σίγμα', 1)
    # Suggestions with greek sigma at the end of the word
    conn.execute_command('FT.SUGADD', 'sug', 'ΝΕΑΝΊΑΣ', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'Νεανίας', 1)
    conn.execute_command('FT.SUGADD', 'sug', 'νεανίας', 1)
    # Suggestion with character İ which has a multi-codepoint folded rune
    conn.execute_command('FT.SUGADD', 'sug', 'İ = Letter I with dot above', 1)

    # Test suggestions with multi-byte characters
    expected = ['синий', 'СИНИЙ красный']
    res = conn.execute_command('FT.SUGGET', 'sug', 'синий')
    env.assertEqual(res, expected, message='синий')

    res = conn.execute_command('FT.SUGGET', 'sug', 'СИНИЙ')
    env.assertEqual(res, expected, message='СИНИЙ')

    # Test suggestions with ß
    expected = ['heiß', 'heiss', 'HEISS', 'HEIẞ']
    res = conn.execute_command('FT.SUGGET', 'sug', 'heiß')
    env.assertEqual(res, expected, message='heiß')

    res = conn.execute_command('FT.SUGGET', 'sug', 'HEIẞ')
    env.assertEqual(res, expected, message='HEIẞ')

    # For this case, the 4 results are returned, because the folded version of
    # heiß = heiss, and it matches as suggestion for `heis`
    res = conn.execute_command('FT.SUGGET', 'sug', 'heis')
    env.assertEqual(res, ['heiß', 'HEIẞ', 'heiss', 'HEISS'], message='heis')

    # For this case, only full match is returned
    expected = ['heiss', 'HEISS']
    res = conn.execute_command('FT.SUGGET', 'sug', 'heiss')
    env.assertEqual(res, expected, message='heiss')

    res = conn.execute_command('FT.SUGGET', 'sug', 'HEISS')
    env.assertEqual(res, expected, message='HEISS')

    # Test suggestions with ß in the middle of the word
    res = conn.execute_command('FT.SUGGET', 'sug', 'dreiẞ')
    env.assertEqual(res, ['Dreißig', 'Dreissig'], message='dreiẞ')

    res = conn.execute_command('FT.SUGGET', 'sug', 'dreis')
    env.assertEqual(res, ['Dreißig', 'Dreissig'], message='dreis')

    res = conn.execute_command('FT.SUGGET', 'sug', 'dreißig')
    env.assertEqual(res, ['Dreißig'], message = 'dreißig')

    res = conn.execute_command('FT.SUGGET', 'sug', 'dreissig')
    env.assertEqual(res, ['Dreissig'], message = 'dreissig')

    # Tests with single byte characters
    res = conn.execute_command('FT.SUGGET', 'sug', 'abcde')
    env.assertEqual(res, ['abcde', 'abcdef'], message='abcde')

    res = conn.execute_command('FT.SUGGET', 'sug', 'abcdef')
    env.assertEqual(res, ['abcdef'], message='abcdef')

    res = conn.execute_command('FT.SUGGET', 'sug', 'abcdefg')
    env.assertEqual(res, [], message='abcdefg')

    # Test suggestions with greek sigma at the beginning of the word
    expected = ['σίγμα', 'ΣΊΓΜΑ', 'Σίγμα']
    res = conn.execute_command('FT.SUGGET', 'sug', 'ΣΊΓΜΑ')
    env.assertEqual(res, expected, message = 'ΣΊΓΜΑ')

    res = conn.execute_command('FT.SUGGET', 'sug', 'Σίγμα')
    env.assertEqual(res, expected, message = 'Σίγμα')

    res = conn.execute_command('FT.SUGGET', 'sug', 'σίγμα')
    env.assertEqual(res, expected, message = 'σίγμα')

    # Test suggestions with greek sigma at the end of the word
    expected = ['νεανίας', 'ΝΕΑΝΊΑΣ', 'Νεανίας']
    res = conn.execute_command('FT.SUGGET', 'sug', 'ΝΕΑΝΊΑΣ')
    env.assertEqual(res, expected, message = 'ΝΕΑΝΊΑΣ')

    res = conn.execute_command('FT.SUGGET', 'sug', 'Νεανίας')
    env.assertEqual(res, expected, message = 'Νεανίας')

    res = conn.execute_command('FT.SUGGET', 'sug', 'νεανίας')
    env.assertEqual(res, expected, message = 'νεανίας')

    # This is a known limitation, when folding the suggestion, we are comparing
    # only the first byte of the folded character, and for that reason
    # 'dreisig' matches 'dreißig' but not 'dreissig'
    res = conn.execute_command('FT.SUGGET', 'sug', 'dreisig')
    env.assertEqual(res, ['Dreißig'], message = 'dreisig')
    res = conn.execute_command('FT.SUGGET', 'sug', 'dreisi')
    env.assertEqual(res, ['Dreißig'], message = 'dreisi')

    # same case for suggestion with İ characters which is folded as two
    # codepoints: (U+0069)(U+0307) : (i)(combining dot above)
    test_value = 'İ = Letter I with dot above'
    expected = [test_value]
    res = conn.execute_command('FT.SUGGET', 'sug', test_value)
    env.assertEqual(res, expected)
    # Search with a lowercase i also returns the same suggestion
    res = conn.execute_command('FT.SUGGET', 'sug', 'i = Letter I with dot above')
    env.assertEqual(res, expected)


def testRussianStemming(env):
    '''Test stemming with multi-byte character words.'''
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx_stem', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'SCHEMA', 't', 'TEXT')

    env.cmd('FT.CREATE', 'idx_no_stem', 'ON', 'HASH', 'LANGUAGE', 'RUSSIAN',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')

    # Create sample data
    # lowercase terms
    conn.execute_command('HSET', 'doc:1L', 't', 'программирование') # programming
    conn.execute_command('HSET', 'doc:2L', 't', 'программирования')
    conn.execute_command('HSET', 'doc:3L', 't', 'программированию')
    conn.execute_command('HSET', 'doc:4L', 't', 'программированием')
    conn.execute_command('HSET', 'doc:5L', 't', 'программировании')
    # mixed case terms
    conn.execute_command('HSET', 'doc:1M', 't', 'ПРОГРАммирование')
    conn.execute_command('HSET', 'doc:2M', 't', 'ПРОГРАммирования')
    conn.execute_command('HSET', 'doc:3M', 't', 'ПРОГРАммированию')
    conn.execute_command('HSET', 'doc:4M', 't', 'ПРОГРАммированием')
    conn.execute_command('HSET', 'doc:5M', 't', 'ПРОГРАммировании')

    # Search using the index with stemming
    expected = [10, 'doc:1M', 'doc:4M', 'doc:5M', 'doc:3M', 'doc:2M',
                    'doc:1L', 'doc:4L', 'doc:5L', 'doc:3L', 'doc:2L']
    res = conn.execute_command(
        'FT.SEARCH', 'idx_stem', 'программирование', 'NOCONTENT',
        'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx_stem', 'ПРОГРАммировании', 'NOCONTENT',
        'SORTBY', 't')
    env.assertEqual(res, expected)

    # Search using the NO_STEM index
    res = conn.execute_command(
        'FT.SEARCH', 'idx_no_stem', 'программирование', 'NOCONTENT',
        'SORTBY', 't')
    env.assertEqual(res, [2, 'doc:1M', 'doc:1L'])

    res = conn.execute_command(
        'FT.SEARCH', 'idx_no_stem', 'ПРОГРАММИРОВАНИИ', 'NOCONTENT',
        'SORTBY', 't')
    env.assertEqual(res, [2, 'doc:5M', 'doc:5L'])

def testGreekStemming(env):
    '''Test stemming with multi-byte character words.'''
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx_stem', 'ON', 'HASH', 'LANGUAGE', 'GREEK',
            'SCHEMA', 't', 'TEXT')

    env.cmd('FT.CREATE', 'idx_no_stem', 'ON', 'HASH', 'LANGUAGE', 'GREEK',
            'SCHEMA', 't', 'TEXT', 'NOSTEM')

    # Create sample data
    # lowercase terms
    conn.execute_command('HSET', 'doc:1L', 't', 'αεροπλάνο') # airplane
    conn.execute_command('HSET', 'doc:2L', 't', 'αεροπλάνα') # airplanes
    conn.execute_command('HSET', 'doc:3L', 't', 'αεροπλάνου') # of airplane

    # upper case terms
    conn.execute_command('HSET', 'doc:1U', 't', 'ΑΕΡΟΠΛΆΝΟ')
    conn.execute_command('HSET', 'doc:2U', 't', 'ΑΕΡΟΠΛΆΝΑ')
    conn.execute_command('HSET', 'doc:3U', 't', 'ΑΕΡΟΠΛΆΝΟΥ')

    # Search using the index with stemming
    expected = [6, 'doc:2U', 'doc:1U', 'doc:3U', 'doc:2L', 'doc:1L', 'doc:3L']
    res = conn.execute_command(
        'FT.SEARCH', 'idx_stem', 'αεροπλάνα', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx_stem', 'Αεροπλάνα', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    res = conn.execute_command(
        'FT.SEARCH', 'idx_stem', 'ΑΕΡΟΠΛΆΝΑ', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, expected)

    # Search using the NO_STEM index
    res = conn.execute_command(
        'FT.SEARCH', 'idx_no_stem', 'αεροπλάνο', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [2, 'doc:1U', 'doc:1L'])

    res = conn.execute_command(
        'FT.SEARCH', 'idx_no_stem', 'αεροπλάΝΑ', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [2, 'doc:2U', 'doc:2L'])

    res = conn.execute_command(
        'FT.SEARCH', 'idx_no_stem', 'ΑΕΡΟΠΛΆΝΟΥ', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [2, 'doc:3U', 'doc:3L'])

def testFuzzySearch(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'NOSTEM')

    # Create sample data
    conn.execute_command('HSET', 'doc:L', 't', 'fußball')
    conn.execute_command('HSET', 'doc:U', 't', 'fuẞball')
    conn.execute_command('HSET', 'doc:1s', 't', 'fusball')
    conn.execute_command('HSET', 'doc:2s', 't', 'fussball')

    # Max distance 1
    # Replacing multi-byte char 'ß' by 'X'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', f'@t:(%fuXball%)', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [3, 'doc:1s', 'doc:L', 'doc:U'])

    # Replacing multi-byte char 'ß' by 'S'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', f'@t:(%fuSball%)', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [4, 'doc:1s', 'doc:2s', 'doc:L', 'doc:U'])

    # Replacing single-byte char 'l' by 'x'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', f'@t:(%fußbalx%)', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [2, 'doc:L', 'doc:U'])

    # Max distance 2.
    # Replacing multi-byte char 'ß'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', f'@t:(%%fuXball%%)', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [4, 'doc:1s', 'doc:2s', 'doc:L', 'doc:U'])

    # Replacing single-byte char 'l'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', f'@t:(%%fußbalx%%)', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [3, 'doc:1s', 'doc:L', 'doc:U'])

    # Replacing single-byte char 'f' and multi-byte char 'ß'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', f'@t:(%%XuXball%%)', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [3, 'doc:1s', 'doc:L', 'doc:U'])

    # Replacing single-byte char 'f' and single-byte char 'l'
    res = conn.execute_command(
        'FT.SEARCH', 'idx', f'@t:(%%Xußbalx%%)', 'NOCONTENT', 'SORTBY', 't')
    env.assertEqual(res, [2, 'doc:L', 'doc:U'])

@skip(cluster=True)
def testTagSearch(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TAG')

    # Create sample data
    conn.execute_command('HSET', 'doc:L', 't', 'fußball')
    conn.execute_command('HSET', 'doc:U', 't', 'fuẞball')
    conn.execute_command('HSET', 'doc:1s', 't', 'fusball')
    conn.execute_command('HSET', 'doc:2s', 't', 'fussball')

    for w in ['fußball', 'fuẞball']:
        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:{{{w}}}', 'NOCONTENT')
        env.assertEqual(res, [2, 'doc:L', 'doc:U'])

    res = conn.execute_command('FT.SEARCH', 'idx', '@t:{fusball}', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc:1s'])

    res = conn.execute_command('FT.SEARCH', 'idx', '@t:{fussball}', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc:2s'])


def test_utf8_lowercase_longer_than_uppercase_tags(env):
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG')
    conn = getConnectionByEnv(env)

    t = unescaped_long_term
    t_lower = t.lower()
    t_escaped = escaped_long_term
    t_escaped_lower = t_escaped.lower()

    env.expect('HSET', '{doc}:1', 't', t).equal(1)
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
        # The term is converted to lowercase
        env.assertEqual(res, [[t_lower, [1]]])

    env.expect('HSET', '{doc}:2', 't', t_lower).equal(1)
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
        env.assertEqual(res, [[t_lower, [1, 2]]])

    for dialect in range(1, 5):
        if dialect == 4:
            expected = [ANY, '{doc}:1', '{doc}:2']
        else:
            expected = [2, '{doc}:1', '{doc}:2']

        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:{{{t_escaped}}}', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected, message=f'Dialect: {dialect}')

        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:{{{t_escaped_lower}}}', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected)

        # Search using TAG autoescape, which is only available in dialect 2 and above
        if dialect > 1:
            res = conn.execute_command(
                'FT.SEARCH', 'idx', f'@t:{{"{t}"}}', 'NOCONTENT', 'DIALECT', dialect)
            # The term is stored in its original form, so the search will return the
            # document with the original term
            env.assertEqual(res, expected)

            # Search lowercase term
            res = conn.execute_command(
                'FT.SEARCH', 'idx', f'@t:{{"{t_lower}"}}', 'NOCONTENT', 'DIALECT', dialect)
            env.assertEqual(res, expected)

        # 1 character, occupying 2 bytes in UTF-8 + 1 byte for the null
        # terminator, so the total length is 3 bytes
        t1 = 'İ'
        # 1 characters, occupying 3 bytes in UTF-8 + 1 byte for the null
        # terminator, so the total length is 4 bytes
        t1_lower = t1.lower()
        conn.execute_command('HSET', '{doc}:upper:1', 't', t1)
        conn.execute_command('HSET', '{doc}:lower:1', 't', t1_lower)

        if not env.isCluster():
            expected_2 = [ANY, '{doc}:upper:1', '{doc}:lower:1']
        else:
            expected_2 = [ANY, '{doc}:lower:1', '{doc}:upper:1']

        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:{{{t1}}}', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected_2)

        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:{{{t1_lower}}}', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected_2)


def test_utf8_lowercase_longer_than_uppercase_texts(env):
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'NOSTEM')
    conn = getConnectionByEnv(env)

    t = unescaped_long_term
    t_lower = t.lower()
    t_escaped = escaped_long_term
    t_escaped_lower = t_escaped.lower()

    env.expect('HSET', '{doc}:1', 't', t_escaped).equal(1)
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        # The term is converted to lowercase
        env.assertEqual(res, [t_lower])

    env.expect('HSET', '{doc}:2', 't', t_escaped_lower).equal(1)
    if not env.isCluster():
        res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx')
        env.assertEqual(res, [t_lower])

    expected = [2, '{doc}:1', '{doc}:2']
    for dialect in range(1, 5):
        # Search escaped original case  term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:({t_escaped})', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected)

        # Search lowercase escaped term
        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:({t_escaped_lower})', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected)

        # If we don't escape the term, each word is treated as a separate term,
        # so the search will return no results
        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:({t})', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, [0])
        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:({t_lower})', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, [0])

        # 1 character, occupying 2 bytes in UTF-8 + 1 byte for the null
        # terminator, so the total length is 3 bytes
        t1 = 'İ'
        # 1 characters, occupying 3 bytes in UTF-8 + 1 byte for the null
        # terminator, so the total length is 4 bytes
        t1_lower = t1.lower()
        conn.execute_command('HSET', '{doc}:upper:1', 't', t1)
        conn.execute_command('HSET', '{doc}:lower:1', 't', t1_lower)

        if not env.isCluster():
            expected_2 = [ANY, '{doc}:upper:1', '{doc}:lower:1']
        else:
            expected_2 = [ANY, '{doc}:lower:1', '{doc}:upper:1']

        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:({t1})', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected_2, message=f'Dialect: {dialect}')

        res = conn.execute_command(
            'FT.SEARCH', 'idx', f'@t:({t1_lower})', 'NOCONTENT', 'DIALECT', dialect)
        env.assertEqual(res, expected_2, message=f'Dialect: {dialect}')

# Codepoints where libnu's casemap tables (regenerated against
# Unicode 17.0) return no folding, for any of the following reasons:
#   - structurally invalid: surrogates (U+D800..U+DFFF) and noncharacters
#     (U+FDD0..U+FDEF, U+xFFFE, U+xFFFF in every plane)
#   - unassigned in Unicode 17.0
#   - assigned but caseless: scripts without case distinction
# This set is the test's allow-list of "no-fold expected"; every other
# codepoint must round-trip through nu_tofold/nu_tolower.
# Reference https://www.unicode.org/Public/17.0.0/ucd/UnicodeData.txt
LIBNU_FOLD_GAPS = {0x1CBB, 0x1CBC}  # Mtavruli gaps (unassigned)
LIBNU_FOLD_GAPS.add(0x1C89)  # Cyrillic Capital Letter TJE (post-9.0)
LIBNU_FOLD_GAPS.update(range(0xA7B9, 0xA7F7))
# Latin Extended-D pairs that libnu folds (and so must be removed
# from the gap set). After the Unicode 17.0 refresh, the cased letters
# in this block whose lowercase pair lives outside it are:
#   A7BA-A7BF (Glottal A/I/U pairs, Unicode 12.0)
#   A7C0 (Old Polish O, Unicode 14.0)
#   A7C2-A7C7 (Anglicana W + related, Unicode 12.0-13.0)
#   A7C9 (Old Polish O variant, Unicode 13.0)
#   A7D0, A7D6, A7D8 (Middle Scots S/TZ/Th, Unicode 14.0)
#   A7F5 (Reversed Gh, Unicode 13.0)
LIBNU_FOLD_GAPS.difference_update(
    {0xA7BA, 0xA7BB, 0xA7BC, 0xA7BD, 0xA7BE, 0xA7BF,
     0xA7C0, 0xA7C2, 0xA7C3, 0xA7C4, 0xA7C5, 0xA7C6,
     0xA7C7, 0xA7C9, 0xA7D0, 0xA7D6, 0xA7D8, 0xA7F5})
# Surrogate pairs (always invalid in Unicode)
LIBNU_FOLD_GAPS.update(range(0xD800, 0xE000))

# Noncharacters in BMP
LIBNU_FOLD_GAPS.update(range(0xFDD0, 0xFDF0))
LIBNU_FOLD_GAPS.add(0xFFFE)
LIBNU_FOLD_GAPS.add(0xFFFF)

LIBNU_FOLD_GAPS.update(range(0x10D40, 0x10D90))  # Garay script (Unicode 16.0)
# Tangut Components, Khitan Small, etc. minus Medefaidrin upper (0x16E40..0x16E5F, Unicode 11.0)
LIBNU_FOLD_GAPS.update(range(0x16B90, 0x16E40))
LIBNU_FOLD_GAPS.update(range(0x16E60, 0x16F00))

# Noncharacters in each plane
for plane in range(0x10000, 0x110000, 0x10000):
    LIBNU_FOLD_GAPS.add(plane + 0xFFFE)
    LIBNU_FOLD_GAPS.add(plane + 0xFFFF)

# Unassigned supplementary planes (as of Unicode 13.0)
LIBNU_FOLD_GAPS.update(range(0x2FA1E, 0xE0000))
LIBNU_FOLD_GAPS.update(range(0xE0080, 0xE0100))
LIBNU_FOLD_GAPS.update(range(0xE01F0, 0x10FFFE))


@skip(cluster=True)
def testToLowerConversionExactMatch(env):
    '''Test that tolower conversion works correctly for all unicode characters.
    This test skips surrogate pairs, which are not valid unicode characters
    and are not supported by the tolower conversion.
    It also skips lowercase characters, because the tolower conversion
    is not expected to change them.
    The test creates a document with a term that contains a single unicode
    character, and then searches for the term in both upper and lower case.
    '''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx_txt', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    env.cmd('FT.CREATE', 'idx_tag', 'ON', 'HASH', 'SCHEMA', 't', 'TAG')

    for idx in ['idx_txt', 'idx_tag']:
        for codepoint in range(0x110000):  # Unicode range from U+0000 to U+10FFFF
            # Skip surrogate pairs (0xD800 to 0xDFFF)
            if 0xD800 <= codepoint <= 0xDFFF:
                continue

            char = chr(codepoint)
            lower_char = char.lower()
            if char == lower_char:
                # If the character is already lowercase, skip it
                continue

            upper_term = char * 5
            lower_term = lower_char * 5
            env.cmd('HSET', 'doc:u', 't', upper_term)
            env.cmd('HSET', 'doc:l', 't', lower_term)

            if idx == 'idx_txt':
                query_u = f'@t:({upper_term})'
                query_l = f'@t:({lower_term})'
            elif idx == 'idx_tag':
                query_u = f'@t:{{{upper_term}}}'
                query_l = f'@t:{{{lower_term}}}'

            if codepoint in LIBNU_FOLD_GAPS:
                # For unsupported codepoints, different terms are created
                # for upper and lower case, so the search will return
                # a single result for each case.
                expected_u = [1, 'doc:u']
                expected_l = [1, 'doc:l']
            else:
                expected_u = [2, 'doc:u', 'doc:l']
                expected_l = expected_u

            # Test exact match for upper and lower case terms
            res = conn.execute_command('FT.SEARCH', idx, query_u, 'NOCONTENT')
            unicode_codes = ' '.join(f"U+{ord(c):04X}" for c in char)
            env.assertEqual(res, expected_u, message = f'{idx} query_u char: {char} {unicode_codes}')
            res = conn.execute_command('FT.SEARCH', idx, query_l, 'NOCONTENT')
            env.assertEqual(res, expected_l, message = f'{idx} query_l char: {char} {unicode_codes}')


@skip(cluster=True)
def testTagToLowerConversionSimilarMatch(env):
    '''Test that tolower conversion works correctly for all unicode characters
    when using TAG fields and running a query with a prefix, infix or suffix.
    This test skips characters that libnu does not fold (see LIBNU_FOLD_GAPS).
    It also skips lowercase characters, because the tolower conversion
    is not expected to change them.
    The test creates a document with a term that contains a single unicode
    character, and then searches for the term in both upper and lower case
    using a prefix, infix or suffix query.
    '''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx_tag', 'ON', 'HASH', 'SCHEMA', 't', 'TAG')

    idx = 'idx_tag'
    error = False
    for codepoint in range(0x110000):  # Unicode range from U+0000 to U+10FFFF
        if codepoint in LIBNU_FOLD_GAPS:
            # Skip unsupported codepoints:
            continue

        char = chr(codepoint)
        lower_char = char.lower()
        if char == lower_char:
            # If the character is already lowercase, skip it
            continue

        upper_term = char * 5
        lower_term = lower_char * 5
        env.cmd('HSET', f'doc:u:{codepoint:04X}', 't', upper_term)
        env.cmd('HSET', f'doc:l:{codepoint:04X}', 't', lower_term)

        queries = [
            # prefix
            (f'@t:{{"{upper_term[:-1]}"*}}'),
            (f'@t:{{"{lower_term[:-1]}"*}}'),
            # infix
            (f'@t:{{*"{upper_term[1:-1]}"*}}'),
            (f'@t:{{*"{lower_term[1:-1]}"*}}'),
            # suffix
            (f'@t:{{*"{upper_term[1:]}"}}'),
            (f'@t:{{*"{lower_term[1:]}"}}'),
        ]

        for query in queries:
            expected = [2, f'doc:u:{codepoint:04X}', f'doc:l:{codepoint:04X}']

            # Test query results
            res = conn.execute_command('FT.SEARCH', idx, query, 'NOCONTENT', 'DIALECT', 2)
            env.assertEqual(res, expected)

        env.cmd('DEL', f'doc:u:{codepoint:04X}')
        env.cmd('DEL', f'doc:l:{codepoint:04X}')

@skip(cluster=True)
def testTextToLowerConversionSimilarMatch(env):
    '''Test that tolower conversion works correctly for all unicode characters
    when using TEXT fields and running a query with a prefix, infix or suffix.
    This test skips characters that libnu does not fold (see LIBNU_FOLD_GAPS).
    It also skips lowercase characters, because the tolower conversion
    is not expected to change them.
    The test creates a document with a term that contains a single unicode
    character, and then searches for the term in both upper and lower case
    '''

    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx_txt', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')

    idx = 'idx_txt'
    for codepoint in range(0x110000):  # Unicode range from U+0000 to U+10FFFF
        # Skip unsupported codepoints:
        if codepoint in LIBNU_FOLD_GAPS:
            continue

        char = chr(codepoint)
        lower_char = char.lower()
        if char == lower_char:
            # If the character is already lowercase, skip it
            continue

        upper_term = char * 5
        lower_term = lower_char * 5
        env.cmd('HSET', f'doc:u:{codepoint:04X}', 't', upper_term)
        env.cmd('HSET', f'doc:l:{codepoint:04X}', 't', lower_term)

        queries = [
            # query, doc_id
            (f'@t:({upper_term[:-1]}*)'),
            (f'@t:({lower_term[:-1]}*)'),
            (f'@t:(*{upper_term[1:-1]}*)'),
            (f'@t:(*{lower_term[1:-1]}*)'),
            (f'@t:(*{upper_term[1:]})'),
            (f'@t:(*{lower_term[1:]})'),
        ]

        for query in queries:
            # Both the upper- and lower-case documents are expected to match,
            # regardless of the lowercase character's UTF-8 length.
            #
            # NOTE: this diverges from the legacy C terms trie, which keyed on
            # 16-bit `rune`s and bit-truncated any astral codepoint (>U+FFFF,
            # i.e. a 4-byte lowercase form) down into the BMP at index time.
            # Truncated terms still matched an exact lookup but were unreachable
            # via the prefix/infix/suffix queries used here, so the old
            # expectation for 4-byte lowercase characters was `[0]`. The Rust
            # `TermDictionary` port keys on UTF-8 and preserves astral
            # codepoints, so those terms are now searchable and match at `[2]`
            # like every other codepoint.
            expected = [2, f'doc:u:{codepoint:04X}', f'doc:l:{codepoint:04X}']

            # Test query results
            res = conn.execute_command('FT.SEARCH', idx, query, 'NOCONTENT')
            env.assertEqual(res, expected)

        env.cmd('DEL', f'doc:u:{codepoint:04X}')
        env.cmd('DEL', f'doc:l:{codepoint:04X}')
