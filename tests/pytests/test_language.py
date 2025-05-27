# -*- coding: utf-8 -*-

from common import getConnectionByEnv, waitForIndex, config_cmd, skip
from RLTest import Env
from common import index_info
import time

def testHashIndexLanguage(env):
    conn = getConnectionByEnv(env)

    # Create sample data in Italian
    conn.execute_command('HSET', '{word}:1', 'word', 'arancia') # orange
    conn.execute_command('HSET', '{word}:2', 'word', 'arance') # oranges

    # Create sample data in English
    conn.execute_command('HSET', '{word}:3', 'word', 'orange')
    conn.execute_command('HSET', '{word}:4', 'word', 'oranges')
    conn.execute_command('HSET', '{word}:5', 'word', 'cherry')
    conn.execute_command('HSET', '{word}:6', 'word', 'cherries')

    # Create index - language Italian per index
    env.cmd('FT.CREATE', 'idx_it', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'italian', 'SCHEMA', 'word', 'TEXT')
    # Wait for index to be created
    waitForIndex(env, 'idx_it')
    
    # Search for "arancia/arance", without passing language argument, should use
    # the language by default: Italian
    # It should return 2 results using stemming in Italian
    expected = [2, '{word}:1', ['word', 'arancia'], '{word}:2', ['word', 'arance']]
    res = env.cmd('FT.SEARCH', 'idx_it', 'arancia', 'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_it', 'arance', 'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)

    # Search for "arancia" using English
    # This returns 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx_it', 'arancia', 'language', 'english')
    env.assertEqual(res, [1, '{word}:1', ['word', 'arancia']])

    # Search for English words using language English in an Italian index
    # This returns invalid results because the stemmer used during data 
    # ingestion was Italian but the words are in English
    res = env.cmd('FT.SEARCH', 'idx_it', 'cherry', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, [1, '{word}:5', ['word', 'cherry']])

    res = env.cmd('FT.SEARCH', 'idx_it', 'cherries', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, [1, '{word}:6', ['word', 'cherries']])

    res = env.cmd('FT.SEARCH', 'idx_it', 'orange', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, [1, '{word}:3', ['word', 'orange']])

    res = env.cmd('FT.SEARCH', 'idx_it', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, [2, '{word}:4', ['word', 'oranges'], 
                          '{word}:3', ['word', 'orange']])

    # Create index - language English per index (by default)
    env.cmd('FT.CREATE', 'idx_en', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'SCHEMA', 'word', 'TEXT')
    # Wait for index to be created
    waitForIndex(env, 'idx_en')

    # Search for "orange/oranges" using English
    # should return 2 results using stemming in English
    expected = [2, '{word}:3', ['word', 'orange'], '{word}:4', ['word', 'oranges']]
    res = env.cmd('FT.SEARCH', 'idx_en', 'orange', 'language', 'english',
                  'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_en', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_en', 'orange', 'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_en', 'oranges', 'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)

    # Search using an unsupported language is not allowed
    env.expect('FT.SEARCH', 'idx_it', 'oranges', 'language', 'any_language').error()

    # Creating an index with an unsupported language is not allowed
    env.expect('FT.CREATE', 'idx_xx', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'any_invalid_language', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', ).error()

@skip(no_json=True)
def testJsonIndexLanguage(env):
    conn = getConnectionByEnv(env)

    # Create sample data in Italian
    conn.execute_command('JSON.SET', '{word}:1', '$', '{"word":"arancia"}') # orange
    conn.execute_command('JSON.SET', '{word}:2', '$', '{"word":"arance"}') # oranges

    # Create sample data in English
    conn.execute_command('JSON.SET', '{word}:3', '$', '{"word":"orange"}')
    conn.execute_command('JSON.SET', '{word}:4', '$', '{"word":"oranges"}')
    conn.execute_command('JSON.SET', '{word}:5', '$', '{"word":"cherry"}')
    conn.execute_command('JSON.SET', '{word}:6', '$', '{"word":"cherries"}')

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx_it', 'ON', 'JSON',
            'LANGUAGE', 'italian', 'SCHEMA', '$.word', 'AS', 'word', 'TEXT')
    # Wait for index to be created
    waitForIndex(env, 'idx_it')

    # Search for "arancia/arance", should use the language by default: Italian
    # It should return 2 results using stemming in Italian
    expected = [2, '{word}:1', ['word', 'arancia', '$', '{"word":"arancia"}'], 
                '{word}:2', ['word', 'arance', '$', '{"word":"arance"}']]
    for word in ['arancia', 'arance']:
        res = env.cmd('FT.SEARCH', 'idx_it', word, 'SORTBY', 'word', 'DESC')
        env.assertEqual(res, expected)

    # Search for "arancia" using English
    # It should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx_it', 'arancia', 'language', 'english')
    env.assertEqual(res, [1, '{word}:1', ['$', '{"word":"arancia"}']])

    # Search for English words using language English in an Italian index
    # This returns invalid results because the stemmer used during data 
    # ingestion was Italian but the words are in English
    expected = [1, '{word}:5', ['word', 'cherry', '$', '{"word":"cherry"}']]
    res = env.cmd('FT.SEARCH', 'idx_it', 'cherry', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)

    expected = [1, '{word}:6', ['word', 'cherries', '$', '{"word":"cherries"}']]
    res = env.cmd('FT.SEARCH', 'idx_it', 'cherries', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)

    expected = [1, '{word}:3', ['word', 'orange', '$', '{"word":"orange"}']]
    res = env.cmd('FT.SEARCH', 'idx_it', 'orange', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)

    expected = [2, '{word}:4', ['word', 'oranges', '$', '{"word":"oranges"}'], 
                '{word}:3', ['word', 'orange', '$', '{"word":"orange"}']]
    res = env.cmd('FT.SEARCH', 'idx_it', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)

    # Create index - language English per index (by default)
    env.cmd('FT.CREATE', 'idx_en', 'ON', 'JSON',
            'SCHEMA', '$.word', 'AS', 'word', 'TEXT')
    # Wait for index to be created
    waitForIndex(env, 'idx_en')

    # Search for "cherry/cherries" using English
    # should return 2 results using stemming in English
    expected = [2, '{word}:6', ['word', 'cherries', '$', '{"word":"cherries"}'],
                '{word}:5', ['word', 'cherry', '$', '{"word":"cherry"}']]
    for word in ['cherry', 'cherries']:
        res = env.cmd('FT.SEARCH', 'idx_en', word, 'language', 'english',
                  'SORTBY', 'word', 'ASC')
        env.assertEqual(res, expected)
        res = env.cmd('FT.SEARCH', 'idx_en', word, 'SORTBY', 'word', 'ASC')
        env.assertEqual(res, expected)

    # Search for "orange" using Italian
    # It returns 1 results using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx_it', 'oranges', 'language', 'italian')
    env.assertEqual(res, [1, '{word}:4', ['$', '{"word":"oranges"}']])

    # Creating an index with an unsupported language is not allowed
    env.expect('FT.CREATE', 'idx3', 'ON', 'JSON', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'any_invalid_language',
            'SCHEMA', 'word', 'TEXT', ).error()
    env.expect('FT.CREATE', 'idx3', 'ON', 'JSON', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'any_invalid_language', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', ).error()

def testHashIndexLanguageField(env):
    conn = getConnectionByEnv(env)
    
    # Create sample data in Italian
    conn.execute_command('HSET', '{word}:1', 'word', 'arancia',
                         '__lang', 'italian') # oranges
    conn.execute_command('HSET', '{word}:2', 'word', 'arance',
                         '__lang', 'italian') # oranges
    conn.execute_command('HSET', '{word}:3', 'word', 'ciliegia') # cherry
    conn.execute_command('HSET', '{word}:4', 'word', 'ciliegie') # cherries
    conn.execute_command('HSET', '{word}:5', 'word', 'orange',
                         '__lang', 'english')
    conn.execute_command('HSET', '{word}:6', 'word', 'oranges',
                         '__lang', 'english')
    conn.execute_command('HSET', '{word}:7', 'word', 'cherry',
                         '__lang', 'english')
    conn.execute_command('HSET', '{word}:8', 'word', 'cherries',
                         '__lang', 'english')
    conn.execute_command('HSET', '{word}:9', 'word', 'xaranja',
                         '__lang', 'unexistent_language')
    conn.execute_command('HSET', '{word}:10', 'word', 'fragola',
                         '__lang', 'italian') # strawberry

    ############################################################################
    # Test with LANGUAGE per index and LANGUAGE_FIELD
    ############################################################################
    # Create index - language per index: Italian
    env.cmd('FT.CREATE', 'idx_it', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'italian', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', )
    # Wait for index to be created
    waitForIndex(env, 'idx_it')

    # Search for "arancia/arance", without passing language argument, should use 
    # language Italian, because it is part of the index schema.
    # It should return 2 results using stemming in Italian
    expected = [2, '{word}:1', ['word', 'arancia'], '{word}:2', ['word', 'arance']]
    for word in ['arancia', 'arance']:
        res = env.cmd('FT.SEARCH', 'idx_it', word, 'SORTBY', 'word', 'DESC')
        env.assertEqual(res, expected)

    # Search for "arancia", passing language argument will override the language
    # per index
    # It should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx_it', 'arancia', 'SORTBY', 'word', 'DESC',
                  'LANGUAGE', 'english')
    expected = [1, '{word}:1', ['word', 'arancia']]
    env.assertEqual(res, expected)

    # Search for "cherry/cherries", passing language argument will override
    # the language per index
    # It should return 2 results using stemming in English
    expected = [2, '{word}:7', ['word', 'cherry'], '{word}:8', ['word', 'cherries']]
    for word in ['cherry', 'cherries']:
        res = env.cmd('FT.SEARCH', 'idx_it', word, 'SORTBY', 'word', 'DESC',
                    'LANGUAGE', 'english')
        env.assertEqual(res, expected)

    # Search for "cherry", without passing language argument should use the 
    # default language of the index
    # It should return 1 result using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx_it', 'cherry', 'SORTBY', 'word', 'ASC')
    expected = [1, '{word}:7', ['word', 'cherry']]
    env.assertEqual(res, expected)

    # Search for "orange", without passing language argument should use the 
    # default language of the index: Italian
    # But in this case, the stemming in Italian, generates matching words
    # in English
    res = env.cmd('FT.SEARCH', 'idx_it', 'orange', 'SORTBY', 'word', 'ASC')
    expected = [2, '{word}:5', ['word', 'orange'], '{word}:6', ['word', 'oranges']]
    env.assertEqual(res, expected)

    # Search for "ciliegia", the document was created without __lang value
    # but during the data ingestion the terms were created using the index 
    # language: Italian
    expected = [2, '{word}:3', ['word', 'ciliegia'], '{word}:4', ['word', 'ciliegie']]
    for word in ['ciliegia', 'ciliegie']:
        res = env.cmd('FT.SEARCH', 'idx_it', word, 'SORTBY', 'word', 'ASC')
        env.assertEqual(res, expected)

    ############################################################################
    # Test index with language per index: Default
    ############################################################################
    # Create index without language per index
    env.cmd('FT.CREATE', 'idx_en', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE_FIELD', '__lang', 'SCHEMA', 'word', 'TEXT', )
    # Wait for index to be created
    waitForIndex(env, 'idx_en')

    # Search for "cherry/cherries", without passing language argument, should
    # use the default language: English
    # It should return 2 results using stemming in English
    expected = [2, '{word}:7', ['word', 'cherry'], '{word}:8', ['word', 'cherries']]
    for word in ['cherry', 'cherries']:
        res = env.cmd('FT.SEARCH', 'idx_en', word, 'SORTBY', 'word', 'DESC')
        env.assertEqual(res, expected)

    # Search for "arance/arancia", "ciliegia/ciliegie", without passing language
    # argument, should use language by default: English
    # To validate this, we check that we have the same results in both cases:
    # 1. passing the language argument = English
    # 2. searching without language argument
    for word in ['arancia', 'arance', 'ciliegia', 'ciliegie']:
        res1 = env.cmd('FT.SEARCH', 'idx_en', word, 'SORTBY', 'word', 'DESC')
        res2 = env.cmd('FT.SEARCH', 'idx_en', word, 'SORTBY', 'word', 'DESC',
                        'LANGUAGE', 'english')
        env.assertEqual(res1, res2)

    ############################################################################
    # Tests indexing the language field, __lang is part of the schema
    ############################################################################
    # Index with language field in the schema and language per Index: Italian
    env.cmd('FT.CREATE', 'idx_lang', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'italian', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', '__lang', 'TAG')
    # Wait for index to be created
    waitForIndex(env, 'idx_lang')

    # Index with language field in the schema and language per Index: Default
    env.cmd('FT.CREATE', 'idx_def_lang', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', '__lang', 'TAG')
    # Wait for index to be created
    waitForIndex(env, 'idx_def_lang')

    for idx in ['idx_lang', 'idx_def_lang']:
        # Search words in Italian - only Italian words should be returned
        res = env.cmd('FT.SEARCH', idx, '@__lang:{Italian}',
                    'SORTBY', 'word')
        env.assertEqual(res, [3, '{word}:2', ['word', 'arance'],
                            '{word}:1', ['word', 'arancia'],
                            '{word}:10', ['word', 'fragola']])
        
        res = env.cmd('FT.SEARCH', idx, '@__lang:{Italian} arance', 
                    'SORTBY', 'word', 'ASC')
        env.assertEqual(res, [2, '{word}:2', ['word', 'arance'],
                            '{word}:1', ['word', 'arancia']])

        # Search words in English - only English words should be returned
        res = env.cmd('FT.SEARCH', idx, '@__lang:{english}',
                    'SORTBY', 'word')
        env.assertEqual(res, [4, '{word}:8', ['word', 'cherries'],
                                '{word}:7', ['word', 'cherry'],
                                '{word}:5', ['word', 'orange'],
                                '{word}:6', ['word', 'oranges']])

        # Search words without any language
        res = env.cmd('FT.SEARCH', idx,
                    '-(@__lang:{english} | @__lang:{italian})',
                    'SORTBY', 'word', 'DIALECT', 2)
        env.assertEqual(res, [3, '{word}:3', ['word', 'ciliegia'],
                            '{word}:4', ['word', 'ciliegie'],
                            '{word}:9', ['word', 'xaranja']])
        # TODO: Bug MOD-6886 - This is an equivalent query to the previous one,
        # but it fails and returns some documents in English and Italian 
        # if RAW_DOCID_ENCODING is true
        raw_encoding = env.cmd(config_cmd(), 'GET', 'RAW_DOCID_ENCODING')
        if raw_encoding == 'false':
                res2 = env.cmd('FT.search', idx,
                        '-(@__lang:{english}) -(@__lang:{italian})',
                        'SORTBY', 'word', 'DIALECT', 2)
                env.assertEqual(res2, res)

    ############################################################################
    # Test that if no language field is defined by the index, if a hash has a 
    # __lang value it should be used as the document language
    ############################################################################
    # Create index - language per index: Italian, without language field
    env.cmd('FT.CREATE', 'idx_it_no_lang_field', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'italian',
            'SCHEMA', 'word', 'TEXT', )
    # Wait for index to be created
    waitForIndex(env, 'idx_it_no_lang_field')

    # The results should be the same as the index with the language field
    for word in ['arancia', 'arance', 'ciliegia', 'ciliegie', 'cherry', 'cherries']:
        res1 = env.cmd('FT.SEARCH', 'idx_it', word, 'NOCONTENT',
                      'SORTBY', 'word', 'DESC')
        res2 = env.cmd('FT.SEARCH', 'idx_it_no_lang_field', word, 'NOCONTENT',
                      'SORTBY', 'word', 'DESC')
        env.assertEqual(res2, res1)

@skip(no_json=True)
def testJsonIndexLanguageField(env):
    conn = getConnectionByEnv(env)

    # Create sample data in Italian
    conn.execute_command('JSON.SET', '{word}:1', '$',
                         r'{"word":"arancia", "__lang": "italian"}') # orange
    conn.execute_command('JSON.SET', '{word}:2', '$',
                         r'{"word":"arance", "__lang": "italian"}') # oranges
    conn.execute_command('JSON.SET', '{word}:3', '$',
                         r'{"word":"ciliegia"}') # cherry
    conn.execute_command('JSON.SET', '{word}:4', '$',
                         r'{"word":"ciliegie"}') # cherries
    conn.execute_command('JSON.SET', '{word}:5', '$',
                         r'{"word":"orange", "__lang": "english"}')
    conn.execute_command('JSON.SET', '{word}:6', '$',
                         r'{"word":"oranges", "__lang": "english"}')
    conn.execute_command('JSON.SET', '{word}:7', '$',
                         r'{"word":"cherry", "__lang": "english"}')
    conn.execute_command('JSON.SET', '{word}:8', '$',
                         r'{"word":"cherries", "__lang": "english"}')
    conn.execute_command('JSON.SET', '{word}:9', '$',
                         r'{"word":"xaranja", "__lang": "unexistent_language"}')
    conn.execute_command('JSON.SET', '{word}:10', '$',
                         r'{"word":"fragola", "__lang": "italian"}') # strawberry

    ############################################################################
    # Test with LANGUAGE per index and LANGUAGE_FIELD
    ############################################################################
    # Create index - language per index: Italian and language field
    env.cmd('FT.CREATE', 'idx_it', 'ON', 'JSON',
            'LANGUAGE', 'italian', 'LANGUAGE_FIELD', '$.__lang',
            'SCHEMA', '$.word', 'AS', 'word', 'TEXT')
    # Wait for index to be created
    waitForIndex(env, 'idx_it')

    # Search for "arancia/arance", without passing language argument, should use 
    # language Italian, because it is part of the index schema.
    # It should return 2 results using stemming in Italian
    expected = [2, '{word}:1', '{word}:2']
    for word in ['arancia', 'arance']:
        res = env.cmd('FT.SEARCH', 'idx_it', word,
                      'SORTBY', 'word', 'DESC', 'NOCONTENT')
        env.assertEqual(res, expected)

    # Search for "arancia", passing language argument will override the language
    # per index
    # It should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx_it', 'arancia', 'SORTBY', 'word', 'DESC',
                  'LANGUAGE', 'english', 'NOCONTENT')
    expected = [1, '{word}:1']
    env.assertEqual(res, expected)

    # Search for "cherry/cherries", passing language argument will override
    # the language per index
    # It should return 2 results using stemming in English
    expected = [2, '{word}:7', '{word}:8']
    for word in ['cherry', 'cherries']:
        res = env.cmd('FT.SEARCH', 'idx_it', word, 'SORTBY', 'word', 'DESC',
                    'LANGUAGE', 'english', 'NOCONTENT')
        env.assertEqual(res, expected)

    # Search for "cherry", without passing language argument should use the 
    # default language of the index
    # It should return 1 result using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx_it', 'cherry', 'SORTBY', 'word', 'ASC',
                  'NOCONTENT')
    expected = [1, '{word}:7']
    env.assertEqual(res, expected)

    # Search for "orange", without passing language argument should use the 
    # default language of the index: Italian
    # But in this case, the stemming in Italian, generates matching words
    # in English
    res = env.cmd('FT.SEARCH', 'idx_it', 'orange', 'SORTBY', 'word', 'ASC',
                  'NOCONTENT')
    expected = [2, '{word}:5', '{word}:6']
    env.assertEqual(res, expected)

    # Search for "ciliegia", the document was created without __lang value
    # but during the data ingestion the terms were created using the index 
    # language: Italian
    expected = [2, '{word}:3', '{word}:4']
    for word in ['ciliegia', 'ciliegie']:
        res = env.cmd('FT.SEARCH', 'idx_it', word, 'SORTBY', 'word', 'ASC',
                      'NOCONTENT')
        env.assertEqual(res, expected)

    ############################################################################
    # Test index with language per index: Default = English
    ############################################################################
    # Create index without language per index
    env.cmd('FT.CREATE', 'idx_en', 'ON', 'JSON',
            'LANGUAGE_FIELD', '__lang',
            'SCHEMA', '$.word', 'AS', 'word', 'TEXT')
    # Wait for index to be created
    waitForIndex(env, 'idx_en')

    # Search for "cherry/cherries", without passing language argument, should
    # use the default language: English
    # It should return 2 results using stemming in English
    expected = [2, '{word}:7', '{word}:8']
    for word in ['cherry', 'cherries']:
        res = env.cmd('FT.SEARCH', 'idx_en', word, 'SORTBY', 'word', 'DESC',
                      'NOCONTENT')
        env.assertEqual(res, expected)

    # Search for "arance/arancia", "ciliegia/ciliegie", without passing language
    # argument, should use language by default: English
    # To validate this, we check that we have the same results in both cases:
    # 1. passing the language argument = English
    # 2. searching without language argument
    for word in ['arancia', 'arance', 'ciliegia', 'ciliegie']:
        res1 = env.cmd('FT.SEARCH', 'idx_en', word, 'SORTBY', 'word', 'DESC')
        res2 = env.cmd('FT.SEARCH', 'idx_en', word, 'SORTBY', 'word', 'DESC',
                        'LANGUAGE', 'english')
        env.assertEqual(res1, res2)

    ############################################################################
    # Tests indexing the language field, __lang is part of the schema
    ############################################################################
    # Index with language field in the schema and language per Index: Italian
    env.cmd('FT.CREATE', 'idx_lang', 'ON', 'JSON',
            'LANGUAGE', 'italian', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA',
            '$.word', 'AS', 'word', 'TEXT',
            '$.__lang', 'AS', '__lang', 'TAG')
    # Wait for index to be created
    waitForIndex(env, 'idx_lang')

    # Index with language field in the schema and language per Index: Default
    env.cmd('FT.CREATE', 'idx_def_lang', 'ON', 'JSON',
            'LANGUAGE_FIELD', '$.__lang',
            'SCHEMA',
            '$.word', 'AS', 'word', 'TEXT',
            '$.__lang', 'AS', '__lang', 'TAG')
    # Wait for index to be created
    waitForIndex(env, 'idx_def_lang')

    for idx in ['idx_lang', 'idx_def_lang']:
        # Search words in Italian - only Italian words should be returned
        res = env.cmd('FT.SEARCH', idx, '@__lang:{Italian}',
                    'SORTBY', 'word', 'NOCONTENT')
        env.assertEqual(res, [3, '{word}:2', '{word}:1', '{word}:10'])
        
        res = env.cmd('FT.SEARCH', idx, '@__lang:{Italian} arance',
                    'SORTBY', 'word', 'ASC', 'NOCONTENT')
        env.assertEqual(res, [2, '{word}:2','{word}:1'])

        # Search words in English - only English words should be returned
        res = env.cmd('FT.SEARCH', idx, '@__lang:{english}',
                    'SORTBY', 'word', 'NOCONTENT')
        env.assertEqual(res, [4, '{word}:8', '{word}:7', '{word}:5', '{word}:6'])

        # Search words without any language
        res = env.cmd('FT.SEARCH', idx,
                    '-(@__lang:{english} | @__lang:{italian})',
                    'SORTBY', 'word', 'NOCONTENT', 'DIALECT', 2)
        env.assertEqual(res, [3, '{word}:3', '{word}:4', '{word}:9'])
        # TODO: Bug MOD-6886 - This is an equivalent query to the previous one,
        # but it fails and returns some documents in English and Italian 
        # if RAW_DOCID_ENCODING is true
        raw_encoding = env.cmd(config_cmd(), 'GET', 'RAW_DOCID_ENCODING')
        if raw_encoding == 'false':
                res2 = env.cmd('FT.SEARCH', idx,
                        '-(@__lang:{english}) -(@__lang:{italian})',
                        'SORTBY', 'word', 'NOCONTENT', 'DIALECT', 2)
                env.assertEqual(res2, res)

    ############################################################################
    # Test that if no language field is defined by the index, if a hash has a 
    # __lang value it should be used as the document language
    ############################################################################
    # Create index - language per index: Italian, without language field
    env.cmd('FT.CREATE', 'idx_it_no_lang_field', 'ON', 'JSON',
            'LANGUAGE', 'italian',
            'SCHEMA', '$.word', 'AS', 'word', 'TEXT')
    # Wait for index to be created
    waitForIndex(env, 'idx_it_no_lang_field')

    # The results should be the same as the index with the language field
    for word in ['arancia', 'arance', 'ciliegia', 'ciliegie', 'cherry', 'cherries']:
        res1 = env.cmd('FT.SEARCH', 'idx_it', word, 'NOCONTENT',
                      'SORTBY', 'word', 'DESC')
        res2 = env.cmd('FT.SEARCH', 'idx_it_no_lang_field', word, 'NOCONTENT',
                      'SORTBY', 'word', 'DESC')
        env.assertEqual(res2, res1)

def testLanguageInfo(env):
    languages = ['arabic', 'armenian', 'basque', 'catalan', 'danish', 'dutch',
                 'finnish', 'french', 'german', 'greek', 'hindi', 'hungarian',
                 'indonesian', 'irish', 'italian', 'lithuanian', 'nepali',
                 'norwegian', 'portuguese', 'romanian', 'russian', 'serbian',
                 'spanish', 'swedish', 'tamil', 'turkish', 'yiddish', 'chinese']
    # 'english' is not printed in FT.INFO because it is the default language
    for language in languages:
        env.cmd('FT.CREATE', 'idx_' + language, 'LANGUAGE', language,
                'SCHEMA', 'text', 'TEXT')
        info = index_info(env, 'idx_' + language)
        index_definition = info['index_definition']
        idx = {index_definition[i]: index_definition[i + 1] for i in range(0, len(index_definition), 2)}
        env.assertEqual(idx['default_language'], language)
