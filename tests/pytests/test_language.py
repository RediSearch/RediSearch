# -*- coding: utf-8 -*-

from common import getConnectionByEnv, waitForIndex
from RLTest import Env
import time

def testSearchHashIndexLanguage(env):
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

def testSearchJsonIndexLanguage(env):
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
    res = env.cmd('FT.SEARCH', 'idx_it', 'arancia', 'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_it', 'arance', 'SORTBY', 'word', 'DESC')
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
    res = env.cmd('FT.SEARCH', 'idx_en', 'cherry', 'language', 'english',
                  'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_en', 'cherries', 'language', 'english',
                  'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_en', 'cherry', 'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_en', 'cherries', 'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)

    # Search for "orange" using Italian
    # It returns 1 results using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx_it', 'oranges', 'language', 'italian')
    env.assertEqual(res, [1, '{word}:4', ['$', '{"word":"oranges"}']])

    # Creating an index with an unsupported language is not allowed
    env.expect('FT.CREATE', 'idx3', 'ON', 'JSON', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'any_invalid_language', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', ).error()

def testSearchIndexLanguageField(env):
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
    res = env.cmd('FT.SEARCH', 'idx_it', 'arancia', 'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_it', 'arance', 'SORTBY', 'word', 'DESC')
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
    res = env.cmd('FT.SEARCH', 'idx_it', 'cherry', 'SORTBY', 'word', 'DESC',
                  'LANGUAGE', 'english')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_it', 'cherries', 'SORTBY', 'word', 'DESC',
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
    res = env.cmd('FT.SEARCH', 'idx_it', 'ciliegia', 'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_it', 'ciliegie', 'SORTBY', 'word', 'ASC')
    env.assertEqual(res, expected)

    # Create index without language per index
    env.cmd('FT.CREATE', 'idx_en', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE_FIELD', '__lang', 'SCHEMA', 'word', 'TEXT', )
    # Wait for index to be created
    waitForIndex(env, 'idx_en')

    # Search for "cherry/cherries", without passing language argument, should
    # use the default language: English
    # It should return 2 results using stemming in English
    expected = [2, '{word}:7', ['word', 'cherry'], '{word}:8', ['word', 'cherries']]
    res = env.cmd('FT.SEARCH', 'idx_en', 'cherry', 'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx_en', 'cherries', 'SORTBY', 'word', 'DESC')
    env.assertEqual(res, expected)

    # Search for "arance/arancia", "ciliegia/ciliegie", without passing language
    # argument, should use language by default: English
    # To validate this, we check that we have the same results in both cases:
    # 1. passing the language argument = English
    # 2. searching without language argument
    for words in ['arancia', 'arance', 'ciliegia', 'ciliegie']:
        res1 = env.cmd('FT.SEARCH', 'idx_en', 'arance', 'SORTBY', 'word', 'DESC')
        res2 = env.cmd('FT.SEARCH', 'idx_en', 'arance', 'SORTBY', 'word', 'DESC',
                        'LANGUAGE', 'english')
        env.assertEqual(res1, res2)

    
