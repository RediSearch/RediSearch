# -*- coding: utf-8 -*-

from common import getConnectionByEnv, waitForIndex
from RLTest import Env
import time

def testSearchHashIndexLanguage(env):
    conn = getConnectionByEnv(env)

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'italian', 'SCHEMA', 'word', 'TEXT')
    
    # Wait for index to be created
    waitForIndex(env, 'idx')
    
    # Create sample data in Italian
    conn.execute_command('HSET', '{word}:1', 'word', 'arancia') # orange
    conn.execute_command('HSET', '{word}:2', 'word', 'arance') # oranges

    # Create sample data in English
    conn.execute_command('HSET', '{word}:3', 'word', 'orange') # orange
    conn.execute_command('HSET', '{word}:4', 'word', 'oranges') # oranges

    # Search for "arancia", should use the language by default: Italian
    # should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, '{word}:1', ['word', 'arancia'], '{word}:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "arancia" using English
    # should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'language', 'english')
    env.assertEqual(res, [1, '{word}:1', ['word', 'arancia']])

    # Search for "oranges" using English
    # should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'ASC')
    expected = [2, '{word}:3', ['word', 'orange'], '{word}:4', ['word', 'oranges']]
    env.assertEqual(res, expected)

    # Search for "orange" in Italian
    # should return 1 results using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'italian')
    env.assertEqual(res, [1, '{word}:4', ['word', 'oranges']])

def testSearchJsonIndexLanguage(env):
    conn = getConnectionByEnv(env)

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON',
            'LANGUAGE', 'italian', 'SCHEMA', '$.word', 'AS', 'word', 'TEXT')
    
    # Wait for index to be created
    waitForIndex(env, 'idx')
    
    # Create sample data in Italian
    conn.execute_command('JSON.SET', '{word}:1', '$', '{"word":"arancia"}') # orange
    conn.execute_command('JSON.SET', '{word}:2', '$', '{"word":"arance"}') # oranges

    # Create sample data in English
    conn.execute_command('JSON.SET', '{word}:3', '$', '{"word":"orange"}') # orange
    conn.execute_command('JSON.SET', '{word}:4', '$', '{"word":"oranges"}') # oranges

    # Search for "arancia", should use the language by default: Italian
    # It should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC', 
                  'NOCONTENT')
    env.assertEqual(res, [2, '{word}:1', '{word}:2'])

    # Search for "arancia" using English
    # It should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'language', 'english')
    env.assertEqual(res, [1, '{word}:1', ['$', '{"word":"arancia"}']])

    # Search for "oranges" using English
    # It should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'ASC', 'NOCONTENT')
    env.assertEqual(res, [2, '{word}:3', '{word}:4'])

    # Search for "orange" using Italian
    # It should return 1 results using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'italian')
    env.assertEqual(res, [1, '{word}:4', ['$', '{"word":"oranges"}']])

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

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE', 'italian', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', )

    # Wait for index to be created
    waitForIndex(env, 'idx')

    # Search for "arancia", without passing language argument, should use 
    # language Italian, because it is part of the index schema.
    # It should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, '{word}:1', ['word', 'arancia'], '{word}:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "arance", without passing language argument, should use 
    # language Italian, because it is part of the index schema
    # it should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arance', 'SORTBY', 'word', 'DESC')
    expected = [2, '{word}:1', ['word', 'arancia'], '{word}:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "arancia", passing language argument will override the language
    # in the schema
    # It should return 1 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC',
                  'LANGUAGE', 'english')
    expected = [1, '{word}:1', ['word', 'arancia']]
    env.assertEqual(res, expected)

    # Search for "cherry", passing language argument will override the language
    # in the schema
    # It should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'cherry', 'SORTBY', 'word', 'DESC',
                  'LANGUAGE', 'english')
    expected = [2, '{word}:7', ['word', 'cherry'], '{word}:8', ['word', 'cherries']]
    env.assertEqual(res, expected)

    # Search for "cherry", without passing language argument should use the 
    # default language of the index
    # It should return 1 result using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'cherry', 'SORTBY', 'word', 'ASC')
    expected = [1, '{word}:7', ['word', 'cherry']]
    env.assertEqual(res, expected)

    # Search for "orange", without passing language argument should use the 
    # default language of the index: Italian
    # But in this case, the stemming in Italian, generates matching words
    # in English
    res = env.cmd('FT.SEARCH', 'idx', 'orange', 'SORTBY', 'word', 'ASC')
    expected = [2, '{word}:5', ['word', 'orange'], '{word}:6', ['word', 'oranges']]
    env.assertEqual(res, expected)

    # Search for "ciliegia", the document was created without __lang value
    # but during the data ingestion the terms were created using the index 
    # language: Italian
    res = env.cmd('FT.SEARCH', 'idx', 'ciliegia', 'SORTBY', 'word', 'ASC')
    expected = [2, '{word}:3', ['word', 'ciliegia'], '{word}:4', ['word', 'ciliegie']]
    env.assertEqual(res, expected)

    # Create index without language per index
    env.cmd('FT.CREATE', 'idx2', 'ON', 'HASH', 'PREFIX', '1', '{word}:',
            'LANGUAGE_FIELD', '__lang', 'SCHEMA', 'word', 'TEXT', )

    # Wait for index to be created
    waitForIndex(env, 'idx2')

    # Search for "cherry/cherries", without passing language argument, should use 
    # language English from the document
    # It should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx2', 'cherry', 'SORTBY', 'word', 'DESC')
    expected = [2, '{word}:7', ['word', 'cherry'], '{word}:8', ['word', 'cherries']]
    env.assertEqual(res, expected)

    res = env.cmd('FT.SEARCH', 'idx2', 'cherries', 'SORTBY', 'word', 'DESC')
    expected = [2, '{word}:7', ['word', 'cherry'], '{word}:8', ['word', 'cherries']]
    env.assertEqual(res, expected)

    # Search for "ciliegia/ciliegie", the document was created without __lang value
    # there is not possible to find related words
    res = env.cmd('FT.SEARCH', 'idx2', 'ciliegia', 'SORTBY', 'word', 'ASC')
    expected = [1, '{word}:3', ['word', 'ciliegia']]
    env.assertEqual(res, expected)

    res = env.cmd('FT.SEARCH', 'idx2', 'ciliegie', 'SORTBY', 'word', 'ASC')
    expected = [1, '{word}:4', ['word', 'ciliegie']]
    env.assertEqual(res, expected)

    # Search for "arance/arancia", without passing language argument, should use
    # language Italian from the document
    # it should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx2', 'arance', 'SORTBY', 'word', 'DESC')
    expected = [2, '{word}:1', ['word', 'arancia'], '{word}:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    res = env.cmd('FT.SEARCH', 'idx2', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, '{word}:1', ['word', 'arancia'], '{word}:2', ['word', 'arance']]
    env.assertEqual(res, expected)
