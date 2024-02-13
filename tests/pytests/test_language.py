# -*- coding: utf-8 -*-

from common import getConnectionByEnv, waitForIndex
from RLTest import Env

def testSearchIndexLanguage(env):
    conn = getConnectionByEnv(env)

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'word:',
            'LANGUAGE', 'italian', 'SCHEMA', 'word', 'TEXT')
    
    # Create sample data in Italian
    conn.execute_command('HSET', 'word:1', 'word', 'arancia') # orange
    conn.execute_command('HSET', 'word:2', 'word', 'arance') # oranges

    # Create sample data in English
    conn.execute_command('HSET', 'word:3', 'word', 'orange') # orange
    conn.execute_command('HSET', 'word:4', 'word', 'oranges') # oranges

    # Wait for index to be created
    waitForIndex(env, 'idx')

    # Search for "arancia", should use the language by default: Italian
    # should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for orange in English
    # should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'ASC')
    expected = [2, 'word:3', ['word', 'orange'], 'word:4', ['word', 'oranges']]
    env.assertEqual(res, expected)

    # Search for "orange" in Italian
    # should return 1 results using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'italian')
    expected = [1, 'word:4', ['word', 'oranges']]
    env.assertEqual(res, expected)

    # drop index
    env.cmd('FT.DROP', 'idx')

def testSearchIndexLanguageField(env):
    conn = getConnectionByEnv(env)

    # Create index - language English by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'word:',
            'LANGUAGE_FIELD', '__lang', 'SCHEMA', 'word', 'TEXT', '__lang', 'TEXT')
    
    # Create sample data in Italian
    conn.execute_command('HSET', 'word:1', 'word', 'arancia', '__lang', 'italian') # orange
    conn.execute_command('HSET', 'word:2', 'word', 'arance', '__lang', 'italian') # oranges

    # Create sample data in English
    conn.execute_command('HSET', 'word:3', 'word', 'orange', '__lang', 'english') # orange
    conn.execute_command('HSET', 'word:4', 'word', 'oranges', '__lang', 'english') # oranges

    # Wait for index to be created
    waitForIndex(env, 'idx')

    # Search for "arancia", should use the field language: Italian
    # should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "arance", should use the field language: Italian
    # should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arance', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "oranges", should use the field language: English
    # should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'SORTBY', 'word', 'ASC')
    expected = [2, 'word:3', ['word', 'orange'], 'word:4', ['word', 'oranges']]
    env.assertEqual(res, expected)
