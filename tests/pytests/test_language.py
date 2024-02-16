# -*- coding: utf-8 -*-

from common import getConnectionByEnv, waitForIndex
from RLTest import Env

def testSearchIndexLanguage(env):
    conn = getConnectionByEnv(env)

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'word:',
            'LANGUAGE', 'italian', 'SCHEMA', 'word', 'TEXT')
    
    # Create sample data in Italian
    env.cmd('HSET', 'word:1', 'word', 'arancia') # orange
    env.cmd('HSET', 'word:2', 'word', 'arance') # oranges

    # Create sample data in English
    env.cmd('HSET', 'word:3', 'word', 'orange') # orange
    env.cmd('HSET', 'word:4', 'word', 'oranges') # oranges

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
    
    # Create sample data in Italian
    env.cmd('HSET', 'word:1', 'word', 'arancia', '__lang', 'italian') # orange
    env.cmd('HSET', 'word:2', 'word', 'arance', '__lang', 'italian') # oranges
    env.cmd('HSET', 'word:3', 'word', 'mela', '__lang', 'italian') # apple
    env.cmd('HSET', 'word:4', 'word', 'ananas', '__lang', 'italian') # pineapple

    # Create index - language English by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'word:',
            'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', '__lang', 'TEXT')

    # Wait for index to be created
    waitForIndex(env, 'idx')

    # Search for "arancia", should use the field language Italian because all 
    # the values in __lang field are Italian
    # it should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "arance", should use the field language: Italian, 
    # it should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arance', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Create sample data in English
    env.cmd('HSET', 'word:5', 'word', 'orange', '__lang', 'english') # orange
    env.cmd('HSET', 'word:6', 'word', 'oranges', '__lang', 'english') # oranges

    # Search for "arancia", should use the field language: English
    # because there are different values in __lang field, it should return 
    # 1 result
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [1, 'word:1', ['word', 'arancia']]
    env.assertEqual(res, expected)
    
    # Search for "oranges", should use the field language: English
    # it should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'SORTBY', 'word', 'ASC')
    expected = [2, 'word:5', ['word', 'orange'], 'word:6', ['word', 'oranges']]
    env.assertEqual(res, expected)

    # TODO:
    # After deleting English keys, language should be Italian again
    env.expect('HDEL', 'word:5', '__lang')
    env.expect('HDEL', 'word:6', '__lang')
    # Search for "arancia", should use the field language: Italian, 
    # it should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)
