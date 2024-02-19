# -*- coding: utf-8 -*-

from common import getConnectionByEnv, waitForIndex
from RLTest import Env

def testSearchHashIndexLanguage(env):

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

    # Search for "arancia" using English
    # should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'language', 'english')
    env.assertEqual(res, [1, 'word:1', ['word', 'arancia']])

    # Search for "oranges" using English
    # should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'ASC')
    expected = [2, 'word:3', ['word', 'orange'], 'word:4', ['word', 'oranges']]
    env.assertEqual(res, expected)

    # Search for "orange" in Italian
    # should return 1 results using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'italian')
    env.assertEqual(res, [1, 'word:4', ['word', 'oranges']])

    # drop index
    env.cmd('FT.DROP', 'idx')


def testSearchJsonIndexLanguage(env):

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON',
            'LANGUAGE', 'italian', 'SCHEMA', '$.word', 'AS', 'word', 'TEXT')
    
    # Create sample data in Italian
    env.cmd('JSON.SET', 'word:1', '$', '{"word":"arancia"}') # orange
    env.cmd('JSON.SET', 'word:2', '$', '{"word":"arance"}') # oranges

    # Create sample data in English
    env.cmd('JSON.SET', 'word:3', '$', '{"word":"orange"}') # orange
    env.cmd('JSON.SET', 'word:4', '$', '{"word":"oranges"}') # oranges

    # Wait for index to be created
    waitForIndex(env, 'idx')

    # Search for "arancia", should use the language by default: Italian
    # It should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC', 
                  'NOCONTENT')
    env.assertEqual(res, [2, 'word:1', 'word:2'])

    # Search for "arancia" using English
    # It should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'language', 'english')
    env.assertEqual(res, [1, 'word:1', ['$', '{"word":"arancia"}']])

    # Search for "oranges" using English
    # It should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'english',
                  'SORTBY', 'word', 'ASC', 'NOCONTENT')
    env.assertEqual(res, [2, 'word:3', 'word:4'])

    # Search for "orange" using Italian
    # It should return 1 results using invalid stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'language', 'italian')
    env.assertEqual(res, [1, 'word:4', ['$', '{"word":"oranges"}']])

    # drop index
    env.cmd('FT.DROP', 'idx')

def testSearchIndexLanguageField(env):
    
    # Create sample data in Italian
    env.cmd('HSET', 'word:1', 'word', 'arancia', '__lang', 'italian') # orange
    env.cmd('HSET', 'word:2', 'word', 'arance', '__lang', 'italian') # oranges
    env.cmd('HSET', 'word:3', 'word', 'mela', '__lang', 'italian') # apple
    env.cmd('HSET', 'word:4', 'word', 'mele', '__lang', 'italian') # apples
    env.cmd('HSET', 'word:5', 'word', 'orange', '__lang', 'english')
    env.cmd('HSET', 'word:6', 'word', 'oranges', '__lang', 'english')

    # Create index - language Italian by default
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'word:',
            'LANGUAGE', 'italian', 'LANGUAGE_FIELD', '__lang',
            'SCHEMA', 'word', 'TEXT', '__lang', 'TEXT')

    # Wait for index to be created
    waitForIndex(env, 'idx')

    # Search for "arancia", without passing language argument, should use 
    # language Italian, because it is part of the index schema.
    # It should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "arance", without passing language argument, should use 
    # language Italian, because it is part of the index schema
    # it should return 2 results using stemming in Italian
    res = env.cmd('FT.SEARCH', 'idx', 'arance', 'SORTBY', 'word', 'DESC')
    expected = [2, 'word:1', ['word', 'arancia'], 'word:2', ['word', 'arance']]
    env.assertEqual(res, expected)

    # Search for "arancia", passing language argument will override the language in the schema
    # It should return 1 results using invalid stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'arancia', 'SORTBY', 'word', 'DESC', 'LANGUAGE', 'english')
    expected = [1, 'word:1', ['word', 'arancia']]
    env.assertEqual(res, expected)

    # Search for "oranges", passing language argument will override the language in the schema
    # It should return 2 results using stemming in English
    res = env.cmd('FT.SEARCH', 'idx', 'oranges', 'SORTBY', 'word', 'ASC', 'LANGUAGE', 'english')
    expected = [2, 'word:5', ['word', 'orange'], 'word:6', ['word', 'oranges']]
    env.assertEqual(res, expected)

