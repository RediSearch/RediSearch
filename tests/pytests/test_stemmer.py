# -*- coding: utf-8 -*-

from common import waitForIndex, config_cmd, debug_cmd

def testHashMinStemLen(env):

    ########################################################
    # Test the default MIN_STEMMING_LEN (4)
    ########################################################
    # Create the index with default MIN_STEMMING_LEN (4)
    env.cmd('FT.CREATE', 'idx_min4', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    env.cmd('HSET', '{doc}:1', 't', 'fry')
    
    # 'fry' is not stemmed when MIN_STEMMING_LEN = 4
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_min4')
    env.assertEqual(res, ['fry'])

    # 'fry' is not found when MIN_STEMMING_LEN = 4
    env.cmd('HSET', '{doc}:2', 't', 'fries')
    env.cmd('HSET', '{doc}:3', 't', 'frying')
    res = env.cmd('FT.SEARCH', 'idx_min4', 'fried', 'SORTBY', 't', 'ASC')
    env.assertEqual(res, [2, '{doc}:2', ['t', 'fries'],
                          '{doc}:3', ['t', 'frying']])

    res = env.cmd(config_cmd(), 'GET', 'MINSTEMLEN')
    env.assertEqual(res[0], ['MINSTEMLEN', '4'])

    ########################################################
    # Test with MIN_STEMMING_LEN = 3
    ########################################################
    # Create the index with MIN_STEMMING_LEN = 3
    env.flush()
    env.cmd(config_cmd(), 'SET', 'MINSTEMLEN', 3)
    env.cmd('FT.CREATE', 'idx_min3', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    env.cmd('HSET', '{doc}:1', 't', 'fry')
    
    # 'fry' is stemmed when MIN_STEMMING_LEN = 3
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_min3')
    env.assertEqual(res, ['+fri', 'fry'])

    # 'fry' is found when MIN_STEMMING_LEN = 3
    env.cmd('HSET', '{doc}:2', 't', 'fries')
    env.cmd('HSET', '{doc}:3', 't', 'frying')
    res = env.cmd('FT.SEARCH', 'idx_min3', 'fried', 'SORTBY', 't', 'ASC')
    env.assertEqual(res, [3, '{doc}:2', ['t', 'fries'], '{doc}:1', ['t', 'fry'],
                          '{doc}:3', ['t', 'frying']])
    
    res = env.cmd(config_cmd(), 'GET', 'MINSTEMLEN')
    env.assertEqual(res[0], ['MINSTEMLEN', '3'])

    ########################################################
    # Test the default MIN_STEMMING_LEN = 5
    ########################################################
    # Create the index with MIN_STEMMING_LEN = 5
    env.flush()
    env.cmd(config_cmd(), 'SET', 'MINSTEMLEN', 5)
    env.cmd('HSET', '{doc}:1', 't', 'stem')
    env.cmd('FT.CREATE', 'idx_min5', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    waitForIndex(env, 'idx_min5')

    # 'stem' is not stemmed because MIN_STEMMING_LEN = 5
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_min5')
    env.assertEqual(res, ['stem'])

    # 'stem' is found when MIN_STEMMING_LEN = 5, because the root word is 'stem'
    env.cmd('HSET', '{doc}:2', 't', 'stemming')
    env.cmd('HSET', '{doc}:3', 't', 'stemmed')
    res = env.cmd('FT.SEARCH', 'idx_min5', 'stemming', 'SORTBY', 't', 'ASC')
    env.assertEqual(res, [3, '{doc}:1', ['t', 'stem'], '{doc}:3', ['t', 'stemmed'], 
                          '{doc}:2', ['t', 'stemming']])

    res = env.cmd(config_cmd(), 'GET', 'MINSTEMLEN')
    env.assertEqual(res[0], ['MINSTEMLEN', '5'])

    ########################################################
    # Test with MIN_STEMMING_LEN = 3 - Spanish
    ########################################################
    # Create the index with MIN_STEMMING_LEN = 3
    env.flush()
    env.cmd(config_cmd(), 'SET', 'MINSTEMLEN', 3)
    env.cmd('FT.CREATE', 'idx_es', 'ON', 'HASH', 'LANGUAGE', 'spanish',
            'SCHEMA', 't', 'TEXT')
    env.cmd('HSET', '{doc}:1', 't', 'dar')
    
    # altough MIN_STEMMING_LEN = 3, 'dar' does not need to be stemmed because
    # the original word is equal to its stem
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_es')
    env.assertEqual(res, ['dar'])

    # stemming works for Spanish
    env.cmd('HSET', '{doc}:2', 't', 'daremos')
    env.cmd('HSET', '{doc}:3', 't', 'daré')
    for word in ['dar', 'daremos', 'daré', 'darían', 'dará']:
        res = env.cmd('FT.SEARCH', 'idx_es', word, 'LANGUAGE', 'spanish')
        env.assertEqual(res[0], 3)

def testJsonMinStemLen(env):

    ########################################################
    # Test the default MIN_STEMMING_LEN (4)
    ########################################################
    # Create the index with default MIN_STEMMING_LEN (4)
    env.cmd('FT.CREATE', 'idx_min4', 'ON', 'JSON',
            'SCHEMA', '$.t', 'AS', 't', 'TEXT')
    env.cmd('JSON.SET', '{doc}:1', '$', r'{"t":"fry"}')
    
    # 'fry' is not stemmed when MIN_STEMMING_LEN = 4
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_min4')
    env.assertEqual(res, ['fry'])

    # 'fry' is not found when MIN_STEMMING_LEN = 4
    env.cmd('JSON.SET', '{doc}:2', '$', r'{"t":"fries"}')
    env.cmd('JSON.SET', '{doc}:3', '$', r'{"t":"frying"}')
    res = env.cmd('FT.SEARCH', 'idx_min4', 'fried', 'SORTBY', 't', 'ASC',
                  'NOCONTENT')
    env.assertEqual(res, [2, '{doc}:2', '{doc}:3'])

    res = env.cmd(config_cmd(), 'GET', 'MINSTEMLEN')
    env.assertEqual(res[0], ['MINSTEMLEN', '4'])

    ########################################################
    # Test with MIN_STEMMING_LEN = 3
    ########################################################
    # Create the index with MIN_STEMMING_LEN = 3
    env.flush()
    env.cmd(config_cmd(), 'SET', 'MINSTEMLEN', 3)
    env.cmd('FT.CREATE', 'idx_min3', 'ON', 'JSON',
            'SCHEMA', '$.t', 'AS', 't', 'TEXT')
    env.cmd('JSON.SET', '{doc}:1', '$', r'{"t":"fry"}')
    
    # 'fry' is stemmed when MIN_STEMMING_LEN = 3
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_min3')
    env.assertEqual(res, ['+fri', 'fry'])

    # 'fry' is found when MIN_STEMMING_LEN = 3
    env.cmd('JSON.SET', '{doc}:2', '$', r'{"t":"fries"}')
    env.cmd('JSON.SET', '{doc}:3', '$', r'{"t":"frying"}')
    res = env.cmd('FT.SEARCH', 'idx_min3', 'fried', 'SORTBY', 't', 'ASC',
                  'NOCONTENT')
    env.assertEqual(res, [3, '{doc}:2', '{doc}:1', '{doc}:3'])
    
    res = env.cmd(config_cmd(), 'GET', 'MINSTEMLEN')
    env.assertEqual(res[0], ['MINSTEMLEN', '3'])

    ########################################################
    # Test the default MIN_STEMMING_LEN = 5
    ########################################################
    # Create the index with MIN_STEMMING_LEN = 5
    env.flush()
    env.cmd(config_cmd(), 'SET', 'MINSTEMLEN', 5)
    env.cmd('JSON.SET', '{doc}:1', '$', r'{"t":"stem"}')
    env.cmd('FT.CREATE', 'idx_min5', 'ON', 'JSON',
            'SCHEMA', '$.t', 'AS', 't', 'TEXT')
    waitForIndex(env, 'idx_min5')

    # 'stem' is not stemmed because MIN_STEMMING_LEN = 5
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_min5')
    env.assertEqual(res, ['stem'])

    # 'stem' is found when MIN_STEMMING_LEN = 5, because the root word is 'stem'
    env.cmd('JSON.SET', '{doc}:2', '$', r'{"t":"stemming"}')
    env.cmd('JSON.SET', '{doc}:3', '$', r'{"t":"stemmed"}')
    res = env.cmd('FT.SEARCH', 'idx_min5', 'stemming', 'SORTBY', 't', 'ASC',
                  'NOCONTENT')
    env.assertEqual(res, [3, '{doc}:1', '{doc}:3', '{doc}:2'])

    res = env.cmd(config_cmd(), 'GET', 'MINSTEMLEN')
    env.assertEqual(res[0], ['MINSTEMLEN', '5'])

    ########################################################
    # Test with MIN_STEMMING_LEN = 3 - Spanish
    ########################################################
    # Create the index with MIN_STEMMING_LEN = 3
    env.flush()
    env.cmd(config_cmd(), 'SET', 'MINSTEMLEN', 3)
    env.cmd('FT.CREATE', 'idx_es', 'ON', 'JSON', 'LANGUAGE', 'spanish',
            'SCHEMA', '$.t', 'AS', 't', 'TEXT')
    env.cmd('JSON.SET', '{doc}:1', '$', r'{"t":"dar"}')
    
    # altough MIN_STEMMING_LEN = 3, 'dar' does not need to be stemmed because
    # the original word is equal to its stem
    res = env.cmd(debug_cmd(), 'DUMP_TERMS', 'idx_es')
    env.assertEqual(res, ['dar'])

    # stemming works for Spanish
    env.cmd('JSON.SET', '{doc}:2', '$', r'{"t":"daremos"}')
    env.cmd('JSON.SET', '{doc}:3', '$', r'{"t":"daré"}')
    for word in ['dar', 'daremos', 'daré', 'darían', 'dará']:
        res = env.cmd('FT.SEARCH', 'idx_es', word, 'LANGUAGE', 'spanish')
        env.assertEqual(res[0], 3)

