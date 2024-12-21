# -*- coding: utf-8 -*-

from common import getConnectionByEnv, debug_cmd, config_cmd
from RLTest import Env


def testMultibyteChars(env):
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

        # Search with prefix
        res = conn.execute_command(
            'FT.SEARCH', 'idx', '@t:БЪЛГА*', 'NOCONTENT')
        env.assertEqual(res, expected)

