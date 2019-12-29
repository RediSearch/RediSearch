from RLTest import Env
import random
from includes import *


def aofTestCommon(env, reloadfn):
        # TODO: Change this attribute in rmtest

        env.cmd('ft.create', 'idx', 'schema',
                'field1', 'text', 'field2', 'numeric')
        reloadfn()
        for x in range(1, 10):
            env.assertCmdOk('ft.add', 'idx', 'doc{}'.format(x), 1.0 / x, 'fields',
                            'field1', 'myText{}'.format(x), 'field2', 20 * x)
        exp = [9L, 'doc1', ['field1', 'myText1', 'field2', '20'], 'doc2', ['field1', 'myText2', 'field2', '40'], 'doc3', ['field1', 'myText3', 'field2', '60'], 'doc4', ['field1', 'myText4', 'field2', '80'], 'doc5', ['field1',
                                                                                                                                                                                                                        'myText5', 'field2', '100'], 'doc6', ['field1', 'myText6', 'field2', '120'], 'doc7', ['field1', 'myText7', 'field2', '140'], 'doc8', ['field1', 'myText8', 'field2', '160'], 'doc9', ['field1', 'myText9', 'field2', '180']]
        reloadfn()
        ret = env.cmd('ft.search', 'idx', 'myt*')
        env.assertEqual(exp, ret)


def testAof():
    env = Env(useAof=True)
    aofTestCommon(env, lambda: env.restart_and_reload())


def testRawAof():
    env = Env(useAof=True)
    if env.env == 'existing-env':
        env.skip()
    aofTestCommon(env, lambda: env.broadcast('debug', 'loadaof'))


def testRewriteAofSortables():
    env = Env(useAof=True)
    env.cmd('FT.CREATE', 'idx', 'schema', 'field1', 'TEXT',
            'SORTABLE', 'num1', 'NUMERIC', 'SORTABLE')
    env.cmd('FT.ADD', 'idx', 'doc', 1.0,
            'FIELDS', 'field1', 'Hello World')
    env.restart_and_reload()
    env.broadcast('SAVE')

    # Load some documents
    for x in xrange(100):
        env.cmd('FT.ADD', 'idx', 'doc{}'.format(x), 1.0, 'FIELDS',
                'field1', 'txt{}'.format(random.random()),
                'num1', random.random())
    for sspec in [('field1', 'asc'), ('num1', 'desc')]:
        cmd = ['FT.SEARCH', 'idx', 'txt', 'SORTBY', sspec[0], sspec[1]]
        res = env.cmd(*cmd)
        env.restart_and_reload()
        res2 = env.cmd(*cmd)
        env.assertEqual(res, res2)


def testAofRewriteSortkeys():
    env = Env(useAof=True)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'foo',
            'TEXT', 'SORTABLE', 'bar', 'TAG')
    env.cmd('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
    env.cmd('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')

    res_exp = env.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                      'RETURN', '1', 'foo', 'WITHSORTKEYS')

    env.restart_and_reload()
    res_got = env.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                      'RETURN', '1', 'foo', 'WITHSORTKEYS')

    env.assertEqual(res_exp, res_got)


def testAofRewriteTags():
    env = Env(useAof=True)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'foo',
            'TEXT', 'SORTABLE', 'bar', 'TAG')
    env.cmd('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
    env.cmd('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')

    info_a = to_dict(env.cmd('FT.INFO', 'idx'))
    env.restart_and_reload()
    info_b = to_dict(env.cmd('FT.INFO', 'idx'))
    env.assertEqual(info_a['fields'], info_b['fields'])

    # Try to drop the schema
    env.cmd('FT.DROP', 'idx')

    # Try to create it again - should work!
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'foo',
            'TEXT', 'SORTABLE', 'bar', 'TAG')
    env.cmd('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
    env.cmd('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')
    res = env.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                  'RETURN', '1', 'foo', 'WITHSORTKEYS')
    env.assertEqual([2L, '1', '$a', ['foo', 'A'],
                     '2', '$b', ['foo', 'B']], res)


def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}
