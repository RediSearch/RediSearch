from RLTest import Env
import pprint
import time
import utils

def testCreateRules(env):
    env.cmd('ft.create', 'idx', 'EXPRESSION',
            'hasprefix("user:")||@year>2015',
            'SCHEMA', 'f1', 'text')

    env.cmd('hset', 'user:mnunberg', 'foo', 'bar')
    env.cmd('hset', 'user:mnunberg', 'f1', 'hello world')
    env.expect('ft.search', 'idx', 'hello') \
            .equal([1L, 'user:mnunberg', ['foo', 'bar', 'f1', 'hello world']])

    env.cmd('del', 'user:mnunberg')
    env.expect('ft.search', 'idx', 'hello').equal([0L])

    env.cmd('hset', 'someDoc', 'year', '2019')
    env.cmd('hset', 'someDoc', 'name', 'mark')
    env.cmd('hset', 'someDoc', 'f1', 'goodbye')

    env.expect('ft.search', 'idx', 'goodbye') \
            .equal([1L, 'someDoc', ['year', '2019', 'name', 'mark', 'f1', 'goodbye']])
    env.cmd('del', 'someDoc')
    env.expect('ft.search', 'idx', 'goodbye').equal([0L])

def testScanRules(env):
    for x in range(10000):
        env.cmd('hset', 'doc{}'.format(x), 'f1', 'hello')
    
    # One document that should not match..
    env.cmd('hset', 'dummy', 'f1', 'hello')

    env.cmd('ft.create', 'idx', 'EXPRESSION', 'hasprefix("doc")', 'schema', 'f1', 'text')

    # print("SCANSTART")
    # env.cmd('ft.scanstart')
    while True:
        if utils.is_synced(env):
            break
        time.sleep(0.01)
    rv = env.cmd('ft.search', 'idx', 'hello')
    env.assertEqual(10000, rv[0])  # Order can be different!

def testPersistence(env):
    env.cmd('ft.create', 'idx', 'EXPRESSION', "hasprefix('doc')", 'SCHEMA', 'f1', 'TEXT')
    utils.dump_and_reload(env)

    env.cmd('hset', 'doc1', 'f1', 'hello world')
    rv = env.cmd('ft.search', 'idx', 'hello')
    env.assertEqual([1L, 'doc1', ['f1', 'hello world']], rv)

def testAddRule(env):
    env.expect('ft.create idx SCHEMA f1 text').ok()

    #PREFIX
    env.expect('ft.ruleadd idx prefixRule PREFIX user:').ok()
    env.cmd('hset user:mnunberg foo bar')
    env.cmd('hset user:mnunberg f1 prefix')
    env.expect('ft.search idx prefix')   \
            .equal([1L, 'user:mnunberg', ['foo', 'bar', 'f1', 'prefix']])

    # HASFIELD
    env.expect('ft.ruleadd idx hasFieldRule HASFIELD country').ok()
    env.cmd('hset dan country Israel f1 field')
    env.expect('ft.search idx field')   \
            .equal([1L, 'dan', ['country', 'Israel', 'f1', 'field']])
    
    # Should not work like this
    # What is the exact API?
    '''
    # WILDCARD
    # TODO fix
    env.expect('ft.ruleadd idx hasFieldRule * INDEX').ok()
    env.cmd('hset any f1 wc')
    env.expect('ft.search idx wc')   \
            .equal([1L, 'any', ['f1', 'wc']])
    '''
    #EXPR - prefix
    env.expect('ft.ruleadd idx useExprPrefix EXPR hasprefix("loc")').ok()
    env.cmd('hset loc:usa f1 exprPrefix')
    env.expect('ft.search idx exprPrefix')   \
            .equal([1L, 'loc:usa', ['f1', 'exprPrefix']])

    #EXPR - hasfield
    env.expect('ft.ruleadd idx useExprHasField EXPR hasfield("user_id")').ok()
    env.cmd('hset field f1 exprHasField user_id 1234')
    env.expect('ft.search idx exprHasField')   \
            .equal([1L, 'field', ['f1', 'exprHasField', 'user_id', '1234']])
    
    #crash
    '''
    #EXPR - condition
    env.expect('ft.ruleadd idx useCond EXPR @zipcode<1000').ok()
    env.cmd('hset cond f1 exprCond zipcode 666')
    env.expect('ft.search idx exprCond')   \
            .equal([1L, 'cond', ['f1', 'exprCond', 'zipcode', '666']])
    '''

def testArgsError(env):
    # no index
    env.expect('ft.ruleadd idx prefixRule PREFIX redis').error().equal('No such index idx')

    env.expect('ft.create idx SCHEMA f1 text').ok()

    error_msg = "wrong number of arguments for 'ft.ruleadd' command"
    env.expect('ft.ruleadd idx prefixRule PREFIX').error().equal(error_msg)
    env.expect('ft.ruleadd idx hasFieldRule HASFIELD').error().equal(error_msg)
    env.expect('ft.ruleadd idx hasFieldRule *').error().equal(error_msg)
    env.expect('ft.ruleadd idx useExprPrefix EXPR').error().equal(error_msg)

    env.expect('ft.ruleadd idx errorRule MADEUP rule').error().equal('No such match type `MADEUP`\n')
    env.expect('ft.ruleadd idx errorRule EXPR').error().equal(error_msg)
    env.expect('ft.ruleadd idx errorRule EXPR hasfield()').error().equal('hasfield needs one argument')
    # hasfield -> hasprefix
    #env.expect('ft.ruleadd idx errorRule EXPR hasprefix()').error().equal('hasprefix needs one argument')
    env.expect('ft.ruleadd idx errorRule EXPR wrong("field")').error().equal('Unknown function name wrong')

def testSetAttributes(env):
    # test score
    env.expect('ft.create idx SCHEMA f1 text').ok()
    env.expect('ft.ruleadd idx score HASFIELD name SETATTR SCORE 1').ok()
    env.cmd('hset setAttr1 f1 scoreAttr name rule')
    env.cmd('hset setAttr2 f1 \"longer string scoreAttr\" name rule')
    env.expect('ft.search idx scoreAttr WITHSCORES')   \
            .equal([1L, 'setAttr1', '0', ['f1', 'scoreAttr', 'name', 'rule']])
    # correct .equal([1L, 'setAttr1', '1', ['f1', 'scoreAttr', 'name', 'rule']])

    # test language
    # TODO: how?

def testSetAttributes(env):
    # test score
    env.expect('ft.create idx SCHEMA f1 text').ok()
    env.expect('ft.ruleadd idx score HASFIELD name LOADATTR SCORE score').ok()
    env.cmd('hset setAttr1 f1 scoreAttr name rule score 1')
    env.cmd('hset setAttr2 f1 scoreAttr name rule score .5')
    env.expect('ft.search idx scoreAttr WITHSCORES')   \
    #        .equal([2L, 'setAttr2', '.5', ['f1', 'scoreAttr', 'name', 'rule', 'score', '.5'],
    #                    'setAttr1', '1', ['f1', 'scoreAttr', 'name', 'rule', 'score', '1']])

    # test language
    # TODO

def testAttributeError(env):
    '''
    env.expect('ft.create idx SCHEMA f1 text').ok()
    env.expect('ft.ruleadd idx score HASFIELD name SETATTR SCORE').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name SETATTR SCORE -1').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name SETATTR SCORE 2').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name SETATTR LANGUAGE').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name SETATTR LANGUAGE hebrew').error().equal(1)

    env.expect('ft.ruleadd idx score HASFIELD name LOADATTR SCORE').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name LOADATTR SCORE -1').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name LOADATTR SCORE 2').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name LOADATTR LANGUAGE').error().equal(1)
    env.expect('ft.ruleadd idx score HASFIELD name LOADATTR LANGUAGE hebrew').error().equal(1)
    '''

def testInvalidType(env):
    env.cmd('set doc1 bar')
    env.expect('ft.create idx SCHEMA foo text').ok()
    env.expect('ft.ruleadd idx hasFieldRule HASFIELD baz').ok()
    env.expect('hset doc1 foo bar baz xyz').error() \
            .equal('WRONGTYPE Operation against a key holding the wrong kind of value')
