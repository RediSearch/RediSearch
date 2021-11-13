from RLTest import Env
from includes import *


def testDictAdd():
    env = Env()
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term3').equal(3)
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term4').equal(1)


def testDictAddWrongArity():
    env = Env()
    env.expect('ft.dictadd', 'dict').raiseError()


def testDictDelete():
    env = Env()
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term3').equal(3)
    env.expect('ft.dictdel', 'dict', 'term1', 'term2', 'term4').equal(2)
    env.expect('ft.dictdel', 'dict', 'term3').equal(1)
    env.expect('keys', '*').equal([])


def testDictDeleteWrongArity():
    env = Env()
    env.expect('ft.dictdel', 'dict').raiseError()


def testDictDeleteOnNoneExistingKey():
    env = Env()
    env.expect('ft.dictdel', 'dict', 'term1').equal(0)


def testDictDump():
    env = Env()
    env.expect('ft.dictadd', 'dict', 'term1', 'term2', 'term3').equal(3)
    env.expect('ft.dictdump', 'dict').equal(['term1', 'term2', 'term3'])


def testDictDumpWrongArity():
    env = Env()
    env.expect('ft.dictdump').raiseError()


def testDictDumpOnNoneExistingKey():
    env = Env()
    env.expect('ft.dictdump', 'dict').raiseError()


def testBasicSpellCheck():
    env = Env()
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'name').equal([['TERM', 'name',
                                                       [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1']]]])
    if not env.isCluster():
        env.expect('ft.spellcheck', 'idx', '@body:name').equal([['TERM', 'name', [['0.66666666666666663', 'name2']]]])


def testBasicSpellCheckWithNoResult():
    env = Env()
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'somenotexiststext').equal([['TERM', 'somenotexiststext', []]])


def testSpellCheckOnExistingTerm():
    env = Env()
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'name').equal([])


def testSpellCheckWithIncludeDict():
    env = Env()
    env.cmd('ft.dictadd', 'dict', 'name3', 'name4', 'name5')
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict').equal([['TERM', 'name',
                                                                                   [['0.66666666666666663', 'name2'],
                                                                                    ['0.33333333333333331', 'name1'],
                                                                                    ['0', 'name3'], ['0', 'name4'],
                                                                                    ['0', 'name5']]]])
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'include', 'dict').equal([['TERM', 'name',
                                                                                   [['0.66666666666666663', 'name2'],
                                                                                    ['0.33333333333333331', 'name1'],
                                                                                    ['0', 'name3'], ['0', 'name4'],
                                                                                    ['0', 'name5']]]])


def testSpellCheckWithDuplications():
    env = Env()
    env.cmd('ft.dictadd', 'dict', 'name1', 'name4', 'name5')
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict').equal([['TERM', 'name',
                                                                                   [['0.66666666666666663', 'name2'],
                                                                                    ['0.33333333333333331', 'name1'],
                                                                                    ['0', 'name4'], ['0', 'name5']]]])


def testSpellCheckExcludeDict():
    env = Env()
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict').equal([])
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'exclude', 'dict').equal([])


def testSpellCheckNoneExistingIndex():
    env = Env()
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict').raiseError()


def testSpellCheckWrongArity():
    env = Env()
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx').raiseError()
    env.expect('ft.spellcheck', 'idx').raiseError()


def testSpellCheckBadFormat():
    env = Env()
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS').raiseError()
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE').raiseError()
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE').raiseError()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE').raiseError()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE', 0).raiseError()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE', -1).raiseError()
    env.expect('ft.spellcheck', 'idx', 'name', 'DISTANCE', 101).raiseError()


def testSpellCheckNoneExistingDicts():
    env = Env()
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict').raiseError()
    env.expect('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict').raiseError()


def testSpellCheckResultsOrder():
    env = Env()
    env.cmd('ft.dictadd', 'dict', 'name')
    env.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'Elior', 'body', 'body1')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'Hila', 'body', 'body2')
    env.expect('ft.spellcheck', 'idx', 'Elioh Hilh').equal([['TERM', 'elioh', [['0.5', 'elior']]], ['TERM', 'hilh', [['0.5', 'hila']]]])


def testSpellCheckIssue437():
    env = Env()
    env.cmd('ft.create', 'incidents', 'SCHEMA', 'report', 'text')
    env.cmd('FT.DICTADD', 'slang', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')
    env.expect('FT.SPELLCHECK', 'incidents',
               'Tooni toque kerfuffle', 'TERMS',
               'EXCLUDE', 'slang', 'TERMS',
               'INCLUDE', 'slang').equal([['TERM', 'tooni', [['0', 'toonie']]]])
