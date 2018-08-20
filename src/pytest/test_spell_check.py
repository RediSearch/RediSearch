from base_case import BaseSearchTestCase


class SpellCheckTestCase(BaseSearchTestCase):

    def testDictAdd(self):
        res = self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term3')
        self.assertEqual(res, 3)
        res = self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term4')
        self.assertEqual(res, 1)

    def testDictAddWrongArity(self):
        with self.assertResponseError():
            self.cmd('ft.dictadd', 'dict')

    def testDictDelete(self):
        self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term3')
        res = self.cmd('ft.dictdel', 'dict', 'term1', 'term2', 'term4')
        self.assertEqual(res, 2)
        res = self.cmd('ft.dictdel', 'dict', 'term3')
        self.assertEqual(res, 1)
        res = self.cmd('keys', '*')
        self.assertEqual(res, [])

    def testDictDeleteWrongArity(self):
        with self.assertResponseError():
            self.cmd('ft.dictdel', 'dict')

    def testDictDeleteOnNoneExistingKey(self):
        res = self.cmd('ft.dictdel', 'dict', 'term1')
        self.assertEqual(res, 0)

    def testDictDump(self):
        self.cmd('ft.dictadd', 'dict', 'term1', 'term2', 'term3')
        res = self.cmd('ft.dictdump', 'dict')
        self.assertEqual(res, ['term1', 'term2', 'term3'])

    def testDictDumpWrongArity(self):
        with self.assertResponseError():
            self.cmd('ft.dictdump')

    def testDictDumpOnNoneExistingKey(self):
        with self.assertResponseError():
            self.cmd('ft.dictdump', 'dict')

    def testBasicSpellCheck(self):
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name')
        self.assertEqual(res, [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1']]]])
        if not self.is_cluster():
            res = self.cmd('ft.spellcheck', 'idx', '@body:name')
            self.assertEqual(res, [['TERM', 'name', [['0.66666666666666663', 'name2']]]])
        else:
            self.skipTest("FIXME: Test not working on cluster")

    def testBasicSpellCheckWithNoResult(self):
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'somenotexiststext')
        self.assertEqual(res, [['TERM', 'somenotexiststext', []]])

    def testSpellCheckOnExistingTerm(self):
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name')
        self.assertEqual(res, [])

    def testSpellCheckWithIncludeDict(self):
        self.cmd('ft.dictadd', 'dict', 'name3', 'name4', 'name5')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
        self.assertEqual(res, [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1'],
                                                 ['0', 'name3'], ['0', 'name4'], ['0', 'name5']]]])
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'include', 'dict')
        self.assertEqual(res, [['TERM', 'name', [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1'],
                                                 ['0', 'name3'], ['0', 'name4'], ['0', 'name5']]]])

    def testSpellCheckWithDuplications(self):
        self.cmd('ft.dictadd', 'dict', 'name1', 'name4', 'name5')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
        self.assertEqual(res, [['TERM', 'name',
                               [['0.66666666666666663', 'name2'], ['0.33333333333333331', 'name1'], ['0', 'name4'], ['0', 'name5']]]])

    def testSpellCheckExcludeDict(self):
        self.cmd('ft.dictadd', 'dict', 'name')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict')
        self.assertEqual(res, [])
        res = self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'exclude', 'dict')
        self.assertEqual(res, [])

    def testSpellCheckNoneExistingIndex(self):
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict')

    def testSpellCheckWrongArity(self):
        self.cmd('ft.dictadd', 'dict', 'name')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx')
        with self.assertResponseError():
            self.cmd('ft.spellcheck')

    def testSpellCheckBadFormat(self):
        self.cmd('ft.dictadd', 'dict', 'name')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE', 0)
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE', -1)
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'DISTANCE', 101)

    def testSpellCheckNoneExistingDicts(self):
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'name1', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'name2', 'body', 'body2')
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'FIELDS', 'name', 'name2', 'body', 'name2')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'INCLUDE', 'dict')
        with self.assertResponseError():
            self.cmd('ft.spellcheck', 'idx', 'name', 'TERMS', 'EXCLUDE', 'dict')

    def testSpellCheckResultsOrder(self):
        self.cmd('ft.dictadd', 'dict', 'name')
        self.cmd('ft.create', 'idx', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'name', 'Elior', 'body', 'body1')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'name', 'Hila', 'body', 'body2')
        res = self.cmd('ft.spellcheck', 'idx', 'Elioh Hilh')
        self.assertEqual(res, [['TERM', 'elioh', [['0.5', 'elior']]], ['TERM', 'hilh', [['0.5', 'hila']]]])

    def testSpellCheckIssue437(self):
        self.cmd('ft.create', 'incidents', 'SCHEMA', 'report', 'text')
        self.cmd('FT.DICTADD', 'slang', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')
        res = self.cmd('FT.SPELLCHECK', 'incidents', 'Tooni toque kerfuffle', 'TERMS', 'EXCLUDE', 'slang', 'TERMS', 'INCLUDE', 'slang')
        self.assertEqual(res, [['TERM', 'tooni', [['0', 'toonie']]]])
