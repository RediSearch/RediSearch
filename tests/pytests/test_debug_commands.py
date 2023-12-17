from RLTest import Env
from includes import *
from common import waitForIndex


class TestDebugCommands(object):

    def __init__(self):
        self.env = Env(testName="testing debug commands")
        self.env.skipOnCluster()
        self.env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
                        'name', 'TEXT', 'SORTABLE',
                        'age', 'NUMERIC', 'SORTABLE',
                        't', 'TAG', 'SORTABLE').ok()
        waitForIndex(self.env, 'idx')
        self.env.expect('FT.ADD', 'idx', 'doc1', '1.0', 'FIELDS', 'name', 'meir', 'age', '29', 't', 'test').ok()
        self.env.cmd('SET', 'foo', 'bar')

    def testDebugWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_invidx').error().contains('wrong number of arguments')
        self.env.expect('FT.DEBUG').error().contains('wrong number of arguments')

    def testDebugUnknownSubcommand(self):
        self.env.expect('FT.DEBUG', 'unknown').error().equal('subcommand was not found')

    def testDebugHelp(self):
        err_msg = 'wrong number of arguments'
        help_list = ['DUMP_INVIDX', 'DUMP_NUMIDX', 'DUMP_NUMIDXTREE', 'DUMP_TAGIDX', 'INFO_TAGIDX', 'DUMP_GEOMIDX', 'IDTODOCID', 'DOCIDTOID', 'DOCINFO',
                     'DUMP_PHONETIC_HASH', 'DUMP_SUFFIX_TRIE', 'DUMP_TERMS', 'INVIDX_SUMMARY', 'NUMIDX_SUMMARY',
                     'GC_FORCEINVOKE', 'GC_FORCEBGINVOKE', 'GC_CLEAN_NUMERIC', 'GIT_SHA', 'TTL', 'VECSIM_INFO']
        self.env.expect('FT.DEBUG', 'help').equal(help_list)

        for cmd in help_list:
            if cmd == 'GIT_SHA':
                # 'GIT_SHA' do not return err_msg
                 continue
            self.env.expect('FT.DEBUG', cmd).error().contains(err_msg)

    def testDocInfo(self):
        rv = self.env.cmd('ft.debug', 'docinfo', 'idx', 'doc1')
        self.env.assertEqual(['internal_id', 1, 'flags', '(0xc):HasSortVector,HasOffsetVector,',
                              'score', '1', 'num_tokens', 1, 'max_freq', 1, 'refcount', 1, 'sortables',
                               [['index', 0, 'field', 'name AS name', 'value', 'meir'],
                                ['index', 1, 'field', 'age AS age', 'value', '29'],
                                ['index', 2, 'field', 't AS t', 'value', 'test']]], rv)
        self.env.expect('ft.debug', 'docinfo', 'idx').error()
        self.env.expect('ft.debug', 'docinfo', 'idx', 'doc2').error()

    def testDumpInvertedIndex(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx', 'meir').equal([1])
        self.env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'meir').equal([1])

    def testDumpInvertedIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx').error()

    def testDumpUnexistsInvertedIndex(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx', 'meir1').error()

    def testDumpInvertedIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx1', 'meir').error()

    def testDumpNumericIndex(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx', 'age').equal([[1]])
        self.env.expect('FT.DEBUG', 'DUMP_NUMIDX', 'idx', 'age').equal([[1]])

    def testDumpNumericIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx').error()

    def testDumpUnexistsNumericIndex(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx', 'ag1').error()

    def testDumpNumericIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx1', 'age').error()

    def testDumpNumericIndexInvalidKeyType(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'foo', 'age').error()

    def testDumpTagIndex(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx', 't').equal([['test', [1]]])
        self.env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([['test', [1]]])

    def testDumpTagIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx').error()

    def testDumpUnexistsTagIndex(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx', 't1').error()

    def testDumpTagIndexInvalidKeyType(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'foo', 't1').error()

    def testDumpTagIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx1', 't').error()

    def testInfoTagIndex(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx', 't').equal(['num_values', 1])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't').equal(['num_values', 1])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'dump_id_entries').equal(['num_values', 1, 'values', []])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'count_value_entries').equal(['num_values', 1, 'values', []])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'dump_id_entries', 'limit', '1') \
            .equal(['num_values', 1, 'values', [['value', 'test', 'num_entries', 1, 'num_blocks', 1, 'entries', [1]]]] )
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'count_value_entries', 'limit', '1') \
            .equal(['num_values', 1, 'values', [['value', 'test', 'num_entries', 1, 'num_blocks', 1]]])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'count_value_entries', 'limit', 'abc').error()

    def testInfoTagIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx').error()

    def testInfoUnexistsTagIndex(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx', 't1').error()

    def testInfoTagIndexInvalidKeyType(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'foo', 't1').error()

    def testInfoTagIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx1', 't').error()

    def testDocIdToId(self):
        self.env.expect('FT.DEBUG', 'docidtoid', 'idx', 'doc1').equal(1)
        self.env.expect('FT.DEBUG', 'DOCIDTOID', 'idx', 'doc1').equal(1)

    def testDocIdToIdOnUnexistingDoc(self):
        self.env.expect('FT.DEBUG', 'docidtoid', 'idx', 'doc').equal(0)

    def testIdToDocId(self):
        self.env.expect('FT.DEBUG', 'idtodocid', 'idx', '1').equal('doc1')
        self.env.expect('FT.DEBUG', 'IDTODOCID', 'idx', '1').equal('doc1')

    def testIdToDocIdOnUnexistingId(self):
        self.env.expect('FT.DEBUG', 'idtodocid', 'idx', '2').error().equal('document was removed')
        self.env.expect('FT.DEBUG', 'idtodocid', 'idx', 'docId').error().equal('bad id given')

    def testDumpPhoneticHash(self):
        self.env.expect('FT.DEBUG', 'dump_phonetic_hash', 'test').equal(['<TST', '<TST'])
        self.env.expect('FT.DEBUG', 'DUMP_PHONETIC_HASH', 'test').equal(['<TST', '<TST'])

    def testDumpPhoneticHashWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_phonetic_hash').error()

    def testDumpTerms(self):
        self.env.expect('FT.DEBUG', 'dump_terms', 'idx').equal(['meir'])
        self.env.expect('FT.DEBUG', 'DUMP_TERMS', 'idx').equal(['meir'])

    def testDumpTermsWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_terms').error()

    def testDumpTermsUnknownIndex(self):
        self.env.expect('FT.DEBUG', 'dump_terms', 'idx1').error()

    def testInvertedIndexSummary(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx', 'meir').equal(['numDocs', 1, 'numEntries', 1, 'lastId', 1, 'flags',
                                                                            83, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

        self.env.expect('FT.DEBUG', 'INVIDX_SUMMARY', 'idx', 'meir').equal(['numDocs', 1, 'numEntries', 1, 'lastId', 1, 'flags',
                                                                            83, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

    def testUnexistsInvertedIndexSummary(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx', 'meir1').error()

    def testInvertedIndexSummaryInvalidIdxName(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx1', 'meir').error()

    def testInvertedIndexSummaryWrongArity(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx1').error()

    def testNumericIdxIndexSummary(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'age').equal(['numRanges', 1, 'numEntries', 1,
                                                                           'lastDocId', 1, 'revisionId', 0,
                                                                           'emptyLeaves', 0, 'RootMaxDepth', 0])

        self.env.expect('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'age').equal(['numRanges', 1, 'numEntries', 1,
                                                                           'lastDocId', 1, 'revisionId', 0,
                                                                           'emptyLeaves', 0, 'RootMaxDepth', 0])

    def testUnexistsNumericIndexSummary(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'age1').error()

    def testNumericIndexSummaryInvalidIdxName(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx1', 'age').error()

    def testNumericIndexSummaryWrongArity(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx1').error()

    def testDumpSuffixWrongArity(self):
        self.env.expect('FT.DEBUG', 'DUMP_SUFFIX_TRIE', 'idx1', 'no_suffix').error()
