from RLTest import Env
from includes import *
from common import *

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
        self.env.expect('FT.DEBUG', 'dump_invidx').raiseError().contains('wrong number of arguments')
        self.env.expect('FT.DEBUG').raiseError().contains('wrong number of arguments')

    def testDebugUnknownSubcommand(self):
        self.env.expect('FT.DEBUG', 'unknown').raiseError().equal('subcommand was not found')

    def testDebugHelp(self):
        err_msg = 'wrong number of arguments'
        help_list = [
            'DUMP_INVIDX',
            'DUMP_NUMIDX',
            'DUMP_NUMIDXTREE',
            'DUMP_TAGIDX',
            'INFO_TAGIDX',
            'DUMP_PREFIX_TRIE',
            'IDTODOCID',
            'DOCIDTOID',
            'DOCINFO',
            'DUMP_PHONETIC_HASH',
            'DUMP_SUFFIX_TRIE',
            'DUMP_TERMS',
            'INVIDX_SUMMARY',
            'NUMIDX_SUMMARY',
            'GC_FORCEINVOKE',
            'GC_FORCEBGINVOKE',
            'GC_CLEAN_NUMERIC',
            'GIT_SHA',
            'TTL',
            'TTL_PAUSE',
            'TTL_EXPIRE',
            'VECSIM_INFO',
            'YIELDS_ON_LOAD_COUNTER',
            'BG_SCAN_CONTROLLER'
        ]
        self.env.expect('FT.DEBUG', 'help').equal(help_list)

        for cmd in help_list:
            if cmd in ['GIT_SHA', 'DUMP_PREFIX_TRIE', 'YIELDS_ON_LOAD_COUNTER']:
                # 'GIT_SHA' and 'DUMP_PREFIX_TRIE' do not return err_msg
                 continue
            self.env.expect('FT.DEBUG', cmd).raiseError().contains(err_msg)

    def testDocInfo(self):
        rv = self.env.cmd('ft.debug', 'docinfo', 'idx', 'doc1')
        self.env.assertEqual(['internal_id', 1, 'flags', '(0xc):HasSortVector,HasOffsetVector,',
                              'score', '1', 'num_tokens', 1, 'max_freq', 1, 'refcount', 1, 'sortables',
                               [['index', 0, 'field', 'name AS name', 'value', 'meir'],
                                ['index', 1, 'field', 'age AS age', 'value', '29'],
                                ['index', 2, 'field', 't AS t', 'value', 'test']]], rv)
        self.env.expect('ft.debug', 'docinfo', 'idx').raiseError()
        self.env.expect('ft.debug', 'docinfo', 'idx', 'doc2').raiseError()

    def testDumpInvertedIndex(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx', 'meir').equal([1])
        self.env.expect('FT.DEBUG', 'DUMP_INVIDX', 'idx', 'meir').equal([1])

    def testDumpInvertedIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx').raiseError()

    def testDumpUnexistsInvertedIndex(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx', 'meir1').raiseError()

    def testDumpInvertedIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'dump_invidx', 'idx1', 'meir').raiseError()

    def testDumpNumericIndex(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx', 'age').equal([[1]])
        self.env.expect('FT.DEBUG', 'DUMP_NUMIDX', 'idx', 'age').equal([[1]])

    def testDumpNumericIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx').raiseError()

    def testDumpUnexistsNumericIndex(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx', 'ag1').raiseError()

    def testDumpNumericIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'idx1', 'age').raiseError()

    def testDumpNumericIndexInvalidKeyType(self):
        self.env.expect('FT.DEBUG', 'dump_numidx', 'foo', 'age').raiseError()

    def testDumpTagIndex(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx', 't').equal([['test', [1]]])
        self.env.expect('FT.DEBUG', 'DUMP_TAGIDX', 'idx', 't').equal([['test', [1]]])

    def testDumpTagIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx').raiseError()

    def testDumpUnexistsTagIndex(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx', 't1').raiseError()

    def testDumpTagIndexInvalidKeyType(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'foo', 't1').raiseError()

    def testDumpTagIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'dump_tagidx', 'idx1', 't').raiseError()

    def testInfoTagIndex(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx', 't').equal(['num_values', 1])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't').equal(['num_values', 1])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'dump_id_entries').equal(['num_values', 1, 'values', []])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'count_value_entries').equal(['num_values', 1, 'values', []])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'dump_id_entries', 'limit', '1') \
            .equal(['num_values', 1, 'values', [['value', 'test', 'num_entries', 1, 'num_blocks', 1, 'entries', [1]]]] )
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'count_value_entries', 'limit', '1') \
            .equal(['num_values', 1, 'values', [['value', 'test', 'num_entries', 1, 'num_blocks', 1]]])
        self.env.expect('FT.DEBUG', 'INFO_TAGIDX', 'idx', 't', 'count_value_entries', 'limit', 'abc').raiseError()

    def testInfoTagIndexWrongArity(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx').raiseError()

    def testInfoUnexistsTagIndex(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx', 't1').raiseError()

    def testInfoTagIndexInvalidKeyType(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'foo', 't1').raiseError()

    def testInfoTagIndexInvalidSchema(self):
        self.env.expect('FT.DEBUG', 'info_tagidx', 'idx1', 't').raiseError()

    def testDocIdToId(self):
        self.env.expect('FT.DEBUG', 'docidtoid', 'idx', 'doc1').equal(1)
        self.env.expect('FT.DEBUG', 'DOCIDTOID', 'idx', 'doc1').equal(1)

    def testDocIdToIdOnUnexistingDoc(self):
        self.env.expect('FT.DEBUG', 'docidtoid', 'idx', 'doc').equal(0)

    def testIdToDocId(self):
        self.env.expect('FT.DEBUG', 'idtodocid', 'idx', '1').equal('doc1')
        self.env.expect('FT.DEBUG', 'IDTODOCID', 'idx', '1').equal('doc1')

    def testIdToDocIdOnUnexistingId(self):
        self.env.expect('FT.DEBUG', 'idtodocid', 'idx', '2').raiseError().equal('document was removed')
        self.env.expect('FT.DEBUG', 'idtodocid', 'idx', 'docId').raiseError().equal('bad id given')

    def testDumpPhoneticHash(self):
        self.env.expect('FT.DEBUG', 'dump_phonetic_hash', 'test').equal(['<TST', '<TST'])
        self.env.expect('FT.DEBUG', 'DUMP_PHONETIC_HASH', 'test').equal(['<TST', '<TST'])

    def testDumpPhoneticHashWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_phonetic_hash').raiseError()

    def testDumpTerms(self):
        self.env.expect('FT.DEBUG', 'dump_terms', 'idx').equal(['meir'])
        self.env.expect('FT.DEBUG', 'DUMP_TERMS', 'idx').equal(['meir'])

    def testDumpTermsWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_terms').raiseError()

    def testDumpTermsUnknownIndex(self):
        self.env.expect('FT.DEBUG', 'dump_terms', 'idx1').raiseError()

    def testInvertedIndexSummary(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx', 'meir').equal(['numDocs', 1, 'numEntries', 1, 'lastId', 1, 'flags',
                                                                            83, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

        self.env.expect('FT.DEBUG', 'INVIDX_SUMMARY', 'idx', 'meir').equal(['numDocs', 1, 'numEntries', 1, 'lastId', 1, 'flags',
                                                                            83, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

    def testUnexistsInvertedIndexSummary(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx', 'meir1').raiseError()

    def testInvertedIndexSummaryInvalidIdxName(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx1', 'meir').raiseError()

    def testInvertedIndexSummaryWrongArity(self):
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx1').raiseError()

    def testNumericIdxIndexSummary(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'age').equal(['numRanges', 1, 'numEntries', 1,
                                                                           'lastDocId', 1, 'revisionId', 0,
                                                                           'emptyLeaves', 0, 'RootMaxDepth', 0])

        self.env.expect('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'age').equal(['numRanges', 1, 'numEntries', 1,
                                                                           'lastDocId', 1, 'revisionId', 0,
                                                                           'emptyLeaves', 0, 'RootMaxDepth', 0])

    def testUnexistsNumericIndexSummary(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'age1').raiseError()

    def testNumericIndexSummaryInvalidIdxName(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx1', 'age').raiseError()

    def testNumericIndexSummaryWrongArity(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx1').raiseError()

    def testDumpSuffixWrongArity(self):
        self.env.expect('FT.DEBUG', 'DUMP_SUFFIX_TRIE', 'idx1', 'no_suffix').error()

    def testTTLcommands(self):
        num_indexes = len(self.env.cmd('FT._LIST'))
        self.env.expect('FT.DEBUG', 'TTL', 'non-existing').error().contains('Unknown index name')
        self.env.expect('FT.DEBUG', 'TTL_PAUSE', 'non-existing').error().contains('Unknown index name')
        self.env.expect('FT.DEBUG', 'TTL_EXPIRE', 'non-existing').error().contains('Unknown index name')
        self.env.expect('FT.DEBUG', 'TTL', 'idx').error().contains('Index is not temporary')
        self.env.expect('FT.DEBUG', 'TTL_PAUSE', 'idx').error().contains('Index is not temporary')
        self.env.expect('FT.DEBUG', 'TTL_EXPIRE', 'idx').error().contains('Index is not temporary')

        self.env.expect('FT.CREATE', 'idx_temp', 'TEMPORARY', 3600, 'PREFIX', 1, 'temp:', 'SCHEMA', 'name', 'TEXT').ok()
        # Should pass if command is called within 10 minutes from creation.
        self.env.assertGreater(self.env.cmd('FT.DEBUG', 'TTL', 'idx_temp'), 3000) # It should be close to 3600.
        self.env.expect('FT.DEBUG', 'TTL_PAUSE', 'idx_temp').ok()
        self.env.expect('FT.DEBUG', 'TTL_PAUSE', 'idx_temp').error().contains('Index does not have a timer')
        self.env.expect('FT.DEBUG', 'TTL_EXPIRE', 'idx_temp').ok()
        with TimeLimit(10):
            while len(self.env.cmd('FT._LIST')) > num_indexes:
                pass
    def testVecsimInfo_badParams(self):

        # Scenerio1: Vecsim Index scheme with vector type with invalid parameter

        # HNSW parameters the causes an execution throw (M > SIZE_MAX/2)
        HALF_SIZE_MAX = 9223372036854775805
        M = HALF_SIZE_MAX + 1
        print(M)
        dim = 2
        self.env.expect('FT.CREATE', 'vectorIdx','SCHEMA','v', 'VECTOR', 'HNSW', '8',
                    'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', M).ok()
        self.env.expect('ft.debug', 'VECSIM_INFO', 'vectorIdx','v').error() \
            .contains("Vector index not found")

def test_yield_counter(env):
    # Giving wrong arity
    env.expect(debug_cmd(), 'YIELDS_ON_LOAD_COUNTER','ExtraARG1','ExtraARG2').error()\
    .contains('wrong number of arguments')
    # Giving wrong subcommand
    env.expect(debug_cmd(), 'YIELDS_ON_LOAD_COUNTER', 'NOT_A_COMMAND').error()\
    .contains('Unknown subcommand')

def testSetMaxScannedDocs(env: Env):

    # Test setting max scanned docs of background scan
    # Insert 10 documents
    num_docs = 10
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)
    # Create a baseline index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexFinishScan(env)
    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx', '*')[0]
    env.assertEqual(docs_in_index, num_docs)

    # Check error handling
    # Giving invalid argument
    env.expect(bgScanCommand(), 'SET_MAX_SCANNED_DOCS', 'notAnumber').error()\
    .contains("Invalid argument for 'SET_MAX_SCANNED_DOCS'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_MAX_SCANNED_DOCS').error()\
    .contains('wrong number of arguments')


    # Set max scanned docs to 5
    max_scanned = 5
    env.expect(bgScanCommand(), 'SET_MAX_SCANNED_DOCS', max_scanned).ok()

    # Create a new index
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexFinishScan(env, 'idx2')
    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx2', '*')[0]
    env.assertEqual(docs_in_index, max_scanned)

    # Reset max scanned docs by setting negative value
    env.expect(bgScanCommand(), 'SET_MAX_SCANNED_DOCS', -1).ok()
    # Create a new index
    env.expect('FT.CREATE', 'idx3', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexFinishScan(env, 'idx3')
    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx3', '*')[0]
    env.assertEqual(docs_in_index, num_docs)

def testPauseOnScannedDocs(env: Env):
    num_docs = 10
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

    # Create a baseline index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexFinishScan(env)
    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx', '*')[0]
    env.assertEqual(docs_in_index, num_docs)


    # Check error handling
    # Giving invalid argument
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 'notAnumber').error()\
    .contains("Invalid argument for 'SET_PAUSE_ON_SCANNED_DOCS'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS').error()\
    .contains('wrong number of arguments')

    # Set max scanned docs to 5
    pause_on_scanned = 5
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', pause_on_scanned).ok()

    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexPauseScan(env, 'idx2')

    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx2', '*')[0]
    env.assertEqual(docs_in_index, pause_on_scanned)

    # Get indexing info
    idx_info = index_info(env, 'idx2')
    env.assertEqual(idx_info['indexing'], '1')
    env.assertEqual(idx_info['percent_indexed'], f'{pause_on_scanned/num_docs}')

    # Check resume error handling
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').error()\
    .contains('wrong number of arguments')

    # Resume indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx2')
    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx2', '*')[0]
    env.assertEqual(docs_in_index, num_docs)

def testPauseBeforeScan(env: Env):
    num_docs = 10
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

    # Create a baseline index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexFinishScan(env)

    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx', '*')[0]
    env.assertEqual(docs_in_index, num_docs)

    # Check error handling
    # Giving invalid argument
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'notTrue').error()\
    .contains("Invalid argument for 'SET_PAUSE_BEFORE_SCAN'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN').error()\
    .contains('wrong number of arguments')

    # Set pause before scan
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()

    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexStatus(env, 'NEW', 'idx2')

    idx_info = index_info(env, 'idx2')
    env.assertEqual(idx_info['indexing'], '1')
    # If is indexing, but debug scanner status is NEW, it means that the scanner is paused before scan

    # Resume indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx2')
    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx2', '*')[0]
    env.assertEqual(docs_in_index, num_docs)

def testDebugScannerStatus(env: Env):
    num_docs = 10
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
    pause_on_scanned = 5
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', pause_on_scanned).ok()
    max_scanned = 7
    env.expect(bgScanCommand(), 'SET_MAX_SCANNED_DOCS', max_scanned).ok()

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexStatus(env, 'NEW')
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexPauseScan(env, 'idx')
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx')
    # When scan is done, the scanner is freed
    checkDebugScannerStatusError(env, 'idx', 'Scanner is not initialized')

    # Test error handling
    # Giving non existing index name
    checkDebugScannerStatusError(env, 'non_existing', 'Unknown index name')
    # Giving invalid argument to debug scanner control command
    env.expect(bgScanCommand(), 'NOT_A_COMMAND', 'notTrue').error()\
    .contains("Invalid command for 'BG_SCAN_CONTROLLER'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'GET_DEBUG_SCANNER_STATUS').error()\
    .contains('wrong number of arguments')

    # Test OOM pause
    # Insert more docs to ensure un-flakey test
    extra_docs = 90
    for i in range(num_docs,extra_docs+num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

    # Remove previous debug scanner settings
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'false').ok()
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 0).ok()
    env.expect(bgScanCommand(), 'SET_MAX_SCANNED_DOCS', 0).ok()
    # Set OOM pause
    # Change the memory limit to 80% so it can be tested without colliding with redis memory limit
    env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80').ok()

    env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
    # Set tight memory limit to trigger OOM
    set_tight_maxmemory_for_oom(env, 0.85)
    # Create an index and expect OOM pause
    env.expect('FT.CREATE', 'idx_oom', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexStatus(env, 'PAUSED_ON_OOM','idx_oom')
    # Resume indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

def testPauseOnOOM(env: Env):
    # Change the memory limit to 80% so it can be tested without colliding with redis memory limit
    env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80').ok()
    num_docs = 1000
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

    # Check error handling
    # Giving invalid argument
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'notAbool').error()\
    .contains("Invalid argument for 'SET_PAUSE_ON_OOM'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM').error()\
    .contains('wrong number of arguments')

    # Set pause on OOM
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
    # Set pause after quarter of the docs were scanned
    num_docs_scanned = num_docs//4
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()

    # Baseline failed scans due to OOM
    failed_idx_oom = env.cmd('INFO', 'modules')['search_OOM_indexing_failures_indexes_count']

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexPauseScan(env, 'idx')

    # At this point num_docs_scanned were scanned
    # Now we set the tight memory limit
    set_tight_maxmemory_for_oom(env, 0.85)
    # After we resume, an OOM should trigger
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

    # At this point, the index should be paused on OOM
    # Wait for INFO metric "OOM_indexing_failures_indexes_count" to increment
    # Note: While there are other ways to check if OOM occurred, this is the most direct way,
    #       as the metric is based directly on the spec field "scan_failed_OOM"
    while (env.cmd('INFO', 'modules')['search_OOM_indexing_failures_indexes_count'])!=(failed_idx_oom+1):
        time.sleep(0.1)

    # At this point, we are certain an OOM occurred, but the index scanning should be paused
    # We can verify this (without using the scanner status to maintain independency) by checking "indexing" entry in ft.info
    idx_info = index_info(env, 'idx')
    env.assertEqual(idx_info['indexing'], '1')
    # The percent index should be close to 0.25 as we set the tight memory limit after 25% of the docs were scanned
    env.assertAlmostEqual(float(idx_info['percent_indexed']), 0.25, delta=0.1)

    # Resume indexing for the sake of completeness
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

    # Test giving false to pause on OOM for coverage
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'false').ok()

def test_terminate_bg_pool(env):
    # Giving wrong arity
    env.expect(bgScanCommand(), 'TERMINATE_BG_POOL','ExtraARG').error()\
    .contains('wrong number of arguments')
    # Test OK returned only after scan complete
    # Insert 1000 docs
    num_docs = 1000
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    env.expect(bgScanCommand(), 'TERMINATE_BG_POOL').ok()
    # Check if the scan is finished
    env.assertEqual(index_info(env, 'idx')['indexing'], '0')

def test_pause_before_oom_retry(env):
    # Check error handling
    # Giving invalid argument
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'notAbool').error()\
    .contains("Invalid argument for 'SET_PAUSE_BEFORE_OOM_RETRY'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY').error()\
    .contains('wrong number of arguments')

def test_update_debug_scanner_config(env):
    # Check error handling
    # Giving wrong arity
    env.expect(bgScanCommand(), 'DEBUG_SCANNER_UPDATE_CONFIG').error()\
    .contains('wrong number of arguments')

    num_docs = 10
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndex(env, 'idx')

    # When scan is done, the scanner is freed
    checkDebugScannerUpdateError(env, 'idx', 'Scanner is not initialized')

    # Test error handling
    # Giving non existing index name
    checkDebugScannerUpdateError(env, 'non_existing', 'Unknown index name')
