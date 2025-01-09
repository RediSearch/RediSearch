from common import *

class TestDebugCommands(object):

    def __init__(self):
        module_args = 'MT_MODE MT_MODE_FULL WORKER_THREADS 2' if MT_BUILD else ''
        self.env = Env(testName="testing debug commands", moduleArgs=module_args)
        self.env.skipOnCluster()
        self.env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
                        'name', 'TEXT', 'SORTABLE',
                        'age', 'NUMERIC', 'SORTABLE',
                        't', 'TAG', 'SORTABLE',
                        'v', 'VECTOR', 'HNSW', 6, 'DIM', 2, 'DISTANCE_METRIC', 'L2', 'TYPE', 'float32').ok()
        waitForIndex(self.env, 'idx')
        self.env.expect('HSET', 'doc1', 'name', 'meir', 'age', '34', 't', 'test').equal(3)
        self.env.cmd('SET', 'foo', 'bar')

    def testDebugWrongArity(self):
        self.env.expect('FT.DEBUG', 'dump_invidx').raiseError().contains('wrong number of arguments')
        self.env.expect('FT.DEBUG').raiseError().contains('wrong number of arguments')

    def testDebugUnknownSubcommand(self):
        self.env.expect('FT.DEBUG', 'unknown').raiseError().equal('subcommand was not found')

    def testDebugHelp(self):
        err_msg = 'wrong number of arguments'
        help_list = [
            "DUMP_INVIDX",
            "DUMP_NUMIDX",
            "DUMP_NUMIDXTREE",
            "DUMP_TAGIDX",
            "INFO_TAGIDX",
            "DUMP_GEOMIDX",
            "DUMP_PREFIX_TRIE",
            "IDTODOCID",
            "DOCIDTOID",
            "DOCINFO",
            "DUMP_PHONETIC_HASH",
            "DUMP_SUFFIX_TRIE",
            "DUMP_TERMS",
            "INVIDX_SUMMARY",
            "NUMIDX_SUMMARY",
            "SPEC_INVIDXES_INFO",
            "GC_FORCEINVOKE",
            "GC_FORCEBGINVOKE",
            "GC_CLEAN_NUMERIC",
            "GC_STOP_SCHEDULE",
            "GC_CONTINUE_SCHEDULE",
            "GC_WAIT_FOR_JOBS",
            "GIT_SHA",
            "TTL",
            "TTL_PAUSE",
            "TTL_EXPIRE",
            "VECSIM_INFO",
            "DELETE_LOCAL_CURSORS",
        ]
        if MT_BUILD:
            help_list.append('WORKER_THREADS')
        self.env.expect('FT.DEBUG', 'help').equal(help_list)

        for cmd in help_list:
            if cmd in ['GIT_SHA', 'DUMP_PREFIX_TRIE', 'GC_WAIT_FOR_JOBS', 'DELETE_LOCAL_CURSORS']:
                # 'GIT_SHA' and 'DUMP_PREFIX_TRIE' do not return err_msg
                continue
            self.env.expect('FT.DEBUG', cmd).raiseError().contains(err_msg)

    def testDocInfo(self):
        rv = self.env.cmd('ft.debug', 'docinfo', 'idx', 'doc1')
        self.env.assertEqual(['internal_id', 1, 'flags', '(0xc):HasSortVector,HasOffsetVector,',
                              'score', '1', 'num_tokens', 1, 'max_freq', 1, 'refcount', 1, 'sortables',
                               [['index', 0, 'field', 'name AS name', 'value', 'meir'],
                                ['index', 1, 'field', 'age AS age', 'value', '34'],
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
                                                                            32851, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

        self.env.expect('FT.DEBUG', 'INVIDX_SUMMARY', 'idx', 'meir').equal(['numDocs', 1, 'numEntries', 1, 'lastId', 1, 'flags',
                                                                            32851, 'numberOfBlocks', 1, 'blocks',
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
        self.env.expect('FT.DEBUG', 'DUMP_SUFFIX_TRIE', 'idx1', 'no_suffix').raiseError()

    def testGCStopAndContinueSchedule(self):
        self.env.expect('FT.DEBUG', 'GC_STOP_SCHEDULE', 'non-existing').error().contains('Unknown index name')
        self.env.expect('FT.DEBUG', 'GC_CONTINUE_SCHEDULE', 'non-existing').error().contains('Unknown index name')
        self.env.expect('FT.DEBUG', 'GC_CONTINUE_SCHEDULE', 'idx').error().contains('GC is already running periodically')
        self.env.expect('FT.DEBUG', 'GC_STOP_SCHEDULE', 'idx').ok()
        self.env.expect('FT.DEBUG', 'GC_CONTINUE_SCHEDULE', 'idx').ok()

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

    def testStopAndResumeWorkersPool(self):
        if not MT_BUILD:
            self.env.skip()
        self.env.expect('FT.DEBUG', 'WORKER_THREADS').error().contains("wrong number of arguments for"
                                                                              " 'FT.DEBUG' command")
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'invalid').error().contains(
            "Invalid argument for 'WORKER_THREADS' subcommand")
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'pause').ok()
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'pause').error()\
            .contains("Operation failed: workers thread pool doesn't exists or is not running")
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'resume').ok()
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'resume').error()\
            .contains("Operation failed: workers thread pool doesn't exists or is already running")

    def testWorkersPoolDrain(self):
        if not MT_BUILD:
            self.env.skip()
        # test stats and drain
        conn = self.env.getConnection()
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'pause').ok()
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'drain').error() \
            .contains("Operation failed: workers thread pool is not running")
        self.env.expect('HSET', 'doc1', 'name', 'meir', 'age', '34', 't', 'test',
                        'v', create_np_array_typed([1, 2]).tobytes()).equal(1)

        # Expect 1 pending ingest job.
        stats = getWorkersThpoolStats(self.env)
        self.env.assertEqual(stats, {'totalJobsDone': 0,
                                     'totalPendingJobs': 1,
                                     'highPriorityPendingJobs': 0,
                                     'lowPriorityPendingJobs': 1})

        # After resuming, expect that the job is done.
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'resume').ok()
        self.env.expect('FT.DEBUG', 'WORKER_THREADS', 'drain').ok()
        stats = getWorkersThpoolStats(self.env)
        self.env.assertEqual(stats, {'totalJobsDone': 1,
                                     'totalPendingJobs': 0,
                                     'highPriorityPendingJobs': 0,
                                     'lowPriorityPendingJobs': 0})

@skip(cluster=True)
def testSpecIndexesInfo(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()

    expected_reply = {
        "inverted_indexes_dict_size": 0,
        "inverted_indexes_memory": 0,
    }
    # Sanity check - empty spec
    debug_output = env.cmd(debug_cmd(), 'SPEC_INVIDXES_INFO', 'idx')
    env.assertEqual(to_dict(debug_output), expected_reply)

    # Add a document
    env.expect('HSET', 'doc1', 'n', 1).equal(1)
    expected_reply["inverted_indexes_dict_size"] = 1

    # assuming the document doesn't exceed the initial block size
    expected_reply["inverted_indexes_memory"] = getInvertedIndexInitialSize(env, ['NUMERIC'])
    debug_output = env.cmd(debug_cmd(), 'SPEC_INVIDXES_INFO', 'idx')
    env.assertEqual(to_dict(debug_output), expected_reply)

def testVecsimInfo_badParams(env: Env):

    # Scenerio1: Vecsim Index scheme with vector type with invalid parameter

    # HNSW parameters the causes an execution throw (M > UINT16_MAX)
    UINT16_MAX = 2**16
    M = UINT16_MAX + 1
    dim = 2
    env.expect('FT.CREATE', 'idx','SCHEMA','v', 'VECTOR', 'HNSW', '8',
                'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', M).ok()
    env.expect(debug_cmd(), 'VECSIM_INFO', 'idx','v').error() \
        .contains("Can't open vector index")
