from common import *
import threading
import time

class TestDebugCommands(object):

    def __init__(self):
        skipTest(cluster=True)
        self.workers_count = 2
        self.env = Env(testName="testing debug commands", moduleArgs=f'WORKERS {self.workers_count}')
        self.env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
                        'name', 'TEXT', 'SORTABLE',
                        'age', 'NUMERIC', 'SORTABLE',
                        't', 'TAG', 'SORTABLE',
                        'v', 'VECTOR', 'HNSW', 6, 'DIM', 2, 'DISTANCE_METRIC', 'L2', 'TYPE', 'float32').ok()
        waitForIndex(self.env, 'idx')
        self.env.expect('HSET', 'doc1', 'name', 'meir', 'age', '34', 't', 'test').equal(3)
        self.env.cmd('SET', 'foo', 'bar')

    def testDebugWrongArity(self):
        self.env.expect(debug_cmd(), 'dump_invidx').error().contains('wrong number of arguments')
        self.env.expect(debug_cmd()).error().contains('wrong number of arguments')

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
            "DUMP_HNSW",
            "SET_MONITOR_EXPIRATION",
            "WORKERS",
            'BG_SCAN_CONTROLLER',
            "INDEXES",
            "INFO",
            'GET_HIDE_USER_DATA_FROM_LOGS',
            'YIELDS_ON_LOAD_COUNTER',
            'INDEXER_SLEEP_BEFORE_YIELD_MICROS',
            'QUERY_CONTROLLER',
            'DUMP_SCHEMA',
            'FT.AGGREGATE',
            '_FT.AGGREGATE',
            'FT.SEARCH',
            '_FT.SEARCH',
            'FT.HYBRID',
            '_FT.HYBRID',
        ]
        coord_help_list = ['SHARD_CONNECTION_STATES', 'PAUSE_TOPOLOGY_UPDATER', 'RESUME_TOPOLOGY_UPDATER', 'CLEAR_PENDING_TOPOLOGY']
        help_list.extend(coord_help_list)

        self.env.expect(debug_cmd(), 'help').equal(help_list)

        arity_2_cmds = ['GIT_SHA', 'DUMP_PREFIX_TRIE', 'GC_WAIT_FOR_JOBS', 'DELETE_LOCAL_CURSORS', 'SHARD_CONNECTION_STATES',
                        'PAUSE_TOPOLOGY_UPDATER', 'RESUME_TOPOLOGY_UPDATER', 'CLEAR_PENDING_TOPOLOGY', 'INFO', 'INDEXES', 'GET_HIDE_USER_DATA_FROM_LOGS', 'YIELDS_ON_LOAD_COUNTER']
        for cmd in [c for c in help_list if c not in arity_2_cmds]:
            self.env.expect(debug_cmd(), cmd).error().contains(err_msg)

    def testDocInfo(self):
        rv = self.env.cmd(debug_cmd(), 'docinfo', 'idx', 'doc1', 'REVEAL')
        self.env.assertEqual(['internal_id', 1, 'flags', '(0xc):HasSortVector,HasOffsetVector,',
                              'score', '1', 'num_tokens', 1, 'max_freq', 1, 'refcount', 1, 'sortables',
                               [['index', 0, 'field', 'name AS name', 'value', 'meir'],
                                ['index', 1, 'field', 'age AS age', 'value', '34'],
                                ['index', 2, 'field', 't AS t', 'value', 'test']]], rv)
        self.env.expect(debug_cmd(), 'docinfo', 'idx').error()
        self.env.expect(debug_cmd(), 'docinfo', 'idx', 'doc2').error()

    def testDumpInvertedIndex(self):
        self.env.expect(debug_cmd(), 'dump_invidx', 'idx', 'meir').equal([1])
        self.env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'meir').equal([1])

    def testDumpInvertedIndexWrongArity(self):
        self.env.expect(debug_cmd(), 'dump_invidx', 'idx').error()

    def testDumpUnexistsInvertedIndex(self):
        self.env.expect(debug_cmd(), 'dump_invidx', 'idx', 'meir1').error()

    def testDumpInvertedIndexInvalidSchema(self):
        self.env.expect(debug_cmd(), 'dump_invidx', 'idx1', 'meir').error()

    def testDumpNumericIndex(self):
        self.env.expect(debug_cmd(), 'dump_numidx', 'idx', 'age').equal([[1]])
        self.env.expect(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'age').equal([[1]])

    def testDumpNumericIndexWrongArity(self):
        self.env.expect(debug_cmd(), 'dump_numidx', 'idx').error()

    def testDumpUnexistsNumericIndex(self):
        self.env.expect(debug_cmd(), 'dump_numidx', 'idx', 'ag1').error()

    def testDumpNumericIndexInvalidSchema(self):
        self.env.expect(debug_cmd(), 'dump_numidx', 'idx1', 'age').error()

    def testDumpNumericIndexInvalidKeyType(self):
        self.env.expect(debug_cmd(), 'dump_numidx', 'foo', 'age').error()

    def testDumpTagIndex(self):
        self.env.expect(debug_cmd(), 'dump_tagidx', 'idx', 't').equal([['test', [1]]])
        self.env.expect(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't').equal([['test', [1]]])

    def testDumpTagIndexWrongArity(self):
        self.env.expect(debug_cmd(), 'dump_tagidx', 'idx').error()

    def testDumpUnexistsTagIndex(self):
        self.env.expect(debug_cmd(), 'dump_tagidx', 'idx', 't1').error()

    def testDumpTagIndexInvalidKeyType(self):
        self.env.expect(debug_cmd(), 'dump_tagidx', 'foo', 't1').error()

    def testDumpTagIndexInvalidSchema(self):
        self.env.expect(debug_cmd(), 'dump_tagidx', 'idx1', 't').error()

    def testInfoTagIndex(self):
        self.env.expect(debug_cmd(), 'info_tagidx', 'idx', 't').equal(['num_values', 1])
        self.env.expect(debug_cmd(), 'INFO_TAGIDX', 'idx', 't').equal(['num_values', 1])
        self.env.expect(debug_cmd(), 'INFO_TAGIDX', 'idx', 't', 'dump_id_entries').equal(['num_values', 1, 'values', []])
        self.env.expect(debug_cmd(), 'INFO_TAGIDX', 'idx', 't', 'count_value_entries').equal(['num_values', 1, 'values', []])
        self.env.expect(debug_cmd(), 'INFO_TAGIDX', 'idx', 't', 'dump_id_entries', 'limit', '1') \
            .equal(['num_values', 1, 'values', [['value', 'test', 'num_entries', 1, 'num_blocks', 1, 'entries', [1]]]] )
        self.env.expect(debug_cmd(), 'INFO_TAGIDX', 'idx', 't', 'count_value_entries', 'limit', '1') \
            .equal(['num_values', 1, 'values', [['value', 'test', 'num_entries', 1, 'num_blocks', 1]]])
        self.env.expect(debug_cmd(), 'INFO_TAGIDX', 'idx', 't', 'count_value_entries', 'limit', 'abc').error()

    def testInfoTagIndexWrongArity(self):
        self.env.expect(debug_cmd(), 'info_tagidx', 'idx').error()

    def testInfoUnexistsTagIndex(self):
        self.env.expect(debug_cmd(), 'info_tagidx', 'idx', 't1').error()

    def testInfoTagIndexInvalidKeyType(self):
        self.env.expect(debug_cmd(), 'info_tagidx', 'foo', 't1').error()

    def testInfoTagIndexInvalidSchema(self):
        self.env.expect(debug_cmd(), 'info_tagidx', 'idx1', 't').error()

    def testDocIdToId(self):
        self.env.expect(debug_cmd(), 'docidtoid', 'idx', 'doc1').equal(1)
        self.env.expect(debug_cmd(), 'DOCIDTOID', 'idx', 'doc1').equal(1)

    def testDocIdToIdOnUnexistingDoc(self):
        self.env.expect(debug_cmd(), 'docidtoid', 'idx', 'doc').equal(0)

    def testIdToDocId(self):
        self.env.expect(debug_cmd(), 'idtodocid', 'idx', '1').equal('doc1')
        self.env.expect(debug_cmd(), 'IDTODOCID', 'idx', '1').equal('doc1')

    def testIdToDocIdOnUnexistingId(self):
        self.env.expect(debug_cmd(), 'idtodocid', 'idx', '2').error().equal('document was removed')
        self.env.expect(debug_cmd(), 'idtodocid', 'idx', 'docId').error().equal('bad id given')

    def testDumpPhoneticHash(self):
        self.env.expect(debug_cmd(), 'dump_phonetic_hash', 'test').equal(['<TST', '<TST'])
        self.env.expect(debug_cmd(), 'DUMP_PHONETIC_HASH', 'test').equal(['<TST', '<TST'])

    def testDumpPhoneticHashWrongArity(self):
        self.env.expect(debug_cmd(), 'dump_phonetic_hash').error()

    def testDumpTerms(self):
        self.env.expect(debug_cmd(), 'dump_terms', 'idx').equal(['meir'])
        self.env.expect(debug_cmd(), 'DUMP_TERMS', 'idx').equal(['meir'])

    def testDumpTermsWrongArity(self):
        self.env.expect(debug_cmd(), 'dump_terms').error()

    def testDumpTermsUnknownIndex(self):
        self.env.expect(debug_cmd(), 'dump_terms', 'idx1').error()

    def testDumpSchema(self):
        self.env.expect(debug_cmd(), 'dump_schema', NEVER_DECODE=True).error().contains('wrong number of arguments')
        self.env.expect(debug_cmd(), 'dump_schema', 'idx1', NEVER_DECODE=True).error().contains('Can not create a search ctx')
        self.env.expect(debug_cmd(), 'dump_schema', 'idx', NEVER_DECODE=True).noError().equal([ANY, ANY])

    def testInvertedIndexSummary(self):
        self.env.expect(debug_cmd(), 'invidx_summary', 'idx', 'meir').equal(['numDocs', 1, 'numEntries', 1, 'lastId', 1, 'flags',
                                                                            557139, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

        self.env.expect(debug_cmd(), 'INVIDX_SUMMARY', 'idx', 'meir').equal(['numDocs', 1, 'numEntries', 1, 'lastId', 1, 'flags',
                                                                            557139, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

    def testUnexistsInvertedIndexSummary(self):
        self.env.expect(debug_cmd(), 'invidx_summary', 'idx', 'meir1').error()

    def testInvertedIndexSummaryInvalidIdxName(self):
        self.env.expect(debug_cmd(), 'invidx_summary', 'idx1', 'meir').error()

    def testInvertedIndexSummaryWrongArity(self):
        self.env.expect(debug_cmd(), 'invidx_summary', 'idx1').error()

    def testNumericIdxIndexSummary(self):
        self.env.expect(debug_cmd(), 'numidx_summary', 'idx', 'age').equal([
            'numRanges', 1, 'numLeaves', 1, 'numEntries', 1, 'lastDocId', 1, 'revisionId', 0,
            'emptyLeaves', 0, 'RootMaxDepth', 0, 'MemoryUsage', ANY,
        ])

        self.env.expect(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'age').equal([
            'numRanges', 1, 'numLeaves', 1, 'numEntries', 1, 'lastDocId', 1, 'revisionId', 0,
            'emptyLeaves', 0, 'RootMaxDepth', 0, 'MemoryUsage', ANY
        ])

    def testUnexistsNumericIndexSummary(self):
        self.env.expect(debug_cmd(), 'numidx_summary', 'idx', 'age1').error()

    def testNumericIndexSummaryInvalidIdxName(self):
        self.env.expect(debug_cmd(), 'numidx_summary', 'idx1', 'age').error()

    def testNumericIndexSummaryWrongArity(self):
        self.env.expect(debug_cmd(), 'numidx_summary', 'idx1').error()

    def testDumpSuffixWrongArity(self):
        self.env.expect(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx1', 'no_suffix').error()

    def testGCStopAndContinueSchedule(self):
        self.env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'non-existing').error().contains('Unknown index name')
        self.env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'non-existing').error().contains('Unknown index name')
        self.env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').error().contains('GC is already running periodically')
        self.env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
        self.env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()

    def testTTLcommands(self):
        num_indexes = len(self.env.cmd('FT._LIST'))
        self.env.expect(debug_cmd(), 'TTL', 'non-existing').error().contains('Unknown index name')
        self.env.expect(debug_cmd(), 'TTL_PAUSE', 'non-existing').error().contains('Unknown index name')
        self.env.expect(debug_cmd(), 'TTL_EXPIRE', 'non-existing').error().contains('Unknown index name')
        self.env.expect(debug_cmd(), 'TTL', 'idx').error().contains('Index is not temporary')
        self.env.expect(debug_cmd(), 'TTL_PAUSE', 'idx').error().contains('Index is not temporary')
        self.env.expect(debug_cmd(), 'TTL_EXPIRE', 'idx').error().contains('Index is not temporary')

        self.env.expect('FT.CREATE', 'idx_temp', 'TEMPORARY', 3600, 'PREFIX', 1, 'temp:', 'SCHEMA', 'name', 'TEXT').ok()
        # Should pass if command is called within 10 minutes from creation.
        self.env.assertGreater(self.env.cmd(debug_cmd(), 'TTL', 'idx_temp'), 3000) # It should be close to 3600.
        self.env.expect(debug_cmd(), 'TTL_PAUSE', 'idx_temp').ok()
        self.env.expect(debug_cmd(), 'TTL_PAUSE', 'idx_temp').error().contains('Index does not have a timer')
        self.env.expect(debug_cmd(), 'TTL_EXPIRE', 'idx_temp').ok()
        with TimeLimit(10):
            while len(self.env.cmd('FT._LIST')) > num_indexes:
                pass

    def testStopAndResumeWorkersPool(self):
        self.env.expect(debug_cmd(), 'WORKERS').error().contains(
            f"wrong number of arguments for '{debug_cmd()}|WORKERS' command")
        self.env.expect(debug_cmd(), 'WORKERS', 'invalid').error().contains(
            "Invalid argument for 'WORKERS' subcommand")
        self.env.expect(debug_cmd(), 'WORKERS', 'pause').ok()
        self.env.expect(debug_cmd(), 'WORKERS', 'pause').error()\
            .contains("Operation failed: workers thread pool doesn't exists or is not running")
        self.env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
        self.env.expect(debug_cmd(), 'WORKERS', 'resume').error()\
            .contains("Operation failed: workers thread pool doesn't exists or is already running")

    def testWorkersPoolDrain(self):
        # test stats and drain
        orig_stats = getWorkersThpoolStats(self.env)
        self.env.expect(debug_cmd(), 'WORKERS', 'pause').ok()
        self.env.expect(debug_cmd(), 'WORKERS', 'drain').error() \
            .contains("Operation failed: workers thread pool is not running")
        self.env.expect('HSET', 'doc1', 'name', 'meir', 'age', '34', 't', 'test',
                        'v', create_np_array_typed([1, 2]).tobytes()).equal(1)

        # Expect another 1 pending ingest job.
        stats = getWorkersThpoolStats(self.env)
        self.env.assertEqual(stats, {'totalJobsDone': orig_stats['totalJobsDone'],
                                     'totalPendingJobs': orig_stats['totalPendingJobs']+1,
                                     'highPriorityPendingJobs': orig_stats['highPriorityPendingJobs'],
                                     'lowPriorityPendingJobs': orig_stats['lowPriorityPendingJobs']+1,
                                     'numThreadsAlive': self.workers_count})

        # After resuming, expect that the job is done.
        orig_stats = stats
        self.env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
        self.env.expect(debug_cmd(), 'WORKERS', 'drain').ok()
        stats = getWorkersThpoolStats(self.env)
        self.env.assertEqual(stats, {'totalJobsDone': orig_stats['totalJobsDone']+1,
                                     'totalPendingJobs': orig_stats['totalPendingJobs']-1,
                                     'highPriorityPendingJobs': orig_stats['highPriorityPendingJobs'],
                                     'lowPriorityPendingJobs': orig_stats['lowPriorityPendingJobs']-1,
                                     'numThreadsAlive': self.workers_count})

    def testWorkersNumThreads(self):
        # test stats and drain
        self.env.expect(debug_cmd(), 'WORKERS', 'n_threads').equal(self.workers_count)


@skip(cluster=True, no_json=True)
def testDumpHNSW(env):
    # Note that this test has its own env as it relies on the specific doc ids in the index created.
    # Had we used this test in the TestDebugCommands env, a background indexing would have been triggered, and
    # with high probability, some documents would be indexed BEFORE the background scan would end, and it will be
    # overwritten (same doc, but with a new doc id...)
    env.cmd('FT.CREATE temp-idx ON JSON prefix 1 _ SCHEMA '
                 '$.v_HNSW AS v_HNSW VECTOR HNSW 6 DIM 2 DISTANCE_METRIC L2 TYPE FLOAT32 '
                 '$.v_HNSW_multi[*] AS v_HNSW_multi VECTOR HNSW 6 DIM 2 DISTANCE_METRIC L2 TYPE FLOAT32 '
                 '$.v_flat AS v_flat VECTOR FLAT 6 DIM 2 DISTANCE_METRIC L2 TYPE FLOAT32 ')
    env.cmd(*['JSON.SET', '_doc1', '$', '{\"v_HNSW\":[1, 1], \"v_HNSW_multi\":[[1, 1], [2, 2]], \"v_flat\":[1, 1]}'])
    env.cmd(*['JSON.SET', '_doc2', '$', '{\"v_HNSW\":[3, 3], \"v_HNSW_multi\":[[3, 3], [4, 4]], \"v_flat\":[3, 3]}'])
    env.cmd(*['JSON.SET', '_doc3', '$', '{\"v_HNSW_multi\":[[5, 5], [6, 6]], \"v_flat\":[5, 5]}'])
    env.expect(index_info(env, 'temp-idx')['num_docs'], 3)

    # Test error handling
    env.expect(debug_cmd(), 'DUMP_HNSW', 'temp-idx').error() \
        .contains(f"wrong number of arguments for '{debug_cmd()}|DUMP_HNSW' command")
    env.expect(debug_cmd(), 'DUMP_HNSW', 'bad_idx', 'v').error() \
        .contains("Can not create a search ctx")
    env.expect(debug_cmd(), 'DUMP_HNSW', 'temp-idx', 'bad_vec_field').error() \
        .contains("Vector index not found")
    env.expect(debug_cmd(), 'DUMP_HNSW', 'temp-idx', 'v_flat').error() \
        .contains("Vector index is not an HNSW index")
    env.expect(debug_cmd(), 'DUMP_HNSW', 'temp-idx', 'v_HNSW_multi').error() \
        .contains("Command not supported for HNSW multi-value index")
    env.expect(debug_cmd(), 'DUMP_HNSW', 'temp-idx', 'v_HNSW', '_bad_doc_name').error() \
        .contains("The given key does not exist in index")

    # Test valid scenarios - with and without specifying a specific document (dump for all if doc is not provided).
    env.expect(debug_cmd(), 'DUMP_HNSW', 'temp-idx', 'v_HNSW', '_doc1').\
        equal(['Doc id', 1, ['Neighbors in level 0', 2]])

    env.expect(debug_cmd(), 'DUMP_HNSW', 'temp-idx', 'v_HNSW').\
        equal([['Doc id', 1, ['Neighbors in level 0', 2]], ['Doc id', 2, ['Neighbors in level 0', 1]],
               "Doc id 3 doesn't contain the given field"])

@skip(cluster=False)
def testCoordDebug(env: Env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Sanity check - regular debug command
    env.expect(debug_cmd(), 'DUMP_TERMS', 'idx').equal([])
    # Test Coordinator only debug command
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').noError()
    # Look for the coordinator only command in the help command
    env.expect(debug_cmd(), 'HELP').contains('SHARD_CONNECTION_STATES')

    # Test topology updater pause and resume
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').error().contains('Topology updater is already paused')
    env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').ok()
    env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').error().contains('Topology updater is already running')

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

    # adding the document will create a new index block (48 bytes) with 1 byte of buffer capacity
    expected_reply["inverted_indexes_memory"] = getInvertedIndexInitialSize(env, ['NUMERIC']) + 49
    debug_output = env.cmd(debug_cmd(), 'SPEC_INVIDXES_INFO', 'idx')
    env.assertEqual(to_dict(debug_output), expected_reply)

def testVecsimInfo_badParams(env: Env):

    # Scenerio1: Vecsim Index scheme with vector type with invalid parameter

    # HNSW parameters the causes an execution throw (M > UINT16_MAX)
    UINT16_MAX = 2**16
    M = UINT16_MAX + 1
    dim = 2
    env.expect('FT.CREATE', 'idx','SCHEMA','v', 'VECTOR', 'HNSW', '8',
                'TYPE', 'FLOAT16', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', M).ok()
    env.expect(debug_cmd(), 'VECSIM_INFO', 'idx','v').error() \
        .contains("Can't open vector index")

def testHNSWdump_badParams(env: Env):
    # Scenerio1: Vecsim Index scheme with vector type with invalid parameter

    # HNSW parameters the causes an execution throw (M > UINT16_MAX)
    UINT16_MAX = 2**16
    M = UINT16_MAX + 1
    dim = 2
    env.expect('FT.CREATE', 'idx','SCHEMA','v', 'VECTOR', 'HNSW', '8',
                'TYPE', 'FLOAT16', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', M).ok()

    # Test dump HNSW with invalid index name
    # If index error is "Can't open vector index" then function tries to accsses null pointer
    env.expect(debug_cmd(), 'DUMP_HNSW', 'idx','v').error() \
        .contains("Can't open vector index")
@skip(cluster=True)
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

@skip(cluster=True)
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
    env.assertEqual(idx_info['indexing'], 1)
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

@skip(cluster=True)
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
    env.assertEqual(idx_info['indexing'], 1)
    # If is indexing, but debug scanner status is NEW, it means that the scanner is paused before scan

    # Resume indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx2')
    # Get count of indexed documents
    docs_in_index = env.cmd('FT.SEARCH', 'idx2', '*')[0]
    env.assertEqual(docs_in_index, num_docs)

@skip(cluster=True)
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

    # Test error handling
    # Giving invalid argument to debug scanner control command
    env.expect(bgScanCommand(), 'NOT_A_COMMAND', 'notTrue').error()\
    .contains("Invalid command for 'BG_SCAN_CONTROLLER'")

    # Test OOM pause
    # Change the memory limit to 80% so it can be tested without colliding with redis memory limit
    env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80').ok()

    # Insert more docs to ensure un-flakey test
    extra_docs = 90
    for i in range(num_docs,extra_docs+num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

    # Remove previous debug scanner settings
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'false').ok()
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 0).ok()
    env.expect(bgScanCommand(), 'SET_MAX_SCANNED_DOCS', 0).ok()
    # Set OOM pause
    env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
    # Set tight memory limit to trigger OOM
    set_tight_maxmemory_for_oom(env, 0.85)
    # Create an index and expect OOM pause
    env.expect('FT.CREATE', 'idx_oom', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexStatus(env, 'PAUSED_ON_OOM','idx_oom')
    # Resume indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

class TestQueryDebugCommands(object):
    def __init__(self):
        # Set the module default behaviour to non strict timeout policy, as this is the main focus of this test suite
        self.env = Env(testName="testing query debug commands", protocol=3, moduleArgs='ON_TIMEOUT RETURN')

        conn = getConnectionByEnv(self.env)

        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
        waitForIndex(self.env, 'idx')
        self.num_docs = 1500 * self.env.shardsCount
        for i in range(self.num_docs):
            conn.execute_command('HSET', f'doc{i}' ,'n', i)

        self.basic_query = []
        self.basic_debug_query = []

        self.cmd = None

    def setBasicDebugQuery(self, cmd):
        self.basic_query  = ['FT.' + cmd, 'idx', '*']
        self.basic_debug_query = [debug_cmd(), *self.basic_query]
        self.cmd = cmd

    def verifyWarning(self, res, message, should_timeout=True, depth=0):
        if should_timeout:
            VerifyTimeoutWarningResp3(self.env, res, depth=depth+1, message=message + " expected warning")
        else:
            self.env.assertFalse(res['warning'], depth=depth+1, message=message + " unexpected warning")

    def verifyResultsResp3(self, res, expected_results_count, message, should_timeout=True, depth=0):
        env = self.env
        env.assertEqual(len(res["results"]), expected_results_count, depth=depth+1, message=message + " unexpected results count")
        self.verifyWarning(res, message, should_timeout=should_timeout, depth=depth+1)

    def verifyResultsResp2(self, res, expected_results_count, message, depth=0):
        env = self.env
        env.assertEqual(len(res[1:] / 2), expected_results_count, depth=depth+1, message=message + " unexpected results count")

    def QueryWithLimit(self, query, timeout_res_count, limit, expected_res_count, should_timeout=False, message="", depth=0):
        env = self.env
        debug_params = ['TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2]
        res = env.cmd(*query, 'LIMIT', 0, limit, *debug_params)
        self.verifyResultsResp3(res, expected_res_count, message=message + " QueryWithLimit:", should_timeout=should_timeout, depth=depth+1)

        return res

    def InvalidParams(self):
        env = self.env
        basic_debug_query = self.basic_debug_query

        basic_debug_query_with_args = [*basic_debug_query, 'limit', 0, 0, 'timeout', 10000] # add random params to reach the minimum required to run the debug command

        def expectError(debug_params, error_message, message="", depth=1):
            test_cmd = [*basic_debug_query_with_args, *debug_params]
            err = env.expect(*test_cmd).error().res
            self.env.assertContains(error_message, err, message=message, depth=depth)

        # Unrecognized arguments
        debug_params = ['TIMEOUT_AFTER_MEOW', 1, 'DEBUG_PARAMS_COUNT', 2]
        expectError(debug_params, "Unrecognized argument: TIMEOUT_AFTER_MEOW")

        debug_params = ['TIMEOUT_AFTER_N', 1, 'PRINT_MEOW', 'DEBUG_PARAMS_COUNT', 3]
        expectError(debug_params, "Unrecognized argument: PRINT_MEOW")

        invalid_numeric_values = ["meow", -1, 0.2]

        # Test invalid params count
        def invalid_params_count(invalid_count, message=""):
            debug_params = ['DEBUG_PARAMS_COUNT', invalid_count]
            expectError(debug_params, 'Invalid DEBUG_PARAMS_COUNT count', message)

        for invalid_count in invalid_numeric_values:
            invalid_params_count(invalid_count, f"DEBUG_PARAMS_COUNT {invalid_count} should be invalid")

        # Test invalid N count
        def invalid_N(invalid_count, message=""):
            debug_params = ['TIMEOUT_AFTER_N', invalid_count, 'DEBUG_PARAMS_COUNT', 2]
            expectError(debug_params, 'Invalid TIMEOUT_AFTER_N count', message)

        for invalid_count in invalid_numeric_values:
            invalid_N(invalid_count, f"TIMEOUT_AFTER_N {invalid_count} should be invalid")

        # test missing params
        # no N
        debug_params = ['TIMEOUT_AFTER_N', 'DEBUG_PARAMS_COUNT', 1]
        expectError(debug_params, 'TIMEOUT_AFTER_N: Expected an argument, but none provided')
        debug_params = ['INTERNAL_ONLY', 'TIMEOUT_AFTER_N', 'DEBUG_PARAMS_COUNT', 2]
        expectError(debug_params, 'TIMEOUT_AFTER_N: Expected an argument, but none provided')

        # INTERNAL_ONLY without TIMEOUT_AFTER_N or PAUSE_AFTER_RP_N/PAUSE_BEFORE_RP_N
        debug_params = ['INTERNAL_ONLY', 'DEBUG_PARAMS_COUNT', 1]
        expectError(debug_params, 'INTERNAL_ONLY is not supported without TIMEOUT_AFTER_N or PAUSE_AFTER_RP_N/PAUSE_BEFORE_RP_N')
        expectError(debug_params, 'INTERNAL_ONLY is not supported without TIMEOUT_AFTER_N or PAUSE_AFTER_RP_N/PAUSE_BEFORE_RP_N')

    def QueryDebug(self):
        env = self.env
        basic_debug_query = self.basic_debug_query

        # Test invalid params
        env.expect(*basic_debug_query).error().contains('wrong number of arguments for')

        basic_debug_query_with_args = [*basic_debug_query, 'limit', 0, 0, 'timeout', 10000] # add random params to reach the minimum required to run the debug command
        env.expect(*basic_debug_query_with_args).error().contains('DEBUG_PARAMS_COUNT arg is missing')

        # in this case we try to parse [*basic_debug_query, 'limit', 0, 0, 'TIMEOUT'] so TIMEOUT count is missing
        test_cmd = [*basic_debug_query_with_args, 'MEOW', 'DEBUG_PARAMS_COUNT', 2]
        env.expect(*test_cmd).error().contains('argument for TIMEOUT')

        self.InvalidParams()

        # ft.<cmd> idx * TIMEOUT_AFTER_N 0 -> expect empty result
        debug_params = ['TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2]
        res = env.cmd(*basic_debug_query, *debug_params)
        self.verifyResultsResp3(res, 0, "QueryDebug:")

    def QueryWithSorter(self, limit=2, sortby_params=[], depth=0):
        # For queries with sorter, the LIMIT determines the heap size.
        # The sorter will continue to ask for results until it gets timeout or EOF.
        # the number of results in this case is the minimum between the LIMIT and the TIMEOUT_AFTER_N counter.

        # Therefore, as opposed to queries without sorter and LIMIT < TIMEOUT_AFTER_N,
        # we will get LIMIT results *and* TIMEOUT warning.
        res = self.QueryWithLimit([*self.basic_debug_query, *sortby_params], timeout_res_count=10, limit=limit, expected_res_count=limit, should_timeout=True, depth=depth+1)
        res_values = [doc_content['extra_attributes']['n'] for doc_content in res["results"]]
        self.env.assertTrue(res_values == sorted(res_values), depth=depth+1, message="QueryWithSorter: expected sorted results")
        self.env.assertTrue(len(res_values) == len(set(res_values)), depth=depth+1, message="QueryWithSorter: expected unique results")

    ######################## Main tests ########################
    def StrictPolicy(self):
        env = self.env
        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()

        with env.assertResponseError(contained="Timeout limit was reached"):
            runDebugQueryCommandTimeoutAfterN(env, self.basic_query, 2)

        # restore the default policy
        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()

    def SearchDebug(self):
        self.setBasicDebugQuery("SEARCH")
        basic_debug_query = self.basic_debug_query
        self.QueryDebug()

        timeout_res_count = 4

        # FT.SEARCH with coord doesn't have a timeout check, therefore it will return shards * timeout_res_count results
        expected_results_count = self.env.shardsCount * timeout_res_count
        # set LIMIT to be larger than the expected results count
        limit = expected_results_count + 1
        self.QueryWithLimit(basic_debug_query, timeout_res_count, limit, expected_res_count=expected_results_count, should_timeout=True, message="SearchDebug:")

        # SEARCH always has a sorter
        self.QueryWithSorter()

        # with no sorter (dialect 4)
        self.QueryWithLimit(basic_debug_query + ["DIALECT", 4], timeout_res_count, limit, expected_res_count=expected_results_count, should_timeout=True, message="SearchDebug:")

        self.StrictPolicy()

    def testSearchDebug(self):
        self.SearchDebug()

    def testSearchDebug_MT(self):
        self.env.expect(config_cmd(), 'SET', 'WORKERS', 4).ok()
        self.SearchDebug()
        self.env.expect(config_cmd(), 'SET', 'WORKERS', 0).ok()

    def AggregateDebug(self):
        env = self.env
        self.setBasicDebugQuery("AGGREGATE")
        basic_debug_query = self.basic_debug_query
        self.QueryDebug()

        # EOF will be reached before the timeout counter
        limit = 2
        res = self.QueryWithLimit(basic_debug_query, timeout_res_count=10, limit=limit, expected_res_count=limit, should_timeout=False)

        self.QueryWithSorter(sortby_params=['sortby', 1, '@n'])

        # with cursor
        timeout_res_count = 200
        limit = self.num_docs
        cursor_count = 600 # higher than timeout_res_count, but lower than limit
        debug_params = ["TIMEOUT_AFTER_N", timeout_res_count, "DEBUG_PARAMS_COUNT", 2]
        cursor_query = [*basic_debug_query, 'WITHCURSOR', 'COUNT', cursor_count]
        res, cursor = env.cmd(*cursor_query, 'LIMIT', 0, limit, *debug_params)
        self.verifyResultsResp3(res, timeout_res_count, "AggregateDebug with cursor:")

        iter = 0
        total_returned = len(res['results'])
        expected_results_per_iter = timeout_res_count

        should_timeout = True
        check_res = True
        while (cursor):
            remaining = limit - total_returned
            if remaining <= timeout_res_count * env.shardsCount:
                # We don't know how many docs are left in each shard, so the result structure is unpredictable.
                # If all shards return fewer results than timeout_res_count, no timeout warning will occur.
                # If at least one shard returns more than timeout_res_count, a timeout warning will be issued.
                # See aggregate/aggregate_debug.h for more details.
                if env.isCluster():
                    check_res = False
                else:
                    # in a single shard the next read will return EOF
                    expected_results_per_iter = remaining
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            total_returned += len(res['results'])
            if cursor == 0:
                should_timeout = False

            if check_res:
                self.verifyResultsResp3(res, expected_results_per_iter, f"AggregateDebug with cursor: iter: {iter}, total_returned: {total_returned}", should_timeout=should_timeout)
            iter += 1
        env.assertEqual(total_returned, self.num_docs, message=f"AggregateDebug with cursor: depletion took {iter} iterations")

        # cursor count smaller than timeout count, expect no timeout
        cursor_count = timeout_res_count // 2
        cursor_query = [*basic_debug_query, 'WITHCURSOR', 'COUNT', cursor_count]
        res, cursor = env.cmd(*cursor_query, 'LIMIT', 0, limit, *debug_params)
        should_timeout = False
        self.verifyResultsResp3(res, cursor_count, should_timeout=should_timeout, message="AggregateDebug with cursor count lower than timeout_res_count:")

        # Test TIMEOUT_AFTER_N 0 INTERNAL_ONLY without WITHCURSOR in cluster mode - should work and return empty results
        if env.isCluster():
            debug_params = ['TIMEOUT_AFTER_N', 0, 'INTERNAL_ONLY', 'DEBUG_PARAMS_COUNT', 3]
            res = env.cmd(*basic_debug_query, *debug_params)
            self.verifyResultsResp3(res, 0, message="AggregateDebug: TIMEOUT_AFTER_N 0 INTERNAL_ONLY without WITHCURSOR in cluster:")

        self.StrictPolicy()

    def testAggregateDebug(self):
        self.AggregateDebug()

    def testAggregateDebug_MT(self):
        self.env.expect(config_cmd(), 'SET', 'WORKERS', 4).ok()
        self.AggregateDebug()
        self.env.expect(config_cmd(), 'SET', 'WORKERS', 0).ok()

    # compare results of regular query and debug query
    def Sanity(self, cmd, query_params):
        # avoid running this test in cluster mode, as it relies on the order of the shards reply.
        skipTest(cluster=True)
        env = self.env
        results_count = 200
        timeout_res_count = results_count - 1 # less than limit to get timeout and not EOF
        query = ['FT.' + cmd, 'idx', '*', *query_params, 'LIMIT', 0, results_count]
        debug_params = ["TIMEOUT_AFTER_N", timeout_res_count, "DEBUG_PARAMS_COUNT", 2]

        # expect that the first timeout_res_count of the regular query will be the same as the debug query
        regular_res = env.cmd(*query)
        debug_res = env.cmd(debug_cmd(), *query, *debug_params)
        self.verifyResultsResp3(debug_res, timeout_res_count, f"{cmd} Sanity: compare regular and debug results", should_timeout=True)

        for i in range(timeout_res_count):
            env.assertEqual(regular_res["results"][i], debug_res["results"][i], message=f"Sanity: compare regular and debug results at index {i}")

    def testSearchSanity(self):
        self.Sanity("SEARCH", ['SORTBY', 'n'])
    def testAggSanity(self):
        self.Sanity("AGGREGATE", ['LOAD', 1, '@n', 'SORTBY', 1, '@n'])

    def testInternalOnly(self):
        env = self.env
        # test we get count * num_shards results with internal only
        timeout_res_count = 4
        limit = self.env.shardsCount * timeout_res_count + 1

        def runCmd(cmd, expected_results_count):
            query = [debug_cmd(), 'FT.' + cmd, 'idx', '*', 'LIMIT', 0, limit + 1, "TIMEOUT_AFTER_N", timeout_res_count, "INTERNAL_ONLY", "DEBUG_PARAMS_COUNT", 3]
            res = env.cmd(*query)
            self.verifyResultsResp3(res, expected_results_count, f"InternalOnly: FT.{cmd}:")

        # we get timeout_res_count from each shard
        runCmd("SEARCH", self.env.shardsCount * timeout_res_count)

        # with AGGREGATE we will get timeout_res_count results because the shard returned timeout
        runCmd("AGGREGATE", timeout_res_count)

    def Resp2(self, cmd, query_params, listResults_func):
        skipTest(cluster=True)
        conn = getConnectionByEnv(self.env)
        conn.execute_command("hello", "2")

        timeout_res_count = 4
        limit = self.env.shardsCount * timeout_res_count + 1
        query = ['FT.' + cmd, 'idx', '*', *query_params, 'LIMIT', 0, limit]
        debug_params = ["TIMEOUT_AFTER_N", timeout_res_count, "DEBUG_PARAMS_COUNT", 2]
        # expect that the first timeout_res_count of the regular query will be the same as the debug query
        regular_res = listResults_func(conn.execute_command(*query))
        debug_res = listResults_func(conn.execute_command(debug_cmd(), *query, *debug_params))
        self.env.assertEqual(len(debug_res), timeout_res_count, message=f"Resp2 with FT.{cmd}: expected results count")

        for i in range(timeout_res_count):
            self.env.assertEqual(regular_res[i], debug_res[i], message=f"Resp2 with FT.{cmd}: compare regular and debug results at index {i}")

    def testAggResp2(self):
        def listResults(res):
            return res[1:]
        self.Resp2("AGGREGATE", ['LOAD', 1, '@n', 'SORTBY', 1, '@n'], listResults)

    def testSearchResp2(self):
        def listResults(res):
            return [{res[i]: res[i + 1]} for i in range(1, len(res[1:]), 2)]
        self.Resp2("SEARCH", ['SORTBY', 'n'], listResults)

# For now allowing access to the value through the debug command
# Maybe in the future it should be accessible through the FT.CONFIG command and the test move to test_config.py
# Didn't want to "break" the API by adding a new config parameter
def test_hideUserDataFromLogs(env):
    env.skipOnCluster()
    value = env.cmd(debug_cmd(), 'GET_HIDE_USER_DATA_FROM_LOGS')
    env.assertEqual(value, 0)
    env.expect('CONFIG', 'SET', 'hide-user-data-from-log', 'yes').ok()
    value = env.cmd(debug_cmd(), 'GET_HIDE_USER_DATA_FROM_LOGS')
    env.assertEqual(value, 1)
    env.expect('CONFIG', 'SET', 'hide-user-data-from-log', 'no').ok()
    value = env.cmd(debug_cmd(), 'GET_HIDE_USER_DATA_FROM_LOGS')
    env.assertEqual(value, 0)


def testIndexObfuscatedInfo(env: Env):
    # we create more indexes to cover the found case in the code (it should break from the loop)
    env.expect('FT.CREATE', 'first', 'SCHEMA', 'name', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    env.expect('FT.CREATE', 'last', 'SCHEMA', 'name', 'TEXT').ok()

    obfuscated_name = 'Index@4e7f626df794f6491574a236f22c100c34ed804f'
    debug_output = env.cmd(debug_cmd(), 'INFO', obfuscated_name)
    info = to_dict(debug_output[0])
    env.assertEqual(info['index_name'], obfuscated_name)
    index_definition = to_dict(info['index_definition'])
    env.assertEqual(index_definition['prefixes'][0], 'Text')
    attr_list = info['attributes']
    field_stats_list = info['field statistics']
    field_count = len(attr_list)
    env.assertEqual(field_count, 1)
    env.assertEqual(len(field_stats_list), field_count)
    for i in range(field_count):
        attr = to_dict(attr_list[i])
        env.assertEqual(attr['identifier'], f'FieldPath@{i}')
        env.assertEqual(attr['attribute'], f'Field@{i}')
        field_stats = to_dict(field_stats_list[i])
        env.assertEqual(field_stats['identifier'], f'FieldPath@{i}')
        env.assertEqual(field_stats['attribute'], f'Field@{i}')

@skip(cluster=True)
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
    env.assertEqual(idx_info['indexing'], 1)
    # The percent index should be close to 0.25 as we set the tight memory limit after 25% of the docs were scanned
    env.assertAlmostEqual(float(idx_info['percent_indexed']), 0.25, delta=0.1)

    # Resume indexing for the sake of completeness
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

@skip(cluster=True)
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
    env.assertEqual(index_info(env, 'idx')['indexing'], 0)

@skip(cluster=True)
def test_pause_before_oom_retry(env):
    # Check error handling
    # Giving invalid argument
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'notAbool').error()\
    .contains("Invalid argument for 'SET_PAUSE_BEFORE_OOM_RETRY'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY').error()\
    .contains('wrong number of arguments')

@skip(cluster=True)
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

@skip(cluster=True)
def test_yield_counter(env):
    # Giving wrong arity
    env.expect(debug_cmd(), 'YIELDS_ON_LOAD_COUNTER','ExtraARG1','ExtraARG2').error()\
    .contains('wrong number of arguments')
    # Giving wrong subcommand
    env.expect(debug_cmd(), 'YIELDS_ON_LOAD_COUNTER', 'NOT_A_COMMAND').error()\
    .contains('Unknown subcommand')

@skip(cluster=True)
def test_query_controller(env):
    # Giving wrong arity
    env.expect(debug_cmd(), 'QUERY_CONTROLLER').error()\
    .contains('wrong number of arguments')
    # Giving wrong subcommand
    env.expect(debug_cmd(), 'QUERY_CONTROLLER', 'NOT_A_COMMAND').error()\
    .contains("Invalid command for 'QUERY_CONTROLLER'")

@skip(cluster=True)
def test_query_controller_pause_and_resume(env):

    # Giving wrong arity
    env.expect(debug_cmd(), 'QUERY_CONTROLLER', 'SET_PAUSE_RP_RESUME', 'ExtraARG').error()\
    .contains('wrong number of arguments')
    env.expect(debug_cmd(), 'QUERY_CONTROLLER', 'GET_IS_RP_PAUSED', 'ExtraARG').error()\
    .contains('wrong number of arguments')
    env.expect(debug_cmd(), 'QUERY_CONTROLLER', 'PRINT_RP_STREAM', 'ExtraARG').error()\
    .contains('wrong number of arguments')

    # Test error when trying to resume when no query is paused
    env.expect(debug_cmd(), 'QUERY_CONTROLLER', 'SET_PAUSE_RP_RESUME').error()\
    .contains('Query is not paused')

    # Test error when trying to print RP stream when no debug RP is set
    env.expect(debug_cmd(), 'QUERY_CONTROLLER', 'PRINT_RP_STREAM').error()\
    .contains('No debug RP is set')

    # Set workers to 2 to make sure the query can be paused
    # 1 worker is for testing we can't debug multiple queries
    env.expect('FT.CONFIG', 'SET', 'WORKERS', 2).ok()

    # Create 1 docs
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    env.expect('HSET', 'doc1', 'name', 'name1').equal(1)

    queries_completed = 0

    for query_type in ['FT.SEARCH', 'FT.AGGREGATE']:
        # We need to call the queries in MT so the paused query won't block the test
        query_result = []

        # Build threads
        t_query = threading.Thread(
            target=call_and_store,
            args=(runDebugQueryCommandPauseBeforeRPAfterN,
                (env, [query_type, 'idx', '*'], 'Index', 0, ['INTERNAL_ONLY'] if query_type == 'FT.AGGREGATE' else None),
                query_result),
            daemon=True
        )

        # Start the query and the pause-check in parallel
        t_query.start()

        while getIsRPPaused(env) != 1:
            time.sleep(0.1)

        # Test error when trying to create multiple debug RPs (should fail with "Failed to create pause RP or another debug RP is already set")
        # This tests the error case in PipelineAddPauseRPcount when RPPauseAfterCount_New returns NULL
        env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*', 'PAUSE_BEFORE_RP_N', 'Index', 0, 'PAUSE_AFTER_RP_N', 'Sorter', 0, 'DEBUG_PARAMS_COUNT', 6).error()\
        .contains('Failed to create pause RP or another debug RP is already set')
        # The query above completed even though it failed
        queries_completed += 1

        # If we are here, the query is paused
        # Verify we have 1 active query
        active_queries = env.cmd('INFO', 'MODULES')['search_total_active_queries']
        env.assertEqual(active_queries, 1)

        # Test PRINT_RP_STREAM
        rp_stream = env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'PRINT_RP_STREAM')
        if query_type == 'FT.SEARCH':
            env.assertEqual(rp_stream, ['Threadsafe-Loader','Sorter','Scorer','DEBUG_RP','Index'])
        if query_type == 'FT.AGGREGATE':
            env.assertEqual(rp_stream, ['DEBUG_RP','Index'])

        # Resume the query
        setPauseRPResume(env)

        t_query.join()

        queries_completed += 1

        # Verify the query returned only 1 result
        env.assertEqual(query_result[0][0], 1)

@skip(cluster=True)
def test_query_controller_add_before_after(env):
    # Set workers to 1 to make sure the query can be paused

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Create 1 docs
    env.expect('HSET', 'doc1', 'name', 'name1').equal(1)

    # Check error when workers is 0
    env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*', 'PAUSE_BEFORE_RP_N', 'Index', 0, 'DEBUG_PARAMS_COUNT', 3).error()\
    .contains("Query PAUSE_BEFORE_RP_N is only supported with WORKERS")

    env.expect('FT.CONFIG', 'SET', 'WORKERS', 1).ok()

    # Check error when insert after Index RP
    env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*', 'PAUSE_AFTER_RP_N', 'Index', 0, 'DEBUG_PARAMS_COUNT', 3).error()\
    .contains("Index RP type not found in stream or tried to insert after last RP")

    for before in [True, False]:

        target_func = runDebugQueryCommandPauseBeforeRPAfterN if before else runDebugQueryCommandPauseAfterRPAfterN

        # Check wrong RP type error
        cmd_str = 'BEFORE' if before else 'AFTER'
        env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*', f'PAUSE_{cmd_str}_RP_N', 'InvalidRP', 0, 'DEBUG_PARAMS_COUNT', 3).error()\
        .contains(f"InvalidRP is an invalid PAUSE_{cmd_str}_RP_N RP type")
        # Check RP type that is not in the stream
        env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*', f'PAUSE_{cmd_str}_RP_N', 'Highlighter', 0, 'DEBUG_PARAMS_COUNT', 3).error()\
        .contains(f"Highlighter RP type not found in stream or tried to insert after last RP")
        env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*', f'PAUSE_{cmd_str}_RP_N', 'Highlighter', -1, 'DEBUG_PARAMS_COUNT', 3).error()\
        .contains(f"Invalid PAUSE_{cmd_str}_RP_N count")
        # Build threads
        t_query = threading.Thread(
            target=target_func,
            args=(env,['FT.SEARCH', 'idx', '*'], 'Sorter', 0),
            daemon=True
        )

        # Start the query and the pause-check in parallel
        t_query.start()

        while getIsRPPaused(env) != 1:
            time.sleep(0.1)
        rp_stream = env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'PRINT_RP_STREAM')
        if before:
            env.assertEqual(rp_stream, ['Threadsafe-Loader','DEBUG_RP','Sorter','Scorer','Index'])
        else:
            env.assertEqual(rp_stream, ['Threadsafe-Loader','Sorter','DEBUG_RP','Scorer','Index'])

        # Resume the query
        setPauseRPResume(env)
        t_query.join()

@skip(cluster=False)
def test_cluster_query_controller_pause_and_resume():
    # Set workers to 1 on all shards to make sure queries can be paused
    env = Env(moduleArgs='WORKERS 1')

    conn = getConnectionByEnv(env)

    # Create index
    res = conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    env.assertEqual(res, 'OK')

    n_docs_per_shard = 100
    # Enough docs to make sure we have results from all shards
    n_docs = n_docs_per_shard * env.shardsCount
    for i in range(n_docs):
        res = conn.execute_command('HSET', f'doc{i}', 't', f'text{i}')
        env.assertEqual(res, 1)


    for query_type in ['FT.SEARCH', 'FT.AGGREGATE']:
        # We need to call the queries in MT so the paused query won't block the test
        query_result = []

        # Build threads
        query_args = [query_type, 'idx', '*']
        if query_type == 'FT.AGGREGATE':
            query_args.append('LOAD')
            query_args.append(1)
            query_args.append('@t')

        t_query = threading.Thread(
            target=call_and_store,
            args=(runDebugQueryCommandPauseBeforeRPAfterN,
                (env, query_args, 'Index', 0, ['INTERNAL_ONLY'] if query_type == 'FT.AGGREGATE' else None),
                query_result),
            daemon=True
        )

        # Start the query and the pause-check in parallel
        t_query.start()

        # Wait for any shard to be paused
        while False in allShards_getIsRPPaused(env):
            time.sleep(0.1)

        # If we are here, at least one query is paused
        # Verify that we have active queries across the cluster
        for shard_id in range(1, env.shardsCount + 1):
            active_queries = env.getConnection(shard_id).execute_command('INFO', 'MODULES')['search_total_active_queries']
            env.assertEqual(active_queries, 1)

        # Resume all shards
        allShards_setPauseRPResume(env)

        t_query.join()

        if query_type == 'FT.SEARCH':
            env.assertEqual(query_result[0][0], n_docs)
        else:
            env.assertEqual(len(query_result[0]) - 1, n_docs)

@skip(cluster=False)
def test_cluster_query_controller_pause_and_resume_coord(env):

    conn = getConnectionByEnv(env)

    # Create index
    res = conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    env.assertEqual(res, 'OK')

    n_docs_per_shard = 100
    # Enough docs to make sure we have results from all shards
    n_docs = n_docs_per_shard * env.shardsCount
    for i in range(n_docs):
        res = conn.execute_command('HSET', f'doc{i}', 't', f'text{i}')
        env.assertEqual(res, 1)

    # Check error when insert after Network RP
    env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t', 'PAUSE_AFTER_RP_N', 'Network', 0, 'DEBUG_PARAMS_COUNT', 3).error()\
    .contains("Network RP type not found in stream or tried to insert after last RP")

    # We need to call the queries in MT so the paused query won't block the test
    query_result = []

    # Build threads
    query_args = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t']

    t_query = threading.Thread(
        target=call_and_store,
        args=(runDebugQueryCommandPauseBeforeRPAfterN,
            (env, query_args, 'Network', 0),
            query_result),
        daemon=True
    )

    # Start the query and the pause-check in parallel
    t_query.start()

    # Wait for the coordinator to be paused
    while getIsRPPaused(env) != 1:
        time.sleep(0.1)

    # Resume the coordinator
    setPauseRPResume(env)

    t_query.join()

    env.assertEqual(len(query_result[0]) - 1, n_docs)
