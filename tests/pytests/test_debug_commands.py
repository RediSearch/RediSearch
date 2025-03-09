from common import *

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
            'FT.AGGREGATE',
            '_FT.AGGREGATE',
            'FT.SEARCH',
            '_FT.SEARCH',
            'GET_HIDE_USER_DATA_FROM_LOGS',
        ]
        coord_help_list = ['SHARD_CONNECTION_STATES', 'PAUSE_TOPOLOGY_UPDATER', 'RESUME_TOPOLOGY_UPDATER', 'CLEAR_PENDING_TOPOLOGY']
        help_list.extend(coord_help_list)

        self.env.expect(debug_cmd(), 'help').equal(help_list)

        arity_2_cmds = ['GIT_SHA', 'DUMP_PREFIX_TRIE', 'GC_WAIT_FOR_JOBS', 'DELETE_LOCAL_CURSORS', 'SHARD_CONNECTION_STATES',
                        'PAUSE_TOPOLOGY_UPDATER', 'RESUME_TOPOLOGY_UPDATER', 'CLEAR_PENDING_TOPOLOGY', 'GET_HIDE_USER_DATA_FROM_LOGS']
        for cmd in [c for c in help_list if c not in arity_2_cmds]:
            self.env.expect(debug_cmd(), cmd).error().contains(err_msg)

    def testDocInfo(self):
        rv = self.env.cmd(debug_cmd(), 'docinfo', 'idx', 'doc1')
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
    # Giving invalid argument
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'notTrue').error()\
    .contains("Invalid argument for 'SET_BG_INDEX_RESUME'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').error()\
    .contains('wrong number of arguments')

    # Resume indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
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
    env.assertEqual(getDebugScannerStatus(env, 'idx2'), 'NEW')

    idx_info = index_info(env, 'idx2')
    env.assertEqual(idx_info['indexing'], 1)
    # If is indexing, but debug scanner status is NEW, it means that the scanner is paused before scan

    # Resume indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
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
    env.assertEqual(getDebugScannerStatus(env, 'idx'), 'NEW')
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()
    waitForIndexPauseScan(env, 'idx')
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()
    waitForIndexFinishScan(env, 'idx')
    # When scan is done, the scanner is freed
    checkDebugScannerError(env, 'idx', 'Scanner is not initialized')

    # Test error handling
    # Giving non existing index name
    checkDebugScannerError(env, 'non_existing', 'Unknown index name')

    # Test error handling
    # Giving invalid argument to debug scanner control command
    env.expect(bgScanCommand(), 'NOT_A_COMMAND', 'notTrue').error()\
    .contains("Invalid command for 'BG_SCAN_CONTROLLER'")
    # Giving wrong arity
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').error()\
    .contains('wrong number of arguments')

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
