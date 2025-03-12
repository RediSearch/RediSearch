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
        ]
        coord_help_list = ['SHARD_CONNECTION_STATES', 'PAUSE_TOPOLOGY_UPDATER', 'RESUME_TOPOLOGY_UPDATER', 'CLEAR_PENDING_TOPOLOGY']
        help_list.extend(coord_help_list)

        self.env.expect(debug_cmd(), 'help').equal(help_list)

        arity_2_cmds = ['GIT_SHA', 'DUMP_PREFIX_TRIE', 'GC_WAIT_FOR_JOBS', 'DELETE_LOCAL_CURSORS', 'SHARD_CONNECTION_STATES',
                        'PAUSE_TOPOLOGY_UPDATER', 'RESUME_TOPOLOGY_UPDATER', 'CLEAR_PENDING_TOPOLOGY']
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

        # INTERNAL_ONLY without TIMEOUT_AFTER_N
        debug_params = ['INTERNAL_ONLY', 'DEBUG_PARAMS_COUNT', 1]
        expectError(debug_params, 'INTERNAL_ONLY must be used with TIMEOUT_AFTER_N')
        expectError(debug_params, 'INTERNAL_ONLY must be used with TIMEOUT_AFTER_N')

        # TIMEOUT_AFTER_N 0 INTERNAL_ONLY without WITHCURSOR is disabled.
        if (self.env.isCluster() and self.cmd == "AGGREGATE"):
            debug_params = ['TIMEOUT_AFTER_N', 0, 'INTERNAL_ONLY', 'DEBUG_PARAMS_COUNT', 3]
            expectError(debug_params, 'INTERNAL_ONLY with TIMEOUT_AFTER_N 0 is not allowed without WITHCURSOR')

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
