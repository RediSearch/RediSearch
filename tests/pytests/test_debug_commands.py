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
            'YIELDS_ON_LOAD_COUNTER',
            'FT.AGGREGATE',
            'FT.SEARCH',
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

class TestQueryDebugCommands(object):
    def __init__(self):
        # Set the module default behaviour to non strict timeout policy, as this is the main focus of this test suite
        self.env = Env(testName="testing query debug commands", protocol=3, moduleArgs='ON_TIMEOUT RETURN')
        self.env.skipOnCluster()
        conn = getConnectionByEnv(self.env)

        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
        waitForIndex(self.env, 'idx')
        self.num_docs = 1500
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

    def verifyResultsResp2(self, res, expected_results_count, message, depth=0):
        env = self.env
        env.assertEqual(len(res[1:] / 2), expected_results_count, depth=depth+1, message=message + " unexpected results count")

    def QueryWithLimit(self, query, timeout_res_count, limit, expected_res_count, should_timeout=False, message="", depth=0):
        env = self.env
        debug_params = ['TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2]
        res = env.cmd(*query, 'LIMIT', 0, limit, *debug_params)
        verifyResultsResp3(env, res, expected_res_count, message=message + " QueryWithLimit:", should_timeout=should_timeout)

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

    def QueryDebug(self, message=""):
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
        verifyResultsResp3(env, res, 0, message + " QueryDebug:")

    def QueryWithSorter(self, limit=2, sortby_params=[], depth=0):
        # For queries with sorter, the LIMIT determines the heap size.
        # The sorter will continue to ask for results until it gets timeout or EOF.
        # the number of results in this case is the minimum between the LIMIT and the TIMEOUT_AFTER_N counter.

        # Therefore, as opposed to queries without sorter and LIMIT < TIMEOUT_AFTER_N,
        # we will get LIMIT results *and* TIMEOUT warning.
        res = self.QueryWithLimit([*self.basic_debug_query, *sortby_params], timeout_res_count=10, limit=limit, expected_res_count=limit, should_timeout=True, depth=depth+1, message="QueryWithSorter:")
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
        self.QueryDebug(message="SearchDebug:")

        timeout_res_count = 4

        expected_results_count = timeout_res_count
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

    def AggregateDebug(self):
        env = self.env
        self.setBasicDebugQuery("AGGREGATE")
        basic_debug_query = self.basic_debug_query
        self.QueryDebug(message="AggregateDebug:")

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
        verifyResultsResp3(env, res, timeout_res_count, "AggregateDebug with cursor:")

        iter = 0
        total_returned = len(res['results'])
        expected_results_per_iter = timeout_res_count

        should_timeout = True
        check_res = True
        while (cursor):
            remaining = limit - total_returned
            if remaining <= timeout_res_count:
                expected_results_per_iter = remaining
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            total_returned += len(res['results'])
            if cursor == 0:
                should_timeout = False

            if check_res:
                verifyResultsResp3(env, res, expected_results_per_iter, f"AggregateDebug with cursor: iter: {iter}, total_returned: {total_returned}", should_timeout=should_timeout)
            iter += 1
        env.assertEqual(total_returned, self.num_docs, message=f"AggregateDebug with cursor: depletion took {iter} iterations")

        # cursor count smaller than timeout count, expect no timeout
        cursor_count = timeout_res_count // 2
        cursor_query = [*basic_debug_query, 'WITHCURSOR', 'COUNT', cursor_count]
        res, cursor = env.cmd(*cursor_query, 'LIMIT', 0, limit, *debug_params)
        should_timeout = False
        verifyResultsResp3(env, res, cursor_count, should_timeout=should_timeout, message="AggregateDebug with cursor count lower than timeout_res_count:")

        self.StrictPolicy()

    def testAggregateDebug(self):
        self.AggregateDebug()

    # compare results of regular query and debug query
    def Sanity(self, cmd, query_params):
        env = self.env
        results_count = 200
        timeout_res_count = results_count - 1 # less than limit to get timeout and not EOF
        query = ['FT.' + cmd, 'idx', '*', *query_params, 'LIMIT', 0, results_count]
        debug_params = ["TIMEOUT_AFTER_N", timeout_res_count, "DEBUG_PARAMS_COUNT", 2]

        # expect that the first timeout_res_count of the regular query will be the same as the debug query
        regular_res = env.cmd(*query)
        debug_res = env.cmd(debug_cmd(), *query, *debug_params)
        verifyResultsResp3(env, debug_res, timeout_res_count, f"{cmd} Sanity: compare regular and debug results", should_timeout=True)

        for i in range(timeout_res_count):
            env.assertEqual(regular_res["results"][i], debug_res["results"][i], message=f"Sanity: compare regular and debug results at index {i}")

    def testSearchSanity(self):
        self.Sanity("SEARCH", ['SORTBY', 'n'])
    def testAggSanity(self):
        self.Sanity("AGGREGATE", ['LOAD', 1, '@n', 'SORTBY', 1, '@n'])

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

@skip(cluster=True)
def test_yield_counter(env):
    # Giving wrong arity
    env.expect(debug_cmd(), 'YIELDS_ON_LOAD_COUNTER','ExtraARG1','ExtraARG2').error()\
    .contains('wrong number of arguments')
    # Giving wrong subcommand
    env.expect(debug_cmd(), 'YIELDS_ON_LOAD_COUNTER', 'NOT_A_COMMAND').error()\
    .contains('Unknown subcommand')
