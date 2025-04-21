from RLTest import Env
from includes import *
from common import (
    waitForIndex,
    TimeLimit,
    getConnectionByEnv,
    debug_cmd,
    verifyResultLen,
    resValuesList,
    config_cmd,
    runDebugQueryCommandTimeoutAfterN,
    resultLen,
    skipTest,
)

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
            "DUMP_INVIDX",
            "DUMP_NUMIDX",
            "DUMP_NUMIDXTREE",
            "DUMP_TAGIDX",
            "INFO_TAGIDX",
            "DUMP_PREFIX_TRIE",
            "IDTODOCID",
            "DOCIDTOID",
            "DOCINFO",
            "DUMP_PHONETIC_HASH",
            "DUMP_SUFFIX_TRIE",
            "DUMP_TERMS",
            "INVIDX_SUMMARY",
            "NUMIDX_SUMMARY",
            "GC_FORCEINVOKE",
            "GC_FORCEBGINVOKE",
            "GC_CLEAN_NUMERIC",
            "GIT_SHA",
            "TTL",
            "TTL_PAUSE",
            "TTL_EXPIRE",
            "VECSIM_INFO",
            'FT.AGGREGATE',
            'FT.SEARCH',
        ]
        self.env.expect('FT.DEBUG', 'help').equal(help_list)

        for cmd in help_list:
            if cmd in ['GIT_SHA', 'DUMP_PREFIX_TRIE']:
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

class TestQueryDebugCommands(object):
    def __init__(self):
        # Set the module default behaviour to non strict timeout policy, as this is the main focus of this test suite
        self.env = Env(testName="testing query debug commands", moduleArgs='ON_TIMEOUT RETURN')
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

    def QueryWithLimit(self, query, timeout_res_count, limit, expected_res_count, message="", depth=0):
        env = self.env
        debug_params = ['TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2]
        res = env.cmd(*query, 'LIMIT', 0, limit, *debug_params)
        verifyResultLen(env, res, expected_res_count, mode=self.cmd, message=message + " QueryWithLimit:")

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
        verifyResultLen(env, res, 0, mode=self.cmd, message=message + " QueryDebug:")

    def QueryWithSorter(self, limit=2, depth=0):
        # For queries with a sorter, the LIMIT parameter determines the heap size.
        # The sorter continues fetching results until it either reaches the timeout (TIMEOUT_AFTER_N) or EOF (end of file).
        # The number of results returned is the minimum between the LIMIT and the TIMEOUT_AFTER_N counter.


        # In a regular case (no timeout), the query processes the entire dataset, sorts it, and returns the highest values.
        # If a timeout occurs, only the first TIMEOUT_AFTER_N documents are processed, and the results reflect
        # the highest values within this subset, not the entire dataset.
        # we use this logic to verify that timeout occurred.
        if self.cmd == "AGGREGATE":
            sortby_params = ['SORTBY', 2, '@n', 'DESC']
        elif self.cmd == "SEARCH":
            sortby_params = ['SORTBY', 'n', 'DESC']
        timeout_res_count = 10
        res = self.QueryWithLimit([*self.basic_debug_query, *sortby_params], timeout_res_count=timeout_res_count, limit=limit, expected_res_count=limit, depth=depth+1, message="QueryWithSorter:")
        self.env.assertEqual(resValuesList(res, self.cmd), [['n', str(timeout_res_count - 1)], ['n', str(timeout_res_count - 2)]], depth=depth+1, message="QueryWithSorter: expected partial results")

    ######################## Main tests ########################
    def StrictPolicy(self):
        env = self.env
        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()
        res = runDebugQueryCommandTimeoutAfterN(env, self.basic_query, 2)
        res[0] == "Timeout limit was reached"

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
        self.QueryWithLimit(basic_debug_query, timeout_res_count, limit, expected_res_count=expected_results_count, message="SearchDebug:")

        self.QueryWithSorter()

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
        res = self.QueryWithLimit(basic_debug_query, timeout_res_count=10, limit=limit, expected_res_count=limit)

        self.QueryWithSorter()

        # with cursor
        timeout_res_count = 200
        limit = self.num_docs
        cursor_count = 600 # higher than timeout_res_count, but lower than limit
        debug_params = ["TIMEOUT_AFTER_N", timeout_res_count, "DEBUG_PARAMS_COUNT", 2]
        cursor_query = [*basic_debug_query, 'WITHCURSOR', 'COUNT', cursor_count]
        res, cursor = env.cmd(*cursor_query, 'LIMIT', 0, limit, *debug_params)
        verifyResultLen(env, res, timeout_res_count, mode="AGG", message="AggregateDebug with cursor:")

        iter = 0
        total_returned = resultLen(res, mode="AGG")
        expected_results_per_iter = timeout_res_count

        check_res = True
        while (cursor):
            remaining = limit - total_returned
            if remaining <= timeout_res_count:
                expected_results_per_iter = remaining
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            total_returned += resultLen(res, mode="AGG")
            if check_res:
                verifyResultLen(env, res, expected_results_per_iter, mode="AGG", message=f"AggregateDebug with cursor: iter: {iter}, total_returned: {total_returned}")
            iter += 1
        env.assertEqual(total_returned, self.num_docs, message=f"AggregateDebug with cursor: depletion took {iter} iterations")

        # cursor count smaller than timeout count, expect no timeout
        cursor_count = timeout_res_count // 2
        cursor_query = [*basic_debug_query, 'WITHCURSOR', 'COUNT', cursor_count]
        res, cursor = env.cmd(*cursor_query, 'LIMIT', 0, limit, *debug_params)
        verifyResultLen(env, res, cursor_count, mode="AGG", message="AggregateDebug with cursor count lower than timeout_res_count:")

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
        reg_res_val_list = resValuesList(regular_res, mode=cmd)

        debug_res = env.cmd(debug_cmd(), *query, *debug_params)
        debug_res_val_list = resValuesList(debug_res, mode=cmd)
        env.assertEqual(len(debug_res_val_list), timeout_res_count, message=f"{cmd} Sanity: compare regular and debug results got unexpected results count")

        for i in range(timeout_res_count):
            env.assertEqual(reg_res_val_list[i], debug_res_val_list[i], message=f"Sanity: compare regular and debug results at index {i}")

    def testSearchSanity(self):
        self.Sanity("SEARCH", ['SORTBY', 'n'])
    def testAggSanity(self):
        self.Sanity("AGGREGATE", ['LOAD', 1, '@n', 'SORTBY', 1, '@n'])

    def Resp2(self, cmd, query_params, listResults_func):
        skipTest(cluster=True)
        conn = getConnectionByEnv(self.env)

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
