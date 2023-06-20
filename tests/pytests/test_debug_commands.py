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
        self.env.expect('FT.DEBUG', 'dump_invidx').raiseError().contains('wrong number of arguments')
        self.env.expect('FT.DEBUG').raiseError().contains('wrong number of arguments')

    def testDebugUnknownSubcommand(self):
        self.env.expect('FT.DEBUG', 'unknown').raiseError().equal('subcommand was not found')

    def testDebugHelp(self):
        err_msg = 'wrong number of arguments'
        help_list = ['DUMP_INVIDX', 'DUMP_NUMIDX', 'DUMP_NUMIDXTREE', 'DUMP_TAGIDX', 'INFO_TAGIDX', 'DUMP_GEOMIDX', 'IDTODOCID', 'DOCIDTOID', 'DOCINFO',
                     'DUMP_PHONETIC_HASH', 'DUMP_SUFFIX_TRIE', 'DUMP_TERMS', 'INVIDX_SUMMARY', 'NUMIDX_SUMMARY',
                     'GC_FORCEINVOKE', 'GC_FORCEBGINVOKE', 'GC_CLEAN_NUMERIC', 'GIT_SHA', 'TTL', 'VECSIM_INFO', 'DUMP_THREADPOOL_BACKTRACE']
        self.env.expect('FT.DEBUG', 'help').equal(help_list)

        for cmd in help_list:
            if cmd == 'GIT_SHA':
                # 'GIT_SHA' do not return err_msg
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
        self.env.expect('FT.DEBUG', 'invidx_summary', 'idx', 'meir').equal(['numDocs', 1, 'lastId', 1, 'flags',
                                                                            83, 'numberOfBlocks', 1, 'blocks',
                                                                            ['firstId', 1, 'lastId', 1, 'numEntries', 1]])

        self.env.expect('FT.DEBUG', 'INVIDX_SUMMARY', 'idx', 'meir').equal(['numDocs', 1, 'lastId', 1, 'flags',
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
                                                                           'lastDocId', 1, 'revisionId', 0])

        self.env.expect('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'age').equal(['numRanges', 1, 'numEntries', 1,
                                                                           'lastDocId', 1, 'revisionId', 0])

    def testUnexistsNumericIndexSummary(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'age1').raiseError()

    def testNumericIndexSummaryInvalidIdxName(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx1', 'age').raiseError()

    def testNumericIndexSummaryWrongArity(self):
        self.env.expect('FT.DEBUG', 'numidx_summary', 'idx1').raiseError()

    def testDumpSuffixWrongArity(self):
        self.env.expect('FT.DEBUG', 'DUMP_SUFFIX_TRIE', 'idx1', 'no_suffix').raiseError()

def get_and_check_threadpools_count(env, threadpools_dict, expected_len):
    threadpools_titles = threadpools_dict[0::2] if env.protocol == 2 else threadpools_dict
    env.assertEqual(len(threadpools_titles), expected_len)
    return threadpools_titles
    
def check_threads_count(env, threadpools_dict, expected_len, thpool_pos = 0):
    if env.protocol == 2:
        # In resp2 for each thpool the output is in the format of [thpool_title, [thread_bt_#0, thread_bt#1....]]
        resp2_thpool_output_len = 2
        curr_backtraces = thpool_pos * resp2_thpool_output_len + 1 # bt relative position 
        backtraces_titles = threadpools_dict[curr_backtraces][0::2]
    elif env.protocol == 3:
        backtraces_titles = threadpools_dict[thpool_pos]
    env.assertEqual(expected_len, len(backtraces_titles))
    
    
def DumpBacktrace_ALL(env: Env, threadpools_attr):
    # Ask for all threadpools
    threadpools_dict = env.cmd('FT.DEBUG', 'DUMP_THREADPOOL_BACKTRACE', 'ALL')
    threadpools_titles = get_and_check_threadpools_count(env, threadpools_dict, len(threadpools_attr))
    
    # Ensure that all the threadpools appear only once
    for i, threadpool in enumerate(threadpools_titles):
        match threadpool:
            case "=== GC THREADS BACKTRACE: ===":
                thpool_name = "GC"
            case "=== WORKERS THREADS BACKTRACE: ===":
                thpool_name = "WORKERS"
            case "=== CLEANSPEC THREADS BACKTRACE: ===":
                thpool_name = "CLEANSPEC"
            case _:
                env.assertTrue(False, message=(f"Threadpool title {threadpool} is unexpected"))
                continue
        env.assertEqual(threadpools_attr[thpool_name]["status"], 'NOT_FOUND')
        threadpools_attr[thpool_name]["status"] = 'FOUND'
        check_threads_count(env, 
                            threadpools_dict, 
                            expected_len= threadpools_attr[thpool_name]["threads_count"], 
                            thpool_pos = i if env.protocol == 2 else threadpool)
    
def threadpool_title(thpool_name):
    return f"=== {thpool_name} THREADS BACKTRACE: ==="

def DumpBacktrace(protocol):
    if MT_BUILD:
        WORKER_THREADS = 3
        expected_threadpools_cnt = 3
        module_args= f'WORKER_THREADS {WORKER_THREADS} MT_MODE MT_MODE_FULL'
    else:
        expected_threadpools_cnt = 2
        module_args=''
        
    env = Env(protocol=protocol, moduleArgs=module_args)
    env.skipOnCluster()
    
    # DUMMY threadpool returns an error
    env.expect('FT.DEBUG', 'DUMP_THREADPOOL_BACKTRACE', 'DUMMY').raiseError().contains('no such threadpool DUMMY')
    
    threadpools_attr = {
        "GC": {"title": threadpool_title("GC"), "status":'NOT_FOUND', "threads_count": 1},
        "CLEANSPEC": {"title": threadpool_title("CLEANSPEC"), "status":'NOT_FOUND', "threads_count": 1}
    }
    
    if MT_BUILD:
        threadpools_attr["WORKERS"] = {"title": threadpool_title("WORKERS"), "status":'NOT_FOUND', "threads_count": WORKER_THREADS}
    
    env.assertEqual(len(threadpools_attr), expected_threadpools_cnt)
    
    DumpBacktrace_ALL(env, threadpools_attr)
    
    for threadpool in threadpools_attr:
        dump_dict = env.cmd('FT.DEBUG', 'DUMP_THREADPOOL_BACKTRACE', threadpool)
        
        # One threadpool should return
        get_and_check_threadpools_count(env, dump_dict, 1)
        
        # Check it has the expected number of threads
        check_threads_count(env, dump_dict, threadpools_attr[threadpool]["threads_count"])
    

def testDumpBacktrace_resp3():
    DumpBacktrace(protocol=3)
    
def testDumpBacktrace_resp2():
    DumpBacktrace(protocol=2)
