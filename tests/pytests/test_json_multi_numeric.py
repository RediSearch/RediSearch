from itertools import chain
import json
import random

from common import *
from includes import *
from numpy import linspace

doc1_content = [
    {
        "name": "top1",
        "seq": [1, 5],
        "nested1":
        [
            {
                "name": "top1_1_1",
                "seq": [2, 3, 40]
            },
            {
                "name": "top1_1_2",
                "seq": [5, 4, 2, 2, 2]
            },
            {
                "name": "top1_1_3",
                "seq": [10]
            }
        ],
        "nested2":
        [
            {
                "name": "top1_2_1",
                "seq": [1.5, 1.6, 2]
            },
            {
                "name": "top1_2_2",
                "seq": [0]
            },
            {
                "name": "top1_2_3",
                "seq": [11]
            }
        ]

    },
    {
        "name": "top2",
        "seq": [2],
        "nested1":
        [
            {
                "name": "top2_1_1",
                "seq": []
            },
            {
                "name": "top2_1_2",
                "seq": [1]
            },
            {
                "name": "top2_1_3",
                "seq": [10, 20 ,30, 40, 50, 60]
            }
        ],
        "nested2":
        [
            {
                "name": "top2_2_1",
                "seq": [-1.5, -1.6, -2]
            },
            {
                "name": "top2_2_2",
                "seq": [3.1415]
            },
            {
                "name": "top2_2_3",
                "seq": [42, 64, -1, 10E+20, -10.0e-5]
            }
        ]
    }
]

doc_non_numeric_content = r'''{
    "attr1": [2, -7, null, 131.42, null , 0, null],
    "attr2": 131.42,
    "attr3": [null, null],
    "attr4": [],
    "attr5": null,
    "attr6": [1, 2, null, 131.42, null, "yikes" ],
    "attr7": [1, 2, null, 131.42, null, false ],
    "attr8": [1, 2, null, 131.42, null, {"obj": "ect"} ],
    "attr9": [1, 2, null, 131.42, null, ["no", "noo"] ],
    "attr10": [1, 2, null, 131.42, null, [7007] ]
}
'''

@skip(no_json=True)
def testBasic(env):
    """ Test multi numeric values (an array of numeric values or multiple numeric values) """

    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$..seq[*]', 'AS', 'seq', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$[0].nested2[0].seq', 'AS', 'seq', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx3', 'ON', 'JSON', 'SCHEMA', '$[1].nested2[2].seq', 'AS', 'seq', 'NUMERIC').ok()

    env.expect('FT.CREATE', 'idx4', 'ON', 'JSON', 'SCHEMA',
        '$[0].nested2[0].seq', 'AS', 'seq1', 'NUMERIC',         # [1.5, 1.6, 2]
        '$[1].nested2[2].seq', 'AS', 'seq2', 'NUMERIC').ok()     # [42, 64, -1, 10E+20, -10.0e-5]

    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))

    # Open/Close range and Not
    env.expect('FT.SEARCH', 'idx1', '@seq:[3 6]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx1', '-@seq:[3 6]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx1', '@seq:[-inf +inf]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx1', '-@seq:[-inf +inf]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx2', '@seq:[1.4 1.5]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '-@seq:[1.4 1.5]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx2', '-@seq:[1.4 (1.5]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '@seq:[1.4 (1.5]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx3', '@seq:[-0.0002 -0.0001]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx3', '@seq:[-0.0002 (-0.0001]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx3', '-@seq:[-0.0002 (-0.0001]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx3', '@seq:[-0.0002 (-0.0001]', 'NOCONTENT').equal([0])

    # Intersect
    env.expect('FT.SEARCH', 'idx4', '@seq1:[1.5 2.5] @seq2:[10e19 10e21]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx4', '@seq1:[1.5 2.5] @seq2:[40 41]', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx4', '@seq1:[0 1]     @seq2:[10e19 10e21]', 'NOCONTENT').equal([0])


@skip(no_json=True)
def testMultiNonNumeric(env):
    """
    test multiple NUMERIC values which include some non-numeric values at root level (null, text, bool, array, object)
    Skip nulls without failing
    Fail on text, bool, object, arr of strings, arr with mixed types
    """
    conn = getConnectionByEnv(env)

    non_numeric_dict = json.loads(doc_non_numeric_content)

    # Create indices and a key per index, e.g.,
    #   FT.CREATE idx1 ON JSON PREFIX 1 doc:1: SCHEMA $ AS root NUMERIC
    #   JSON.SET doc:1: $ '[2, -7, null, 131.42, null , 0, null]'
    #
    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_numeric_dict.values()):
        doc = 'doc:{}:'.format(i+1)
        idx = 'idx{}'.format(i+1)
        conn.execute_command('FT.CREATE', idx, 'ON', 'JSON', 'PREFIX', '1', doc, 'SCHEMA', '$', 'AS', 'root', 'NUMERIC')
        waitForIndex(env, idx)
        conn.execute_command('JSON.SET', doc, '$', json.dumps(v))
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, idx)['hash_indexing_failures']), res_failures, message=str(i))

    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@root:[131 132]', 'NOCONTENT').equal([1, 'doc:1:'])
    env.expect('FT.SEARCH', 'idx2', '@root:[131 132]', 'NOCONTENT').equal([1, 'doc:2:'])

@skip(no_json=True)
def testMultiNonNumericNested(env):
    """
    test multiple NUMERIC values which include some non-numeric values at inner level (null, text, bool, array, object)
    Skip nulls without failing
    Fail on text, bool, object, arr of strings, arr with mixed types
    """

    conn = getConnectionByEnv(env)

    non_numeric_dict = json.loads(doc_non_numeric_content)

    # Create indices, e.g.,
    #   FT.CREATE idx1 ON JSON SCHEMA $.attr1 AS attr NUMERIC
    for (i,v) in enumerate(non_numeric_dict.values()):
        conn.execute_command('FT.CREATE', 'idx{}'.format(i+1), 'ON', 'JSON', 'SCHEMA', '$.attr{}'.format(i+1), 'AS', 'attr', 'NUMERIC')
    conn.execute_command('JSON.SET', 'doc:1', '$', doc_non_numeric_content)

    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_numeric_dict.values()):
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, 'idx{}'.format(i+1))['hash_indexing_failures']), res_failures)

    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@attr:[131 132]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '@attr:[131 132]', 'NOCONTENT').equal([1, 'doc:1'])


@skip(no_json=True)
def testRange(env):
    """ Test multi numeric ranges """

    conn = getConnectionByEnv(env)

    arr_len = 20
    sub_arrays = [
        # positive
        [i for i in linspace(1, 5, num=arr_len)],       # float asc
        [i for i in linspace(5, 1, num=arr_len)],       # float desc
        [i for i in range(1, arr_len + 1)],             # int asc
        [i for i in range(arr_len, 0, -1)],             # int desc
        [0, 0],
        # negative
        [i for i in linspace(-1, -5, num=arr_len)],     # float desc
        [i for i in linspace(-5, -1, num=arr_len)],     # float asc
        [i for i in range(-1, -arr_len - 1, -1)],       # int desc
        [i for i in range(-arr_len, 0, 1)],             # int asc
        [-0, -0]
    ]
    doc_num = 5
    for doc in range(0, doc_num):
        top = {}
        for (i,arr) in enumerate(sub_arrays):
            delta = 100 if i < len(sub_arrays) / 2 else -100
            top['arr{}'.format(i+1)] = {'value': [v + doc * delta for v in arr]}
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc + 1), '$', json.dumps({'top': top}))

    env.expect('FT.CREATE', 'idx:all', 'ON', 'JSON', 'SCHEMA', '$..value[*]', 'AS', 'val', 'NUMERIC').ok()
    waitForIndex(env, 'idx:all')

    max_val = (doc_num - 1) * 100 + arr_len
    env.expect('FT.SEARCH', 'idx:all', '@val:[-inf (-{}]'.format(max_val), 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx:all', '@val:[({} +inf]'.format(max_val), 'NOCONTENT').equal([0])

    for dialect in [1, 2, 3]:
        for doc in range(doc_num, 0, -1):
            expected = [doc_num + 1 - doc]
            max_val = (doc - 1) * 100 + arr_len
            for i in range(doc_num, doc -1, -1):
                lastdoc = 'doc:{}'.format(i)
                expected.append(lastdoc)
            res = conn.execute_command('FT.SEARCH', 'idx:all',
                                       '@val:[-inf -{}]'.format(max_val),
                                       'NOCONTENT', 'DIALECT', dialect)
            env.assertEqual(toSortedFlatList(res), toSortedFlatList(expected),
                            message = '[-inf -{}]'.format(max_val))

            res = conn.execute_command('FT.SEARCH', 'idx:all',
                                       '@val:[{} +inf]'.format(max_val),
                                       'NOCONTENT', 'DIALECT', dialect)
            env.assertEqual(toSortedFlatList(res), toSortedFlatList(expected),
                            message = '[{} +inf]'.format(max_val))

            if dialect > 1:
                res = conn.execute_command('FT.SEARCH', 'idx:all',
                                           '@val:[{}]'.format(max_val),
                                           'NOCONTENT', 'DIALECT', dialect)
                env.assertEqual(toSortedFlatList(res), [1, lastdoc],
                                message = '[{}]'.format(lastdoc))

@skip(cluster=True, no_json=True)
def testDebugDump(env):
    """ Test FT.DEBUG DUMP_INVIDX and NUMIDX_SUMMARY with multi numeric values """

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx:top', 'ON', 'JSON', 'SCHEMA', '$[*]', 'AS', 'val', 'NUMERIC').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps([-1, 2, 3]))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps([-2, -1, 2]))

    env.expect(debug_cmd(), 'DUMP_NUMIDX' ,'idx:top', 'val').equal([[1, 2]])
    env.expect(debug_cmd(), 'NUMIDX_SUMMARY', 'idx:top', 'val').equal(['numRanges', 1, 'numEntries', 6,
                                                                      'lastDocId', 2, 'revisionId', 0,
                                                                      'emptyLeaves', 0, 'RootMaxDepth', 0])

@skip(cluster=True, no_json=True)
def testInvertedIndexMultipleBlocks(env):
    """ Test internal addition of new inverted index blocks (beyond INDEX_BLOCK_SIZE entries)"""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.arr', 'AS', 'arr', 'NUMERIC', '$.arr2', 'AS', 'arr2', 'NUMERIC').ok()
    overlap = 10
    doc_num = 1200
    # The first overlap docs (in 2nd value in arr) share the same value as the last overlap docs (in 1st value in arr)
    # So the same value is found in 2 docs, e.g., for 200 docs:
    #   JSON.SET doc:195 $ '{\"arr\": [195, 385], \"arr2\": [195]}'
    #   JSON.SET doc:194 $ '{\"arr\": [194, 384], \"arr2\": [194]}'
    #   JSON.SET doc:193 $ '{\"arr\": [193, 383], \"arr2\": [193]}'
    #   JSON.SET doc:192 $ '{\"arr\": [192, 382], \"arr2\": [192]}'
    #   JSON.SET doc:191 $ '{\"arr\": [191, 381], \"arr2\": [191]}'
    #   ...
    #   JSON.SET doc:5 $ '{\"arr\": [5, 195], \"arr2\": [5]}'
    #   JSON.SET doc:4 $ '{\"arr\": [4, 194], \"arr2\": [4]}'
    #   JSON.SET doc:3 $ '{\"arr\": [3, 193], \"arr2\": [3]}'
    #   JSON.SET doc:2 $ '{\"arr\": [2, 192], \"arr2\": [2]}'
    #   JSON.SET doc:1 $ '{\"arr\": [1, 191], \"arr2\": [1]}'
    for doc in range(doc_num, 0, -1):
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({ 'arr':  [doc, doc + doc_num - overlap],
                                                                                 'arr2': [doc]}))
    expected_ids = range(1, doc_num + 1)
    res = conn.execute_command(debug_cmd(), 'DUMP_NUMIDX' ,'idx', 'arr')
    env.assertEqual(set(toSortedFlatList(res)), set(expected_ids), message='DUMP_NUMIDX')

    res = to_dict(conn.execute_command(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'arr'))
    env.assertEqual(res['numEntries'], doc_num * 2)
    env.assertEqual(res['lastDocId'], doc_num)

    # Should find the first and last overlap docs
    # e.g., for 200 docs:
    #   FT.SEARCH idx '@arr:[191 200]' NOCONTENT LIMIT 0 20
    res = conn.execute_command('FT.SEARCH', 'idx', '@arr:[{} {}]'.format(doc_num - overlap + 1, doc_num), 'NOCONTENT', 'LIMIT', '0', overlap * 2)
    expected_docs = ['doc:{}'.format(i) for i in chain(range(1, overlap + 1), range(doc_num - overlap + 1, doc_num + 1))]
    env.assertEqual(toSortedFlatList(res[1:]),toSortedFlatList(expected_docs), message='FT.SEARCH')


def checkInfoAndGC(env, idx, doc_num, create, delete):
    """ Helper function for testInfoAndGC """
    conn = getConnectionByEnv(env)

    # Start empty
    env.assertEqual(True, True, message = 'check {}'.format(idx))
    info = index_info(env, idx)
    env.assertEqual(int(info['num_docs']), 0)
    env.assertLessEqual(int(info['total_inverted_index_blocks']), 1) # 1 block might already be there
    env.assertEqual(int(info['inverted_sz_mb']), 0)

    create(env, doc_num)

    # Consume something
    info = index_info(env, idx)
    env.assertEqual(int(info['num_docs']), doc_num)
    env.assertGreater(int(info['total_inverted_index_blocks']), doc_num / 100)
    env.assertGreater(float(info['inverted_sz_mb']), 0)

    delete(env, doc_num)
    forceInvokeGC(env, idx)

    # Cleaned up
    info = index_info(env, idx)
    env.assertEqual(int(info['num_docs']), 0)
    env.assertLessEqual(int(info['total_inverted_index_blocks']), 1) # 1 block might be left
    env.assertEqual(float(info['inverted_sz_mb']), 0)

def printSeed(env):
    # Print the random seed for reproducibility
    seed = str(time.time())
    env.assertNotEqual(seed, None, message='random seed ' + seed)
    random.seed(seed)

@skip(cluster=True, no_json=True)
def testInfoAndGC(env):
    """ Test cleanup of numeric ranges """
    if env.env == 'existing-env':
        env.skip()
    conn = getConnectionByEnv(env)

    printSeed(env)

    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()

    # Various lambdas to create and delete docs
    def create_json_docs_multi(env, doc_num):
        for doc in range(1, doc_num + 1):
            if doc % 10:
                val_count = random.randint(1, 50)
            else:
                # Fill up an inverted index block with all values from the same doc
                val_count = random.randint(100, 150)
            val_list = [random.uniform(1, 100000) for i in range(val_count)]
            conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({'top': val_list}))

    def create_json_docs_single(env, doc_num):
        for doc in range(1, doc_num + 1):
            conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({'top': random.uniform(1, 100000)}))

    def delete_json_docs(env, doc_num):
        for doc in range(1, doc_num + 1):
            conn.execute_command('JSON.DEL', 'doc:{}'.format(doc), '$')

    def create_hash_docs(env, doc_num):
        for doc in range(1, doc_num + 1):
            conn.execute_command('HSET', 'doc:{}'.format(doc), 'top', random.uniform(1, 100000))

    def delete_hash_docs(env, doc_num):
        for doc in range(1, doc_num + 1):
            conn.execute_command('DEL', 'doc:{}'.format(doc), '$')

    # The actual test
    doc_num = 1000

    # JSON multi
    env.expect('FT.CREATE', 'idx_json_mult', 'ON', 'JSON', 'SCHEMA', '$.top[*]', 'AS', 'val', 'NUMERIC').ok()
    checkInfoAndGC(env, 'idx_json_mult', doc_num, create_json_docs_multi, delete_json_docs)

    # JSON single
    env.flush()
    env.expect('FT.CREATE', 'idx_json_single', 'ON', 'JSON', 'SCHEMA', '$.top', 'AS', 'val', 'NUMERIC').ok()
    checkInfoAndGC(env, 'idx_json_single', doc_num, create_json_docs_single, delete_json_docs)

    # Hash
    env.flush()
    env.expect('FT.CREATE', 'idx_hash', 'ON', 'HASH', 'SCHEMA', 'top', 'NUMERIC').ok()
    checkInfoAndGC(env, 'idx_hash', doc_num, create_hash_docs, delete_hash_docs)


def prepareSortBy(env, is_flat_arr, default_dialect):
    """ Helper function for testing sort of multi numeric values """

    printSeed(env)

    dialect_param = ['DIALECT', 3] if not default_dialect else []

    conn = getConnectionByEnv(env)
    jsonpath = '$.top[*]' if is_flat_arr else '$.top'
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', jsonpath, 'AS', 'val', 'NUMERIC').ok()

    doc_num = 200
    for doc in range(1, doc_num + 1):
            val_count = random.randint(0, 10)
            val_list = [random.uniform(1, 100000) for i in range(val_count)]
            # Allow also empty arrays
            if val_count:
                val_list.insert(0, -doc)
                # Set the first value which is the sort key
                val_list.insert(0, doc)
            conn.execute_command('JSON.SET', '{}'.format(doc), '$', json.dumps({'top': val_list}))

    # Make sure there are at least 2 result
    query = ['FT.SEARCH', 'idx',
        '@val:[3000 8000] | @val:[{} {}] | @val:[{} {}]'.format(int(doc_num/2), int(doc_num/2), doc_num, doc_num),
        'NOCONTENT', 'LIMIT', 0, doc_num, *dialect_param]
    return query

def checkSortByBWC(env, is_flat_arr):
    """ Helper function for backward compatibility of sorting multi numeric values """

    default_dialect = True
    env.assertEqual(1, 1, message='flat {}, default dialect {}'.format(is_flat_arr, default_dialect))
    query = prepareSortBy(env, is_flat_arr, default_dialect)
    conn = getConnectionByEnv(env)

    # Path leading to an array was loading a JSON string representation of the array,
    # Comparing values lexicographically
    #
    # Path leading to multi value was loading the first element (in this case it is numeric),
    # Comparing values according to type of first element
    def checkGreater(a, b):
        if is_flat_arr:
            env.assertGreater(int(a), int(b))
        else:
            env.assertGreater(str(a), str(b))

    def checkLess(a, b):
        if is_flat_arr:
            env.assertLess(int(a), int(b))
        else:
            env.assertLess(str(a), str(b))

    # Results should be ascending
    res = conn.execute_command(*query, 'SORTBY', 'val')
    for i in range(2, len(res)):
        checkGreater(int(res[i]), int(res[i - 1]))

    # Results should be descending
    res = conn.execute_command(*query, 'SORTBY', 'val', 'DESC')
    for i in range(2, len(res)):
        checkLess(int(res[i]), int(res[i - 1]))

@skip(no_json=True)
def testSortByBWC(env):
    """ Test sorting multi numeric values with flat array """
    checkSortByBWC(env, True)

@skip(no_json=True)
def testSortByArrBWC(env):
    """ Test backward compatibility of sorting multi numeric values with array """
    checkSortByBWC(env, False)


def checkSortBy(env, is_flat_arr):
    """ Helper function for testing of sorting multi numeric values """

    default_dialect = False
    env.assertEqual(1, 1, message='flat {}, default dialect {}'.format(is_flat_arr, default_dialect))
    query = prepareSortBy(env, is_flat_arr, default_dialect)
    conn = getConnectionByEnv(env)

    # Results should be ascending
    res = conn.execute_command(*query, 'SORTBY', 'val')
    for i in range(2, len(res)):
        env.assertGreater(int(res[i]), int(res[i - 1]))

    # Results should be descending
    res = conn.execute_command(*query, 'SORTBY', 'val', 'DESC')
    for i in range(2, len(res)):
        env.assertLess(int(res[i]), int(res[i - 1]))

@skip(no_json=True)
def testSortBy(env):
    """ Test sorting multi numeric values with flat array """
    checkSortBy(env, True)

@skip(no_json=True)
def testSortByArr(env):
    """ Test sorting multi numeric values with array """
    checkSortBy(env, False)

def keep_dict_keys(dict, keys):
        return {k:v for k,v in dict.items() if k in keys}

@skip(no_json=True)
def testInfoStats(env):
    """ Check that stats of single value are equivalent to multi value"""

    printSeed(env)
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx:single', 'ON', 'JSON', 'PREFIX', 1, 'doc:single:', 'SCHEMA', '$.top', 'AS', 'val', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx:multi', 'ON', 'JSON', 'PREFIX', 1, 'doc:multi:', 'SCHEMA', '$.top', 'AS', 'val', 'NUMERIC').ok()

    doc_num = 200
    doc_created = 0
    while doc_created < doc_num:
        val_count = random.randint(1, 5)
        if doc_created + val_count > doc_num:
            val_count = doc_num - doc_created
        val_list = [random.uniform(1, 100000) for i in range(val_count)]
        doc_created += 1
        # Single doc with multi value
        conn.execute_command('JSON.SET', 'doc:multi:{{{}}}'.format(doc_created), '$', json.dumps({'top': val_list}))
        # Multi docs with single value
        for i in range(val_count):
            conn.execute_command('JSON.SET', 'doc:single:{{{}}}'.format(doc_created + i), '$', json.dumps({'top': val_list[i]}))
        doc_created += val_count - 1

    interesting_attr = ['num_records', 'total_inverted_index_blocks']
    info_single = keep_dict_keys(index_info(env, 'idx:single'), interesting_attr)
    info_multi = keep_dict_keys(index_info(env, 'idx:multi'), interesting_attr)
    env.assertEqual(info_single, info_multi)

@skip(no_json=True)
def testInfoStatsAndSearchAsSingle(env):
    """ Check that search results and relevant stats are the same for single values and equivalent multi values """

    printSeed(env)

    conn = getConnectionByEnv(env)
    max_attr_num = 5
    schema_list = [['$.val{}'.format(i), 'AS', 'val{}'.format(i), 'NUMERIC'] for i in range(1, max_attr_num + 1)]
    create_idx_single = ['FT.CREATE', 'idx:single', 'ON', 'JSON', 'PREFIX', 1, 'doc:single:', 'SCHEMA']
    [create_idx_single.extend(a) for a in schema_list]
    create_idx_multi = ['FT.CREATE', 'idx:multi', 'ON', 'JSON', 'PREFIX', 1, 'doc:multi:', 'SCHEMA']
    create_idx_multi.extend(schema_list[0])
    # Create 2 indeices such as
    #  FT.CREATE idx:single ON JSON PREFIX 1 doc:single: SCHEMA $.val1 AS val1 NUMERIC $.val2 AS val2 NUMERIC ... $.val5 AS val5 NUMERIC
    # and
    #  FT.CREATE idx:multi ON JSON PREFIX 1 doc:multi: SCHEMA $.val1 AS val1 NUMERIC
    env.expect(*create_idx_single).ok()
    env.expect(*create_idx_multi).ok()

    doc_num = 200
    for doc in range(1, doc_num + 1):
        val_count = random.randint(1, max_attr_num)
        val_list = [random.uniform(-50000, 50000) for i in range(val_count)]
        # Use slot id tag to make results from single and multi indices in same order
        # Doc with a single multi value, e.g.,
        #  JSON.SET doc:single:1 $ '{"val1": 10, "val2": 20, "val3": 30}'
        conn.execute_command('JSON.SET', 'doc:multi:{{{}}}'.format(doc), '$', json.dumps({'val1': val_list}))
        # Doc with several single values, e.g.,
        #  JSON.SET doc:multi:1 $ '{"val1": [10, 20, 30]}'
        json_val = {k:v for (k,v) in zip(['val{}'.format(i + 1) for i in range(val_count)], val_list)}
        conn.execute_command('JSON.SET', 'doc:single:{{{}}}'.format(doc), '$', json.dumps(json_val))

    # Compare INFO stats
    interesting_attr = ['num_docs', 'max_doc_id', 'num_records', 'total_inverted_index_blocks']
    info_single = keep_dict_keys(index_info(env, 'idx:single'), interesting_attr)
    info_multi = keep_dict_keys(index_info(env, 'idx:multi'), interesting_attr)
    env.assertEqual(info_single, info_multi)

    # Compare search results
    for _ in range(1000):
        val_from = random.uniform(-70000, 70000)
        val_to = max(1000, val_from + random.uniform(1, 140000 - val_from))
        expression_for_single = '|'.join(['@val{}:[{} {}]'.format(i, val_from, val_to) for i in range(1, max_attr_num + 1)])
        res_single = conn.execute_command('FT.SEARCH', 'idx:single', expression_for_single, 'NOCONTENT')
        res_single = list(map(lambda v: v.replace(':single:', '::') if isinstance(v, str) else v, res_single))
        res_multi = conn.execute_command('FT.SEARCH', 'idx:multi', '@val1:[{} {}]'.format(val_from, val_to), 'NOCONTENT')
        res_multi = list(map(lambda v: v.replace(':multi:', '::') if isinstance(v, str) else v, res_multi))
        env.assertEqual(res_single, res_multi, message = '[{} {}]'.format(val_from, val_to))

@skip(cluster=True, no_json=True)
def testConsecutiveValues(env):
    """ Test with many consecutive values which should cause range tree to do rebalancing (also for code coverage) """
    if env.env == 'existing-env':
        env.skip()

    conn = getConnectionByEnv(env)

    num_docs = 10000

    # Add values from -5000 to 5000
    # Add to the right, rebalance to the left
    i = -5000
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'NUMERIC').ok()
    for doc in range(1, num_docs + 1):
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({'val': [i, i+1]}))
        i = i + 1

    env.expect('FT.SEARCH', 'idx', '@val:[-5000 -4999]', 'NOCONTENT').equal([2, 'doc:1', 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@val:[5 6]', 'NOCONTENT').equal([3, 'doc:5005', 'doc:5006', 'doc:5007'])
    env.expect('FT.SEARCH', 'idx', '@val:[4999 5000]', 'NOCONTENT').equal([2, 'doc:9999', 'doc:10000'])
    summary1 = env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'val')

    # Add values from 5000 to -5000
    # Add to the left, rebalance to the right
    env.flush()
    i = 5000
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'NUMERIC').ok()
    for doc in range(1, num_docs + 1):
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({'val': [i, i-1]}))
        i = i - 1

    env.expect('FT.SEARCH', 'idx', '@val:[4999 5000]', 'NOCONTENT').equal([2, 'doc:1', 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@val:[-6 -5]', 'NOCONTENT').equal([3, 'doc:5005', 'doc:5006', 'doc:5007'])
    env.expect('FT.SEARCH', 'idx', '@val:[-5000 -4999]', 'NOCONTENT').equal([2, 'doc:9999', 'doc:10000'])
    summary2 = env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'val')

    env.assertEqual(summary1, summary2)

@skip(cluster=True, no_json=True)
def testDebugRangeTree(env):
    """ Test debug of range tree """
    if env.env == 'existing-env':
        env.skip()
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'NUMERIC').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'val': [1, 2, 3]}))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps({'val': [1, 2, 3]}))
    conn.execute_command('JSON.SET', 'doc:3', '$', json.dumps({'val': [3, 4, 5]}))

    env.expect(debug_cmd(), 'DUMP_NUMIDXTREE', 'idx', 'val').equal(['numRanges', 1, 'numEntries', 9, 'lastDocId', 3, 'revisionId', 0, 'uniqueId', 0, 'emptyLeaves', 0,
        'root', ['range', ['minVal', str(1), 'maxVal', str(5), 'unique_sum', str(0), 'invertedIndexSize [bytes]', str(109), 'card', 0, 'cardCheck', 1, 'splitCard', 16,
                'entries', ['numDocs', 3, 'numEntries', 9, 'lastId', 3, 'size', 1, 'blocks_efficiency (numEntries/size)', str(9), 'values',
                    ['value', str(1), 'docId', 1, 'value', str(2), 'docId', 1, 'value', str(3), 'docId', 1, 'value', str(1), 'docId', 2, 'value', str(2), 'docId', 2, 'value', str(3), 'docId', 2, 'value', str(3), 'docId', 3, 'value', str(4), 'docId', 3, 'value', str(5), 'docId', 3]]]],
            'Tree stats:', ['Average memory efficiency (numEntries/size)/numRanges', str(9)]])

def checkUpdateNumRecords(env, is_json):
    """ Helper function for testing update of `num_records` """
    if env.env == 'existing-env':
        env.skip()
    conn = getConnectionByEnv(env)

    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()

    if is_json:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'NUMERIC').ok()
        conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'val': [1, 2, 3]}))
        conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps({'val': [1, 2, 3]}))
        conn.execute_command('JSON.SET', 'doc:3', '$', json.dumps({'val': [3, 4, 5]}))
    else:
        env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'val1', 'NUMERIC', 'val2', 'NUMERIC', 'val3', 'NUMERIC').ok()
        conn.execute_command('HSET', 'doc:1', 'val1', 1, 'val2', 2, 'val3', 3)
        conn.execute_command('HSET', 'doc:2', 'val1', 1, 'val2', 2, 'val3', 3)
        conn.execute_command('HSET', 'doc:3', 'val1', 3, 'val2', 4, 'val3', 5)

    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], 9)

    # Update doc to have one value less
    if is_json:
        conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'val': [1, 2]}))
    else:
        conn.execute_command('HDEL', 'doc:1', 'val3')

    # INFO is not accurately updated before GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], 11)

    forceInvokeGC(env, 'idx')

    # Info is accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], 8)

    # Delete doc
    if is_json:
        conn.execute_command('JSON.DEL', 'doc:1', '$')
    else:
        conn.execute_command('DEL', 'doc:1')

    # INFO is not accurately updated before GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], 8)

    forceInvokeGC(env, 'idx')

    # Info is accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], 6)

    if is_json:
        conn.execute_command('JSON.DEL', 'doc:2', '$')
        conn.execute_command('JSON.DEL', 'doc:3', '$')
    else:
        conn.execute_command('DEL', 'doc:2')
        conn.execute_command('DEL', 'doc:3')

    # Info is not accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], 6)

    forceInvokeGC(env, 'idx')

    # Info is accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], 0)

@skip(cluster=True, no_json=True)
def testUpdateNumRecordsJson(env):
    """ Test update of `num_records` when using JSON """
    checkUpdateNumRecords(env, True)

@skip(cluster=True)
def testUpdateNumRecordsHash(env):
    """ Test update of `num_records` when using Hashes """
    checkUpdateNumRecords(env, False)

def checkMultiNumericReturn(env, expected, default_dialect, is_sortable):
    """ Helper function for RETURN with multiple NUMERIC values """

    conn = getConnectionByEnv(env)

    dialect_param = ['DIALECT', 3] if not default_dialect else []
    sortable_param = ['SORTABLE'] if is_sortable else []
    message='dialect {}, sortable {}'.format('default' if default_dialect else 3, is_sortable)
    env.assertEqual(len(expected), 3, message=message)

    env.expect('FT.CREATE', 'idx_flat', 'ON', 'JSON', 'SCHEMA', '$.arr[*]', 'AS', 'val', 'NUMERIC', *sortable_param).ok()
    env.expect('FT.CREATE', 'idx_arr', 'ON', 'JSON', 'SCHEMA', '$.arr', 'AS', 'val', 'NUMERIC', *sortable_param).ok()
    doc1_content = {"arr":[1, 2, 3]}
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))

    # Multi flat
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_flat', '@val:[2 3]', 'RETURN', '3', '$.arr[1]', 'AS', 'arr_1', *dialect_param),
                    expected[0], message=message)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_flat', '@val:[2 3]', 'RETURN', '1', 'val', *dialect_param),
                    expected[1], message=message)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_flat', '@val:[2 3]', 'RETURN', '3', '$.arr[*]', 'AS', 'val', *dialect_param),
                    expected[1], message=message)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_flat', '@val:[2 3]', 'RETURN', '3', '$.arr', 'AS', 'val', *dialect_param),
                    expected[2], message=message)

    env.assertEqual(conn.execute_command('FT.AGGREGATE', 'idx_flat', '@val:[2 3]', 'LOAD', '1', '@val', *dialect_param),
                    [1, ['val', expected[1][2][1]]], message=message)

    env.assertEqual(conn.execute_command('FT.AGGREGATE', 'idx_flat', '@val:[2 3]', 'GROUPBY', '1', '@val', *dialect_param),
                    [1, ['val', expected[1][2][1]]], message=message)

    # Array
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_arr', '@val:[2 3]', 'RETURN', '3', '$.arr[1]', 'AS', 'arr_1', *dialect_param),
                    expected[0], message=message)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_arr', '@val:[2 3]', 'RETURN', '1', 'val', *dialect_param),
                    expected[2], message=message)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_arr', '@val:[2 3]', 'RETURN', '3', '$.arr[*]', 'AS', 'val', *dialect_param),
                    expected[1], message=message)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx_arr', '@val:[2 3]', 'RETURN', '3', '$.arr', 'AS', 'val', *dialect_param),
                    expected[2], message=message)

    res = conn.execute_command('FT.AGGREGATE', 'idx_arr',
        '@val:[2 3]', 'GROUPBY', '1', '@val', *dialect_param)
    # Ignore the result with older dialect
    #  Schema attribute with path to an array was not supported (lead to indexing failure)
    if not default_dialect:
        env.assertEqual(res, [1, ['val', expected[2][2][1]]])


    env.assertEqual(conn.execute_command('FT.AGGREGATE', 'idx_arr', '@val:[2 3]', 'LOAD', '1', '@val', *dialect_param),
                    [1, ['val', expected[2][2][1]]], message=message)

    # RETURN ALL
    res = conn.execute_command('FT.SEARCH', 'idx_flat', '@val:[2 3]', *dialect_param)
    env.assertEqual(json.loads(res[2][1]), [doc1_content] if not default_dialect else doc1_content)

@skip(no_json=True)
def testMultiNumericReturn(env):
    """ test RETURN with multiple NUMERIC values """

    res1 = [1, 'doc:1', ['arr_1', '[2]']]
    res2 = [1, 'doc:1', ['val', '[1,2,3]']]
    res3 = [1, 'doc:1', ['val', '[[1,2,3]]']]

    checkMultiNumericReturn(env, [res1, res2, res3], False, False)
    env.flush()
    checkMultiNumericReturn(env, [res1, res2, res3], False, True)

@skip(no_json=True)
def testMultiNumericReturnBWC(env):
    """ test backward compatibility of RETURN with multiple NUMERIC values """
    res1 = [1, 'doc:1', ['arr_1', '2']]
    res2 = [1, 'doc:1', ['val', '1']]
    res3 = [1, 'doc:1', ['val', '[1,2,3]']]

    checkMultiNumericReturn(env, [res1, res2, res3], True, False)
    env.flush()
    checkMultiNumericReturn(env, [res1, res2, res3], True, True)
