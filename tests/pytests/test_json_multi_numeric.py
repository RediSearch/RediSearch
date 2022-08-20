from itertools import chain
import json

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


def testInfo(env):
    #TODO:
    pass

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
    
    max_val = (doc_num - 1) * 100 + arr_len
    env.expect('FT.SEARCH', 'idx:all', '@val:[-inf (-{}]'.format(max_val), 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx:all', '@val:[({} +inf]'.format(max_val), 'NOCONTENT').equal([0])
    
    for doc in range(doc_num, 0, -1):
        expected = [doc_num + 1 - doc]
        max_val = (doc - 1) * 100 + arr_len
        for i in range(doc_num, doc -1, -1):
            expected.append('doc:{}'.format(i))
        res = conn.execute_command('FT.SEARCH', 'idx:all', '@val:[-inf -{}]'.format(max_val), 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected), message = '[-inf -{}]'.format(max_val))

        res = conn.execute_command('FT.SEARCH', 'idx:all', '@val:[{} +inf]'.format(max_val), 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected), message = '[{} +inf]'.format(max_val))

def testDebugDump(env):
    """ Test FT.DEBUG DUMP_INVIDX and NUMIDX_SUMMARY with multi numeric values """

    env.skipOnCluster()

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx:top', 'ON', 'JSON', 'SCHEMA', '$[*]', 'AS', 'val', 'NUMERIC').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps([-1, 2, 3]))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps([-2, -1, 2]))

    env.expect('FT.DEBUG', 'DUMP_NUMIDX' ,'idx:top', 'val').equal([[1, 2]])
    env.expect('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx:top', 'val').equal(['numRanges', 1, 'numEntries', 6,
                                                                      'lastDocId', 2, 'revisionId', 0])

def testInvertedIndexBlockNum(env):
    """ Test internal addition of new inverted index block """

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.arr', 'AS', 'arr', 'NUMERIC', '$.arr2', 'AS', 'arr2', 'NUMERIC').ok()
    overlap = 10
    doc_num = 200
    for doc in range(doc_num, 0, -1):
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({ 'arr':  [doc, doc + doc_num - overlap],
                                                                                 'arr2': [doc]}))
    expected_ids = range(1, doc_num + 1)
    res = conn.execute_command('FT.DEBUG', 'DUMP_NUMIDX' ,'idx', 'arr')
    env.assertListEqual(set(toSortedFlatList(res)), set(expected_ids), message='DUMP_NUMIDX')

    res = to_dict(conn.execute_command('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'arr'))
    env.assertEqual(res['numEntries'], doc_num * 2)
    env.assertEqual(res['lastDocId'], doc_num)

    res = conn.execute_command('FT.SEARCH', 'idx', '@arr:[{} {}]'.format(doc_num - overlap + 1, doc_num), 'NOCONTENT')
    expected_docs = ['doc:{}'.format(i) for i in chain(range(1, overlap + 1), range(doc_num - overlap + 1, doc_num + 1))]
    env.assertListEqual(toSortedFlatList(res[1:]),toSortedFlatList(expected_docs), message='FT.SEARCH')


    
