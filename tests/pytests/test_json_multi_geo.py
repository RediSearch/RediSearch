from itertools import chain
import json
import random

from common import *
from includes import *
from numpy import linspace

doc1_content = [
    {
        "name": "top1",
        "loc": ["1,2", "3,4"],
        "nested1":
        [
            {
                "name": "top1_1_1",
                "loc": ["2,3", "40,25"]
            },
            {
                "name": "top1_1_2",
                "loc": ["21.2,21.3", "21.4,21.5", "22,22"]
            },
            {
                "name": "top1_1_3",
                "loc": ["10,20"]
            }
        ],
        "nested2":
        [
            {
                "name": "top1_2_1",
                "loc": ["1.2,1.3", "1.4,1.5", "2,2"]
            },
            {
                "name": "top1_2_2",
                "loc": ["0,0"]
            },
            {
                "name": "top1_2_3",
                "loc": ["11,11", "12,12"]
            }
        ]
    
    },
    {
        "name": "top2",
        "loc": ["2,0"],
        "nested1":
        [
            {
                "name": "top2_1_1",
                "loc": []
            },
            {
                "name": "top2_1_2",
                "loc": ["1,2"]
            },
            {
                "name": "top2_1_3",
                "loc": []
            }
        ],
        "nested2":
        [
            {
                "name": "top2_2_1",
                "loc": ["-1.5,-1.6", "-2,2"]
            },
            {
                "name": "top2_2_2",
                "loc": ["3.1415,2.7182"]
            },
            {
                "name": "top2_2_3",
                "loc": ["42,64", "-50,-72", "-100,-20", "43.422649,11.126973", "29.497825,-82.141870"]
            }
        ]
    }
]

doc_non_geo_content = r'''{
    "attr1": ["29.725425,34.967088", "-1.23,-4.56", null, "31.312604,35.352586", null , "-1,-2", null],
    "attr2": ["29.725425,34.967088"],
    "attr3": [null, null],
    "attr4": [],
    "attr5": null,
    "attr6": ["29.725425"],
    "attr7": ["-1.23,-4.56", "7.8,9.0", null, "31,32", null, "yikes" ],
    "attr8": ["-1.23,-4.56", "7.8,9.0", null, "31,32", null, false ],
    "attr9": ["-1.23,-4.56", "7.8,9.0", null, "31,32", null, {"obj": "ect"} ],
    "attr10": ["-1.23,-4.56", "7.8,9.0", null, "31,32", null, ["no", "noo"] ],
    "attr11": ["-1.23,-4.56", "7.8,9.0", null, "31,32", null, ["31,32"] ]
}
'''

def testBasic(env):
    """ Test multi GEO values (an array of GEO values or multiple GEO values) """

    conn = getConnectionByEnv(env)
    
    env.expect('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$..loc[*]', 'AS', 'loc', 'GEO').ok()
    env.expect('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA',
        '$[0].nested2[0].loc', 'AS', 'loc', 'GEO').ok()     # ["1.2,1.3", "1.4,1.5", "2,2"]
    env.expect('FT.CREATE', 'idx3', 'ON', 'JSON', 'SCHEMA',
        '$[1].nested2[2].loc', 'AS', 'loc', 'GEO').ok()     # ["42,64", "-50,-72", "-100,-20", "43.422649,11.126973", "29.497825,-82.141870"]

    env.expect('FT.CREATE', 'idx4', 'ON', 'JSON', 'SCHEMA',
        '$[0].nested2[0].loc', 'AS', 'loc1', 'GEO',         # ["1.2,1.3", "1.4,1.5", "2,2"]
        '$[1].nested2[2].loc', 'AS', 'loc2', 'GEO').ok()    # ["42,64", "-50,-72", "-100,-20", "43.422649,11.126973", "29.497825,-82.141870"]

    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))
    
    # Geo range and Not
    env.expect('FT.SEARCH', 'idx1', '@loc:[1.2 1.1 40 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx1', '-@loc:[1.2 1.1 40 km]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx1', '@loc:[0 0 +inf km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx1', '-@loc:[0 0 +inf km]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx2', '@loc:[1.42 1.52 5 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '-@loc:[1.42 1.52 5 km]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx3', '-@loc:[82 82 200 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx3', '@loc:[82 82 200 km]', 'NOCONTENT').equal([0])

    # Intersect
    env.expect('FT.SEARCH', 'idx4', '@loc1:[1.2 1.1 40 km] @loc2:[29.5 -82 20 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx4', '@loc1:[1.2 1.1 40 km] @loc2:[50 50 1 km]', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx4', '@loc1:[-10 -10 1 km]     @loc2:[29.5 -82 20 km]', 'NOCONTENT').equal([0])


def testMultiNonGeo(env):
    """
    test multiple GEO values which include some non-geo values at root level (null, numeric, text with illegal coordinates, bool, array, object)
    Skip nulls without failing
    Fail on text with illegal coordinates, numeric, bool, object, arr of strings, arr with mixed types
    """
    conn = getConnectionByEnv(env)
    
    non_geo_dict = json.loads(doc_non_geo_content)
    
    # Create indices and a key per index, e.g.,
    #   FT.CREATE idx1 ON JSON PREFIX 1 doc:1: SCHEMA $ AS root GEO
    #   JSON.SET doc:1: $ '["1,1", ...]'
    #
    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_geo_dict.values()):
        doc = 'doc:{}:'.format(i+1)
        idx = 'idx{}'.format(i+1)
        conn.execute_command('FT.CREATE', idx, 'ON', 'JSON', 'PREFIX', '1', doc, 'SCHEMA', '$', 'AS', 'root', 'GEO')
        waitForIndex(env, idx)
        conn.execute_command('JSON.SET', doc, '$', json.dumps(v))
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, idx)['hash_indexing_failures']), res_failures, message=str(i))
    
    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@root:[29.72 34.96 1 km]', 'NOCONTENT').equal([1, 'doc:1:'])
    env.expect('FT.SEARCH', 'idx2', '@root:[29.72 34.96 1 km]', 'NOCONTENT').equal([1, 'doc:2:'])

def testMultiNonGeoNested(env):
    """
    test multiple GEO values which include some non-geo values at inner level (null, numeric, text with illegal coordinates, bool, array, object)
    Skip nulls without failing
    Fail on text with illegal coordinates, numeric, bool, object, arr of strings, arr with mixed types
    """

    conn = getConnectionByEnv(env)

    non_geo_dict = json.loads(doc_non_geo_content)
    
    # Create indices, e.g.,
    #   FT.CREATE idx1 ON JSON SCHEMA $.attr1 AS attr GEO
    for (i,v) in enumerate(non_geo_dict.values()):
        conn.execute_command('FT.CREATE', 'idx{}'.format(i+1), 'ON', 'JSON', 'SCHEMA', '$.attr{}'.format(i+1), 'AS', 'attr', 'GEO')
    conn.execute_command('JSON.SET', 'doc:1', '$', doc_non_geo_content)
    
    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_geo_dict.values()):
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, 'idx{}'.format(i+1))['hash_indexing_failures']), res_failures)
    
    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@attr:[29.72 34.96 1 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '@attr:[29.72 34.96 1 km]', 'NOCONTENT').equal([1, 'doc:1'])



def testRange(env):
    """ Test multi geo ranges """

    conn = getConnectionByEnv(env)

    arr_len = 20
    sub_arrays = [
        # positive        
        [i for i in linspace(1, 5, num=arr_len)],       # float asc
        [i for i in linspace(5, 1, num=arr_len)],       # float desc
        [i for i in range(1, arr_len + 1)],             # int asc
        [i for i in range(arr_len, 0, -1)],             # int desc
        [0],
        # negative
        [i for i in linspace(-1, -5, num=arr_len)],     # float desc
        [i for i in linspace(-5, -1, num=arr_len)],     # float asc
        [i for i in range(-1, -arr_len - 1, -1)],       # int desc
        [i for i in range(-arr_len, 0, 1)],             # int asc
        [-0]
    ]
    doc_num = 3
    delta_size = 30

    for doc in range(0, doc_num):
        top = {}
        for (i,arr) in enumerate(sub_arrays):
            delta = delta_size if i < len(sub_arrays) / 2 else - delta_size
            top['arr{}'.format(i+1)] = {'loc': ["{},{}".format(v + doc * delta,v + doc * delta) for v in arr]}
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc + 1), '$', json.dumps({'top': top}))

    env.expect('FT.CREATE', 'idx:all', 'ON', 'JSON', 'SCHEMA', '$..loc[*]', 'AS', 'loc', 'GEO').ok()
    
    max_val = (doc_num - 1) * delta_size + arr_len
    env.expect('FT.SEARCH', 'idx:all', '@loc:[-{} -{} 1 km]'.format(max_val,max_val), 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx:all', '@loc:[{} {} 1 km]'.format(max_val,max_val), 'NOCONTENT').equal([0])
    
    for doc in range(doc_num, 0, -1):
        expected = [doc_num + 1 - doc]
        max_val = (doc - 1) * delta_size + arr_len
        for i in range(doc_num, doc -1, -1):
            expected.append('doc:{}'.format(i))
        res = conn.execute_command('FT.SEARCH', 'idx:all', '@loc:[-inf -{}]'.format(max_val), 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected), message = '[-inf -{}]'.format(max_val))

        res = conn.execute_command('FT.SEARCH', 'idx:all', '@loc:[{} +inf]'.format(max_val), 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected), message = '[{} +inf]'.format(max_val))

def testDebugDump(env):
    """ Test FT.DEBUG DUMP_INVIDX and NUMIDX_SUMMARY with multi numeric values """

    env.skipOnCluster()

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx:top', 'ON', 'JSON', 'SCHEMA', '$[*]', 'AS', 'val', 'GEO').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps([-1, 2, 3]))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps([-2, -1, 2]))

    env.expect('FT.DEBUG', 'DUMP_NUMIDX' ,'idx:top', 'val').equal([[1, 2]])
    env.expect('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx:top', 'val').equal(['numRanges', 1, 'numEntries', 6,
                                                                      'lastDocId', 2, 'revisionId', 0])

def testInvertedIndexMultipleBlocks(env):
    """ Test internal addition of new inverted index blocks (beyond INDEX_BLOCK_SIZE entries)"""

    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.arr', 'AS', 'arr', 'GEO', '$.arr2', 'AS', 'arr2', 'GEO').ok()
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
    res = conn.execute_command('FT.DEBUG', 'DUMP_NUMIDX' ,'idx', 'arr')
    env.assertListEqual(set(toSortedFlatList(res)), set(expected_ids), message='DUMP_NUMIDX')

    res = to_dict(conn.execute_command('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'arr'))
    env.assertEqual(res['numEntries'], doc_num * 2)
    env.assertEqual(res['lastDocId'], doc_num)

    # Should find the first and last overlap docs
    # e.g., for 200 docs:
    #   FT.SEARCH idx '@arr:[191 200]' NOCONTENT LIMIT 0 20
    res = conn.execute_command('FT.SEARCH', 'idx', '@arr:[{} {}]'.format(doc_num - overlap + 1, doc_num), 'NOCONTENT', 'LIMIT', '0', overlap * 2)
    expected_docs = ['doc:{}'.format(i) for i in chain(range(1, overlap + 1), range(doc_num - overlap + 1, doc_num + 1))]
    env.assertListEqual(toSortedFlatList(res[1:]),toSortedFlatList(expected_docs), message='FT.SEARCH')


def checkInfoAndGC(env, idx, doc_num, create, delete):
    """ Helper function for testInfoAndGC """
    conn = getConnectionByEnv(env)
    
    # Start empty
    env.assertEqual(True, True, message = 'check {}'.format(idx))
    info = index_info(env, idx)
    env.assertEqual(int(info['num_docs']), 0)
    env.assertEqual(int(info['total_inverted_index_blocks']), 0)
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

def testInfoAndGC(env):
    """ Test cleanup of numeric ranges """
    env.skipOnCluster()
    if env.env == 'existing-env':
        env.skip()
    conn = getConnectionByEnv(env)

    printSeed(env)

    env.expect('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()

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
    env.expect('FT.CREATE', 'idx_json_mult', 'ON', 'JSON', 'SCHEMA', '$.top[*]', 'AS', 'val', 'GEO').ok()
    checkInfoAndGC(env, 'idx_json_mult', doc_num, create_json_docs_multi, delete_json_docs)

    # JSON single
    env.flush()    
    env.expect('FT.CREATE', 'idx_json_single', 'ON', 'JSON', 'SCHEMA', '$.top', 'AS', 'val', 'GEO').ok()
    checkInfoAndGC(env, 'idx_json_single', doc_num, create_json_docs_single, delete_json_docs)

    # Hash
    env.flush()
    env.expect('FT.CREATE', 'idx_hash', 'ON', 'HASH', 'SCHEMA', 'top', 'GEO').ok()
    checkInfoAndGC(env, 'idx_hash', doc_num, create_hash_docs, delete_hash_docs)

def testSortBy(env):
    """ Test sort of multi numeric values """
    
    printSeed(env)

    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.top[*]', 'AS', 'val', 'GEO').ok()
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
        'NOCONTENT', 'LIMIT', 0, doc_num]
    
    # Results should be ascending
    res = conn.execute_command(*query, 'SORTBY', 'val')
    for i in range(2, len(res)):
        env.assertGreater(int(res[i]), int(res[i - 1]))

    # Results should be descending
    res = conn.execute_command(*query, 'SORTBY', 'val', 'DESC')    
    for i in range(2, len(res)):
        env.assertLess(int(res[i]), int(res[i - 1]))

def keep_dict_keys(dict, keys):
        return {k:v for k,v in dict.items() if k in keys}

def testInfoStats(env):
    """ Check that stats of single value are equivalent to multi value"""
    
    printSeed(env)
    conn = getConnectionByEnv(env)
    
    env.expect('FT.CREATE', 'idx:single', 'ON', 'JSON', 'PREFIX', 1, 'doc:single:', 'SCHEMA', '$.top', 'AS', 'val', 'GEO').ok()
    env.expect('FT.CREATE', 'idx:multi', 'ON', 'JSON', 'PREFIX', 1, 'doc:multi:', 'SCHEMA', '$.top', 'AS', 'val', 'GEO').ok()

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

def testInfoStatsAndSearchAsSingle(env):
    """ Check that search results and relevant stats are the same for single values and equivalent multi values """
    
    printSeed(env)

    conn = getConnectionByEnv(env)
    max_attr_num = 5
    schema_list = [['$.val{}'.format(i), 'AS', 'val{}'.format(i), 'GEO'] for i in range(1, max_attr_num + 1)]
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

def testConsecutiveValues(env):
    """ Test with many consecutive values which should cause range tree to do rebalancing (also for code coverage) """
    
    env.skipOnCluster()
    if env.env == 'existing-env':
        env.skip()
    
    conn = getConnectionByEnv(env)

    num_docs = 10000
    
    # Add values from -5000 to 5000
    # Add to the right, rebalance to the left
    i = -5000
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'GEO').ok()
    for doc in range(1, num_docs + 1):
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({'val': [i, i+1]}))
        i = i + 1
    
    env.expect('FT.SEARCH', 'idx', '@val:[-5000 -4999]', 'NOCONTENT').equal([2, 'doc:1', 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@val:[5 6]', 'NOCONTENT').equal([3, 'doc:5005', 'doc:5006', 'doc:5007'])
    env.expect('FT.SEARCH', 'idx', '@val:[4999 5000]', 'NOCONTENT').equal([2, 'doc:9999', 'doc:10000'])
    summary1 = env.execute_command('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'val')

    # Add values from 5000 to -5000
    # Add to the left, rebalance to the right
    env.flush()
    i = 5000
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'GEO').ok()
    for doc in range(1, num_docs + 1):
        conn.execute_command('JSON.SET', 'doc:{}'.format(doc), '$', json.dumps({'val': [i, i-1]}))
        i = i - 1
    
    env.expect('FT.SEARCH', 'idx', '@val:[4999 5000]', 'NOCONTENT').equal([2, 'doc:1', 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@val:[-6 -5]', 'NOCONTENT').equal([3, 'doc:5005', 'doc:5006', 'doc:5007'])
    env.expect('FT.SEARCH', 'idx', '@val:[-5000 -4999]', 'NOCONTENT').equal([2, 'doc:9999', 'doc:10000'])
    summary2 = env.execute_command('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'val')

    env.assertEqual(summary1, summary2)

def testDebugRangeTree(env):
    """ Test debug of range tree """

    env.skipOnCluster()
    if env.env == 'existing-env':
        env.skip()
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'GEO').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'val': [1, 2, 3]}))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps({'val': [1, 2, 3]}))
    conn.execute_command('JSON.SET', 'doc:3', '$', json.dumps({'val': [3, 4, 5]}))

    env.expect('FT.DEBUG', 'DUMP_NUMIDXTREE', 'idx', 'val').equal( ['numRanges', 1, 'numEntries', 9, 'lastDocId', 3, 'revisionId', 0, 'uniqueId', 0,
        'root', ['value', 0, 'maxDepth', 0,
            'range', ['minVal', 1, 'maxVal', 5, 'unique_sum', 0, 'invertedIndexSize', 11, 'card', 0, 'cardCheck', 1, 'splitCard', 16,
                'entries', ['numDocs', 3, 'lastId', 3, 'size', 1, 'values',
                    ['value', 1, 'docId', 1, 'value', 2, 'docId', 1, 'value', 3, 'docId', 1, 'value', 1, 'docId', 2, 'value', 2, 'docId', 2, 'value', 3, 'docId', 2, 'value', 3, 'docId', 3, 'value', 4, 'docId', 3, 'value', 5, 'docId', 3]]],
            'left', [], 'right', []]])

def checkUpdateNumRecords(env, is_json):
    """ Helper function for testing update of `num_records` """

    env.skipOnCluster()
    if env.env == 'existing-env':
        env.skip()
    conn = getConnectionByEnv(env)

    env.expect('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()

    if is_json:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'GEO').ok()
        conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'val': [1, 2, 3]}))
        conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps({'val': [1, 2, 3]}))
        conn.execute_command('JSON.SET', 'doc:3', '$', json.dumps({'val': [3, 4, 5]}))
    else:
        env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'val1', 'GEO', 'val2', 'GEO', 'val3', 'GEO').ok()
        conn.execute_command('HSET', 'doc:1', 'val1', 1, 'val2', 2, 'val3', 3)
        conn.execute_command('HSET', 'doc:2', 'val1', 1, 'val2', 2, 'val3', 3)
        conn.execute_command('HSET', 'doc:3', 'val1', 3, 'val2', 4, 'val3', 5)

    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], '9')
    
    # Update doc to have one value less
    if is_json:
        conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'val': [1, 2]}))
    else:
        conn.execute_command('HDEL', 'doc:1', 'val3')

    # INFO is not accurately updated before GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], '11')

    forceInvokeGC(env, 'idx')

    # Info is accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], '8')

    # Delete doc
    if is_json:
        conn.execute_command('JSON.DEL', 'doc:1', '$')
    else:
        conn.execute_command('DEL', 'doc:1')

    # INFO is not accurately updated before GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], '8')

    forceInvokeGC(env, 'idx')

    # Info is accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], '6')

    if is_json:
        conn.execute_command('JSON.DEL', 'doc:2', '$')
        conn.execute_command('JSON.DEL', 'doc:3', '$')
    else:
        conn.execute_command('DEL', 'doc:2')
        conn.execute_command('DEL', 'doc:3')

    # Info is not accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], '6')

    forceInvokeGC(env, 'idx')

    # Info is accurately updated after GC
    info = index_info(env, 'idx')
    env.assertEqual(info['num_records'], '0')

def testUpdateNumRecordsJson(env):
    """ Test update of `num_records` when using JSON """
    checkUpdateNumRecords(env, True)

def testUpdateNumRecordsHash(env):
    """ Test update of `num_records` when using Hashes """
    checkUpdateNumRecords(env, False)