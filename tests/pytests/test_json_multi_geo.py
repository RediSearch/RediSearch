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


def checkInfo(env, idx, num_docs, inverted_sz_mb):
    """ Helper function for testInfoAndGC """
    conn = getConnectionByEnv(env)

    # Start empty
    env.assertEqual(True, True, message = 'check {}'.format(idx))
    info = index_info(env, idx)
    env.assertEqual(int(info['num_docs']), num_docs)
    env.assertEqual(float(info['inverted_sz_mb']), inverted_sz_mb)

@skip(cluster=True)
def testBasic(env):
    """ Test multi GEO values (an array of GEO values or multiple GEO values) """

    conn = getConnectionByEnv(env)

    conn.execute_command(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0)

    env.expect('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$..loc[*]', 'AS', 'loc', 'GEO').ok()
    env.expect('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA',
        '$[0].nested2[0].loc', 'AS', 'loc', 'GEO').ok()     # ["1.2,1.3", "1.4,1.5", "2,2"]
    env.expect('FT.CREATE', 'idx3', 'ON', 'JSON', 'SCHEMA',
        '$[1].nested2[2].loc', 'AS', 'loc', 'GEO').ok()     # ["42,64", "-50,-72", "-100,-20", "43.422649,11.126973", "29.497825,-82.141870"]

    env.expect('FT.CREATE', 'idx4', 'ON', 'JSON', 'SCHEMA',
        '$[0].nested2[0].loc', 'AS', 'loc1', 'GEO',         # ["1.2,1.3", "1.4,1.5", "2,2"]
        '$[1].nested2[2].loc', 'AS', 'loc2', 'GEO').ok()    # ["42,64", "-50,-72", "-100,-20", "43.422649,11.126973", "29.497825,-82.141870"]

    # check stats for an empty index
    checkInfo(env, 'idx1', 0, 0)

    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))

    # check stats after insert

    # idx1 contains 24 entries, expected size of inverted index = 407
    # the size is distributed in the left and right children ranges as follows:

    # left range size = 303
    #     Size of NewInvertedIndex() structure = 96
    #         sizeof_InvertedIndex(Index_StoreNumeric) = 48
    #         sizeof(IndexBlock) = 48
    #     Buffer grows up to 207 bytes trying to store 23 entries 8 bytes each.
    #     See Buffer_Grow() in inverted_index.c

    # right range size = 104:
    #     Size of NewInvertedIndex() structure = 96
    #         sizeof_InvertedIndex(Index_StoreNumeric) = 48
    #         sizeof(IndexBlock) = 48
    #     Buffer grows up to 8 bytes trying to store 1 entry 8 bytes each = 8
    checkInfo(env, 'idx1', 1, 407 / (1024 * 1024))

    # Expected size of inverted index for idx2 = 96 + 25 = 121
    #     Size of NewInvertedIndex() structure = 96
    #     Buffer grows up to 25 bytes trying to store 3 entries 8 bytes each = 25
    checkInfo(env, 'idx2', 1, 121 / (1024 * 1024))

    # Expected size of inverted index for idx2 = 96 + 46 = 142
    #     Size of NewInvertedIndex() structure = 96
    #     Buffer grows up to 46 bytes trying to store 5 entries, 8 bytes each = 46
    checkInfo(env, 'idx3', 1, 142 / (1024 * 1024))

    # idx4 contains two GEO fields, the expected size of inverted index is
    # equivalent to the sum of the size of idx2 and idx3 = 121 + 142 = 263
    checkInfo(env, 'idx4', 1, 263 / (1024 * 1024))

    # Geo range and Not
    env.expect('FT.SEARCH', 'idx1', '@loc:[1.2 1.1 40 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx1', '-@loc:[1.2 1.1 40 km]', 'NOCONTENT').equal([0])


    env.expect('FT.SEARCH', 'idx1', '@loc:[0 0 +inf km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx1', '-@loc:[0 0 +inf km]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx2', '@loc:[1.42 1.52 5 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '-@loc:[1.42 1.52 5 km]', 'NOCONTENT').equal([0])

    env.expect('FT.SEARCH', 'idx3', '@loc:[42 64 200 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx3', '-@loc:[42 64 200 km]', 'NOCONTENT').equal([0])

    # Intersect
    env.expect('FT.SEARCH', 'idx4', '@loc1:[1.2 1.1 40 km] @loc2:[29.5 -82 20 km]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx4', '@loc1:[1.2 1.1 40 km] @loc2:[50 50 1 km]', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx4', '@loc1:[-10 -10 1 km]     @loc2:[29.5 -82 20 km]', 'NOCONTENT').equal([0])

    # check stats after deletion
    conn.execute_command('DEL', 'doc:1')
    forceInvokeGC(env, 'idx1')
    checkInfo(env, 'idx1', 0, 0)


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

@skip(cluster=True)
def testDebugDump(env):
    """ Test FT.DEBUG DUMP_INVIDX and NUMIDX_SUMMARY with multi GEO values """

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx:top', 'ON', 'JSON', 'SCHEMA', '$[*]', 'AS', 'val', 'GEO').ok()
    env.expect('JSON.SET', 'doc:1', '$', json.dumps(["21.2,21.3", "21.4,21.5", "22,22"])).ok()
    env.expect('JSON.SET', 'doc:2', '$', json.dumps(["1.2,1.3", "1.4,1.5", "2,2"])).ok()

    env.expect(debug_cmd(), 'DUMP_NUMIDX' ,'idx:top', 'val').equal([[1, 2]])
    env.expect(debug_cmd(), 'NUMIDX_SUMMARY', 'idx:top', 'val').equal(['numRanges', 1, 'numEntries', 6,
                                                                      'lastDocId', 2, 'revisionId', 0,
                                                                      'emptyLeaves', 0, 'RootMaxDepth', 0])

def checkMultiGeoReturn(env, expected, default_dialect, is_sortable):
    """ Helper function for RETURN with multiple GEO values """

    conn = getConnectionByEnv(env)

    dialect_param = ['DIALECT', 3] if not default_dialect else []
    sortable_param = ['SORTABLE'] if is_sortable else []
    env.assertEqual(len(expected), 3, message='dialect {}, sortable {}'.format(dialect_param, is_sortable))

    env.expect('FT.CREATE', 'idx_flat', 'ON', 'JSON', 'SCHEMA', '$.arr[*]', 'AS', 'val', 'GEO', *sortable_param).ok()
    env.expect('FT.CREATE', 'idx_arr', 'ON', 'JSON', 'SCHEMA', '$.arr', 'AS', 'val', 'GEO', *sortable_param).ok()
    doc1_content = {"arr":["40.6,70.35", "29.7,34.9", "21,22"]}
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))

    expr = '@val:[29.7 34.8 15 km]'

    # Multi flat
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '3', '$.arr[1]', 'AS', 'arr_1', *dialect_param).equal(expected[0])
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '1', 'val', *dialect_param).equal(expected[1])
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '3', '$.arr[*]', 'AS', 'val', *dialect_param).equal(expected[1])
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '3', '$.arr', 'AS', 'val', *dialect_param).equal(expected[2])

    env.expect('FT.AGGREGATE', 'idx_flat',
               expr, 'LOAD', '1', '@val', *dialect_param).equal([1, ['val', expected[1][2][1]]])

    env.expect('FT.AGGREGATE', 'idx_flat',
               expr, 'GROUPBY', '1', '@val', *dialect_param).equal([1, ['val', expected[1][2][1]]])

    # Array
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '3', '$.arr[1]', 'AS', 'arr_1', *dialect_param).equal(expected[0])
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '1', 'val', *dialect_param).equal(expected[2])
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '3', '$.arr[*]', 'AS', 'val', *dialect_param).equal(expected[1])
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '3', '$.arr', 'AS', 'val', *dialect_param).equal(expected[2])

    res = conn.execute_command('FT.AGGREGATE', 'idx_arr',
        expr, 'GROUPBY', '1', '@val', *dialect_param)
    # Ignore the result with older dialect
    #  Schema attribute with path to an array was not supported (lead to indexing failure)
    if not default_dialect:
        env.assertEqual(res, [1, ['val', expected[2][2][1]]])


    env.expect('FT.AGGREGATE', 'idx_arr',
               expr, 'LOAD', '1', '@val', *dialect_param).equal([1, ['val', expected[2][2][1]]])

    # RETURN ALL
    res = conn.execute_command('FT.SEARCH', 'idx_flat', expr, *dialect_param)
    env.assertEqual(json.loads(res[2][1]), [doc1_content] if not default_dialect else doc1_content)


def testMultiGeoReturn(env):
    """ test RETURN with multiple GEO values """

    res1 = [1, 'doc:1', ['arr_1', '["29.7,34.9"]']]
    res2 = [1, 'doc:1', ['val', '["40.6,70.35","29.7,34.9","21,22"]']]
    res3 = [1, 'doc:1', ['val', '[["40.6,70.35","29.7,34.9","21,22"]]']]

    checkMultiGeoReturn(env, [res1, res2, res3], False, False)
    env.flush()
    checkMultiGeoReturn(env, [res1, res2, res3], False, True)

def testMultiGeoReturnBWC(env):
    """ test backward compatibility of RETURN with multiple GEO values """
    res1 = [1, 'doc:1', ['arr_1', '29.7,34.9']]
    res2 = [1, 'doc:1', ['val', '40.6,70.35']]
    res3 = [1, 'doc:1', ['val', '["40.6,70.35","29.7,34.9","21,22"]']]

    checkMultiGeoReturn(env, [res1, res2, res3], True, False)
    env.flush()
    checkMultiGeoReturn(env, [res1, res2, res3], True, True)
