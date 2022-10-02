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


