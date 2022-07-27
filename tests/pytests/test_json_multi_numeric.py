import json

from common import *
from includes import *

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
                "seq": [42, 64, -1, 10E+20, -10.0e-20]
            }
        ]
    }
]




def testNumeric(env):
    """ Test multi numeric values (an array of numeric values or multiple numeric values) """

    conn = getConnectionByEnv(env)
    # TODO: FIXME: Remove debug config
    env.expect('FT.CONFIG', 'SET', 'TIMEOUT', 0).ok()

    #env.expect('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$..seq[*]', 'AS', 'seq', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$[0].seq[0]', 'AS', 'seq', 'NUMERIC').ok()

    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))
    

    res1 = [1, 'doc:1']
    env.expect('FT.SEARCH', 'idx1', '@seq:[3 6]', 'NOCONTENT').equal(res1)


