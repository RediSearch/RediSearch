# -*- coding: utf-8 -*-

from includes import *
from common import *
from RLTest import Env

# search result processors
search_result_processors = [
    ['Type', 'Index', 'Results processed', ANY],
    ['Type', 'Scorer', 'Results processed', ANY],
    ['Type', 'Sorter', 'Results processed', ANY],
    ['Type', 'Loader', 'Results processed', ANY]
]

# This is common for all test with `SEARCH hello`
expected_shard_standalone_profile = [[
    'SEARCH',
    [
        'Warning',
        ['None'],
        'Iterators profile',
        [
            'Type', 'TEXT',
            'Term', 'hello',
            'Number of reading operations', 1,
            'Estimated number of matches', 1
        ],
        'Result processors profile',
        search_result_processors
    ],
    'VSIM',
    [
        'Warning',
        ['None'],
        'Iterators profile',
        [
            'Type', 'VECTOR',
            'Number of reading operations', 4,
            'Vector search mode', 'STANDARD_KNN'
        ],
        'Result processors profile',
        [
            ['Type', 'Index', 'Results processed', ANY],
            ['Type', 'Metrics Applier', 'Results processed', ANY],
            ['Type', 'Vector Normalizer', 'Results processed', ANY],
            ['Type', 'Loader', 'Results processed', ANY]
        ]
    ]
]]

query_and_profile = [
    # Tuple items:
    # Query:
    #   - query,
    # Standalone expected profile:
    #   - expected_shard_standalone_profile,
    #   - expected_coordinator_standalone_profile
    # Cluster expected profile:
    #   - expected_shard_cluster_profile,
    #   - expected_coordinator_cluster_profile

    # Test: Minimal hybrid query
    (
        ['FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
         'SEARCH', 'hello',
         'VSIM', '@v', '$blob',
         'PARAMS', 2, 'blob', 'aaaaaaaa'
        ],
        # expected_shard_standalone_profile
        expected_shard_standalone_profile,
        # expected_coordinator_standalone_profile
        [
            'Warning', ['None'],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ],
        # expected_shard_cluster_profile
        [
            'Shard ID', ANY,
            'SEARCH',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                {
                    # Each shard can have different iterator profile, so we
                    # check that one of the following values is in the profile
                    'Valid Values':
                    [
                        ['Type', 'EMPTY', 'Number of reading operations', 0],
                        [
                            'Type', 'TEXT',
                            'Term', 'hello',
                            'Number of reading operations', 1,
                            'Estimated number of matches', 1
                        ]
                    ]
                },
                'Result processors profile',
                search_result_processors
            ],
            'VSIM',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                [
                    'Type', 'VECTOR',
                    'Number of reading operations', ANY,
                    'Vector search mode', 'STANDARD_KNN'
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Metrics Applier', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Vector Normalizer', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY]
                ]
            ]
        ],
        # expected_coordinator_cluster_profile
        [
            'Shard ID', ANY,
            'Warning', ['None'],
            'Subqueries result processors profile',
            [
                'SEARCH',
                [
                    ['Type', 'Network', 'Results processed', 1],
                    ['Type', 'Sorter', 'Results processed', 1],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ]
            ],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                # Sorter is added by default, to sort by score.
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ]
    ),
    # Test: Hybrid query with LOAD * and NOSORT
    (
        ['FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
         'SEARCH', 'hello',
         'VSIM', '@v', '$blob',
         'PARAMS', 2, 'blob', 'aaaaaaaa',
         'LOAD', '*', 'NOSORT'
        ],
        # expected_shard_standalone_profile
        expected_shard_standalone_profile,
        # expected_coordinator_standalone_profile
        [
            'Warning', ['None'],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
            ]
        ],
        # expected_shard_cluster_profile
        [
            'Shard ID', ANY,
            'SEARCH',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                 {
                     # Each shard can have different iterator profile, so we
                    # check that one of the following values is in the profile
                    'Valid Values':
                    [
                        ['Type', 'EMPTY', 'Number of reading operations', 0],
                        [
                            'Type', 'TEXT',
                            'Term', 'hello',
                            'Number of reading operations', 1,
                            'Estimated number of matches', 1
                        ]
                    ]
                },
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Sorter', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                ]
            ],
            'VSIM',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                [
                    'Type', 'VECTOR',
                    'Number of reading operations', ANY,
                    'Vector search mode', 'STANDARD_KNN'
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Metrics Applier', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Vector Normalizer', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                    # Additional loader for LOAD *
                    ['Type', 'Loader', 'Results processed', ANY]
                ]
            ]
        ],
        # expected_coordinator_cluster_profile
        [
            'Shard ID', ANY,
            'Warning', ['None'],
            'Subqueries result processors profile',
            [
                'SEARCH',
                [
                    ['Type', 'Network', 'Results processed', 1],
                    ['Type', 'Sorter', 'Results processed', 1],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ]
            ],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4]
                # No sorter because of NOSORT
            ]
        ]
    ),
    # Test: Hybrid query with LIMIT
    (
        ['FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
         'SEARCH', 'hello',
         'VSIM', '@v', '$blob',
         'PARAMS', 2, 'blob', 'aaaaaaaa',
         'LIMIT', 0, 2
        ],
        # expected_shard_standalone_profile
        expected_shard_standalone_profile,
        # expected_coordinator_standalone_profile.
        [
            'Warning', ['None'],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                # The limit is applied by the sorter.
                ['Type', 'Sorter', 'Results processed', 2]
            ]
        ],
        # expected_shard_cluster_profile
        [
            'Shard ID', ANY,
            'SEARCH',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                 {
                     # Each shard can have different iterator profile, so we
                    # check that one of the following values is in the profile
                    'Valid Values':
                    [
                        ['Type', 'EMPTY', 'Number of reading operations', 0],
                        [
                            'Type', 'TEXT',
                            'Term', 'hello',
                            'Number of reading operations', 1,
                            'Estimated number of matches', 1
                        ]
                    ]
                },
                'Result processors profile',
                search_result_processors
            ],
            'VSIM',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                [
                    'Type', 'VECTOR',
                    'Number of reading operations', ANY,
                    'Vector search mode', 'STANDARD_KNN'
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Metrics Applier', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Vector Normalizer', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY]
                ]
            ]
        ],
        # expected_coordinator_cluster_profile
        [
            'Shard ID', ANY,
            'Warning', ['None'],
            'Subqueries result processors profile',
            [
                'SEARCH',
                [
                    ['Type', 'Network', 'Results processed', 1],
                    ['Type', 'Sorter', 'Results processed', 1],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ]
            ],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 2]
            ]
        ]
    ),
    # Test: Hybrid query with GROUPBY
    (
        ['FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
         'SEARCH', 'hello',
         'VSIM', '@v', '$blob',
         'GROUPBY', 1, '@t',
         'REDUCE', 'COUNT', 0, 'AS', 'count',
         'PARAMS', 2, 'blob', 'aaaaaaaa',
        ],
        # expected_shard_standalone_profile
        expected_shard_standalone_profile,
        # expected_coordinator_standalone_profile.
        [
            'Warning', ['None'],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Loader', 'Results processed', 4],
                ['Type', 'Grouper', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ],
        # expected_shard_cluster_profile
        [
            'Shard ID', ANY,
            'SEARCH',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                ANY,
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Sorter', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                ]
            ],
            'VSIM',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                [
                    'Type', 'VECTOR',
                    'Number of reading operations', ANY,
                    'Vector search mode', 'STANDARD_KNN'
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Metrics Applier', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Vector Normalizer', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                ]
            ]
        ],
        # expected_coordinator_cluster_profile
        [
            'Shard ID', ANY,
            'Warning', ['None'],
            'Subqueries result processors profile',
            [
                'SEARCH',
                [
                    ['Type', 'Network', 'Results processed', 1],
                    ['Type', 'Sorter', 'Results processed', 1],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ]
            ],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Grouper', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ]
    ),
    # Test: Hybrid query with wildcard query and fuzzy (without LIMITED)
    (
        ['FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
         'SEARCH', '%hell% hel*',
         'VSIM', '@v', '$blob',
         'PARAMS', 2, 'blob', 'aaaaaaaa',
        ],
        # expected_shard_standalone_profile
        [[
            'SEARCH',
            [
                'Warning',
                ['None'],
                'Iterators profile',
                [
                    'Type', 'INTERSECT', 'Number of reading operations', 2,
                    'Child iterators',
                    [
                        [
                            'Type', 'UNION', 'Query type', 'FUZZY - hell',
                            'Number of reading operations', 2,
                            'Child iterators',
                            [
                                ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                                ['Type', 'TEXT', 'Term', 'help', 'Number of reading operations', 1, 'Estimated number of matches', 1]
                            ]
                        ],
                        [
                            'Type', 'UNION', 'Query type', 'PREFIX - hel',
                            'Number of reading operations', 2,
                            'Child iterators',
                            [
                                ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                                ['Type', 'TEXT', 'Term', 'help', 'Number of reading operations', 1, 'Estimated number of matches', 1]
                            ]
                        ]
                    ]
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', 2],
                    ['Type', 'Scorer', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Loader', 'Results processed', 2]
                ]
            ],
            'VSIM',
            [
                'Warning',
                ['None'],
                'Iterators profile',
                ['Type', 'VECTOR', 'Number of reading operations', 4, 'Vector search mode', 'STANDARD_KNN'],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', 4],
                    ['Type', 'Metrics Applier', 'Results processed', 4],
                    ['Type', 'Vector Normalizer', 'Results processed', 4],
                    ['Type', 'Loader', 'Results processed', 4]
                ]
            ]
        ]],
        # expected_coordinator_standalone_profile.
        [
            'Warning', ['None'],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ],
        # expected_shard_cluster_profile
        [
            'Shard ID', ANY,
            'SEARCH',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                {
                    # Each shard can have different iterator profile, so we
                    # check that one of the following values is in the profile
                    'Valid Values':
                    [
                        ['Type', 'EMPTY', 'Number of reading operations', 0],
                        [
                            'Type', 'INTERSECT',
                            'Number of reading operations', 2,
                            'Child iterators',
                            [
                                [
                                    'Type', 'UNION',
                                    'Query type', 'FUZZY - hell',
                                    'Number of reading operations', 2,
                                    'Child iterators',
                                    [
                                        ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                                        ['Type', 'TEXT', 'Term', 'help', 'Number of reading operations', 1, 'Estimated number of matches', 1]
                                    ]
                                ],
                                [
                                    'Type', 'UNION',
                                    'Query type', 'PREFIX - hel',
                                    'Number of reading operations', 2,
                                    'Child iterators',
                                    [
                                        ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                                        ['Type', 'TEXT', 'Term', 'help', 'Number of reading operations', 1, 'Estimated number of matches', 1]
                                    ]
                                ]
                            ]
                        ],
                    ],
                },
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Sorter', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                ]
            ],
            'VSIM',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                [
                    'Type', 'VECTOR',
                    'Number of reading operations', ANY,
                    'Vector search mode', 'STANDARD_KNN'
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Metrics Applier', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Vector Normalizer', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                ]
            ]
        ],
        # expected_coordinator_cluster_profile
        [
            'Shard ID', ANY,
            'Warning', ['None'],
            'Subqueries result processors profile',
            [
                'SEARCH',
                [
                    ['Type', 'Network', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY],
                ]
            ],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ]
    ),
    # Test: Hybrid query with wildcard query and fuzzy (LIMITED)
    (
        ['FT.PROFILE', 'idx', 'HYBRID', 'LIMITED', 'QUERY',
         'SEARCH', '%hell% hel*',
         'VSIM', '@v', '$blob',
         'PARAMS', 2, 'blob', 'aaaaaaaa',
        ],
        # expected_shard_standalone_profile
        [[
            'SEARCH',
            [
                'Warning',
                ['None'],
                'Iterators profile',
                [
                    'Type', 'INTERSECT', 'Number of reading operations', 2,
                    'Child iterators',
                    [
                        [
                            'Type', 'UNION', 'Query type', 'FUZZY - hell',
                            'Number of reading operations', 2,
                            'Child iterators',
                            'The number of iterators in the union is 2'
                        ],
                        [
                            'Type', 'UNION', 'Query type', 'PREFIX - hel',
                            'Number of reading operations', 2,
                            'Child iterators',
                            'The number of iterators in the union is 2'
                        ]
                    ]
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', 2],
                    ['Type', 'Scorer', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Loader', 'Results processed', 2]
                ]
            ],
            'VSIM',
            [
                'Warning',
                ['None'],
                'Iterators profile',
                ['Type', 'VECTOR', 'Number of reading operations', 4, 'Vector search mode', 'STANDARD_KNN'],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', 4],
                    ['Type', 'Metrics Applier', 'Results processed', 4],
                    ['Type', 'Vector Normalizer', 'Results processed', 4],
                    ['Type', 'Loader', 'Results processed', 4]
                ]
            ]
        ]],
        # expected_coordinator_standalone_profile.
        [
            'Warning', ['None'],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ],
        # expected_shard_cluster_profile
        [
            'Shard ID', ANY,
            'SEARCH',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                {
                    # Each shard can have different iterator profile, so we
                    # check that one of the following values is in the profile
                    'Valid Values':
                    [
                        ['Type', 'EMPTY', 'Number of reading operations', 0],
                        [
                            'Type', 'INTERSECT', 'Number of reading operations', 2,
                            'Child iterators',
                            [
                                [
                                    'Type', 'UNION',
                                    'Query type', 'FUZZY - hell',
                                    'Number of reading operations', 2,
                                    'Child iterators',
                                    'The number of iterators in the union is 2'
                                ],
                                [
                                    'Type', 'UNION',
                                    'Query type', 'PREFIX - hel',
                                    'Number of reading operations', 2,
                                    'Child iterators',
                                    'The number of iterators in the union is 2'
                                ]
                            ]
                        ],
                    ]
                },
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Sorter', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                ]
            ],
            'VSIM',
            [
                'Warning', ['None'],
                'Internal cursor reads', 1,
                'Iterators profile',
                [
                    'Type', 'VECTOR',
                    'Number of reading operations', ANY,
                    'Vector search mode', 'STANDARD_KNN'
                ],
                'Result processors profile',
                [
                    ['Type', 'Index', 'Results processed', ANY],
                    ['Type', 'Metrics Applier', 'Results processed', ANY],
                    ['Type', 'Scorer', 'Results processed', ANY],
                    ['Type', 'Vector Normalizer', 'Results processed', ANY],
                    ['Type', 'Loader', 'Results processed', ANY],
                ]
            ]
        ],
        # expected_coordinator_cluster_profile
        [
            'Shard ID', ANY,
            'Warning', ['None'],
            'Subqueries result processors profile',
            [
                'SEARCH',
                [
                    ['Type', 'Network', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', ANY],
                ]
            ],
            'Result processors profile',
            [
                ['Type', 'Hybrid Merger', 'Results processed', 4],
                ['Type', 'Sorter', 'Results processed', 4]
            ]
        ]
    )
]


def _setup_index_and_data(env):
    # Create index with both text and vector fields
    env.expect('FT.CREATE idx SCHEMA t TEXT v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('hset', '1', 't', 'hello world', 'v', 'bababaca')
    conn.execute_command('hset', '2', 't', 'help space', 'v', 'babababa')
    conn.execute_command('hset', '3', 't', 'world space', 'v', 'aabbaabb')
    conn.execute_command('hset', '4', 't', 'other text', 'v', 'bbaabbaa')

def _verify_profile_structure(env, protocol, actual_res):
    if protocol == 2:
        # Verify the response structure
        env.assertTrue(isinstance(actual_res, list))
        # Should have 9 elements
        env.assertEqual(len(actual_res), 9)

        # Verify the flat list structure:
        # ['total_results', value,
        #  'results', value,
        #  'warnings', value,
        #  'execution_time',
        #  value, profile_data]
        env.assertEqual(actual_res[0], 'total_results')
        env.assertTrue(isinstance(actual_res[1], int))
        env.assertEqual(actual_res[2], 'results')
        env.assertTrue(isinstance(actual_res[3], list))
        env.assertEqual(actual_res[4], 'warnings')
        env.assertTrue(isinstance(actual_res[5], list))
        env.assertEqual(actual_res[6], 'execution_time')
        env.assertTrue(isinstance(actual_res[7], str))
        # Verify profile data structure
        profile_data = actual_res[8]
        env.assertTrue(isinstance(profile_data, list))
        env.assertEqual(len(profile_data), 4)
        # Should have ['Shards', [...], 'Coordinator', [...]] structure
        env.assertEqual(profile_data[0], 'Shards')
        env.assertEqual(profile_data[2], 'Coordinator')
    else:
        # TODO: verify RESP3 structure
        pass

@skip(cluster=True)
def test_profile_standalone():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 _PRINT_PROFILE_CLOCK false')
    _setup_index_and_data(env)
    for query, expected_shard_profile, expected_coordinator_profile, _, _ in query_and_profile:
        actual_res = env.execute_command(*query)
        _verify_profile_structure(env, env.protocol, actual_res)
        env.assertEqual(actual_res[8][1], expected_shard_profile,
                        message=f'query: {query}')
        env.assertEqual(actual_res[8][3], expected_coordinator_profile,
                        message=f'query: {query}')

@skip(cluster=False)
def test_profile_cluster():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 _PRINT_PROFILE_CLOCK false')
    _setup_index_and_data(env)
    for query, _, _, expected_shard_profile, expected_coordinator_profile in query_and_profile:
        actual_res = env.execute_command(*query)
        _verify_profile_structure(env, env.protocol, actual_res)

        shard_profiles = actual_res[8][1]
        for i in range(len(shard_profiles)):
            shard_profile = shard_profiles[i]
            # Shard profile is a list of 6 items
            env.assertEqual(len(shard_profile), 6)
            env.assertEqual(len(expected_shard_profile), 6)

            # Verify the profile data structure
            # ['Shard ID', ANY, 'SEARCH', [...], 'VSIM', [...]]
            env.assertEqual(shard_profile[0], expected_shard_profile[0])
            env.assertEqual(shard_profile[2], expected_shard_profile[2])
            env.assertEqual(shard_profile[4], expected_shard_profile[4])

            # Verify the SEARCH and VSIM profile data
            for k in [3, 5]:
                err_message = 'SEARCH' if k == 3 else 'VSIM'
                for i in range(len(expected_shard_profile[k])):
                    expected_value = expected_shard_profile[k][i]
                    # skip if the expected value is ANY
                    if expected_value is ANY:
                        continue

                    # If the expected value is a dict, check that the current
                    # value is in the valid values.
                    # This is used for the iterator profile, where each shard
                    # can have different iterator profile.
                    if isinstance(expected_value, dict):
                        valid_values = expected_value['Valid Values']
                        env.assertIn(shard_profile[k][i], valid_values,
                                    message=f'{err_message} query: {query}')
                        continue

                    env.assertEqual(shard_profile[k][i],
                                    expected_value,
                                    message=f'{err_message} query: {query}')

        # Verify the coordinator profile data
        coordinator_profile = actual_res[8][3]
        env.assertEqual(coordinator_profile, expected_coordinator_profile,
                        message=f'COORDINATOR query: {query}')

def test_profile_time():
    # Test that the time is greater or equal to 0 for all timings in the profile
    env = Env(moduleArgs='DEFAULT_DIALECT 2 _PRINT_PROFILE_CLOCK true', protocol=3)
    _setup_index_and_data(env)
    for query, _, _, _, _ in query_and_profile:
        actual_res = env.execute_command(*query)
        # Verify that the time is greater or equal to 0
        env.assertGreaterEqual(actual_res['execution_time'], 0)
        for shard in actual_res['Profile']['Shards']:
            for subquery in ['SEARCH', 'VSIM']:
                env.assertGreater(
                    shard[subquery]['Total profile time'], 0)
                env.assertGreaterEqual(
                    shard[subquery]['Parsing time'], 0)
                env.assertGreaterEqual(
                    shard[subquery]['Pipeline creation time'], 0)
                if CLUSTER:
                    env.assertGreaterEqual(
                        shard[subquery]['Coordinator dispatch time [ms]'], 0)
                env.assertGreaterEqual(
                    shard[subquery]['Iterators profile']['Time'], 0)
                for processor in shard[subquery]['Result processors profile']:
                    env.assertGreaterEqual(processor['Time'], 0)

        # Verify the coordinator profile data
        coordinator = actual_res['Profile']['Coordinator']
        # Verify the subqueries profile data
        if CLUSTER:
            for subquery in ['SEARCH', 'VSIM']:
                for processor in coordinator['Subqueries result processors profile'][subquery]:
                    env.assertGreaterEqual(processor['Time'], 0)
        # Verify the coordinator result processors profile
        for processor in coordinator['Result processors profile']:
            if processor['Type'] == 'Threadsafe-Depleter':
                env.assertGreaterEqual(processor['Depletion time'], 0)
            env.assertGreaterEqual(processor['Time'], 0)

def test_profile_errors():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    _setup_index_and_data(env)

    # Invalid number of arguments
    env.expect('FT.PROFILE').error()\
        .contains("wrong number of arguments for 'FT.PROFILE' command")
    env.expect('FT.PROFILE idx').error()\
        .contains("wrong number of arguments for 'FT.PROFILE' command")
    # Missing QUERY
    env.expect(
        'FT.PROFILE', 'idx', 'HYBRID',
        'SEARCH', 'world',
        'VSIM', '@v', '$blob',
        'PARAMS', 2, 'blob', 'aaaaaaaa').error()\
            .contains('The QUERY keyword is expected')
    # Missing HYBRID
    env.expect(
        'FT.PROFILE', 'idx', 'QUERY',
        'SEARCH', 'world',
        'VSIM', '@v', '$blob',
        'PARAMS', 2, 'blob', 'aaaaaaaa').error()\
            .contains('No `SEARCH`, `AGGREGATE`, or `HYBRID` provided')
    # Missing SEARCH
    env.expect(
        'FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
        'VSIM', '@v', '$blob',
        'PARAMS', 2, 'blob', 'aaaaaaaa').error()\
            .contains('Invalid subqueries count: expected an unsigned integer')
    # Missing VSIM
    env.expect(
        'FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
        'SEARCH', '$blob',
        'PARAMS', 2, 'blob', 'aaaaaaaa').error()\
            .contains('Unknown argument `PARAMS` in SEARCH')
    # Missing PARAMS
    env.expect(
        'FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
        'SEARCH', 'world',
        'VSIM', '@v', '$blob').error()\
            .contains('No such parameter `blob`')
    # Invalid vector argument
    env.expect(
        'FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
        'SEARCH', 'world',
        'VSIM', '@v', 'aaaaaaaa').error()\
            .contains('Invalid vector argument, expected a parameter name starting with $')
