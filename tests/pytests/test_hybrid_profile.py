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

# vsim result processors
vsim_result_processors = [
    ['Type', 'Index', 'Results processed', ANY],
    ['Type', 'Metrics Applier', 'Results processed', ANY],
    ['Type', 'Vector Normalizer', 'Results processed', ANY],
    ['Type', 'Loader', 'Results processed', ANY]
]

# This is common for all test queries
expected_shard_standalone_profile = [[
    'SEARCH',
    [
        'Warning',
        ['None'],
        'Iterators profile',
        [
            'Type', 'TEXT',
            'Term', 'hello',
            'Number of reading operations', 2,
            'Estimated number of matches', 2
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
        vsim_result_processors
    ]
]]

query_and_profile = [
    # Tuple items:
    # - query,
    # - expected_shard_standalone_profile,
    # - expected_coordinator_standalone_profile
    # - expected_shard_cluster_profile,
    # - expected_coordinator_cluster_profile

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
                ANY,
                # Iterators profile can be one of:
                # ['Type', 'EMPTY', 'Number of reading operations', 0],
                # ['Type', 'TEXT', 'Term', 'hello',
                #  'Number of reading operations', 2,
                #  'Estimated number of matches', 2],
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
                    ['Type', 'Network', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 3]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 5]
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
                    ['Type', 'Network', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 4]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 5]
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
                ANY,
                # Iterators profile can be one of:
                # ['Type', 'EMPTY', 'Number of reading operations', 0],
                # ['Type', 'TEXT', 'Term', 'hello',
                #  'Number of reading operations', 2,
                #  'Estimated number of matches', 2],
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
                    ['Type', 'Network', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 4]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 5]
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
                    ['Type', 'Network', 'Results processed', 2],
                    ['Type', 'Sorter', 'Results processed', 2],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 4]
                ],
                'VSIM',
                [
                    ['Type', 'Network', 'Results processed', 4],
                    ['Type', 'Sorter', 'Results processed', 4],
                    ['Type', 'Threadsafe-Depleter', 'Results processed', 5]
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
]


class testHybridProfile:
    def __init__(self):
        self.env = Env(moduleArgs='DEFAULT_DIALECT 2 _PRINT_PROFILE_CLOCK false')
        self._setup_index_and_data()

    def _setup_index_and_data(self):
        # Create index with both text and vector fields
        self.env.expect('FT.CREATE idx SCHEMA t TEXT v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok()
        conn = getConnectionByEnv(self.env)
        conn.execute_command('hset', '1', 't', 'hello world', 'v', 'bababaca')
        conn.execute_command('hset', '2', 't', 'hello space', 'v', 'babababa')
        conn.execute_command('hset', '3', 't', 'world space', 'v', 'aabbaabb')
        conn.execute_command('hset', '4', 't', 'other text', 'v', 'bbaabbaa')

    def _verify_profile_structure(self, protocol, actual_res):
       env = self.env
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

    def test_profile_standalone(self):
        if CLUSTER:
            raise SkipTest()
        env = self.env
        for query, expected_shard_profile, expected_coordinator_profile, _, _ in query_and_profile:
            actual_res = env.execute_command(*query)
            self._verify_profile_structure(env.protocol, actual_res)
            env.assertEqual(actual_res[8][1], expected_shard_profile,
                            message=f'query: {query}')
            env.assertEqual(actual_res[8][3], expected_coordinator_profile,
                            message=f'query: {query}')

    def test_profile_cluster(self):
        if not CLUSTER:
            raise SkipTest()
        env = self.env
        for query, _, _, expected_shard_profile, expected_coordinator_profile in query_and_profile:
            actual_res = env.execute_command(*query)
            self._verify_profile_structure(env.protocol, actual_res)

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
                        # skip if the expected value is ANY
                        if expected_shard_profile[k][i] is ANY:
                            continue
                        env.assertEqual(shard_profile[k][i],
                                        expected_shard_profile[k][i],
                                        message=f'{err_message} query: {query}')

            # Verify the coordinator profile data
            coordinator_profile = actual_res[8][3]
            env.assertEqual(coordinator_profile, expected_coordinator_profile,
                            message=f'COORDINATOR query: {query}')
