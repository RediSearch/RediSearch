# -*- coding: utf-8 -*-

from includes import *
from common import *
from RLTest import Env

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
        [
            'Type', 'VECTOR',
            'Number of reading operations', 4,
            'Vector search mode', 'STANDARD_KNN'
        ],
        'Result processors profile',
        [
            ['Type', 'Index', 'Results processed', 4],
            ['Type', 'Metrics Applier', 'Results processed', 4],
            ['Type', 'Vector Normalizer', 'Results processed', 4],
            ['Type', 'Loader', 'Results processed', 4]
        ]
    ]
]]

query_and_profile = [
    # Tuple items:
    # - query,
    # - expected_shard_standalone_profile,
    # - expected_coordinator_standalone_profile
    # - expected_shard_cluster_profile,
    # - expected_coordinator_cluster_profile

    # minimal hybrid query
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
        [], # TODO: verify cluster profile
        # expected_coordinator_cluster_profile
        [] # TODO: verify cluster profile
    ),
    # hybrid query with LOAD * and NOSORT
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
        [], # TODO: verify cluster profile
        # expected_coordinator_cluster_profile
        [] # TODO: verify cluster profile
    ),
    # hybrid query with LIMIT
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
        [], # TODO: verify cluster profile
        # expected_coordinator_cluster_profile
        [] # TODO: verify cluster profile
    ),
    # # hybrid query with FILTER
    # (
    #     ['FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
    #      'SEARCH', 'hello',
    #      'VSIM', '@v', '$blob',
    #      'FILTER', '@t:(hello)',
    #      'LOAD', 2, '@t', '@v',
    #      'FILTER', '@t=="world"',
    #      'PARAMS', 2, 'blob', 'aaaaaaaa',
    #     ],
    #     # expected_shard_standalone_profile
    #     expected_shard_standalone_profile,
    #     # expected_coordinator_standalone_profile.
    #     [
    #         'Warning', ['None'],
    #         'Result processors profile',
    #         [
    #             ['Type', 'Hybrid Merger', 'Results processed', 2],
    #             ['Type', 'Filter - Predicate ==', 'Results processed', 0],
    #             ['Type', 'Sorter', 'Results processed', 0]
    #         ]
    #     ],
    #     # expected_shard_cluster_profile
    #     [], # TODO: verify cluster profile
    #     # expected_coordinator_cluster_profile
    #     [] # TODO: verify cluster profile
    # )
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

    # def test_profile_cluster(self):
    #     if not CLUSTER:
    #         raise SkipTest()
    #     env = self.env
    #     for query, _, _, expected_shard_profile, expected_coordinator_profile in query_and_profile:
    #         actual_res = env.execute_command(*query)
    #         self._verify_profile_structure(env.protocol, actual_res)
    #         env.assertEqual(actual_res[8][1], expected_shard_profile,
    #                         message=f'query: {query}')
    #         env.assertEqual(actual_res[8][3], expected_coordinator_profile,
    #                         message=f'query: {query}')

