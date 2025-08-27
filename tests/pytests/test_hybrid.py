import numpy as np
from typing import List, Tuple, Optional
from RLTest import Env
from common import *
from utils.rrf import rrf, Result
from itertools import groupby
import re

# Constant string used in create_comparison_table() to indicate missing value
# or missing ranking info
MISSING_VALUE = "---"


def _sort_adjacent_same_scores(results: List[Result]) -> None:
    """
    Sort adjacent results with the same score by key for deterministic tests.

    Only sorts consecutive results with identical scores. Preserves score ordering.
    Does NOT sort non-adjacent results with the same score.

    Example: [Result('c', 0.5), Result('b', 1.0), Result('a', 1.0)] -> [Result('c', 0.5), Result('a', 1.0), Result('b', 1.0)]
    """
    grouped = []
    for _, group in groupby(results, key=lambda x: x.score):
        group_list = list(group)
        if len(group_list) > 1:
            group_list.sort(key=lambda x: x.key)
        grouped.extend(group_list)
    results[:] = grouped


def _validate_results(env, actual_results: List[Result], expected_results: List[Result], comparison_table: str) -> None:
    """Compare actual vs expected results, allowing for small score variations"""

    # Every test case should return at least one result
    env.assertGreater(len(actual_results), 0, message=comparison_table)

    # We assume the number of actual result is correct
    env.assertLessEqual(len(actual_results), len(expected_results), message=comparison_table)
    for i in range(len(actual_results)):
        env.assertEqual(
            actual_results[i].key, expected_results[i].key,
            message=f'key mismatch at index {i}: actual: {actual_results[i].key}, expected: {expected_results[i].key}')
        env.assertAlmostEqual(
            actual_results[i].score, expected_results[i].score, delta=1e-10,
            message=f'score mismatch at index {i}: actual: {actual_results[i].score}, expected: {expected_results[i].score}')


# =============================================================================
# HYBRID SEARCH TESTS CLASS
# =============================================================================

class testHybridSearch:
    '''
    Run all hybrid search tests on a single env without taking
    env down between tests. The test data is created once in __init__.
    '''
    def __init__(self):
        self.env = Env()
        self.dim = 128
        self.index_name = "idx"
        self._create_index(self.index_name, self.dim)
        self._generate_hybrid_test_data(self.dim)
        self.vector_blob = create_np_array_typed([2.3] * self.dim).tobytes()


    def _create_index(self, index_name: str, dim: int):
        """Create index with vector, text, numeric and tag fields"""
        data_type = "FLOAT32"
        try:
            self.env.cmd('FT.DROPINDEX', index_name)
        except:
            pass  # Index doesn't exist, which is fine
        self.env.expect(
            'FT.CREATE', index_name, 'SCHEMA',
            'vector', 'VECTOR', 'FLAT', '6', 'TYPE', data_type, 'DIM', dim,
            'DISTANCE_METRIC', 'L2',
            'vector_hnsw', 'VECTOR', 'HNSW', '6', 'TYPE', data_type, 'DIM', dim,
            'DISTANCE_METRIC', 'COSINE',
            'text', 'TEXT',
            'number', 'NUMERIC',
            'tag', 'TAG').ok()


    def _generate_hybrid_test_data(self, dim: int):
        """
        Generate sample data for hybrid search tests.
        This runs once when the class is instantiated.
        """
        num_vectors = 10
        # Generate and load data
        np.random.seed(42)  # For reproducibility
        conn = getConnectionByEnv(self.env)
        p = conn.pipeline(transaction=False)

        words = ["zero", "one", "two", "three", "four", "five", "six", "seven",
                 "eight", "nine"]

        for i in range(1, num_vectors + 1):
            # Generate field values

            if i % 2 == 0:
                tag_value = "even"
            else:
                tag_value = "odd"

            text_value = f"{words[i % len(words)]} {tag_value}"

            # Create documents with only text
            p.execute_command('HSET', f'text_{i:02d}',
                              'text', f'text {text_value}',
                              'number', i,
                              'tag', tag_value)

            # Create documents with only vector
            vector_value_1 = np.random.rand(dim).astype(np.float32).tobytes()
            p.execute_command('HSET', f'vector_{i:02d}',
                              'vector', vector_value_1,
                              'vector_hnsw', vector_value_1,
                              'number', i, 'tag', tag_value)

            # Create documents with both vector and text data
            vector_value_2 = np.random.rand(dim).astype(np.float32).tobytes()
            p.execute_command('HSET', f'both_{i:02d}',
                              'vector', vector_value_2,
                              'vector', vector_value_2,
                              'text', f'both {text_value}',
                              'number', i,
                              'tag', tag_value)

        p.execute()

    ############################################################################
    # KNN Vector search tests
    ############################################################################
    def test_knn_single_token_search(self):
        """Test hybrid search using KNN + single token search scenario"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "Single token text search",
            "hybrid_query": "SEARCH two VSIM @vector $BLOB LIMIT 0 11",
            "search_equivalent": "two",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_wildcard_search(self):
        """Test hybrid search using KNN + wildcard search scenario"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "Wildcard text search",
            "hybrid_query": "SEARCH * VSIM @vector $BLOB",
            "search_equivalent": "*",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        # TODO: Why the search_equivalent query returns 'vector_' docs with higher scores than the ones from 'both_' docs?
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_custom_k(self):
        """Test hybrid search using KNN with custom k scenario"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with custom k",
            "hybrid_query": "SEARCH even VSIM @vector $BLOB KNN 2 K 5",
            "search_equivalent": "even",
            "vector_equivalent": "*=>[KNN 5 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_custom_rrf_window(self):
        """Test hybrid search using KNN with custom k scenario"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with custom RRF WINDOW",
            "hybrid_query": "SEARCH even VSIM @vector $BLOB KNN 2 K 10 COMBINE RRF 2 WINDOW 2",
            "search_equivalent": "even",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]",
            "vector_suffix": "LIMIT 0 2"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_ef_runtime(self):
        """Test hybrid search using KNN + EF_RUNTIME parameter"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN query with parameters",
            "hybrid_query": "SEARCH even VSIM @vector_hnsw $BLOB KNN 4 K 10 EF_RUNTIME 100",
            "search_equivalent": "even",
            "vector_equivalent": "*=>[KNN 10 @vector_hnsw $BLOB EF_RUNTIME 100]=>{$YIELD_DISTANCE_AS: vector_distance}"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    # # TODO: Enable this test after adding support for YIELD_DISTANCE_AS in VSIM
    # def test_knn_yield_distance_as(self):
    #     """Test hybrid search using KNN + YIELD_DISTANCE_AS parameter"""
    #     if CLUSTER:
    #         raise SkipTest()
    #     scenario = {
    #         "test_name": "KNN query with parameters",
    #         "hybrid_query": "SEARCH even VSIM @vector $BLOB KNN 4 K 10 YIELD_DISTANCE_AS vector_distance",
    #         "search_equivalent": "even",
    #         "vector_equivalent": "*=>[KNN 10 @vector $BLOB]=>{$YIELD_DISTANCE_AS: vector_distance}"
    #     }
    #     run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_text_vector_prefilter(self):
        """Test hybrid search using KNN + VSIM text prefilter"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with text prefilter",
            "hybrid_query": "SEARCH @text:(even) VSIM @vector $BLOB FILTER @text:(two|four|six)",
            "search_equivalent": "@text:(even)",
            "vector_equivalent": "(@text:(two|four|six))=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_numeric_vector_prefilter(self):
        """Test hybrid search using KNN + numeric prefilter"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with numeric prefilter",
            "hybrid_query": "SEARCH @text:even VSIM @vector $BLOB FILTER '@number:[2 5]'",
            "search_equivalent": "@text:even",
            "vector_equivalent": "(@number:[2 5])=>[KNN 10 @vector $BLOB AS vector_distance]",
            "vector_suffix": "LIMIT 0 10"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_tag_vector_prefilter(self):
        """Test hybrid search using KNN + tag prefilter"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with tag prefilter",
            "hybrid_query": "SEARCH @text:even VSIM @vector $BLOB FILTER @tag:{odd}",
            "search_equivalent": "@text:even",
            "vector_equivalent": "(@tag:{odd})=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_no_vector_results(self):
        """Test hybrid search using KNN + vector prefilter that returns zero results"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with vector prefilter that returns zero results",
            "hybrid_query": "SEARCH @text:even VSIM @vector $BLOB FILTER @tag:{invalid_tag}",
            "search_equivalent": "@text:even",
            "vector_equivalent": "(@tag:{invalid_tag})=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_no_text_results(self):
        """Test hybrid search using KNN + text prefilter that returns zero results"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with vector prefilter that returns zero results",
            "hybrid_query": "SEARCH @text:(invalid_text) VSIM @vector $BLOB",
            "search_equivalent": "@text:(invalid_text)",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_knn_default_output(self):
        """Test hybrid search using default output fields"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(both) @number:[5 5]' "
            "VSIM @vector $BLOB FILTER '@number:[1 1]' "
            "COMBINE RRF 2 K 1"
        )
        # DocId     | SEARCH_RANK | VECTOR_RANK | SCORE
        # ----------------------------------------------------
        # both_01   | -           | 1           | 1/(4) = 0.25
        # both_05   | 1           | -           | 1/(4) = 0.25
        # vector_01 | -           | 2           | 1/(5) = 0.20
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob,self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        expected = [
            'format', 'STRING',
            'results',
            [
                ['attributes', [['__key', 'both_01', '__score', '0.5']]],
                ['attributes', [['__key', 'both_05', '__score', '0.5']]],
                ['attributes', [['__key', 'vector_01', '__score', '0.333333333333']]]
            ],
            'total_results', 3,
            'warning', [],
            'execution_time', ANY
        ]
        self.env.assertEqual(res, expected)

    def test_knn_load_key(self):
        """Test hybrid search + LOAD __key"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(even four)' "
            "VSIM @vector $BLOB FILTER @tag:{invalid_tag} "
            "LOAD 3 __key AS my_key"
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob,self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        # TODO: The attributes format is wrong, should be fixed by MOD-11034
        self.env.assertEqual(
            res[3][0][1],
            [['my_key', 'text_04']])
        self.env.assertEqual(
            res[3][1][1],
            [['my_key', 'both_04']])

    # # TODO: Enable this test after fixing MOD-10987
    # def test_knn_load_score(self):
    #     """Test hybrid search + LOAD __score"""
    #     if CLUSTER:
    #         raise SkipTest()
    #     hybrid_query = f"SEARCH '-@text:(both) -@text:(text)' VSIM @vector $BLOB FILTER @tag:{{invalid_tag}} COMBINE RRF 4 K 3 WINDOW 2 LOAD 3 __score AS my_score"
    #     hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
    #     res = self.env.executeCommand(*hybrid_cmd)
    #     self.env.assertEqual(
    #         res[3][0][1],
    #         [['my_score', '0.25']])
    #     self.env.assertEqual(
    #         res[3][1][1],
    #         [['my_score', '0.2']])

    def test_knn_load_fields(self):
        """Test hybrid search using LOAD to load fields"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(even four)' "
            "VSIM @vector $BLOB FILTER @tag:{invalid_tag} "
            "LOAD 9 @text AS my_text @number AS my_number @tag AS my_tag"
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        self.env.assertEqual(
            res[3][0][1],
            [['my_text', 'text four even',
              'my_number', '4',
              'my_tag', 'even']])
        self.env.assertEqual(
            res[3][1][1],
            [['my_text', 'both four even',
              'my_number', '4',
              'my_tag', 'even']])

    def test_knn_apply_on_default_output(self):
        """Test hybrid search using APPLY on default output fields"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(even four)' "
            "VSIM @vector $BLOB FILTER @tag:{invalid_tag} "
            "APPLY upper(@__key) AS upper_key "
            "APPLY @__score*2 AS double_score "
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)

        res_1 = to_dict(res[3][0][1][0])
        self.env.assertEqual(len(res_1), 4)
        self.env.assertEqual(res_1['__key'].upper(), res_1['upper_key'])
        self.env.assertAlmostEqual(
            float(res_1['__score']) * 2,
            float(res_1['double_score']),
            delta=0.0000001)

        res_2 = to_dict(res[3][1][1][0])
        self.env.assertEqual(len(res_2), 4)
        self.env.assertEqual(res_2['__key'].upper(), res_2['upper_key'])
        self.env.assertAlmostEqual(
            float(res_2['__score']) * 2,
            float(res_2['double_score']),
            delta=0.0000001)

    def test_knn_apply_on_custom_loaded_fields(self):
        """Test hybrid search using APPLY on custom loaded fields"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(even four)' "
            "VSIM @vector $BLOB FILTER @tag:{invalid_tag} "
            "LOAD 6 @text AS my_text @number AS my_number "
            "APPLY upper(@my_text) AS upper_text "
            "APPLY @my_number*2 AS double_number "
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)

        res_1 = to_dict(res[3][0][1][0])
        self.env.assertEqual(len(res_1), 4)
        self.env.assertEqual(res_1['my_text'].upper(), res_1['upper_text'])
        self.env.assertAlmostEqual(
            float(res_1['my_number']) * 2,
            float(res_1['double_number']),
            delta=0.0000001)

        res_2 = to_dict(res[3][1][1][0])
        self.env.assertEqual(len(res_2), 4)
        self.env.assertEqual(res_2['my_text'].upper(), res_2['upper_text'])
        self.env.assertAlmostEqual(
            float(res_2['my_number']) * 2,
            float(res_2['double_number']),
            delta=0.0000001)

    # # TODO: Enable this test after fixing GROUPBY in hybrid search
    # def test_knn_groupby(self):
    #     """Test hybrid search using GROUPBY"""
    #     if CLUSTER:
    #         raise SkipTest()
    #     hybrid_query = (
    #         "SEARCH '@text:(even four)' "
    #         "VSIM @vector $BLOB FILTER @tag:{invalid_tag} "
    #         "LOAD 1 @tag "
    #         "GROUPBY 1 @tag "
    #         "REDUCE COUNT 0 AS count "
    #     )
    #     hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
    #     res = self.env.executeCommand(*hybrid_cmd)
    #     print(res)
    #     self.env.assertEqual(res, [
    #         'format', 'STRING',
    #         'results',
    #         [
    #             ['attributes', [['tag', 'even', 'count', '2']]]
    #         ],
    #         'total_results', 1,
    #         'warning', [],
    #         'execution_time', ANY
    #     ])

    def test_knn_sortby_key_and_score(self):
        """Test hybrid search using SORTBY with key and score"""
        if CLUSTER:
            raise SkipTest()
        # Sort by key descending, score ascending
        hybrid_query = (
            "SEARCH '@text:(both) @number:[5 5]' "
            "VSIM @vector $BLOB FILTER '@number:[1 1]' "
            "COMBINE RRF 2 K 1 "
            "SORTBY 4 @__key DESC @__score ASC "
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        expected = [
            'format', 'STRING',
            'results',
            [
                ['attributes', [['__key', 'vector_01', '__score', '0.333333333333']]],
                ['attributes', [['__key', 'both_05', '__score', '0.5']]],
                ['attributes', [['__key', 'both_01', '__score', '0.5']]],
            ],
            'total_results', 3,
            'warning', [],
            'execution_time', ANY
        ]
        self.env.assertEqual(res, expected)

        # Sort by score ascending, key ascending
        hybrid_query = (
            "SEARCH '@text:(both) @number:[5 5]' "
            "VSIM @vector $BLOB FILTER '@number:[1 1]' "
            "COMBINE RRF 2 K 1 "
            "SORTBY 4 @__score ASC @__key ASC"
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        expected = [
            'format', 'STRING',
            'results',
            [
                ['attributes', [['__key', 'vector_01', '__score', '0.333333333333']]],
                ['attributes', [['__key', 'both_01', '__score', '0.5']]],
                ['attributes', [['__key', 'both_05', '__score', '0.5']]],
            ],
            'total_results', 3,
            'warning', [],
            'execution_time', ANY
        ]
        self.env.assertEqual(res, expected)


    def test_knn_sortby_with_apply(self):
        """Test hybrid search using SORTBY with APPLY"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(both) @number:[5 5]' "
            "VSIM @vector $BLOB FILTER '@number:[1 1]' "
            "COMBINE RRF 2 K 1 "
            "LOAD 3 @number @__key @__score "
            "APPLY 10-@number AS 10_minus_number "
            "SORTBY 6 @10_minus_number ASC @__key ASC @__score ASC"
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        expected = [
            'format', 'STRING',
            'results',
            [
                ['attributes', [['number', '5', '__key', 'both_05', '__score', '0.5', '10_minus_number', '5']]],
                ['attributes', [['number', '1', '__key', 'both_01', '__score', '0.5', '10_minus_number', '9']]],
                ['attributes', [['number', '1', '__key', 'vector_01', '__score', '0.333333333333', '10_minus_number', '9']]]
            ],
            'total_results', 3,
            'warning', [],
            'execution_time', ANY
        ]
        self.env.assertEqual(res, expected)

    def test_knn_with_params(self):
        """Test hybrid search using KNN with parameters"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(both) @number:[4 5]' "
            "VSIM @vector $BLOB FILTER '@number:[3 3]' "
            "COMBINE RRF 2 K 1"
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        expected_result = self.env.executeCommand(*hybrid_cmd)
        expected_result[9] = ANY # Ignore execution time

        # Use parameters in vector value
        vector = f'{self.vector_blob.decode("utf-8")}'
        hybrid_cmd = (
            'FT.HYBRID', self.index_name,
            'SEARCH', '@text:(both) @number:[4 5]',
            'VSIM', '@vector', '$MYVECTOR', 'FILTER', '@number:[3 3]',
            'COMBINE', 'RRF', '2', 'K', '1',
            'PARAMS', '2', 'MYVECTOR', vector
        )
        res = self.env.executeCommand(*hybrid_cmd)
        self.env.assertEqual(res, expected_result)

        # Use parameters in SEARCH term
        # TODO: MOD-10970 This requires DIALECT 2
        hybrid_cmd = (
            'FT.HYBRID', self.index_name,
            'SEARCH', '@text:($MYTEXT) @number:[4 5]',
            'VSIM', '@vector', vector, 'FILTER', '@number:[3 3]',
            'COMBINE', 'RRF', '2', 'K', '1',
            'PARAMS', '2', 'MYTEXT', 'both',
            'DIALECT', '2'
        )
        res = self.env.executeCommand(*hybrid_cmd)
        self.env.assertEqual(res, expected_result)

        # Use parameters in VSIM FILTER
        # TODO: MOD-10970 This requires DIALECT 2
        hybrid_cmd = (
            'FT.HYBRID', self.index_name,
            'SEARCH', '@text:(both) @number:[4 5]',
            'VSIM', '@vector', vector, 'FILTER', '@number:[$MYNUMBER 3]',
            'COMBINE', 'RRF', '2', 'K', '1',
            'PARAMS', '2', 'MYNUMBER', '3',
            'DIALECT', '2'
        )
        res = self.env.executeCommand(*hybrid_cmd)
        self.env.assertEqual(res, expected_result)

        # Multiple parameters
        hybrid_cmd = (
            'FT.HYBRID', self.index_name,
            'SEARCH', '@text:($MYTEXT) @number:[$FOUR $FIVE]',
            'VSIM', '@vector', '$MYVECTOR', 'FILTER', '@number:[$THREE $THREE]',
            'COMBINE', 'RRF', 2, 'K', 1,
            'PARAMS', 10, 'MYTEXT', 'both', 'MYVECTOR', vector, 'THREE', 3,
            'FOUR', 4, 'FIVE', 5,
            'DIALECT', 2
        )
        res = self.env.executeCommand(*hybrid_cmd)
        self.env.assertEqual(res, expected_result)


    def test_knn_post_filter(self):
        """Test hybrid search using KNN + post-filter"""
        if CLUSTER:
            raise SkipTest()
        # Run query without post-filter
        hybrid_cmd = [
            'FT.HYBRID', self.index_name,
            'SEARCH', '@text:(both) @number:[1 3]',
            'VSIM', '@vector', f'{self.vector_blob.decode('utf-8')}',
            'FILTER', '@text:(both) @number:[1 3]',
            'COMBINE', 'RRF', '2', 'K', '3',
            'LOAD', '2', '__key', '__score',
        ]
        unfiltered_res = self.env.executeCommand(*hybrid_cmd)
        unfiltered_dict = to_dict(unfiltered_res)

        # Add post-filter and re-run
        hybrid_cmd.append('FILTER')
        hybrid_cmd.append('@__key == "both_01"')
        filtered_res = self.env.executeCommand(*hybrid_cmd)
        filtered_dict = to_dict(filtered_res)

        # total_results should be the number of results before filtering.
        self.env.assertEqual(unfiltered_dict['total_results'], 3)
        self.env.assertEqual(filtered_dict['total_results'], 3)

        # But only 1 result is returned by the filtered query:
        expected = [
            'format', 'STRING',
            'results',
            [
                ['attributes', [['__key', 'both_01', '__score', '0.45']]]
            ],
            'total_results', 3,
            'warning', [],
            'execution_time', ANY
        ]
        self.env.assertEqual(filtered_res, expected)


    ############################################################################
    # Range query tests
    ############################################################################
    def test_range_basic(self):
        """Test hybrid search using range query scenario"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "Range query",
            "hybrid_query": "SEARCH @text:(four|even) VSIM @vector $BLOB RANGE 2 RADIUS 5",
            "search_equivalent": "@text:(four|even)",
            "vector_equivalent": "@vector:[VECTOR_RANGE 5 $BLOB]=>{$YIELD_DISTANCE_AS: vector_distance}"
        }
        run_test_scenario(self.env, self.index_name, scenario)

    def test_range_epsilon(self):
        """Test hybrid search using range with parameters"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "Range query",
            "hybrid_query": "SEARCH @text:(four|even) VSIM @vector_hnsw $BLOB RANGE 4 RADIUS 5 EPSILON 0.5",
            "search_equivalent": "@text:(four|even)",
            "vector_equivalent": "@vector_hnsw:[VECTOR_RANGE 5 $BLOB]=>{$EPSILON:0.5; $YIELD_DISTANCE_AS: vector_distance}"
        }
        run_test_scenario(self.env, self.index_name, scenario)


# =============================================================================
# QUERY TRANSLATION LAYER
# =============================================================================

def process_search_response(search_results):
    """
    Process search response into list of Result objects

    Args:
        search_results: Raw Redis search response like:
                       [349, b'25669', b'10.94315946939261', b'64068', b'10.822403974287118', ...]

    Returns:
        list: [Result(key=doc_id_str, score=score_float), ...] objects
    """
    if not search_results or len(search_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = search_results[1:]

    # Pack into Result objects
    processed = []
    for i in range(0, len(results_data), 2):
        if i + 1 < len(results_data):
            doc_id = results_data[i].decode('utf-8') if isinstance(results_data[i], bytes) else str(results_data[i])
            score = float(results_data[i + 1].decode('utf-8') if isinstance(results_data[i + 1], bytes) else results_data[i + 1])
            processed.append(Result(key=doc_id, score=score))
    return processed


def process_aggregate_response(aggregate_results):
    """
    Process aggregate response into list of Result objects

    Args:
        aggregate_results: Raw Redis aggregate response like:
        [30,
            ['__score', '1.69230771347', '__key', 'vector_10'],
            ['__score', '1.69230771347', '__key', 'vector_09'],...

    Returns:
        list: [Result(key=doc_id_str, score=score_float), ...] objects
    """
    if not aggregate_results or len(aggregate_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = aggregate_results[1:]

    # Pack into Result objects
    processed = []
    score = [float(row[row.index('__score') + 1] if '__score' in row else '0') for row in results_data]
    doc_id = [row[row.index('__key') + 1] for row in results_data]
    for i in range(0, len(score)):
        processed.append(Result(key=doc_id[i], score=score[i]))

    return processed


def process_hybrid_response(hybrid_results, expected_results: Optional[List[Result]] = None) -> Tuple[List[Result], dict]:
    """
    Process hybrid response into list of Result objects and ranking info

    Args:
        hybrid_results: Raw Redis hybrid response like:
             ['format', 'STRING', 'results', [['attributes', [['__key', 'both_02', 'SEARCH_RANK', '2', 'VECTOR_RANK', '5', '__score', '0.0312805474096']]], ...]]
        expected_results: Optional list of expected Result objects for comparison

    Returns:
        tuple: ([Result(key=doc_id_str, score=score_float), ...], ranking_info_dict)

    Note: ranking_info_dict contains search_ranks and vector_ranks for each document
    """
    if not hybrid_results or len(hybrid_results) < 4:
        return [], {}

    # Extract the results array from index 3
    # Structure: ['format', 'STRING', 'results', [result_items...]]
    results_data = hybrid_results[3]
    if not results_data:
        return [], {}

    processed = []
    ranking_info = {'search_ranks': {}, 'vector_ranks': {}}

    for result_item in results_data:
        # Each result_item is: ['attributes', [['__key', doc_id, 'SEARCH_RANK', '2', 'VECTOR_RANK', '5', '__score', score_str]]]
        if (len(result_item) >= 2 and
            result_item[0] == 'attributes' and
            result_item[1] and
            isinstance(result_item[1][0], list)):

            # Convert flat key-value list to dict using zip with slicing
            attr_list = result_item[1][0]
            attrs = dict(zip(attr_list[::2], attr_list[1::2]))

            # Extract doc_id and score if both exist
            if '__key' in attrs and '__score' in attrs:
                try:
                    score = float(attrs['__score'])
                    doc_id = attrs['__key']

                    # Extract ranking information
                    search_rank = attrs.get('SEARCH_RANK', '-')
                    vector_rank = attrs.get('VECTOR_RANK', '-')

                    # Store ranking info (convert to int if not '-')
                    if search_rank != '-':
                        try:
                            ranking_info['search_ranks'][doc_id] = int(search_rank)
                        except ValueError:
                            pass

                    if vector_rank != '-':
                        try:
                            ranking_info['vector_ranks'][doc_id] = int(vector_rank)
                        except ValueError:
                            pass

                    processed.append(Result(key=doc_id, score=score))
                except (ValueError, TypeError):
                    pass  # Skip invalid scores

    return processed, ranking_info


def create_comparison_table(actual_results: List[Result], expected_results: List[Result],
                           ranking_info: dict = None, original_search_results: List[Result] = None,
                           original_vector_results: List[Result] = None) -> str:
    """Create side-by-side comparison table of actual vs expected results with search/vector rankings"""
    lines = []
    lines.append("="*200)
    lines.append(f"{'RANK':<6} {'ACTUAL DOC_ID':<20} {'ACTUAL SCORE':<15} {'A_SEARCH':<10} {'A_VECTOR':<10} {'|':<3} {'EXPECTED DOC_ID':<20} {'EXPECTED SCORE':<15} {'E_SEARCH':<10} {'E_VECTOR':<10} {'MATCH':<8}")
    lines.append("-"*200)

    # Get ranking maps from hybrid results (for actual results)
    actual_search_rank_map = ranking_info.get('search_ranks', {}) if ranking_info else {}
    actual_vector_rank_map = ranking_info.get('vector_ranks', {}) if ranking_info else {}

    # Create ranking maps from original search and vector results (for expected results)
    expected_search_rank_map = {}
    expected_vector_rank_map = {}

    if original_search_results:
        for rank, result in enumerate(original_search_results, 1):
            expected_search_rank_map[result.key] = rank

    if original_vector_results:
        for rank, result in enumerate(original_vector_results, 1):
            expected_vector_rank_map[result.key] = rank

    max_len = max(len(actual_results), len(expected_results))

    for i in range(max_len):
        # Get actual result
        if i < len(actual_results):
            actual_result = actual_results[i]
            actual_doc_str = actual_result.key[:19]  # Truncate if too long
            actual_score_str = f"{actual_result.score:.10f}"

            # Get search and vector rankings for actual doc (from hybrid results)
            actual_search_rank = actual_search_rank_map.get(actual_result.key, MISSING_VALUE)
            actual_vector_rank = actual_vector_rank_map.get(actual_result.key, MISSING_VALUE)
            actual_search_str = str(actual_search_rank) if actual_search_rank != MISSING_VALUE else MISSING_VALUE
            actual_vector_str = str(actual_vector_rank) if actual_vector_rank != MISSING_VALUE else MISSING_VALUE
        else:
            actual_doc_str = MISSING_VALUE
            actual_score_str = MISSING_VALUE
            actual_search_str = MISSING_VALUE
            actual_vector_str = MISSING_VALUE

        # Get expected result
        if i < len(expected_results):
            expected_result = expected_results[i]
            expected_doc_str = expected_result.key[:19]  # Truncate if too long
            expected_score_str = f"{expected_result.score:.10f}"

            # Get search and vector rankings for expected doc (from original results)
            expected_search_rank = expected_search_rank_map.get(expected_result.key, MISSING_VALUE)
            expected_vector_rank = expected_vector_rank_map.get(expected_result.key, MISSING_VALUE)
            expected_search_str = str(expected_search_rank) if expected_search_rank != MISSING_VALUE else MISSING_VALUE
            expected_vector_str = str(expected_vector_rank) if expected_vector_rank != MISSING_VALUE else MISSING_VALUE
        else:
            expected_doc_str = MISSING_VALUE
            expected_score_str = MISSING_VALUE
            expected_search_str = MISSING_VALUE
            expected_vector_str = MISSING_VALUE

        # Check if they match
        if (i < len(actual_results) and i < len(expected_results) and
            actual_results[i].key == expected_results[i].key):
            match_str = "✓"
        else:
            match_str = "✗"

        lines.append(f"{i+1:<6} {actual_doc_str:<20} {actual_score_str:<15} {actual_search_str:<10} {actual_vector_str:<10} {'|':<3} {expected_doc_str:<20} {expected_score_str:<15} {expected_search_str:<10} {expected_vector_str:<10} {match_str:<8}")

    lines.append("="*200)
    return "\n" + "\n".join(lines) + "\n"

def process_vector_response(vector_results):
    """
    Process vector response into list of Result objects

    Args:
        vector_results: Raw Redis vector response like:
                       [10, b'45767', [b'score', b'0.961071372032'], b'16617', [b'score', b'0.956172764301'], ...]

    Returns:
        list: [Result(key=doc_id_str, score=score_float), ...] objects
    """
    if not vector_results or len(vector_results) < 2:
        return []

    # Remove the first element (total count)
    results_data = vector_results[1:]

    # Pack into Result objects
    processed = []
    for i in range(0, len(results_data), 2):
        if i + 1 < len(results_data):
            doc_id = results_data[i].decode('utf-8') if isinstance(results_data[i], bytes) else str(results_data[i])

            # Extract score from nested array [b'score', b'0.961071372032']
            score_data = results_data[i + 1]
            if isinstance(score_data, list) and len(score_data) >= 2:
                score_value = score_data[1]
                score = float(score_value.decode('utf-8') if isinstance(score_value, bytes) else score_value)
            else:
                score = 0.0  # fallback

            processed.append(Result(key=doc_id, score=score))
    return processed


def translate_vector_query(vector_query, vector_blob, index_name, cmd_suffix):
    """
    Translate simple vector query notation to working Redis command

    Args:
        simple_query: Simple notation like "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        vector_blob: Vector data as bytes
        index_name: Redis index name

    Returns:
        list: Command parts for redis_client.execute_command
    """
    command_parts = [
        'FT.SEARCH', index_name, vector_query,
        'PARAMS', '2', 'BLOB', vector_blob,
        'RETURN', '1', 'vector_distance', 'SORTBY', 'vector_distance',
        'DIALECT', '2',
    ]
    if cmd_suffix:
        command_parts.extend(cmd_suffix.split(' '))
    return command_parts


def translate_search_query(search_query, index_name):
    """
    Translate simple search query to Redis command

    Args:
        simple_query: Like "FT.SEARCH idx number"
        index_name: Redis index name

    Returns:
        list: Command parts for redis_client.execute_command
    """

    # Split into command parts
    command_parts = [
        'FT.SEARCH', index_name, search_query, 'WITHSCORES', 'NOCONTENT',
        'DIALECT', '2'
    ]
    return command_parts

def translate_hybrid_query(hybrid_query, vector_blob, index_name):
    """
    Translate simple hybrid query notation to working Redis command

    Args:
        simple_query: Simple notation like "SEARCH hello VSIM @vector $BLOB"
        vector_blob: Vector data as bytes
        index_name: Redis index name

    Returns:
        list: Command parts for redis_client.execute_command
    """
    cmd = hybrid_query.replace('$BLOB', vector_blob.decode('utf-8'))
    cmd = f'FT.HYBRID {index_name} {cmd}'
    # Split into command parts, keeping single quoted strings together
    command_parts = [p for p in re.split(r" (?=(?:[^']*'[^']*')*[^']*$)", cmd) if p]
    return command_parts

# =============================================================================
# TEST EXECUTION
# =============================================================================

def run_test_scenario(env, index_name, scenario):
    """
    Run a test scenario from dict

    Args:
        scenario: Dict with test scenario
        index_name: Redis index name
    """

    conn = getConnectionByEnv(env)

    # Create test vector (zero vector for now)
    dim = 128
    test_vector = create_np_array_typed([3.1415] * dim)
    vector_blob = test_vector.tobytes()

    # Execute search query
    search_cmd = translate_search_query(scenario['search_equivalent'], index_name)
    search_results_raw = conn.execute_command(*search_cmd)

    # Process search results
    search_results = process_search_response(search_results_raw)

    # Execute vector query using translation
    vector_cmd = translate_vector_query(scenario['vector_equivalent'], vector_blob, index_name, scenario.get('vector_suffix', ''))
    vector_results_raw = conn.execute_command(*vector_cmd)

    # Process vector results
    vector_results = process_vector_response(vector_results_raw)

    expected_rrf = rrf(search_results, vector_results)
    _sort_adjacent_same_scores(expected_rrf)

    hybrid_cmd = translate_hybrid_query(scenario['hybrid_query'], vector_blob, index_name)
    hybrid_results_raw = conn.execute_command(*hybrid_cmd)

    hybrid_results, ranking_info = process_hybrid_response(hybrid_results_raw)
    _sort_adjacent_same_scores(hybrid_results)

    # Create comparison table for debugging
    comparison_table = create_comparison_table(hybrid_results, expected_rrf, ranking_info, search_results, vector_results)
    # print(comparison_table)

    # Assert with detailed comparison table on failure
    _validate_results(env, hybrid_results, expected_rrf, comparison_table)
    return True
