import numpy as np
from RLTest import Env
from common import *
from utils.hybrid import *


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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

    def test_knn_custom_rrf_constant(self):
        """Test hybrid search using KNN with custom RRF CONSTANT"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with custom RRF CONSTANT",
            "hybrid_query": "SEARCH even VSIM @vector $BLOB KNN 2 K 10 COMBINE RRF 2 CONSTANT 50",
            "search_equivalent": "even",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]",
            "rrf_constant": 50
        }
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

    def test_knn_custom_rrf_window(self):
        """Test hybrid search using KNN with custom RRF WINDOW"""
        if CLUSTER:
            raise SkipTest()
        scenario = {
            "test_name": "KNN with custom RRF WINDOW",
            "hybrid_query": "SEARCH even VSIM @vector $BLOB KNN 2 K 10 COMBINE RRF 2 WINDOW 2",
            "search_equivalent": "even",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]",
            "vector_suffix": "LIMIT 0 2"
        }
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

    def test_knn_default_output(self):
        """Test hybrid search using default output fields"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(both) @number:[5 5]' "
            "VSIM @vector $BLOB FILTER '@number:[1 1]' "
            "COMBINE RRF 2 CONSTANT 1"
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
                ['attributes', ['__key', 'both_01', '__score', '0.5']],
                ['attributes', ['__key', 'both_05', '__score', '0.5']],
                ['attributes', ['__key', 'vector_01', '__score', '0.333333333333']]
            ],
            'total_results', 3,
            'warnings', [],
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
        self.env.assertEqual(
            res[3][0][1],
            ['my_key', 'text_04'])
        self.env.assertEqual(
            res[3][1][1],
            ['my_key', 'both_04'])

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
    #         ['my_score', '0.25'])
    #     self.env.assertEqual(
    #         res[3][1][1],
    #         ['my_score', '0.2'])

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
            [
                'my_text', 'text four even',
                'my_number', '4',
                'my_tag', 'even'
            ]
        )
        self.env.assertEqual(
            res[3][1][1],
            [
                'my_text', 'both four even',
                'my_number', '4',
                'my_tag', 'even'
            ]
        )

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

        res_1 = to_dict(res[3][0][1])
        self.env.assertEqual(len(res_1), 4)
        self.env.assertEqual(res_1['__key'].upper(), res_1['upper_key'])
        self.env.assertAlmostEqual(
            float(res_1['__score']) * 2,
            float(res_1['double_score']),
            delta=0.0000001)

        res_2 = to_dict(res[3][1][1])
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

        res_1 = to_dict(res[3][0][1])
        self.env.assertEqual(len(res_1), 4)
        self.env.assertEqual(res_1['my_text'].upper(), res_1['upper_text'])
        self.env.assertAlmostEqual(
            float(res_1['my_number']) * 2,
            float(res_1['double_number']),
            delta=0.0000001)

        res_2 = to_dict(res[3][1][1])
        self.env.assertEqual(len(res_2), 4)
        self.env.assertEqual(res_2['my_text'].upper(), res_2['upper_text'])
        self.env.assertAlmostEqual(
            float(res_2['my_number']) * 2,
            float(res_2['double_number']),
            delta=0.0000001)

    def test_knn_groupby(self):
        """Test hybrid search using GROUPBY"""
        if CLUSTER:
            raise SkipTest()
        hybrid_query = (
            "SEARCH '@text:(even four)' "
            "VSIM @vector $BLOB FILTER @tag:{invalid_tag} "
            "LOAD 1 @tag "
            "GROUPBY 1 @tag "
            "REDUCE COUNT 0 AS count "
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        print(res)
        self.env.assertEqual(res, [
            'format', 'STRING',
            'results',
            [
                ['attributes', ['tag', 'even', 'count', '2']]
            ],
            'total_results', 1,
            'warnings', [],
            'execution_time', ANY
        ])

    def test_knn_sortby_key_and_score(self):
        """Test hybrid search using SORTBY with key and score"""
        if CLUSTER:
            raise SkipTest()
        # Sort by key descending, score ascending
        hybrid_query = (
            "SEARCH '@text:(both) @number:[5 5]' "
            "VSIM @vector $BLOB FILTER '@number:[1 1]' "
            "COMBINE RRF 2 CONSTANT 1 "
            "SORTBY 4 @__key DESC @__score ASC "
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        expected = [
            'format', 'STRING',
            'results',
            [
                ['attributes', ['__key', 'vector_01', '__score', '0.333333333333']],
                ['attributes', ['__key', 'both_05', '__score', '0.5']],
                ['attributes', ['__key', 'both_01', '__score', '0.5']],
            ],
            'total_results', 3,
            'warnings', [],
            'execution_time', ANY
        ]
        self.env.assertEqual(res, expected)

        # Sort by score ascending, key ascending
        hybrid_query = (
            "SEARCH '@text:(both) @number:[5 5]' "
            "VSIM @vector $BLOB FILTER '@number:[1 1]' "
            "COMBINE RRF 2 CONSTANT 1 "
            "SORTBY 4 @__score ASC @__key ASC"
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        res = self.env.executeCommand(*hybrid_cmd)
        expected = [
            'format', 'STRING',
            'results',
            [
                ['attributes', ['__key', 'vector_01', '__score', '0.333333333333']],
                ['attributes', ['__key', 'both_01', '__score', '0.5']],
                ['attributes', ['__key', 'both_05', '__score', '0.5']],
            ],
            'total_results', 3,
            'warnings', [],
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
            "COMBINE RRF 2 CONSTANT 1 "
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
                ['attributes', ['number', '5', '__key', 'both_05', '__score', '0.5', '10_minus_number', '5']],
                ['attributes', ['number', '1', '__key', 'both_01', '__score', '0.5', '10_minus_number', '9']],
                ['attributes', ['number', '1', '__key', 'vector_01', '__score', '0.333333333333', '10_minus_number', '9']]
            ],
            'total_results', 3,
            'warnings', [],
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
            "COMBINE RRF 2 CONSTANT 1"
        )
        hybrid_cmd = translate_hybrid_query(hybrid_query, self.vector_blob, self.index_name)
        expected_result = self.env.executeCommand(*hybrid_cmd)
        expected_result[9] = ANY # Ignore execution time

        # Use parameters in vector value
        hybrid_cmd = (
            'FT.HYBRID', self.index_name,
            'SEARCH', '@text:(both) @number:[4 5]',
            'VSIM', '@vector', '$MYVECTOR', 'FILTER', '@number:[3 3]',
            'COMBINE', 'RRF', '2', 'CONSTANT', '1',
            'PARAMS', '2', 'MYVECTOR', self.vector_blob
        )
        res = self.env.executeCommand(*hybrid_cmd)
        self.env.assertEqual(res, expected_result)

        # Use parameters in SEARCH term
        # TODO: MOD-10970 This requires DIALECT 2
        hybrid_cmd = (
            'FT.HYBRID', self.index_name,
            'SEARCH', '@text:($MYTEXT) @number:[4 5]',
            'VSIM', '@vector', self.vector_blob, 'FILTER', '@number:[3 3]',
            'COMBINE', 'RRF', '2', 'CONSTANT', '1',
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
            'VSIM', '@vector', self.vector_blob,
            'FILTER', '@number:[$MYNUMBER 3]',
            'COMBINE', 'RRF', '2', 'CONSTANT', '1',
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
            'COMBINE', 'RRF', 2, 'CONSTANT', 1,
            'PARAMS', 10, 'MYTEXT', 'both', 'MYVECTOR', self.vector_blob,
            'THREE', 3, 'FOUR', 4, 'FIVE', 5,
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
            'VSIM', '@vector', self.vector_blob,
            'FILTER', '@text:(both) @number:[1 3]',
            'COMBINE', 'RRF', '2', 'CONSTANT', '3',
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
                ['attributes', ['__key', 'both_01', '__score', '0.45']]
            ],
            'total_results', 3,
            'warnings', [],
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
        run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)

    # # TODO: Fix MOD-11236: FT.HYBRID + VECTOR_RANGE + HNSW vector field returns less results than expected
    # def test_range_epsilon(self):
    #     """Test hybrid search using range with parameters"""
    #     if CLUSTER:
    #         raise SkipTest()
    #     scenario = {
    #         "test_name": "Range query",
    #         "hybrid_query": "SEARCH @text:(four|even) VSIM @vector_hnsw $BLOB RANGE 4 RADIUS 5 EPSILON 0.5",
    #         "search_equivalent": "@text:(four|even)",
    #         "vector_equivalent": "@vector_hnsw:[VECTOR_RANGE 5 $BLOB]=>{$EPSILON:0.5; $YIELD_DISTANCE_AS: vector_distance}"
    #     }
    #     run_test_scenario(self.env, self.index_name, scenario, self.vector_blob)


