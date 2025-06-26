from common import *

def _prepare_index(env, idx, dim=4):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', idx, 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'TYPE', 'FLOAT32',
               't', 'TEXT',
               'x', 'NUMERIC').ok()

    conn.execute_command('HSET', 'doc1', 'x', '1', 'v', create_np_array_typed([1.1] * dim).tobytes(), 't', 'pizza')
    conn.execute_command('HSET', 'doc2', 'x', '1', 'v', create_np_array_typed([1.4] * dim).tobytes())
    conn.execute_command('HSET', 'doc3', 'x', '1', 't', 'pizzas')
    conn.execute_command('HSET', 'doc4', 'x', '1', 't', 'beer')
    conn.execute_command('HSET', 'doc5', 'x', '1')

def testWithVectorRange(env):
    dim = 4
    _prepare_index(env, 'idx', dim)
    conn = getConnectionByEnv(env)

    # Get vector distance and text score without APPLY
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx', '@v:[VECTOR_RANGE .7 $vector]=>{$YIELD_DISTANCE_AS: vector_distance} | @t:(pizza) | @x==1',
        'ADDSCORES', 'SCORER', 'BM25STD',
        'PARAMS', '2', 'vector', create_np_array_typed([1.2] * dim).tobytes(),
        'LOAD', '3', '@vector_distance', '@__key', 't',
        'SORTBY', '2', '@__key', 'ASC',
        'DIALECT', '2')
    nresults = 5
    env.assertEqual(res[0], nresults)
    v_dist = [float(row[row.index('vector_distance') + 1] if 'vector_distance' in row else '0') for row in res[1:]]
    t_score = [float(row[row.index('__score') + 1] if '__score' in row else '0') for row in res[1:]]

    # Test APPLY with case function
    apply_expr_and_expected_results = [
        (
            'case(!exists(@__score), 0, 1)',
            [1] * nresults  # All documents have __score, so all should return 1
        ),
        (
            'case(exists(@vector_distance), (@__score*0.3 + @vector_distance*0.7), (@__score*0.3))',
            sorted([(t_score[i]*0.3 + v_dist[i]*0.7) for i in range(nresults)], reverse=True)
        ),
        (
            'case(!exists(@vector_distance), (@__score)*0.3, (@__score*0.3 + @vector_distance))',
            sorted([t_score[i]*0.3 + v_dist[i] for i in range(nresults)], reverse=True)
        ),
        (
            'case(NULL, 1, 0)',
            [0] * nresults  # NULL condition in the case is evaluated as false, so all should return 0
        ),
        (
            'case(!NULL, 1, 0)',
            [1] * nresults  # !NULL condition is always true, so all should return 1
        )
    ]

    # Test APPLY with case function and linear combination of vector distance and text score
    for apply_expr, expected_score in apply_expr_and_expected_results:
        res = conn.execute_command(
            'FT.AGGREGATE', 'idx', '@v:[VECTOR_RANGE .7 $vector]=>{$YIELD_DISTANCE_AS: vector_distance} | @t:(pizza) | @x==1',
            'ADDSCORES', 'SCORER', 'BM25STD',
            'PARAMS', '2', 'vector', create_np_array_typed([1.2] * dim).tobytes(),
            'LOAD', '3', '@vector_distance', '@__key', 't',
            'APPLY', apply_expr, 'AS', 'final_score',
            'SORTBY', '2', '@final_score', 'DESC',
            'DIALECT', '2')

        final_scores = [float(row[row.index('final_score') + 1]) for row in res[1:]]
        for i in range(len(final_scores)):
            env.assertAlmostEqual(
                final_scores[i], expected_score[i], delta=0.0000001,
                message=f"Failed for apply_expr: {apply_expr} at index {i}")

def testInvalidApplyFunction(env):
    dim = 4
    _prepare_index(env, 'idx', dim)
    conn = getConnectionByEnv(env)

    # Invalid case condition (string instead of number)
    invalid_apply_exprs = [
        'case(@t > 1, 1, 0)',
        'case(1, @t > 1, 0)',
        'case(0, 1, @t > 1)',
    ]
    for apply_expr in invalid_apply_exprs:
        # filter by text, to make sure @t is a string
        env.expect(
            'FT.AGGREGATE', 'idx', '@t:(pizza|beer)',
            'LOAD', '1', '@t',
            'APPLY', apply_expr, 'AS', 'final_score',
            'DIALECT', '2').error().contains("Error converting string")

    # Invalid function name in APPLY clause
    invalid_apply_exprs = [
        'case(invalid_function(@vector_distance), 1, 0)',
        'case(!invalid_function(@vector_distance), 1, 0)',
        'case(!!invalid_function(@vector_distance), 1, 0)',
        'case(!!!invalid_function(@vector_distance), 1, 0)',
    ]
    for apply_expr in invalid_apply_exprs:
        env.expect(
            'FT.AGGREGATE', 'idx', '*',
            'APPLY', apply_expr, 'AS', 'final_score',
            'DIALECT', '2').error().contains("Unknown function name")

    # Missing properties in case
    invalid_apply_exprs = [
        'case(@vector_distance, 0, 1)',
        'case(1, @vector_distance, 0)',
        'case(0, 0, @vector_distance)',
    ]
    env.expect(
        'FT.AGGREGATE', 'idx', '@v:[VECTOR_RANGE .7 $vector]=>{$YIELD_DISTANCE_AS: vector_distance} | @t:(pizza) | @x==1',
        'ADDSCORES', 'SCORER', 'BM25STD',
        'PARAMS', '2', 'vector', create_np_array_typed([1.2] * dim).tobytes(),
        'LOAD', '3', '@vector_distance', '@__key', 't',
        'APPLY', 'case(@vector_distance, 0, 1)', 'AS', 'has_vector_distance',
        'SORTBY', '2', '@__key', 'ASC',
        'DIALECT', '2').error().contains("Could not find the value for a parameter name, consider using EXISTS")

    # Property missing in case branches
    invalid_apply_exprs = [
        'case(exists(@missing), 1, 0)',
        'case(1, exists(@missing), 0)',
        'case(0, 0, exists(@missing))',
    ]
    for apply_expr in invalid_apply_exprs:
        env.expect(
            'FT.AGGREGATE', 'idx', '*',
            'APPLY', apply_expr, 'AS', 'final_score',
            'DIALECT', '2').error().contains("Property `missing` not loaded nor in pipeline")

def testCaseFunction(env):
    """Test the case function in APPLY clause with various conditions"""
    _prepare_index(env, 'idx')
    conn = getConnectionByEnv(env)

    # Test basic case function with exists condition
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx', '*',
        'LOAD', '2', '@t', '@__key',
        'APPLY', 'case(exists(@t), 1, 0)', 'AS', 'has_text',
        'SORTBY', '2', '@__key', 'ASC',
        'DIALECT', '2')

    env.assertEqual(res[0], 5)  # 5 documents total

    # Check that documents with 't' field have has_text=1, others have has_text=0
    for i, row in enumerate(res[1:]):
        doc_id = row[row.index('__key') + 1]  # Get the document ID
        has_text_idx = row.index('has_text')
        has_text_val = int(row[has_text_idx + 1])

        if doc_id in ['doc1', 'doc3', 'doc4']:  # These docs have 't' field
            env.assertEqual(has_text_val, 1, message=f"Expected doc {doc_id} to have has_text=1")
        else:  # doc2 doesn't have 't' field
            env.assertEqual(has_text_val, 0, message=f"Expected doc {doc_id} to have has_text=0")

def testCaseWithComparison(env):
    """Test case function with comparison operators"""
    conn = getConnectionByEnv(env)

    # Create index with numeric field
    env.expect('FT.CREATE', 'idx_num', 'SCHEMA',
               'num', 'NUMERIC', 'SORTABLE',
               't', 'TEXT').ok()

    # Add documents with different numeric values
    conn.execute_command('HSET', 'doc1', 'num', '10', 't', 'hello')
    conn.execute_command('HSET', 'doc2', 'num', '20', 't', 'world')
    conn.execute_command('HSET', 'doc3', 'num', '30', 't', 'redis')
    conn.execute_command('HSET', 'doc4', 'num', '40', 't', 'search')

    # Test case with numeric comparison
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx_num', '*',
        'LOAD', '2', '@num', '@t',
        'APPLY', 'case(@num < 20, "low", case(@num < 40, "medium", "high"))', 'AS', 'category',
        'GROUPBY', '1', '@category',
        'REDUCE', 'COUNT', '0', 'AS', 'count',
        'SORTBY', '2', '@category', 'ASC',
        'DIALECT', '2')

    # Expected: 3 groups - low (1 doc), medium (2 docs), high (1 doc)
    env.assertEqual(res[0], 3)
    categories = {row[1]: int(row[3]) for row in res[1:]}
    env.assertEqual(categories['low'], 1)
    env.assertEqual(categories['medium'], 2)
    env.assertEqual(categories['high'], 1)

def testNestedCaseAndGroup(env):
    """Test nested case functions and group by"""
    env.expect('FT.CREATE', 'idx_nested', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)

    # Add documents with different text values
    conn.execute_command('HSET', 'doc1', 't', 'pizza')
    conn.execute_command('HSET', 'doc2', 't', 'pasta')
    conn.execute_command('HSET', 'doc3', 't', 'burger')
    conn.execute_command('HSET', 'doc4', 't', 'salad')

    # Test nested case expressions
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx_nested', '*',
        'LOAD', '1', '@t',
        'APPLY', 'case(contains(@t, "pizza"), ' \
        '              "Italian", ' \
        '              case(contains(@t, "pasta"), ' \
        '                   "Italian", ' \
        '                   case(contains(@t, "burger"), "American", "Other")))', 'AS', 'cuisine',
        'GROUPBY', '1', '@cuisine',
        'REDUCE', 'COUNT', '0', 'AS', 'count',
        'SORTBY', '2', '@cuisine', 'ASC',
        'DIALECT', '2')

    # Expected: 3 groups - American, Italian, Other
    env.assertEqual(res[0], 3)
    cuisines = {row[1]: int(row[3]) for row in res[1:]}
    env.assertEqual(cuisines['American'], 1)
    env.assertEqual(cuisines['Italian'], 2)
    env.assertEqual(cuisines['Other'], 1)

def testCaseWithLogicalOperators(env):
    """Test case function with logical operators (AND, OR, NOT)"""
    conn = getConnectionByEnv(env)

    # Create index with multiple fields
    env.expect('FT.CREATE', 'idx_logic', 'SCHEMA',
               'category', 'TAG',
               'price', 'NUMERIC',
               'in_stock', 'TAG').ok()

    # Add test documents
    conn.execute_command('HSET', 'prod1', 'category', 'electronics', 'price', '100', 'in_stock', 'yes')
    conn.execute_command('HSET', 'prod2', 'category', 'electronics', 'price', '200', 'in_stock', 'no')
    conn.execute_command('HSET', 'prod3', 'category', 'books', 'price', '15', 'in_stock', 'yes')
    conn.execute_command('HSET', 'prod4', 'category', 'books', 'price', '25', 'in_stock', 'no')

    # Test case with logical operators
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx_logic', '*',
        'LOAD', '4', '@category', '@price', '@in_stock', '@__key',
        'APPLY', 'case((@category == "electronics") && (@price < 150),' \
        '              "budget_electronics", ' \
        '              case((@category == "books") || (@in_stock == "yes"), ' \
        '                   "available_items",' \
        '                   "other"))', 'AS', 'product_class',
        'SORTBY', '2', '@__key', 'ASC',
        'DIALECT', '2')

    # Expected results:
    # prod1 first case true ==> budget_electronics
    # prod2 first case false, second case false ==> other
    # prod3 first case false, second case true ==> available_items
    # prod4 first case false, second case true ==> available_items

    env.assertEqual(res[0], 4)  # 4 documents total
    # Check that each product has the correct class
    products = {row[row.index('__key') + 1]: row[row.index('product_class') + 1] for row in res[1:]}
    env.assertEqual(products['prod1'], 'budget_electronics')
    env.assertEqual(products['prod2'], 'other')
    env.assertEqual(products['prod3'], 'available_items')
    env.assertEqual(products['prod4'], 'available_items')


def testCaseWithMissingFields(env):
    """Test case function behavior with missing fields"""
    conn = getConnectionByEnv(env)

    # Create index
    env.expect('FT.CREATE', 'idx_missing', 'SCHEMA',
               'field1', 'TEXT',
               'field2', 'NUMERIC').ok()

    # Add documents with some missing fields
    conn.execute_command('HSET', 'doc1', 'field1', 'hello', 'field2', '10')
    conn.execute_command('HSET', 'doc2', 'field1', 'world')  # field2 missing
    conn.execute_command('HSET', 'doc3', 'field2', '30')     # field1 missing
    conn.execute_command('HSET', 'doc4', 'field3', 'extra')                     # both fields missing

    # Test case with exists() for handling missing fields
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx_missing', '*',
        'LOAD', '3', '@field1', '@field2', '@__key',
        'APPLY', 'case(exists(@field1) && exists(@field2), ' \
        '              "both", ' \
        '               case(exists(@field1), ' \
        '                   "only_field1", ' \
        '                   case(exists(@field2), ' \
        '                       "only_field2", ' \
        '                       "none")))', 'AS', 'fields_status',
        'SORTBY', '2', '@__key', 'ASC',
        'DIALECT', '2')

    # Check the results
    env.assertEqual(res[0], 4) # 4 documents total
    products = {row[row.index('__key') + 1]: row[row.index('fields_status') + 1] for row in res[1:]}
    env.assertEqual(products['doc1'], 'both')
    env.assertEqual(products['doc2'], 'only_field1')
    env.assertEqual(products['doc3'], 'only_field2')
    env.assertEqual(products['doc4'], 'none')

def testNestedCaseDepth(env):
    """Test deep nesting of the case function"""

    def generate_case_expr(depth):
        """ Generate a recursive CASE structure with the specified depth in the form: CASE(0, "a", "b") """
        if depth <= 1:
            return f"case(0, 'a{depth}', 'b')"

        return f"case(0, 'a{depth}', {generate_case_expr(depth-1)})"

    conn = getConnectionByEnv(env)

    # Create an index and add a doc
    env.expect('FT.CREATE', 'idx_depth', 'SCHEMA',
               't', 'TAG').ok()
    conn.execute_command('HSET', 'doc:1', 't', 'foo')
    # Test deep nesting of case
    depth = 20
    case_expr = generate_case_expr(depth)

    res = conn.execute_command(
        'FT.AGGREGATE', 'idx_depth', '*',
        'APPLY', case_expr, 'AS', 'nested_value',
        'DIALECT', '2')
    # Expect the deepest `else` value
    env.assertEqual(res[1], ['nested_value', 'b'])

def testNestedConditionsCase(env):
    """Test case function with nested conditions in both result branches"""
    conn = getConnectionByEnv(env)

    # Create index with status and priority fields
    env.expect('FT.CREATE', 'idx_orders', 'SCHEMA',
               'status', 'TAG', 'SORTABLE',
               'priority', 'TAG', 'SORTABLE',
               'order_id', 'NUMERIC', 'SORTABLE').ok()

    # Add test documents with different status and priority combinations
    conn.execute_command('HSET', 'order:1', 'status', 'pending', 'priority', 'high', 'order_id', '1')
    conn.execute_command('HSET', 'order:2', 'status', 'pending', 'priority', 'low', 'order_id', '2')
    conn.execute_command('HSET', 'order:3', 'status', 'completed', 'priority', 'high', 'order_id', '3')
    conn.execute_command('HSET', 'order:4', 'status', 'completed', 'priority', 'low', 'order_id', '4')
    conn.execute_command('HSET', 'order:5', 'status', 'cancelled', 'priority', 'high', 'order_id', '5')
    conn.execute_command('HSET', 'order:6', 'status', 'cancelled', 'priority', 'low', 'order_id', '6')

    # Test case with nested conditions in both result branches
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx_orders', '*',
        'LOAD', '3', '@status', '@priority', '@order_id',
        'APPLY', 'case(@status == "pending", ' \
        '              case(@priority == "high", 1, 2), ' \
        '              case(@status == "completed", 3, 4))', 'AS', 'status_code',
        'SORTBY', '2', '@order_id', 'ASC',
        'DIALECT', '2')

    # Expected status codes:
    # order:1 (pending, high) -> 1
    # order:2 (pending, low) -> 2
    # order:3 (completed, high) -> 3
    # order:4 (completed, low) -> 3
    # order:5 (cancelled, high) -> 4
    # order:6 (cancelled, low) -> 4

    env.assertEqual(res[0], 6)  # 6 documents total

    # Check that each order has the correct status code
    expected_status_codes = {
        '1': '1',  # pending + high
        '2': '2',  # pending + low
        '3': '3',  # completed + high
        '4': '3',  # completed + low
        '5': '4',  # cancelled + high
        '6': '4'   # cancelled + low
    }

    for row in res[1:]:
        order_id = row[row.index('order_id') + 1]
        status_code = row[row.index('status_code') + 1]
        env.assertEqual(status_code, expected_status_codes[order_id],
                       message=f"Order {order_id} with status '{row[row.index('status') + 1]}' and priority '{row[row.index('priority') + 1]}' should have status_code {expected_status_codes[order_id]}")

    # Test equivalent functionality using multiple APPLY statements (mapping approach)
    res = conn.execute_command(
        'FT.AGGREGATE', 'idx_orders', '*',
        'LOAD', '3', '@status', '@priority', '@order_id',
        'APPLY', 'case(@status == "pending", 1, 0)', 'AS', 'is_pending',
        'APPLY', 'case(@is_pending == 1 && @priority == "high", 1, 2)', 'AS', 'status_pending',
        'APPLY', 'case(@is_pending == 0 && @status == "completed", 3, 4)', 'AS', 'status_completed',
        'APPLY', 'case(@is_pending == 1, @status_pending, @status_completed)', 'AS', 'status_code',
        'SORTBY', '2', '@order_id', 'ASC',
        'DIALECT', '2')
    env.assertEqual(res[0], 6)  # 6 documents total

    # Check that each order has the correct final status
    for row in res[1:]:
        order_id = row[row.index('order_id') + 1]
        status_code = row[row.index('status_code') + 1]
        env.assertEqual(status_code, expected_status_codes[order_id],
                       message=f"Order {order_id} with status '{row[row.index('status') + 1]}' and priority '{row[row.index('priority') + 1]}' should have status_code {expected_status_codes[order_id]}")

def testCaseWithFieldsWithNullValue(env):
    dim = 4
    _prepare_index(env, 'idx', dim)
    conn = getConnectionByEnv(env)

    res = conn.execute_command(
        'FT.AGGREGATE', 'idx', '@v:[VECTOR_RANGE .7 $vector]=>{$YIELD_DISTANCE_AS: vector_distance} | @t:(pizza) | @x==1',
        'ADDSCORES', 'SCORER', 'BM25STD',
        'PARAMS', '2', 'vector', create_np_array_typed([1.2] * dim).tobytes(),
        'LOAD', '3', '@vector_distance', '@__key', 't',
        # Note: Using NULL directly in APPLY to simulate a null value
        'APPLY', 'NULL', 'AS', 'null_value',
        # Using case to check if the value is null, NULL is treated as false
        'APPLY', 'case(@null_value, 0, 1)', 'AS', 'is_null_value',
        'SORTBY', '2', '@__key', 'ASC',
        'DIALECT', '2')

    value = [float(row[row.index('is_null_value') + 1]) for row in res[1:]]
    for i in range(len(value)):
        env.assertEqual(value[i], 1, message=f"Failed for index {i}")
