from common import *

import json
from json_multi_text_content import *

@skip(no_json=True)
def testMultiTagReturnSimple(env):
    """ test multiple TAG values (array of strings) """
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    conn.execute_command('JSON.SET', 'doc:2', '$', doc2_content)
    conn.execute_command('JSON.SET', 'doc:3', '$', doc3_content)
    conn.execute_command('JSON.SET', 'doc:4', '$', doc4_content)

    # Index multi flat values
    env.expect('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TAG').ok()
    # Index an array
    env.expect('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TAG').ok()

    waitForIndex(env, 'idx1')
    waitForIndex(env, 'idx2')

    res1 = [1, 'doc:1', ['category', 'mathematics and computer science']]
    res2 = [1, 'doc:1', ['category_arr', '["mathematics and computer science","logic","programming","database"]']]

    # Currently return a single value (only the first value)
    env.expect('FT.SEARCH', 'idx1', '@category:{mathematics\ and\ computer\ science}', 'RETURN', '1', 'category').equal(res1)
    env.expect('FT.SEARCH', 'idx1', '@category:{logic}', 'RETURN', '1', 'category').equal(res1)
    env.expect('FT.SEARCH', 'idx1', '@category:{logic}', 'RETURN', '3', '$.category', 'AS', 'category_arr').equal(res2)

@skip(no_json=True)
def testMultiTagBool(env):
    """ test multiple TAG values (array of Boolean) """

    conn = getConnectionByEnv(env)
    # Index single and multi bool values
    conn.execute_command('JSON.SET', 'doc:1', '$', '{"foo": {"bar": [true, true]}, "fu": {"bar": [false, true]}}')
    conn.execute_command('JSON.SET', 'doc:2', '$', '{"foo": {"bar": [true, true]}, "fu": {"bar": [true, true]}}')
    conn.execute_command('JSON.SET', 'doc:3', '$', '{"foo": {"bar": [false, false]}, "fu": {"bar": [false, false]}}')
    env.expect('FT.CREATE', 'idx_multi', 'ON', 'JSON', 'SCHEMA', '$..bar[*]', 'AS', 'bar', 'TAG').ok()
    env.expect('FT.CREATE', 'idx_single', 'ON', 'JSON', 'SCHEMA', '$.foo.bar[0]', 'AS', 'bar', 'TAG').ok()
    waitForIndex(env, 'idx_multi')
    waitForIndex(env, 'idx_single')

    # FIXME:
    # res = env.cmd('FT.SEARCH', 'idx_multi', '@bar:{true}', 'NOCONTENT')
    # env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:1']))
    # res = env.cmd('FT.SEARCH', 'idx_multi', '@bar:{false}', 'NOCONTENT')
    # env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:3', 'doc:1']))

    res = env.cmd('FT.SEARCH', 'idx_single', '@bar:{true}', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:1']))
    env.expect('FT.SEARCH', 'idx_single', '@bar:{false}', 'NOCONTENT').equal([1, 'doc:3'])

@skip(no_json=True)
def testMultiTag(env):
    """ test multiple TAG values at root level (array of strings) """

    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(json.loads(doc1_content)['category']))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps(json.loads(doc2_content)['category']))
    conn.execute_command('JSON.SET', 'doc:3', '$', json.dumps(json.loads(doc3_content)['category']))
    conn.execute_command('JSON.SET', 'doc:4', '$', json.dumps(json.loads(doc4_content)['category']))

    # Index multi flat values
    env.cmd('FT.CREATE', 'idx_category_flat', 'ON', 'JSON', 'SCHEMA', '$.[*]', 'AS', 'category', 'TAG')
    # Index an array
    env.cmd('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$', 'AS', 'category', 'TAG')
    # Index both multi flat values and an array
    env.cmd('FT.CREATE', 'idx_category_arr_author_flat', 'ON', 'JSON', 'SCHEMA',
        '$.[*]', 'AS', 'author', 'TAG', # testing root path, so reuse the single top-level value
        '$', 'AS', 'category', 'TAG')

    waitForIndex(env, 'idx_category_flat')
    waitForIndex(env, 'idx_category_arr')
    waitForIndex(env, 'idx_category_arr_author_flat')

    searchMultiTagCategory(env)

@skip(no_json=True)
def testMultiTagNested(env):
    """ test multiple TAG values at inner level (array of strings) """

    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    conn.execute_command('JSON.SET', 'doc:2', '$', doc2_content)
    conn.execute_command('JSON.SET', 'doc:3', '$', doc3_content)
    conn.execute_command('JSON.SET', 'doc:4', '$', doc4_content)

    # Index multi flat values
    env.cmd('FT.CREATE', 'idx_category_flat', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TAG')
    env.cmd('FT.CREATE', 'idx_author_flat', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors[*]', 'AS', 'author', 'TAG')
    # Index an array
    env.cmd('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TAG')
    # Index an array of arrays
    env.cmd('FT.CREATE', 'idx_author_arr', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors', 'AS', 'author', 'TAG')
    # Index both multi flat values and an array
    env.cmd('FT.CREATE', 'idx_category_arr_author_flat', 'ON', 'JSON', 'SCHEMA',
        '$.books[*].authors[*]', 'AS', 'author', 'TAG',
        '$.category', 'AS', 'category', 'TAG')

    waitForIndex(env, 'idx_category_flat')
    waitForIndex(env, 'idx_author_flat')
    waitForIndex(env, 'idx_category_arr')
    waitForIndex(env, 'idx_author_arr')
    waitForIndex(env, 'idx_category_arr_author_flat')

    searchMultiTagCategory(env)
    searchMultiTagAuthor(env)

    env.cmd('FT.CREATE', 'idx_book', 'ON', 'JSON', 'SCHEMA',
        '$.category', 'AS', 'category', 'TAG',
        '$.books[*].authors[*]', 'AS', 'author', 'TAG',
        '$.books[*].name', 'AS', 'name', 'TAG')
    waitForIndex(env, 'idx_book')
    res = env.cmd('FT.SEARCH', 'idx_book',
        '(@name:{design*} -@category:{cloud}) | '
        '(@name:{Kubernetes*} @category:{cloud})',
        'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:3', 'doc:1']), message='idx_book')

def searchMultiTagCategory(env):
    """ helper function for searching multi-value attributes """

    conn = getConnectionByEnv(env)

    for idx in ['idx_category_arr', 'idx_category_arr_author_flat']:
        env.debugPrint(idx, force=TEST_DEBUG)

        # Use toSortedFlatList when scores are not distinct (to succedd also with coordinaotr)
        res = env.cmd('FT.SEARCH', idx, '@category:{database}', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:1', 'doc:2']), message="A " + idx)

        res = env.cmd('FT.SEARCH', idx, '@category:{performance}', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'doc:3']), message="B " + idx)

        env.expect('FT.SEARCH', idx, '@category:{high\ performance}', 'NOCONTENT').equal([1, 'doc:2'])
        env.expect('FT.SEARCH', idx, '@category:{cloud}', 'NOCONTENT').equal([1, 'doc:3'])

def searchMultiTagAuthor(env):
    """ helper function for searching multi-value attributes """

    # Not indexing array of arrays
    env.assertEqual(int(index_info(env, 'idx_author_arr')['hash_indexing_failures']), 3)

    for idx in ['idx_author_flat']:
        env.expect('FT.SEARCH', idx, '@author:{Donald\ Knuth}', 'NOCONTENT').equal([1, 'doc:1'])

        # Use toSortedFlatList when scores are not distinct (to succedd also with coordinaotr)
        res = env.cmd('FT.SEARCH', idx, '@author:{Brendan*}', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:3']))

        res = env.cmd('FT.SEARCH', idx, '@author:{Redis*}', 'NOCONTENT')
        # Notice doc:4 is not in result (path `$.books[*].authors[*]` does not match a scalar string in `authors`)
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([3, 'doc:1', 'doc:2', 'doc:3']))

    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@category:{programming}', 'NOCONTENT').equal([1, 'doc:1'])

@skip(no_json=True)
def testMultiNonText(env):
    """
    test multiple TAG values which include some non-text values at root level (null, number, bool, array, object)
    Skip nulls without failing
    Fail on number, bool, object, arr of strings, arr with mixed types
    """
    conn = getConnectionByEnv(env)

    non_text_dict = json.loads(doc_non_text_content)

    # Create indices and a key per index, e.g.,
    #   FT.CREATE idx1 ON JSON PREFIX 1 doc:1: SCHEMA $ AS root TAG
    #   JSON.SET doc:1: $ '["first", "second", null, "third", null, "null", null]'
    #
    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_text_dict.values()):
        doc = 'doc:{}:'.format(i+1)
        idx = 'idx{}'.format(i+1)
        env.cmd('FT.CREATE', idx, 'ON', 'JSON', 'PREFIX', '1', doc, 'SCHEMA', '$', 'AS', 'root', 'TAG')
        waitForIndex(env, idx)
        conn.execute_command('JSON.SET', doc, '$', json.dumps(v))
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, idx)['hash_indexing_failures']), res_failures, message=str(i))

    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@root:{third}', 'NOCONTENT').equal([1, 'doc:1:'])
    env.expect('FT.SEARCH', 'idx2', '@root:{third}', 'NOCONTENT').equal([1, 'doc:2:'])

@skip(no_json=True)
def testMultiNonTextNested(env):
    """
    test multiple TAG values which include some non-text values at inner level (null, number, bool, array, object)
    Skip nulls without failing
    Fail on number, bool, object, arr of strings, arr with mixed types
    """

    conn = getConnectionByEnv(env)

    non_text_dict = json.loads(doc_non_text_content)

    # Create indices, e.g.,
    #   FT.CREATE idx1 ON JSON SCHEMA $.attr1 AS attr TEXT
    for (i,v) in enumerate(non_text_dict.values()):
        env.cmd('FT.CREATE', 'idx{}'.format(i+1), 'ON', 'JSON', 'SCHEMA', '$.attr{}'.format(i+1), 'AS', 'attr', 'TAG')
    conn.execute_command('JSON.SET', 'doc:1', '$', doc_non_text_content)

    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_text_dict.values()):
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, 'idx{}'.format(i+1))['hash_indexing_failures']), res_failures)

    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@attr:{third}', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '@attr:{third}', 'NOCONTENT').equal([1, 'doc:1'])

def checkMultiTagReturn(env, expected, default_dialect, is_sortable, is_sortable_unf):
    """ Helper function for RETURN with multiple TAG values """

    conn = getConnectionByEnv(env)

    dialect_param = ['DIALECT', 3] if not default_dialect else []
    env.assertTrue(not is_sortable_unf or is_sortable)
    sortable_param = ['SORTABLE', 'UNF'] if is_sortable_unf else (['SORTABLE'] if is_sortable else [])
    env.assertEqual(len(expected), 4, message='dialect {}, sortable {}, unf {}'.format(dialect_param, is_sortable, is_sortable_unf))

    doc1_content = {
        "Name": "Product1",
        "Sellers": [
            {
                "Name": "Seller1",
                "Locations": ["FL", "AL"]
            },
            {
                "Name": "Seller2",
                "Locations": ["MS", "GA"]
            }
        ]
    }

    env.expect('FT.CREATE', 'idx_flat', 'ON', 'JSON', 'SCHEMA', '$.Sellers[*].Locations[*]', 'AS', 'val', 'TAG', *sortable_param).ok()
    env.expect('FT.CREATE', 'idx_arr', 'ON', 'JSON', 'SCHEMA', '$.Sellers[0].Locations', 'AS', 'val', 'TAG', *sortable_param).ok()

    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))

    def expect_case(val):
        return val.lower() if (is_sortable and not is_sortable_unf) and isinstance(val,str) else val

    expr = '@val:{al}'

    # Multi flat
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '3', '$.Sellers[0].Locations[1]', 'AS', 'arr_1', *dialect_param).equal(expect_case(expected[0]))
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '1', 'val', *dialect_param).equal(expect_case(expected[3]))
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '3', '$.Sellers[*].Locations[*]', 'AS', 'val', *dialect_param).equal(expect_case(expected[3]))
    env.expect('FT.SEARCH', 'idx_flat', expr,
               'RETURN', '3', '$.Sellers[0].Locations[*]', 'AS', 'val', *dialect_param).equal(expect_case(expected[1]))
    env.expect('FT.SEARCH', 'idx_flat', expr,
        'RETURN', '3', '$.Sellers[0].Locations', 'AS', 'val', *dialect_param).equal(expect_case(expected[2]))

    # Currently not considering `UNF` with multi value (MOD-4345)
    res = conn.execute_command('FT.AGGREGATE', 'idx_flat',
        expr, 'LOAD', '1', '@val', *dialect_param)
    env.assertEqual(res[1][1].lower(), expected[3][2][1].lower())

    res = conn.execute_command('FT.AGGREGATE', 'idx_flat',
        expr, 'GROUPBY', '1', '@val', *dialect_param)
    env.assertEqual(res[1][1].lower(), expected[3][2][1].lower())

    # Array
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '3', '$.Sellers[0].Locations[1]', 'AS', 'arr_1', *dialect_param).equal(expect_case(expected[0]))
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '1', 'val', *dialect_param).equal(expect_case(expected[2]))
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '3', '$.Sellers[*].Locations[*]', 'AS', 'val', *dialect_param).equal(expect_case(expected[3]))
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '3', '$.Sellers[0].Locations[*]', 'AS', 'val', *dialect_param).equal(expect_case(expected[1]))
    env.expect('FT.SEARCH', 'idx_arr', expr,
               'RETURN', '3', '$.Sellers[0].Locations', 'AS', 'val', *dialect_param).equal(expect_case(expected[2]))

    res = conn.execute_command('FT.AGGREGATE', 'idx_arr',
        expr, 'GROUPBY', '1', '@val', *dialect_param)
    # Ignore the result with older dialect
    #  Schema attribute with path to an array was not supported (lead to indexing failure)
    if not default_dialect:
        env.assertEqual(res[1][1].lower(), expected[2][2][1].lower())

    res = conn.execute_command('FT.AGGREGATE', 'idx_arr',
        expr, 'LOAD', '1', '@val', *dialect_param)
    env.assertEqual(res[1][1].lower(), expected[2][2][1].lower())

    # RETURN ALL
    res = conn.execute_command('FT.SEARCH', 'idx_flat', expr, *dialect_param)
    env.assertEqual(json.loads(res[2][1]), [doc1_content] if not default_dialect else doc1_content)

@skip(no_json=True)
def testMultiTagReturn(env):
    """ test RETURN with multiple TAG values """

    res1 = [1, 'doc:1', ['arr_1', '["AL"]']]
    res2 = [1, 'doc:1', ['val', '["FL","AL"]']]
    res3 = [1, 'doc:1', ['val', '[["FL","AL"]]']]
    res4 = [1, 'doc:1', ['val', '["FL","AL","MS","GA"]']]

    checkMultiTagReturn(env, [res1, res2, res3, res4], False, False, False)
    env.flush()
    checkMultiTagReturn(env, [res1, res2, res3, res4], False, True, False)
    env.flush()
    checkMultiTagReturn(env, [res1, res2, res3, res4], False, True, True)

@skip(no_json=True)
def testMultiTagReturnBWC(env):
    """ test backward compatibility of RETURN with multiple TAG values """
    res1 = [1, 'doc:1', ['arr_1', 'AL']]
    res2 = [1, 'doc:1', ['val', 'FL']]
    res3 = [1, 'doc:1', ['val', '["FL","AL"]']]

    checkMultiTagReturn(env, [res1, res2, res3, res2], True, False, False)
    env.flush()
    checkMultiTagReturn(env, [res1, res2, res3, res2], True, True, False)
    env.flush()
    checkMultiTagReturn(env, [res1, res2, res3, res2], True, True, True)
