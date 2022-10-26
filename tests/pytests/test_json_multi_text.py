from cmath import exp
import json

from RLTest import Env

from common import *
from includes import *

doc1_content = r'''{
    "name": "wonderbar",
    "category": ["mathematics and computer science", "logic", "programming", "database"],
    "books": [ 
        {
            "name": "Structure and Interpretation of Computer Programs",
            "authors": [
                "Harold Abelson", "Gerald Jay Sussman", "Julie Sussman"
            ]
        },
        {
            "name": "The Art of Computer Programming",
            "authors": [
                "Donald Knuth"
            ]
        },
        {
            "name": "Introduction to Algorithms",
            "authors": [
                "Thomas H. Cormen", "Charles E. Leiserson", "Ronald L. Rivest", "Clifford Stein"
            ]
        },
        {
            "name": "Classical Mathematical Logic: The Semantic Foundations of Logic",
            "authors": [
                "Richard L. Epstein"
            ]
        },
        {
            "name": "Design Patterns: Elements of Reusable Object-Oriented Software",
            "authors": [
                "Erich Gamma", "Richard Helm", "Ralph Johnson", "John Vlissides"
            ]
        },
        {
            "name": "Redis Microservices for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Redis 4.x Cookbook",
            "authors": [
                "Pengcheng Huang", "Zuofei Wang"
            ]
        }
    ]}
'''

doc2_content = r'''{
    "name": "foo",
    "category": ["database", "high performance"],
    "books": [ 
        {
            "name": "Redis for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Redis Microservices for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Systems Performance - Enterprise and the Cloud",
            "authors": [
                "Brendan Gregg"
            ]
        }
    ]}
'''

doc3_content = r'''{
    "name": "bar",
    "category": ["performance", "cloud"],
    "books": [ 
        {
            "name": "Redis for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Designing Data-Intensive Applications",
            "authors": [
                "Martin Kleppmann"
            ]
        },
        {
            "name": "Kubernetes: Up and Running",
            "authors": [
                "Kelsey Hightower", "Brendan Burns", "Joe Beda"
            ]
        }
    ]}
'''

doc4_content = r'''{
    "name": "rebar",
    "category": ["general"],
    "books": [ 
        {
            "name": "Redis for Dummies",
            "authors": "Redis Ltd."
        }
    ]}
'''

doc_non_text_content = r'''{
    "attr1": ["first", "second", null, "third", null , "null", null],
    "attr2": "third",
    "attr3": [null, null],
    "attr4": [],
    "attr5": null,
    "attr6": ["first", "second", null, "third", null, 2.04 ],
    "attr7": ["first", "second", null, "third", null, false ],
    "attr8": ["first", "second", null, "third", null, {"obj": "ection"} ],
    "attr9": ["first", "second", null, "third", null, ["recursi", "on"] ],
    "attr10": ["first", "second", null, "third", null, ["recursi", 50071] ]
}
'''


def expect_undef_order(query : Query):
    query.error().contains("has undefined ordering")


def checkMultiTagReturn(env, expected, default_dialect):
    
    dialect_param = ['DIALECT', 3] if not default_dialect else []
    env.assertEqual(len(expected), 3, message='dialect {}'.format(dialect_param))

    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    conn.execute_command('JSON.SET', 'doc:2', '$', doc2_content)
    conn.execute_command('JSON.SET', 'doc:3', '$', doc3_content)
    conn.execute_command('JSON.SET', 'doc:4', '$', doc4_content)

    # Index multi flat values
    env.expect('FT.CREATE', 'idx_flat', 'ON', 'JSON', 'SCHEMA',
               '$.category[*]', 'AS', 'category', 'TAG').ok()
    # Index an array
    env.expect('FT.CREATE', 'idx_arr', 'ON', 'JSON', 'SCHEMA',
               '$.category', 'AS', 'category', 'TAG').ok()

    waitForIndex(env, 'idx_flat')
    waitForIndex(env, 'idx_arr')

    env.expect('FT.SEARCH', 'idx_flat',
               '@category:{logic}', 'RETURN', '3', '$.category[1]', 'AS', 'category_1', *dialect_param).equal(expected[0])
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:{mathematics\ and\ computer\ science}', 'RETURN', '1', 'category', *dialect_param).equal(expected[1])
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:{logic}', 'RETURN', '1', 'category', *dialect_param).equal(expected[1])
    env.expect('FT.SEARCH', 'idx_flat', '@category:{logic}', 'RETURN',
               '3', '$.category', 'AS', 'category_arr', *dialect_param).equal(expected[2])
    
    res = conn.execute_command('FT.AGGREGATE', 'idx_flat',
        '@category:{logic}', 'GROUPBY', '1', '@category', *dialect_param)
    env.assertEqual(res[1][1].lower(), expected[1][2][1].lower())

    res = conn.execute_command('FT.AGGREGATE', 'idx_flat',
        '@category:{logic}', 'LOAD', '1', '@category', *dialect_param)
    env.assertEqual(res[1][1].lower(), expected[1][2][1].lower())
    
    # RETURN ALL
    res = conn.execute_command('FT.SEARCH', 'idx_flat', '@category:{logic}', *dialect_param)
    env.assertEqual(json.loads(res[2][1]), [json.loads(doc1_content)] if not default_dialect else json.loads(doc1_content))
    
    # Not indexing array
    env.assertEqual(int(index_info(env, 'idx_arr')['hash_indexing_failures']), 4)

def testMultiTagString(env):
    """ test multiple TAG values (array of strings) """

    res1 = [1, 'doc:1', ['category_1',
                         '["logic"]']]
    res2 = [1, 'doc:1', ['category',
                         '["mathematics and computer science","logic","programming","database"]']]
    res3 = [1, 'doc:1', ['category_arr',
                         '[["mathematics and computer science","logic","programming","database"]]']]

    checkMultiTagReturn(env, [res1, res2, res3], False)

def testMultiTagStringBWC(env):
    """ test multiple TAG values (array of strings) """

    res1 = [1, 'doc:1', ['category_1',
                         'logic']]
    res2 = [1, 'doc:1', ['category',
                         'mathematics and computer science']]
    res3 = [1, 'doc:1', ['category_arr',
                         '["mathematics and computer science","logic","programming","database"]']]

    checkMultiTagReturn(env, [res1, res2, res3], True)


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
    # res = env.execute_command('FT.SEARCH', 'idx_multi', '@bar:{true}', 'NOCONTENT')
    # env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:1']))    
    # res = env.execute_command('FT.SEARCH', 'idx_multi', '@bar:{false}', 'NOCONTENT')
    # env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:3', 'doc:1']))

    res = env.execute_command('FT.SEARCH', 'idx_single', '@bar:{true}', 'NOCONTENT')
    env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:1']))
    env.expect('FT.SEARCH', 'idx_single', '@bar:{false}', 'NOCONTENT').equal([1, 'doc:3'])

def testMultiTextReturn(env):
    """ test multiple TEXT values (array of strings) """
    conn = getConnectionByEnv(env)

    # Index multi flat values
    env.expect('FT.CREATE', 'idx_flat', 'ON', 'JSON', 'SCHEMA',
               '$.category[*]', 'AS', 'category', 'TEXT').ok()
    # Index an array
    env.expect('FT.CREATE', 'idx_arr', 'ON', 'JSON', 'SCHEMA',
               '$.category', 'AS', 'category', 'TEXT').ok()

    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    conn.execute_command('JSON.SET', 'doc:2', '$', doc2_content)
    conn.execute_command('JSON.SET', 'doc:3', '$', doc3_content)
    conn.execute_command('JSON.SET', 'doc:4', '$', doc4_content)
   
    res1 = [1, 'doc:1', ['category_1',
                         '["logic"]']]
    res2 = [1, 'doc:1', ['category',
                         '["mathematics and computer science","logic","programming","database"]']]
    res3 = [1, 'doc:1', ['category',
                         '[["mathematics and computer science","logic","programming","database"]]']]
    # Multi flat
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:logic', 'RETURN', '3', '$.category[1]', 'AS', 'category_1', 'DIALECT', 3).equal(res1)
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:(mathematics and computer science)', 'RETURN', '1', 'category', 'DIALECT', 3).equal(res2)
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:logic', 'RETURN', '1', 'category', 'DIALECT', 3).equal(res2)
    env.expect('FT.SEARCH', 'idx_flat', '@category:logic', 'RETURN',
               '3', '$.category', 'AS', 'category', 'DIALECT', 3).equal(res3)

    env.expect('FT.AGGREGATE', 'idx_flat',
               '@category:logic', 'GROUPBY', '1', '@category', 'DIALECT', 3).equal([1, ['category', res2[2][1]]])
    

    # Array
    env.expect('FT.SEARCH', 'idx_arr',
               '@category:logic', 'RETURN', '3', '$.category[1]', 'AS', 'category_1', 'DIALECT', 3).equal(res1)
    env.expect('FT.SEARCH', 'idx_arr',
               '@category:(mathematics and computer science)', 'RETURN', '1', 'category', 'DIALECT', 3).equal(res3)
    env.expect('FT.SEARCH', 'idx_arr',
               '@category:logic', 'RETURN', '1', 'category', 'DIALECT', 3).equal(res3)
    env.expect('FT.SEARCH', 'idx_arr', '@category:logic', 'RETURN',
               '3', '$.category', 'AS', 'category', 'DIALECT', 3).equal(res3)

    env.expect('FT.AGGREGATE', 'idx_arr',
               '@category:logic', 'GROUPBY', '1', '@category', 'DIALECT', 3).equal([1, ['category', res3[2][1]]])
               
    # RETURN ALL
    res = conn.execute_command('FT.SEARCH', 'idx_flat', '@category:logic', 'DIALECT', 3)
    env.assertEqual(json.loads(res[2][1]), [json.loads(doc1_content)])
    
    #
    # Test backward compatibility (before DIALECT 3)
    #
    res1_single = [1, 'doc:1', ['category_1',
                         'logic']]
    res2_single = [1, 'doc:1', ['category',
                         'mathematics and computer science']]
    res3_single = [1, 'doc:1', ['category',
                         '["mathematics and computer science","logic","programming","database"]']]
    # Multi flat
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:logic', 'RETURN', '3', '$.category[1]', 'AS', 'category_1').equal(res1_single)
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:(mathematics and computer science)', 'RETURN', '1', 'category').equal(res2_single)
    env.expect('FT.SEARCH', 'idx_flat',
               '@category:logic', 'RETURN', '1', 'category').equal(res2_single)
    env.expect('FT.SEARCH', 'idx_flat', '@category:logic', 'RETURN',
               '3', '$.category', 'AS', 'category').equal(res3_single)

    env.expect('FT.AGGREGATE', 'idx_flat', '@category:logic', 'GROUPBY', '1', '@category').equal([1, ['category', res2_single[2][1]]])
    
    # Array
    env.expect('FT.SEARCH', 'idx_arr',
               '@category:logic', 'RETURN', '3', '$.category[1]', 'AS', 'category_1').equal(res1_single)
    env.expect('FT.SEARCH', 'idx_arr',
               '@category:(mathematics and computer science)', 'RETURN', '1', 'category').equal(res3_single)
    env.expect('FT.SEARCH', 'idx_arr',
               '@category:logic', 'RETURN', '1', 'category').equal(res3_single)
    env.expect('FT.SEARCH', 'idx_arr', '@category:logic', 'RETURN',
               '3', '$.category', 'AS', 'category').equal(res3_single)

    env.expect('FT.AGGREGATE', 'idx_arr', '@category:logic', 'GROUPBY', '1', '@category').equal([1, ['category', res3_single[2][1]]])

    # RETURN ALL
    res = conn.execute_command('FT.SEARCH', 'idx_flat', '@category:logic')
    env.assertEqual(json.loads(res[2][1]), json.loads(doc1_content))


def testMultiText(env):
    """ test multiple TEXT values at root level (array of strings) """
    
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(json.loads(doc1_content)['category']))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps(json.loads(doc2_content)['category']))
    conn.execute_command('JSON.SET', 'doc:3', '$', json.dumps(json.loads(doc3_content)['category']))
    conn.execute_command('JSON.SET', 'doc:4', '$', json.dumps(json.loads(doc4_content)['category']))

    # Index multi flat values
    env.execute_command('FT.CREATE', 'idx_category_flat', 'ON', 'JSON', 'SCHEMA', '$.[*]', 'AS', 'category', 'TEXT')
    # Index an array
    env.execute_command('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$', 'AS', 'category', 'TEXT')
    # Index both multi flat values and an array
    env.execute_command('FT.CREATE', 'idx_category_arr_author_flat', 'ON', 'JSON', 'SCHEMA',
        '$.[*]', 'AS', 'author', 'TEXT', # testing root path, so reuse the single top-level value
        '$', 'AS', 'category', 'TEXT')
    
    waitForIndex(env, 'idx_category_flat')
    waitForIndex(env, 'idx_category_arr')
    waitForIndex(env, 'idx_category_arr_author_flat')
    
    searchMultiTextCategory(env)

def testMultiTextNested(env):
    """ test multiple TEXT values at inner level (array of strings) """

    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    conn.execute_command('JSON.SET', 'doc:2', '$', doc2_content)
    conn.execute_command('JSON.SET', 'doc:3', '$', doc3_content)
    conn.execute_command('JSON.SET', 'doc:4', '$', doc4_content)

    # Index multi flat values
    env.execute_command('FT.CREATE', 'idx_category_flat', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TEXT')
    env.execute_command('FT.CREATE', 'idx_author_flat', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors[*]', 'AS', 'author', 'TEXT')
    # Index an array
    env.execute_command('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TEXT')
    # Index an array of arrays
    env.execute_command('FT.CREATE', 'idx_author_arr', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors', 'AS', 'author', 'TEXT')
    # Index both multi flat values and an array
    env.execute_command('FT.CREATE', 'idx_category_arr_author_flat', 'ON', 'JSON', 'SCHEMA',
        '$.books[*].authors[*]', 'AS', 'author', 'TEXT',
        '$.category', 'AS', 'category', 'TEXT')
    
    waitForIndex(env, 'idx_category_flat')
    waitForIndex(env, 'idx_author_flat')
    waitForIndex(env, 'idx_category_arr')
    waitForIndex(env, 'idx_author_arr')
    waitForIndex(env, 'idx_category_arr_author_flat')

    searchMultiTextCategory(env)
    searchMultiTextAuthor(env)

    env.execute_command('FT.CREATE', 'idx_book', 'ON', 'JSON', 'SCHEMA',
        '$.category', 'AS', 'category', 'TEXT',
        '$.books[*].authors[*]', 'AS', 'author', 'TEXT',
        '$.books[*].name', 'AS', 'name', 'TEXT')
    waitForIndex(env, 'idx_book')
    res = env.execute_command('FT.SEARCH', 'idx_book',
        '(@name:(design*) -@category:(cloud)) | '
        '(@name:(Kubernetes*) @category:(cloud))',
        'NOCONTENT')
    env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:3', 'doc:1']), message='idx_book')

def searchMultiTextCategory(env):
    """ helper function for searching multi-value attributes """

    cond = ConditionalExpected(env, has_json_api_v2)
    def expect_0(q):
        q.equal([0])
        
    def expect_1(q):
        q.equal([1, 'doc:1'])

    for idx in ['idx_category_arr', 'idx_category_arr_author_flat']:
        env.debugPrint(idx, force=True)
        cond.call('FT.SEARCH', idx, '@category:(database programming)', 'NOCONTENT', 'SLOP', '98') \
        .expect_when(True,  expect_0) \
        .expect_when(False, expect_undef_order)
        cond.call('FT.SEARCH', idx, '@category:(database programming)', 'NOCONTENT', 'SLOP', '99') \
            .expect_when(True,  expect_1) \
            .expect_when(False, expect_undef_order)
        cond.call('FT.SEARCH', idx, '@category:(database programming)', 'NOCONTENT', 'SLOP', '99', 'INORDER') \
            .expect_when(True,  expect_0) \
            .expect_when(False, expect_undef_order)
        cond.call('FT.SEARCH', idx, '@category:(database programming)=>{$slop:99}', 'NOCONTENT', 'SLOP', '1') \
            .expect_when(True,  expect_1) \
            .expect_when(False, expect_undef_order)
        cond.call('FT.SEARCH', idx, '@category:(database programming)=>{$slop:100; $inorder:true}', 'NOCONTENT') \
            .expect_when(True,  expect_0) \
            .expect_when(False, expect_undef_order)
        cond.call('FT.SEARCH', idx, '@category:(database programming)=>{$slop:100; $inorder:false}', 'NOCONTENT') \
            .expect_when(True,  expect_1) \
            .expect_when(False, expect_undef_order)

        # Use toSortedFlatList when scores are not distinct (to succedd also with coordinaotr)
        res = env.execute_command('FT.SEARCH', idx, '@category:(database)', 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:1', 'doc:2']), message="A " + idx)
        
        res = env.execute_command('FT.SEARCH', idx, '@category:(performance)', 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:3']), message="B " + idx)

        env.expect('FT.SEARCH', idx, '@category:(high performance)', 'NOCONTENT').equal([1, 'doc:2'])
        env.expect('FT.SEARCH', idx, '@category:(cloud)', 'NOCONTENT').equal([1, 'doc:3'])
    
    # Multi-value attributes which have no definite ordering cannot use slop or inorder
    env.expect('FT.SEARCH', 'idx_category_flat', '@category:(programming science)=>{$slop:200}').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_flat', '@category:(programming science)=>{$inorder:false}').error().contains("has undefined ordering")    

def searchMultiTextAuthor(env):
    """ helper function for searching multi-value attributes """

    # Not indexing array of arrays
    env.assertEqual(int(index_info(env, 'idx_author_arr')['hash_indexing_failures']), 3)

    for idx in ['idx_author_flat']:
        env.debugPrint(idx, force=True)
        env.expect('FT.SEARCH', idx, '@author:(Richard)', 'NOCONTENT').equal([1, 'doc:1'])
        
        # Use toSortedFlatList when scores are not distinct (to succedd also with coordinaotr)
        res = env.execute_command('FT.SEARCH', idx, '@author:(Brendan)', 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:3']))

        res = env.execute_command('FT.SEARCH', idx, '@author:(Redis)', 'NOCONTENT')
        # Notice doc:4 is not in result (path `$.books[*].authors[*]` does not match a scalar string in `authors`)
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([3, 'doc:1', 'doc:2', 'doc:3']))

    # None-exact phrase using multi-value attributes which have no definite ordering cannot use slop or inorder
    env.expect('FT.SEARCH', 'idx_author_flat', '@author:(Redis Ltd.)=>{$slop:200}').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_author_flat', '@author:(Redis Ltd.)=>{$inorder:true}').error().contains("has undefined ordering")
    
    env.expect('FT.SEARCH', 'idx_author_flat', '@category|author:(Redis Ltd.)=>{$slop:200}').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_author_flat', '@category|author:(Redis Ltd.)=>{$inorder:true}').error().contains("has undefined ordering")

    env.expect('FT.SEARCH', 'idx_author_flat', '@category|author:("Redis Ltd.")=>{$inorder:true}').error().contains("has undefined ordering")
    
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@author:(Redis Ltd.)=>{$slop:200}').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@author:(Redis Ltd.)=>{$inorder:true}').error().contains("has undefined ordering")

    cond = ConditionalExpected(env, has_json_api_v2)
    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(programming science)=>{$slop:200; $inorder:false}', 'NOCONTENT') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, lambda q: q.error().contains("has undefined ordering"))
    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(programming science)=>{$slop:200}', 'NOCONTENT') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, lambda q: q.error().contains("has undefined ordering"))
    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(programming science)=>{$inorder:false}', 'NOCONTENT') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, lambda q: q.error().contains("has undefined ordering"))

def testInvalidPath(env):
    """ Test invalid JSONPath """

    cond = ConditionalExpected(env, has_json_api_v2)
    cond.call('FT.CREATE', 'idx_with_bad_path', 'ON', 'JSON', 'SCHEMA',
                           '$.books[*.authors', 'AS', 'author', 'TEXT',
                           '$.category..', 'AS', 'category', 'TEXT') \
    .expect_when(True, lambda q: q.error().contains("Invalid JSONPath")) \
    .expect_when(False, lambda q: q.ok())

def testUndefinedOrderingWithSlopAndInorder(env):
    """ Test that query attributes `slop` and `inorder` cannot be used when order is not well defined """

    # Index both multi flat values and an array
    env.execute_command('FT.CREATE', 'idx_category_arr_author_flat', 'ON', 'JSON', 'SCHEMA',
        '$.books[*].authors', 'AS', 'author', 'TEXT',
        '$.category', 'AS', 'category', 'TEXT')
    waitForIndex(env, 'idx_category_arr_author_flat')
    
    cond = ConditionalExpected(env, has_json_api_v2)
    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(does not matter)=>{$slop:200}') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(does not matter)=>{$inorder:false}') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(does not matter)=>{$inorder:true}') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)

    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@author:(does not matter)=>{$slop:200}').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@author:(does not matter)=>{$inorder:false}').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@author:(does not matter)=>{$inorder:true}').error().contains("has undefined ordering")

    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(does not matter)', 'SLOP', '200') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr_author_flat', '@category:(does not matter)', 'INORDER') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)


    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@author:(does not matter)', 'SLOP', '200').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@author:(does not matter)', 'INORDER').error().contains("has undefined ordering")

    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '(does not matter)', 'SLOP', '200').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '(does not matter)', 'INORDER').error().contains("has undefined ordering")

    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', 'does not matter', 'SLOP', '200').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', 'does not matter', 'INORDER').error().contains("has undefined ordering")
    
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '(does not matter)=>{$inorder:false}', 'SLOP', '200').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '(does not matter)=>{$slop:200}', 'INORDER').error().contains("has undefined ordering")

    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '(does not matter)=>{$inorder:false}').error().contains("has undefined ordering")
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '(does not matter)=>{$slop:200}').error().contains("has undefined ordering")

    
    # NOOFFSETS - SLOP/INORDER are not considered - No need to fail on undefined ordering
    env.execute_command('FT.CREATE', 'idx_category_arr_author_flat_2', 'ON', 'JSON',
        'NOOFFSETS',
        'SCHEMA',
            '$.books[*].authors', 'AS', 'author', 'TEXT',
            '$.category', 'AS', 'category', 'TEXT')
    waitForIndex(env, 'idx_category_arr_author_flat_2')
    env.expect('FT.SEARCH', 'idx_category_arr_author_flat_2', '@author:(does not matter)', 'INORDER', 'SLOP', '10').equal([0])

    
def testMultiNonText(env):
    """
    test multiple TEXT values which include some non-text values at root level (null, number, bool, array, object)
    Skip nulls without failing
    Fail on number, bool, object, arr of strings, arr with mixed types
    """
    conn = getConnectionByEnv(env)
    
    non_text_dict = json.loads(doc_non_text_content)
    
    # Create indices and a key per index, e.g.,
    #   FT.CREATE idx1 ON JSON PREFIX 1 doc:1: SCHEMA $ AS root TEXT
    #   JSON.SET doc:1: $ '["first", "second", null, "third", null, "null", null]'
    #
    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_text_dict.values()):
        doc = 'doc:{}:'.format(i+1)
        idx = 'idx{}'.format(i+1)
        env.execute_command('FT.CREATE', idx, 'ON', 'JSON', 'PREFIX', '1', doc, 'SCHEMA', '$', 'AS', 'root', 'TEXT')
        waitForIndex(env, idx)
        conn.execute_command('JSON.SET', doc, '$', json.dumps(v))
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, idx)['hash_indexing_failures']), res_failures, message=str(i))
    
    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@root:(third)', 'NOCONTENT').equal([1, 'doc:1:'])
    env.expect('FT.SEARCH', 'idx2', '@root:(third)', 'NOCONTENT').equal([1, 'doc:2:'])

def testMultiNonTextNested(env):
    """
    test multiple TEXT values which include some non-text values at inner level (null, number, bool, array, object)
    Skip nulls without failing
    Fail on number, bool, object, arr of strings, arr with mixed types
    """

    conn = getConnectionByEnv(env)

    non_text_dict = json.loads(doc_non_text_content)
    
    # Create indices, e.g.,
    #   FT.CREATE idx1 ON JSON SCHEMA $.attr1 AS attr TEXT
    for (i,v) in enumerate(non_text_dict.values()):
        env.execute_command('FT.CREATE', 'idx{}'.format(i+1), 'ON', 'JSON', 'SCHEMA', '$.attr{}'.format(i+1), 'AS', 'attr', 'TEXT')
    conn.execute_command('JSON.SET', 'doc:1', '$', doc_non_text_content)
    
    # First 5 indices are OK (nulls are skipped)
    for (i,v) in enumerate(non_text_dict.values()):
        res_failures = 0 if i+1 <= 5 else 1
        env.assertEqual(int(index_info(env, 'idx{}'.format(i+1))['hash_indexing_failures']), res_failures)
    
    # Search good indices with content
    env.expect('FT.SEARCH', 'idx1', '@attr:(third)', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '@attr:(third)', 'NOCONTENT').equal([1, 'doc:1'])

def trim_in_list(val, lst):
    for i, v in enumerate(lst):
        if type(v) == str:
            lst[i] = v.replace(val, '')
    return lst

def testMultiSortRoot(env):
    """
    test sorting by multiple TEXT at root level
    Should sort by first value
    """
    conn = getConnectionByEnv(env)

    (gag_arr, text_cmd_args, tag_cmd_args) = sortMultiPrepare()
    
    env.execute_command('FT.CREATE', 'idx1_multi_text', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'multi:', 'SCHEMA', '$', 'AS', 'gag', 'TEXT')
    env.execute_command('FT.CREATE', 'idx2_multi_tag', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'multi:', 'SCHEMA', '$.[*]', 'AS', 'gag', 'TAG')
    env.execute_command('FT.CREATE', 'idx3_multi_text_sort', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'multi:', 'SCHEMA', '$', 'AS', 'gag', 'TEXT', 'SORTABLE')
    
    env.execute_command('FT.CREATE', 'idx1_single_text', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'single:', 'SCHEMA', '$', 'AS', 'gag', 'TEXT')
    env.execute_command('FT.CREATE', 'idx2_single_tag', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'single:', 'SCHEMA', '$', 'AS', 'gag', 'TAG')
    env.execute_command('FT.CREATE', 'idx3_single_test_sort', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'single:', 'SCHEMA', '$', 'AS', 'gag', 'TEXT', 'SORTABLE')
    
    # docs with array of strings
    for i, gag in enumerate(gag_arr):
        conn.execute_command('JSON.SET', 'multi:doc:{}'.format(i+1), '$', json.dumps(gag))
    
    # docs with a single string
    for i, gag in enumerate(gag_arr):
        conn.execute_command('JSON.SET', 'single:doc:{}'.format(i+1), '$', json.dumps(gag[0]))

    sortMulti(env, text_cmd_args, tag_cmd_args)

def testMultiSortNested(env):
    """
    Test sorting by multiple TEXT at inner level
    Should sort by first value
    """

    conn = getConnectionByEnv(env)

    (gag_arr, text_cmd_args, tag_cmd_args) = sortMultiPrepare()
    
    env.execute_command('FT.CREATE', 'idx1_multi_text', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'multi:', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TEXT')
    env.execute_command('FT.CREATE', 'idx2_multi_tag', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'multi:', 'SCHEMA', '$.chalkboard[*]', 'AS', 'gag', 'TAG')
    env.execute_command('FT.CREATE', 'idx3_multi_text_sort', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'multi:', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TEXT', 'SORTABLE')
    
    env.execute_command('FT.CREATE', 'idx1_single_text', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'single:', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TEXT')
    env.execute_command('FT.CREATE', 'idx2_single_tag', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'single:', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TAG')
    env.execute_command('FT.CREATE', 'idx3_single_test_sort', 'ON', 'JSON', 'STOPWORDS', '0', 'PREFIX', '1', 'single:', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TEXT', 'SORTABLE')
    
    # docs with array of strings
    for i, gag in enumerate(gag_arr):
        conn.execute_command('JSON.SET', 'multi:doc:{}'.format(i+1), '$', json.dumps({ "chalkboard": gag}))
    
    # docs with a single string
    for i, gag in enumerate(gag_arr):
        conn.execute_command('JSON.SET', 'single:doc:{}'.format(i+1), '$', json.dumps({ "chalkboard": gag[0]}))

    sortMulti(env, text_cmd_args, tag_cmd_args)

def sortMultiPrepare():
    """ helper function for sorting multi-value attributes """

    gag_arr = [
        ["mistral"],
        ["fireplace"],
        ["mismatch"],
        ["firefly"],
        ["ahoy"],
        ["class"],
        ["firecracker"],
        ["cluster"],
        ["firewall"],
        ["mischief"],        
        ["classroom"],
        ["mistake"],
        ["classify"]
    ]

    text_cmd_args = [
        ['@gag:(fire*)', 'NOCONTENT', 'SORTBY', 'gag', 'ASC'],
        ['@gag:(class*)', 'NOCONTENT', 'SORTBY', 'gag', 'ASC'],
        ['@gag:(fire*)', 'NOCONTENT', 'SORTBY', 'gag', 'DESC'],
        ['@gag:(class*)', 'NOCONTENT', 'SORTBY', 'gag', 'DESC'],
        ['*', 'NOCONTENT', 'SORTBY', 'gag', 'ASC']
    ]
    tag_cmd_args = [
        ['@gag:{fire*}', 'NOCONTENT', 'SORTBY', 'gag', 'ASC'],
        ['@gag:{class*}', 'NOCONTENT', 'SORTBY', 'gag', 'ASC'],
        ['@gag:{fire*}', 'NOCONTENT', 'SORTBY', 'gag', 'DESC'],
        ['@gag:{class*}', 'NOCONTENT', 'SORTBY', 'gag', 'DESC'],
        ['*', 'NOCONTENT', 'SORTBY', 'gag', 'ASC']
    ]

    return (gag_arr, text_cmd_args, tag_cmd_args)

def sortMulti(env, text_cmd_args, tag_cmd_args):
    """ helper function for sorting multi-value attributes """    
    
    # Check that order is the same
    for i, (text_arg,tag_arg) in enumerate(zip(text_cmd_args, tag_cmd_args)):
        # Multi TEXT with single TEXT
        env.assertEqual(trim_in_list('multi:', env.execute_command('FT.SEARCH', 'idx1_multi_text', *text_arg)),
                        trim_in_list('single:', env.execute_command('FT.SEARCH', 'idx1_single_text', *text_arg)),
                        message = '{} with arg `{}`'.format('multi TEXT with single TEXT', text_arg))
        # Multi TAG with single TAG
        env.assertEqual(trim_in_list('multi:', env.execute_command('FT.SEARCH', 'idx2_multi_tag', *tag_arg)),
                        trim_in_list('single:', env.execute_command('FT.SEARCH', 'idx2_single_tag', *tag_arg)),
                        message = '{} arg `{}`'.format('multi TAG with single TAG', tag_arg))
        # Multi TEXT with multi TAG
        env.assertEqual(env.execute_command('FT.SEARCH', 'idx1_multi_text', *text_arg),
                        env.execute_command('FT.SEARCH', 'idx2_multi_tag', *tag_arg),
                        message = '{} text arg `{}` tag arg `{}`'.format('multi TEXT with multi TAG', text_arg, tag_arg))

    if not env.isCluster():
        # (skip this comparison in cluster since score is affected by the numer of shards/distribution of keys across shards)
        # Check that order and scores are the same
        for i, text_arg in enumerate(text_cmd_args):
            text_arg.append('WITHSCORES')
            # Multi TEXT with single TEXT
            env.assertEqual(trim_in_list('multi:', env.execute_command('FT.SEARCH', 'idx1_multi_text', *text_arg)),
                            trim_in_list('single:', env.execute_command('FT.SEARCH', 'idx1_single_text', *text_arg)),
                            message = '{} arg {}'.format('multi TEXT with single TEXT', text_arg))


def testMultiEmptyBlankOrNone(env):
    """ Test empty array or arrays comprised of empty strings or None """
    conn = getConnectionByEnv(env)
    
    values = [
        ["", "", ""],
        [""],
        [],
        [None],
        [None, None],
        ["", None, ""]
    ]

    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.val', 'AS', 'val', 'TEXT')
    
    for i, val in enumerate(values):
        conn.execute_command('JSON.SET', 'doc:{}'.format(i+1), '$', json.dumps({ "val": val}))
    conn.execute_command('JSON.SET', 'doc', '$', json.dumps({"val": ["haha"]}))
    env.expect('FT.SEARCH', 'idx', '@val:(haha)', 'NOCONTENT', 'SORTBY', 'val', 'ASC').equal([1, 'doc'])

    env.assertEqual(env.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')[0], len(values) + 1)

def getFtConfigCmd(env):
    if not env.isCluster():
        return "FT.CONFIG"
    else:
        return "_FT.CONFIG"

def testconfigMultiTextOffsetDelta(env):
    """ test default ft.config `MULTI_TEXT_SLOP` """
    
    if env.env == 'existing-env':
        env.skip()

    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    env.execute_command('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TEXT')
    waitForIndex(env, 'idx_category_arr')

    env.expect(getFtConfigCmd(env), 'SET', 'MULTI_TEXT_SLOP', '101').error().contains("Not modifiable at runtime")

    
    # MULTI_TEXT_SLOP = 100 (Default)
    #
    # Offsets:
    # ["mathematics and computer science", "logic", "programming", "database"]
    #   1                2        3      ,  103   ,  203         ,  303
    
    res = env.execute_command(getFtConfigCmd(env), 'GET', 'MULTI_TEXT_SLOP')
    env.assertEqual(res[0][1], '100')
    env.expect('FT.SEARCH', 'idx_category_arr', '@category:(mathematics database)', 'NOCONTENT').equal([1, 'doc:1'])
    
    cond = ConditionalExpected(env, has_json_api_v2)
    cond.call('FT.SEARCH', 'idx_category_arr', '@category:(mathematics database)', 'NOCONTENT', 'SLOP', '300') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr', '@category:(mathematics database)', 'NOCONTENT', 'SLOP', '301') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, expect_undef_order)

def testconfigMultiTextOffsetDeltaSlop101():
    """ test ft.config `MULTI_TEXT_SLOP` 101 """
    env = Env(moduleArgs = 'MULTI_TEXT_SLOP 101')

    # MULTI_TEXT_SLOP = 101
    conn = getConnectionByEnv(env)
    res = env.execute_command(getFtConfigCmd(env), 'GET', 'MULTI_TEXT_SLOP')
    env.assertEqual(res[0][1], '101')
    # Offsets:
    # ["mathematics and computer science", "logic", "programming", "database"]
    #   1                2        3      ,  104   ,  205         ,  306
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    env.execute_command('FT.CREATE', 'idx_category_arr_2', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TEXT')
    waitForIndex(env, 'idx_category_arr_2')

    cond = ConditionalExpected(env, has_json_api_v2)
    cond.call('FT.SEARCH', 'idx_category_arr_2', '@category:(mathematics database)', 'NOCONTENT', 'SLOP', '303') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr_2', '@category:(mathematics database)', 'NOCONTENT', 'SLOP', '304') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, expect_undef_order)

    cond.call('FT.SEARCH', 'idx_category_arr_2', '@category:(science database)', 'NOCONTENT', 'SLOP', '301') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr_2', '@category:(science database)', 'NOCONTENT', 'SLOP', '302') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, expect_undef_order)

def testconfigMultiTextOffsetDeltaSlop0():
    """ test ft.config `MULTI_TEXT_SLOP` 0 """
    env = Env(moduleArgs = 'MULTI_TEXT_SLOP 0')

    # MULTI_TEXT_SLOP = 0
    conn = getConnectionByEnv(env)
    res = env.execute_command(getFtConfigCmd(env), 'GET', 'MULTI_TEXT_SLOP')
    env.assertEqual(res[0][1], '0')
    # Offsets:
    # ["mathematics and computer science", "logic", "programming", "database"]
    #   1                2        3      ,  4   ,    5         ,    6
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    env.execute_command('FT.CREATE', 'idx_category_arr_3', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TEXT')
    waitForIndex(env, 'idx_category_arr_3')
    
    cond = ConditionalExpected(env, has_json_api_v2)
    cond.call('FT.SEARCH', 'idx_category_arr_3', '@category:(mathematics database)', 'NOCONTENT', 'SLOP', '3') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr_3', '@category:(mathematics database)', 'NOCONTENT', 'SLOP', '4') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, expect_undef_order)

    cond.call('FT.SEARCH', 'idx_category_arr_3', '@category:(science database)', 'NOCONTENT', 'SLOP', '1') \
        .expect_when(True, lambda q: q.equal([0])) \
        .expect_when(False, expect_undef_order)
    cond.call('FT.SEARCH', 'idx_category_arr_3', '@category:(science database)', 'NOCONTENT', 'SLOP', '2') \
        .expect_when(True, lambda q: q.equal([1, 'doc:1'])) \
        .expect_when(False, expect_undef_order)

def testMultiNoHighlight(env):
    """ highlight is not supported with multiple TEXT """
    pass

def checkMultiTextReturn(env, expected, default_dialect, is_sortable, is_sortable_unf):
    """ Helper function for RETURN with multiple TEXT values """

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

    env.expect('FT.CREATE', 'idx_flat', 'ON', 'JSON', 'SCHEMA', '$.Sellers[*].Locations[*]', 'AS', 'val', 'TEXT', *sortable_param).ok()
    env.expect('FT.CREATE', 'idx_arr', 'ON', 'JSON', 'SCHEMA', '$.Sellers[0].Locations', 'AS', 'val', 'TEXT', *sortable_param).ok()
    
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(doc1_content))

    def expect_case(val):
        return val.lower() if (is_sortable and not is_sortable_unf) and isinstance(val,str) else val

    expr = '@val:(al)'
    
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


def testMultiTextReturn(env):
    """ test RETURN with multiple TEXT values """

    res1 = [1, 'doc:1', ['arr_1', '["AL"]']]
    res2 = [1, 'doc:1', ['val', '["FL","AL"]']]
    res3 = [1, 'doc:1', ['val', '[["FL","AL"]]']]
    res4 = [1, 'doc:1', ['val', '["FL","AL","MS","GA"]']]
    

    checkMultiTextReturn(env, [res1, res2, res3, res4], False, False, False)
    env.flush()
    checkMultiTextReturn(env, [res1, res2, res3, res4], False, True, False)
    env.flush()
    checkMultiTextReturn(env, [res1, res2, res3, res4], False, True, True)

def testMultiTextReturnBWC(env):
    """ test backward compatibility of RETURN with multiple TEXT values """
    res1 = [1, 'doc:1', ['arr_1', 'AL']]
    res2 = [1, 'doc:1', ['val', 'FL']]
    res3 = [1, 'doc:1', ['val', '["FL","AL"]']]

    checkMultiTextReturn(env, [res1, res2, res3, res2], True, False, False)
    env.flush()
    checkMultiTextReturn(env, [res1, res2, res3, res2], True, True, False)
    env.flush()
    checkMultiTextReturn(env, [res1, res2, res3, res2], True, True, True)
