import json

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
        }
    ]}
'''


def testMultiTag(env):
    """ test multiple TAG values (array of strings) """
    conn = getConnectionByEnv(env)
    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()

    # As multi flat values
    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TAG')
    # As array
    #X env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TAG')
    
    waitForIndex(env, 'idx1')
    #X waitForIndex(env, 'idx2')
    
    res1 = [1, 'doc:1', ['category', 'mathematics and computer science']]
    res2 = [1, 'doc:1', ['category', '["mathematics and computer science","logic","programming","database"]']]
    # FIXME: Should return an array and not just the first entry
    env.expect('FT.SEARCH', 'idx1', '@category:{mathematics\ and\ computer\ science}', 'RETURN', '1', 'category').equal(res1)
    env.expect('FT.SEARCH', 'idx1', '@category:{logic}', 'RETURN', '1', 'category').equal(res1)
    env.expect('FT.SEARCH', 'idx1', '@category:{logic}', 'RETURN', '3', '$.category', 'AS', 'category').equal(res2)

def testMultiText(env):
    """ test multiple TEXT values at root level (array of strings) """
    pass # TODO:

def testMultiTextNested(env):
    # test multiple TEXT values at inner level (array of strings)

    conn = getConnectionByEnv(env)
    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()
    env.expect('JSON.SET', 'doc:2', '$', doc2_content).ok()

    # As multi flat values
    env.execute_command('FT.CREATE', 'idx_category_flat', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TEXT')
    env.execute_command('FT.CREATE', 'idx_author_flat', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors[*]', 'AS', 'author', 'TEXT')
    waitForIndex(env, 'idx_category_flat')
    waitForIndex(env, 'idx_author_flat')

    # Multi-value attributes which have no definite ordering cannot be SORTABLE
    ##env.expect('FT.CREATE', 'idx_category_flat_fail', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TEXT', 'SORTABLE').error().contains("zzz")
    ##env.expect('FT.CREATE', 'idx_author_flat_fail', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors[*]', 'AS', 'author', 'TEXT', 'SORTABLE').error().contains("zzz")
    
    # Multi-value attributes which have no definite ordering cannot use slop, inorder or SORTBY
    ##env.expect('FT.SEARCH', 'idx_category_flat', '@category:(programming science)=>{$slop:200}').error().contains("zzz")
    ##env.expect('FT.SEARCH', 'idx_category_flat', '@category:(programming science)=>{$inorder:false}').error().contains("zzz")
    ##env.expect('FT.SEARCH', 'idx_category_flat', '@category:(programming science)', 'SORTBY', 'category', 'ASC').error().contains("zzz")
    
    ##env.expect('FT.SEARCH', 'idx_author_flat', '@author:(Richard)=>{$slop:200}').error().contains("zzz")
    ##env.expect('FT.SEARCH', 'idx_author_flat', '@author:(Richard)=>{$inorder:false}').error().contains("zzz")
    ##env.expect('FT.SEARCH', 'idx_author_flat', '@author:(Richard)', 'SORTBY', 'author', 'ASC').error().contains("zzz")

    # Multi-value attributes which have no definite ordering can be searched for

    env.expect('FT.SEARCH', 'idx_category_flat', '@category:(databse programming)', 'NOCONTENT').equal([1, 'doc:1'])
    res = env.execute_command('FT.SEARCH', 'idx_category_flat', '@category:(database)', 'NOCONTENT')
    env.assertEqual(res[0], 2)

    env.expect('FT.SEARCH', 'idx_author_flat', '@author:(Richard)', 'NOCONTENT').equal([1, 'doc:1'])
    res = env.execute_command('FT.SEARCH', 'idx_author_flat', '@author:(Redis)', 'NOCONTENT')
    env.assertEqual(res[0], 2)
    

def testMultiTextArr(env):
    # test multiple TEXT values at root level (array of array of strings)
    pass # TODO:

def testMultiTextArrNested(env):
    """ test multiple TEXT values at inner level (array of array of strings) """
    conn = getConnectionByEnv(env)
    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()

    # As array
    env.execute_command('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TEXT')
    
    # As array of arrays
    env.execute_command('FT.CREATE', 'idx_author_arrays', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors', 'AS', 'author', 'TEXT')
    
    
    waitForIndex(env, 'idx_category_arr')
    waitForIndex(env, 'idx_author_arrays')
    
    res1 = [1, 'doc:1', ['category', '["mathematics and computer science","logic","programming","database"]']]
    
    env.expect('FT.SEARCH', 'idx_category_arr', '@category:(programming science)=>{$slop:200; $inorder:false}', 'RETURN', '1', 'category').equal(res1)
    env.expect('FT.SEARCH', 'idx_category_arr', '@category:(programming science)=>{$slop:5; $inorder:false}', 'RETURN', '1', 'category').equal([0])
        
    env.expect('FT.SEARCH', 'idx_author_arrays', '@author:(Richard)', 'NOCONTENT').equal([0])
    env.assertEqual(int(index_info(env, 'idx_author_arrays')['hash_indexing_failures']), 1)


def testMultiNonText(env):
    """ test multiple TEXT values includind some non-text values at root level (null, number, bool, array, object) """
    pass # TODO:

def testMultiNonTextNested(env):
    """ test multiple TEXT values includind some non-text values at inner level (null, number, bool, array, object) """
    conn = getConnectionByEnv(env)
    
    # Skip nulls without failing
    # fail on number, bool, object, arr of strings, arr with mixed types
    doc_content = r'''{
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
    for i in range(1, 11):
        env.execute_command('FT.CREATE', 'idx{}'.format(i), 'ON', 'JSON', 'SCHEMA', '$.attr{}'.format(i), 'AS', 'attr', 'TEXT')
    env.expect('JSON.SET', 'doc:1', '$', doc_content).ok()
    
    # First 5 indices are OK (nulls are skipped)
    for i in range(1, 6):
        env.assertEqual(int(index_info(env, 'idx{}'.format(i))['hash_indexing_failures']), 0)
    env.expect('FT.SEARCH', 'idx1', '@attr:(third)', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx2', '@attr:(third)', 'NOCONTENT').equal([1, 'doc:1'])

    # Other indices are not OK
    for i in range(6, 11):
        env.assertEqual(int(index_info(env, 'idx{}'.format(i))['hash_indexing_failures']), 1)

def testMultiSortRoot(env):
    """ test sorting by multiple TEXT at root level"""
    pass # TODO:

def testMultiSortNested(env):
    """test sorting by multiple TEXT at inner level"""

    conn = getConnectionByEnv(env)
    
    gag_arr = [
        ["Next time it could be", "me on the scaffolding"],                 #sorted 6th
        ["I am not", "authorized to", "fire substitute teachers"],          #sorted 2nd
        ["A", "fire drill", "does not demand a fire"],                      #sorted 1st
        ["I will not use abbrev."],                                         #sorted 4rd
        ["I will not yell", "\"fire\"", "in a", "crowded classroom"],       #sorted 5th
        ["I", "will", "not", "flip", "the", "classroom", "upside", "down"]  #sorted 3th
    ]

    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'STOPWORDS', '0', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TEXT')
    env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'STOPWORDS', '0', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TEXT', 'SORTABLE')
    env.execute_command('FT.CREATE', 'idx3', 'ON', 'JSON', 'STOPWORDS', '0', 'SCHEMA', '$.chalkboard', 'AS', 'gag', 'TEXT', 'SORTABLE', 'UNF')
    
    # TODO: FIXME: Remove debug config
    env.expect('FT.CONFIG', 'SET', 'TIMEOUT', 0).ok()

    cmd_args1 = ['@gag:(fire)', 'NOCONTENT', 'SORTBY', 'gag', 'ASC']
    sorted_res1 = [3, 'doc:3', 'doc:2', 'doc:5']
    
    cmd_args2 = ['@gag:(class*)', 'NOCONTENT', 'SORTBY', 'gag', 'ASC']
    sorted_res2 = [2, 'doc:6', 'doc:5']

    cmd_args3 = ['*', 'NOCONTENT', 'SORTBY', 'gag', 'ASC']
    sorted_res3 = [6, 'doc:3', 'doc:2', 'doc:6', 'doc:4', 'doc:5', 'doc:1']

    cmd_args4 = ['@gag:(fire)', 'NOCONTENT']
    sorted_res4 = [3, 'doc:2', 'doc:3', 'doc:5']

    # docs with array of strings
    for i, gag in enumerate(gag_arr):
        env.expect('JSON.SET', 'doc:{}'.format(i+1), '$', json.dumps({ "chalkboard": gag})).ok()
    
    # TODO:
    # Index without SORTABLE - SEARCH fails with SORTBY
    env.expect('FT.SEARCH', 'idx1', *cmd_args1).equal([0])
    env.expect('FT.SEARCH', 'idx1', *cmd_args2).equal([0])
    env.expect('FT.SEARCH', 'idx1', *cmd_args3).equal([0])
    # Index without SORTABLE - SEARCH succeeds without SORTBY
    env.expect('FT.SEARCH', 'idx1', *cmd_args4).equal(sorted_res4)

    # Indices with SORTABLE failed
    env.assertEqual(int(index_info(env, 'idx2')['hash_indexing_failures']), len(gag_arr))
    env.assertEqual(int(index_info(env, 'idx3')['hash_indexing_failures']), len(gag_arr))
    env.expect('FT.SEARCH', 'idx2', *cmd_args1).equal([0])
    env.expect('FT.SEARCH', 'idx3', *cmd_args1).equal([0])    
    
    # doc with a single string (concatentaed)
    for i, gag in enumerate(gag_arr):
        env.expect('JSON.SET', 'doc:{}'.format(i+1), '$', json.dumps({ "chalkboard": " ".join(gag)})).ok()
    
    env.expect('FT.SEARCH', 'idx1', *cmd_args1).equal(sorted_res1)
    env.expect('FT.SEARCH', 'idx2', *cmd_args1).equal(sorted_res1)
    env.expect('FT.SEARCH', 'idx3', *cmd_args1).equal(sorted_res1)

    env.expect('FT.SEARCH', 'idx1', *cmd_args2).equal(sorted_res2)
    env.expect('FT.SEARCH', 'idx2', *cmd_args2).equal(sorted_res2)
    env.expect('FT.SEARCH', 'idx3', *cmd_args2).equal(sorted_res2)

    env.expect('FT.SEARCH', 'idx1', *cmd_args3).equal(sorted_res3)
    env.expect('FT.SEARCH', 'idx2', *cmd_args3).equal(sorted_res3)
    env.expect('FT.SEARCH', 'idx3', *cmd_args3).equal(sorted_res3)


def testMultiEmptyBlankNone(env):
    # test empty array or arrays comprized of empty strings or None
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
        env.expect('JSON.SET', 'doc:{}'.format(i+1), '$', json.dumps({ "val": val})).ok()
    env.expect('FT.SEARCH', 'idx', '@val:(haha)', 'NOCONTENT', 'SORTBY', 'val', 'ASC').equal([0])


def testMultiNoHighlight(env):
    """ test highlight is not supported with multiple TEXT"""
    pass # TODO:

def testMultiNoHighlight(env):
    """ test highlight is not supported with multiple TEXT"""
    pass # TODO:
