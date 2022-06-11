import json

from common import *
from includes import *

doc1_content = r'''{
    "name": "wonderbar",
    "category": ["mathematics and computer science", "logic", "programming"],
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
        }
    ]}
'''


def testMultiTag(env):
    # test multiple TAG values (array of strings)
    conn = getConnectionByEnv(env)
    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()

    # As multi flat values
    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TAG')
    # As array
    #X env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TAG')
    
    waitForIndex(env, 'idx1')
    #X waitForIndex(env, 'idx2')
    
    res1 = [1, 'doc:1', ['category', 'mathematics and computer science']]
    env.expect('FT.SEARCH', 'idx1', '@category:{logic}', 'RETURN', '1', 'category').equal(res1)


def testMultiText(env):
    # test multiple TEXT values (array of array of strings)
    conn = getConnectionByEnv(env)
    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()

    # As multi flat values
    #X env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TEXT')
    #X env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.category[0]', 'AS', 'category', 'TEXT')
    
    # As array
    env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TEXT')
    
    #env.execute_command('FT.CREATE', 'idx3', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors[*]', 'AS', 'authors', 'TAG')
    #env.execute_command('FT.CREATE', 'idx4', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors', 'AS', 'authors', 'TAG')
    
    ## waitForIndex(env, 'idx1')
    waitForIndex(env, 'idx2')
    #waitForIndex(env, 'idx3')
    #waitForIndex(env, 'idx4')
    
    res1 = [1, 'doc:1', ['category', '["mathematics and computer science","logic","programming"]']]
    
    # res = env.execute_command('FT.EXPLAIN', 'idx2', '@category:(science programming)', 'RETURN', '1', 'category')
    # env.assertNotEqual(res, None, message=res)
    
    env.expect('FT.SEARCH', 'idx2', '@category:(programming science)=>{$slop:200; $inorder:false}', 'RETURN', '1', 'category').equal(res1)
    env.expect('FT.SEARCH', 'idx2', '@category:(programming science)=>{$slop:5; $inorder:false}', 'RETURN', '1', 'category').equal([0])


def testMultiNonText(env):
    # test multiple TEXT values includind some non-text values (null, number, bool, array, object)
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
        

    