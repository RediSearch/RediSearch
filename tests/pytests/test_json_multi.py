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
