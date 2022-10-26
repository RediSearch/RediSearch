import json

from RLTest import Env

from common import *
from includes import *

from json_multi_text_content import *

def testMultiTag(env):
    """ test multiple TAG values at root level (array of strings) """
    
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(json.loads(doc1_content)['category']))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps(json.loads(doc2_content)['category']))
    conn.execute_command('JSON.SET', 'doc:3', '$', json.dumps(json.loads(doc3_content)['category']))
    conn.execute_command('JSON.SET', 'doc:4', '$', json.dumps(json.loads(doc4_content)['category']))

    # Index multi flat values
    env.execute_command('FT.CREATE', 'idx_category_flat', 'ON', 'JSON', 'SCHEMA', '$.[*]', 'AS', 'category', 'TAG')
    # Index an array
    env.execute_command('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$', 'AS', 'category', 'TAG')
    # Index both multi flat values and an array
    env.execute_command('FT.CREATE', 'idx_category_arr_author_flat', 'ON', 'JSON', 'SCHEMA',
        '$.[*]', 'AS', 'author', 'TAG', # testing root path, so reuse the single top-level value
        '$', 'AS', 'category', 'TAG')
    
    waitForIndex(env, 'idx_category_flat')
    waitForIndex(env, 'idx_category_arr')
    waitForIndex(env, 'idx_category_arr_author_flat')
    
    searchMultiTagCategory(env)

def testMultiTagNested(env):
    """ test multiple TAG values at inner level (array of strings) """

    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', doc1_content)
    conn.execute_command('JSON.SET', 'doc:2', '$', doc2_content)
    conn.execute_command('JSON.SET', 'doc:3', '$', doc3_content)
    conn.execute_command('JSON.SET', 'doc:4', '$', doc4_content)

    # Index multi flat values
    env.execute_command('FT.CREATE', 'idx_category_flat', 'ON', 'JSON', 'SCHEMA', '$.category[*]', 'AS', 'category', 'TAG')
    env.execute_command('FT.CREATE', 'idx_author_flat', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors[*]', 'AS', 'author', 'TAG')
    # Index an array
    env.execute_command('FT.CREATE', 'idx_category_arr', 'ON', 'JSON', 'SCHEMA', '$.category', 'AS', 'category', 'TAG')
    # Index an array of arrays
    env.execute_command('FT.CREATE', 'idx_author_arr', 'ON', 'JSON', 'SCHEMA', '$.books[*].authors', 'AS', 'author', 'TAG')
    # Index both multi flat values and an array
    env.execute_command('FT.CREATE', 'idx_category_arr_author_flat', 'ON', 'JSON', 'SCHEMA',
        '$.books[*].authors[*]', 'AS', 'author', 'TAG',
        '$.category', 'AS', 'category', 'TAG')
    
    waitForIndex(env, 'idx_category_flat')
    waitForIndex(env, 'idx_author_flat')
    waitForIndex(env, 'idx_category_arr')
    waitForIndex(env, 'idx_author_arr')
    waitForIndex(env, 'idx_category_arr_author_flat')

    searchMultiTagCategory(env)
    searchMultiTagAuthor(env)

    env.execute_command('FT.CREATE', 'idx_book', 'ON', 'JSON', 'SCHEMA',
        '$.category', 'AS', 'category', 'TAG',
        '$.books[*].authors[*]', 'AS', 'author', 'TAG',
        '$.books[*].name', 'AS', 'name', 'TAG')
    waitForIndex(env, 'idx_book')
    res = env.execute_command('FT.SEARCH', 'idx_book',
        '(@name:{design*} -@category:{cloud}) | '
        '(@name:{Kubernetes*} @category:{cloud})',
        'NOCONTENT')
    env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:3', 'doc:1']), message='idx_book')

def searchMultiTagCategory(env):
    """ helper function for searching multi-value attributes """

    conn = getConnectionByEnv(env)

    for idx in ['idx_category_arr', 'idx_category_arr_author_flat']:
        env.debugPrint(idx, force=True)
        
        # Use toSortedFlatList when scores are not distinct (to succedd also with coordinaotr)
        res = env.execute_command('FT.SEARCH', idx, '@category:{database}', 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:1', 'doc:2']), message="A " + idx)
        
        res = env.execute_command('FT.SEARCH', idx, '@category:{performance}', 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([1, 'doc:3']), message="B " + idx)

        env.expect('FT.SEARCH', idx, '@category:{high\ performance}', 'NOCONTENT').equal([1, 'doc:2'])
        env.expect('FT.SEARCH', idx, '@category:{cloud}', 'NOCONTENT').equal([1, 'doc:3'])
    

def searchMultiTagAuthor(env):
    """ helper function for searching multi-value attributes """

    # Not indexing array of arrays
    env.assertEqual(int(index_info(env, 'idx_author_arr')['hash_indexing_failures']), 3)

    for idx in ['idx_author_flat']:
        env.debugPrint(idx, force=True)
        env.expect('FT.SEARCH', idx, '@author:{Donald\ Knuth}', 'NOCONTENT').equal([1, 'doc:1'])
        
        # Use toSortedFlatList when scores are not distinct (to succedd also with coordinaotr)
        res = env.execute_command('FT.SEARCH', idx, '@author:{Brendan*}', 'NOCONTENT')
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc:2', 'doc:3']))

        res = env.execute_command('FT.SEARCH', idx, '@author:{Redis*}', 'NOCONTENT')
        # Notice doc:4 is not in result (path `$.books[*].authors[*]` does not match a scalar string in `authors`)
        env.assertListEqual(toSortedFlatList(res), toSortedFlatList([3, 'doc:1', 'doc:2', 'doc:3']))

    env.expect('FT.SEARCH', 'idx_category_arr_author_flat', '@category:{programming}', 'NOCONTENT').equal([1, 'doc:1'])
    