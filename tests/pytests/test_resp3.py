from common import *
from math import nan
import json
from test_coordinator import test_error_propagation_from_shards
from test_profile import TimedoutTest_resp3, TimedOutWarningtestCoord

def order_dict(d):
    ''' Sorts a dictionary recursively by keys '''

    result = {}
    for k, v in sorted(d.items()):
        if isinstance(v, dict):
            result[k] = order_dict(v)
        else:
            result[k] = v
    return result

@skip(redis_less_than="7.0.0")
def test_search():
    env = Env(protocol=3)
    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    exp = {
      'attributes': [], 'warning': [], 'total_results': 2, 'format': 'STRING',
      'results': [
        {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]}
    env.expect('FT.search', 'idx1', "*", 'SCORER', 'TFIDF').equal(exp)

    # test withscores
    exp = {
      'attributes': [], 'warning': [], 'total_results': 2, 'format': 'STRING',
      'results': [
        { 'id': 'doc2',
          'score': [
            1.0, [
              'Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
              ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']
            ]
           ],
           'payload': None,
           'sortkey': None,
           'extra_attributes': {'f1': '3', 'f2': '2'},
           'values': []
        },
        { 'id': 'doc1',
          'score': [
            0.5, [
              'Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 2 / slop 1',
              ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']
            ]
          ],
          'payload': None,
          'sortkey': None,
          'extra_attributes': {'f1': '3', 'f2': '3'},
          'values': []
        }
      ]
    }
    if env.isCluster():
      # in 2.6 with RESP2, WITHSORTKEYS but without SORTBY does not return a null `sortey` with coordinator
      del exp['results'][0]['sortkey']
      del exp['results'][1]['sortkey']

    env.expect('FT.search', 'idx1', "*", "VERBATIM", 'SCORER', 'TFIDF', "WITHSCORES", "EXPLAINSCORE", "WITHPAYLOADS",
               "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "FORMAT", "STRING").equal(exp)

    # test with sortby
    exp = {
      'attributes': [], 'warning': [], 'total_results': 2, 'format': 'STRING',
      'results': [
        { 'id': 'doc1',
          'score': 0.5,
          'payload': None,
          'sortkey': '$3',
          'extra_attributes': {'f2': '3', 'f1': '3'},
          'values': []
        },
        { 'id': 'doc2',
          'score': 1.0,
          'payload': None,
          'sortkey': '$2',
          'extra_attributes': {'f2': '2', 'f1': '3'},
          'values': []
        }
      ]
    }
    env.expect('FT.search', 'idx1', "*", "VERBATIM", 'SCORER', 'TFIDF', "WITHSCORES", "WITHPAYLOADS", "WITHSORTKEYS",
               "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC", "FORMAT", "STRING").equal(exp)

    # test with limit 0 0
    exp = {'attributes': [], 'warning': [], 'total_results': 2, 'format': 'STRING', 'results': []}
    env.expect('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS",
               "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC", "LIMIT", 0, 0, "FORMAT", "STRING").equal(exp)

    # test without RETURN
    exp = {
      'attributes': [],
      'warning': [],
      'total_results': 2,
      'format': 'STRING',
      'results': [
        {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]
    }
    env.expect('FT.search', 'idx1', "*", 'SCORER', 'TFIDF').equal(exp)

@skip(redis_less_than="7.0.0")
def test_search_timeout():
    num_range = 1000
    env = Env(protocol=3, moduleArgs=f'DEFAULT_DIALECT 2 MAXPREFIXEXPANSIONS {num_range} TIMEOUT 1 ON_TIMEOUT FAIL')
    conn = getConnectionByEnv(env)

    env.cmd('ft.create', 'myIdx', 'schema', 't', 'TEXT', 'geo', 'GEO')
    for i in range(num_range):
        conn.execute_command('HSET', f'doc{i}', 't', f'aa{i}', 'geo', f"{i/10000},{i/1000}")

    # TODO: Add these tests again once MOD-5965 is merged
    # env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', str(num_range)). \
    #   contains('Timeout limit was reached')
    # env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', str(num_range), 'timeout', 1).\
    #   contains('Timeout limit was reached')

    # (coverage) Later failure than the above tests - in pipeline execution
    # phase. For this, we need more documents in the index, such that we will
    # fail for sure
    num_range_2 = 25000 * env.shardsCount
    p = conn.pipeline(transaction=False)
    for i in range(num_range, num_range_2):
      p.execute_command('HSET', f'doc{i}', 't', f'{i}', 'geo', f"{i/10000},{i/1000}")
    p.execute()

    env.expect(
      'FT.SEARCH', 'myIdx', '*', 'LIMIT', '0', str(num_range_2), 'TIMEOUT', '1'
    ).error().contains('Timeout limit was reached')

@skip(cluster=True, redis_less_than="7.0.0")
def test_profile(env):
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    # test with profile
    exp = {
      'Results': {
        'attributes': [], 'warning': [], 'total_results': 2, 'format': 'STRING',
        'results': [
          {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
          {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
        ],
      },
      'Profile': {
        'Shards': [{
          'Total profile time': ANY,
          'Parsing time': ANY,
          'Pipeline creation time': ANY,
          'Total GIL time': ANY,
          'Warning': 'None',
          'Iterators profile':
            {'Type': 'WILDCARD', 'Time': ANY, 'Number of reading operations': 2},
          'Result processors profile': [
            {'Type': 'Index',  'Time': ANY, 'Results processed': 2},
            {'Type': 'Scorer', 'Time': ANY, 'Results processed': 2},
            {'Type': 'Sorter', 'Time': ANY, 'Results processed': 2},
            {'Type': 'Loader', 'Time': ANY, 'Results processed': 2}
          ]
        }],
        'Coordinator': {}
      }
    }
    env.expect('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', '*', "FORMAT", "STRING", 'SCORER', 'TFIDF').equal(exp)

@skip(cluster=False, redis_less_than="7.0.0")
def test_coord_profile():
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    # test with profile
    exp = {
      'Results': {
        'attributes': [],
        'warning': [],
        'total_results': 2,
        'format': 'STRING',
        'results': [
          {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
          {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
        ],
      },
      'Profile': {
        'Shards': env.shardsCount * [
                      {'Total profile time': ANY, 'Parsing time': ANY, 'Pipeline creation time': ANY, 'Total GIL time': ANY, 'Warning': 'None',
                        'Iterators profile': {'Type': 'WILDCARD', 'Time': ANY, 'Number of reading operations': ANY},
                        'Result processors profile': [{'Type': 'Index', 'Time': ANY, 'Results processed': ANY},
                                                      {'Type': 'Scorer', 'Time': ANY, 'Results processed': ANY},
                                                      {'Type': 'Sorter', 'Time': ANY, 'Results processed': ANY},
                                                      {'Type': 'Loader', 'Time': ANY, 'Results processed': ANY}]}],
        'Coordinator': {'Total Coordinator time': ANY, 'Post Processing time': ANY},
      },
    }
    res = env.cmd('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', '*', 'FORMAT', 'STRING', 'SCORER', 'TFIDF')
    res['Results']['results'].sort(key=lambda x: x['extra_attributes'].get('f1', ''))
    env.assertEqual(res, exp)

    exp = {
      'Results': {
        'attributes': [],
        'warning': [],
        'total_results': 2,
        'format': 'STRING',
        'results': [
          {'extra_attributes': {}, 'values': []},
          {'extra_attributes': {}, 'values': []}
        ],
      },
      'Profile': {
        'Shards': ANY, # Checking separately. When profiling Aggregation, the number of shards is not fixed (empty replies are not returned)
        'Coordinator': {
          'Total profile time': ANY,
          'Parsing time': ANY,
          'Pipeline creation time': ANY,
          'Total GIL time': ANY,
          'Warning': 'None',
          'Result processors profile': [{'Type': 'Network', 'Time': ANY, 'Results processed': 2}]
        }
      }
    }
    shard = {
      'Total profile time': ANY,
      'Parsing time': ANY,
      'Pipeline creation time': ANY,
      'Total GIL time': ANY,
      'Warning': 'None',
      'Iterators profile': {'Type': 'WILDCARD', 'Time': ANY, 'Number of reading operations': ANY},
      'Result processors profile': [{'Type': 'Index', 'Time': ANY, 'Results processed': ANY},]
    }
    res = env.cmd('FT.PROFILE', 'idx1', 'AGGREGATE', 'QUERY', '*', 'FORMAT', 'STRING')
    env.assertEqual(res, exp)
    env.assertLessEqual(len(res['Profile']['Shards']), env.shardsCount)
    for shard_res in res['Profile']['Shards']:
      env.assertEqual(shard_res, shard)

@skip(redis_less_than="7.0.0")
def test_aggregate():
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    res = env.cmd('FT.aggregate', 'idx1', "*", "LOAD", 2, "f1", "f2", "FORMAT", "STRING")
    res['results'].sort(key=lambda x: "" if x['extra_attributes'].get('f2') == None else x['extra_attributes'].get('f2'))
    exp = {
      'attributes': [],
      'warning': [],
      'total_results': ANY,
      'format': 'STRING',
      'results': [
        {'extra_attributes': {}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '2'}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]
    }
    env.assertEqual(res, exp)

    res = env.cmd('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3", "FORMAT", "STRING")
    exp = {
      'attributes': [],
      'warning': [],
      'total_results': ANY,
      'format': 'STRING',
      'results': [
        {'extra_attributes': {}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]
    }
    res['results'].sort(key=lambda x: "" if x['extra_attributes'].get('f2') == None else x['extra_attributes'].get('f2'))
    env.assertEqual(res, exp)

    # test with sortby
    exp = {
      'attributes': [],
      'warning': [],
      'total_results': ANY,
      'format': 'STRING',
      'results': [
        {'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'extra_attributes': {}, 'values': []}
      ]
    }
    res = env.cmd('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3", "SORTBY", 2, "@f2", "DESC", "FORMAT", "STRING")
    env.assertEqual(res, exp)

@skip(redis_less_than="7.0.0")
def test_cursor():
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    docs = 3
    exp = {
      'attributes': [],
      'warning': [],
      'total_results': docs,
      'format': 'STRING',
      'results': [
        {'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]}
    res, cursor = env.cmd('FT.aggregate', 'idx1', '*', 'LOAD', 3, 'f1', 'f2', 'f3',
                          'SORTBY', 2, '@f2', 'DESC', 'WITHCURSOR', 'COUNT', 1)
    env.assertEqual(res, exp)

    exp = {
      'attributes': [], 'warning': [], 'total_results': docs, 'format': 'STRING',
      'results': [
          {'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []}
        ]}
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx1', cursor)
    env.assertEqual(res, exp)

    exp = {
      'attributes': [], 'warning': [], 'total_results': docs, 'format': 'STRING',
      'results': [{'extra_attributes': {}, 'values': []}]}
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx1', cursor)
    env.assertEqual(res, exp)

    exp = {'attributes': [], 'warning': [], 'total_results': docs, 'format': 'STRING', 'results': []}
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx1', cursor)
    env.assertEqual(res, exp)
    env.assertEqual(cursor, 0)

    env.cmd('FT.create', 'idx2', "PREFIX", 1, "folder",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx2')

    exp = {'attributes': [], 'warning': [], 'total_results': 0, 'format': 'STRING', 'results': []}
    res, cursor = env.cmd('FT.aggregate', 'idx2', '*', 'LOAD', 3, 'f1', 'f2', 'f3',
                          'SORTBY', 2, '@f2', 'DESC', 'WITHCURSOR', 'COUNT', 1)
    env.assertEqual(res, exp)
    env.assertEqual(cursor, 0)

@skip(redis_less_than="7.0.0")
def test_list():
    env = Env(protocol=3)

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    env.cmd('FT.create', 'idx2', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")
    env.expect('FT._LIST').equal(['idx2', 'idx1'])

@skip(redis_less_than="7.0.0")
def test_info():
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    exp = {
      'attributes': [{'WEIGHT': 1.0, 'attribute': 'f1', 'flags': [], 'identifier': 'f1', 'type': 'TEXT'},
                     {'WEIGHT': 1.0, 'attribute': 'f2', 'flags': [], 'identifier': 'f2', 'type': 'TEXT'}],
      'field statistics': [
                      {'attribute': 'f1',
                       'identifier': 'f1',
                       'Index Errors': {
                                        'indexing failures': 0,
                                        'last indexing error': 'N/A',
                                        'last indexing error key': 'N/A'
                                       }
                      },
                      {'attribute': 'f2',
                       'identifier': 'f2',
                       'Index Errors': {
                                        'indexing failures': 0,
                                        'last indexing error': 'N/A',
                                        'last indexing error key': 'N/A'
                                       }
                        }
                      ],
      'bytes_per_record_avg': ANY,
      'cleaning': 0,
      'cursor_stats': {'global_idle': 0, 'global_total': 0, 'index_capacity': ANY, 'index_total': 0},
      'dialect_stats': {'dialect_1': 0, 'dialect_2': 0, 'dialect_3': 0, 'dialect_4': 0},
      'doc_table_size_mb': ANY,
      'gc_stats': ANY,
      'hash_indexing_failures': 0,
      'index_definition': {'default_score': 1.0, 'key_type': 'HASH', 'prefixes': ['doc'] },
      'index_name': 'idx1',
      'index_options': [],
      'index_definition': {
          'default_score': 1.0,
          'key_type': 'HASH',
          'prefixes': ['doc'],
          'indexes_all': 'false'
        },
      'indexing': 0,
      'inverted_sz_mb': ANY,
      'key_table_size_mb': ANY,
      'tag_overhead_sz_mb': ANY,
      'text_overhead_sz_mb': ANY,
      'total_index_memory_sz_mb': ANY,
      'max_doc_id': ANY,
      'num_docs': 3,
      'num_records': 3,
      'num_terms': ANY,
      'number_of_uses': ANY,
      'offset_bits_per_record_avg': ANY,
      'offset_vectors_sz_mb': ANY,
      'offsets_per_term_avg': ANY,
      'percent_indexed': 1.0,
      'records_per_doc_avg': ANY,
      'sortable_values_size_mb': 0.0,
      'geoshapes_sz_mb': 0.0,
      'total_inverted_index_blocks': ANY,
      'vector_index_sz_mb': 0.0,
      'Index Errors': {
          'indexing failures': 0,
          'last indexing error': 'N/A',
          'last indexing error key': 'N/A',
          'background indexing status': 'OK',
          }
      }
    res = env.cmd('FT.info', 'idx1')
    res.pop('total_indexing_time', None)
    env.assertEqual(order_dict(res), order_dict(exp))

@skip(redis_less_than="7.0.0")
def test_config():
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    env.cmd('FT.create', 'idx2', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")

    res = env.cmd(config_cmd(), "SET", "TIMEOUT", 501)

    res = env.cmd(config_cmd(), "GET", "*")
    env.assertEqual(res['TIMEOUT'], '501')

    res = env.cmd(config_cmd(), "GET", "TIMEOUT")
    env.assertEqual(res, {'TIMEOUT': '501'})

    res = env.cmd(config_cmd(), "HELP", "TIMEOUT")
    env.assertEqual(res, {'TIMEOUT': {'Description': 'Query (search) timeout', 'Value': '501'}})

@skip(redis_less_than="7.0.0")
def test_dictdump():
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    env.cmd('FT.create', 'idx2', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")

    env.cmd("FT.DICTADD", "dict1", "foo", "1", "bar", "2")
    def sort_dict(dict_list):
        dict_list.sort()
        return dict_list
    env.expect("FT.DICTDUMP", "dict1").noError().apply(sort_dict).equal(['1', '2', 'bar', 'foo'])

def testSpellCheckIssue437():
    env = Env(protocol=3)
    env.cmd('ft.create', 'incidents', 'ON', 'HASH', 'SCHEMA', 'report', 'text')
    env.cmd('FT.DICTADD', 'slang', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')
    env.expect('FT.SPELLCHECK', 'incidents',
               'Tooni toque kerfuffle',
               'TERMS', 'EXCLUDE', 'slang',
               'TERMS', 'INCLUDE', 'slang').equal({ 'results': { 'tooni': [{'toonie': 0.0}] } })

def testSpellCheckOnExistingTerm(env):
    env = Env(protocol=3)
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TEXT', 'body', 'TEXT')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('hset', 'doc1', 'name', 'name', 'body', 'body1')
        r.execute_command('hset', 'doc2', 'name', 'name2', 'body', 'body2')
        r.execute_command('hset', 'doc3', 'name', 'name2', 'body', 'name2')
    waitForIndex(env, 'idx')
    env.expect('ft.spellcheck', 'idx', 'name').equal({'results': {}})

@skip(redis_less_than="7.0.0")
def test_spell_check():
    env = Env(protocol=3)

    env.cmd('ft.create', 'incidents', 'ON', 'HASH', 'SCHEMA', 'report', 'text')
    env.cmd('FT.DICTADD', 'dict1', 'timmies', 'toque', 'toonie', 'Toonif', 'serviette', 'kerfuffle', 'chesterfield')
    env.cmd('FT.DICTADD', 'dict2', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')

    exp = {
      'results': {
        'tooni':     [ {'Toonif': 0.0}, {'toonie': 0.0} ],
        'toque':     [ {'toque': 0.0} ],
        'kerfuffle': [ {'kerfuffle': 0.0} ]
      }
    }
    env.expect('FT.SPELLCHECK', 'incidents', 'Tooni toque kerfuffle', 'TERMS',
               'INCLUDE', 'dict1', 'dict2').equal(exp)

@skip(redis_less_than="7.0.0")
def test_syndump():
    env = Env(protocol=3)

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.expect('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring').ok()
    env.expect('ft.synupdate', 'idx', 'id2', 'baby', 'child').ok()
    env.expect('ft.synupdate', 'idx', 'id3', 'tree', 'wood').ok()
    exp = {
      'baby': ['id2'], 'wood': ['id3'], 'boy': ['id1'],
      'tree': ['id3'], 'child': ['id1', 'id2'], 'offspring': ['id1']}
    env.expect('ft.syndump', 'idx').equal(exp)

@skip(redis_less_than="7.0.0")
def test_tagvals():
    env = Env(protocol=3)

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TAG", "f2", "TAG", "f5", "TAG")
    waitForIndex(env, 'idx1')
    env.expect('FT.TAGVALS', 'idx1', 'f1').equal(['3'])
    env.expect('FT.TAGVALS', 'idx1', 'f2').equal(['2', '3'])
    env.expect('FT.TAGVALS', 'idx1', 'f5').equal([])

@skip(cluster=False)
def test_clusterinfo():
    env = Env(protocol=3)
    verify_shard_init(env)

    generic_shard = {
      'slots': ANY,
      'host': '127.0.0.1',
      'id': ANY,
      'port': ANY,
    }
    exp = {
      'cluster_type': 'redis_oss',
      'num_partitions': env.shardsCount,
      'shards': [generic_shard] * env.shardsCount
    }
    res = env.cmd('SEARCH.CLUSTERINFO')
    env.assertEqual(order_dict(res), order_dict(exp))

def test_profile_crash_mod5323():
    env = Env(protocol=3)
    env.cmd("FT.CREATE", "idx", "SCHEMA", "t", "TEXT")
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command("HSET", "1", "t", "hello")
        r.execute_command("HSET", "2", "t", "hell")
        r.execute_command("HSET", "3", "t", "help")
        r.execute_command("HSET", "4", "t", "helowa")
    waitForIndex(env, 'idx')

    res = env.cmd("FT.PROFILE", "idx", "SEARCH", "LIMITED", "QUERY", "%hell% hel*", "NOCONTENT") # codespell:ignore hel
    exp = {
      'Results': {
        'warning': [],
        'attributes': [],
        'results': [
          {'values': [], 'id': '1'},
          {'values': [], 'id': '2'},
          {'values': [], 'id': '3'}],
        'total_results': 3,
        'format': 'STRING',
      },
      'Profile': {
        'Shards': [{
          'Iterators profile':
            { 'Child iterators': [
              { 'Child iterators': 'The number of iterators in the union is 3',
                'Number of reading operations': 3,
                'Query type': 'FUZZY - hell',
                'Time': ANY,
                'Type': 'UNION'
                },
                { 'Child iterators': 'The number of iterators in the union is 4',
                  'Number of reading operations': 3,
                  'Query type': 'PREFIX - hel',
                  'Time': ANY,
                  'Type': 'UNION'
                }
              ],
              'Number of reading operations': 3,
              'Time': ANY,
              'Type': 'INTERSECT'
            },
          'Parsing time': ANY,
          'Pipeline creation time': ANY,
          'Total GIL time': ANY,
          'Warning': 'None',
          'Result processors profile': [
            { 'Results processed': 3, 'Time': ANY, 'Type': 'Index' },
            { 'Results processed': 3, 'Time': ANY, 'Type': 'Scorer' },
            { 'Results processed': 3, 'Time': ANY, 'Type': 'Sorter' }
          ],
          'Total profile time': ANY
        }],
        'Coordinator': {},
       },
    }
    if not env.isCluster():  # on cluster, lack of crash is enough
        env.assertEqual(res, exp)

def test_profile_child_itrerators_array():
    env = Env(protocol=3)
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('hset', '1', 't', 'hello')
      r.execute_command('hset', '2', 't', 'world')

    # test UNION
    res = env.cmd('ft.profile', 'idx', 'search', 'query', 'hello|world', 'nocontent')
    exp = {
      'Results': {
        'warning': [],
        'attributes': [],
        'results': [
          { 'values': [], 'id': '1' },
          { 'values': [], 'id': '2' }
        ],
        'total_results': 2,
        'format': 'STRING',
      },
      'Profile': {
        'Shards': [{
          'Iterators profile':
            { 'Child iterators': [
                {'Number of reading operations': 1, 'Estimated number of matches': 1, 'Term': 'hello', 'Time': ANY, 'Type': 'TEXT'},
                {'Number of reading operations': 1, 'Estimated number of matches': 1, 'Term': 'world', 'Time': ANY, 'Type': 'TEXT'}
              ],
              'Number of reading operations': 2,
              'Query type': 'UNION',
              'Time': ANY,
              'Type': 'UNION'
            },
          'Parsing time': ANY,
          'Pipeline creation time': ANY,
          'Total GIL time': ANY,
          'Warning': 'None',
          'Result processors profile': [
            {'Results processed': 2, 'Time': ANY, 'Type': 'Index'},
            {'Results processed': 2, 'Time': ANY, 'Type': 'Scorer'},
            {'Results processed': 2, 'Time': ANY, 'Type': 'Sorter'}
          ],
          'Total profile time': ANY
        }],
        'Coordinator': {},
      },
    }
    if not env.isCluster():  # on cluster, lack of crash is enough
        env.assertEqual(res, exp)

    # test INTERSECT
    res = env.cmd('ft.profile', 'idx', 'search', 'query', 'hello world', 'nocontent')
    exp = {
      'Results': {
        'warning': [],
        'attributes': [],
        'results': [],
        'total_results': 0,
        'format': 'STRING',
      },
      'Profile': {
        'Shards': [{
          'Iterators profile':
            { 'Child iterators': [
                {'Number of reading operations': 1, 'Estimated number of matches': 1, 'Term': 'hello', 'Time': ANY, 'Type': 'TEXT'},
                {'Number of reading operations': 1, 'Estimated number of matches': 1, 'Term': 'world', 'Time': ANY, 'Type': 'TEXT'}
              ],
              'Number of reading operations': 0,
              'Time': ANY,
              'Type': 'INTERSECT'
            },
          'Parsing time': ANY,
          'Pipeline creation time': ANY,
          'Total GIL time': ANY,
          'Warning': 'None',
          'Result processors profile': [
            { 'Results processed': 0, 'Time': ANY, 'Type': 'Index'},
            { 'Results processed': 0, 'Time': ANY, 'Type': 'Scorer'},
            {'Results processed': 0, 'Time': ANY, 'Type': 'Sorter'}
          ],
          'Total profile time': ANY
        }],
        'Coordinator': {},
      },
    }
    if not env.isCluster():  # on cluster, lack of crash is enough
        env.assertEqual(res, exp)

@skip(no_json=True)
def testExpandErrorsResp3():
  env = Env(protocol=3)
  # On JSON
  env.cmd('ft.create', 'idx', 'on', 'json', 'SCHEMA', '$.arr', 'as', 'arr', 'numeric')
  env.expect('FT.SEARCH', 'idx', '*', 'FORMAT').error().contains('Need an argument for FORMAT')
  env.expect('FT.SEARCH', 'idx', '*', 'FORMAT', 'XPAND').error().contains('FORMAT XPAND is not supported')
  env.expect('FT.AGGREGATE', 'idx', '*', 'FORMAT').error().contains('Need an argument for FORMAT')
  env.expect('FT.AGGREGATE', 'idx', '*', 'FORMAT', 'XPAND').error().contains('FORMAT XPAND is not supported')

  env.expect('FT.SEARCH', 'idx', '*', 'FORMAT', 'EXPAND', 'DIALECT', 2).error().contains('requires dialect 3 or greater')

  # On HASH
  env.cmd('ft.create', 'idx2', 'on', 'hash', 'SCHEMA', '$.arr', 'as', 'arr', 'numeric')
  env.expect('FT.SEARCH', 'idx2', '*', 'FORMAT', 'EXPAND').error().contains('EXPAND format is only supported with JSON')

  env.expect(
    'FT.AGGREGATE', 'idx2', '*', 'FORMAT', 'EXPAND'
  ).error().contains('EXPAND format is only supported with JSON')

@skip(no_json=True)
def testExpandErrorsResp2():
  env = Env(protocol=2)
  env.cmd('ft.create', 'idx', 'on', 'json', 'SCHEMA', '$.arr', 'as', 'arr', 'numeric')
  env.expect('FT.SEARCH', 'idx', '*', 'FORMAT', 'EXPAND').error().contains('EXPAND format is only supported with RESP3')

  env.expect(
    'FT.AGGREGATE', 'idx', '*', 'FORMAT', 'EXPAND'
  ).error().contains('EXPAND format is only supported with RESP3')

  # On HASH
  env.cmd('ft.create', 'idx2', 'on', 'hash', 'SCHEMA', 'num', 'numeric', 'str', 'text')
  env.expect('FT.SEARCH', 'idx2', '*', 'FORMAT', 'EXPAND').error().contains('EXPAND format is only supported with RESP3')

  env.expect(
    'FT.AGGREGATE', 'idx2', '*', 'FORMAT', 'EXPAND'
  ).error().contains('EXPAND format is only supported with RESP3')

@skip(no_json=True)
def testExpandJson():
  ''' Test returning values for JSON in expanded format (raw RESP3 instead of stringified JSON) '''
  env = Env(protocol=3)
  env.cmd('ft.create', 'idx', 'on', 'json', 'SCHEMA',
          '$.arr', 'as', 'arr','numeric',
          '$.num', 'as', 'num', 'numeric',
          '$.str', 'as', 'str', 'text',
          '$..arr[*]', 'as', 'multi', 'numeric')

  doc1 = {
     "arr":[1.0,2.1,3.14],
     "num":1,
     "str":"foo",
     "sub":{"s1":False},
     "sub2":{"arr":[10,20,33.33]},
     "empty_arr":[],
     "empty_obj":{}
  }
  doc1_js = json.dumps(doc1, separators=(',',':'))

  doc2 = {
    "arr":[3,4,None],
    "num":2,
    "str":"bar",
    "sub":{"s2":True},
    "sub2":{"arr":[40,50,66.66]},
    "empty_arr":[],
    "empty_obj":{}
  }
  doc2_js = json.dumps(doc2, separators=(',',':'))

  doc3 = {
     "arr":[5,6,7],
     "num":3,
     "str":"baaz",
     "sub":{"s3":False},
     "sub2":{"arr":[70,80,99.99]},
     "empty_arr":[],
     "empty_obj":{}
  }
  doc3_js = json.dumps(doc3, separators=(',',':'))

  with env.getClusterConnectionIfNeeded() as r:
    r.execute_command('json.set', 'doc1', '$', doc1_js)
    r.execute_command('json.set', 'doc2', '$', doc2_js)
    r.execute_command('json.set', 'doc3', '$', doc3_js)

  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'num': '1', '$': doc1_js}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'num': '2','$': doc2_js}, 'values': []},
    ]
  }
  exp_expand = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'EXPAND',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'num': [1], '$': [doc1]}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'num': [2], '$': [doc2]}, 'values': []},
    ]
  }
  # Default FORMAT is STRING

  # Test FT.SEARCH
  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2, 'SORTBY', 'num')
  env.assertEqual(res, exp_string)

  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2, 'FORMAT', 'EXPAND', 'SORTBY', 'num')
  env.assertEqual(res, exp_expand)

  res = env.cmd('FT.SEARCH', 'idx', '*','LIMIT', 0, 2, 'FORMAT', 'STRING', 'SORTBY', 'num')
  env.assertEqual(res, exp_string)

  # Test FT.AGGREAGTE
  del exp_expand['results'][0]['id']
  del exp_expand['results'][1]['id']

  del exp_string['results'][0]['id']
  del exp_string['results'][1]['id']

  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'LOAD', '*', 'SORTBY', 2, '@num', 'ASC')
  env.assertEqual(res, exp_string)

  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'FORMAT', 'EXPAND', 'LOAD', '*', 'SORTBY', 2, '@num', 'ASC')
  env.assertEqual(res, exp_expand)

  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'FORMAT', 'STRING', 'LOAD', '*', 'SORTBY', 2, '@num', 'ASC')
  env.assertEqual(res, exp_string)

  #
  # Return specific fields
  #

  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'$.arr[?(@>2)]': '[2.1,3.14]', 'str': '["foo"]', 'multi': '[1.0,2.1,3.14,10,20,33.33]', "arr":'[[1.0,2.1,3.14]]'}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'$.arr[?(@>2)]': '[3,4]', 'str': '["bar"]', 'multi': '[3,4,null,40,50,66.66]', "arr": '[[3,4,null]]'}, 'values': []},
    ]
  }

  exp_string_default_dialect = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'$.arr[?(@>2)]': '2.1', 'str': 'foo', 'multi': '1', "arr":'[1.0,2.1,3.14]'}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'$.arr[?(@>2)]': '3', 'str': 'bar', 'multi': '3', "arr": '[3,4,null]'}, 'values': []},
    ]
  }

  exp_expand_default_dialect = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'EXPAND',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'$.arr[?(@>2)]':2.1, 'str':'foo', 'multi': 1, "arr":[1, 2.1, 3.14]}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'$.arr[?(@>2)]':3, 'str':'bar', 'multi': 3, "arr":[3, 4, None]}, 'values': []},
    ]
  }

  exp_expand = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'EXPAND',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'$.arr[?(@>2)]':[2.1, 3.14], 'str':['foo'], 'multi': [1, 2.1, 3.14, 10, 20, 33.33], "arr":[[1, 2.1, 3.14]]}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'$.arr[?(@>2)]':[3, 4], 'str':['bar'], 'multi': [3, 4, None, 40, 50, 66.66], "arr":[[3, 4, None]]}, 'values': []},
    ]
  }

  load_args = [6, '$.arr[?(@>2)]', 'str', 'multi', 'arr', 'empty_arr', 'empty_obj']

  # Test FT.SEARCH
  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2, 'FORMAT', 'EXPAND', 'RETURN', *load_args, 'DIALECT', 3)
  env.assertEqual(res, exp_expand)

  # Default FORMAT is STRING
  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2, 'RETURN', *load_args)
  env.assertEqual(res, exp_string_default_dialect)

  # Default FORMAT is STRING
  # Add DIALECT 3 to get multi values as with EXPAND
  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2, 'RETURN', *load_args, 'DIALECT', 3)
  env.assertEqual(res, exp_string)

  # Add DIALECT 3 to get multi values as with EXPAND
  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2, 'FORMAT', 'STRING', 'RETURN', *load_args, 'DIALECT', 3)
  env.assertEqual(res, exp_string)

  # Test FT.AGGREAGTE
  del exp_expand['results'][0]['id']
  del exp_expand['results'][1]['id']

  del exp_string_default_dialect['results'][0]['id']
  del exp_string_default_dialect['results'][1]['id']

  del exp_string['results'][0]['id']
  del exp_string['results'][1]['id']

  # Default FORMAT is STRING
  # Add DIALECT 3 to get multi values as with EXPAND
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'LOAD', *load_args, 'SORTBY', 2, '@str', 'DESC', 'DIALECT', 3)
  env.assertEqual(res, exp_string)

  # Default FORMAT is STRING
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'LOAD', *load_args, 'SORTBY', 2, '@str', 'DESC')
  env.assertEqual(res, exp_string_default_dialect)


  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'FORMAT', 'EXPAND', 'LOAD', *load_args, 'SORTBY', 2, '@str', 'DESC')
  env.assertEqual(res, exp_expand)

  # Add DIALECT 3 to get multi values as with EXPAND
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'FORMAT', 'STRING', 'LOAD', *load_args, 'SORTBY', 2, '@str', 'DESC', 'DIALECT', 3)
  env.assertEqual(res, exp_string)

def testExpandHash():
  ''' Test returning values for HASH in stringified format (not expanded RESP3)'''
  env = Env(protocol=3)
  env.cmd('ft.create', 'idx', 'on', 'hash', 'SCHEMA', 'num', 'numeric', 'str', 'text', 't', 'tag')

  with env.getClusterConnectionIfNeeded() as r:
    r.execute_command('hset', 'doc1', 'num', 1, 'str', 'foo', 'other', 'fu')
    r.execute_command('hset', 'doc2', 'num', 2, 'str', 'bar', 'other', 'bur')
    r.execute_command('hset', 'doc3', 'num', 3 ,'str', 'baz', 'other', 'buz')

  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'num': '1', 'str': 'foo', 'other': 'fu'}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'num': '2', 'str': 'bar', 'other': 'bur'}, 'values': []},
    ]
  }

  # Default FORMAT is STRING

  # Test FT.SEARCH
  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2)
  # Unflake test if score is zero and docid is same (zero) on shards
  res['results'].sort(key=lambda x: "" if x['extra_attributes'].get('num') == None else x['extra_attributes'].get('num'))
  env.assertEqual(res, exp_string)

  res = env.cmd('FT.SEARCH', 'idx', '*','LIMIT', 0, 2, 'FORMAT', 'STRING')
  # Unflake test if score is zero and docid is same (zero) on shards
  res['results'].sort(key=lambda x: "" if x['extra_attributes'].get('num') == None else x['extra_attributes'].get('num'))
  env.assertEqual(res, exp_string)

  # Test FT.AGGREGATE
  del exp_string['results'][0]['id']
  del exp_string['results'][1]['id']
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'SORTBY', 2, '@num', 'ASC', 'LOAD', '*')
  env.assertEqual(res, exp_string)


  #
  # Return specific fields
  #
  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'num': '1', 'other': 'fu'}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'num': '2', 'other': 'bur'}, 'values': []},
    ]
  }

  # Test FT.SEARCH
  res = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 2, 'RETURN', 2, 'num', 'other')
  env.assertEqual(res, exp_string)

  # Test FT.AGGREGATE
  del exp_string['results'][0]['id']
  del exp_string['results'][1]['id']
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 2, 'LOAD', 2, 'num', 'other', 'SORTBY', 2, '@num', 'ASC')
  env.assertEqual(res, exp_string)


@skip(no_json=True)
def testExpandJsonVector():
  ''' Test returning values for VECTOR in expanded format (raw RESP3 instead of stringified JSON) '''
  env = Env(protocol=3, moduleArgs='DEFAULT_DIALECT 2')
  conn = getConnectionByEnv(env)
  conn.execute_command('FT.CREATE', 'idx', 'ON', 'JSON',
                      'SCHEMA', '$.v', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3','DISTANCE_METRIC', 'L2',
                      '$.num', 'AS', 'num', 'NUMERIC')

  doc1_content = {
     "v":[1,2,3],
     "num":1
  }
  # json string format
  doc1_content_js = json.dumps(doc1_content, separators=(',', ':'))

  doc2_content = {
     "v":[4,2,0],
     "num":2
  }
  # json string format
  doc2_content_js = json.dumps(doc2_content, separators=(',', ':'))

  with env.getClusterConnectionIfNeeded() as r:

    r.execute_command('json.set', 'doc1', '$', doc1_content_js)
    r.execute_command('json.set', 'doc2', '$', doc2_content_js)

  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'__vec_score': '6.70958423615', '$': doc1_content_js}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'__vec_score': '12.7095842361', '$': doc2_content_js}, 'values': []}
    ]
  }

  exp_expand = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'EXPAND',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'__vec_score': 6.7095842361450195, '$': [doc1_content]}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'__vec_score': 12.70958423614502, '$': [doc2_content]}, 'values': []},
    ]
  }

  cmd = ['FT.SEARCH', 'idx', '*=>[KNN 2 @vec $B]', 'PARAMS', '2', 'B', '????????????']

  res = env.cmd(*cmd, 'FORMAT', 'STRING')
  env.assertEqual(res, exp_string)

  # Default FORMAT is STRING
  res = env.cmd(*cmd)
  env.assertEqual(res, exp_string)

  res = env.cmd(*cmd, 'FORMAT', 'EXPAND')
  env.assertEqual(res, exp_expand)

  # Test without WITHSORTKEYS

  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'__vec_score': '6.70958423615', "num": "1", '$': doc1_content_js}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'__vec_score': '12.7095842361', "num": "2", '$': doc2_content_js}, 'values': []}
    ]
  }

  exp_expand = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'EXPAND',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'__vec_score': 6.7095842361450195, 'num': [1], '$': [doc1_content]}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'__vec_score': 12.70958423614502, 'num': [2], '$': [doc2_content]}, 'values': []},
    ]
  }

  cmd = ['FT.SEARCH', 'idx', '*=>[KNN 2 @vec $B]', 'PARAMS', '2', 'B', '????????????', 'SORTBY', 'num', 'ASC']

  res = env.cmd(*cmd, 'FORMAT', 'STRING')
  env.assertEqual(res, exp_string)

  # Default FORMAT is STRING
  res = env.cmd(*cmd)
  env.assertEqual(res, exp_string)

  res = env.cmd(*cmd, 'FORMAT', 'EXPAND')
  env.assertEqual(res, exp_expand)

  # Test with WITHSORTKEYS

  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'sortkey': '#1', 'extra_attributes': {'__vec_score': '6.70958423615', "num": "1", '$': doc1_content_js}, 'values': []},
      {'id': 'doc2', 'sortkey': '#2', 'extra_attributes': {'__vec_score': '12.7095842361', "num": "2", '$': doc2_content_js}, 'values': []}
    ]
  }

  exp_expand = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'EXPAND',
    'results': [
      {'id': 'doc1', 'sortkey': '#1', 'extra_attributes': {'__vec_score': 6.7095842361450195, 'num': [1], '$': [doc1_content]}, 'values': []},
      {'id': 'doc2', 'sortkey': '#2', 'extra_attributes': {'__vec_score': 12.70958423614502, 'num': [2], '$': [doc2_content]}, 'values': []},
    ]
  }

  cmd = [*cmd, 'WITHSORTKEYS']

  res = env.cmd(*cmd, 'FORMAT', 'STRING')
  env.assertEqual(res, exp_string)

  # Default FORMAT is STRING
  res = env.cmd(*cmd)
  env.assertEqual(res, exp_string)

  res = env.cmd(*cmd, 'FORMAT', 'EXPAND')
  env.assertEqual(res, exp_expand)

  #
  # Return specific field
  #
  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'STRING',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'$': doc1_content_js}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'$': doc2_content_js}, 'values': []},
    ]
  }
  exp_expand = {
    'attributes': [],
    'warning': [],
    'total_results': 2,
    'format': 'EXPAND',
    'results': [
      {'id': 'doc1', 'extra_attributes': {'$': [doc1_content]}, 'values': []},
      {'id': 'doc2', 'extra_attributes': {'$': [doc2_content]}, 'values': []},
    ]
  }

  cmd = ['FT.SEARCH', 'idx', '*=>[KNN 2 @vec $B]', 'PARAMS', '2', 'B', '????????????', 'RETURN', '1', '$']

  res = env.cmd(*cmd, 'FORMAT', 'STRING')
  env.assertEqual(res, exp_string)

  # Default FORMAT is STRING
  res = env.cmd(*cmd)
  env.assertEqual(res, exp_string)

  res = env.cmd(*cmd, 'FORMAT', 'EXPAND')
  env.assertEqual(res, exp_expand)

  #
  # Test FT.AGGREGATE
  #
  exp_string = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'STRING',
    'results': [
      {'extra_attributes': {'__vec_score': '6.70958423615', '$': doc1_content_js, "num": "1", 'num_pow': '1'}, 'values': []},
      {'extra_attributes': {'__vec_score': '12.7095842361', '$': doc2_content_js, "num": "2", 'num_pow': '8'}, 'values': []}
    ]
  }

  exp_expand = {
    'attributes': [],
    'warning': [],
    'total_results': ANY,
    'format': 'EXPAND',
    'results': [
      {'extra_attributes': {'__vec_score': 6.7095842361450195, '$': [doc1_content], 'num': [1], 'num_pow': 1}, 'values': []},
      {'extra_attributes': {'__vec_score': 12.70958423614502, '$': [doc2_content], 'num': [2], 'num_pow': 8}, 'values': []},
    ]
  }
  cmd = ['FT.AGGREGATE', 'idx', '*=>[KNN 2 @vec $B]', 'PARAMS', '2', 'B', '????????????', 'LOAD', '2', '$', '@num', 'APPLY', '@num^3', 'AS', 'num_pow', 'SORTBY', 2, '@num_pow', 'ASC']

  res = env.cmd(*cmd, 'FORMAT', 'STRING')
  env.assertEqual(res, exp_string)

  # Default FORMAT is STRING
  res = env.cmd(*cmd)
  env.assertEqual(res, exp_string)

  res = env.cmd(*cmd, 'FORMAT', 'EXPAND')
  env.assertEqual(res, exp_expand)

def test_ft_info():
    env = Env(protocol=3)
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
    with env.getClusterConnectionIfNeeded() as r:
      nodes = 1
      if env.isCluster():
         res = r.execute_command("cluster info")
         nodes = float(res['cluster_known_nodes'])

      # Initial size = sizeof(DocTable) + (INITIAL_DOC_TABLE_SIZE * sizeof(DMDChain *))
      #              = 72 + (1000 * 16) = 16072 bytes
      initial_doc_table_size_mb = 16072 / (1024 * 1024)
      # Size of an empty TrieMap
      key_table_sz_mb = 24 / (1024 * 1024)
      total_index_memory_sz_mb = initial_doc_table_size_mb + key_table_sz_mb

      res = order_dict(r.execute_command('ft.info', 'idx'))

      exp = {
        'attributes': [
          { 'WEIGHT': 1.0,
            'attribute': 't',
            'flags': [],
            'identifier': 't',
            'type': 'TEXT'
          }
        ],
        'field statistics' :[
            {
              'identifier': 't',
              'attribute': 't',
              'Index Errors': {
                  'indexing failures': 0,
                  'last indexing error': 'N/A',
                  'last indexing error key': 'N/A'
              }
            }
        ],
        'bytes_per_record_avg': nan,
        'cleaning': 0,
        'cursor_stats': {
          'global_idle': 0,
          'global_total': 0,
          'index_capacity': ANY,
          'index_total': 0
        },
        'dialect_stats': {
          'dialect_1': 0,
          'dialect_2': 0,
          'dialect_3': 0,
          'dialect_4': 0
        },
        'doc_table_size_mb': initial_doc_table_size_mb,
        'gc_stats': {
          'average_cycle_time_ms': nan,
          'bytes_collected': 0.0,
          'gc_blocks_denied': 0.0,
          'gc_numeric_trees_missed': 0.0,
          'last_run_time_ms': 0.0,
          'total_cycles': 0.0,
          'total_ms_run': 0.0
        },
        'hash_indexing_failures': 0.0,
        'index_definition': {
          'default_score': 1.0,
          'key_type': 'HASH',
          'prefixes': [''],
          'indexes_all': 'false'
        },
        'index_name': 'idx',
        'index_options': [],
        'indexing': 0.0,
        'inverted_sz_mb': 0.0,
        'key_table_size_mb': key_table_sz_mb,
        'tag_overhead_sz_mb': 0.0,
        'text_overhead_sz_mb': 0.0,
        'total_index_memory_sz_mb': total_index_memory_sz_mb,
        'max_doc_id': 0.0,
        'num_docs': 0.0,
        'num_records': 0.0,
        'num_terms': 0.0,
        'number_of_uses': 1,
        'offset_bits_per_record_avg': nan,
        'offset_vectors_sz_mb': 0.0,
        'offsets_per_term_avg': nan,
        'percent_indexed': 1.0,
        'records_per_doc_avg': nan,
        'sortable_values_size_mb': 0.0,
        'geoshapes_sz_mb': 0.0,
        'total_indexing_time': 0.0,
        'total_inverted_index_blocks': 0.0,
        'vector_index_sz_mb': 0.0,
        'Index Errors': {
              'indexing failures': 0,
              'last indexing error': 'N/A',
              'last indexing error key': 'N/A',
              'background indexing status': 'OK',
        }
      }

      exp_cluster = {
        'attributes': [
          { 'WEIGHT': 1.0,
            'attribute': 't',
            'flags': [],
            'identifier': 't',
            'type': 'TEXT'
          }
        ],
        'field statistics' :[
            {
              'identifier': 't',
              'attribute': 't',
              'Index Errors': {
                  'indexing failures': 0,
                  'last indexing error': 'N/A',
                  'last indexing error key': 'N/A'
              }
            }
        ],
        'bytes_per_record_avg': nan,
        'cleaning': 0,
        'cursor_stats': {
          'global_idle': 0,
          'global_total': 0,
          'index_capacity': ANY,
          'index_total': 0
        },
        'dialect_stats': {'dialect_1': 0,
                          'dialect_2': 0,
                          'dialect_3': 0,
                          'dialect_4': 0},
        'doc_table_size_mb': nodes * initial_doc_table_size_mb,
        'gc_stats': {
              'average_cycle_time_ms': 0.0,
              'bytes_collected': 0.0,
              'gc_blocks_denied': 0.0,
              'gc_numeric_trees_missed': 0.0,
              'last_run_time_ms': 0.0,
              'total_cycles': 0.0,
              'total_ms_run': 0.0
        },
        'hash_indexing_failures': 0,
        'index_definition': {
          'default_score': 1.0,
          'key_type': 'HASH',
          'prefixes': [''],
          'indexes_all': 'false'
          },
        'index_name': 'idx',
        'index_options': [],
        'indexing': 0,
        'inverted_sz_mb': 0.0,
        'key_table_size_mb': nodes * key_table_sz_mb,
        'tag_overhead_sz_mb': 0.0,
        'text_overhead_sz_mb': 0.0,
        'total_index_memory_sz_mb': nodes * total_index_memory_sz_mb,
        'max_doc_id': 0,
        'num_docs': 0,
        'num_records': 0,
        'num_terms': 0,
        'number_of_uses': 1,
        'offset_bits_per_record_avg': nan,
        'offset_vectors_sz_mb': 0.0,
        'offsets_per_term_avg': nan,
        'percent_indexed': 1.0,
        'records_per_doc_avg': nan,
        'sortable_values_size_mb': 0.0,
        'geoshapes_sz_mb': 0.0,
        'total_inverted_index_blocks': 0,
        'vector_index_sz_mb': 0.0,
        'Index Errors': {
              'indexing failures': 0,
              'last indexing error': 'N/A',
              'last indexing error key': 'N/A',
              'background indexing status': 'OK',
        }
      }

      env.assertEqual(dict_diff(res, exp_cluster if env.isCluster() else exp), {})

def test_vecsim_1():
    env = Env(protocol=3)
    env.cmd("ft.create", "vecsimidx0", "prefix", "1", "docvecsimidx0z", "schema", "vector_FLAT", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2")
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command("HSET", "docvecsimidx0z0", "vector_FLAT", np.array([0.0, 0.0], dtype=np.float32).tobytes())
        r.execute_command("HSET", "docvecsimidx0z1", "vector_FLAT", np.array([1.0, 1.0], dtype=np.float32).tobytes())
        r.execute_command("HSET", "docvecsimidx0z2", "vector_FLAT", np.array([2.0, 2.0], dtype=np.float32).tobytes())
        r.execute_command("HSET", "docvecsimidx0z3", "vector_FLAT", np.array([3.0, 3.0], dtype=np.float32).tobytes())
    exp3 = { 'attributes': [],
             'warning': [],
             'total_results': 4,
             'format': 'STRING',
             'results': [
                 { 'id': 'docvecsimidx0z0',
                   # 'sortkey': None,
                   'values': []
                 },
                 { 'id': 'docvecsimidx0z1',
                   # 'sortkey': None,
                   'values': []
                 },
                 { 'id': 'docvecsimidx0z2',
                   # 'sortkey': None,
                   'values': []
                 },
                 { 'id': 'docvecsimidx0z3',
                   # 'sortkey': None,
                   'values': []
                 }
             ]
           }
    exp2 = [3, 'docvecsimidx0z0', 'docvecsimidx0z1', 'docvecsimidx0z2', 'docvecsimidx0z3']
    res = env.cmd("FT.SEARCH", "vecsimidx0", "(*)=>[KNN 4 @vector_FLAT $BLOB]", "NOCONTENT", "SORTBY",
               "__vector_FLAT_score", "ASC", "DIALECT", "2", "LIMIT", "0", "4",
               "params", "2", "BLOB", "\x00\x00\x00\x00\x00\x00\x00\x00")
    env.assertEqual(dict_diff(res, exp3 if env.protocol == 3 else exp2, show=True,
                    exclude_regex_paths=[r"\['sortkey'\]"]), {})

def test_error_propagation_from_shards_resp3():
    env = Env(protocol=3)
    test_error_propagation_from_shards(env)

@skip(cluster=True)
def testTimedOutWarning_resp3():
  env = Env(protocol=3)
  TimedoutTest_resp3(env)

@skip(asan=True, msan=True, cluster=False)
def testTimedOutWarningCoord_resp3():
   env = Env(protocol=3)
   TimedOutWarningtestCoord(env)

def test_error_with_partial_results():
  """Test that we get 'warnings' with partial results on non-strict timeout
  policy"""

  env = Env(protocol=3)
  conn = getConnectionByEnv(env)

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Populate the index
  num_docs = 25000 * env.shardsCount
  for i in range(num_docs):
      conn.execute_command('HSET', f'doc{i}', 't', str(i))

  # `FT.AGGREGATE`
  res = runDebugQueryCommandTimeoutAfterN(env,
    ['FT.AGGREGATE', 'idx', '*'],
    timeout_res_count=3,
  )
  # Assert that we got results
  env.assertGreater(len(res['results']), 0)

  # Assert that we got a warning
  VerifyTimeoutWarningResp3(env, res)

  # `FT.SEARCH`
  res = runDebugQueryCommandTimeoutAfterN(env,
    ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', str(num_docs)],
    timeout_res_count=3,
  )

  # Assert that we got results
  env.assertGreater(len(res['results']), 0)
  # Assert that we got a warning
  VerifyTimeoutWarningResp3(env, res)

def test_warning_maxprefixexpansions():
  env = Env(protocol=3, moduleArgs='DEFAULT_DIALECT 2')
  conn = env.getClusterConnectionIfNeeded()
  env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 't2', 'TAG')

  # Add documents to ONE OF THE SHARDS ONLY, such that MAXPREFIXEXPANSIONS will
  # be reached only on that shard (others are empty)
  # (This configuration is enforced on the shard level, thus every shard may
  # expand a term up to `MAXPREFIXEXPANSIONS` times)
  conn.execute_command('HSET', 'doc1{3}', 't', 'foo', 't2', 'foo')

  populated_shard_conn = env.getConnectionByKey('doc1{3}', 'HSET')

  # Set `MAXPREFIXEXPANSIONS` to 1
  populated_shard_conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

  # Test that we don't throw an warning in case the amount of expansions is
  # exactly the threshold (1)
  # ------------------------------ FT.SEARCH -----------------------------------
  # TEXT
  res = env.cmd('FT.SEARCH', 'idx', 'fo*', 'nocontent')
  env.assertEqual(res['total_results'], 1)
  env.assertEqual(res['results'], [{'id': 'doc1{3}', 'values': []}])
  env.assertEqual(res['warning'], [])
  # TAG
  res = env.cmd('FT.SEARCH', 'idx', '@t2:{fo*}', 'nocontent') # codespell:ignore
  env.assertEqual(res['total_results'], 1)
  env.assertEqual(res['results'], [{'id': 'doc1{3}', 'values': []}])
  env.assertEqual(res['warning'], [])

  # Add another document
  conn.execute_command('HSET', 'doc2{3}', 't', 'fooo', 't2', 'fooo')

  # ------------------------------ FT.AGGREGATE -----------------------------------
  # TEXT
  res = env.cmd('FT.AGGREGATE', 'idx', 'fo*', 'load', '*')
  env.assertEqual(res['total_results'], 1)
  env.assertEqual(res['results'], [{'extra_attributes': {'t': 'foo', 't2': 'foo'}, 'values': []}])
  env.assertEqual(res['warning'], ['Max prefix expansions limit was reached'])
  # TAG
  res = env.cmd('FT.AGGREGATE', 'idx', '@t2:{fo*}', 'load', '*') # codespell:ignore fo
  env.assertEqual(res['total_results'], 1)
  env.assertEqual(res['results'], [{'extra_attributes': {'t': 'foo', 't2': 'foo'}, 'values': []}])
  env.assertEqual(res['warning'], ['Max prefix expansions limit was reached'])

  # ------------------------------- All results --------------------------------
  # Set `MAXPREFIXEXPANSIONS` to 10
  populated_shard_conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '10')

  res = env.cmd('FT.SEARCH', 'idx', 'fo*', 'nocontent')
  env.assertEqual(res['total_results'], 2)
  env.assertEqual(res['results'], [{'id': 'doc1{3}', 'values': []}, {'id': 'doc2{3}', 'values': []}])
  env.assertEqual(res['warning'], [])

  # -------------------------------- FT.PROFILE --------------------------------
  # Check the FT.PROFILE response. Specifically the shard warnings
  # Set `MAXPREFIXEXPANSIONS` to 1
  populated_shard_conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'fo*')

  # Check that we have a warning in the response, and a warning in each shard
  env.assertEqual(res['Results']['warning'], ['Max prefix expansions limit was reached'])
  n_warnings = 0
  for i, shard in enumerate(res['Profile']['Shards']):
      if shard['Warning']== 'Max prefix expansions limit was reached':
         n_warnings += 1
  env.assertEqual(n_warnings, 1)

  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', 'fo*')

  # Check that we have a warning in the response, and a warning in one shard only
  env.assertEqual(res['Results']['warning'], ['Max prefix expansions limit was reached'])
  n_warnings = 0
  for i, shard in enumerate(res['Profile']['Shards']):
    if shard['Warning']== 'Max prefix expansions limit was reached':
         n_warnings += 1
  env.assertEqual(n_warnings, 1)

# TODO: `total_results` is currently not  on cluster - to be fixed in MOD-9094
@skip(cluster=True)
def test_totalResults_aggregate():
  """Tests that the `total_results` field on `FT.AGGREGATE` is correct when
  using the RESP3 protocol"""

  env = Env(protocol=3, moduleArgs='DEFAULT_DIALECT 2')
  conn = env.getClusterConnectionIfNeeded()

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Populate the index
  n_docs = 15 * env.shardsCount
  for i in range(n_docs):
      conn.execute_command('HSET', f'doc{i}', 't', str(i))

  # Test that the `total_results` field is correct
  res = env.cmd('FT.AGGREGATE', 'idx', '*')
  env.assertEqual(res['total_results'], n_docs)

  # Test the `total_results` field for a cursor
  res, cid = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', '5')
  while cid:
    env.assertEqual(res['total_results'], n_docs)
    res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', cid)

  # Cursor is depleted.
  env.assertEqual(res['total_results'], n_docs)
  env.assertEqual(res['results'], [])
