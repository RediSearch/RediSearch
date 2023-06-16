from common import *
import operator


def order_dict(d):
    ''' Sorts a dictionary recursively by keys '''

    result = {}
    for k, v in sorted(d.items()):
        if isinstance(v, dict):
            result[k] = order_dict(v)
        else:
            result[k] = v
    return result

def redis_version(con, is_cluster=False):
    res = con.execute_command('INFO')
    ver = ""
    if is_cluster:
        try:
            ver = list(res.values())[0]['redis_version']
        except:
            ver = res['redis_version']
    else:
        ver = res['redis_version']
    return version.parse(ver)

def should_skip(env):
    r_ver = redis_version(env)
    if r_ver < version.parse("7.0.0"):
        return True
    with env.getClusterConnectionIfNeeded() as r:
        r_ver = redis_version(r)
        if r_ver < version.parse("7.0.0"):
            return True
    return False

def test_search():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    exp = {
      'attributes': [], 'error': [], 'total_results': 2,
      'results': [
        {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]}
    env.expect('FT.search', 'idx1', "*").equal(exp)

    # test withscores
    exp = {
      'attributes': [], 'error': [], 'total_results': 2,
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

    env.expect('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "EXPLAINSCORE", "WITHPAYLOADS",
               "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2').equal(exp)

    # test with sortby
    exp = {
      'attributes': [], 'error': [], 'total_results': 2,
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
    env.expect('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS", "WITHSORTKEYS",
               "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC").equal(exp)

    # test with limit 0 0
    exp = {'attributes': [], 'error': [], 'total_results': 2, 'results': []}
    env.expect('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS",
               "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC", "LIMIT", 0, 0).equal(exp)

    # test without RETURN
    exp = {
      'attributes': [],
      'error': [],
      'total_results': 2,
      'results': [
        {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]
    }
    env.expect('FT.search', 'idx1', "*").equal(exp)

def test_search_timeout():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()
    env.skipOnCluster()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    # test with timeout
    num_range = 1000
    env.cmd('ft.config', 'set', 'timeout', '1')
    env.cmd('ft.config', 'set', 'maxprefixexpansions', num_range)
    env.cmd('ft.create', 'myIdx', 'schema', 't', 'TEXT', 'geo', 'GEO')
    for i in range(num_range):
        env.cmd('HSET', f'doc{i}', 't', f'aa{i}', 'geo', f"{i/10000},{i/1000}")

    env.expect('ft.config', 'set', 'on_timeout', 'fail').ok()
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', '0'). \
      contains('Timeout limit was reached')
    env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'timeout', 1).\
      error().contains('Timeout limit was reached')

@skip(cluster=True)
def test_profile(env):
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    # test with profile
    exp = {
      'attributes': [], 'error': [], 'total_results': 2,
      'results': [
        {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ],
      'profile': {
        'Total profile time': ANY,
        'Parsing time': ANY,
        'Pipeline creation time': ANY,
        'Iterators profile': [
          {'Type': 'WILDCARD', 'Time': ANY, 'Counter': 2}
        ],
        'Result processors profile': [
          {'Type': 'Index',  'Time': ANY, 'Counter': 2},
          {'Type': 'Scorer', 'Time': ANY, 'Counter': 2},
          {'Type': 'Sorter', 'Time': ANY, 'Counter': 2},
          {'Type': 'Loader', 'Time': ANY, 'Counter': 2}
        ]
      }
    }
    env.expect('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', '*').equal(exp)

def test_coord_profile():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()
    if not env.isCluster() or env.shardsCount != 3:
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    # test with profile
    exp = {
        'attributes': [],
        'error': [],
        'total_results': 2,
        'results': [
          {'id': 'doc2', 'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
          {'id': 'doc1', 'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
        ],
        'shards':
        {'Shard #1': {'Total profile time': ANY, 'Parsing time': ANY, 'Pipeline creation time': ANY,
                      'Iterators profile': [{'Type': 'WILDCARD', 'Time': ANY, 'Counter': ANY}],
                      'Result processors profile': [{'Type': 'Index', 'Time': ANY, 'Counter': ANY},
                                                    {'Type': 'Scorer', 'Time': ANY, 'Counter': ANY},
                                                    {'Type': 'Sorter', 'Time': ANY, 'Counter': ANY},
                                                    {'Type': 'Loader', 'Time': ANY, 'Counter': ANY}]},
        'Shard #2': {'Total profile time': ANY, 'Parsing time': ANY, 'Pipeline creation time': ANY,
                     'Iterators profile': [{'Type': 'WILDCARD', 'Time': ANY, 'Counter': ANY}],
                     'Result processors profile': [{'Type': 'Index', 'Time': ANY, 'Counter': ANY},
                                                   {'Type': 'Scorer', 'Time': ANY, 'Counter': ANY},
                                                   {'Type': 'Sorter', 'Time': ANY, 'Counter': ANY},
                                                   {'Type': 'Loader', 'Time': ANY, 'Counter': ANY}]},
        'Shard #3': {'Total profile time': ANY, 'Parsing time': ANY, 'Pipeline creation time': ANY,
                     'Iterators profile': [{'Type': 'WILDCARD', 'Time': ANY, 'Counter': ANY}],
                     'Result processors profile': [{'Type': 'Index', 'Time': ANY, 'Counter': ANY},
                                                   {'Type': 'Scorer', 'Time': ANY, 'Counter': ANY},
                                                   {'Type': 'Sorter', 'Time': ANY, 'Counter': ANY},
                                                   {'Type': 'Loader', 'Time': ANY, 'Counter': ANY}]},
        'Coordinator': {'Total Coordinator time': ANY, 'Post Proccessing time': ANY}}}
    res = env.cmd('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', '*')
    res['results'].sort(key=lambda x: "" if x['extra_attributes'].get('f1') == None else x['extra_attributes']['f1'])
    env.assertEqual(res, exp)

def test_aggregate():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    res = env.cmd('FT.aggregate', 'idx1', "*", "LOAD", 2, "f1", "f2")
    res['results'].sort(key=lambda x: "" if x['extra_attributes'].get('f2') == None else x['extra_attributes'].get('f2'))
    exp = {
      'attributes': [],
      'error': [],
      'total_results': ANY,
      'results': [
        {'extra_attributes': {}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '2'}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]
    }
    env.assertEqual(res, exp)

    res = env.execute_command('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3")
    exp = {
      'attributes': [],
      'error': [],
      'total_results': ANY,
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
      'error': [],
      'total_results': ANY,
      'results': [
        {'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []},
        {'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []},
        {'extra_attributes': {}, 'values': []}
      ]
    }
    res = env.execute_command('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3", "SORTBY", 2, "@f2", "DESC")
    env.assertEqual(res, exp)

def test_cursor():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    exp = {
      'attributes': [],
      'error': [],
      'total_results': 3,
      'results': [
        {'extra_attributes': {'f1': '3', 'f2': '3'}, 'values': []}
      ]}
    res, cursor = env.cmd('FT.aggregate', 'idx1', '*', 'LOAD', 3, 'f1', 'f2', 'f3',
                          'SORTBY', 2, '@f2', 'DESC', 'WITHCURSOR', 'COUNT', 1)
    env.assertEqual(res, exp)

    exp = {
      'attributes': [], 'error': [], 'total_results': 0,
      'results': [
          {'extra_attributes': {'f1': '3', 'f2': '2', 'f3': '4'}, 'values': []}
        ]}
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx1', cursor)
    env.assertEqual(res, exp)

    exp = {
      'attributes': [], 'error': [], 'total_results': 0,
      'results': [{'extra_attributes': {}, 'values': []}]}
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx1', cursor)
    env.assertEqual(res, exp)

    exp = {'attributes': [], 'error': [], 'total_results': 0, 'results': []}
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx1', cursor)
    env.assertEqual(res, exp)
    env.assertEqual(cursor, 0)

    env.cmd('FT.create', 'idx2', "PREFIX", 1, "folder",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx2')

    exp = {'attributes': [], 'error': [], 'total_results': 0, 'results': []}
    res, cursor = env.cmd('FT.aggregate', 'idx2', '*', 'LOAD', 3, 'f1', 'f2', 'f3',
                          'SORTBY', 2, '@f2', 'DESC', 'WITHCURSOR', 'COUNT', 1)
    env.assertEqual(res, exp)
    env.assertEqual(cursor, 0)

def test_list():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    env.cmd('FT.create', 'idx2', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")
    env.expect('FT._LIST').equal({'idx2', 'idx1'})

def test_info():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    waitForIndex(env, 'idx1')

    exp = {
      'attributes': [{'WEIGHT': 1.0, 'attribute': 'f1', 'flags': [], 'identifier': 'f1', 'type': 'TEXT'},
                     {'WEIGHT': 1.0, 'attribute': 'f2', 'flags': [], 'identifier': 'f2', 'type': 'TEXT'}],
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
      'indexing': 0,
      'inverted_sz_mb': ANY,
      'key_table_size_mb': ANY,
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
      'total_geometries_index_size_mb': 0.0,
      'total_inverted_index_blocks': ANY,
      'vector_index_sz_mb': 0.0}
    res = env.cmd('FT.info', 'idx1')
    res.pop('total_indexing_time', None)
    env.assertEqual(order_dict(res), order_dict(exp))

def test_config():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    env.execute_command('FT.create', 'idx2', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")

    if env.isCluster():
        return

    res = env.execute_command("FT.CONFIG", "SET", "TIMEOUT", 501)

    res = env.execute_command("FT.CONFIG", "GET", "*")
    env.assertEqual(res['TIMEOUT'], '501')

    res = env.execute_command("FT.CONFIG", "GET", "TIMEOUT")
    env.assertEqual(res, {'TIMEOUT': '501'})

    res = env.execute_command("FT.CONFIG", "HELP", "TIMEOUT")
    env.assertEqual(res, {'TIMEOUT': {'Description': 'Query (search) timeout', 'Value': '501'}})

def test_dictdump():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
      r.execute_command('HSET', 'doc3', 'f5', '4')

    env.cmd('FT.create', 'idx1', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
    env.cmd('FT.create', 'idx2', "PREFIX", 1, "doc",
            "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")

    env.cmd("FT.DICTADD", "dict1", "foo", "1", "bar", "2")
    env.expect("FT.DICTDUMP", "dict1").equal({'2', 'bar', 'foo', '1'})

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

def test_spell_check():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

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

def test_syndump():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.expect('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring').ok()
    env.expect('ft.synupdate', 'idx', 'id2', 'baby', 'child').ok()
    env.expect('ft.synupdate', 'idx', 'id3', 'tree', 'wood').ok()
    exp = {
      'baby': ['id2'], 'wood': ['id3'], 'boy': ['id1'],
      'tree': ['id3'], 'child': ['id1', 'id2'], 'offspring': ['id1']}
    env.expect('ft.syndump', 'idx').equal(exp)

def test_tagvals():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
      r.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')

    env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                        "SCHEMA", "f1", "TAG", "f2", "TAG", "f5", "TAG")
    waitForIndex(env, 'idx1')

    env.expect('FT.TAGVALS', 'idx1', 'f1').equal({'3'})
    env.expect('FT.TAGVALS', 'idx1', 'f2').equal({'2', '3'})
    env.expect('FT.TAGVALS', 'idx1', 'f5').equal(set())

def test_clusterinfo(env):
    if not env.isCluster() or env.shardsCount != 3:
        env.skip()
    env = Env(protocol=3)
    exp = {
      'cluster_type': 'redis_oss',
      'hash_func': 'CRC16',
      'num_partitions': 3,
      'num_slots': 16384,
      'slots': [
        { 'end': 5461,
          'nodes': [
            { 'host': '127.0.0.1',
              'id': ANY,
              'port': ANY,
              'role': 'master self'
            }
          ],
          'start': 0
        },
        { 'end': 10923,
          'nodes': [
            {'host': '127.0.0.1',
             'id': ANY,
             'port': ANY,
             'role': 'master '}
          ],
          'start': 5462
        },
        { 'end': 16383,
          'nodes': [
            { 'host': '127.0.0.1',
              'id': ANY,
              'port': ANY,
              'role': 'master '
            }
          ],
          'start': 10924
        }
      ]
    }
    res = env.cmd('SEARCH.CLUSTERINFO')
    res['slots'].sort(key=lambda x: x['start'])
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

    res = env.cmd("FT.PROFILE", "idx", "SEARCH", "LIMITED", "QUERY", "%hell% hel*", "NOCONTENT")
    exp = {
      'error': [],
      'attributes': [],
      'profile': {
        'Iterators profile': [
          { 'Child iterators': [
             { 'Child iterators': 'The number of iterators in the union is 3',
               'Counter': 3,
               'Query type': 'FUZZY - hell',
               'Time': ANY,
               'Type': 'UNION'
              },
              { 'Child iterators': 'The number of iterators in the union is 4',
                'Counter': 3,
                'Query type': 'PREFIX - hel',
                'Time': ANY,
                'Type': 'UNION'
              }
            ],
            'Counter': 3,
            'Time': ANY,
            'Type': 'INTERSECT'
          }
        ],
        'Parsing time': ANY,
        'Pipeline creation time': ANY,
        'Result processors profile': [
          { 'Counter': 3, 'Time': ANY, 'Type': 'Index' },
          { 'Counter': 3, 'Time': ANY, 'Type': 'Scorer' },
          { 'Counter': 3, 'Time': ANY, 'Type': 'Sorter' }
        ],
        'Total profile time': ANY
       },
       'results': [
         {'values': [], 'id': '1'},
         {'values': [], 'id': '2'},
         {'values': [], 'id': '3'}],
       'total_results': 3
    }
    if not env.isCluster:  # on cluster, lack of crash is enough
        env.assertEqual(res, exp)

def test_profile_child_itrerators_array():
    env = Env(protocol=3)
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
    with env.getClusterConnectionIfNeeded() as r:
      r.execute_command('hset', '1', 't', 'hello')
      r.execute_command('hset', '2', 't', 'world')

    # test UNION
    res = env.execute_command('ft.profile', 'idx', 'search', 'query', 'hello|world', 'nocontent')
    exp = {
      'error': [],
      'attributes': [],
      'profile': {
        'Iterators profile': [
          { 'Child iterators': [
              {'Counter': 1, 'Size': 1, 'Term': 'hello', 'Time': ANY, 'Type': 'TEXT'},
              {'Counter': 1, 'Size': 1, 'Term': 'world', 'Time': ANY, 'Type': 'TEXT'}
            ],
            'Counter': 2,
            'Query type': 'UNION',
            'Time': ANY,
            'Type': 'UNION'
          }
        ],
        'Parsing time': ANY,
        'Pipeline creation time': ANY,
        'Result processors profile': [
          {'Counter': 2, 'Time': ANY, 'Type': 'Index'},
          {'Counter': 2, 'Time': ANY, 'Type': 'Scorer'},
          {'Counter': 2, 'Time': ANY, 'Type': 'Sorter'}
        ],
        'Total profile time': ANY
      },
      'results': [
        { 'values': [], 'id': '1' },
        { 'values': [], 'id': '2' }
      ],
      'total_results': 2
    }
    if not env.isCluster:  # on cluster, lack of crash is enough
        env.assertEqual(res, exp)

    # test INTERSECT
    res = env.execute_command('ft.profile', 'idx', 'search', 'query', 'hello world', 'nocontent')
    exp = {
      'error': [],
      'attributes': [],
      'profile': {
        'Iterators profile': [
          { 'Child iterators': [
              {'Counter': 1, 'Size': 1, 'Term': 'hello', 'Time': 0.0, 'Type': 'TEXT'},
              {'Counter': 1, 'Size': 1, 'Term': 'world', 'Time': 0.0, 'Type': 'TEXT'}
            ],
            'Counter': 0,
            'Time': 0.0,
            'Type': 'INTERSECT'
          }
        ],
        'Parsing time': 0.0,
        'Pipeline creation time': 0.0,
        'Result processors profile': [
          { 'Counter': 0, 'Time': 0.0, 'Type': 'Index'},
          { 'Counter': 0, 'Time': 0.0, 'Type': 'Scorer'},
          {'Counter': 0, 'Time': 0.0, 'Type': 'Sorter'}
        ],
        'Total profile time': 0.0
      },
      'results': [],
      'total_results': 0
    }
    if not env.isCluster:  # on cluster, lack of crash is enough
        env.assertEqual(res, exp)
