from common import *
from unittest.mock import ANY
import operator

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

    expected = {
      'fields_names': [], 'error': [], 'total_results': 2,
      'results': [
        {'id': 'doc2', 'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []},
        {'id': 'doc1', 'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}
      ]}
    env.expect('FT.search', 'idx1', "*").equal(expected)

    # test withscores
    expected = {
      'fields_names': [], 'error': [], 'total_results': 2,
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
           'fields': {'f1': '3', 'f2': '2'},
           'fields_values': []
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
          'fields': {'f1': '3', 'f2': '3'},
          'fields_values': []
        }
      ]
    }
    env.expect('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "EXPLAINSCORE", "WITHPAYLOADS",
               "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2').equal(expected)

    # test with sortby
    expected = {
      'fields_names': [], 'error': [], 'total_results': 2,
      'results': [
        { 'id': 'doc1',
          'score': 0.5,
          'payload': None,
          'sortkey': '$3',
          'fields': {'f2': '3', 'f1': '3'},
          'fields_values': []
        },
        { 'id': 'doc2',
          'score': 1.0,
          'payload': None,
          'sortkey': '$2',
          'fields': {'f2': '2', 'f1': '3'},
          'fields_values': []
        }
      ]
    }
    env.expect('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS", "WITHSORTKEYS",
               "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC").equal(expected)

    # test with limit 0 0
    expected = {'fields_names': [], 'error': [], 'total_results': 2, 'results': []}
    env.expect('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS",
               "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC", "LIMIT", 0, 0).equal(expected)

    # test without RETURN
    expected = \
      {'fields_names': [],
       'error': [],
       'total_results': 2,
       'results': [
         {'id': 'doc2', 'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []},
         {'id': 'doc1', 'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}
       ]
      }
    res = env.cmd('FT.search', 'idx1', "*")

    # test with profile
    expected = \
      {'fields_names': [], 'error': [], 'total_results': 2, 'results':
       [{'id': 'doc2', 'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []},
        {'id': 'doc1', 'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}],
        'profile':
        [['Total profile time', ANY], ['Parsing time', ANY],
         ['Pipeline creation time', ANY],
         ['Iterators profile', ['Type', 'WILDCARD', 'Time', ANY, 'Counter', 2]],
         ['Result processors profile', ['Type', 'Index', 'Time', ANY, 'Counter', 2],
          ['Type', 'Scorer', 'Time', ANY, 'Counter', 2],
          ['Type', 'Sorter', 'Time', ANY, 'Counter', 2],
          ['Type', 'Loader', 'Time', ANY, 'Counter', 2]]]}
    env.expect('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', "*").equal(expected)

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
    res['results'].sort(key=lambda x: "" if x['fields'].get('f2') == None else x['fields'].get('f2'))
    expected = \
      { 'fields_names': [],
        'error': [],
        'total_results': 1,
        'results': [
          {'fields': {}, 'fields_values': []},
          {'fields': {'f1': '3', 'f2': '2'}, 'fields_values': []},
          {'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}
        ]
       }
    env.assertEqual(res, expected)

    res = env.execute_command('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3")
    expected = \
      { 'fields_names': [],
        'error': [],
        'total_results': 1,
        'results': [
          {'fields': {}, 'fields_values': []},
          {'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []},
          {'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}
        ]
      }
    res['results'].sort(key=lambda x: "" if x['fields'].get('f2') == None else x['fields'].get('f2'))
    env.assertEqual(res, expected)

    # test with sortby
    expected = \
      { 'fields_names': [],
        'error': [],
        'total_results': 3,
        'results': [
          {'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []},
          {'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []},
          {'fields': {}, 'fields_values': []}
        ]
      }
    res = env.execute_command('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3", "SORTBY", 2, "@f2", "DESC")
    env.assertEqual(res, expected)

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

    expected = {
      'fields_names': [],
      'error': [],
      'total_results': 3,
      'results': [
        {'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}
      ],
      'cursor': ANY}
    res = env.cmd('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3",
                  "SORTBY", 2, "@f2", "DESC", "WITHCURSOR", 'COUNT', 1)
    env.assertEqual(res, expected)

    expected = {
      'fields_names': [], 'error': [], 'total_results': 0,
      'results': [
          {'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []}
        ],
        'cursor': ANY}
    res = env.cmd('FT.CURSOR', 'READ', 'idx1', res['cursor'])
    env.assertEqual(res, expected)

    expected = {
      'fields_names': [], 'error': [], 'total_results': 0,
      'results': [{'fields': {}, 'fields_values': []}],
      'cursor': ANY}
    res = env.cmd('FT.CURSOR', 'READ', 'idx1', res['cursor'])
    env.assertEqual(res, expected)

    expected = {'fields_names': [], 'error': [], 'total_results': 0, 'results': [], 'cursor': 0}
    res = env.cmd('FT.CURSOR', 'READ', 'idx1', res['cursor'])
    env.assertEqual(res, expected)

def test_list():
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
    res = env.execute_command('FT.info', 'idx1')

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

    res = env.execute_command("FT.CONFIG", "SET", "TIMEOUT", 501)

    res = env.execute_command("FT.CONFIG", "GET", "*")
    env.assertEqual(res['TIMEOUT'], {'Value': '501'})

    res = env.execute_command("FT.CONFIG", "GET", "TIMEOUT")
    env.assertEqual(res, {'TIMEOUT': {'Value': '501'}})

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

def test_spell_check():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    env.cmd('ft.create', 'incidents', 'ON', 'HASH', 'SCHEMA', 'report', 'text')
    env.cmd('FT.DICTADD', 'dict1', 'timmies', 'toque', 'toonie', 'Toonif', 'serviette', 'kerfuffle', 'chesterfield')
    env.cmd('FT.DICTADD', 'dict2', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')

    expected = [
      0, {
        'tooni': [{'Toonif': 0.0}, {'toonie': 0.0}]
      }
    ]
    env.expect('FT.SPELLCHECK', 'incidents', 'Tooni toque kerfuffle', 'TERMS',
               'INCLUDE', 'dict1', 'dict2', 'FULLSCOREINFO').equal(expected)

    expected = [
      {'tooni': [{'Toonif': 0.0}, {'toonie': 0.0}]}
    ]
    env.expect('FT.SPELLCHECK', 'incidents', 'Tooni toque kerfuffle', 'TERMS',
               'INCLUDE', 'dict1', 'dict2').equal(expected)

def test_syndump():
    env = Env(protocol=3)
    if should_skip(env):
        env.skip()

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    env.expect('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring').ok()
    env.expect('ft.synupdate', 'idx', 'id2', 'baby', 'child').ok()
    env.expect('ft.synupdate', 'idx', 'id3', 'tree', 'wood').ok()
    expected = {'baby': ['id2'], 'wood': ['id3'], 'boy': ['id1'],
                'tree': ['id3'], 'child': ['id1', 'id2'], 'offspring': ['id1']}
    env.expect('ft.syndump', 'idx').equal(expected)

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
