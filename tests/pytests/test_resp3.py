from includes import *
from common import *
from unittest.mock import ANY
import operator
from pprint import pprint


def test_1():
    env = Env(protocol=2)
    env.cmd('SEARCH.CLUSTERINFO')

def test_2(env):
    BB()
    env = Env(protocol=3)
    env.cmd('FT.CREATE', 'doc', 'PREFIX', 1, 'doc:', 'SCHEMA', 'name', 'TEXT')
    res = env.cmd('ft.info', 'doc')
    pprint(res)
    print('done')

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
    # if r_ver >= version.parse("7.0.0"):
    #     env.skipOnCluster() # TODO: remove when redis-py is fixed
    with env.getClusterConnectionIfNeeded() as r:
        r_ver = redis_version(r)
        if r_ver < version.parse("7.0.0"):
            return True

    env.cmd('FLUSHALL')
    return False

class testResp3():
    def __init__(self):
        self.env = Env(protocol=3)

    def test_search(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
        waitForIndex(env, 'idx1')

        env.expect('FT.search', 'idx1', "*").equal(\
                   {'fields_names': [], 'error': [], 'total_results': 2,
                    'results':
                      [{'id': 'doc2', 'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []},
                       {'id': 'doc1', 'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}]})

        # test withscores
        res = env.execute_command('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "EXPLAINSCORE", "WITHPAYLOADS", "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2')
        assert res == {'fields_names': [], 'error': [], 'total_results': 2, 'results':
         [{'id': 'doc2',
           'score':
           [1.0, ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 1 / slop 1',
                  ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]],
                  'payload': None, 'sortkey': None, 'fields': {'f1': '3', 'f2': '2'}, 'fields_values': []
            },
            {'id': 'doc1', 'score':
             [0.5, ['Final TFIDF : words TFIDF 1.00 * document score 1.00 / norm 2 / slop 1',
                    ['(TFIDF 1.00 = Weight 1.00 * Frequency 1)']]],
                     'payload': None, 'sortkey': None, 'fields': {'f1': '3', 'f2': '3'},
                     'fields_values': []}]}

        # test with sortby
        res = env.execute_command('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS", "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC")
        assert res == {'fields_names': [], 'error': [], 'total_results': 2, 'results':
                       [{'id': 'doc1', 'score': 0.5, 'payload': None, 'sortkey': '$3',
                         'fields': {'f2': '3', 'f1': '3'}, 'fields_values': []},
                         {'id': 'doc2', 'score': 1.0, 'payload': None, 'sortkey': '$2',
                          'fields': {'f2': '2', 'f1': '3'}, 'fields_values': []}]}

        # test with limit 0 0
        res = env.execute_command('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS", "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC", "LIMIT", 0, 0)
        assert res == {'fields_names': [], 'error': [], 'total_results': 2, 'results': []}

        # test without RETURN
        res = env.execute_command('FT.search', 'idx1', "*")
        assert res == {'fields_names': [], 'error': [], 'total_results': 2,
                       'results':
                       [{'id': 'doc2', 'fields': {'f1': '3', 'f2': '2', 'f3': '4'},
                         'fields_values': []}, {'id': 'doc1', 'fields': {'f1': '3', 'f2': '3'},
                                                'fields_values': []}]}

        # test with profile
        res = env.cmd('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', "*")
        assert res == \
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

        # test with timeout
        num_range = 1000
        env.cmd('ft.config', 'set', 'timeout', '1')
        env.cmd('ft.config', 'set', 'maxprefixexpansions', num_range)
        env.cmd('ft.create', 'myIdx', 'schema', 't', 'TEXT', 'geo', 'GEO')
        for i in range(num_range):
            env.expect('HSET', 'doc%d'%i, 't', 'aa' + str(i), 'geo', str(i/10000) + ',' + str(i/1000))

        env.expect('ft.config', 'set', 'on_timeout', 'fail').ok()
        env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'limit', '0', '0') \
        .contains('Timeout limit was reached')
        env.expect('ft.search', 'myIdx', 'aa*|aa*|aa*|aa* aa*', 'timeout', 1)    \
        .error().contains('Timeout limit was reached')

    def test_aggregate(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('HSET', 'doc3', 'f5', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT")

        waitForIndex(env, 'idx1')

        res = env.execute_command('FT.aggregate', 'idx1', "*", "LOAD", 2, "f1", "f2")

        res['results'].sort(key=lambda x: "" if x['fields'].get('f2') == None else x['fields'].get('f2'))
        assert res == {'fields_names': [], 'error': [], 'total_results': 1, 
                       'results': [{'fields': {}, 'fields_values': []}, 
                                   {'fields': {'f1': '3', 'f2': '2'}, 'fields_values': []}, 
                                   {'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}]}

        res = env.execute_command('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3")
        res['results'].sort(key=lambda x: "" if x['fields'].get('f2') == None else x['fields'].get('f2'))
        assert res == {'fields_names': [], 'error': [], 'total_results': 1, 'results': 
                       [{'fields': {}, 'fields_values': []}, 
                        {'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []}, 
                        {'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}]}

        # test with sortby
        res = env.execute_command('FT.aggregate', 'idx1', "*", "LOAD", 3, "f1", "f2", "f3", "SORTBY", 2, "@f2", "DESC")
        assert res == {'fields_names': [], 'error': [], 'total_results': 3, 
                       'results': [
                           {'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}, 
                           {'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []}, 
                           {'fields': {}, 'fields_values': []}]}

    def test_list(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('HSET', 'doc3', 'f5', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
        env.execute_command('FT.create', 'idx2', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")
        env.expect('FT._LIST').equal({'idx2', 'idx1'})

    def test_info(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('HSET', 'doc3', 'f5', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
        res = env.execute_command('FT.info', 'idx1')

    def test_config(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('HSET', 'doc3', 'f5', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
        env.execute_command('FT.create', 'idx2', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")

        res = env.execute_command("FT.CONFIG", "GET", "*")
        assert res['TIMEOUT'] == {'Value': '0'}

        res = env.execute_command("FT.CONFIG", "GET", "TIMEOUT")
        assert res == {'TIMEOUT': {'Value': '0'}}

        res = env.execute_command("FT.CONFIG", "HELP", "TIMEOUT")
        assert res == {'TIMEOUT': {'Description': 'Query (search) timeout', 'Value': '0'}}

    def test_dictdump(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('HSET', 'doc3', 'f5', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
        env.execute_command('FT.create', 'idx2', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT", "f3", "TEXT")

        env.execute_command("FT.DICTADD", "dict1", "foo", "1", "bar", "2")
        res = env.execute_command("FT.DICTDUMP", "dict1")
        assert res == {'2', 'bar', 'foo', '1'}

    def test_spellcheck(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.cmd('ft.create', 'incidents', 'ON', 'HASH', 'SCHEMA', 'report', 'text')
        env.cmd('FT.DICTADD', 'dict1', 'timmies', 'toque', 'toonie', 'Toonif', 'serviette', 'kerfuffle', 'chesterfield')
        env.cmd('FT.DICTADD', 'dict2', 'timmies', 'toque', 'toonie', 'serviette', 'kerfuffle', 'chesterfield')
        res = env.cmd('FT.SPELLCHECK', 'incidents',
               'Tooni toque kerfuffle', 'TERMS',
               'INCLUDE', 'dict1', 'dict2', 'FULLSCOREINFO')
        assert res == [0, {'tooni': [{'Toonif': 0.0}, {'toonie': 0.0}]}]

        res = env.cmd('FT.SPELLCHECK', 'incidents',
               'Tooni toque kerfuffle', 'TERMS',
               'INCLUDE', 'dict1', 'dict2')
        assert res == [{'tooni': [{'Toonif': 0.0}, {'toonie': 0.0}]}]

    def test_syndump(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
        env.assertEqual(env.execute_command('ft.synupdate', 'idx', 'id1', 'boy', 'child', 'offspring'), 'OK')
        env.assertEqual(env.execute_command('ft.synupdate', 'idx', 'id2', 'baby', 'child'), 'OK')
        env.assertEqual(env.execute_command('ft.synupdate', 'idx', 'id3', 'tree', 'wood'), 'OK')
        res = env.execute_command('ft.syndump', 'idx')
        assert res == {'baby': ['id2'], 'wood': ['id3'], 'boy': ['id1'], 
                       'tree': ['id3'], 'child': ['id1', 'id2'], 'offspring': ['id1']}

    def test_tagvals(self):
        env = self.env
        if should_skip(env):
            env.skip()

        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TAG", "f2", "TAG", "f5", "TAG")
        waitForIndex(env, 'idx1')

        res = env.execute_command('FT.TAGVALS', 'idx1', 'f1')
        assert res == {'3'}

        res = env.execute_command('FT.TAGVALS', 'idx1', 'f2')
        assert res == {'2', '3'}

        res = env.execute_command('FT.TAGVALS', 'idx1', 'f5')
        assert res == set()
