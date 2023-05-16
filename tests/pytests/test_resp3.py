from common import *

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


class testResp3():
    def __init__(self):
        self.env = Env(protocol=3)

    def test_resp3(self):
        env = self.env
        r_ver = redis_version(env)
        if r_ver < version.parse("7.0.0"):
            env.skip()
        # if r_ver >= version.parse("7.0.0"):
        #     env.skipOnCluster() # TODO: remove when redis-py is fixed
        with env.getClusterConnectionIfNeeded() as r:
            r_ver = redis_version(r)
            if r_ver < version.parse("7.0.0"):
                env.skip()

        env.cmd('FLUSHALL')
        env.execute_command('HSET', 'doc1', 'f1', '3', 'f2', '3')
        env.execute_command('HSET', 'doc2', 'f1', '3', 'f2', '2', 'f3', '4')
        env.execute_command('FT.create', 'idx1', "PREFIX", 1, "doc",
                            "SCHEMA", "f1", "TEXT", "f2", "TEXT")
        res = env.execute_command('FT.search', 'idx1', "*")
        assert res == {'fields_names': [], 'error': [], 'total_results': 2, 
                       'results': 
                       [{'id': 'doc2', 'fields': {'f1': '3', 'f2': '2', 'f3': '4'}, 'fields_values': []}, 
                        {'id': 'doc1', 'fields': {'f1': '3', 'f2': '3'}, 'fields_values': []}]}

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
        res = env.execute_command('FT.search', 'idx1', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS", "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC")
        assert res == {'fields_names': [], 'error': [], 'total_results': 2, 'results': 
                       [{'id': 'doc1', 'score': 0.5, 'payload': None, 'sortkey': '$3', 
                         'fields': {'f2': '3', 'f1': '3'}, 'fields_values': []}, 
                         {'id': 'doc2', 'score': 1.0, 'payload': None, 'sortkey': '$2', 
                          'fields': {'f2': '2', 'f1': '3'}, 'fields_values': []}]}
    
        #res = env.cmd('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', "*", "VERBATIM", "WITHSCORES", "WITHPAYLOADS", "WITHSORTKEYS", "RETURN", 2, 'f1', 'f2', "SORTBY", 'f2', "DESC")
        #res = env.cmd('FT.PROFILE', 'idx1', 'SEARCH', 'QUERY', "*")
        #print(res)

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

