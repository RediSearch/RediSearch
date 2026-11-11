from common import *

SWITCH = 'search-_disk-score-skipping-query'


def _populate(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'g', 'TAG').ok()
    for i in range(1, 301):
        # Repeat the common term a varying number of times so scores spread out, and give many
        # documents equal scores so the heap threshold sits on ties.
        text = ' '.join(['hello'] * (i % 7 + 1) + ['world'] * (i % 3) + ['filler'] * (i % 11))
        conn.execute_command('HSET', f'doc{i}', 't', text, 'n', i, 'g', f'tag{i % 4}')
    waitForIndex(env, 'idx')


QUERIES = [
    ['hello'],
    ['hello world'],
    ['hello|world'],
    ['hello @n:[50 250]'],
    ['hello @g:{tag1}'],
    ['hello -world'],
    ['hello ~world'],
    ['hel*'],
]
SCORERS = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'BM25STD', 'DOCSCORE', 'DISMAX']
LIMITS = [['LIMIT', 0, 5], ['LIMIT', 10, 10], ['LIMIT', 0, 1000]]


def _search_all(env):
    replies = []
    for query in QUERIES:
        for scorer in SCORERS:
            for limit in LIMITS:
                replies.append(env.cmd('FT.SEARCH', 'idx', *query, 'SCORER', scorer, 'WITHSCORES',
                                       'NOCONTENT', *limit))
    return replies


@skip(cluster=True)
def test_kill_switch_changes_no_results(env):
    """Turning the query-side score-bound switch off or on at runtime changes no reply."""
    _populate(env)
    env.expect('CONFIG', 'GET', SWITCH).equal([SWITCH, 'yes'])

    with_bounds = _search_all(env)
    env.expect('CONFIG', 'SET', SWITCH, 'no').ok()
    without_bounds = _search_all(env)
    env.expect('CONFIG', 'SET', SWITCH, 'yes').ok()
    again = _search_all(env)

    env.assertEqual(with_bounds, without_bounds)
    env.assertEqual(with_bounds, again)


@skip(cluster=True)
def test_kill_switch_mid_cursor(env):
    """Flipping the switch while a sorted cursor is open changes none of its pages."""
    _populate(env)

    def read_all(flip):
        rows = []
        res, cursor = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'ADDSCORES', 'SORTBY', 2, '@__score',
                              'DESC', 'MAX', 50, 'WITHCURSOR', 'COUNT', 10)
        rows.extend(res[1:])
        while cursor:
            if flip:
                current = env.cmd('CONFIG', 'GET', SWITCH)[1]
                env.cmd('CONFIG', 'SET', SWITCH, 'no' if current == 'yes' else 'yes')
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            rows.extend(res[1:])
        env.cmd('CONFIG', 'SET', SWITCH, 'yes')
        return rows

    env.assertEqual(read_all(flip=False), read_all(flip=True))
