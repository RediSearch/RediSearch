# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import json
import math

from common import getConnectionByEnv, skip, waitForIndex


ROW = {'n': 2, 'fraction': 2.1, 'text': 'alpha', 'tag': 'red',
       'a': [1, 2, 3], 'groups': [[1, 2, 3], [1, 2]], 'o': {'x': 1}, 'empty': []}
OTHER = {'n': 9, 'fraction': 9.9, 'text': 'beta', 'tag': 'blue',
         'a': [4, 8], 'groups': [[4, 8], [9]], 'o': {'y': 2}, 'empty': [1]}

# Each predicate selects a stored node; its computed intermediate value is not indexed.
PREDICATES = {
    'add': '@.n + 1 == 3',
    'subtract': '@.n - 1 == 1',
    'multiply': '@.n * 2 == 4',
    'divide': '@.n / 2 == 1',
    'modulo': '@.n % 2 == 0',
    'unary_minus': '-@.n == -2',
    'unary_plus': '+@.n == 2',
    'array_equal': '@.a == [1,2,3]',
    'array_not_equal': '@.a != [4,8]',
    'object_equal': '@.o == {"x":1}',
    'object_not_equal': '@.o != {"y":2}',
    'negation': '!(@.n == 9)',
    'in': '@.n in [2,3]',
    'nin': '@.n nin [9]',
    'size': '@.a size 3',
    'sizeof': '@.a sizeof 3',
    'sizeof_string': '@.text sizeof 5',
    'sizeof_object': '@.o sizeof 1 && @.n == 2',
    'sizeof_nodelist': '@.groups[*] sizeof 3',
    'empty': '@.empty empty true',
    'object_operator': '@.o~ == "x"',
    'length_array': 'length(@.a) == 3',
    'length_object': 'length(@.o) == 1 && @.n == 2',
    'length_string': 'length(@.text) == 5',
    'abs': 'abs(@.n) == 2',
    'ceiling': 'ceiling(@.fraction) == 3',
    'floor': 'floor(@.fraction) == 2',
    'match': 'match(@.text,"a.*")',
    'search': 'search(@.text,"ph")',
    'concat': 'concat(@.text,"x") == "alphax"',
    'first': 'first(@.a) == 1',
    'last': 'last(@.a) == 3',
    'index': 'index(@.a,1) == 2',
    'append': '@.a.append(4).length() == 4',
    'min': 'min(@.a) == 1',
    'max': 'max(@.a) == 3',
    'avg': 'avg(@.a) == 2',
    'sum': 'sum(@.a) == 6',
    'stddev': 'stddev(@.a) < 1',
    'keys': '@.o.keys() == "x"',
    'count': 'count(@.a[*]) == 3',
    'value': 'value(@.o.x) == 1',
    'subsetof': '@.a subsetof [1,2,3]',
    'subsetof_nodelist': '@.groups[*] subsetof [1,2,3]',
    'anyof': '@.a anyof [1]',
    'anyof_nodelist': '@.groups[*] anyof [1]',
    'noneof': '@.a noneof [4]',
    'noneof_nodelist': '@.groups[*] noneof [4]',
}


def require_extensions(env):
    modules = [dict(zip(m[::2], m[1::2])) for m in env.cmd('MODULE', 'LIST')]
    versions = {m['name']: m['ver'] for m in modules}
    if versions.get('ReJSON', 0) < 81000:
        env.skip()
    print(f"JSONPath versions: Redis={env.cmd('INFO', 'server')['redis_version']} "
          f"modules={versions}")


def create_index(env, name, predicate):
    base = f'$.rows[?({predicate})]'
    env.expect('FT.CREATE', name, 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               base + '.text', 'AS', 't', 'TEXT', 'SORTABLE',
               base + '.tag', 'AS', 'g', 'TAG', 'SORTABLE',
               base + '.n', 'AS', 'n', 'NUMERIC', 'SORTABLE').ok()
    return base


def assert_selected(env, index, base, keys, values):
    for field in ('text', 'tag', 'n'):
        path = base + '.' + field
        for key in keys:
            expected = values[field]
            env.assertEqual(json.loads(env.cmd('JSON.GET', key, path)), expected)
            res = env.cmd('FT.SEARCH', index, '*', 'INKEYS', 1, key,
                          'RETURN', 3, path, 'AS', 'selected', 'DIALECT', 3)
            env.assertEqual(res, [1, key, ['selected', json.dumps(expected, separators=(',', ':'))]])
            res = env.cmd('FT.AGGREGATE', index, '*', 'LOAD', 4, '__key', path,
                          'AS', 'selected', 'FILTER', f'@__key == "{key}"', 'DIALECT', 3)
            env.assertEqual(res[1:], [['__key', key, 'selected', json.dumps(expected, separators=(',', ':'))]])
    for query in ('@t:alpha', '@g:{red}', '@n:[2 2]'):
        result = env.cmd('FT.SEARCH', index, query, 'INKEYS', len(keys), *keys,
                         'NOCONTENT', 'LIMIT', 0, 100)
        env.assertEqual(result[0], len(keys))
        env.assertEqual(sorted(result[1:]), sorted(keys))
    for query in ('@t:beta', '@g:{blue}', '@n:[9 9]'):
        env.expect('FT.SEARCH', index, query, 'NOCONTENT').equal([0])
    info = env.cmd('FT.INFO', index)
    info = dict(zip(info[::2], info[1::2]))
    env.assertEqual(info['hash_indexing_failures'], 0)


def check_predicate(env, predicate):
    require_extensions(env)
    conn = getConnectionByEnv(env)
    document = {'rows': [ROW, OTHER]}
    conn.execute_command('JSON.SET', 'doc:before', '$', json.dumps(document))
    base = create_index(env, 'idx', predicate)
    waitForIndex(env, 'idx')
    conn.execute_command('JSON.SET', 'doc:after', '$', json.dumps(document))
    assert_selected(env, 'idx', base, ['doc:before', 'doc:after'],
                    {'text': ['alpha'], 'tag': ['red'], 'n': [2]})
    conn.execute_command('JSON.ARRAPPEND', 'doc:after', '$.rows', json.dumps(ROW))
    assert_selected(env, 'idx', base, ['doc:after'],
                    {'text': ['alpha', 'alpha'], 'tag': ['red', 'red'], 'n': [2, 2]})
    conn.execute_command('JSON.SET', 'doc:before', '$.rows[0]', json.dumps(OTHER))
    conn.execute_command('JSON.DEL', 'doc:after', '$.rows[0]')
    conn.execute_command('JSON.DEL', 'doc:after', '$.rows[1]')
    for key in ('doc:before', 'doc:after'):
        env.assertEqual(json.loads(conn.execute_command('JSON.GET', key, base + '.text')), [])
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, key, 'RETURN', 3,
                   base + '.text', 'AS', 'selected', 'DIALECT', 3).equal([1, key, []])
    for query in ('@t:alpha', '@g:{red}', '@n:[2 2]'):
        env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT').equal([0])
    conn.execute_command('JSON.ARRAPPEND', 'doc:before', '$.rows', json.dumps(ROW))
    assert_selected(env, 'idx', base, ['doc:before'],
                    {'text': ['alpha'], 'tag': ['red'], 'n': [2]})
    conn.execute_command('DEL', 'doc:before', 'doc:after')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([0])


def predicate_test(name, predicate):
    @skip(cluster=True, no_json=True)
    def test(env):
        """Index and load stored nodes selected by an extended JSONPath predicate."""
        check_predicate(env, predicate)
    test.__name__ = 'test_jsonpath_' + name
    return test


for _name, _predicate in PREDICATES.items():
    globals()['test_jsonpath_' + _name] = predicate_test(_name, _predicate)


@skip(cluster=True, no_json=True)
def test_jsonpath_projection_rejection(env):
    """Projections fail validated TEXT schemas and are omitted from RETURN/LOAD."""
    require_extensions(env)
    env.cmd('JSON.SET', 'doc:1', '$', json.dumps({'rows': [ROW]}))
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.rows[*].text', 'AS', 't', 'TEXT').ok()
    waitForIndex(env, 'idx')
    projections = {
        '$.rows[0].n + 1': [3],
        '$.rows[0].n - 1': [1],
        '$.rows[0].n * 2': [4],
        '$.rows[0].n / 2': [1],
        '$.rows[0].n % 2': [0],
        '-$.rows[0].n': [-2],
        '+$.rows[0].n': [2],
        'length($.rows[0].a)': [3],
        'abs($.rows[0].n)': [2],
        'ceiling($.rows[0].fraction)': [3],
        'floor($.rows[0].fraction)': [2],
        '$.rows[0].text.match("a.*")': [True],
        'search($.rows[0].text,"ph")': [True],
        'concat($.rows[0].text,"x")': ['alphax'],
        'first($.rows[0].a)': [1],
        'last($.rows[0].a)': [3],
        'index($.rows[0].a,1)': [2],
        '$.rows[0].a.min()': [1],
        'max($.rows[0].a)': [3],
        'avg($.rows[0].a)': [2],
        'sum($.rows[0].a)': [6],
        'stddev($.rows[0].a)': [math.sqrt(2 / 3)],
        '$.rows[0].o~': ['x'],
        '$.rows[0].o.keys()': ['x'],
        '$.rows[0].a.append(4)': [1, 2, 3, 4],
        'count($.rows[0].a[*])': [3],
        'value($.rows[0].o.x)': [1],
    }
    for path, expected in projections.items():
        env.assertEqual(json.loads(env.cmd('JSON.GET', 'doc:1', path)), expected)
        env.expect('FT.CREATE', 'rejected', 'ON', 'JSON', 'SCHEMA', path,
                   'AS', 'v', 'TEXT').error().contains('Invalid JSONPath')
        env.expect('FT.SEARCH', 'idx', '*', 'RETURN', 3, path, 'AS', 'v',
                   'DIALECT', 3).equal([1, 'doc:1', []])
        result = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 3, path, 'AS', 'v', 'DIALECT', 3)
        env.assertEqual(result[1:], [[]])
    env.assertEqual(json.loads(env.cmd('JSON.GET', 'doc:1', '$')), [{'rows': [ROW]}])
    env.expect('FT.SEARCH', 'idx', 'alpha', 'NOCONTENT').equal([1, 'doc:1'])


@skip(cluster=True, no_json=True)
def test_jsonpath_updates_and_reload(env):
    """Predicate inputs and selected values stay consistent across writes and RDB reload."""
    require_extensions(env)
    conn = getConnectionByEnv(env)
    for name in ('match', 'min', 'arithmetic'):
        predicate = {'match': 'match(@.text,"a.*")', 'min': 'min(@.a) == 1',
                     'arithmetic': '@.n + 1 == 3'}[name]
        create_index(env, name, predicate)
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'rows': [ROW, OTHER]}))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps({'rows': [OTHER]}))
    for _ in env.reloadingIterator():
        for index in ('match', 'min', 'arithmetic'):
            predicate = {'match': 'match(@.text,"a.*")', 'min': 'min(@.a) == 1',
                         'arithmetic': '@.n + 1 == 3'}[index]
            assert_selected(env, index, f'$.rows[?({predicate})]', ['doc:1'],
                            {'text': ['alpha'], 'tag': ['red'], 'n': [2]})
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$.rows[0].text', '"amber"')
    conn.execute_command('JSON.SET', 'doc:1', '$.rows[0].tag', '"green"')
    conn.execute_command('JSON.SET', 'doc:1', '$.rows[0].n', '3')
    conn.execute_command('JSON.SET', 'doc:1', '$.rows[0].a[0]', '5')
    for index in ('match', 'min', 'arithmetic'):
        env.expect('FT.SEARCH', index, '@t:alpha', 'NOCONTENT').equal([0])
        env.expect('FT.SEARCH', index, '@g:{red}', 'NOCONTENT').equal([0])
        env.expect('FT.SEARCH', index, '@n:[2 2]', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'match', '@t:amber @g:{green} @n:[3 3]', 'NOCONTENT').equal([1, 'doc:1'])
    for index in ('min', 'arithmetic'):
        env.expect('FT.SEARCH', index, '@t:amber', 'NOCONTENT').equal([0])
    conn.execute_command('JSON.ARRAPPEND', 'doc:2', '$.rows', json.dumps(ROW))
    for _ in env.reloadingIterator():
        for index in ('match', 'min', 'arithmetic'):
            env.expect('FT.SEARCH', index, '@t:alpha', 'NOCONTENT').equal([1, 'doc:2'])
        env.expect('FT.SEARCH', 'match', '@t:amber', 'NOCONTENT').equal([1, 'doc:1'])
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.DEL', 'doc:2', '$')
    for index in ('match', 'min', 'arithmetic'):
        env.expect('FT.SEARCH', index, '@t:alpha', 'NOCONTENT').equal([0])


@skip(cluster=True, no_json=True)
def test_jsonpath_edges_and_sorting(env):
    """Empty and incompatible predicate inputs do not index stale or fabricated values."""
    require_extensions(env)
    conn = getConnectionByEnv(env)
    base = create_index(env, 'idx', 'min(@.a) == 1 && match(@.text,"a.*")')
    for number in (2, 3):
        row = dict(ROW, n=number, text='alpha' if number == 2 else 'amber',
                   tag='red' if number == 2 else 'green')
        conn.execute_command('JSON.SET', f'doc:{number}', '$', json.dumps({'rows': [row]}))
    for field, ascending in (('n', ['doc:2', 'doc:3']), ('t', ['doc:2', 'doc:3']),
                             ('g', ['doc:3', 'doc:2'])):
        env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'SORTBY', field, 'ASC').equal([2, *ascending])
        env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'SORTBY', field, 'DESC').equal([2, *reversed(ascending)])
    cases = [[], [{}], [{'a': None, 'text': None}],
             [dict(ROW, a=[])], [dict(ROW, a={})], [dict(ROW, a='')],
             [dict(ROW, a=[None, 'x', {}])], [dict(ROW, text='')],
             [dict(ROW, text=3)], [dict(ROW, a=[1, 'x'])]]
    for rows in cases:
        conn.execute_command('JSON.SET', 'doc:edge', '$', json.dumps({'rows': rows}))
        env.assertEqual(json.loads(conn.execute_command('JSON.GET', 'doc:edge', base + '.text')), [])
        env.expect('FT.SEARCH', 'idx', '@t:alpha', 'INKEYS', 1, 'doc:edge', 'NOCONTENT').equal([0])
    info = env.cmd('FT.INFO', 'idx')
    env.assertEqual(dict(zip(info[::2], info[1::2]))['hash_indexing_failures'], 0)
    for path in ('$.rows[?(@.n ==)]', '$.rows[?(match(@.text,))]', '$.rows['):
        env.expect('FT.CREATE', 'bad', 'ON', 'JSON', 'SCHEMA', path, 'AS', 'v', 'TEXT').error().contains('Invalid JSONPath')
    for predicate in ('match(@.text,"[")', 'search(@.text,"[")', '@.n / 0 == 1',
                      'value(@.a[*]) == 1', 'length(@.n) == 1'):
        path = f'$.rows[?({predicate})].text'
        env.assertEqual(json.loads(conn.execute_command('JSON.GET', 'doc:2', path)), [])
        env.expect('FT.CREATE', 'edge', 'ON', 'JSON', 'SCHEMA', path, 'AS', 't', 'TEXT').ok()
        waitForIndex(env, 'edge')
        env.expect('FT.SEARCH', 'edge', 'alpha', 'NOCONTENT').equal([0])
        env.expect('FT.DROPINDEX', 'edge').ok()
    env.expect('PING').equal(True)


@skip(cluster=True, no_json=True)
def test_jsonpath_shared_api(env):
    """Call the bundled JSON shared API directly, independently of document cardinality."""
    import os
    import shlex
    import subprocess
    import tempfile
    from pathlib import Path

    require_extensions(env)
    root = Path(__file__).resolve().parents[2]
    # A test-only module keeps JSON LLAPI introspection out of Search's command surface.
    with tempfile.TemporaryDirectory(prefix='jsonpath-api-') as directory:
        module = Path(directory) / 'jsonpath_api.so'
        os.chmod(directory, 0o755)
        flags = ['-bundle', '-undefined', 'dynamic_lookup'] if os.uname().sysname == 'Darwin' else ['-shared']
        subprocess.run([*shlex.split(os.getenv('CC', 'cc')), *flags, '-fPIC', '-Wall', '-Werror',
                        '-I', str(root / 'src'), str(Path(__file__).with_name('jsonpath_api.c')),
                        '-o', str(module)], check=True)
        env.cmd('MODULE', 'LOAD', str(module))
        try:
            for path in ('$', '$.rows[0].text', "$['rows'][0]['text']", '$.missing'):
                env.expect('jsonpath_test.flags', path).equal([1, 1])
            paths = ['$.rows[*].text', '$..text', '$.rows[0:1].text',
                     '$.rows[0,1].text', '$.rows[?(@.n == 2)].text']
            paths += [f'$.rows[?({predicate})].text' for predicate in PREDICATES.values()]
            for path in paths:
                env.expect('jsonpath_test.flags', path).equal([0, 0])
            env.cmd('JSON.SET', 'doc:single', '$', json.dumps({'rows': [ROW]}))
            for path in ('$.rows[*].text', '$..text', '$.rows[?(match(@.text,"a.*"))].text'):
                env.assertEqual(json.loads(env.cmd('JSON.GET', 'doc:single', path)), ['alpha'])
                env.expect('jsonpath_test.flags', path).equal([0, 0])
            for path in ('$.rows[0].n + 1', '$.rows[0].a.min()',
                         '$.rows[0].text.match("a.*")', '$.rows[0].o~'):
                env.expect('jsonpath_test.flags', path).error().contains('computed/projection')
            env.expect('jsonpath_test.flags', '$.rows[').error()
        finally:
            env.cmd('MODULE', 'UNLOAD', 'jsonpath_test')


@skip(cluster=True, no_json=True)
def test_jsonpath_incompatible_selected_values(env):
    """Type errors are observable and valid subsequent writes restore index membership."""
    require_extensions(env)
    conn = getConnectionByEnv(env)
    create_index(env, 'idx', 'min(@.a) == 1')
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'rows': [dict(ROW, n='wrong')]}))
    info = env.cmd('FT.INFO', 'idx')
    env.assertEqual(dict(zip(info[::2], info[1::2]))['hash_indexing_failures'], 1)
    env.expect('FT.SEARCH', 'idx', '@n:[2 2]', 'NOCONTENT').equal([0])
    conn.execute_command('JSON.SET', 'doc:1', '$.rows[0].n', '2')
    env.expect('FT.SEARCH', 'idx', '@t:alpha @g:{red} @n:[2 2]', 'NOCONTENT').equal([1, 'doc:1'])
    conn.execute_command('JSON.SET', 'doc:1', '$.rows[0].n', 'null')
    env.expect('FT.SEARCH', 'idx', '@n:[2 2]', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '@t:alpha', 'NOCONTENT').equal([1, 'doc:1'])
    conn.execute_command('JSON.DEL', 'doc:1', '$.rows[0].n')
    env.expect('FT.SEARCH', 'idx', '@n:[2 2]', 'NOCONTENT').equal([0])
    env.expect('PING').equal(True)


@skip(cluster=True, no_json=True)
def test_jsonpath_set_relation_functions(env):
    """Search agrees with JSON.GET, including versions without relation functions."""
    require_extensions(env)
    env.cmd('JSON.SET', 'doc:1', '$', json.dumps({'rows': [ROW]}))
    for name in ('subsetof', 'anyof', 'noneof'):
        rhs = '[9]' if name == 'noneof' else '[1,2,3]'
        path = f'$.rows[?({name}(@.a,{rhs}))].text'
        selected = json.loads(env.cmd('JSON.GET', 'doc:1', path))
        env.assertIn(selected, [[], ['alpha']])
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', path, 'AS', 't', 'TEXT').ok()
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH', 'idx', 'alpha', 'NOCONTENT').equal(
            [1, 'doc:1'] if selected else [0])
        fields = ['v', json.dumps(selected, separators=(',', ':'))] if selected else []
        env.expect('FT.SEARCH', 'idx', '*', 'RETURN', 3, path, 'AS', 'v',
                   'DIALECT', 3).equal([1, 'doc:1', fields])
        result = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 3, path, 'AS', 'v', 'DIALECT', 3)
        env.assertEqual(result[1:], [fields])
        env.expect('FT.DROPINDEX', 'idx').ok()
        projection = f'{name}($.rows[0].a,{rhs})'
        computed = json.loads(env.cmd('JSON.GET', 'doc:1', projection))
        env.assertIn(computed, [[], [True]])
        env.expect('FT.CREATE', 'bad', 'ON', 'JSON', 'SCHEMA', projection,
                   'AS', 'v', 'TEXT').error().contains('Invalid JSONPath')
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.rows[0].text',
                   'AS', 't', 'TEXT').ok()
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH', 'idx', '*', 'RETURN', 3, projection, 'AS', 'v',
                   'DIALECT', 3).equal([1, 'doc:1', []])
        result = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 3, projection, 'AS', 'v', 'DIALECT', 3)
        env.assertEqual(result[1:], [[]])
        env.expect('FT.DROPINDEX', 'idx').ok()
