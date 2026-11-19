import json

from common import *
from includes import *

# Headcount is 90: the value above 100 belongs to the revenue element.
COMPANY_SMALL = {'hq': 'US', 'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': 90},
                                         {'k': 'revenue', 'r': 'FY0', 'v': 500}]}
COMPANY_LARGE = {'hq': 'US', 'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': 150},
                                         {'k': 'revenue', 'r': 'FY0', 'v': 50}]}
# The first headcount element has no value, so the second one is the only candidate.
COMPANY_NULL = {'hq': 'IT', 'metrics': [{'k': 'headcount', 'r': 'FY1', 'v': None},
                                        {'k': 'revenue', 'r': 'FY0', 'v': 700},
                                        {'k': 'headcount', 'r': 'FY0', 'v': 300}]}

# The first element has no `v` at all, where COMPANY_NULL has a null.
COMPANY_ABSENT = {'hq': 'DE', 'metrics': [{'k': 'headcount', 'r': 'FY0'},
                                          {'k': 'revenue', 'r': 'FY0', 'v': 150}]}
COMPANY_NULL_LAST = {'hq': 'NL', 'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': 300},
                                             {'k': 'revenue', 'r': 'FY0', 'v': None}]}
COMPANY_SINGLE = {'hq': 'ES', 'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': 200}]}
COMPANY_WITHOUT_VALUES = {'hq': 'PT', 'metrics': [{'k': 'headcount', 'r': 'FY0'}]}

METRICS_GROUP = ['$.metrics[*]', 'AS', 'metrics', 'NESTED', '3',
                 '$.k', 'AS', 'k', 'TAG',
                 '$.r', 'AS', 'r', 'TAG',
                 '$.v', 'AS', 'v', 'NUMERIC']
GROUP_SCHEMA = ['$.hq', 'AS', 'hq', 'TAG'] + METRICS_GROUP
# The same values as a plain multi-value field, which is what `v` must sort and load like.
MULTI_VALUE_SCHEMA = ['$.metrics[*].v', 'AS', 'v', 'NUMERIC', 'SORTABLE']
MULTI_VALUE_TAG = ['$.metrics[*].k', 'AS', 'k', 'TAG']


def createCompanies(env, schema=GROUP_SCHEMA, more=None):
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *schema).ok()
    conn = getConnectionByEnv(env)
    for key, company in {'small': COMPANY_SMALL, 'large': COMPANY_LARGE, 'null': COMPANY_NULL,
                         **(more or {})}.items():
        setCompany(conn, key, company)
    return conn


def setCompany(conn, key, company):
    conn.execute_command('JSON.SET', key, '$', json.dumps(company))


def search(env, query, *args):
    res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', *args)
    return sorted(res[1:])


@skip(cluster=True, no_json=True)
def testConditionsOutsideGroupMatchWholeDocument(env):
    """Without the group syntax, each condition may hold on a different element."""
    createCompanies(env)
    env.assertEqual(search(env, '@k:{headcount} @v:[(100 +inf]'), ['large', 'null', 'small'])


@skip(cluster=True, no_json=True)
def testElementNumbersAreNotWordPositions(env):
    """SLOP and INORDER ignore a tag of a group, as they ignore any other tag."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.title', 'AS', 'title', 'TEXT',
               '$.plain', 'AS', 'plain', 'TAG', *METRICS_GROUP).ok()
    conn = getConnectionByEnv(env)
    for key, title in [('near', 'hello world'), ('far', 'hello big world')]:
        setCompany(conn, key, {'title': title, 'plain': 'x',
                               'metrics': [{'k': 'y'}, {'k': 'x'}]})
    for tag in ['@plain:{x}', '@k:{x}', '@k:{y} @k:{x}', '@k:{x} @k:{y}']:
        for query in [f'{tag} hello world', f'hello {tag} world', f'hello world {tag}']:
            env.assertEqual(search(env, query, 'SLOP', '0', 'INORDER'), ['near'], message=query)


@skip(cluster=True, no_json=True)
def testGroupFieldSortsByItsFirstNumber(env):
    """FT.SEARCH SORTBY orders by the first number of a group's numeric field: an element
    without a value does not hide the values after it. RETURN shows the sorted field.
    FT.AGGREGATE that loads the field sorts by the loaded value, the first JSON value:
    COMPANY_NULL sorts by its null, as it does under a plain multi-value field."""
    by_loaded_value = ['FT.AGGREGATE', 'idx', '*', 'LOAD', '2', '@__key', '@v',
                       'SORTBY', '2', '@v', 'ASC']
    createCompanies(env, MULTI_VALUE_SCHEMA)
    env.assertEqual(env.cmd(*by_loaded_value)[1:], [
        ['__key', 'null', 'v', None], ['__key', 'small', 'v', '90'],
        ['__key', 'large', 'v', '150']])
    env.cmd('FT.DROPINDEX', 'idx', 'DD')

    createCompanies(env, more={'single': COMPANY_SINGLE})
    env.assertEqual(env.cmd(*by_loaded_value)[1:], [
        ['__key', 'null', 'v', None], ['__key', 'small', 'v', '90'],
        ['__key', 'large', 'v', '150'], ['__key', 'single', 'v', '200']])
    res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'v', 'ASC', 'RETURN', '1', 'v')
    env.assertEqual(res[1:], ['small', ['v', '90'], 'large', ['v', '150'],
                              'single', ['v', '200'], 'null', ['v', '700']])
    res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'v', 'ASC', 'RETURN', '1', 'v',
                  'DIALECT', '3')
    env.assertEqual(res[1:], ['small', ['v', '[90,500]'], 'large', ['v', '[150,50]'],
                              'single', ['v', '200'], 'null', ['v', '[null,700,300]']])


MORE_COMPANIES = {'absent': COMPANY_ABSENT, 'null_last': COMPANY_NULL_LAST,
                  'single': COMPANY_SINGLE, 'without': COMPANY_WITHOUT_VALUES}
RETURN_FIELDS = ['FT.SEARCH', 'idx', '*', 'RETURN', '2', 'k', 'v']
LOAD_FIELDS = ['FT.AGGREGATE', 'idx', '*', 'LOAD', '3', '@__key', '@k', '@v',
               'SORTBY', '1', '@__key']
RETURN_SORTED = ['FT.SEARCH', 'idx', '*', 'RETURN', '2', 'k', 'v', 'SORTBY', 'v']
SORT_KEYS = ['FT.SEARCH', 'idx', '*', 'SORTBY', 'v', 'WITHSORTKEYS']
LOAD_SORTED = ['FT.AGGREGATE', 'idx', '*', 'LOAD', '3', '@__key', '@k', '@v',
               'SORTBY', '2', '@v', '@__key']


def displayReplies(env, schema, variants):
    """The reply, under `schema`, to each command that shows `k` and `v`, with each of
    `variants`. The sorted commands run without COMPANY_NULL, which a group sorts by its first
    number and a plain field by its null."""
    conn = createCompanies(env, schema, MORE_COMPANIES)
    replies = [env.cmd(*command, *variant)
               for command in [RETURN_FIELDS, LOAD_FIELDS] for variant in variants]
    conn.execute_command('DEL', 'null')
    replies += [env.cmd(*command, *variant)
                for command in [RETURN_SORTED, SORT_KEYS, LOAD_SORTED] for variant in variants]
    env.cmd('FT.DROPINDEX', 'idx', 'DD')
    return replies


@skip(cluster=True, no_json=True)
def testGroupFieldsDisplayLikeMultiValueFields(env):
    """RETURN and LOAD show the fields of a group across the elements of the array, also where
    an element has no value or a null, as they show plain multi-value fields."""
    conn = createCompanies(env, GROUP_SCHEMA, MORE_COMPANIES)
    # Without DIALECT 3 a field shows its first value: `k` is `headcount` in every company.
    k = ['k', 'headcount']
    env.assertEqual(env.cmd(*RETURN_FIELDS), [
        7, 'small', [*k, 'v', '90'], 'large', [*k, 'v', '150'], 'null', [*k, 'v', None],
        'absent', [*k, 'v', '150'], 'null_last', [*k, 'v', '300'], 'single', [*k, 'v', '200'],
        'without', k])
    env.assertEqual(env.cmd(*LOAD_FIELDS), [
        7, ['__key', 'absent', *k, 'v', '150'], ['__key', 'large', *k, 'v', '150'],
        ['__key', 'null', *k, 'v', None], ['__key', 'null_last', *k, 'v', '300'],
        ['__key', 'single', *k, 'v', '200'], ['__key', 'small', *k, 'v', '90'],
        ['__key', 'without', *k]])
    res = env.cmd('FT.SEARCH', 'idx', '@hq:{IT}', 'RETURN', '2', 'k', 'v', 'DIALECT', '3')
    env.assertEqual(res, [1, 'null', ['k', '["headcount","revenue","headcount"]',
                                      'v', '[null,700,300]']])

    # `large` and `absent` both sort by 150, and `without` has no value to sort by. FT.SEARCH
    # puts the field of SORTBY first.
    conn.execute_command('DEL', 'null')
    env.assertEqual(env.cmd(*RETURN_SORTED), [
        6, 'small', ['v', '90', *k], 'large', ['v', '150', *k], 'absent', ['v', '150', *k],
        'single', ['v', '200', *k], 'null_last', ['v', '300', *k], 'without', k])
    # Each result is its key, its sort key, and `v` with the whole document.
    res = env.cmd(*SORT_KEYS)
    env.assertEqual(res[1::3], ['small', 'large', 'absent', 'single', 'null_last', 'without'])
    env.assertEqual(res[2::3], ['#90', '#150', '#150', '#200', '#300', None])
    env.assertEqual(res[3], ['v', '90', '$', json.dumps(COMPANY_SMALL, separators=(',', ':'))])
    env.assertEqual(env.cmd(*LOAD_SORTED), [
        6, ['__key', 'small', *k, 'v', '90'], ['__key', 'absent', *k, 'v', '150'],
        ['__key', 'large', *k, 'v', '150'], ['__key', 'single', *k, 'v', '200'],
        ['__key', 'null_last', *k, 'v', '300'], ['__key', 'without', *k]])
    env.cmd('FT.DROPINDEX', 'idx', 'DD')

    dialects = [[], ['DIALECT', '2'], ['DIALECT', '3']]
    env.assertEqual(displayReplies(env, GROUP_SCHEMA, dialects),
                    displayReplies(env, MULTI_VALUE_SCHEMA + MULTI_VALUE_TAG, dialects))


@skip(cluster=True, no_json=True)
def testGroupFieldsDisplayLikeMultiValueFieldsResp3():
    """Under RESP3 too, where FORMAT EXPAND shows the values and not their JSON text."""
    env = Env(protocol=3)
    variants = [dialect + fmt for dialect in [[], ['DIALECT', '2'], ['DIALECT', '3']]
                for fmt in [[], ['FORMAT', 'STRING'], ['FORMAT', 'EXPAND']]]
    env.assertEqual(displayReplies(env, GROUP_SCHEMA, variants),
                    displayReplies(env, MULTI_VALUE_SCHEMA + MULTI_VALUE_TAG, variants))


@skip(cluster=True, no_json=True)
def testPathOfGroupFieldAtDocumentRootIsNotTheField(env):
    """A value at the path that a field has inside an element, but at the document root, is
    neither indexed for the field nor returned as the field."""
    createCompanies(env)
    setCompany(getConnectionByEnv(env), 'rooted', {'hq': 'FR', 'k': 'root', 'v': 1000})
    env.assertEqual(search(env, '@k:{root}'), [])
    env.assertEqual(search(env, '@v:[1000 1000]'), [])
    res = env.cmd('FT.SEARCH', 'idx', '@hq:{FR}', 'RETURN', '2', 'k', 'v')
    env.assertEqual(res, [1, 'rooted', []])
    res = env.cmd('FT.SEARCH', 'idx', '@hq:{FR}', 'RETURN', '3', '$.k', 'AS', 'root_k')
    env.assertEqual(res, [1, 'rooted', ['root_k', 'root']])


@skip(cluster=True, no_json=True)
def testInfoOfGroupAndItsFields(env):
    """FT.INFO names the group of each of its fields, and lists no attribute for the group
    itself. The numeric field is sortable."""
    createCompanies(env)
    group = ['NESTED', 'metrics', 'NESTED_PATH', '$.metrics[*]']
    env.assertEqual(index_info(env)['attributes'], [
        ['identifier', '$.hq', 'attribute', 'hq', 'type', 'TAG', 'SEPARATOR', ''],
        ['identifier', '$.k', 'attribute', 'k', 'type', 'TAG', 'SEPARATOR', '', *group],
        ['identifier', '$.r', 'attribute', 'r', 'type', 'TAG', 'SEPARATOR', '', *group],
        ['identifier', '$.v', 'attribute', 'v', 'type', 'NUMERIC', *group, 'SORTABLE', 'UNF'],
    ])


@skip(cluster=True, no_json=True)
def testObfuscatedInfoHidesTheGroup(env):
    """The obfuscated FT.INFO names the group of a field by its position, and shows no path."""
    createCompanies(env)
    info = to_dict(env.cmd(debug_cmd(), 'INFO', 'ALL')[0])
    in_group = [attribute for attribute in info['attributes'] if 'NESTED' in attribute]
    env.assertEqual(len(in_group), 3)
    for attribute in in_group:
        env.assertEqual(attribute[attribute.index('NESTED') + 1], 'NestedGroup@0')
        env.assertFalse('NESTED_PATH' in attribute, message=attribute)
    env.assertFalse('metrics' in str(info['attributes']))


@skip(cluster=True, no_json=True)
def testNestedGroupRejectsUnsupportedSchemas(env):
    """FT.CREATE rejects what a group cannot hold, and FT.ALTER cannot add a group."""
    group = ['$.metrics[*]', 'AS', 'metrics', 'NESTED']
    one = [*group, '1']
    k = ['$.k', 'AS', 'k']
    unsupported = [([*one, *k, *definition], 'supports only TAG and NUMERIC fields, SORTABLE'
                    ' only on NUMERIC, and neither NOINDEX')
                   for definition in [['TEXT'], ['TAG', 'SORTABLE'], ['TAG', 'NOINDEX'],
                                      ['TAG', 'INDEXEMPTY'], ['TAG', 'INDEXMISSING'],
                                      ['NUMERIC', 'NOINDEX']]]
    no_count = [([*group, *count], 'NESTED requires the number of fields in the group')
                for count in [[], ['0'], ['x']]]
    # `$v` is a path on its own, and no path after the path of the group.
    after_group = 'Invalid JSONPath after the path of the NESTED group'
    for schema, error in unsupported + no_count + [
            ([*group, '2', *k, 'TAG'], 'fewer fields than declared'),
            ([*group, '2', *k, 'TAG', '$.inner[*]', 'AS', 'inner', 'NESTED', '1',
              '$.r', 'AS', 'r', 'TAG'], 'NESTED group inside a NESTED group'),
            (['$.metrics[', 'AS', 'metrics', 'NESTED', '1', *k, 'TAG'],
             "Invalid JSONPath '$.metrics[' of NESTED group 'metrics'"),
            ([*one, '$.v[', 'AS', 'v', 'NUMERIC'], "Invalid JSONPath '$.v[' in attribute 'v'"),
            ([*one, '$v', 'AS', 'v', 'NUMERIC'], after_group),
            ([*one, '$v', 'AS', 'k', 'TAG'], after_group),
            (['$.metrics', 'AS', 'metrics', 'NESTED', '1', *k, 'TAG'],
             'must select the elements of an array'),
            ([*one, '$.tags[*]', 'AS', 'k', 'TAG'], 'at most one value in an element'),
            ([*one, '$..k', 'AS', 'k', 'TAG'], 'at most one value in an element')]:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *schema).error().contains(error)

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'metrics', 'NESTED', '1', 'k', 'TAG').error() \
        .contains('NESTED is supported only for in-memory JSON indexes')

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.hq', 'AS', 'hq', 'TAG').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *METRICS_GROUP).error() \
        .contains('cannot be added to an existing index')


@skip(cluster=True, no_json=True)
def testGroupFieldValueOfWrongTypeFailsTheDocument(env):
    """A value of the wrong type in an element fails the document, with the error of a plain
    field of that type."""
    conn = createCompanies(env)
    not_a_number = 'NUMERIC fields can only contain numeric or nulls'
    not_a_string = 'TEXT/TAG fields can only contain strings or nulls'
    wrong = [('v', 'many', not_a_number), ('v', [1, 2], not_a_number),
             ('v', {'n': 1}, not_a_number), ('v', True, not_a_number),
             ('k', 7, not_a_string), ('k', ['headcount'], not_a_string)]
    for failures, (field, value, error) in enumerate(wrong, start=1):
        element = {'k': 'headcount', 'r': 'FY0', 'v': 400, field: value}
        setCompany(conn, 'wrong', {'hq': 'FR', 'metrics': [element]})
        env.assertEqual(search(env, '@hq:{FR}'), [], message=value)
        errors = index_errors(env)
        env.assertEqual(errors['indexing failures'], failures, message=value)
        env.assertContains(error, errors['last indexing error'], message=value)


@skip(cluster=True, no_json=True)
def testGroupFieldSkipsNullAndIndexesFraction(env):
    """A null in a tag field is skipped, as a null in a numeric field is, and a numeric field
    takes a number that is not an integer."""
    conn = createCompanies(env)
    setCompany(conn, 'fraction', {'hq': 'FR', 'metrics': [{'k': None, 'r': 'FY0', 'v': 2.5},
                                                          {'k': 'profit', 'r': 'FY0', 'v': None}]})
    env.assertEqual(index_errors(env)['indexing failures'], 0)
    env.assertEqual(search(env, '@k:{profit}'), ['fraction'])
    env.assertEqual(search(env, '@v:[2.5 2.5]'), ['fraction'])
    env.assertEqual(search(env, '@r:{FY0} @hq:{FR}'), ['fraction'])


@skip(cluster=True)
def testFieldNamedNestedAfterTypeOption(env):
    """NESTED after an option of a field is the name of the next field, as it is without groups."""
    for i, options in enumerate([['SORTABLE', 'CASESENSITIVE'], ['WITHSUFFIXTRIE'],
                                 ['INDEXMISSING']]):
        index = f'idx{i}'
        env.expect('FT.CREATE', index, 'SCHEMA', 't', 'TAG', *options, 'NESTED', 'TEXT').ok()
        # An attribute is `identifier <path> attribute <name> type <type> ...`.
        attributes = [attribute[3:6:2] for attribute in index_info(env, index)['attributes']]
        env.assertEqual(attributes, [['t', 'TAG'], ['NESTED', 'TEXT']], message=options)


@skip(cluster=True, no_json=True)
def testGroupFieldAggregatesLikeMultiValueField(env):
    """GROUPBY, a reducer and APPLY read a numeric field of a group as they read a plain
    multi-value sortable field: by its first value."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *GROUP_SCHEMA).ok()
    env.expect('FT.CREATE', 'plain', 'ON', 'JSON', 'SCHEMA', *MULTI_VALUE_SCHEMA).ok()
    conn = getConnectionByEnv(env)
    setCompany(conn, 'small', COMPANY_SMALL)
    setCompany(conn, 'large', COMPANY_LARGE)
    setCompany(conn, 'same', COMPANY_SMALL)

    def aggregate(index, *args):
        res = env.cmd('FT.AGGREGATE', index, '*', *args)
        return sorted(res[1:])

    # The first values are 90, 150 and 90. `aggregate` sorts the rows as lists of strings.
    for args, expected in [
            (['GROUPBY', '1', '@v', 'REDUCE', 'COUNT', '0', 'AS', 'n'],
             [['v', '150', 'n', '1'], ['v', '90', 'n', '2']]),
            (['GROUPBY', '0', 'REDUCE', 'SUM', '1', '@v', 'AS', 'sum'], [['sum', '330']]),
            (['LOAD', '1', '@v', 'APPLY', '@v + 1', 'AS', 'next', 'SORTBY', '2', '@next', 'ASC'],
             [['v', '150', 'next', '151'], ['v', '90', 'next', '91'], ['v', '90', 'next', '91']])]:
        env.assertEqual(aggregate('idx', *args), expected, message=args)
        env.assertEqual(aggregate('plain', *args), expected, message=args)
