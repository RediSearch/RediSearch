import json
import struct

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
UNSUPPORTED_IN_GROUP = 'A condition on one array element must be'
ZERO_VECTOR = struct.pack('2f', 0, 0)


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


@skip(no_json=True)
def testGroupMatchesOneElement(env):
    """All the conditions of a group hold on one element. Runs on a cluster too."""
    createCompanies(env)
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(100 +inf])'), ['large', 'null'])
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @r:{FY1})'), ['null'])
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @r:{FY1} @v:[(100 +inf])'), [])


@skip(cluster=True, no_json=True)
def testGroupMatchesAnyElementWithTheKey(env):
    """A tag that occurs in several elements makes each of them a candidate."""
    conn = createCompanies(env)
    grown = {'hq': 'FR', 'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': 50},
                                     {'k': 'headcount', 'r': 'FY1', 'v': 400}]}
    setCompany(conn, 'grown', grown)
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(350 +inf])'), ['grown'])
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[-inf (60])'), ['grown'])


@skip(cluster=True, no_json=True)
def testGroupOfNumericConditionsOnly(env):
    """Without a tag to name candidates, every element of a matching document is tested."""
    createCompanies(env)
    env.assertEqual(search(env, '@metrics:(@v:[(100 200])'), ['large'])
    # `small` has a value above 100 and a value up to 200, in different elements.
    env.assertEqual(search(env, '@metrics:(@v:[(100 +inf] @v:[-inf 200])'), ['large'])
    env.assertEqual(search(env, '@v:[(100 +inf] @v:[-inf 200]'), ['large', 'small'])


@skip(cluster=True, no_json=True)
def testGroupWithOrOfTagConditions(env):
    """An OR of tag conditions inside an AND matches the elements of either."""
    createCompanies(env)
    env.assertEqual(search(env, '@metrics:(@k:{headcount} (@r:{FY1} | @r:{FY-1}))'), ['null'])
    env.assertEqual(search(env, '@metrics:((@k:{headcount} | @r:{FY1}) @v:[(600 +inf])'), [])
    env.assertEqual(search(env, '@metrics:((@k:{revenue} | @r:{FY1}) @v:[(600 +inf])'), ['null'])


@skip(cluster=True, no_json=True)
def testOrNotAndOptionalAtTheTopOfGroup(env):
    """Each alternative of an OR, and the expression under a NOT or `~`, is a group of its own."""
    createCompanies(env)
    alternatives = '(@k:{headcount} @v:[(100 +inf]) | (@k:{revenue} @v:[(450 550])'
    env.assertEqual(search(env, f'@metrics:({alternatives})'), ['large', 'null', 'small'])
    # Across elements `null` has a headcount and a value in the range; no one element has both.
    env.assertEqual(search(env, '@metrics:((@k:{headcount} @v:[(600 +inf]) | @r:{FY-1})'), [])
    env.assertEqual(search(env, '@metrics:(-(@k:{headcount} @v:[(100 +inf]))'), ['small'])
    env.assertEqual(search(env, '@hq:{US} @metrics:(~(@k:{headcount} @v:[(100 +inf]))'),
                    ['large', 'small'])


@skip(cluster=True, no_json=True)
def testGroupRejectsConditionsItCannotMatchToAnElement(env):
    """A shape with no meaning on one element is an error, not a whole-document match."""
    createCompanies(env)
    for query in ['@metrics:(@k:{headcount} -@v:[(100 +inf])',
                  # `a` is a stop word, so the text leaves no expression behind.
                  '@metrics:(a)',
                  '@metrics:(a) @hq:{US}',
                  '@metrics:(@metrics:(a))']:
        env.expect('FT.SEARCH', 'idx', query).error().contains(UNSUPPORTED_IN_GROUP)


@skip(cluster=True, no_json=True)
def testGroupNameMustBeTheGroupOfItsFields(env):
    """The field before the colon is a NESTED group, and the conditions are on its fields."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *GROUP_SCHEMA,
               '$.owners[*]', 'AS', 'owners', 'NESTED', '2',
               '$.name', 'AS', 'name', 'TAG',
               '$.share', 'AS', 'share', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    setCompany(conn, 'held', {**COMPANY_LARGE, 'owners': [{'name': 'ann', 'share': 10},
                                                         {'name': 'bob', 'share': 60}]})
    env.assertEqual(search(env, '@owners:(@name:{bob} @share:[50 +inf])'), ['held'])
    env.assertEqual(search(env, '@owners:(@name:{ann} @share:[50 +inf])'), [])
    # Element 1 of each array: the two groups do not share element numbers.
    both = '@owners:(@name:{bob} @share:[50 +inf]) @metrics:(@k:{revenue} @v:[-inf 100])'
    env.assertEqual(search(env, both), ['held'])
    env.expect('FT.SEARCH', 'idx', '@metrics:(@name:{bob} @v:[(100 +inf])').error() \
        .contains(UNSUPPORTED_IN_GROUP)
    # A field is not a group: the error is at its name, as it is without groups.
    env.expect('FT.SEARCH', 'idx', '@hq:(@k:{headcount} @v:[(100 +inf])').error() \
        .equal('SEARCH_SYNTAX Syntax error at offset 0 near hq')
    # A group is not a field of any type.
    for query in ['@metrics:hello', '@metrics:(hello)', '@metrics:{a}', '@metrics:[1 2]',
                  'ismissing(@metrics)']:
        env.expect('FT.SEARCH', 'idx', query).error().contains(UNSUPPORTED_IN_GROUP)


@skip(cluster=True, no_json=True)
def testLegacyFiltersApplyToTheDocument(env):
    """FILTER and INKEYS stay conditions on the document when the query is one group."""
    createCompanies(env)
    group = '@metrics:(@k:{headcount} @r:{FY0})'
    env.assertEqual(search(env, group), ['large', 'null', 'small'])
    env.assertEqual(search(env, group, 'INKEYS', '2', 'small', 'null'), ['null', 'small'])
    # The headcount of `small` is 90: the 500 that passes the filter is in another element.
    env.assertEqual(search(env, group, 'FILTER', 'v', '400', '600'), ['small'])


@skip(cluster=True, no_json=True)
def testOptimizerLeavesGroupConditionsInPlace(env):
    """Sorting by the field of a group's numeric condition does not take it out of the group."""
    createCompanies(env)
    group = '@metrics:(@k:{headcount} @v:[(100 +inf])'
    for args in [['SORTBY', 'v', 'DIALECT', '4'], ['SORTBY', 'v', 'WITHOUTCOUNT']]:
        res = env.cmd('FT.SEARCH', 'idx', group, 'NOCONTENT', *args)
        env.assertEqual(res[1:], ['large', 'null'], message=args)


@skip(cluster=True, no_json=True)
def testTwoGroupsAreIndependent(env):
    """Two groups on one array may be satisfied by different elements."""
    createCompanies(env)
    query = '@metrics:(@k:{headcount} @v:[(100 +inf]) @metrics:(@k:{revenue} @v:[(600 +inf])'
    env.assertEqual(search(env, query), ['null'])


@skip(cluster=True, no_json=True)
def testNegatedGroupReadsEveryTagOfAnOr(env):
    """Under a NOT, or with weight 0, an OR of tags still reports the elements of each tag."""
    createCompanies(env)
    # The revenue of `null` is 700, in the element after a headcount without a value.
    for either in ['(@k:{headcount} | @k:{revenue})', '@k:{headcount | revenue}']:
        env.assertEqual(search(env, f'-@metrics:({either} @v:[(600 +inf])'), ['large', 'small'])
        env.assertEqual(search(env, f'@metrics:({either}=>{{$weight: 0}} @v:[(600 +inf])'),
                        ['null'])


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
def testGroupFollowsDocumentUpdate(env):
    """An updated document is matched on its new elements only."""
    conn = createCompanies(env)
    setCompany(conn, 'small', {'hq': 'US', 'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': 120}]})
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(100 +inf])'),
                    ['large', 'null', 'small'])
    env.assertEqual(search(env, '@metrics:(@k:{revenue} @v:[(100 +inf])'), ['null'])


@skip(cluster=True, no_json=True)
def testGroupAfterDeleteAndGC(env):
    """Deleting a document and collecting its entries leaves the others matching."""
    conn = createCompanies(env)
    conn.execute_command('DEL', 'large')
    forceInvokeGC(env)
    collected = int(to_dict(index_info(env)['gc_stats'])['bytes_collected'])
    env.assertGreater(collected, 0, message=collected)
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(100 +inf])'), ['null'])
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @r:{FY0})'), ['null', 'small'])


def createTagOptionsGroup(env):
    """A group whose tag field has a separator and is case sensitive."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.metrics[*]', 'AS', 'metrics', 'NESTED', '2',
               '$.k', 'AS', 'k', 'TAG', 'SEPARATOR', ',', 'CASESENSITIVE',
               '$.v', 'AS', 'v', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    setCompany(conn, 'doc', {'metrics': [{'k': 'Head,count', 'v': 90},
                                         {'k': 'count', 'v': 500}]})
    return conn


@skip(cluster=True, no_json=True)
def testTagOptionsOfGroupFieldApplyInEachElement(env):
    """SEPARATOR and CASESENSITIVE apply to a tag field of a group, element by element."""
    createTagOptionsGroup(env)
    env.assertEqual(search(env, '@metrics:(@k:{Head} @v:[-inf 100])'), ['doc'])
    env.assertEqual(search(env, '@metrics:(@k:{Head} @v:[(100 +inf])'), [])
    env.assertEqual(search(env, '@metrics:(@k:{head} @v:[-inf 100])'), [])
    # `count` is a tag of both elements.
    env.assertEqual(search(env, '@metrics:(@k:{count} @v:[-inf 100])'), ['doc'])
    env.assertEqual(search(env, '@metrics:(@k:{count} @v:[(100 +inf])'), ['doc'])


@skip(cluster=True, no_json=True)
def testGroupFieldValueWithoutTagIsNotIndexed(env):
    """A value that holds no tag gives the tag field nothing to index for the document."""
    conn = createTagOptionsGroup(env)
    for value in ['', ',', ' ']:
        setCompany(conn, 'empty', {'metrics': [{'k': value, 'v': 90}]})
        env.assertEqual(search(env, '@metrics:(@v:[-inf 100])'), ['doc', 'empty'], message=value)
        env.assertEqual(search(env, '@metrics:(@k:{count} @v:[-inf 100])'), ['doc'],
                        message=value)


@skip(cluster=True, no_json=True)
def testGroupCountsElementThatIsNotAnObject(env):
    """An element that is not an object has no value, and still has its element number. A
    path that selects an object, and not the elements of an array, gives the group nothing."""
    conn = createCompanies(env)
    setCompany(conn, 'mixed', {'hq': 'FR',
                               'metrics': [7, {'k': 'headcount', 'r': 'FY0', 'v': 400}]})
    setCompany(conn, 'object', {'hq': 'FR', 'metrics': {'k': 'headcount', 'r': 'FY0', 'v': 400}})
    env.assertEqual(search(env, '@hq:{FR}'), ['mixed', 'object'])
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(350 +inf])'), ['mixed'])


@skip(cluster=True, no_json=True)
def testExplicitSortableChangesNothing(env):
    """A numeric field of a group is sortable with or without SORTABLE."""
    createCompanies(env, GROUP_SCHEMA + ['SORTABLE'])
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(100 +inf])'), ['large', 'null'])


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
def testGroupSyntaxRequiresDialect2(env):
    """DIALECT 1 has no syntax for a group, and says so."""
    createCompanies(env)
    # Also for text of stop words only, which leaves no expression behind.
    for query in ['@metrics:(@k:{headcount} @v:[(100 +inf])', '@metrics:(a)']:
        env.expect('FT.SEARCH', 'idx', query, 'DIALECT', '1') \
            .error().contains('require DIALECT 2 or higher')
    # The tag condition of DIALECT 1 takes a group for a name that is not a field.
    env.expect('FT.SEARCH', 'idx', '@metrics:{headcount}', 'DIALECT', '1').equal([0])


def assertTagExpansionsInGroup(env, expansions):
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.metrics[*]', 'AS', 'metrics', 'NESTED', '2',
               '$.k', 'AS', 'k', 'TAG', 'WITHSUFFIXTRIE',
               '$.v', 'AS', 'v', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    # The first tag that every expansion finds is in the element whose value fails.
    setCompany(conn, 'doc', {'metrics': [{'k': 'head1', 'v': 1}, {'k': 'head2', 'v': 500}]})
    setCompany(conn, 'other', {'metrics': [{'k': 'abc', 'v': 500}]})
    for tags in expansions:
        group = f'@metrics:(@k:{tags} @v:[(100 +inf])'
        env.assertEqual(search(env, group), ['doc'], message=tags)
        env.assertEqual(search(env, f'-{group}'), ['other'], message=tags)


@skip(cluster=True, no_json=True)
def testTagExpansionsInGroup(env):
    """A prefix, suffix, contains or wildcard tag condition reports every tag it expands to."""
    assertTagExpansionsInGroup(env, ['{head*}', '{*ad2}', '{*ead*}', "{w'he?d2'}", "{w'he*'}"])


@skip(cluster=True, no_json=True)
def testTagRangeInGroup(env):
    """So does a range tag condition, whose syntax needs the unstable features."""
    enable_unstable_features(env)
    try:
        assertTagExpansionsInGroup(env, ['>={head1}'])
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'no')


def createTitledCompany(env):
    """One company with a title, a plain tag and a group whose element 0 matches `a` and `b`."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.title', 'AS', 'title', 'TEXT',
               '$.plain', 'AS', 'plain', 'TAG', *METRICS_GROUP).ok()
    setCompany(getConnectionByEnv(env), 'doc',
               {'title': 'hello world', 'plain': 'a',
                'metrics': [{'k': 'a', 'r': 'b', 'v': 5}, {'k': 'c', 'r': 'd', 'v': 500}]})


@skip(cluster=True, no_json=True)
def testGroupHasNoWordPositions(env):
    """A group inside a SLOP and INORDER phrase is ignored by the proximity check, and by
    HIGHLIGHT."""
    createTitledCompany(env)
    for group in ['@metrics:(@k:{a} @r:{b})', '@metrics:(@k:{a} @v:[0 10])',
                  '@metrics:(@k:{c} @r:{d} @v:[400 600])']:
        for query in [f'hello {group} world', f'{group} hello world', f'hello world {group}']:
            env.assertEqual(search(env, query, 'SLOP', '0', 'INORDER'), ['doc'], message=query)
    res = env.cmd('FT.SEARCH', 'idx', 'hello @metrics:(@k:{a} @r:{b})',
                  'HIGHLIGHT', 'FIELDS', '1', 'title', 'RETURN', '1', 'title')
    env.assertEqual(res, [1, 'doc', ['title', '<b>hello</b> world']])


@skip(cluster=True, no_json=True)
def testGroupDoesNotChangeTheScore(env):
    """A group filters documents: with any weight, the score is that of the rest of the query."""
    createTitledCompany(env)
    for scorer in [[], ['SCORER', 'TFIDF']]:
        def score(query):
            return env.cmd('FT.SEARCH', 'idx', query, 'WITHSCORES', 'NOCONTENT', *scorer)[2]
        for group in ['@metrics:(@k:{a} @r:{b})', '@metrics:(@k:{a} @v:[0 10])',
                      '@metrics:(@k:{a} @v:[0 10])=>{$weight: 3}']:
            env.assertEqual(score(f'hello {group}'), score('hello'), message=(scorer, group))


@skip(cluster=True, no_json=True)
def testExplainAndProfileShowTheGroup(env):
    """FT.EXPLAIN names the group of the conditions and FT.PROFILE shows the element filter."""
    createCompanies(env)
    group = '@metrics:(@k:{headcount} @v:[(100 +inf])'
    env.assertContains('SAME-ELEMENT:@metrics {', env.cmd('FT.EXPLAIN', 'idx', group))
    profile = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', group, 'NOCONTENT')
    iterators = to_dict(to_dict(profile[1][1][0])['Iterators profile'])
    env.assertEqual(iterators['Type'], 'ELEMENT-FILTER')
    env.assertEqual(to_dict(iterators['Child iterator'])['Type'], 'TAG')


@skip(cluster=True, no_json=True)
def testGroupWithParams(env):
    """PARAMS fill the tag and the range of a group."""
    createCompanies(env)
    res = search(env, '@metrics:(@k:{$key} @v:[($low +inf])', 'PARAMS', '4', 'key', 'headcount',
                 'low', '100')
    env.assertEqual(res, ['large', 'null'])


@skip(cluster=True, no_json=True)
def testGroupInAggregate(env):
    """FT.AGGREGATE runs a group query, with and without a cursor."""
    createCompanies(env)
    group = '@metrics:(@k:{headcount} @v:[(100 +inf])'
    res = env.cmd('FT.AGGREGATE', 'idx', group, 'LOAD', '1', '@__key', 'SORTBY', '1', '@__key')
    env.assertEqual(res[1:], [['__key', 'large'], ['__key', 'null']])
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', group, 'LOAD', '1', '@__key',
                          'WITHCURSOR', 'COUNT', '1')
    keys = [row[1] for row in res[1:]]
    while cursor:
        res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        keys += [row[1] for row in res[1:]]
    env.assertEqual(sorted(keys), ['large', 'null'])


def createCompaniesWithVector(env):
    """The companies of `createCompanies`, each with the same vector, which `ZERO_VECTOR` is."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *GROUP_SCHEMA,
               '$.vec', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for key, company in [('small', COMPANY_SMALL), ('large', COMPANY_LARGE),
                         ('null', COMPANY_NULL)]:
        setCompany(conn, key, {**company, 'vec': [0, 0]})


@skip(cluster=True, no_json=True)
def testGroupAsKnnFilter(env):
    """A group is the filter of a KNN query."""
    createCompaniesWithVector(env)
    res = env.cmd('FT.SEARCH', 'idx', '@metrics:(@k:{headcount} @v:[(100 +inf])=>[KNN 3 @vec $b]',
                  'PARAMS', '2', 'b', ZERO_VECTOR, 'NOCONTENT')
    env.assertEqual(sorted(res[1:]), ['large', 'null'])


@skip(cluster=True, no_json=True)
def testGroupInHybrid(env):
    """A group is the SEARCH subquery of FT.HYBRID, and the FILTER of its VSIM subquery."""
    createCompaniesWithVector(env)
    group = '@metrics:(@k:{headcount} @v:[(100 +inf])'
    # The other subquery matches nothing, so the results are those of the group.
    nothing = '@hq:{nowhere}'

    def hybrid(search, vsim_filter):
        res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search,
                      'VSIM', '@vec', '$b', 'FILTER', vsim_filter, 'PARAMS', '2', 'b', ZERO_VECTOR)
        results, count = get_results_from_hybrid_response(res)
        env.assertEqual(count, len(results), message=res)
        return sorted(results)

    env.assertEqual(hybrid(nothing, nothing), [])
    env.assertEqual(hybrid(group, nothing), ['large', 'null'])
    env.assertEqual(hybrid(nothing, group), ['large', 'null'])
    env.assertEqual(hybrid(nothing, '@k:{headcount} @v:[(100 +inf]'), ['large', 'null', 'small'])


@skip(cluster=True, no_json=True)
def testNumericOnlyGroupWithoutValues(env):
    """A document whose array is empty, or holds no number, matches no numeric group."""
    conn = createCompanies(env)
    setCompany(conn, 'empty', {'hq': 'FR', 'metrics': []})
    setCompany(conn, 'nulls', {'hq': 'FR', 'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': None}]})
    env.assertEqual(search(env, '@hq:{FR}'), ['empty', 'nulls'])
    env.assertEqual(search(env, '@metrics:(@v:[-inf +inf] @v:[-inf +inf])'),
                    ['large', 'null', 'small'])
    env.assertEqual(search(env, '-@metrics:(@v:[-inf +inf] @v:[-inf +inf])'), ['empty', 'nulls'])


@skip(cluster=True, no_json=True)
def testGroupSkipsExpiredDocument(env):
    """A document that expired after it was indexed is not returned."""
    conn = createCompanies(env)
    env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', 0)
    try:
        now = env.cmd('TIME')
        deadline = int(now[0]) * 1000 + int(now[1]) // 1000 + 100
        conn.execute_command('PEXPIREAT', 'large', deadline)
        # TIME observes the deadline without touching the key or expiring it.
        wait_for_condition(lambda: (int(env.cmd('TIME')[0]) * 1000 > deadline, {}),
                           'key TTL did not elapse')
        env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(100 +inf])'), ['null'])
    finally:
        env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', 1)


@skip(cluster=True, no_json=True)
def testGroupWithWorkers():
    """A group query runs on a worker thread."""
    env = Env(moduleArgs='WORKERS 2')
    createCompanies(env)
    env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(100 +inf])'), ['large', 'null'])


GROUP_QUERIES = [('@metrics:(@k:{headcount} @v:[(100 +inf])', ['large', 'null']),
                 ('@metrics:(@k:{headcount} @r:{FY1})', ['null']),
                 ('@metrics:(@v:[(100 +inf] @v:[-inf 200])', ['large'])]


@skip(cluster=True, no_json=True)
def testGroupOnReplica():
    """A replica gets a group from the replicated FT.CREATE and, after a full sync, from the
    RDB."""
    env = Env(useSlaves=True, forceTcp=True)
    if env.env == 'existing-env':
        env.skip()
    master, replica = env.getConnection(), env.getSlaveConnection()

    def assertReplicaHasGroup():
        env.expect('WAIT', '1', '10000').equal(1)
        env.assertEqual(replica.execute_command('FT._LIST'), ['idx'])
        res = replica.execute_command('FT.SEARCH', 'idx', '@k:{headcount} @v:[(100 +inf]',
                                      'NOCONTENT')
        env.assertEqual(sorted(res[1:]), ['large', 'null', 'small'])
        for query, expected in GROUP_QUERIES:
            res = replica.execute_command('FT.SEARCH', 'idx', query, 'NOCONTENT')
            env.assertEqual(sorted(res[1:]), expected, message=query)

    # The replica is attached before the index exists, so the index reaches it as a command.
    env.expect('WAIT', '1', '10000').equal(1)
    full_syncs = master.info('stats')['sync_full']
    createCompanies(env)
    assertReplicaHasGroup()
    env.assertEqual(master.info('stats')['sync_full'], full_syncs)

    # A replica that was a master for a while cannot continue from where it was.
    replica.execute_command('REPLICAOF', 'NO', 'ONE')
    replica.execute_command('FLUSHALL')
    replica.execute_command('REPLICAOF', '127.0.0.1', master.info('server')['tcp_port'])

    def replicaLoadedTheIndex():
        if replica.info('replication')['master_link_status'] != 'up':
            return False, {}
        return to_dict(replica.execute_command('FT.INFO', 'idx'))['indexing'] == 0, {}

    wait_for_condition(replicaLoadedTheIndex, 'the replica did not sync with the master again')
    env.assertEqual(master.info('stats')['sync_full'], full_syncs + 1)
    assertReplicaHasGroup()


@skip(cluster=True, no_json=True)
def testGroupIsCreatedAgainFromAof():
    """Loading the append-only file replays the FT.CREATE of a group, and the group is created."""
    env = Env(useAof=True)
    # With the preamble a rewrite starts the file with an RDB, which holds the index and not
    # its FT.CREATE.
    env.expect('CONFIG', 'SET', 'aof-use-rdb-preamble', 'no').ok()
    createCompanies(env)

    def restart():
        # The rewrite that the restart starts needs the preamble to save the documents.
        env.expect('CONFIG', 'SET', 'aof-use-rdb-preamble', 'yes').ok()
        env.restartAndReload()

    # The first load replays the commands. The restart rewrites the file first, so it loads
    # the index from the RDB that the rewrite made.
    for load in [lambda: env.expect('DEBUG', 'LOADAOF').ok(), restart]:
        load()
        waitForIndex(env, 'idx')
        env.assertEqual(search(env, '@k:{headcount} @v:[(100 +inf]'), ['large', 'null', 'small'])
        for query, expected in GROUP_QUERIES:
            env.assertEqual(search(env, query), expected, message=query)


@skip(no_json=True)
def testGroupOnEveryShardBeforeAndAfterReload(env):
    """On a cluster too: documents of several shards are matched, a condition that a group
    cannot hold is an error, and a reload keeps both."""
    createCompanies(env)
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        env.assertEqual(search(env, '@k:{headcount}'), ['large', 'null', 'small'])
        for query, expected in GROUP_QUERIES:
            env.assertEqual(search(env, query), expected, message=query)
        env.expect('FT.SEARCH', 'idx', '@metrics:(@k:{headcount} -@r:{FY1})').error() \
            .contains(UNSUPPORTED_IN_GROUP)


@skip(cluster=True, no_json=True)
def testAlterIndexWithGroup(env):
    """FT.ALTER adds ordinary fields to an index that has a group. The group and the new
    fields match together, before and after a reload."""
    conn = createCompanies(env)
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '$.city', 'AS', 'city', 'TAG',
               '$.founded', 'AS', 'founded', 'NUMERIC', 'SORTABLE').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '$.other', 'AS', 'metrics', 'TAG').error() \
        .contains('Duplicate NESTED group or field in schema - metrics')
    setCompany(conn, 'late', {'hq': 'FR', 'city': 'paris', 'founded': 1999,
                              'metrics': [{'k': 'headcount', 'r': 'FY0', 'v': 400}]})
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        env.assertEqual(search(env, '@metrics:(@k:{headcount} @v:[(100 +inf])'),
                        ['large', 'late', 'null'])
        env.assertEqual(search(env, '@metrics:(@k:{headcount} @r:{FY1})'), ['null'])
        env.assertEqual(search(env, '@city:{paris} @metrics:(@k:{headcount} @v:[(350 +inf])'),
                        ['late'])
        env.assertEqual(search(env, '@founded:[1999 1999] @metrics:(@k:{headcount} @r:{FY1})'), [])
        res = env.cmd('FT.SEARCH', 'idx', '@metrics:(@k:{headcount} @v:[(350 +inf])',
                      'SORTBY', 'founded', 'RETURN', '2', 'founded', 'v')
        env.assertEqual(res, [1, 'late', ['founded', '1999', 'v', '400']])


@skip(cluster=True, no_json=True)
def testGroupOverTagIndexOfSeveralBlocks(env):
    """A tag whose entries fill several blocks of its index is read through a group, both by
    reading on and by skipping to the documents that a rarer condition names."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *GROUP_SCHEMA).ok()
    conn = getConnectionByEnv(env)
    # More documents than three blocks of a tag index that records elements hold.
    num_docs, rare = 350, [5, 150, 151, 349]
    for i in range(num_docs):
        # `common` is the key of element 1 everywhere, where the value is the document's number.
        metrics = [{'k': 'other', 'r': 'FY1', 'v': 1000 + i}, {'k': 'common', 'r': 'FY0', 'v': i}]
        if i in rare:
            metrics.append({'k': 'common', 'r': 'rare', 'v': -1})
        setCompany(conn, f'doc{i:03}', {'hq': 'rare' if i in rare else 'usual', 'metrics': metrics})
    env.assertEqual(index_info(env)['hash_indexing_failures'], 0)

    def docs(numbers):
        return [f'doc{i:03}' for i in numbers]

    limit = ['LIMIT', '0', str(num_docs)]
    env.assertEqual(search(env, '@metrics:(@k:{common} @v:[300 +inf])', *limit),
                    docs(range(300, num_docs)))
    env.assertEqual(search(env, '@metrics:(@k:{common} @v:[1000 +inf])', *limit), [])
    env.assertEqual(search(env, '@metrics:(@k:{common} @r:{rare})', *limit), docs(rare))
    env.assertEqual(search(env, '@metrics:(@k:{common} @r:{rare} @v:[0 +inf])', *limit), [])
    env.assertEqual(search(env, '@hq:{rare} @metrics:(@k:{common} @v:[100 200])', *limit),
                    docs([150, 151]))
    env.assertEqual(search(env, '-@metrics:(@k:{common} @v:[2 +inf])', *limit), docs([0, 1]))


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


@skip(cluster=True, no_json=True)
def testGroupOverArrayOfHundredsOfElements(env):
    """An array of some hundred elements is matched on the one element that satisfies every
    condition of a group, also after an update that makes the array shorter."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', *GROUP_SCHEMA).ok()
    conn = getConnectionByEnv(env)
    # Element numbers from 128 need two bytes in an entry of the tag index.
    longer, shorter = 300, 200
    for num_elements in [longer, shorter]:
        last = num_elements - 1
        metrics = [{'k': 'odd' if i % 2 else 'even', 'r': 'FY0', 'v': i}
                   for i in range(num_elements)]
        setCompany(conn, 'big', {'hq': 'US', 'metrics': metrics})
        env.assertEqual(index_info(env)['hash_indexing_failures'], 0)
        env.assertEqual(search(env, f'@metrics:(@k:{{odd}} @v:[{last} {last}])'), ['big'])
        env.assertEqual(search(env, f'@metrics:(@k:{{even}} @v:[{last} {last}])'), [])
        env.assertEqual(search(env, f'@metrics:(@v:[{last} +inf] @v:[-inf {last}])'), ['big'])
        env.assertEqual(search(env, f'@metrics:(@v:[({last} +inf] @v:[-inf {last}])'), [])
    # The elements that the shorter array no longer has do not match.
    env.assertEqual(search(env, f'@metrics:(@k:{{odd}} @v:[{longer - 1} {longer - 1}])'), [])

