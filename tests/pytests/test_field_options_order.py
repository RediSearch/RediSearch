# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *


@skip(cluster=True)
def testTypeOptionsAfterGenericOptions(env):
    """A field's type-specific options parse the same whether they come before,
    between or after SORTABLE and NOINDEX."""
    # (field type, options in the order that always parsed, the same options reordered)
    cases = [
        ('TAG', ['CASESENSITIVE', 'SORTABLE'], ['SORTABLE', 'CASESENSITIVE']),
        ('TAG', ['SEPARATOR', ';', 'INDEXEMPTY', 'INDEXMISSING', 'SORTABLE'],
                ['SORTABLE', 'SEPARATOR', ';', 'INDEXEMPTY', 'INDEXMISSING']),
        ('TAG', ['CASESENSITIVE', 'SEPARATOR', ';', 'SORTABLE'],
                ['CASESENSITIVE', 'SORTABLE', 'SEPARATOR', ';']),
        ('TAG', ['WITHSUFFIXTRIE', 'SORTABLE', 'UNF', 'NOINDEX'],
                ['SORTABLE', 'UNF', 'NOINDEX', 'WITHSUFFIXTRIE']),
        ('TEXT', ['NOSTEM', 'WEIGHT', '2', 'PHONETIC', 'dm:en', 'SORTABLE'],
                 ['SORTABLE', 'NOSTEM', 'WEIGHT', '2', 'PHONETIC', 'dm:en']),
        ('TEXT', ['WITHSUFFIXTRIE', 'INDEXEMPTY', 'INDEXMISSING', 'SORTABLE', 'UNF'],
                 ['INDEXEMPTY', 'SORTABLE', 'UNF', 'WITHSUFFIXTRIE', 'INDEXMISSING']),
        ('TEXT', ['NOSTEM', 'NOINDEX'], ['NOINDEX', 'NOSTEM']),
        ('NUMERIC', ['INDEXMISSING', 'SORTABLE'], ['SORTABLE', 'INDEXMISSING']),
        ('GEO', ['INDEXMISSING', 'SORTABLE'], ['SORTABLE', 'INDEXMISSING']),
    ]
    for i, (field_type, canonical, reordered) in enumerate(cases):
        # The trailing field checks that parsing resumes at the next field.
        env.expect('FT.CREATE', f'canonical{i}', 'SCHEMA',
                   'f', field_type, *canonical, 'n', 'NUMERIC').ok()
        env.expect('FT.CREATE', f'reordered{i}', 'SCHEMA',
                   'f', field_type, *reordered, 'n', 'NUMERIC').ok()
        expected = index_info(env, f'canonical{i}')['attributes']
        env.assertEqual(len(expected), 2, message=expected)
        env.assertEqual(index_info(env, f'reordered{i}')['attributes'], expected,
                        message=(field_type, reordered))


@skip(cluster=True)
def testCaseSensitiveAfterSortable(env):
    """CASESENSITIVE after SORTABLE makes the tag case sensitive and, as when it
    comes first, implies UNF."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'SORTABLE', 'CASESENSITIVE').ok()
    attrs = index_info(env)['attributes']
    env.assertEqual(attrs, [['identifier', 't', 'attribute', 't', 'type', 'TAG',
                             'SEPARATOR', ',', 'CASESENSITIVE', 'SORTABLE', 'UNF']])

    conn.execute_command('HSET', 'doc1', 't', 'Foo')
    env.expect('FT.SEARCH', 'idx', '@t:{Foo}', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.SEARCH', 'idx', '@t:{foo}', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testTypeOptionErrorsAfterGenericOptions(env):
    """A type-specific option after SORTABLE or NOINDEX reports its own error
    rather than being taken for the next field's name."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'SORTABLE', 'SEPARATOR', 'ab') \
        .error().contains('Tag separator must be a single character')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'SORTABLE', 'SEPARATOR') \
        .error().contains('SEPARATOR requires an argument')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE', 'WEIGHT', 'heavy') \
        .error().contains('Bad arguments for weight')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'NOINDEX', 'INDEXMISSING') \
        .error().contains('Field cannot be defined with both `NOINDEX` and `INDEXMISSING`')
    # An argument that is not an option of the field's type still starts a new field.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 'CASESENSITIVE') \
        .error().contains('Field `CASESENSITIVE` does not have a type')
