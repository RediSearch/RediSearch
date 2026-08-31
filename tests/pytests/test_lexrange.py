# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Lexicographic range queries on TAG and TEXT fields: `@field:>(v)`, `>=`, `<`, `<=`.

The syntax is gated behind `ENABLE_UNSTABLE_FEATURES`; see
`docs/CONTRIBUTING-unstable-features.md`.
"""

from RLTest import Env
from includes import *
from common import *


NAMES = ['alice', 'bob', 'charlie', 'dave', 'eve']


def build_index(env, field_type, *field_args):
    """Create `idx` with a single `name` field of `field_type`, holding NAMES."""
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', field_type,
               *field_args).ok()
    conn = getConnectionByEnv(env)
    for i, name in enumerate(NAMES):
        conn.execute_command('HSET', f'doc{i + 1}', 'name', name)
    return conn


def search(env, query, *args):
    """Run `query` under DIALECT 2 and return the sorted document keys it matched.

    Every query here pins the dialect because the syntax lives only in the v2
    query parser; under DIALECT 1 the operator is not part of the language.
    """
    res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', 'LIMIT', '0', '100',
                  'DIALECT', '2', *args)
    return sorted(res[1:])


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

def testLexRangeGatedOff(env):
    """With the flag off every operator is rejected, and the error says how to
    enable it.

    Silently reading `@name:>(bob)` as the pre-feature `@name:(bob)` would hand a
    client that reached for the operator a plausible but wrong result set.
    """
    run_command_on_all_shards(env, 'CONFIG', 'SET',
                              'search-enable-unstable-features', 'no')
    build_index(env, 'TEXT')

    for query in ('@name:>(bob)', '@name:>=(bob)', '@name:<(bob)', '@name:<=(bob)'):
        env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT', 'DIALECT', '2') \
            .error().contains('search-enable-unstable-features')

    # The clause without an operator is untouched.
    env.assertEqual(search(env, '@name:(bob)'), ['doc2'])


def testLexRangeNotInDialect1(env):
    """The syntax lives in the v2 parser only; DIALECT 1 never sees the operator."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    res = env.cmd('FT.SEARCH', 'idx', '@name:>(bob)', 'NOCONTENT', 'DIALECT', '1')
    env.assertEqual(res, [0])


def testLexRangeEnabled(env):
    """The same query becomes a range once the flag is on."""
    enable_unstable_features(env)
    build_index(env, 'TEXT')

    env.assertEqual(search(env, '@name:>(bob)'), ['doc3', 'doc4', 'doc5'])


# ---------------------------------------------------------------------------
# Operators, on both field types
# ---------------------------------------------------------------------------

def testTagOperators(env):
    """All four operators over a TAG field."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    env.assertEqual(search(env, '@name:>(charlie)'), ['doc4', 'doc5'])
    env.assertEqual(search(env, '@name:>=(charlie)'), ['doc3', 'doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<(charlie)'), ['doc1', 'doc2'])
    env.assertEqual(search(env, '@name:<=(charlie)'), ['doc1', 'doc2', 'doc3'])


def testTextOperators(env):
    """All four operators over a TEXT field."""
    enable_unstable_features(env)
    build_index(env, 'TEXT')

    env.assertEqual(search(env, '@name:>(charlie)'), ['doc4', 'doc5'])
    env.assertEqual(search(env, '@name:>=(charlie)'), ['doc3', 'doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<(charlie)'), ['doc1', 'doc2'])
    env.assertEqual(search(env, '@name:<=(charlie)'), ['doc1', 'doc2', 'doc3'])


def testBraceDelimiter(env):
    """`@field:>{v}` mirrors the tag-list shape and means the same as `>(v)`."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    env.assertEqual(search(env, '@name:>{charlie}'), ['doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<={charlie}'), ['doc1', 'doc2', 'doc3'])


def testBoundOutsideTheIndex(env):
    """A bound need not be an indexed value - it only has to be comparable."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    for q in ['@name:>(c)', '@name:<(c)', '@name:>(zzz)', '@name:<(zzz)']:
        try:
            print("DEBUG", q, "=>", env.cmd('FT.EXPLAIN', 'idx', q, 'DIALECT', '2'))
        except Exception as e:
            print("DEBUG", q, "=> EXC", e)
    # "c" sits between "bob" and "charlie", and is itself indexed nowhere.
    env.assertEqual(search(env, '@name:>(c)'), ['doc3', 'doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<(c)'), ['doc1', 'doc2'])

    # Bounds past either end of the data.
    env.assertEqual(search(env, '@name:>(zzz)'), [])
    env.assertEqual(search(env, '@name:<(zzz)'), ['doc1', 'doc2', 'doc3', 'doc4', 'doc5'])


def testEmptyBound(env):
    """The empty string is a bound of its own, not an unbounded side."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    # Every value sorts after the empty string.
    env.assertEqual(search(env, '@name:>("")'), ['doc1', 'doc2', 'doc3', 'doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<("")'), [])


# ---------------------------------------------------------------------------
# Bounds that are not plain words
# ---------------------------------------------------------------------------

def testQuotedBoundWithSpace(env):
    """A bound containing a space is written quoted; unquoted it is a syntax error."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'city', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc1', 'city', 'new york')
    conn.execute_command('HSET', 'doc2', 'city', 'paris')
    conn.execute_command('HSET', 'doc3', 'city', 'amsterdam')

    res = env.cmd('FT.SEARCH', 'idx', '@city:>("new york")', 'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['doc2'])

    # Two terms is not a bound: the grammar takes exactly one.
    env.expect('FT.SEARCH', 'idx', '@city:>(new york)', 'NOCONTENT', 'DIALECT', '2').error()


def testNumericLookingBound(env):
    """A bound is compared as a string even when it looks like a number."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'sku', 'TAG').ok()
    conn = getConnectionByEnv(env)
    for i, sku in enumerate(['1st', '5stars', '5zzz', '9']):
        conn.execute_command('HSET', f'doc{i + 1}', 'sku', sku)

    # Lexicographic, so "9" > "5zzz" > "5stars" > "1st".
    env.assertEqual(search(env, '@sku:>(5stars)'), ['doc3', 'doc4'])
    env.assertEqual(search(env, '@sku:>(2)'), ['doc2', 'doc3', 'doc4'])


def testParameterBound(env):
    """A bound may be a query parameter, which is what keyset pagination needs."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    res = env.cmd('FT.SEARCH', 'idx', '@name:>($cursor)', 'PARAMS', '2', 'cursor', 'charlie',
                  'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['doc4', 'doc5'])

    # A parameter value is taken literally, spaces and all.
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc6', 'name', 'zoe pratt')
    res = env.cmd('FT.SEARCH', 'idx', '@name:>=($cursor)', 'PARAMS', '2', 'cursor', 'zoe pratt',
                  'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['doc6'])


def testEscapedBound(env):
    """An escaped separator is part of the bound, not a second token."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'city', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc1', 'city', 'new york')
    conn.execute_command('HSET', 'doc2', 'city', 'paris')

    res = env.cmd('FT.SEARCH', 'idx', '@city:>(new\\ york)', 'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['doc2'])


def testTagAndTextAgreeOnPrefixBounds(env):
    """The two evaluators walk different tries; a bound that prefixes a term - or
    a term that prefixes the bound - must still order them the same way.

    That family of bounds is where the walk runs out of bound in the middle of a
    stored key, and where the two implementations could disagree.
    """
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'tag_idx', 'ON', 'HASH', 'PREFIX', '1', 'tag:',
               'SCHEMA', 'v', 'TAG').ok()
    env.expect('FT.CREATE', 'text_idx', 'ON', 'HASH', 'PREFIX', '1', 'txt:',
               'SCHEMA', 'v', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    values = ['ban', 'banana', 'band', 'bank']
    for v in values:
        conn.execute_command('HSET', f'tag:{v}', 'v', v)
        conn.execute_command('HSET', f'txt:{v}', 'v', v)

    expected = {
        # "banb" is not a value; "ban" and "banana" sort below it.
        '@v:<(banb)': ['ban', 'banana'],
        '@v:<=(banb)': ['ban', 'banana'],
        # "ban" is a value *and* a proper prefix of the other three.
        '@v:>(ban)': ['banana', 'band', 'bank'],
        '@v:>=(ban)': ['ban', 'banana', 'band', 'bank'],
        '@v:<(ban)': [],
        '@v:>(band)': ['bank'],
        '@v:<(bandana)': ['ban', 'banana', 'band'],
    }
    for query, want in expected.items():
        for idx, prefix in (('tag_idx', 'tag:'), ('text_idx', 'txt:')):
            res = env.cmd('FT.SEARCH', idx, query, 'NOCONTENT', 'LIMIT', '0', '10',
                          'DIALECT', '2')
            got = sorted(k[len(prefix):] for k in res[1:])
            env.assertEqual(got, want, message=f'{idx} {query}')


# ---------------------------------------------------------------------------
# Case handling
# ---------------------------------------------------------------------------

def testTagCaseInsensitiveByDefault(env):
    """A TAG field folds case unless declared CASESENSITIVE, and so does its bound."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc1', 'name', 'Alice')
    conn.execute_command('HSET', 'doc2', 'name', 'Zoe')

    # The bound is folded the same way the values were, so "Bob" compares as "bob".
    env.assertEqual(search(env, '@name:>(Bob)'), ['doc2'])
    env.assertEqual(search(env, '@name:>(bob)'), ['doc2'])


def testTagCaseSensitive(env):
    """A CASESENSITIVE TAG field compares bound and values byte for byte."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'name', 'TAG', 'CASESENSITIVE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc1', 'name', 'Zoe')
    conn.execute_command('HSET', 'doc2', 'name', 'alice')

    # ASCII uppercase sorts before lowercase, so 'Zoe' < 'a' < 'alice'.
    env.assertEqual(search(env, '@name:<(a)'), ['doc1'])
    env.assertEqual(search(env, '@name:>(a)'), ['doc2'])


def testTextBoundIsLowercased(env):
    """TEXT terms are indexed lowercased, so a TEXT bound is lowercased too."""
    enable_unstable_features(env)
    build_index(env, 'TEXT')

    env.assertEqual(search(env, '@name:>(Charlie)'), search(env, '@name:>(charlie)'))
    env.assertEqual(search(env, '@name:>(Charlie)'), ['doc4', 'doc5'])


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------

def testBoundedRangeFromTwoClauses(env):
    """Two clauses intersect into a range closed on both sides."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    env.assertEqual(search(env, '@name:>(bob) @name:<=(dave)'), ['doc3', 'doc4'])
    env.assertEqual(search(env, '@name:>=(bob) @name:<(dave)'), ['doc2', 'doc3'])


def testCombinedWithOtherClauses(env):
    """A range composes with the rest of the query language."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'name', 'TAG', 'age', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i, (name, age) in enumerate([('alice', 30), ('bob', 40), ('charlie', 50)]):
        conn.execute_command('HSET', f'doc{i + 1}', 'name', name, 'age', age)

    env.assertEqual(search(env, '@name:>(alice) @age:[0 45]'), ['doc2'])
    env.assertEqual(search(env, '-@name:>(alice)'), ['doc1'])
    env.assertEqual(search(env, '@name:>(bob) | @name:<(bob)'), ['doc1', 'doc3'])


def testKeysetPagination(env):
    """The pattern the feature exists for: page forward on the last key seen."""
    enable_unstable_features(env)
    build_index(env, 'TAG', 'SORTABLE')

    page = env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'SORTBY', 'name', 'ASC',
                   'LIMIT', '0', '2', 'DIALECT', '2')
    env.assertEqual(page[1:], ['doc1', 'doc2'])

    page = env.cmd('FT.SEARCH', 'idx', '@name:>(bob)', 'NOCONTENT', 'SORTBY', 'name', 'ASC',
                   'LIMIT', '0', '2', 'DIALECT', '2')
    env.assertEqual(page[1:], ['doc3', 'doc4'])

    page = env.cmd('FT.SEARCH', 'idx', '@name:>(dave)', 'NOCONTENT', 'SORTBY', 'name', 'ASC',
                   'LIMIT', '0', '2', 'DIALECT', '2')
    env.assertEqual(page[1:], ['doc5'])


def testSortableTagUsesTheIndexedValue(env):
    """A SORTABLE TAG field ranges over its indexed values, not the sorting vector."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TAG', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i, name in enumerate(NAMES):
        conn.execute_command('HSET', f'doc{i + 1}', 'name', name)

    env.assertEqual(search(env, '@name:>(charlie)'), ['doc4', 'doc5'])


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

def testWrongFieldType(env):
    """Only TAG and TEXT fields can be compared lexicographically."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'age', 'NUMERIC', 'loc', 'GEO').ok()

    env.expect('FT.SEARCH', 'idx', '@age:>(10)', 'DIALECT', '2').error().contains('TAG')
    env.expect('FT.SEARCH', 'idx', '@loc:>(10)', 'DIALECT', '2').error().contains('TAG')


def testUnknownField(env):
    enable_unstable_features(env)
    build_index(env, 'TAG')
    env.expect('FT.SEARCH', 'idx', '@nosuch:>(a)', 'DIALECT', '2').error().contains('Unknown field')


def testMissingBound(env):
    """The operator needs a bound, and the bound needs its delimiters."""
    enable_unstable_features(env)
    build_index(env, 'TAG')

    env.expect('FT.SEARCH', 'idx', '@name:>()', 'DIALECT', '2').error()
    env.expect('FT.SEARCH', 'idx', '@name:>', 'DIALECT', '2').error()
    env.expect('FT.SEARCH', 'idx', '@name:>bob', 'DIALECT', '2').error()


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

@skip(cluster=True)
def testExplain(env):
    """FT.EXPLAIN spells out which side is bounded and whether the bound is in range."""
    enable_unstable_features(env)
    build_index(env, 'TEXT')

    def explain(query):
        return env.cmd('FT.EXPLAIN', 'idx', query, 'DIALECT', '2')

    env.assertContains('LEXRANGE{(bob...+inf}', explain('@name:>(bob)'))
    env.assertContains('LEXRANGE{[bob...+inf}', explain('@name:>=(bob)'))
    env.assertContains('LEXRANGE{-inf...(bob}', explain('@name:<(bob)'))
    env.assertContains('LEXRANGE{-inf...[bob}', explain('@name:<=(bob)'))


@skip(cluster=True)
def testProfile(env):
    """The range is executed as a union of the per-term readers it expanded to."""
    enable_unstable_features(env)
    build_index(env, 'TEXT')

    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@name:>(charlie)', 'NOCONTENT',
                  'DIALECT', '2')
    env.assertContains('LEXRANGE', str(res))
