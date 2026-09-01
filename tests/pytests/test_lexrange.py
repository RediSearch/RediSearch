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


def testOperatorWithoutABoundIsUnchanged(env):
    """Without a bound delimiter the operator is not the gated syntax, so it
    parses as it did before the feature existed and the gate stays quiet.

    `@name:>` is malformed whether or not the flag is on, so naming the flag
    would be wrong advice; `test_tags.py` pins it as a plain syntax error.
    """
    build_index(env, 'TEXT')

    for enabled in ('no', 'yes'):
        run_command_on_all_shards(env, 'CONFIG', 'SET',
                                  'search-enable-unstable-features', enabled)
        env.expect('FT.SEARCH', 'idx', '@name:>', 'DIALECT', '2') \
            .error().contains('Syntax error')
        # A bare word after the operator is not the supported spelling either, so
        # the operator is dropped and the clause reads as `@name:(bob)`.
        env.assertEqual(search(env, '@name:>bob'), ['doc2'], message=enabled)


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


def testIndexedEmptyValueIsInRange(env):
    """An INDEXEMPTY field's empty value participates in the ordering.

    A zero-length key is refused by the tries, so an indexed empty value lives
    only in its own inverted index and no range walk reaches it. It still sorts
    below every other value, so a range covering it has to include it.
    """
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'txt', 'TEXT', 'INDEXEMPTY', 'tg', 'TAG', 'INDEXEMPTY').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'empty', 'txt', '', 'tg', '')
    conn.execute_command('HSET', 'alice', 'txt', 'alice', 'tg', 'alice')

    for field in ('txt', 'tg'):
        # The empty value is at or below every bound that covers it.
        env.assertEqual(search(env, f'@{field}:>=("")'), ['alice', 'empty'],
                        message=field)
        env.assertEqual(search(env, f'@{field}:<=("")'), ['empty'], message=field)
        env.assertEqual(search(env, f'@{field}:<(alice)'), ['empty'], message=field)

        # ... and out of range once the bound excludes it.
        env.assertEqual(search(env, f'@{field}:>("")'), ['alice'], message=field)
        env.assertEqual(search(env, f'@{field}:<("")'), [], message=field)


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


# The cap is per shard, so a cluster admits one expansion per shard and the
# coordinator unions them; the count below only holds for a single shard.
@skip(cluster=True)
def testExpansionCapCountsTheEmptyTerm(env):
    """The empty term is one of the terms in the range, so it is capped like any
    other rather than appended past the limit."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'txt', 'TEXT', 'INDEXEMPTY').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'empty', 'txt', '')
    conn.execute_command('HSET', 'alice', 'txt', 'alice')
    conn.execute_command('HSET', 'bob', 'txt', 'bob')

    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')
    try:
        # The empty term sorts first, so it is the one expansion admitted.
        env.assertEqual(search(env, '@txt:>=("")'), ['empty'])
    finally:
        run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '200')


@skip(cluster=True)
def testRangeWeightAppliedOnce(env):
    """A weighted TAG range must not multiply the weight into both the child
    readers and the union above them.

    DISMAX scores a match as its weight, so the expected numbers are exact: the
    weight applied twice would read 4 rather than 2.
    """
    enable_unstable_features(env)
    build_index(env, 'TAG')

    def scores(query):
        res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', 'WITHSCORES',
                      'SCORER', 'DISMAX', 'DIALECT', '2')
        return sorted(float(x) for x in res[2::2])

    # Two matching values, so a real union is built rather than a collapsed one.
    env.assertEqual(scores('@name:>(charlie)'), [1.0, 1.0])
    env.assertEqual(scores('@name:>(charlie)=>{$weight: 2.0}'), [2.0, 2.0])


def testLiteralBoundAlongsideAnotherParameter(env):
    """A literal range must not leave an empty parameter slot behind.

    A tag node retags every slot of its children as a real parameter, so a
    leftover slot with no name crashed the server once any other parameter in the
    query made resolution walk the tree.
    """
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'name', 'TAG', 'age', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i, (name, age) in enumerate([('alice', 30), ('bob', 40), ('charlie', 50)]):
        conn.execute_command('HSET', f'doc{i + 1}', 'name', name, 'age', age)

    res = env.cmd('FT.SEARCH', 'idx', '@name:>(bob) @age>=$min', 'PARAMS', '2', 'min', '0',
                  'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['doc3'])

    # The same shape on a TEXT field, which does not go through the tag retag.
    env.expect('FT.CREATE', 'txt_idx', 'ON', 'HASH', 'PREFIX', '1', 'doc',
               'SCHEMA', 'name', 'TEXT', 'age', 'NUMERIC').ok()
    # The documents predate this index, so it is filled by a background backfill.
    waitForIndex(env, 'txt_idx')
    res = env.cmd('FT.SEARCH', 'txt_idx', '@name:>(bob) @age>=$min', 'PARAMS', '2', 'min', '0',
                  'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['doc3'])


def testInteriorNulTruncatesBoundAndValueAlike(env):
    """An interior NUL ends a bound, exactly as it ends an indexed value.

    The indexer files `ab\\0z` under `ab` for both field types, and an
    exact-match parameter truncates the same way, so a range bound has to as
    well: keeping the suffix would compare against keys that cannot exist.
    """
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'txt', 'TEXT', 'tg', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'plain', 'txt', 'ab', 'tg', 'ab')
    conn.execute_command('HSET', 'nul', 'txt', 'ab\0z', 'tg', 'ab\0z')

    for field in ('txt', 'tg'):
        # Both documents index under "ab", which an exact match confirms.
        exact = env.cmd('FT.SEARCH', 'idx', f'@{field}:{{ab}}' if field == 'tg'
                        else f'@{field}:(ab)', 'NOCONTENT', 'DIALECT', '2')
        env.assertEqual(sorted(exact[1:]), ['nul', 'plain'], message=field)

        # So a bound of "ab\0z" is the bound "ab", and both sides agree on it.
        below = env.cmd('FT.SEARCH', 'idx', f'@{field}:<($cursor)',
                        'PARAMS', '2', 'cursor', 'ab\0z',
                        'NOCONTENT', 'LIMIT', '0', '100', 'DIALECT', '2')
        env.assertEqual(below[1:], [], message=field)

        at_or_below = env.cmd('FT.SEARCH', 'idx', f'@{field}:<=($cursor)',
                              'PARAMS', '2', 'cursor', 'ab\0z',
                              'NOCONTENT', 'LIMIT', '0', '100', 'DIALECT', '2')
        env.assertEqual(sorted(at_or_below[1:]), ['nul', 'plain'], message=field)


def testInteriorNulTruncatesCaseSensitiveTagBound(env):
    """The same truncation on a CASESENSITIVE TAG field.

    `tag_strtolower` folds case and reports the truncated length together; with
    folding skipped, the length has to come from the truncated content instead.
    """
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               'tg', 'TAG', 'CASESENSITIVE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'plain', 'tg', 'ab')
    conn.execute_command('HSET', 'nul', 'tg', 'ab\0z')

    # Both index under "ab", so a bound of "ab\0z" is the bound "ab".
    below = env.cmd('FT.SEARCH', 'idx', '@tg:<($cursor)', 'PARAMS', '2', 'cursor', 'ab\0z',
                    'NOCONTENT', 'LIMIT', '0', '100', 'DIALECT', '2')
    env.assertEqual(below[1:], [])

    at_or_below = env.cmd('FT.SEARCH', 'idx', '@tg:<=($cursor)', 'PARAMS', '2', 'cursor', 'ab\0z',
                          'NOCONTENT', 'LIMIT', '0', '100', 'DIALECT', '2')
    env.assertEqual(sorted(at_or_below[1:]), ['nul', 'plain'])


def testEscapedBoundIsNotUnescapedTwice(env):
    """A TAG bound is unescaped once, by the tag evaluator, as a tag token is."""
    enable_unstable_features(env)
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'v', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'bang', 'v', 'a!')
    conn.execute_command('HSET', 'zed', 'v', 'z')

    # `a\!` in the query text is the value `a!`, so a strict lower bound on it
    # excludes the document holding exactly that value.
    res = env.cmd('FT.SEARCH', 'idx', '@v:>(a\\!)', 'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['zed'])
    res = env.cmd('FT.SEARCH', 'idx', '@v:>=(a\\!)', 'NOCONTENT', 'DIALECT', '2')
    env.assertEqual(sorted(res[1:]), ['bang', 'zed'])


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

    # `@name:>bob` is not this grammar at all: with no delimiter the operator is
    # dropped and the clause reads as `@name:(bob)`, which a TAG field rejects as
    # a text clause. `testOperatorWithoutABoundIsUnchanged` shows the TEXT side,
    # where the same spelling is a plain term search rather than an error.
    env.expect('FT.SEARCH', 'idx', '@name:>bob', 'DIALECT', '2').error().contains('TEXT')


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
