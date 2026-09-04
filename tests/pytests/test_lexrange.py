# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Lexicographic range queries on TAG fields: `@field:>{v}`, `>=`, `<`, `<=`.

The syntax is gated behind `ENABLE_UNSTABLE_FEATURES`; see
`docs/CONTRIBUTING-unstable-features.md`.
"""

from RLTest import Env
from includes import *
from common import *


NAMES = ['alice', 'bob', 'charlie', 'dave', 'eve']


def create_index(env, *schema):
    """Create an empty `idx` with `schema`, and return a connection to fill it."""
    env.flush()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', *schema).ok()
    return getConnectionByEnv(env)


def build_index(env, field_type='TAG', *field_args):
    """Create `idx` with a single `name` field of `field_type`, holding NAMES."""
    conn = create_index(env, 'name', field_type, *field_args)
    for i, name in enumerate(NAMES):
        conn.execute_command('HSET', f'doc{i + 1}', 'name', name)
    return conn


def build_city_index(env):
    """Create `idx` over a `city` TAG field holding two values, one with a space."""
    conn = create_index(env, 'city', 'TAG')
    conn.execute_command('HSET', 'doc1', 'city', 'new york')
    conn.execute_command('HSET', 'doc2', 'city', 'paris')
    return conn


def build_named_ages(env):
    """Create `idx` over a TAG name and a NUMERIC age, holding three documents."""
    conn = create_index(env, 'name', 'TAG', 'age', 'NUMERIC')
    for i, (name, age) in enumerate([('alice', 30), ('bob', 40), ('charlie', 50)]):
        conn.execute_command('HSET', f'doc{i + 1}', 'name', name, 'age', age)
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

    Silently reading `@name:>{bob}` as the pre-feature `@name:{bob}` would hand a
    client that reached for the operator a plausible but wrong result set.
    """
    run_command_on_all_shards(env, 'CONFIG', 'SET',
                              'search-enable-unstable-features', 'no')
    build_index(env)

    for query in ('@name:>{bob}', '@name:>={bob}', '@name:<{bob}', '@name:<={bob}'):
        env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT', 'DIALECT', '2') \
            .error().contains('search-enable-unstable-features')

    # The clause without an operator is untouched.
    env.assertEqual(search(env, '@name:{bob}'), ['doc2'])


def testOperatorWithoutABoundIsUnchanged(env):
    """Without a brace the operator is not the gated syntax, so it parses as it
    did before the feature existed and the gate stays quiet.

    `@name:>` is malformed whether or not the flag is on, so naming the flag
    would be wrong advice; `test_tags.py` pins it as a plain syntax error. The
    field here is TEXT because that is where the pre-feature reading of
    `@name:>bob` is a result rather than an error.
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
    build_index(env)

    # Without the operator the clause is the tag match `@name:{bob}`.
    res = env.cmd('FT.SEARCH', 'idx', '@name:>{bob}', 'NOCONTENT', 'DIALECT', '1')
    env.assertEqual(res, [1, 'doc2'])


def testLexRangeEnabled(env):
    """The same query becomes a range once the flag is on."""
    enable_unstable_features(env)
    build_index(env)

    env.assertEqual(search(env, '@name:>{bob}'), ['doc3', 'doc4', 'doc5'])


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------

def testTagOperators(env):
    """All four operators over a TAG field."""
    enable_unstable_features(env)
    build_index(env)

    env.assertEqual(search(env, '@name:>{charlie}'), ['doc4', 'doc5'])
    env.assertEqual(search(env, '@name:>={charlie}'), ['doc3', 'doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<{charlie}'), ['doc1', 'doc2'])
    env.assertEqual(search(env, '@name:<={charlie}'), ['doc1', 'doc2', 'doc3'])


def testBraceDelimitsTheBound(env):
    """Braces delimit the bound, as they do for an ordinary tag clause, and the
    parenthesized spelling is not this syntax."""
    enable_unstable_features(env)
    build_index(env)

    env.assertEqual(search(env, '@name:>{bob}'), ['doc3', 'doc4', 'doc5'])
    # Parentheses make it a text clause, which a TAG field rejects.
    env.expect('FT.SEARCH', 'idx', '@name:>(bob)', 'DIALECT', '2') \
        .error().contains('TEXT')


def testBoundOutsideTheIndex(env):
    """A bound need not be an indexed value - it only has to be comparable."""
    enable_unstable_features(env)
    build_index(env)

    # "c" sits between "bob" and "charlie", and is itself indexed nowhere.
    env.assertEqual(search(env, '@name:>{c}'), ['doc3', 'doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<{c}'), ['doc1', 'doc2'])

    # Bounds past either end of the data.
    env.assertEqual(search(env, '@name:>{zzz}'), [])
    env.assertEqual(search(env, '@name:<{zzz}'), ['doc1', 'doc2', 'doc3', 'doc4', 'doc5'])


def testEmptyBound(env):
    """The empty string is a bound of its own, not an unbounded side."""
    enable_unstable_features(env)
    build_index(env)

    # Every value sorts after the empty string.
    env.assertEqual(search(env, '@name:>{""}'), ['doc1', 'doc2', 'doc3', 'doc4', 'doc5'])
    env.assertEqual(search(env, '@name:<{""}'), [])


def testIndexedEmptyValueIsInRange(env):
    """An INDEXEMPTY field's empty value participates in the ordering.

    A zero-length key is refused by the value trie, so an indexed empty value
    lives only in its own inverted index and no range walk reaches it. It still
    sorts below every other value, so a range covering it has to include it.
    """
    enable_unstable_features(env)
    conn = create_index(env, 'tg', 'TAG', 'INDEXEMPTY')
    conn.execute_command('HSET', 'empty', 'tg', '')
    conn.execute_command('HSET', 'alice', 'tg', 'alice')

    # The empty value is at or below every bound that covers it.
    env.assertEqual(search(env, '@tg:>={""}'), ['alice', 'empty'])
    env.assertEqual(search(env, '@tg:<={""}'), ['empty'])
    env.assertEqual(search(env, '@tg:<{alice}'), ['empty'])

    # ... and out of range once the bound excludes it.
    env.assertEqual(search(env, '@tg:>{""}'), ['alice'])
    env.assertEqual(search(env, '@tg:<{""}'), [])


# ---------------------------------------------------------------------------
# Bounds that are not plain words
# ---------------------------------------------------------------------------

def testQuotedBoundWithSpace(env):
    """A bound containing a space is written quoted; unquoted it is a syntax error."""
    enable_unstable_features(env)
    conn = build_city_index(env)
    conn.execute_command('HSET', 'doc3', 'city', 'amsterdam')

    env.assertEqual(search(env, '@city:>{"new york"}'), ['doc2'])

    # Two terms is not a bound: the grammar takes exactly one.
    env.expect('FT.SEARCH', 'idx', '@city:>{new york}', 'NOCONTENT', 'DIALECT', '2').error()


def testNumericLookingBound(env):
    """A bound is compared as a string even when it looks like a number."""
    enable_unstable_features(env)
    conn = create_index(env, 'sku', 'TAG')
    for i, sku in enumerate(['1st', '5stars', '5zzz', '9']):
        conn.execute_command('HSET', f'doc{i + 1}', 'sku', sku)

    # Lexicographic, so "9" > "5zzz" > "5stars" > "1st".
    env.assertEqual(search(env, '@sku:>{5stars}'), ['doc3', 'doc4'])
    env.assertEqual(search(env, '@sku:>{2}'), ['doc2', 'doc3', 'doc4'])


def testParameterBound(env):
    """A bound may be a query parameter, which is what keyset pagination needs."""
    enable_unstable_features(env)
    build_index(env)

    env.assertEqual(search(env, '@name:>{$cursor}', 'PARAMS', '2', 'cursor', 'charlie'),
                    ['doc4', 'doc5'])

    # A parameter value is taken literally, spaces and all.
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc6', 'name', 'zoe pratt')
    env.assertEqual(search(env, '@name:>={$cursor}', 'PARAMS', '2', 'cursor', 'zoe pratt'),
                    ['doc6'])


def testEscapedBound(env):
    """An escaped separator is part of the bound, not a second token."""
    enable_unstable_features(env)
    conn = create_index(env, 'city', 'TAG')
    conn.execute_command('HSET', 'doc1', 'city', 'new york')
    conn.execute_command('HSET', 'doc2', 'city', 'paris')

    env.assertEqual(search(env, '@city:>{new\\ york}'), ['doc2'])


def testPrefixBounds(env):
    """A bound that prefixes a value, or a value that prefixes the bound, must
    still order the two correctly.

    That family of bounds is where the walk runs out of bound in the middle of a
    stored key, which is the case the trie descent is easiest to get wrong.
    """
    enable_unstable_features(env)
    conn = create_index(env, 'v', 'TAG')
    for v in ['ban', 'banana', 'band', 'bank']:
        conn.execute_command('HSET', v, 'v', v)

    # (operator, bound) -> the values in range.
    expected = {
        # "banb" is not a value; "ban" and "banana" sort below it.
        ('<', 'banb'): ['ban', 'banana'],
        ('<=', 'banb'): ['ban', 'banana'],
        # "ban" is a value *and* a proper prefix of the other three.
        ('>', 'ban'): ['banana', 'band', 'bank'],
        ('>=', 'ban'): ['ban', 'banana', 'band', 'bank'],
        ('<', 'ban'): [],
        ('>', 'band'): ['bank'],
        ('<', 'bandana'): ['ban', 'banana', 'band'],
    }
    for (op, value), want in expected.items():
        query = f'@v:{op}{{{value}}}'
        env.assertEqual(search(env, query), want, message=query)


# ---------------------------------------------------------------------------
# Case handling
# ---------------------------------------------------------------------------

def testTagCaseInsensitiveByDefault(env):
    """A TAG field folds case unless declared CASESENSITIVE, and so does its bound."""
    enable_unstable_features(env)
    conn = create_index(env, 'name', 'TAG')
    conn.execute_command('HSET', 'doc1', 'name', 'Alice')
    conn.execute_command('HSET', 'doc2', 'name', 'Zoe')

    # The bound is folded the same way the values were, so "Bob" compares as "bob".
    env.assertEqual(search(env, '@name:>{Bob}'), ['doc2'])
    env.assertEqual(search(env, '@name:>{bob}'), ['doc2'])


def testTagCaseSensitive(env):
    """A CASESENSITIVE TAG field compares bound and values byte for byte."""
    enable_unstable_features(env)
    conn = create_index(env, 'name', 'TAG', 'CASESENSITIVE')
    conn.execute_command('HSET', 'doc1', 'name', 'Zoe')
    conn.execute_command('HSET', 'doc2', 'name', 'alice')

    # ASCII uppercase sorts before lowercase, so 'Zoe' < 'a' < 'alice'.
    env.assertEqual(search(env, '@name:<{a}'), ['doc1'])
    env.assertEqual(search(env, '@name:>{a}'), ['doc2'])


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------

def testBoundedRangeFromTwoClauses(env):
    """Two clauses intersect into a range closed on both sides."""
    enable_unstable_features(env)
    build_index(env)

    env.assertEqual(search(env, '@name:>{bob} @name:<={dave}'), ['doc3', 'doc4'])
    env.assertEqual(search(env, '@name:>={bob} @name:<{dave}'), ['doc2', 'doc3'])


def testCombinedWithOtherClauses(env):
    """A range composes with the rest of the query language."""
    enable_unstable_features(env)
    build_named_ages(env)

    env.assertEqual(search(env, '@name:>{alice} @age:[0 45]'), ['doc2'])
    env.assertEqual(search(env, '-@name:>{alice}'), ['doc1'])
    env.assertEqual(search(env, '@name:>{bob} | @name:<{bob}'), ['doc1', 'doc3'])


def testKeysetPagination(env):
    """The pattern the feature exists for: page forward on the last key seen."""
    enable_unstable_features(env)
    build_index(env, 'TAG', 'SORTABLE')

    page = env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'SORTBY', 'name', 'ASC',
                   'LIMIT', '0', '2', 'DIALECT', '2')
    env.assertEqual(page[1:], ['doc1', 'doc2'])

    page = env.cmd('FT.SEARCH', 'idx', '@name:>{bob}', 'NOCONTENT', 'SORTBY', 'name', 'ASC',
                   'LIMIT', '0', '2', 'DIALECT', '2')
    env.assertEqual(page[1:], ['doc3', 'doc4'])

    page = env.cmd('FT.SEARCH', 'idx', '@name:>{dave}', 'NOCONTENT', 'SORTBY', 'name', 'ASC',
                   'LIMIT', '0', '2', 'DIALECT', '2')
    env.assertEqual(page[1:], ['doc5'])


def testSortableTagUsesTheIndexedValue(env):
    """A SORTABLE TAG field ranges over its indexed values, not the sorting vector."""
    enable_unstable_features(env)
    build_index(env, 'TAG', 'SORTABLE')

    env.assertEqual(search(env, '@name:>{charlie}'), ['doc4', 'doc5'])


# The cap is per shard, so a cluster admits one expansion per shard and the
# coordinator unions them; the counts below only hold for a single shard.
@skip(cluster=True)
def testExpansionCapStopsTheWalk():
    """Past MAXPREFIXEXPANSIONS the walk stops and says so.

    An unbounded range over a high-cardinality field is unbounded work, so the
    cap has to end the walk rather than merely decline to open further readers,
    and the truncation has to be reported rather than silently returning fewer
    documents.
    """
    env = Env(protocol=3)
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'name', 'TAG').ok()
    for i in range(20):
        conn.execute_command('HSET', f'doc{i:03d}', 'name', f'v{i:03d}')

    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '5')
    try:
        res = env.cmd('FT.SEARCH', 'idx', '@name:>{""}', 'NOCONTENT',
                      'LIMIT', '0', '100', 'DIALECT', '2')
        env.assertContains('Max prefix expansions limit was reached', res['warning'])
        env.assertEqual(res['total_results'], 5, message=res)
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
    build_index(env)

    def scores(query):
        res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', 'WITHSCORES',
                      'SCORER', 'DISMAX', 'DIALECT', '2')
        return sorted(float(x) for x in res[2::2])

    # Two matching values, so a real union is built rather than a collapsed one.
    env.assertEqual(scores('@name:>{charlie}'), [1.0, 1.0])
    env.assertEqual(scores('@name:>{charlie}=>{$weight: 2.0}'), [2.0, 2.0])


def testLiteralBoundAlongsideAnotherParameter(env):
    """A literal range must not leave an empty parameter slot behind.

    A tag node retags every slot of its children as a real parameter, so a
    leftover slot with no name crashed the server once any other parameter in the
    query made resolution walk the tree.
    """
    enable_unstable_features(env)
    build_named_ages(env)

    env.assertEqual(search(env, '@name:>{bob} @age>=$min', 'PARAMS', '2', 'min', '0'),
                    ['doc3'])


def assertNulBoundTruncates(env):
    """Assert `ab\\0z` is read as the bound `ab` on the `tg` field.

    `plain` holds `ab` and `nul` holds `ab\\0z`, which the indexer files under
    `ab` as well, so a strict upper bound of `ab` excludes both and an inclusive
    one takes both.
    """
    env.assertEqual(search(env, '@tg:<{$cursor}', 'PARAMS', '2', 'cursor', 'ab\0z'), [])
    env.assertEqual(search(env, '@tg:<={$cursor}', 'PARAMS', '2', 'cursor', 'ab\0z'),
                    ['nul', 'plain'])


def testInteriorNulTruncatesBoundAndValueAlike(env):
    """An interior NUL ends a bound, exactly as it ends an indexed value.

    The indexer files `ab\\0z` under `ab`, and an exact-match parameter truncates
    the same way, so a range bound has to as well: keeping the suffix would
    compare against keys that cannot exist.
    """
    enable_unstable_features(env)
    conn = create_index(env, 'tg', 'TAG')
    conn.execute_command('HSET', 'plain', 'tg', 'ab')
    conn.execute_command('HSET', 'nul', 'tg', 'ab\0z')

    # Both documents index under "ab", which an exact match confirms.
    env.assertEqual(search(env, '@tg:{ab}'), ['nul', 'plain'])
    assertNulBoundTruncates(env)


def testTagParameterBoundIsUnescapedLikeATagToken(env):
    """A TAG parameter is read as query-syntax text, backslashes and all, and a
    range bound agrees with an exact match about that.

    `tag_strtolower` runs the escape-removal pass on both, so the parameter
    `a\\!` names the indexed value `a!` rather than the literal `a\\!`. That is
    arguably the wrong reading of a parameter, but it is TAG-wide rather than
    specific to ranges, so this pins the two constructs agreeing rather than
    asserting either is right. Changing it means changing `@tag:{$p}` too.
    """
    enable_unstable_features(env)
    conn = create_index(env, 'tg', 'TAG', 'CASESENSITIVE')
    conn.execute_command('HSET', 'bang', 'tg', 'a!')
    conn.execute_command('HSET', 'esc', 'tg', 'a\\!')
    conn.execute_command('HSET', 'zed', 'tg', 'z')

    # An exact match on the parameter resolves it to "a!", not the literal "a\\!"
    # that the index holds separately...
    env.assertEqual(search(env, '@tg:{$p}', 'PARAMS', '2', 'p', 'a\\!'), ['bang'])

    # ... and a range bound resolves it the same way, so everything above "a!"
    # matches, including the backslashed value.
    env.assertEqual(search(env, '@tg:>{$p}', 'PARAMS', '2', 'p', 'a\\!'), ['esc', 'zed'])


def testInteriorNulTruncatesCaseSensitiveTagBound(env):
    """The same truncation on a CASESENSITIVE TAG field.

    `tag_strtolower` folds case and reports the truncated length together; with
    folding skipped, the length has to come from the truncated content instead.
    """
    enable_unstable_features(env)
    conn = create_index(env, 'tg', 'TAG', 'CASESENSITIVE')
    conn.execute_command('HSET', 'plain', 'tg', 'ab')
    conn.execute_command('HSET', 'nul', 'tg', 'ab\0z')

    assertNulBoundTruncates(env)


def testEscapedBoundIsNotUnescapedTwice(env):
    """A TAG bound is unescaped once, by the tag evaluator, as a tag token is."""
    enable_unstable_features(env)
    conn = create_index(env, 'v', 'TAG')
    conn.execute_command('HSET', 'bang', 'v', 'a!')
    conn.execute_command('HSET', 'zed', 'v', 'z')

    # `a\!` in the query text is the value `a!`, so a strict lower bound on it
    # excludes the document holding exactly that value.
    env.assertEqual(search(env, '@v:>{a\\!}'), ['zed'])
    env.assertEqual(search(env, '@v:>={a\\!}'), ['bang', 'zed'])


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

def testWrongFieldType(env):
    """Only TAG fields can be compared lexicographically."""
    enable_unstable_features(env)
    create_index(env, 'age', 'NUMERIC', 'loc', 'GEO', 'txt', 'TEXT')

    for field in ('age', 'loc', 'txt'):
        env.expect('FT.SEARCH', 'idx', f'@{field}:>{{10}}', 'DIALECT', '2') \
            .error().contains('TAG')


def testUnknownField(env):
    enable_unstable_features(env)
    build_index(env)
    env.expect('FT.SEARCH', 'idx', '@nosuch:>{a}', 'DIALECT', '2').error().contains('Unknown field')


def testMissingBound(env):
    """The operator needs a bound, and the bound needs its braces."""
    enable_unstable_features(env)
    build_index(env)

    env.expect('FT.SEARCH', 'idx', '@name:>{}', 'DIALECT', '2').error()
    env.expect('FT.SEARCH', 'idx', '@name:>', 'DIALECT', '2').error()

    # `@name:>bob` is not this grammar at all: with no brace the operator is
    # dropped and the clause reads as `@name:(bob)`, which a TAG field rejects as
    # a text clause. `testOperatorWithoutABoundIsUnchanged` shows the same
    # spelling on a TEXT field, where it is a plain term search rather than an
    # error.
    env.expect('FT.SEARCH', 'idx', '@name:>bob', 'DIALECT', '2').error().contains('TEXT')


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

@skip(cluster=True)
def testExplain(env):
    """FT.EXPLAIN spells out which side is bounded and whether the bound is in range."""
    enable_unstable_features(env)
    build_index(env)

    def explain(query):
        return env.cmd('FT.EXPLAIN', 'idx', query, 'DIALECT', '2')

    env.assertContains('LEXRANGE{(bob...+inf}', explain('@name:>{bob}'))
    env.assertContains('LEXRANGE{[bob...+inf}', explain('@name:>={bob}'))
    env.assertContains('LEXRANGE{-inf...(bob}', explain('@name:<{bob}'))
    env.assertContains('LEXRANGE{-inf...[bob}', explain('@name:<={bob}'))


@skip(cluster=True)
def testProfile(env):
    """The range is executed as a union of the per-value readers it expanded to."""
    enable_unstable_features(env)
    build_index(env)

    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@name:>{charlie}', 'NOCONTENT',
                  'DIALECT', '2')
    env.assertContains('LEXRANGE', str(res))
