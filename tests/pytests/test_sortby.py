# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from cmath import inf
from email import message

from contextlib import contextmanager

from common import *
from includes import *
from RLTest import Env


def check_order(env, item1, item2, asc=True):
    if not asc:
        item1, item2 = item2, item1

    try:
        item1 = float(item1)
    except:
        item1 = -inf if "-inf" in item1 else inf
    try:
        item1 = float(item1)
    except:
        item2 = -inf if "-inf" in item2 else inf

    if float(item1) > float(item2):
        env.assertTrue(float(item1) <= float(item2))
        return False
    return True


# check order within returned results
def check_sortby(env, query, params, msg=None):
    cmds = ["ft.search", "ft.aggregate"]
    idx = 2 if query[0] == cmds[0] else 1
    msg = cmds[idx % 2] + " limit %d %d : " % (params[1], params[2]) + msg

    sort_order = ["ASC", "DESC"]
    for sort in range(len(sort_order)):
        print_err = False
        res = env.cmd(*query, sort_order[sort], *params)

        # put all `n` values into a list
        res_list = [to_dict(n)["n"] for n in res[idx::idx]]
        err_msg = msg + " : " + sort_order[sort] + f" : len={len(res_list)}"

        for i in range(len(res_list) - 1):
            if not check_order(
                env, res_list[i], res_list[i + 1], sort_order[sort] == sort_order[0]
            ):
                print_err = True

        if print_err:
            if (len(res)) < 100:
                env.debugPrint(str(res), force=TEST_DEBUG)
                env.debugPrint(str(res_list), force=TEST_DEBUG)
                input("stop")

        env.assertFalse(print_err, message=err_msg)


# check ASC vs DESC
# number of result must be less than limit
def compare_asc_desc(env, query, params, msg=None):
    asc_res = env.cmd(*query, "ASC", *params)[1:]
    desc_res = env.cmd(*query, "DESC", *params)[1:]
    # env.debugPrint(str(asc_res), force=TEST_DEBUG)
    # env.debugPrint(str(desc_res), force=TEST_DEBUG)

    desc_res.reverse()
    cmp_res = []
    for i, j in zip(desc_res[0::2], desc_res[1::2]):
        cmp_res.extend([j, i])
    # env.debugPrint(str(cmp_res), force=TEST_DEBUG)

    failed = False
    for i in range(1, len(asc_res), 2):
        if asc_res[i][1] != cmp_res[i][1]:
            env.assertEqual(asc_res[i][1], cmp_res[i][1], message=query)
    env.assertFalse(failed, message=query)


def testSortby(env):
    if SANITIZER:
        repeat = 1000  # TODO: get back to 10000
    else:
        repeat = 10000
    conn = getConnectionByEnv(env)
    env.cmd("FT.CREATE", "idx", "SCHEMA", "n", "NUMERIC", "t", "TEXT", "tag", "TAG")
    env.cmd(
        "FT.CREATE",
        "idx_sortable",
        "SCHEMA",
        "n",
        "NUMERIC",
        "SORTABLE",
        "t",
        "TEXT",
        "tag",
        "TAG",
    )

    words = ["hello", "world", "foo", "bar", "baz"]
    for i in range(0, repeat, len(words)):
        for j in range(len(words)):
            conn.execute_command(
                "hset", i + j, "t", words[j], "tag", words[j], "n", i % 1000 + j
            )

    # with inf values
    for j in range(len(words)):
        conn.execute_command(
            "hset", repeat + j, "t", words[j], "tag", words[j], "n", "inf"
        )
    for j in range(len(words)):
        conn.execute_command(
            "hset", repeat + 5 + j, "t", words[j], "tag", words[j], "n", "+inf"
        )
    for j in range(len(words)):
        conn.execute_command(
            "hset", repeat + 10 + j, "t", words[j], "tag", words[j], "n", "-inf"
        )

    limits = [
        [0, 5],
        [0, 30],
        [0, 150],
        [5, 5],
        [20, 30],
        [100, 10],
        [500, 100],
        [5000, 1000],
        [9900, 1010],
        [0, 100000],
    ]
    ranges = [
        ["-5", "105"],
        ["0", "3"],
        ["30", "60"],
        ["-10", "5"],
        ["950", "1100"],
        ["2000", "3000"],
        ["42", "42"],
        ["-inf", "+inf"],
        ["-inf", "100"],
        ["990", "inf"],
    ]
    params = ["limit", 0, 0]

    for _ in env.reloadingIterator():
        for i in range(len(limits)):
            params[1] = limits[i][0]
            params[2] = limits[i][1]
            for j in range(len(ranges)):
                numRange = str(f"@n:[{ranges[j][0]} {ranges[j][1]}]")

                ### (1) TEXT and range with sort ###
                check_sortby(
                    env,
                    ["ft.search", "idx", "foo " + numRange, "SORTBY", "n"],
                    params,
                    "case 1 " + numRange,
                )

                ### (3) TAG and range with sort ###
                check_sortby(
                    env,
                    ["ft.search", "idx", "@tag:{foo} " + numRange, "SORTBY", "n"],
                    params,
                    "case 3 " + numRange,
                )

                ### (5) numeric range with sort ###
                check_sortby(
                    env,
                    ["ft.search", "idx", numRange, "SORTBY", "n"],
                    params,
                    "case 5 " + numRange,
                )

            ### (7) filter with sort ###
            # Search only minimal number of ranges
            check_sortby(
                env, ["ft.search", "idx", "foo", "SORTBY", "n"], params, "case 7"
            )

            ### (9) no sort, no score, with sortby ###
            # Search only minimal number of ranges
            check_sortby(
                env, ["ft.search", "idx", "@tag:{foo}", "SORTBY", "n"], params, "case 9"
            )

            ### (11) wildcard with sort ###
            # Search only minimal number of ranges
            check_sortby(
                env, ["ft.search", "idx", "*", "SORTBY", "n"], params, "case 11"
            )

    # update parameters for ft.aggregate
    params = ["limit", 0, 0, "LOAD", 4, "@__key", "@n", "@t", "@tag"]

    for _ in env.reloadingIterator():
        for i in range(len(limits)):
            params[1] = limits[i][0]
            params[2] = limits[i][1]

            for j in range(len(ranges)):
                numRange = f"@n:[{ranges[j][0]} {ranges[j][1]}]"

                ### (1) TEXT and range with sort ###
                check_sortby(
                    env,
                    ["ft.aggregate", "idx", "foo " + numRange, "SORTBY", 2, "@n"],
                    params,
                    "case 1 " + numRange,
                )

                ### (3) TAG and range with sort ###
                check_sortby(
                    env,
                    [
                        "ft.aggregate",
                        "idx",
                        "@tag:{foo} " + numRange,
                        "SORTBY",
                        2,
                        "@n",
                    ],
                    params,
                    "case 3 " + numRange,
                )

                ### (5) numeric range with sort ###
                check_sortby(
                    env,
                    ["ft.aggregate", "idx", numRange, "SORTBY", 2, "@n"],
                    params,
                    "case 5 " + numRange,
                )

            ### (7) filter with sort ###
            # aggregate only minimal number of ranges
            check_sortby(
                env, ["ft.aggregate", "idx", "foo", "SORTBY", 2, "@n"], params, "case 7"
            )

            ### (9) no sort, no score, with sortby ###
            # aggregate only minimal number of ranges
            check_sortby(
                env,
                ["ft.aggregate", "idx", "@tag:{foo}", "SORTBY", 2, "@n"],
                params,
                "case 9",
            )

            ### (11) wildcard with sort ###
            # aggregate only minimal number of ranges
            check_sortby(
                env, ["ft.aggregate", "idx", "*", "SORTBY", 2, "@n"], params, "case 11"
            )

    # with inf values
    params = ["limit", 0, 100, "return", 1, "n"]
    compare_asc_desc(
        env, ["ft.search", "idx", "foo @n:[995 inf]", "SORTBY", "n"], params
    )
    compare_asc_desc(env, ["ft.search", "idx", "@n:[999 inf]", "SORTBY", "n"], params)
    compare_asc_desc(
        env, ["ft.search", "idx", "foo @n:[-inf 5]", "SORTBY", "n"], params
    )
    compare_asc_desc(env, ["ft.search", "idx", "@n:[-inf 1]", "SORTBY", "n"], params)
    params[2] = 10015
    compare_asc_desc(
        env, ["ft.search", "idx", "foo @n:[-inf inf]", "SORTBY", "n"], params
    )
    compare_asc_desc(env, ["ft.search", "idx", "@n:[-inf inf]", "SORTBY", "n"], params)


@skip(cluster=True)
def testSortableInvalidUtf8DoesNotCrash(env):
    # regression test for denial-of-service report
    # a schema that is TEXT SORTABLE _must_ be UTF8 for searching to work correctly
    # and storing malformed document is technically user-error, but it may happen in practice
    # and we must make sure to not crash because of it
    conn = getConnectionByEnv(env)
    env.expect("FT.CREATE", "idx", "SCHEMA", "t", "TEXT", "SORTABLE").ok()
    conn.execute_command("HSET", "doc1", "t", "foo")
    conn.execute_command("HSET", "doc2", "t", b"\xff")

    # the malformed input will result in an indexing error
    errors = index_errors(env, "idx")
    env.assertEqual(errors["indexing failures"], 1)
    env.assertContains("Invalid UTF-8", errors["last indexing error"])
    env.assertEqual(errors["last indexing error key"], "doc2")

    # and the offeding document is not included in search results
    env.expect("FT.SEARCH", "idx", "*", "NOCONTENT").equal([1, "doc1"])


@skip(cluster=True)
def testSortableTagInvalidUtf8DoesNotCrash(env):
    conn = getConnectionByEnv(env)
    env.expect("FT.CREATE", "idx_tag", "SCHEMA", "tag", "TAG", "SORTABLE").ok()
    conn.execute_command("HSET", "doc1", "tag", "foo")
    conn.execute_command("HSET", "doc2", "tag", b"\xff")

    # the malformed input will result in an indexing error
    errors = index_errors(env, "idx_tag")
    env.assertEqual(errors["indexing failures"], 1)
    env.assertContains("Invalid UTF-8", errors["last indexing error"])
    env.assertEqual(errors["last indexing error key"], "doc2")

    # and the offeding document is not included in search results
    env.expect("FT.SEARCH", "idx_tag", "*", "NOCONTENT").equal([1, "doc1"])


def testSortkeyWithEmbeddedNul(env):
    """WITHSORTKEYS serializes string sort keys length-aware: a value with an
    embedded NUL byte is returned in full instead of being cut at the NUL.
    The field is deliberately not SORTABLE, so the sorter loads the raw
    document value per row (the sorting-vector path truncates at the NUL
    while indexing, before serialization is involved). Both docs share the
    bytes before the NUL and differ only after it, so the ASC order also
    proves the full key is what gets compared — on a cluster that comparison
    is the coordinator's cross-shard merge of the serialized keys."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx_nul', 'SCHEMA', 't', 'TAG').ok()
    conn.execute_command('HSET', 'doc:nul:1', 't', 'a\x00b')
    conn.execute_command('HSET', 'doc:nul:2', 't', 'a\x00a')

    res = env.cmd('FT.SEARCH', 'idx_nul', '*', 'SORTBY', 't', 'ASC', 'WITHSORTKEYS', 'NOCONTENT')
    env.assertEqual(res, [2, 'doc:nul:2', '$a\x00a', 'doc:nul:1', '$a\x00b'])


# ---------------------------------------------------------------------------
# Sorting by more than one field, behind ENABLE_UNSTABLE_FEATURES.
#
# FT.SEARCH names a second sort field in a second SORTBY clause, the way it
# names a second numeric filter in a second FILTER clause. FT.AGGREGATE keeps
# its `SORTBY <nargs> @f ASC ...` spelling and still rejects a second step.
# ---------------------------------------------------------------------------

@contextmanager
def unstable_features(env, enabled=True):
    """Set the gate for the body and put the previous value back afterwards.

    Tests in a file share one server, and RLTest reuses that server across files
    when the environments match, so a test that leaves the gate on hands it to
    whatever runs next. `test_issues:testMultiSortby` asserts the gate-off error
    for this very syntax, so the leak would turn into a failure there.
    """
    previous = env.cmd('CONFIG', 'GET', 'search-enable-unstable-features')[1]
    if enabled:
        enable_unstable_features(env)
    else:
        run_command_on_all_shards(env, 'CONFIG', 'SET',
                                  'search-enable-unstable-features', 'no')
    try:
        yield
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET',
                                  'search-enable-unstable-features', previous)


def build_sortby_pairs(env, idx, prefix):
    """The shape from the feature request: a primary field whose ties only a
    secondary field can break.

    PREFIX keeps the index off the documents the other tests in this file leave
    behind, since they share one server.
    """
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', idx, 'ON', 'HASH', 'PREFIX', '1', f'{prefix}:',
               'SCHEMA', 'a', 'NUMERIC', 'SORTABLE', 'b', 'NUMERIC', 'SORTABLE').ok()
    conn.execute_command('HSET', f'{prefix}:1', 'a', '1', 'b', '1')
    conn.execute_command('HSET', f'{prefix}:2', 'a', '1', 'b', '2')
    conn.execute_command('HSET', f'{prefix}:3', 'a', '2', 'b', '3')
    return conn


def testMultiSortbyRequiresUnstableFeatures(env):
    """With the gate off, a second SORTBY returns the error it returns today.

    `SORTBY a ASC SORTBY b DESC` already parses on a shipped build and fails
    with this message, so Requirement 1 of
    `docs/CONTRIBUTING-unstable-features.md` keeps it as it is rather than
    replacing it with one that names the flag.
    """
    with unstable_features(env, enabled=False):
        build_sortby_pairs(env, 'idx_ms_off', 'ms_off')

        env.expect('FT.SEARCH', 'idx_ms_off', '*', 'SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'DESC') \
            .error().contains('Multiple SORTBY steps are not allowed')


def testSingleSortbyAfterAnotherOptionStillSorts(env):
    """A single SORTBY keeps working when another option comes before it.

    This needs no gate: it pins existing behaviour. The coordinator reads its
    arguments with a parser that does not know SORTBY, so the clause surfaces as
    an unrecognized token, and an earlier version of this change skipped it there
    and dropped the sort for every query that put another option first. The
    result count stayed correct throughout, which is why the order is what is
    asserted here.
    """
    build_sortby_pairs(env, 'idx_ms_single', 'ms_single')

    env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_single', '*', 'NOCONTENT',
                            'SORTBY', 'b', 'DESC'),
                    [3, 'ms_single:3', 'ms_single:2', 'ms_single:1'])
    env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_single', '*', 'LIMIT', '0', '3',
                            'NOCONTENT', 'SORTBY', 'b', 'ASC'),
                    [3, 'ms_single:1', 'ms_single:2', 'ms_single:3'])


def testFieldNamedSortbyIsUnaffectedWithTheGateOff(env):
    """A field named SORTBY in a RETURN list is not a second sort clause.

    The coordinator reads its arguments one token at a time and cannot tell the
    two apart, which is why its rejection is read behind the gate: with the gate
    off this query has to keep working exactly as it does on a shipped build.
    """
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx_ms_named', 'ON', 'HASH', 'PREFIX', '1', 'ms_named:',
               'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 'SORTBY', 'TEXT').ok()
    conn.execute_command('HSET', 'ms_named:1', 'n', '1', 'SORTBY', 'x')

    with unstable_features(env, enabled=False):
        res = env.cmd('FT.SEARCH', 'idx_ms_named', '*', 'SORTBY', 'n', 'ASC',
                      'RETURN', '2', 'SORTBY', 'n')
        env.assertEqual(res[0], 1, message=res)
        env.assertEqual(res[1], 'ms_named:1', message=res)


@skip(cluster=True)
def testMultiSortbySecondFieldBreaksTies(env):
    """The second clause orders the documents the first leaves tied."""
    with unstable_features(env):
        build_sortby_pairs(env, 'idx_ms', 'ms')

        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'ASC'),
                        [3, 'ms:1', 'ms:2', 'ms:3'])
        # Only the tie between ms:1 and ms:2 moves, which is the whole point: the
        # primary still decides everything it can decide.
        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'DESC'),
                        [3, 'ms:2', 'ms:1', 'ms:3'])
        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'DESC', 'SORTBY', 'b', 'ASC'),
                        [3, 'ms:3', 'ms:1', 'ms:2'])


@skip(cluster=True)
def testMultiSortbyDefaultsToAscending(env):
    """A clause without ASC or DESC sorts ascending, as a single clause does."""
    with unstable_features(env):
        build_sortby_pairs(env, 'idx_ms_def', 'ms_def')

        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_def', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'SORTBY', 'b'),
                        [3, 'ms_def:1', 'ms_def:2', 'ms_def:3'])
        # The direction belongs to the clause it follows, not to the whole sort.
        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_def', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'SORTBY', 'b', 'DESC'),
                        [3, 'ms_def:2', 'ms_def:1', 'ms_def:3'])


@skip(cluster=True)
def testMultiSortbyThirdFieldBreaksRemainingTies(env):
    """Three clauses, with the first two tied across two documents."""
    with unstable_features(env):
        conn = getConnectionByEnv(env)
        env.expect('FT.CREATE', 'idx_ms3', 'ON', 'HASH', 'PREFIX', '1', 'ms3:',
                   'SCHEMA', 'a', 'NUMERIC', 'SORTABLE', 'b', 'NUMERIC', 'SORTABLE',
                   'c', 'TAG', 'SORTABLE').ok()
        conn.execute_command('HSET', 'ms3:1', 'a', '1', 'b', '1', 'c', 'y')
        conn.execute_command('HSET', 'ms3:2', 'a', '1', 'b', '1', 'c', 'x')
        conn.execute_command('HSET', 'ms3:3', 'a', '1', 'b', '0', 'c', 'z')

        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms3', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'ASC', 'SORTBY', 'c', 'ASC'),
                        [3, 'ms3:3', 'ms3:2', 'ms3:1'])
        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms3', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'ASC', 'SORTBY', 'c', 'DESC'),
                        [3, 'ms3:3', 'ms3:1', 'ms3:2'])


@skip(cluster=True)
def testMultiSortbyRanksAMissingSecondaryValueLast(env):
    """A document with no value for a later sort field is ordered last, whichever
    direction that field carries.

    The comparison treats a missing value as the worst one rather than reversing
    it under DESC. That rule is reachable from FT.SEARCH only now that a sort can
    have a second field.
    """
    with unstable_features(env):
        conn = getConnectionByEnv(env)
        env.expect('FT.CREATE', 'idx_ms_miss', 'ON', 'HASH', 'PREFIX', '1', 'ms_miss:',
                   'SCHEMA', 'a', 'NUMERIC', 'SORTABLE', 'b', 'NUMERIC', 'SORTABLE').ok()
        conn.execute_command('HSET', 'ms_miss:1', 'a', '1', 'b', '2')
        conn.execute_command('HSET', 'ms_miss:2', 'a', '1')
        conn.execute_command('HSET', 'ms_miss:3', 'a', '1', 'b', '1')

        for direction in ('ASC', 'DESC'):
            res = env.cmd('FT.SEARCH', 'idx_ms_miss', '*', 'NOCONTENT',
                          'SORTBY', 'a', 'ASC', 'SORTBY', 'b', direction)
            env.assertEqual(res[3], 'ms_miss:2', message=f'{direction}: {res}')


@skip(cluster=True)
def testMultiSortbyLoadsFieldsThatAreNotSortable(env):
    """A secondary field that is not SORTABLE is loaded for the sort.

    The shared arrange step resolves each key for reading and falls back to
    loading it when the field has no sorting vector, so the field only has to be
    in the schema.
    """
    with unstable_features(env):
        conn = getConnectionByEnv(env)
        env.expect('FT.CREATE', 'idx_ms_load', 'ON', 'HASH', 'PREFIX', '1', 'ms_load:',
                   'SCHEMA', 'a', 'NUMERIC', 'SORTABLE', 'b', 'NUMERIC').ok()
        conn.execute_command('HSET', 'ms_load:1', 'a', '1', 'b', '2')
        conn.execute_command('HSET', 'ms_load:2', 'a', '1', 'b', '1')

        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_load', '*', 'NOCONTENT',
                                'SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'ASC'),
                        [2, 'ms_load:2', 'ms_load:1'])


@skip(cluster=True)
def testMultiSortbyRejectsUnknownFieldInALaterClause(env):
    """A later clause names a field like the first one does, and is checked."""
    with unstable_features(env):
        build_sortby_pairs(env, 'idx_ms_unknown', 'ms_unknown')

        env.expect('FT.SEARCH', 'idx_ms_unknown', '*', 'SORTBY', 'a', 'ASC',
                   'SORTBY', 'nosuch', 'ASC').error().contains('not loaded nor in schema')


@skip(cluster=True)
def testMultiSortbyFieldLimit(env):
    """The sort directions live in a bitmap with a fixed number of slots, so the
    clause count is capped rather than silently losing a direction."""
    with unstable_features(env):
        build_sortby_pairs(env, 'idx_ms_cap', 'ms_cap')

        # SORTASCMAP_MAXFIELDS clauses are accepted, one more is not.
        args = []
        for _ in range(8):
            args += ['SORTBY', 'a', 'ASC']
        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_cap', '*', 'NOCONTENT', *args)[0], 3)

        env.expect('FT.SEARCH', 'idx_ms_cap', '*', 'NOCONTENT', *(args + ['SORTBY', 'b', 'ASC'])) \
            .error().contains('Cannot sort by more than')


@skip(cluster=True)
def testMultiSortbyWithSortkeysReturnsTheFirstKey(env):
    """WITHSORTKEYS still returns one key per document, the primary one.

    The reply carries a single key per result, so this pins which one rather
    than leaving it to the reader. Returning every key would change the shape of
    the reply, which is a decision for graduation.
    """
    with unstable_features(env):
        build_sortby_pairs(env, 'idx_ms_keys', 'ms_keys')

        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_keys', '*', 'NOCONTENT', 'WITHSORTKEYS',
                                'SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'DESC'),
                        [3, 'ms_keys:2', '#1', 'ms_keys:1', '#1', 'ms_keys:3', '#2'])


@skip(cluster=True)
def testMultiSortbyDoesNotChangeAggregate(env):
    """FT.AGGREGATE names every field in one step, and still rejects a second
    step with the gate on."""
    with unstable_features(env):
        build_sortby_pairs(env, 'idx_ms_agg', 'ms_agg')

        env.expect('FT.AGGREGATE', 'idx_ms_agg', '*', 'SORTBY', '2', '@a', 'ASC',
                   'SORTBY', '2', '@b', 'ASC') \
            .error().contains('Multiple SORTBY steps are not allowed. Sort multiple fields in a single step')
        # The supported spelling is unaffected.
        env.assertEqual(env.cmd('FT.AGGREGATE', 'idx_ms_agg', '*', 'LOAD', '1', '@a',
                                'SORTBY', '4', '@a', 'ASC', '@b', 'DESC')[0], 3)


def testMultiSortbyRejectedInCluster(env):
    """The coordinator merges shard replies on a single sort key, so it rejects a
    second clause rather than returning a wrong global order.

    Only when the query actually fans out: `DistSearchCommandImp` hands a
    one-shard cluster to the standalone path before the coordinator parses
    anything, and with one shard there is no merge to get wrong.
    """
    SkipOnNonCluster(env)
    if env.shardsCount < 2:
        env.skip()
    with unstable_features(env):
        build_sortby_pairs(env, 'idx_ms_cluster', 'ms_cluster')

        for args in (['SORTBY', 'a', 'ASC', 'SORTBY', 'b', 'DESC'],
                     # Without directions the whole run of clauses is swallowed by
                     # one AC_ParseArgSpec call, so a guard that only inspects the
                     # token the outer loop is standing on never sees the second.
                     ['SORTBY', 'a', 'SORTBY', 'b']):
            env.expect('FT.SEARCH', 'idx_ms_cluster', '*', *args) \
                .error().contains('Multiple SORTBY steps are not allowed')

        # One clause is unaffected. This asserts the order rather than the
        # result count, because a count survives a sort that was dropped.
        env.assertEqual(env.cmd('FT.SEARCH', 'idx_ms_cluster', '*', 'NOCONTENT',
                                'SORTBY', 'b', 'DESC'),
                        [3, 'ms_cluster:3', 'ms_cluster:2', 'ms_cluster:1'])
