# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""FT.AGGREGATE over multi-value JSON fields, as query -> expected rows.

The schema is in utils/multi_value_aggregation.py. Every FT.AGGREGATE query is
checked in DIALECT 1, 2 and 3 and must return the same rows there, and in
cluster mode the same rows as standalone.
"""

from common import *
from utils.multi_value_aggregation import index_docs, index_hash_docs, aggregate, expect_rows

A = {'name': 'A', 'tags': ['cat'], 'nums': [30]}
B = {'name': 'B', 'tags': ['fox', 'bat', 'cat'], 'nums': [25, 5, 40]}


@skip(no_json=True)
def test_groupby_forms_a_group_per_value(env):
    """Each tag of a document is a group of its own; cat holds both documents."""
    for sortable in (False, True):
        index_docs(env, A, B, sortable=sortable)
        expect_rows(env, 'GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
            {'tags': 'bat', 'count': '1'},
            {'tags': 'cat', 'count': '2'},
            {'tags': 'fox', 'count': '1'},
        ])


@skip(no_json=True)
def test_groupby_on_a_filtered_query_sees_every_value_of_a_match(env):
    """Only B matches, and it joins the group of each of its tags, not only fox's."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
        {'tags': 'bat', 'count': '1'},
        {'tags': 'cat', 'count': '1'},
        {'tags': 'fox', 'count': '1'},
    ], query='@tags:{fox}')


@skip(no_json=True)
def test_groupby_counts_a_document_once_per_value(env):
    """C lists cat twice but is one document: cat counts 3 documents, not 4."""
    index_docs(env, A, B, {'name': 'C', 'tags': ['cat', 'cat']})
    expect_rows(env, 'GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
        {'tags': 'bat', 'count': '1'},
        {'tags': 'cat', 'count': '3'},
        {'tags': 'fox', 'count': '1'},
    ])


@skip(no_json=True)
def test_groupby_numeric_field_forms_a_group_per_value(env):
    """A numeric field groups the same way, by each number."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 1 @nums REDUCE COUNT 0 AS count', [
        {'nums': '5', 'count': '1'},
        {'nums': '25', 'count': '1'},
        {'nums': '30', 'count': '1'},
        {'nums': '40', 'count': '1'},
    ])


@skip(no_json=True)
def test_groupby_two_multi_value_fields_forms_every_combination(env):
    """D joins one group per (tag, number) pair it holds."""
    index_docs(env, {'name': 'D', 'tags': ['cat', 'fox'], 'nums': [1, 2]})
    expect_rows(env, 'GROUPBY 2 @tags @nums REDUCE COUNT 0 AS count', [
        {'tags': 'cat', 'nums': '1', 'count': '1'},
        {'tags': 'cat', 'nums': '2', 'count': '1'},
        {'tags': 'fox', 'nums': '1', 'count': '1'},
        {'tags': 'fox', 'nums': '2', 'count': '1'},
    ])


@skip(no_json=True)
def test_groupby_single_valued_field_is_unchanged(env):
    """A single-valued field keeps its per-dialect form, also after SORTBY."""
    index_docs(env, A, B)
    for query in ('GROUPBY 1 @name', 'SORTBY 2 @name ASC GROUPBY 1 @name'):
        env.assertEqual(aggregate(env, query, dialect=2), [{'name': 'A'}, {'name': 'B'}])
        env.assertEqual(aggregate(env, query, dialect=3), [{'name': '["A"]'}, {'name': '["B"]'}])


@skip(no_json=True)
def test_groupby_after_sortby_or_limit_forms_a_group_per_value(env):
    """GROUPBY after SORTBY or LIMIT groups the same way; in cluster the coordinator
    does it."""
    index_docs(env, A, B)
    for arrange in ('SORTBY 2 @name ASC', 'LIMIT 0 10'):
        expect_rows(env, f'{arrange} GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
            {'tags': 'bat', 'count': '1'},
            {'tags': 'cat', 'count': '2'},
            {'tags': 'fox', 'count': '1'},
        ])


@skip(no_json=True)
def test_reducers_after_sortby_take_every_value(env):
    """The coordinator runs SUM itself here, over every value."""
    index_docs(env, A, B)
    expect_rows(env, 'SORTBY 2 @name ASC GROUPBY 0 REDUCE SUM 1 @nums AS sum', [{'sum': '100'}])


@skip(no_json=True)
def test_groupby_after_sortby_single_valued_field_is_unchanged_with_format_expand():
    """FORMAT EXPAND (RESP3) still shows a single-valued field as its array of matches
    when, in cluster, the coordinator groups."""
    env = Env(protocol=3)
    index_docs(env, A, B)
    env.assertEqual(aggregate(env, 'FORMAT EXPAND SORTBY 2 @name ASC GROUPBY 1 @name', dialect=3),
                    [{'name': ['A']}, {'name': ['B']}])


def test_hash_index_is_unchanged(env):
    """A HASH TAG holds one string, and groups on it whole, also after SORTBY."""
    index_hash_docs(env, {'name': 'A', 'tags': 'cat,fox', 'nums': '30'})
    for arrange in ('', 'SORTBY 2 @name ASC'):
        expect_rows(env, f'{arrange} GROUPBY 1 @tags REDUCE SUM 1 @nums AS sum', [
            {'tags': 'cat,fox', 'sum': '30'},
        ])


@skip(no_json=True)
def test_groupby_after_loading_or_filtering_on_the_field(env):
    """Reading the field first changes nothing, also when the coordinator groups."""
    index_docs(env, A, B)
    for steps in ('LOAD 1 @tags', 'LOAD 1 @tags SORTBY 2 @name ASC',
                  "LOAD 1 @tags FILTER 'exists(@tags)' SORTBY 2 @name ASC"):
        expect_rows(env, f'{steps} GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
            {'tags': 'bat', 'count': '1'},
            {'tags': 'cat', 'count': '2'},
            {'tags': 'fox', 'count': '1'},
        ])


@skip(no_json=True)
def test_groupby_on_a_load_alias_groups_the_loaded_value(env):
    """A LOAD alias named like the field replaces the field."""
    index_docs(env, A, B)
    # The loaded name shows bare only before DIALECT 3.
    expect_rows(env, 'LOAD 3 @name AS tags GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
        {'tags': 'A', 'count': '1'},
        {'tags': 'B', 'count': '1'},
    ], dialects=(1, 2))


@skip(no_json=True)
def test_groupby_on_an_apply_alias_groups_the_computed_value(env):
    """A value computed under the field's name replaces the field, also after SORTBY."""
    index_docs(env, A, B)
    for arrange in ('', 'SORTBY 2 @name ASC'):
        expect_rows(env, f"APPLY '\"pet\"' AS tags {arrange} GROUPBY 1 @tags REDUCE COUNT 0 AS count", [
            {'tags': 'pet', 'count': '2'},
        ])


@skip(no_json=True)
def test_groupby_document_without_values_groups_under_null(env):
    """An empty array, a missing field and only nulls all group under null, also when
    the coordinator groups."""
    index_docs(env, {'name': 'E', 'tags': []}, {'name': 'F'}, {'name': 'G', 'tags': [None]})
    for arrange in ('', 'SORTBY 2 @name ASC'):
        expect_rows(env, f'{arrange} GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
            {'tags': None, 'count': '3'},
        ])


@skip(no_json=True)
def test_groupby_skips_null_values(env):
    """H's null is not a value; its cat is."""
    index_docs(env, {'name': 'H', 'tags': ['cat', None]})
    expect_rows(env, 'GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
        {'tags': 'cat', 'count': '1'},
    ])


@skip(no_json=True)
def test_groupby_values_are_taken_as_stored(env):
    """Values come from the document, not normalized, even for a SORTABLE field:
    Cat and cat differ."""
    for sortable in (False, True):
        index_docs(env, {'name': 'I', 'tags': ['Cat']}, {'name': 'J', 'tags': ['cat']},
                   sortable=sortable)
        expect_rows(env, 'GROUPBY 1 @tags REDUCE COUNT 0 AS count', [
            {'tags': 'Cat', 'count': '1'},
            {'tags': 'cat', 'count': '1'},
        ])


@skip(no_json=True)
def test_reducers_take_every_value(env):
    """All four numbers take part; COUNT still counts the two documents."""
    for sortable in (False, True):
        index_docs(env, A, B, sortable=sortable)
        expect_rows(env, 'GROUPBY 0 REDUCE SUM 1 @nums AS sum REDUCE MIN 1 @nums AS min '
                         'REDUCE MAX 1 @nums AS max REDUCE COUNT 0 AS count', [
            {'sum': '100', 'min': '5', 'max': '40', 'count': '2'},
        ])


@skip(no_json=True)
def test_reducers_in_a_group_take_every_value_of_its_documents(env):
    """cat holds A and B, so its sum is 30 + 25 + 5 + 40."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 1 @tags REDUCE SUM 1 @nums AS sum', [
        {'tags': 'bat', 'sum': '70'},
        {'tags': 'cat', 'sum': '100'},
        {'tags': 'fox', 'sum': '70'},
    ])


@skip(no_json=True)
def test_avg_divides_by_the_number_of_values(env):
    """100 over 4 values, not over 2 documents."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 0 REDUCE AVG 1 @nums AS avg', [{'avg': '25'}])


@skip(no_json=True)
def test_quantile_and_stddev_take_every_value(env):
    """Over 30, 25, 5 and 40: the 0.9 quantile is 40, the sample stddev sqrt(650/3)."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 0 REDUCE QUANTILE 2 @nums 0.9 AS q REDUCE STDDEV 1 @nums AS sd', [
        {'q': '40', 'sd': '14.7196014439'},
    ])


@skip(no_json=True)
def test_tolist_returns_the_distinct_values(env):
    """cat is in both documents and listed once."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 0 REDUCE TOLIST 1 @tags AS tags', [
        {'tags': ['bat', 'cat', 'fox']},
    ])


@skip(no_json=True)
def test_count_distinctish_counts_each_value(env):
    """cat, in both documents, counts once."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 0 REDUCE COUNT_DISTINCTISH 1 @tags AS count', [{'count': '3'}])


@skip(no_json=True)
def test_count_distinct_counts_each_value(env):
    """cat, in both documents, counts once."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 0 REDUCE COUNT_DISTINCT 1 @tags AS count', [{'count': '3'}])


@skip(no_json=True)
def test_count_distinct_in_a_group_per_value(env):
    """cat holds A and B; in cluster COUNT_DISTINCT makes the coordinator group."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 1 @tags REDUCE COUNT_DISTINCT 1 @name AS names', [
        {'tags': 'bat', 'names': '1'},
        {'tags': 'cat', 'names': '2'},
        {'tags': 'fox', 'names': '1'},
    ])


@skip(no_json=True)
def test_first_value_compares_a_document_by_its_best_value(env):
    """P has the smallest value (5) and Q the largest (40)."""
    index_docs(env, {'name': 'P', 'nums': [5, 20]}, {'name': 'Q', 'nums': [10, 40]})
    # The returned name shows bare only before DIALECT 3.
    expect_rows(env, 'GROUPBY 0 REDUCE FIRST_VALUE 4 @name BY @nums ASC AS first', [
        {'first': 'P'},
    ], dialects=(1, 2))
    expect_rows(env, 'GROUPBY 0 REDUCE FIRST_VALUE 4 @name BY @nums DESC AS first', [
        {'first': 'Q'},
    ], dialects=(1, 2))


@skip(no_json=True)
def test_random_sample_samples_values(env):
    """A sample larger than the four tags holds each of them, cat twice."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 0 REDUCE RANDOM_SAMPLE 2 @tags 10 AS sample', [
        {'sample': ['bat', 'cat', 'cat', 'fox']},
    ])


@skip(no_json=True)
def test_numeric_reducers_count_a_repeated_value_each_time(env):
    """R holds 5 twice; both count, unlike in GROUPBY."""
    index_docs(env, {'name': 'R', 'nums': [5, 5]})
    expect_rows(env, 'GROUPBY 0 REDUCE SUM 1 @nums AS sum', [{'sum': '10'}])


@skip(no_json=True)
def test_reducer_over_the_grouped_field_sees_every_value_of_the_documents(env):
    """bat's only document is B, so its list holds all of B's tags."""
    index_docs(env, A, B)
    expect_rows(env, 'GROUPBY 1 @tags REDUCE TOLIST 1 @tags AS list', [
        {'tags': 'bat', 'list': ['bat', 'cat', 'fox']},
        {'tags': 'cat', 'list': ['bat', 'cat', 'fox']},
        {'tags': 'fox', 'list': ['bat', 'cat', 'fox']},
    ])


@skip(no_json=True)
def test_avg_skips_a_document_without_numbers(env):
    """E has no nums, so it adds nothing to divide by."""
    index_docs(env, A, B, {'name': 'E'})
    expect_rows(env, 'GROUPBY 0 REDUCE AVG 1 @nums AS avg', [{'avg': '25'}])


@skip(no_json=True)
def test_reducer_skips_null_values(env):
    """H's null is not a value; its cat is."""
    index_docs(env, {'name': 'H', 'tags': ['cat', None]})
    expect_rows(env, 'GROUPBY 0 REDUCE TOLIST 1 @tags AS tags', [{'tags': ['cat']}])


@skip(no_json=True)
def test_reducer_on_an_apply_alias_takes_the_computed_value(env):
    """A value computed under the field's name replaces the field: 7 per document."""
    index_docs(env, A, B)
    expect_rows(env, 'APPLY 7 AS nums GROUPBY 0 REDUCE SUM 1 @nums AS sum', [{'sum': '14'}])


@skip(no_json=True)
def test_reducer_single_valued_field_is_unchanged(env):
    """A single-valued field keeps its per-dialect form."""
    index_docs(env, A, B)
    env.assertEqual(aggregate(env, 'GROUPBY 0 REDUCE TOLIST 1 @name AS names', dialect=2),
                    [{'names': ['A', 'B']}])
    env.assertEqual(aggregate(env, 'GROUPBY 0 REDUCE TOLIST 1 @name AS names', dialect=3),
                    [{'names': ['["A"]', '["B"]']}])


@skip(no_json=True)
def test_avg_shard_count_is_not_a_user_reducer(env):
    """The count a distributed AVG divides by is internal to the shards."""
    index_docs(env, A, B)
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '0',
               'REDUCE', '__COUNT_NUMBERS', '1', '@nums').error().contains('No such reducer')


@skip(no_json=True)
def test_groupby_on_the_coordinators_request_name_is_an_unknown_property(env):
    """The prefixed name the coordinator asks shards for is not a property to group by."""
    for index in (lambda: index_docs(env, A),
                  lambda: index_hash_docs(env, {'name': 'A', 'tags': 'cat', 'nums': '30'})):
        index()
        env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@\x01tags').error() \
            .contains('No such property')
