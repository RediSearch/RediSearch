---
title: "Query dialects"
linkTitle: "Query dialects"
weight: 5
description: >
    Learn how to use query dialects
---

Redis Stack currently supports four query dialects for use with the `FT.SEARCH`, `FT.AGGREGATE`, and other search and query commands.

## `DIALECT 1`

Dialect version 1 was the default query syntax dialect from the first release of search and query until dialect version 2 was introduced with version [2.4](https://github.com/RediSearch/RediSearch/releases/tag/v2.4.3).
This dialect is also the default dialect. See below for information about changing the default dialect.

## `DIALECT 2`

Dialect version 2 was introduced in the [2.4](https://github.com/RediSearch/RediSearch/releases/tag/v2.4.3) release to address query parser inconsistencies found in previous versions of Redis Stack. Dialect version 1 remains the default dialect. To use dialect version 2, append `DIALECT 2` to your query command.

`FT.SEARCH ... DIALECT 2`

It was determined that under certain conditions some query parsing rules did not behave as originally intended.
Particularly, some queries containing the operators below could return unexpected results.

1. AND, multi-word phrases that imply intersection
1. `"..."` (exact), `~` (optional), `-` (negation), and `%` (fuzzy)
1. OR, words separated by the `|` (pipe) character that imply union
1. wildcard characters

Existing queries that used dialect 1 may behave differently using dialect 2 if they fall into any of the following categories:

1. Your query has a field modifier followed by multiple words. Consider the sample query:

    `@name:James Brown`

    Here, the field modifier `@name` is followed by two words, `James` and `Brown`.

    In `DIALECT 1`, this query would be interpreted as find `James Brown` in the `@name` field.
    In `DIALECT 2`, this query would be interpreted as find `James` in the `@name` field, and `Brown` in any text field. In other words, it would be interpreted as `(@name:James) Brown`. 
    In `DIALECT 2`, you could achieve the dialect 1 behavior by updating your query to `@name:(James Brown)`.

1. Your query uses `"..."`, `~`, `-`, and/or `%`. Consider a simple query with negation:

    `-hello world`

    In `DIALECT 1`, this query is interpreted as find values in any field that do not contain `hello` and do not contain `world`; the equivalent of `-(hello world)` or `-hello -world`.
    In `DIALECT 2`, this query is interpreted as `-hello` and `world` (only `hello` is negated).
    In `DIALECT 2`, you could achieve the dialect 1 behavior by updating your query to `-(hello world)`.

1. Your query used `|`. Consider the simple query:

    `hello world | "goodbye" moon`

    In `DIALECT 1`, this query is interpreted as searching for `(hello world | "goodbye") moon`.
    In `DIALECT 2`, this query is interpreted as searching for either `hello world` `"goodbye" moon`.

1. Your query uses a wildcard pattern. Consider the simple query:

    `"w'foo*bar?'"`

    As shown above, you must use double quotes to contain the `w` pattern.

With `DIALECT 2` you can use un-escaped spaces in tag queries, even with stopwords.

{{% alert title=Note %}}
`DIALECT 2` is required with vector searches.
{{% /alert %}}

## `DIALECT 3`

Dialect version 3 was introduced in the [2.6](https://github.com/RediSearch/RediSearch/releases/tag/v2.6.3) release. The primary change is that, with version 3, JSON is returned rather than scalars for multi-value attributes. Apart from specifying `DIALECT 3` at the end of a `FT.SEARCH` command, there are no other syntactic changes. Dialect version 1 remains the default dialect. To use dialect version 3, append `DIALECT 3` to your query command.

`FT.SEARCH ... DIALECT 3`

**Example**

Sample JSON:

```
{
  "id": 123,
  "underlyings": [
    {
      "currency": "USD",
      "spot": 99,
      "underlier": "AAPL UW"
    },
    {
      "currency": "USD",
      "spot": 100,
      "underlier": "NFLX UW"
    }
  ]
}
```

Create an index:

```
FT.CREATE js_idx ON JSON PREFIX 1 js: SCHEMA $.underlyings[*].underlier AS und TAG
```

Now search, with and without `DIALECT 3`.

- With dialect 1 (default):

    ```
    ft.search js_idx * return 1 und
      1) (integer) 1
      2) "js:1"
      3) 1) "und"
         2) "AAPL UW"
    ```
    
    Only one of the expected two elements is returned.

- With dialect 3:

    ```
    ft.search js_idx * return 1 und DIALECT 3
        1) (integer) 1
        2) "js:1"
        3) 1) "und"
           2) "[\"AAPL UW\",\"NFLX UW\"]"
    ```

    Both elements are returned.

{{% alert title=Note %}}
DIALECT 3 is required for shape-based (`POINT` or `POLYGON`) geospatial queries.
{{% /alert %}}

## `DIALECT 4`

Dialect version 4 was introduced in the [2.8](https://github.com/RediSearch/RediSearch/releases/tag/v2.8.4) release. It introduces performance optimizations for sorting operations on `FT.SEARCH` and `FT.AGGREGATE`. Apart from specifying `DIALECT 4` at the end of a `FT.SEARCH` command, there are no other syntactic changes. Dialect version 1 remains the default dialect. To use dialect version 4, append `DIALECT 4` to your query command.

`FT.SEARCH ... DIALECT 4`

Dialect version 4 will improve performance in four different scenarios:

1. Skip sorter - applied when there is no sorting to be done. The query can return once it reaches the `LIMIT` of requested results.
1. Partial range - applied when there is a `SORTBY` on a numeric field, either with no filter or with a filter by the same numeric field. Such queries will iterate on a range large enough to satisfy the `LIMIT` of requested results.
1. Hybrid - applied when there is a `SORTBY` on a numeric field in addition to another non-numeric filter. It could be the case that some results will get filtered, leaving too small a range to satisfy any specified `LIMIT`. In such cases, the iterator then is re-wound and additional iterations occur to collect result up to the requested `LIMIT`.
1. No optimization - If there is a sort by score or by a non-numeric field, there is no other option but to retrieve all results and compare their values to the search parameters.

## Change the default dialect

The default dialect is `DIALECT 1`. If you wish to change that, you can do so by using the `DEFAULT_DIALECT` parameter when loading the RediSearch module:

```
$ redis-server --loadmodule ./redisearch.so DEFAULT_DIALECT 2
```

You can also change the query dialect on an already running server using the `FT.CONFIG` command:

```
FT.CONFIG SET DEFAULT_DIALECT 2
```