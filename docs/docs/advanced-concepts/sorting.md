---
title: "Sorting"
linkTitle: "Sorting"
weight: 5
description: Support for sorting query results
aliases: 
    - /docs/stack/search/reference/sorting/
    - /redisearch/reference/sorting
---

# Sorting by indexed fields

As of RediSearch 0.15, you can bypass the scoring function mechanism and order search results by the value of different document attributes (fields) directly, even if the sorting field is not used by the query. For example, you can search for first name and sort by last name.

## Declaring sortable fields

When creating an index with `FT.CREATE`, you can declare `TEXT`, `TAG`, `NUMERIC`, and `GEO` attributes as `SORTABLE`. When an attribute is sortable, you can order the results by its values with relatively low latency. When an attribute is not sortable, it can still be sorted by its values, but with increased latency. For example, in the following schema:

```
FT.CREATE users SCHEMA first_name TEXT last_name TEXT SORTABLE age NUMERIC SORTABLE
```

The fields `last_name` and `age` are sortable, but `first_name` isn't. This means you can search by either first and/or last name, and sort by last name or age.

### Note on sortable fields

In the current implementation, when declaring a sortable field, its content gets copied into a special location in the index that provides for fast access during sorting. This means that making long fields sortable is very expensive and you should be careful with it.

### Normalization (UNF option)

By default, text fields get normalized and lowercased in a Unicode-safe way when stored for sorting. For example, `America` and `america` are considered equal in terms of sorting.

Using the `UNF` (un-normalized form) argument, it is possible to disable the normalization and keep the original form of the value. Therefore, `America` will come before `america`.

## Specifying SORTBY

If an index includes sortable fields, you can add the `SORTBY` parameter to the search request (outside the query body) to order the results. This overrides the scoring function mechanism, and the two cannot be combined. If `WITHSCORES` is specified together with `SORTBY`, the scores returned are simply the relative position of each result in the result set.

The syntax for `SORTBY` is:

```
SORTBY {field_name} [ASC|DESC]
```

* `field_name` must be a sortable field defined in the schema.

* `ASC` means ascending order, `DESC` means descending order.

* The default ordering is `ASC`.

## Example

```
> FT.CREATE users ON HASH PREFIX 1 "user" SCHEMA first_name TEXT SORTABLE last_name TEXT age NUMERIC SORTABLE

# Add some users
> HSET user1 first_name "alice" last_name "jones" age 35
> HSET user2 first_name "bob" last_name "jones" age 36

# Searching while sorting

# Searching by last name and sorting by first name
> FT.SEARCH users "@last_name:jones" SORTBY first_name DESC

# Searching by both first and last name, and sorting by age
> FT.SEARCH users "jones" SORTBY age ASC

```
