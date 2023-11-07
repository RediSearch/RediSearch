---
title: "Range queries"
linkTitle: "Range"
description: Perform numeric range queries
weight: 2
---

A range query on a numeric field returns the values that are in between a given start and end value:

```
FT.SEARCH index "@field:[start end]"
```

You can also use the `FILTER` argument, but you need to know that the query execution plan is different because the filter is applied after the query string (e.g., `*`) is evaluated:

```
FT.SEARCH index "*" FILTER field start end
```

## Start and end values

Start and end values are by default inclusive, but you can prepend `(` to a value to exclude it from the range.

The values `-inf`, `inf`, and `+inf` are valid values that allow you to define open ranges.

## Result set

An open-range query can lead to a large result set. 

By default, `FT.SEARCH` returns only the first ten results. The `LIMIT` argument helps you to scroll through the result set. The `SORTBY` argument ensures that the documents in the result set are returned in the specified order.

```
FT.SEARCH index "@field:[start end]" SORTBY field LIMIT page_start page_end
```

You can find further details about using the `LIMIT` and `SORTBY` in the [`FT.SEARCH` command reference](/commands/ft.search/).

## Examples

The following query finds bicycles within a price range greater or equal to 500 USD and smaller or equal to 1000 USD (`500 <= price <= 1000`):

```
FT.SEARCH idx:bicycle "@price:[500 1000]"
```

This is semantically equivalent to:

```
FT.SEARCH idx:bicycle "*" FILTER price 500 1000
```

For bicycles with a price greater than 1000 USD (`price > 1000`), you can use:

```
FT.SEARCH idx:bicycle "@price:[(1000 +inf]"
```

The example below returns bicycles with a price lower or equal to 2000 USD (`price <= 2000`) by returning the five cheapest bikes:

```
FT.SEARCH idx:bicycle "@price:[-inf 2000] SORTBY price LIMIT 0 5"
```

## Non-numeric range queries

You can learn more about non-numeric range queries, such as [geo-spatial](/docs/interact/search-and-query/query/geo-spatial) or [vector similarity search](/docs/interact/search-and-query/query/vector-similarity) queries, in their dedicated documentation articles.