---
title: "Range"
linkTitle: "Range"
description: Numeric range queries
---

A range query on a numeric field returns you the values that are within a given start and end value:

```
FT.SEARCH index "@field:[start end]"
```

You can alternatively use `FT.SEARCH`'s `FILTER` argument, but you need to know that the query execution plan is different because the filter gets applied after the query string (e.g., `*`) gets evaluated:

```
FT.SEARCH index "*" FILTER field start end
```

The values `-inf`, `inf`, and `+inf` are valid values that allow you to define open ranges. Start and end values are by default inclusive, but you can prepend `(` to a value to exclude it from the range.

> Open range queries can lead to large result sets. `FT.SEARCH` returns by default the first ten results only. You can find further details about using the `LIMIT` argument to scroll through the complete result set in the [`FT.SEARCH` command reference](/commands/ft.search/).

Here are some examples:

* Bicycles within a price range greater or equal to 500 USD and smaller or equal to 1000 USD (`500 <= price <= 1000`):

```
FT.SEARCH idx:bicycle "@price:[500 1000]"
```

This is semantically equivalent to:

```
FT.SEARCH idx:bicycle "*" FILTER price 500 1000
```

* Bicycles with a price greater than 1000 USD (`price > 1000`):

```
FT.SEARCH idx:bicycle "@price:[(1000 +inf]"
```

* Bicycles with a price lower or equal to 2000 USD (`price <= 2000`):

```
FT.SEARCH idx:bicycle "@price:[-inf 2000]"
```

You can learn more about non-numeric range queries, such as geo-spatial or vector similarity search queries, in their dedicated documentation articles:

* [Geo-spatial](TODO)
* [Vector similarity](TODO)