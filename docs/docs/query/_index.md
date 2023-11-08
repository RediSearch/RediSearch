---
title: "Query data"
linkTitle: "Query data"
description: Understand how to query, search, and aggregate Redis data
hideListLinks: True
weight: 5
---

Redis Stack distinguishes between the [FT.SEARCH](/commands/ft.search/) and [FT.AGGREGATE](/commands/ft.aggregate/) query commands. You should use [FT.SEARCH](/commands/ft.search/) if you want to perform selections and projections only. If you also need to apply simple mapping functions, group or aggregate data, use [FT.AGGREGATE](/commands/ft.aggregate/) command. 

* **Selection**: A selection allows you to return all documents that fulfill specific criteria. The example query `FT.SEARCH idx:bicycle "@condition:{new}` returns new bicycles.
* **Projection**: Projections are used to return specific fields of the result set. The example query `FT.SEARCH idx:bicycle "@condition:{new} RETURN 1 condition` only returns the single field 'condition'.
* **Aggregation**: Aggregations collect and summarize data across several fields.

The following articles provide an overview of how to query data with the [FT.SEARCH](/commands/ft.search/) command:

* [Exact match queries](/docs/interact/search-and-query/query/exact-match)
* [Range queries](/docs/interact/search-and-query/query/range)
* [Full-text search ](/docs/interact/search-and-query/query/full-text)
* [Geo-spatial queries](/docs/interact/search-and-query/query/geo-spatial)
* [Vector similarity search](/docs/interact/search-and-query/query/vector-similarity)

You can find further details about aggregation queries with [FT.AGGREGATE](/commands/ft.aggregate/) in the following article:

* [Aggregation queries](/docs/interact/search-and-query/query/aggegation)