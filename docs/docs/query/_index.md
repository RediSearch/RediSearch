---
title: "Query data"
linkTitle: "Query"
description: Understand how to query, search, and aggregate Redis data
hideListLinks: True
weight: 5
aliases:
  - /docs/interact/search-and-query/basic-constructs/query-constructs/
---

Redis Stack distinguishes between the [FT.SEARCH](/commands/ft.search/) and [FT.AGGREGATE](/commands/ft.aggregate/) query commands. You should use [FT.SEARCH](/commands/ft.search/) if you want to perform selections and projections only. If you also need to apply mapping functions, group, or aggregate data, use the [FT.AGGREGATE](/commands/ft.aggregate/) command. 

* **Selection**: A selection allows you to return all documents that fulfill specific criteria.
* **Projection**: Projections are used to return specific fields of the result set. You can also map/project to calculated field values.
* **Aggregation**: Aggregations collect and summarize data across several fields.

Here is a short SQL comparison using the [bicycle dataset](./data/bicycles.txt):

|Type| SQL | Redis Stack |
|----------| --- | ----------- |
| Selection | `SELECT * FROM bicycles WHERE price >= 1000` | `FT.SEARCH idx:bicycle "@price:[1000 +inf]"` |
| Simple projection | `SELECT id, price FROM bicycles` | `FT.SEARCH idx:bicycle "*" RETURN 2 __key, price` |
| Calculated projection| `SELECT id, price-price*0.1 AS discounted FROM bicycles`| `FT.AGGREGATE idx:bicycle "*" LOAD 2 __key price APPLY "@price-@price*0.1" AS discounted`| 
| Aggregation | `SELECT condition, AVG(price) AS avg_price FROM bicycles GROUP BY condition` | `FT.AGGREGATE idx:bicycle "*" GROUPBY 1 @condition REDUCE AVG 1 @price AS avg_price` |

The following articles provide an overview of how to query data with the [FT.SEARCH](/commands/ft.search/) command:

* [Exact match queries](/docs/interact/search-and-query/query/exact-match)
* [Range queries](/docs/interact/search-and-query/query/range)
* [Full-text search ](/docs/interact/search-and-query/query/full-text)
* [Geospatial queries](/docs/interact/search-and-query/query/geo-spatial)
* [Vector search](/docs/interact/search-and-query/query/vector-search)
* [Combined queries](/docs/interact/search-and-query/query/combined)

You can find further details about aggregation queries with [FT.AGGREGATE](/commands/ft.aggregate/) in the following article:

* [Aggregation queries](/docs/interact/search-and-query/query/aggregation)