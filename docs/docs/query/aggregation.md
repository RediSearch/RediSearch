---
title: "Aggregation queries"
linkTitle: "Aggregations"
description: Group and aggregate query results
weight: 9
---

An aggregation query allows you to perform the following on the result set of a query:

- Apply simple mapping functions 
- Group data based on field values
- Apply aggregation functions on the grouped data

Redis Stack distinguishes between the [FT.SEARCH](/commands/ft.search/) and [FT.AGGREGATE](/commands/ft.aggregate/) commands. You should use [FT.SEARCH](/commands/ft.search/) if you want to perform selections and projections only. If you need to apply simple functions, group data or aggregate, then you need to use [FT.AGGREGATE](/commands/ft.aggregate/) command.

This article explains the basic usage [FT.AGGREGATE](/commands/ft.aggregate/). Please take a look at the [command specification](/commands/ft.aggregate/) for further details.

The examples in this article use a schema with the following fields:

| Field name | Field type |
| ---------- | ---------- |
| condition | TAG |
| price | NUMERIC |


## Simple mapping

The `APPLY` clause allows you to apply a simple mapping function to a result set that is returned based on the query expression.

```
FT.AGGREGATE index "query_expr" LOAD n field1 .. fieldn APPLY "function_expr" AS "result_field"
```

Here is a more detailed explanation of the query syntax:

1. **Query expression**: You can use the same query expressions as with the `FT.SEARCH` command. You might substitute the `query_expr` with any of the expressions that was explained in the articles of this [query topic](/docs/interact/search-and-query/query/). An exception are vector similarity queries. You can't combine a vector similirity search with an aggregation query.
2. **Loaded fields**: If field values weren't loaded into the aggregation pipeline yet, then you can force their presence via the `LOAD` clause. The clause takes the number of fields, followed by the field names.
3. **Mapping function**: This is the mapping function that operates on the field values. A specific field is referenced via `@field_name` within the function expression. The result of the function is returned within the field named `result_field`.


Here is an example that calculates a discounted price for new bicycles:

```
FT.AGGREGATE idx:bicycle "@condition:{new}" LOAD 2 __key price APPLY "@price - (@price * 0.1)" AS discounted
```

The field `__key` is a built-in field. The output of this query is:

```
1) "1"
2) 1) "__key"
   1) "bicycle:0"
   2) "price"
   3) "270"
   4) "discounted"
   5) "243"
3) 1) "__key"
   1) "bicycle:5"
   2) "price"
   3) "810"
   4) "discounted"
   5) "729"
4) 1) "__key"
   1) "bicycle:6"
   2) "price"
   3) "2300"
   4) "discounted"
   5) "2070"
...
```

## Simple aggregation

> TODO

## Grouping with aggration

> TODO

## Grouping without aggregation

> TODO


