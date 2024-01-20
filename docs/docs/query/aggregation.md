---
title: "Aggregation queries"
linkTitle: "Aggregation"
description: Group and aggregate query results
weight: 10
---

An aggregation query allows you to perform the following actions:

- Apply simple mapping functions.
- Group data based on field values.
- Apply aggregation functions on the grouped data.

This article explains the basic usage of the [FT.AGGREGATE](/commands/ft.aggregate/) command. For further details, see the [command specification](/commands/ft.aggregate/) and the [aggregations reference documentation](/docs/interact/search-and-query/advanced-concepts/aggregations).

The examples in this article use a schema with the following fields:

| Field name | Field type |
| ---------- | ---------- |
| `condition` | `TAG` |
| `price` | `NUMERIC` |

## Simple mapping

The `APPLY` clause allows you to apply a simple mapping function to a result set that is returned based on the query expression.

```
FT.AGGREGATE index "query_expr" LOAD n "field_1" .. "field_n" APPLY "function_expr" AS "result_field"
```

Here is a more detailed explanation of the query syntax:

1. **Query expression**: you can use the same query expressions as you would use with the `FT.SEARCH` command. You can substitute `query_expr` with any of the expressions explained in the articles of this [query topic](/docs/interact/search-and-query/query/). Vector search queries are an exception. You can't combine a vector search with an aggregation query.
2. **Loaded fields**: if field values weren't already loaded into the aggregation pipeline, you can force their presence via the `LOAD` clause. This clause takes the number of fields (`n`), followed by the field names (`"field_1" .. "field_n"`).
3. **Mapping function**: this mapping function operates on the field values. A specific field is referenced as `@field_name` within the function expression. The result is returned as `result_field`.

The following example shows you how to calculate a discounted price for new bicycles:

```
FT.AGGREGATE idx:bicycle "@condition:{new}" LOAD 2 "__key" "price" APPLY "@price - (@price * 0.1)" AS "discounted"
```

The field `__key` is a built-in field. 

The output of this query is:

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

## Grouping with aggregation

The previous example did not group the data. You can group and aggregate data based on one or many criteria in the following way:

```
FT.AGGREGATE index "query_expr" ...  GROUPBY n "field_1" .. "field_n" REDUCE AGG_FUNC m "@field_param_1" .. "@field_param_m" AS "aggregated_result_field"
```

Here is an explanation of the additional constructs:

1. **Grouping**: you can group by one or many fields. Each ordered sequence of field values then defines one group. It's also possible to group by values that resulted from a previous `APPLY ... AS`.
2. **Aggregation**: you must replace `AGG_FUNC` with one of the supported aggregation functions (e.g., `SUM` or `COUNT`). A complete list of functions is available in the [aggregations reference documentation](/docs/interact/search-and-query/advanced-concepts/aggregations). Replace `aggregated_result_field` with a value of your choice.

The following query shows you how to group by the field `condition` and apply a reduction based on the previously derived `price_category`. The expression `@price<1000` causes a bicycle to have the price category `1` if its price is lower than 1000 USD. Otherwise, it has the price category `0`. The output is the number of affordable bicycles grouped by price category.

```
FT.AGGREGATE idx:bicycle "*" LOAD 1 price APPLY "@price<1000" AS price_category GROUPBY 1 @condition REDUCE SUM 1 "@price_category" AS "num_affordable"
```

```
1) "3"
2) 1) "condition"
   1) "refurbished"
   2) "num_affordable"
   3) "1"
3) 1) "condition"
   1) "used"
   2) "num_affordable"
   3) "1"
4) 1) "condition"
   1) "new"
   2) "num_affordable"
   3) "3"
```

{{% alert title="Note" color="warning" %}}
You can also create more complex aggregation pipelines with [FT.AGGREGATE](/commands/ft.aggregate/). Applying multiple reduction functions under one `GROUPBY` clause is possible. In addition, you can also chain groupings and mix in additional mapping steps (e.g., `GROUPBY ... REDUCE ... APPLY ... GROUPBY ... REDUCE`)
{{% /alert  %}}


## Aggregating without grouping

You can't use an aggregation function outside of a `GROUPBY` clause, but you can construct your pipeline in a way that the aggregation happens on a single group that spans all documents. If your documents don't share a common attribute, you can add it via an extra `APPLY` step.

Here is an example that adds a type attribute `bicycle` to each document before counting all documents with that type:

```
FT.AGGREGATE idx:bicycle "*" APPLY "'bicycle'" AS type GROUPBY 1 @type REDUCE COUNT 0 AS num_total
```

The result is:

```
1) "1"
2) 1) "type"
   1) "bicycle"
   2) "num_total"
   3) "10"
```

## Grouping without aggregation

It's sometimes necessary to group your data without applying a mathematical aggregation function. If you need a grouped list of values, then the `TOLIST` function is helpful.

The following example shows how to group all bicycles by `condition`:

```
FT.AGGREGATE idx:bicycle "*" LOAD 1 "__key" GROUPBY 1 "@condition" REDUCE TOLIST 1 "__key" AS bicylces
```

The output of this query is:

```
1) "3"
2) 1) "condition"
   1) "refurbished"
   2) "bicylces"
   3) 1) "bicycle:9"
3) 1) "condition"
   1) "used"
   2) "bicylces"
   3) 1) "bicycle:1"
      1) "bicycle:2"
      2) "bicycle:3"
      3) "bicycle:4"
4) 1) "condition"
   1) "new"
   2) "bicylces"
   3) 1) "bicycle:0"
      1) "bicycle:5"
      2) "bicycle:6"
      3) "bicycle:8"
      4) "bicycle:7"
```