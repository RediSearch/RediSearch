---
title: "Combined queries"
linkTitle: "Combined"
description: Combine query expressions
weight: 9
---

A combined query is a combination of several query types, such as:

* [Exact match](/docs/interact/search-and-query/query/exact-match)
* [Range](/docs/interact/search-and-query/query/range)
* [Full-text](/docs/interact/search-and-query/query/full-text)
* [Geo-spatial](/docs/interact/search-and-query/query/geo-spatial)
* [Vector search](/docs/interact/search-and-query/query/vector-search)

You can use logical query operators to combine query expressions for numeric, tag, and text fields. For vector fields, you can combine a KNN query with a pre-filter.

{{% alert title="Note" color="warning" %}}
The operators are interpreted slightly differently, depending on the query dialect used. This article uses the second version of the query dialect and tries to straighten the examples with the help of additional brackets (`(...)`). Further details can be found in the [query syntax documentation](/docs/interact/search-and-query/advanced-concepts/query_syntax/). 
{{% /alert  %}}

The examples in this article use the following schema:

| Field name  | Field type |
| ----------- | ---------- |
| description | TEXT       |
| condition   | TAG        |
| price       | NUMERIC    |
| vector      | VECTOR     |

## AND

The binary operator ` ` (space) is used to intersect the results of two or more expressions.

```
FT.SEARCH index "(expr1) (expr2)"
```

If you want to perform an intersection based on multiple values within a specific text field, then you should use the following simplified notion:

```
FT.SEARCH index "@text_field:( value1 value2 ... )"
```

The following example shows you a query that finds bicycles in a price range from 500 USD to 1000 USD:

```
FT.SEARCH idx:bicycle "@price:[500 1000] @condition:{new}"
```

You might also be interested in bicycles for kids. The query below shows you how to combine a full-text search with the criteria of the previous query:

```
FT.SEARCH idx:bicycle "kids (@price:[500 1000] @condition:{used})"
```

## OR

You can use the binary operator `|` (vertical slash) to perform a union.

```
FT.SEARCH index "(expr1) | (expr2)"
```

{{% alert title="Note" color="warning" %}}
The logical `AND` takes precedence over `OR` when using dialect version two. The expression `expr1 expr2 | expr3 expr4` means `(expr1 expr2) | (expr3 expr4)`. Version one of the query dialect behaves differently. It is advised to use round brackets in query strings to ensure that the order is clear.
 {{% /alert  %}}


If you want to perform the union based on multiple values within a single tag or text field, then you should use the following simplified notion:


```
FT.SEARCH index "@text_field:( value1 | value2 | ... )"
```

```
FT.SEARCH index "@tag_field:{ value1 | value2 | ... }"
```

The following query shows you how to find used bicycles that contain either the word 'kids' or 'small':

```
FT.SEARCH idx:bicycle "(kids | small) @condition:{used}"
```

The previous query searches across all text fields. The following example shows you how to limit the search to the description field:

```
FT.SEARCH idx:bicycle "@description:(kids | small) @condition:{used}"
```

If you want to extend the search to new bicycles, then the below example shows you how to do that:

```
FT.SEARCH idx:bicycle "@description:(kids | small) @condition:{new | used}"
```


## NOT

A minus (`-`) in front of a query expression negates the expression.

```
FT.SEARCH index "-(expr)"
```

If you want to exclude new bicycles from the search within the previous price range, you can use this query:

```
FT.SEARCH idx:bicycle "@price:[500 1000] -@condition:{new}"
```

## Numeric filter

The [FT.SEARCH](/commands/ft.search/) command allows you to combine any query expression with a numeric filter.

```
FT.SEARCH index "expr" FILTER numeric_field start end
```

Please see the [range query article](/docs/interact/search-and-query/query/range) to learn more about numeric range queries and such filters.


## Pre-filter for a KNN  vector query

You can use a simple or more complex query expression with logical operators as a pre-filter in a KNN vector query. 

```
FT.SEARCH index "(filter_expr)=>[KNN num_neighbours @field $vector]" PARAMS 2 vector "binary_data" DIALECT 2
```

Here is an example:

```
FT.SEARCH idx:bikes_vss "(@price:[500 1000] @condition:{new})=>[KNN 3 @vector $query_vector]" PARAMS 2 "query_vector" "Z\xf8\x15:\xf23\xa1\xbfZ\x1dI>\r\xca9..." DIALECT 2
```

The [vector search article](/docs/interact/search-and-query/query/vector-search) provides further details about vector queries in general.