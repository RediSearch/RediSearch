---
title: "Exact match"
linkTitle: "Exact match"
description: Simple exact match queries
---

An exact match query allows you to select all documents where a field matches a specific value. 

You can use exact match queries on several field types. The query syntax varies depending on the type. The examples in this article use a schema with the following fields:

| Field name | Field type |
| ---------- | ---------- |
| description| TEXT |
| condition | TAG |
| price | NUMERIC |

You can find more details about creating the index and the demo data in the [quick start guide](/docs/interact/search-and-query/quickstart/).

## Numeric field

To perform an exact match query on a numeric field, you need to construct a range query with the same start and end value:

```
FT.SEARCH index "@field:[start end]"
```

As described in the [article about range queries](/docs/search-and-query/query/range), you can alternatively use the `FILTER` argument:

```
FT.SEARCH index "*" FILTER field start end
```

The following examples show you how to query for bicycles with a price of exactly 270 USD:

```
FT.SEARCH idx:bicycle "@price:[270 270]"
```

```
FT.SEARCH idx:bicycle "*" FILTER price 270 270
```


## Tag field

A tag is a short text. If you need to query for short texts only, then a tag query is more efficient than a full-text query. The [tag vs. text article](TODO) explains why this is the case. 

You can construct a tag query for a single tag in the following way:

```
FT.SEARCH index "@field:{tag}"
```

> Note: The curly brackets are mandatory for tag queries. You can read why in the [combined queries article](TODO).

This short example shows you how to query for new bicycles:

```
FT.SEARCH idx:bicycle "@condition:{new}"
```

## Full-text field

A detailed explanation of full-text queries is available in the [full-text queries documentation](TODO). You can also query for an exact match of words within a text:

```
FT.SEARCH index "@field:\"text\""
```

> Important: The text must be wrapped by escaped double quotes for an exact match query.


Here is an example for finding all bicycles that have a description that contains the exact text 'rough terrain':

```
FT.SEARCH idx:bicycle "@description:\"rough terrain\""
```