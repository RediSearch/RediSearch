---
title: "Exact match queries"
linkTitle: "Exact match"
description: Perform simple exact match queries
weight: 1
---

An exact match query allows you to select all documents where a field matches a specific value. 

You can use exact match queries on several field types. The query syntax varies depending on the type. 

The examples in this article use a schema with the following fields:

| Field name | Field type |
| ---------- | ---------- |
| `description`| `TEXT` |
| `condition` | `TAG` |
| `price` | `NUMERIC` |

You can find more details about creating the index and loading the demo data in the [quick start guide](/docs/interact/search-and-query/quickstart/).

## Numeric field

To perform an exact match query on a numeric field, you need to construct a range query with the same start and end value:

```
FT.SEARCH index "@field:[start end]"
```

As described in the [article about range queries](/docs/interact/search-and-query/query/range), you can also use the `FILTER` argument:

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

A tag is a short sequence of text, for example, "new" or "Los Angeles". 

{{% alert title="Important" color="warning" %}}
If you need to query for short texts, use a tag query instead of a full-text query. Tag fields are more space-efficient for storing index entries and often lead to lower query complexity for exact match queries.
{{% /alert  %}}

You can construct a tag query for a single tag in the following way:

```
FT.SEARCH index "@field:{tag}"
```

{{% alert title="Note" color="warning" %}}
The curly brackets are mandatory for tag queries.
{{% /alert  %}}

This short example shows you how to query for new bicycles:

```
FT.SEARCH idx:bicycle "@condition:{new}"
```

## Full-text field

A detailed explanation of full-text queries is available in the [full-text queries documentation](/docs/interact/search-and-query/query/full-text). You can also query for an exact match of a phrase within a text field:

```
FT.SEARCH index "@field:\"phrase\""
```

{{% alert title="Important" color="warning" %}}
The phrase must be wrapped by escaped double quotes for an exact match query.

You can't use a phrase that starts with a [stop word](/docs/interact/search-and-query/advanced-concepts/stopwords).
{{% /alert  %}}

Here is an example for finding all bicycles that have a description containing the exact text 'rough terrain':

```
FT.SEARCH idx:bicycle "@description:\"rough terrain\""
```