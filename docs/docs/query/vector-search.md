---
title: "Vector search"
linkTitle: "Vector"
description: Query for data based on vector embeddings
weight: 5
aliases:
  - /docs/interact/search-and-query/query/vector-similarity/
---

This article gives you a good overview of how to perform vector search queries with Redis Stack. It's recommended to read more about Redis as a vector database in the [Redis as a vector database quick start guide](/docs/get-started/vector-database/). You can find more detailed information about all the parameters in [vector reference documentation](/docs/interact/search-and-query/advanced-concepts/vectors/).

A vector search query on a vector field allows you to find all vectors in a vector space that are close to a given vector. You can either query for the k-nearest neighbors or vectors within a given radius.

The examples in this article use a schema with the following fields:

| JSON field               | Field alias | Field type  | Description |
| ------------------------ | ----------- | ----------- | ----------- |
| $.description            | description | TEXT        | The description of a bicycle as unstructured text |
| $.description_embeddings | vector      | VECTOR      | The vector that a machine learning model derived from the description text | 


## K-neareast neighbours (KNN)

The Redis command [FT.SEARCH](commands/ft.search/) takes the index name, the query string, and additional query parameters as arguments. You need to pass the number of nearest neighbors, the vector field name and the vector's binary representation over in the following way:

```
FT.SEARCH index "(*)=>[KNN num_neighbours @field $vector]" PARAMS 2 vector "binary_data" DIALECT 2
```

Here is a more detailed explanation of this query:

1. **Pre-filter**: The first expression within the round brackets is a filter. It allows you to decide which vectors should be taken into account before the vector search is performed. The expression `(*)` means that all vectors are considered.
2. **Next step**: The `=>` arrow indicates that the pre-filtering happens before the vector search.
3. **KNN query**: The expression `[KNN num_neighbours @field $vector]` is a parameterized query expression. A parameter name is indicated by the `$` prefix within the query string.
4. **Vector binary data**: You need to use the `PARAMS` argument to substitute `$vector` with the binary representation of the vector. The value `2` indicates that `PARAMS` is followed by two arguments, the parameter name `vector` and the parameter value.
5. **Dialect**: The vector search feature has been available since version two of the query dialect.

You can read more about the `PARAMS` argument in the [FT.SEARCH command reference](/commands/ft.search/).

The following example shows you how to query for the three bikes based on the description embeddings and by using the field alias `vector`. The result is returned in ascending order based on the distance. You can see that the query only returns the fields `__vector_score` and `description`. The field `__vector_score` is out of the box present. Because you can have multiple vector fields in your schema, the vector score field name depends on the name of the vector field. If you change the field name `@vector` to `@foo`, the score field name changes to `__foo_score`.

```
FT.SEARCH idx:bikes_vss "(*)=>[KNN 3 @vector $query_vector]" PARAMS 2 "query_vector" "Z\xf8\x15:\xf23\xa1\xbfZ\x1dI>\r\xca9..." SORTBY "__vector_score" ASC RETURN 2 "__vector_score" "description" DIALECT 2
```

<!-- Python query>
query = (
    Query('(*)=>[KNN 3 @vector $query_vector]')
     .sort_by('__vector_score')
     .return_fields('__vector_score', 'description')
     .dialect(2)
)
</!-->

{{% alert title="Note" color="warning" %}}
The binary value of the query vector is significantly shortened in this example.
{{% /alert  %}}


## Radius

Instead of the number of nearest neighbors, you need to pass the radius along with the index name, the vector field name, and the vector's binary value over:

```
FT.SEARCH index "@field:[VECTOR_RANGE radius $vector]" PARAMS 2 vector "binary_data" DIALECT 2
```

If you want to sort by distance, then you must yield the distance via the range query parameter `$YIELD_DISTANCE_AS`:

```
FT.SEARCH index "@field:[VECTOR_RANGE radius $vector]=>{$YIELD_DISTANCE_AS: dist_field}" PARAMS 2 vector "binary_data" SORTBY dist_field DIALECT 2
```

Here is a more detailed explanation of this query:

1. **Range query**: The syntax of a radius query is very similar to the regular range query, except of the keyword `VECTOR_RANGE`. You can also combine a vector radius query with other queries in the same way as regular range queries.  Please find further details in the [combined queries article](/docs/query/combined).
2. **Additional step**: The `=>` arrow means that the range query is followed by evaluating additional parameters.
3. **Range query parameters**: Parameters such as `$YIELD_DISTANCE_AS`, whereby a full set of parameters can be found in the [vectors reference documentation](/docs/interact/search-and-query/advanced-concepts/vectors/).
4. **Vector binary data**: You need to use `PARAMS` to pass the binary representation of the vector over.
5. **Dialect**: Vector search has been available since version two of the query dialect.


{{% alert title="Note" color="warning" %}}
By default, `FT.SEARCH` returns only the first ten results. The [range query article](/docs/interact/search-and-query/query/range) explains to you how to scroll through the result set.
{{% /alert  %}}

The example below shows a radius query that returns the description and the distance within a radius of `0.5`. The result is again sorted by the distance:

```
FT.SEARCH idx:bikes_vss "@vector:[VECTOR_RANGE 0.5 $query_vector]=>{$YIELD_DISTANCE_AS: vector_dist}" PARAMS 2 "query_vector" "Z\xf8\x15:\xf23\xa1\xbfZ\x1dI>\r\xca9..." SORTBY vector_dist ASC RETURN 2 vector_dist description DIALECT 2
```

<!-- Python query>
query = (
    Query('@vector:[VECTOR_RANGE 0.5 $query_vector]=>{$YIELD_DISTANCE_AS: vector_dist}')
     .sort_by('vector_dist')
     .return_fields('vector_dist', 'description')
     .dialect(2)
)
</!-->