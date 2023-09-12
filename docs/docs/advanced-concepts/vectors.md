---
title: "Vectors"
linkTitle: "Vectors"
weight: 14
math: true
description: Learn how to use vector fields and vector similarity queries
aliases:
  - /docs/stack/search/reference/vectors/
  - /redisearch/reference/vectors
---

*Vector fields* allow you to use vector similarity queries in the `FT.SEARCH` command.
*Vector similarity* enables you to load, index, and query vectors stored as fields in Redis hashes or in JSON documents (via integration with the [JSON module](/docs/stack/json/))

Vector similarity provides these functionalities:

* Realtime vector indexing supporting two indexing methods:

    - FLAT - Brute-force index

    - HNSW - Modified version of [nmslib/hnswlib](https://github.com/nmslib/hnswlib), which is an implementation of [Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs](https://arxiv.org/ftp/arxiv/papers/1603/1603.09320.pdf).

* Realtime vector update/delete, triggering an update of the index.

* K-nearest neighbors (KNN) search and range filter (from v2.6.1) supporting three distance metrics to measure the degree of similarity between two vectors $u$, $v$ $\in \mathbb{R}^n$ where $n$ is the length of the vectors:

    - L2 - Euclidean distance between two vectors

         $d(u, v) = \sqrt{ \displaystyle\sum_{i=1}^n{(u_i - v_i)^2}}$

    - IP - Inner product of two vectors

         $d(u, v) = 1 -u\cdot v$

    - COSINE - Cosine distance of two vectors

         $d(u, v) = 1 -\frac{u \cdot v}{\lVert u \rVert \lVert v  \rVert}$

## Create a vector field

You can add vector fields to the schema in FT.CREATE using this syntax:

```
FT.CREATE ... SCHEMA ... {field_name} VECTOR {algorithm} {count} [{attribute_name} {attribute_value} ...]
```

Where:

* `{algorithm}` must be specified and be a supported vector similarity index algorithm. The supported algorithms are:

    - FLAT - Brute force algorithm.

    - HNSW - Hierarchical Navigable Small World algorithm.

The `{algorithm}` attribute specifies the algorithm to use when searching k most similar vectors in the index or filtering vectors by range.

* `{count}` specifies the number of attributes for the index. Must be specified. 
Notice that `{count}` counts the total number of attributes passed for the index in the   command, although algorithm parameters should be submitted as named arguments. 

For example:

```
FT.CREATE my_idx SCHEMA vec_field VECTOR FLAT 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC L2
```

Here, three parameters are passed for the index (`TYPE`, `DIM`, `DISTANCE_METRIC`), and `count` counts the total number of attributes (6).

* `{attribute_name} {attribute_value}` are algorithm attributes for the creation of the vector index. Every algorithm has its own mandatory and optional attributes.

## Creation attributes per algorithm

### FLAT

Mandatory parameters are:

* `TYPE` - Vector type. Current supported types are `FLOAT32` and `FLOAT64`.
    
* `DIM` - Vector dimension specified as a positive integer.
    
* `DISTANCE_METRIC` - Supported distance metric, one of {`L2`, `IP`, `COSINE`}.

Optional parameters are:

* `INITIAL_CAP` - Initial vector capacity in the index affecting memory allocation size of the index.

* `BLOCK_SIZE` - Block size to hold `BLOCK_SIZE` amount of vectors in a contiguous array.
        This is useful when the index is dynamic with respect to addition and deletion.
        Defaults to 1024.

**Example**

```
FT.CREATE my_index1 
SCHEMA vector_field VECTOR 
FLAT 
10 
TYPE FLOAT32 
DIM 128 
DISTANCE_METRIC L2 
INITIAL_CAP 1000000 
BLOCK_SIZE 1000
```

### HNSW

Mandatory parameters are:

* `TYPE` - Vector type. Current supported types are `FLOAT32` and `FLOAT64`.
    
* `DIM` - Vector dimension, specified as a positive integer.
    
* `DISTANCE_METRIC` - Supported distance metric, one of {`L2`, `IP`, `COSINE`}.

Optional parameters are:

* `INITIAL_CAP` - Initial vector capacity in the index affecting memory allocation size of the index.

* `M` - Number of maximum allowed outgoing edges for each node in the graph in each layer. on layer zero the maximal number of outgoing edges will be `2M`. Default is 16.

* `EF_CONSTRUCTION` - Number of maximum allowed potential outgoing edges candidates for each node in the graph, during the graph building. Default is 200.

* `EF_RUNTIME` - Number of maximum top candidates to hold during the KNN search. 
Higher values of `EF_RUNTIME` lead to more accurate results at the expense of a longer runtime. Default is 10.

* `EPSILON` - Relative factor that sets the boundaries in which a range query may search for candidates. That is, vector candidates whose distance from the query vector is `radius*(1 + EPSILON)` are potentially scanned, allowing more extensive search and more accurate results (on the expense of runtime). Default is 0.01.   

**Example**

```
FT.CREATE my_index2 
SCHEMA vector_field VECTOR 
HNSW 
16 
TYPE FLOAT64 
DIM 128 
DISTANCE_METRIC L2 
INITIAL_CAP 1000000 
M 40 
EF_CONSTRUCTION 250 
EF_RUNTIME 20
EPSILON 0.8
```

## Indexing vectors

### Storing vectors in hashes
Storing vectors in Redis hashes is done by sending a binary blob of vector data. A common way of doing so is by using python numpy with redis-py client:
```py
import numpy as np
from redis import Redis

redis_conn = Redis(host = 'localhost', port = 6379)
vector_field = "vector"
dim = 128

# Store a blob of a random vector of type float32 under a field named 'vector' in Redis hash.
np_vector = np.random.rand(dim).astype(np.float32)
redis_conn.hset('key', mapping = {vector_field: np_vector.tobytes()})
```
Note that the vector blob size must match the vector field dimension and type specified in the schema, otherwise the indexing will fail in the background.  

### Storing vectors in JSON
Vector fields are supported upon indexing fields of JSON documents as well:

```
FT.CREATE my_index ON JSON SCHEMA $.vec as vector VECTOR FLAT 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2 
```

Unlike in hashes, vectors are stored in JSON documents as arrays (not as blobs).

**Example**
```
JSON.SET 1 $ '{"vec":[1,2,3,4]}'
```

As of v2.6.1, JSON supports multi-value indexing. This capability accounts for vectors as well. Thus, it is possible to index multiple vectors under the same JSONPath. Additional information is available in the [Indexing JSON documents](/docs/stack/search/indexing_json/#index-json-arrays-as-vector) section. 

**Example**
```
JSON.SET 1 $ '{"vec":[[1,2,3,4], [5,6,7,8]]}'
JSON.SET 1 $ '{"foo":{"vec":[1,2,3,4]}, "bar":{"vec":[5,6,7,8]}}'
```

## Querying vector fields

You can use vector similarity queries in the `FT.SEARCH` query command. To use a vector similarity query, you must specify the option `DIALECT 2` or greater in the command itself, or set the `DEFAULT_DIALECT` option to `2` or greater, by either using the command `FT.CONFIG SET` or when loading the `redisearch` module and passing it the argument `DEFAULT_DIALECT 2`.

There are two types of vector queries: *KNN* and *range*:

### KNN search
The syntax for vector similarity KNN queries is `*=>[<vector_similarity_query>]` for running the query on an entire vector field, or `<primary_filter_query>=>[<vector_similarity_query>]` for running similarity query on the result of the primary filter query. 

As of version 2.4, you can use vector similarity *once* in the query, and over the entire query filter.

**Invalid example** 

`"(@title:Matrix)=>[KNN 10 @v $B] @year:[2020 2022]"`

**Valid example** 

`"(@title:Matrix @year:[2020 2022])=>[KNN 10 @v $B]"`

The `<vector_similarity_query>` part inside the square brackets needs to be in the following format:

```
KNN (<number> | $<number_attribute>) @<vector_field> $<blob_attribute> [<vector_query_param_name> <value>|$<value_attribute>] [...]] [ AS <dist_field_name> | $<dist_field_name_attribute>]
```

Every `*_attribute` parameter should refer to an attribute in the [`PARAMS`](/commands/ft.search) section.

* `<number> | $<number_attribute>` - Number of requested results ("K").

* `@<vector_field>` - `vector_field` should be the name of a vector field in the index.

* `$<blob_attribute>` - An attribute that holds the query vector as blob and must be passed through the `PARAMS` section. The blob's byte size should match the vector field dimension and type.

* `[<vector_query_param_name> <value>|$<value_attribute> [...]]` - An optional part for passing one or more vector similarity query parameters. Parameters should come in key-value pairs and should be valid parameters for the query. See which [runtime parameters](/docs/stack/search/reference/vectors/#runtime-attributes) are valid for each algorithm.

* `[AS <dist_field_name> | $<dist_field_name_attribute>]` - An optional part for specifying a distance field name, for later sorting by the similarity metric and/or returning it. By default, the distance field name is "`__<vector_field>_score`" and it can be used for sorting without using `AS <dist_field_name>` in the query.

**Note:** As of v2.6, vector query params and distance field name can be specified in [query attributes](/docs/stack/search/reference/query_syntax/#query-attributes) like syntax as well. Thus, the following format is also supported:

```
<primary_filter_query>=>[<vector_similarity_query>]=>{$<param>: (<value> | $<value_attribute>); ... }
```

where every valid `<vector_query_param_name>` can be sent as a `$<param>`, and `$yield_distance_as` is the equivalent for `AS` with respect to specifying the optional `<dist_field_name>` (see examples below). 

### Range query

Range queries is a way of filtering query results by the distance between a vector field value and a query vector, in terms of the relevant vector field distance metric.  
The syntax for range query is `@<vector_field>: [VECTOR_RANGE (<radius> | $<radius_attribute>) $<blob_attribute>]`. Range queries can appear multiple times in a query, similarly to NUMERIC and GEO clauses, and in particular they can be a part of the `<primary_filter_query>` in KNN Hybrid search.

* `@<vector_field>` - `vector_field` should be the name of a vector field in the index.

* `<radius> | $<radius_attribute>` - A positive number that indicates the maximum distance allowed between the query vector and the vector field value.

* `$<blob_attribute>` - An attribute that holds the query vector as blob and must be passed through the `PARAMS` section. The blob's byte size should match the vector field dimension and type.

* **Range query params**: range query clause can be followed by a query attributes section as following: `@<vector_field>: [VECTOR_RANGE (<radius> | $<radius_attribute>) $<blob_attribute>]=>{$<param>: (<value> | $<value_attribute>); ... }`, where the relevant params in that case are `$yield_distance_as` and `$epsilon`. Note that there is **no default distance field name** in range queries.

## Hybrid queries

Vector similarity KNN queries of the form `<primary_filter_query>=>[<vector_similarity_query>]` are considered *hybrid queries*. Redis Stack has an internal mechanism for optimizing the computation of such queries. Two modes in which hybrid queries are executed are: 

1. Batches mode - In this mode, a batch of the high-scoring documents from the vector index are retrieved. These documents are returned ONLY if `<primary_filter_query>` is satisfied. In other words, the document contains a similar vector and meets the filter criteria. The procedure terminates when `k` documents that pass the `<primary_filter_query>` are returned or after every vector in the index was obtained and processed.
    
The *batch size* is determined by a heuristics that is based on `k`, and the ratio between the expected number of documents in the index that pass the `<primary_filter_query>` and the vector index size. 
The goal of the heuristics is to minimize the total number of batches required to get the `k` results, while preserving a small batch size as possible. 
Note that the batch size may change *dynamically* in each iteration, based on the number of results that passed the filter in previous batches.

2. Ad-hoc brute-force mode - In general, this approach is preferable when the number of documents that pass the `<primary_filter_query>` part of the query is relatively small. 
Here, the score of *every* vector which corresponds to a document that passes the filter is computed, and the top `k` results are selected and returned. 
Note that the results of the KNN query will *always be accurate* in this mode, even if the underline vector index algorithm is an approximate one.

The specific execution mode of a hybrid query is determined by a heuristics that aims to minimize the query runtime, and is based on several factors that derive from the query and the index. 
Moreover, the execution mode may change from *batches* to *ad-hoc BF* during the run, based on estimations of some relevant factors, that are being updated from one batch to another.  

## Runtime attributes

### Hybrid query attributes

These optional attributes allow overriding the default auto-selected policy in which a hybrid query is executed:

* `HYBRID_POLICY` - The policy to run the hybrid query in. Possible values are `BATCHES` and `ADHOC_BF` (not case sensitive). Note that the batch size will be auto selected dynamically in `BATCHES` mode, unless the `BATCH_SIZE` attribute is given.

* `BATCH_SIZE` - A fixed batch size to use in every iteration, when the `BATCHES` policy is auto-selected or requested.

### Algorithm-specific attributes

#### FLAT

Currently, no runtime parameters are available for FLAT indexes.

#### HNSW

Optional parameters are:

* `EF_RUNTIME` - The number of maximum top candidates to hold during the KNN search. Higher values of `EF_RUNTIME` will lead to a more accurate results on the expense of a longer runtime. Defaults to the `EF_RUNTIME` value passed on creation (which defaults to 10).

* `EPSILON` - Relative factor that sets the boundaries in which a range query may search for candidates. That is, vector candidates whose distance from the query vector is `radius*(1 + EPSILON)` are potentially scanned, allowing more extensive search and more accurate results (on the expense of runtime). Defaults to the `EPSILON` value passed on creation (which defaults to 0.01).

{{% alert title="Important notes" color="info" %}}

1. Although specifying `K` requested results in KNN search, the default `LIMIT` is 10, to get all the returned results, specify `LIMIT 0 <K>` in your command.

2. By default, the results are sorted by their document's score. To sort by some vector similarity score, use `SORTBY <dist_field_name>`. See examples below.

3. It is recommended to adjust the `<radius>` parameter in range queries to the corresponding vector field distance metric and to the data itself. In particular, recall that the distance between the vectors in an index whose distance metric is Cosine is bounded by `2`, while L2 distance between the vectors is not bounded. Hence, it is better to consider the distance between the vectors that are considered similar and choose `<radius>` accordingly.   

{{% /alert %}}

## Vector search examples

### "Pure" KNN queries
Return the 10 documents for which the vector stored under its `vec` field is the closest to the vector represented by the following 4-bytes blob:
```
FT.SEARCH idx "*=>[KNN 10 @vec $BLOB]" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2
```
Now, sort the results by their distance from the query vector: 
```
FT.SEARCH idx "*=>[KNN 10 @vec $BLOB]" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" SORTBY __vec_score DIALECT 2
```
Return the top 10 similar documents, use *query params* (see "params" section in [FT.SEARCH command](/commands/ft.search/)) for specifying `K` and `EF_RUNTIME` parameter, and set `EF_RUNTIME` value to 150 (assuming `vec` is an HNSW index):
```
FT.SEARCH idx "*=>[KNN $K @vec $BLOB EF_RUNTIME $EF]" PARAMS 6 BLOB "\x12\xa9\xf5\x6c" K 10 EF 150 DIALECT 2
```
Similar to the previous queries, this time use a custom distance field name to sort by it: 
```
FT.SEARCH idx "*=>[KNN $K @vec $BLOB AS my_scores]" PARAMS 4 BLOB "\x12\xa9\xf5\x6c" K 10 SORTBY my_scores DIALECT 2
```
Use query attributes syntax to specify optional parameters and the distance field name: 
```
FT.SEARCH idx "*=>[KNN 10 @vec $BLOB]=>{$EF_RUNTIME: $EF; $YIELD_DISTANCE_AS: my_scores}" PARAMS 4 EF 150 BLOB "\x12\xa9\xf5\x6c" SORTBY my_scores DIALECT 2
```

### Hybrid KNN queries
Among documents that have `'Dune'` in their `title` field and their `num` value is in the range `[2020, 2022]`, return the top 10 for which the vector stored in the `vec` field is the closest to the vector represented by the following 4-bytes blob:
```
FT.SEARCH idx "(@title:Dune @num:[2020 2022])=>[KNN $K @vec $BLOB AS my_scores]" PARAMS 4 BLOB "\x12\xa9\xf5\x6c" K 10 SORTBY my_scores DIALECT 2
```
Use a different filter for the hybrid query: this time, return the top 10 results from the documents that contain a `'shirt'` tag  in the `type` field and optionally a `'blue'` tag in their `color` field. Here, the results are sorted by the full-text scorer.
```
FT.SEARCH idx "(@type:{shirt} ~@color:{blue})=>[KNN $K @vec $BLOB]" PARAMS 4 BLOB "\x12\xa9\xf5\x6c" K 10 DIALECT 2
```
And, here's a hybrid query in which the hybrid policy is set explicitly to "ad-hoc brute force" (rather than auto-selected):
```
FT.SEARCH idx "(@type:{shirt})=>[KNN $K @vec $BLOB HYBRID_POLICY ADHOC_BF]" PARAMS 4 BLOB "\x12\xa9\xf5\x6c" K 10 SORTBY __vec_scores DIALECT 2
```
And, now, here's a hybrid query in which the hybrid policy is set explicitly to "batches", and the batch size is set explicitly to be 50 using a query parameter:
```
FT.SEARCH idx "(@type:{shirt})=>[KNN $K @vec $BLOB HYBRID_POLICY BATCHES BATCH_SIZE $B_SIZE]" PARAMS 6 BLOB "\x12\xa9\xf5\x6c" K 10 B_SIZE 50 DIALECT 2
```
Run the same query as above and use the query attributes syntax to specify optional parameters:
```
FT.SEARCH idx "(@type:{shirt})=>[KNN 10 @vec $BLOB]=>{$HYBRID_POLICY: BATCHES; $BATCH_SIZE: 50}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2
```

See additional Python examples in [this Jupyter notebook](https://github.com/RediSearch/RediSearch/blob/master/docs/docs/vecsim-hybrid_queries_examples.ipynb)

### Range queries

Return 100 documents for which the distance between its vector stored under the `vec` field to the specified query vector blob is at most 5 (in terms of `vec` field `DISTANCE_METRIC`):
```
FT.SEARCH idx "@vec:[VECTOR_RANGE $r $BLOB]" PARAMS 4 BLOB "\x12\xa9\xf5\x6c" r 5 LIMIT 0 100 DIALECT 2
```
Run the same query as above and set `EPSILON` parameter to `0.5` (assuming `vec` is HNSW index), yield the vector distance between `vec` and the query result in a field named `my_scores`, and sort the results by that distance.
```
FT.SEARCH idx "@vec:[VECTOR_RANGE 5 $BLOB]=>{$EPSILON:0.5; $YIELD_DISTANCE_AS: my_scores}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" SORTBY my_scores LIMIT 0 100 DIALECT 2
```
Use the vector range query in a complex query: return all the documents that contain either `'shirt'` in their `type` tag with their `num` value in the range `[2020, 2022]` OR a vector stored in `vec` whose distance from the query vector is no more than `0.8`, then sort results by their vector distance, if it is in the range: 
```
FT.SEARCH idx "(@type:{shirt} @num:[2020 2022]) | @vec:[VECTOR_RANGE 0.8 $BLOB]=>{$YIELD_DISTANCE_AS: my_scores}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" SORTBY my_scores DIALECT 2
```

See additional Python examples in [this Jupyter notebook](https://github.com/RediSearch/RediSearch/blob/master/docs/docs/vecsim-range_queries_examples.ipynb)
