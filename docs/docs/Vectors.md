# Vector Fields

Vector fields offers the ability to use vector similarity queries in the `FT.SEARCH` command.

Vector Similarity search capability offers the ability to load, index and query vectors stored as fields in a redis hashes. 

At present, the key functionalites offered are:

* Realtime vector indexing supporting 2 Indexing Methods -

    * FLAT - Brute-force index.

    * HNSW - Modified version of [nmslib/hnswlib](https://github.com/nmslib/hnswlib) which is the author's implementation of [Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs](https://arxiv.org/ftp/arxiv/papers/1603/1603.09320.pdf).

* Realtime vector update/delete - triggering update of the index.

* K nearest neighbors queries supporting 3 distance metrics to measure the degree of similarity between vectors. Metrics:

    * L2 - Euclidean distance between two vectors.

    * IP - Internal product of two vectors.

    * COSINE - Cosine similarity of two vectors.

## Creating a vector field

Vector fields can be added to the schema in FT.CREATE with the following syntax:

```
FT.CREATE ... SCHEMA ... {field_name} VECTOR {algorithm} {count} [{attribute_name} {attribute_value} ...]
```

* `{algorithm}`

    Must be specified and be a supported vector similarity index algorithm. the supported algorithms are:

    **FLAT** - brute force algorithm.

    **HNSW** - Hierarchical Navigable Small World algorithm.

    The `algorithm` attribute specify which algorithm to use when searching for the k most similar vectors in the index.

* `{count}`

    Specify the number of attributes for the index. Must be specified.

    Notice that this attribute counts the total number of attributes passed for the index in the command,
    although algorithm parameters should be submitted as named arguments. For example:

    ```
    FT.CREATE my_idx SCHEMA vec_field VECTOR FLAT 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC L2
    ```

    Here we pass 3 parameters for the index (TYPE, DIM, DISTANCE_METRIC), and `count` counts the total number of attributes (6).

* `{attribute_name} {attribute_value}`

    Algorithm attributes for the creation of the vector index. Every algorithm has its own mandatory and optional attributes.

## Specific creation attributes per algorithm

### FLAT

* **Mandatory parameters**

    * **TYPE** - 
        Vector type. Current supported type is `FLOAT32`.
    
    * **DIM** - 
        Vector dimention. should be positive integer.
    
    * **DISTANCE_METRIC** - 
        Supported distance metric. Currently one of **{`L2`, `IP`, `COSINE`}**

* **Optional parameters**

    * **INITIAL_CAP** - 
        Initial vector capacity in the index. Effects memory allocation size of the index.

    * **BLOCK_SIZE** - 
        block size to hold `BLOCK_SIZE` amount of vectors in a contiguous array.
        This is useful when the index is dynamic with respect to addition and deletion.
        Defaults to 1048576 (1024*1024).

* Example

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

* **Mandatory parameters**

    * **TYPE** - 
        Vector type. Current supported type is `FLOAT32`.
    
    * **DIM** - 
        Vector dimention. should be positive integer.
    
    * **DISTANCE_METRIC** - 
        Supported distance metric. Currently one of **{`L2`, `IP`, `COSINE`}**

* **Optional parameters**

    * **INITIAL_CAP** - 
        Initial vector capacity in the index. Effects memory allocation size of the index.

    * **M** - 
        Number the maximal allowed outgoing edges for each node in the graph in each layer. on layer zero the maximal number of outgoing edges will be `2M`. Defaults to 16.

    * **EF_CONSTRUCTION** - 
        Number the maximal allowed potential outgoing edges candidates for each node in the graph, during the graph building. Defaults to 200.

    * **EF_RUNTIME** - 
        The number of maximum top candidates to hold during the KNN search. Higher values of `EF_RUNTIME` will lead to a more accurate results on the expense of a longer runtime. Defaults to 10.

* Example

    ```
    FT.CREATE my_index2 
    SCHEMA vector_field VECTOR 
    HNSW 
    14 
    TYPE FLOAT32 
    DIM 128 
    DISTANCE_METRIC L2 
    INITIAL_CAP 1000000 
    M 40 
    EF_CONSTRUCTION 250 
    EF_RUNTIME 20
    ```

## Querying vector fields

We allow using vector similarity queries in the `FT.SEARCH` "query" parameter. The syntax for vector similarity queries is `*=>[{vector similarity query}]` for running the query on an entire vector field, or `{primary filter query}=>[{vector similarity query}]` for running similarity query on the result of the primary filter query.

As of version 2.4, we allow vector similarity to be used **once** in the query, and over the entire query filter.

* Invalid example: `"(@title:Matrix)=>[KNN 10 @v $B] @year:[2020 2022]"`

* Valid example: `"(@title:Matrix @year:[2020 2022])=>[KNN 10 @v $B]"`

The `{vector similarity query}` part inside the square brackets needs to be in the following format:

    KNN { number | $number_attribute } @{vector field} $blob_attribute [{vector query param name} {value|$value_attribute} [...]] [ AS {score field name | $score_field_name_attribute}]

Every "`*_attribute`" parameter should refer to an attribute in the [`PARAMS`](Commands.md#ftsearch) section.

*   `{ number | $number_attribute }` - The number of requested results ("K").

*   `@{vector field}` - `vector field` should be a name of a vector field in the index.

*   `$blob_attribute` - An attribute that holds the query vector as blob. must be passed through the `PARAMS` section.

*   `[{vector query param name} {value|$value_attribute} [...]]` - An optional part for passing vector similarity query parameters. Parameters should come in key-value pairs and should be valid parameters for the query. see what [runtime parameters](Vectors.md#specific_runtime_attributse_per_algorithm) are valid for each algorithm.

*   `[ AS {score field name | $score_field_name_attribute}]` - An optional part for specifing a score field name, for later sorting by the similarity score. By default the score field name is "`__{vector field}_score`" and it can be used for sorting without using `AS {score field name}` in the query.

## Specific runtime attributes per algorithm

### FLAT

Currently there are no runtime parameters available for FLAT indexes

### HNSW

* **Optional parameters**

    * **EF_RUNTIME** - 
        The number of maximum top candidates to hold during the KNN search. Higher values of `EF_RUNTIME` will lead to a more accurate results on the expense of a longer runtime. Defaults to the `EF_RUNTIME` value passed on creation (which defaults to 10).

### A few notes

1. Although specifing `K` requested results, the default `LIMIT` in RediSearch is 10, so for getting all the returned results, make sure to specify `LIMIT 0 {K}` in your command.

2. By default, the resluts are sorted by their documents default RediSearch score. for getting the results sorted by similarity score, use `SORTBY {score field name}` as explained earlier.

### Examples for querying vector fields

*   ```
    FT.SEARCH idx "*=>[KNN 100 @vec $BLOB]" PARAMS 2 BLOB "\12\a9\f5\6c"
    ```

*   ```
    FT.SEARCH idx "*=>[KNN 100 @vec $BLOB]" PARAMS 2 BLOB "\12\a9\f5\6c" SORTBY __vec_score
    ```

*   ```
    FT.SEARCH idx "*=>[KNN $K @vec $BLOB EF_RUNTIME $EF]" PARAMS 6 BLOB "\12\a9\f5\6c" K 10 EF 150
    ```

*   ```
    FT.SEARCH idx "*=>[KNN $K @vec $BLOB AS my_scores]" PARAMS 4 BLOB "\12\a9\f5\6c" K 10 SORTBY my_scores
    ```

*   ```
    FT.SEARCH idx "(@title:Dune @num:[2020 2022])=>[KNN $K @vec $BLOB AS my_scores]" PARAMS 4 BLOB "\12\a9\f5\6c" K 10 SORTBY my_scores
    ```

*   ```
    FT.SEARCH idx "(@type:{shirt} ~@color:{blue})=>[KNN $K @vec $BLOB AS my_scores]" PARAMS 4 BLOB "\12\a9\f5\6c" K 10 SORTBY my_scores
    ```
