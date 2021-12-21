# Vector Fields

Vector fields offers the ability to use vector similarity queries in the `FT.SEARCH` command.

Vector Similarity search capability offers the ability to load, index and query vectors stored as fields in a redis hashes. 

At present, the key functionalites offered are:

* Realtime vector indexing supporting 2 Indexing Methods -

    * FLAT - Brute-force index.

    * HNSW - Modified version of [nmslib/hnswlib](https://github.com/nmslib/hnswlib) which is the author's implementation of [Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs](https://arxiv.org/ftp/arxiv/papers/1603/1603.09320.pdf).

* Realtime vector update/delete - triggering update of the index.

* Top K queries supporting 3 distance metrics to measure the degree of similarity between vectors. Metrics:

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

    The `algorithm` attribute specify which algorithm to use when searching for the top-k most similar vectors in the index.

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

## Specific attributse per algorithm

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
        Number the maximal allowed outgoing edges for each node in the graph. Defaults to 16.

    * **EF_CONSTRUCTION** - 
        Number the maximal allowed potential outgoing edges candidates for each node in the graph, during the graph building. Defaults to 200.

    * **EF_RUNTIME** - 
        Number the maximal allowed potential candidates during the top-K query. Defaults to 10.

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

<!-- ## Querying vector fields

TO BE ADDED -->
