---
title: "Vector similarity"
linkTitle: "Vector similarity"
weight: 15
description: Use Redis as a vector database
aliases:
  - /docs/stack/search/reference/vectors/
  - /redisearch/reference/vectors
---
## The unstructured data problem

Today, about 80% of the data organizations generate is unstructured; data that either does not have a well-defined schema or cannot be restructured into a familiar columnar format. Typical examples of unstructured data include free-form text, images, videos, and sound clips. The amount of unstructured data is expected to grow in the coming decades.

Unstructured data is high-dimensional and noisy, making it more challenging to analyze and interpret using traditional methods. But it is also packed with information and meaning.  

Traditionally, unstructured data is processed to extract specific features, effectively turning it into structured data. Once in the realm of structured data, we can search the data with SQL queries (if stored in a relational database) or with a text search engine.

The approach of transforming unstructured data into structured data has a few issues. First, engineering features out of unstructured data can be computationally expensive and error-prone, significantly delaying when we can effectively use the data. Second, some fidelity and information may be lost in the extraction/transformation process, because unique, latent features can't be easily categorized or quantified.

## Enter vector databases

An approach to dealing with unstructured data is to vectorize the data. By vectorizing, we mean to somehow convert something like a text passage, an image, a video, or a song into a flat sequence of numbers representing a particular piece of data. These vectors are representations of the data in N-dimensional space. With vectorizing, we gain the ability to use linear algebra techniques to compare, group, and operate on our data. This is the foundation of a vector database; the ability to store and operate on vectors. This approach is not new and has been around for a long time. The difference today is how the techniques for generating the vectors have advanced.

## Using machine learning embeddings as vectors

Traditional methods for converting unstructured textual data into vector form include Bag-of-Words (BoW) and Term Frequency-Inverse Document Frequency (TF-IDF). For categorical data, one-hot encoding is a commonly used approach. Hashing and feature extraction techniques, such as edge detection, texture analysis, or color histograms, have been employed for high-dimensionality data like images.

While powerful in their own right, these approaches reveal limitations when confronted with high-dimensional and intricate data forms like long text passages, images, and audio. Consider, for example, how a text passage could be restructured through sentence rearrangement, synonym usage, or alterations in narrative style. Such simple modifications could effectively sidestep techniques like Bag-of-Words, preventing systems using the generated encodings from identifying text passages with similar meanings.

This is where advancements in machine learning, particularly deep learning, make their mark. Machine learning models have facilitated the rise of embeddings as a widely embraced method for generating dense, low-dimensional vector representations. Given a suitable model, the generated embeddings can encapsulate complex patterns and semantic meanings inherent in data, thus overcoming the limitations of their traditional counterparts.

### Generate embeddings for the bikes dataset

To investigate vector similarity, we'll use a subset of the **bikes** dataset, a relatively simple synthetic dataset. The dataset has 11 bicycle records in a JSON file named `bikes.json` and includes the fields `model`, `brand`, `price`, `type`, `specs`, and `description`. The `description` field is particularly interesting for our purposes since it consists of a free-form textual description of a bicycle.

#### Before getting started with the code examples

Code examples are currently provided for Redis CLI and Python. For Python, you will need to create a virtual environment and install the following Python packages:

1. redis
1. pandas
1. sentence-transformers
1. (optional) tabulate; this package is used by Pandas to convert dataframe tables to Markdown

You'll also need the following imports:

{{< clients-example search_vss imports />}}

#### Loading json **bikes** dataset

Let's load the bikes dataset as a JSON array using the following Python 3 code:

{{< clients-example search_vss get_data />}}

#### Inspect the bikes JSON

Let's inspect the content of the JSON array in table form:

| model | brand | price | type | specs | description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Jigger | Velorim | 270 | Kids bikes | {'material': 'aluminium', 'weight': '10'} | Small and powerful, the Jigger is the best rid... |
| Hillcraft | Bicyk | 1200 | Kids Mountain Bikes | {'material': 'carbon', 'weight': '11'} | Kids want to ride with as little weight as pos... |
| Chook air 5 | Nord | 815 | Kids Mountain Bikes | {'material': 'alloy', 'weight': '9.1'} | The Chook Air 5  gives kids aged six years and... |
`...`

Let's take a look at the structure of one of our bike JSON documents:

{{< clients-example search_vss dump_data />}}

```json
{
  "model": "Jigger",
  "brand": "Velorim",
  "price": 270,
  "type": "Kids bikes",
  "specs": {
    "material": "aluminium",
    "weight": "10"
  },
  "description": "Small and powerful, the Jigger is the best ride for the smallest of tikes! ...
}
```

#### Generating text embeddings using SentenceTransformers

We will use the [SentenceTransformers](https://www.sbert.net/) framework to generate embeddings for the bikes descriptions. Sentence-BERT (SBERT) is a [BERT](https://en.wikipedia.org/wiki/BERT_(language_model)) model modification that produces consistent and contextually rich sentence embeddings. SBERT improves tasks like semantic search and text grouping by allowing for efficient and meaningful comparison of sentence-level semantic similarity.

### Selecting a suitable pre-trained model

We must pick a suitable model based on the task at hand when generating embeddings. In our case, we want to query for bicycles using short sentences against the longer bicycle descriptions. This is referred to as **asymmetric semantic search**, often employed in cases where the search query and the documents being searched are of a different nature or structure. Suitable models for asymmetric semantic search include pre-trained [MS MARCO](https://microsoft.github.io/msmarco/) models. MS MARCO models are optimized for understanding real-world queries and retrieving relevant responses. They are widely used in search engines, chatbots, and other AI applications. At the time this tutorial was written, the highest performing MS MARCO model tuned for cosine-similarity available in the SentenceTranformers package is `msmarco-distilbert-base-v4`. 

Let's load the model using the `SentenceTransformer` function:

```python
from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer('msmarco-distilbert-base-v4')
```

Let's grab the description from the first bike in the JSON array:

```python
from textwrap import TextWrapper

sample_description = bikes[0]['description']
wrapped_sample_description = TextWrapper(width=120).wrap(sample_description)
print(wrapped_sample_description)
['Small and powerful, the Jigger is the best ride for the smallest of tikes! This is the tiniest kids’ pedal bike on the',
 'market available without a coaster brake, the Jigger is the vehicle of choice for the rare tenacious little rider raring',
 'to go. We say rare because this smokin’ little bike is not ideal for a nervous first-time rider, but it’s a true giddy',
 ...]
```

To generate the vector embeddings, we use the `encode` function:

```python
embedding = embedder.encode(sample_description)
```

Let's take a peek at the first 5 elements of the generated vector:

```python
print(embedding.tolist()[:5])
[0.20076484978199005, -0.1300073117017746, 0.3081613779067993, 0.2062796652317047, -0.3692358434200287]
```

Let's look at the length of the vector embeddings generated by the model.

```python
print(len(embedding))
768
```

The chosen model generates vector embeddings of length `768` regardless of the length of the input text.

## Storing our bikes in Redis

### Redis Stack setup

There are many ways to install and run Redis. See [Install Redis Stack](https://redis.io/docs/getting-started/install-stack/) for more information.

Now that we know how to vectorize the bikes descriptions, it's time to start working with Redis.

### Redis Python client

To interact with Redis, we'll install the [redis-py](https://github.com/redis/redis-py) client library, which encapsulates the commands to work with OSS Redis as well as Redis Stack. For an overview of how to use `redis-py`, see the [Redis Python Guide](https://redis.io/docs/clients/python/).

#### Create a `redis-py` client and test the server

We'll instantiate the Redis client, connecting to the localhost on Redis' default port `6379`. By default, Redis returns binary responses; to decode them, we'll pass the `decode_responses` parameter set to `True`:

{{< clients-example search_vss connect />}}

Let's use Redis' [PING](https://redis.io/commands/ping/) command to check that Redis is up and running:

{{< clients-example search_vss connection_test />}}

### Storing the bikes as JSON documents in Redis

Redis Stack includes a [JSON](https://redis.io/docs/stack/json/) data type. Like any other Redis data type, the JSON datatype allows you to use Redis commands to save, update, and retrieve JSON values. Since we already have the bikes data loaded in memory as the `bikes` JSON array. We will iterate over `bikes`, generate a suitable Redis key and store them in Redis using the [JSON.SET](https://redis.io/commands/json.set/) command. We'll do this using a [pipeline](https://redis.io/docs/manual/pipelining/) to minimize the round-trip times:

{{< clients-example search_vss load_data />}}

Let's retrieve a specific value from one of the JSON bikes in Redis using a [JSONPath](https://goessner.net/articles/JsonPath/) expression:

{{< clients-example search_vss get />}}

### Vectorize all of the Bikes descriptions

To vectorize all the descriptions in the database, we will first collect all the Redis keys for the bikes. Then 

{{< clients-example search_vss get_keys />}}

We'll use the keys as a parameter to the [JSON.MGET](https://redis.io/commands/json.mget/) command, along with the JSONPath expression `$.description` to collect the descriptions in a list. We will then pass the list to the `encode` method to get a list of vectorized embeddings:

{{< clients-example search_vss generate_embeddings />}}

Now we can add the vectorized descriptions to the JSON documents in Redis using the `JSON.SET` command to insert a new field in each of the documents under the JSONPath `$.description_embeddings`. Once again we'll do this using a pipeline:

{{< clients-example search_vss load_embeddings />}}

Let's inspect one of the vectorized bike documents using the `JSON.GET` command:

{{< clients-example search_vss dump_example />}}

When storing a vector embedding as part of a JSON datatype, the embedding is stored as a JSON array, in our case, under the field `description_embeddings` as shown. Note: in the example above, the array was shortened considerably for the sake of readability.

### Making the bikes collection searchable

Redis Stack provides a [powerful search engine](https://redis.io/docs/stack/search/) that introduces [commands](https://redis.io/commands/?group=search) to create and maintain search indexes for both collections of HASHES and [JSON](https://redis.io/docs/stack/search/indexing_json/) documents.

To create a search index for the bikes collection, we'll use the [FT.CREATE](https://redis.io/commands/ft.create/) command:

{{< clients-example search_vss create_index >}}
1️⃣ FT.CREATE idx:bikes_vss ON JSON 
2️⃣  PREFIX 1 bikes: SCORE 1.0 
3️⃣  SCHEMA 
4️⃣    $.model TEXT WEIGHT 1.0 NOSTEM 
5️⃣    $.brand TEXT WEIGHT 1.0 NOSTEM 
6️⃣    $.price NUMERIC 
7️⃣    $.type TAG SEPARATOR "," 
8️⃣    $.description AS description TEXT WEIGHT 1.0 
9️⃣    $.description_embeddings AS vector VECTOR FLAT 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE
{{< /clients-example >}}

There is a lot to unpack here, so let's take it from the top:

1. We start by specifying the name of the index; `idx:bikes` indexing keys of type `JSON`.
1. The keys being indexed are found using the `bikes:` key prefix.
1. The `SCHEMA` keyword marks the beginning of the schema field definitions.
1. Declares that field in the JSON document at the JSONPath `$.model` will be indexed as a `TEXT` field, allowing full-text search queries (disabling stemming).
1. The `$.brand` field will also be treated as a `TEXT` schema field.
1. The `$.price` field will be indexed as a `NUMERIC` allowing numeric range queries.
1. The `$.type` field will be indexed as a `TAG` field. Tag fields allow exact-match queries, and are suitable for categorical values.
1. The `$.description` field will also be indexed as a `TEXT` field
1. Finally, the vector embeddings in `$.description_embeddings` are indexed as a `VECTOR` field and assigned to the alias `vector`. 
  
Let's break down the `VECTOR` schema field definition to better understand the inner workings of vector similarity in Redis:

* `FLAT`: Specifies the indexing method, which can be `FLAT` or `HNSW`. FLAT (brute-force indexing) provides exact results but at a higher computational cost, while HNSW (Hierarchical Navigable Small World) is a more efficient method that provides approximate results with lower computational overhead.
* `TYPE`: Set to `FLOAT32`. Current supported types are `FLOAT32` and `FLOAT64`.
* `DIM`: The length or dimension of our embeddings, which we determined previosly to be `768`.
* `DISTANCE_METRIC`: One of `L2`, `IP`, `COSINE`. 
  - `L2` stands for Euclidean distance, a straight-line distance between the vectors. Preferred when the absolute differences, including magnitude, matter most.
  - `IP` stands for inner product; `IP` measures the projection of one vector onto another. It emphasizes the angle between vectors rather than their absolute positions in the vector space.
  - `COSINE` stands for cosine similarity; a normalized form of inner product. This metric measures only the angle between two vectors, making it magnitude-independent.  
  - For our querying purposes, the direction of the vectors carry more meaning (indicating semantic similarity), and the magnitude is largely influenced by the length of the documents, therefore `COSINE` similarity is chosen. Also, our chosen embedding model is fine-tuned for `COSINE` similarity.

#### Check the state of the index

After the `FT.CREATE` command creates the index, the indexing process is automatically started in the background. In a short amount of time, our 11 JSON documents should be indexed and ready to be searched. To validate that, we use the [FT.INFO](https://redis.io/commands/ft.info/) command to check some information and statistics of the index. Of particular interest are the number of documents successfully indexed and the number of failures:  

{{< clients-example search_vss validate_index >}}
FT_INFO idx:bikes_vss
{{< /clients-example >}}

### Structured data searches with Redis

The index `idx:bikes_vss` indexes the structured fields of our JSON documents `model`, `brand`, `price`, and `type`. It also indexes the unstructured free-form text `description` and the generated embeddings in `description_embeddings`. Before we dive deeper into Vector Similarity Search (VSS), we need to understand the basics of querying a Redis index. The Redis command of interest is [FT.SEARCH](https://redis.io/commands/ft.search/). Like a SQL `select` statement, an `FT.SEARCH` statement can be as simple or as complex as needed. 

Let's try a few simple queries that give enough context to complete our VSS examples. For example, to retrieve all bikes where the `brand` is `Peaknetic`, we can use the following command:

{{< clients-example search_vss simple_query_1 >}}
FT.SEARCH idx:bikes_vss '@brand:Peaknetic'
{{< /clients-example >}}

This command will return all matching documents. With the inclusion of the vector embeddings, that's a little too verbose. If we wanted only to return specific fields from our JSON documents, for example, the document `id`, the `brand`, `model` and `price`, we could use:

{{< clients-example search_vss simple_query_2 >}}
FT.SEARCH idx:bikes_vss '@brand:Peaknetic' RETURN 4 id, brand, model, price
{{< /clients-example >}}

In this query, we are searching against a schema field of type `TEXT`.

Let's say we only wanted bikes under $1000. We can add a numeric range clause to our query since the `price` field is indexed as `NUMERIC`:

{{< clients-example search_vss simple_query_3 />}}

### Semantic searching with VSS

Now that the bikes collection is stored and properly indexed in Redis, we want to query it using short query prompts. Let's put our queries in a list so we can execute them in bulk:

{{< clients-example search_vss def_bulk_queries />}}

We need to encode the query prompts to query the database using VSS. Just like we did with the descriptions of the bikes, we'll use the SentenceTransformers model to encode the queries:

{{< clients-example search_vss enc_bulk_queries />}}

#### Constructing a pure K-nearest neighbors (KNN) VSS query

We'll start with a KNN query. KNN is a foundational algorithm used in VSS, where the goal is to find the most similar items to a given query item. Using the chosen distance metric, the KNN algorithm calculates the distance between the query vector and each vector in the database. It then returns the K items with the smallest distances to the query vector. These are the most similar items.

The syntax for vector similarity KNN queries is `(*)=>[<vector_similarity_query>]` where the `(*)` (the `*` meaning all) is the filter query for the search engine. That way, one can reduce the search space by filtering the collection on which the KNN algorithm operates. 

* The `$query_vector` represents the query parameter we'll use to pass the vectorized query prompt.
* The results will be filtered by `vector_score`, which is a field derived from the name of the field indexed as a vector by appending `_score` to it, in our case, `vector` (the alias for `$.description_embeddings`). 
* Our query will return the `vector_score`, the `id`s of the matched documents, and the `$.brand`, `$.model`, and `$.description`. 
* Finally, to utilize a vector similarity query with the `FT.SEARCH` command, we must specify DIALECT 2 or greater.

```python
query = (
    Query('(*)=>[KNN 3 @vector $query_vector AS vector_score]')
     .sort_by('vector_score')
     .return_fields('vector_score', 'id', 'brand', 'model', 'description')
     .dialect(2)
)
```

We pass the vectorized query as `$query_vector` to the search function to execute the query. The following code shows an example of creating a NumPy array from a vectorized query prompt (`encoded_query`) as a single precision floating point array and converting it into a compact, byte-level representation that we can pass as a Redis parameter:

```python
client.ft(INDEX_NAME).search(query, { 'query_vector': np.array(encoded_query, dtype=np.float32).tobytes() }).docs
```

With the template for the query in place, we can use Python to execute all query prompts in a loop, passing the vectorized query prompts. Notice that for each result we calculate the `vector_score` as `1 - doc.vector_score`, since we use cosine "distance" as the metric, the items with the smallest "distance" are closer and therefore more similar to our query. 

We will then loop over the matched documents and create a list of results we can convert into a Pandas table to visualize the results:

{{< clients-example search_vss define_bulk_query />}}

The query results show the individual queries' top 3 matches (our K parameter) along with the bike's id, brand, and model for each query. For example, for the query "Best Mountain bikes for kids", the highest similarity score (`0.54`) and therefore the closest match was the 'Nord' brand 'Chook air 5' bike model, described as:

> "The Chook Air 5 gives kids aged six years and older a durable and uberlight mountain bike for their first experience on tracks and easy cruising through forests and fields. The lower top tube makes it easy to mount and dismount in any situation, giving your kids greater safety on the trails. The Chook Air 5 is the perfect intro to mountain biking."

From the description, we gather that this bike is an excellent match for younger children, and the MS MARCO model-generated embeddings seem to have captured the semantics of the description accurately.

{{< clients-example search_vss run_knn_query />}}

| query | score | id | brand | model | description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Best Mountain bikes for kids | 0.54 | bikes:003 | Nord | Chook air 5 | The Chook Air 5  gives kids aged six years and older a durable and uberlight mountain bike for their first experience on tracks and easy cruising through forests and fields. The lower  top tube makes it easy to mount and dismount in any situation, giving your kids greater safety on the trails. The Chook Air 5 is the perfect intro to mountain biking. |
|  | 0.51 | bikes:010 | nHill | Summit | This budget mountain bike from nHill performs well both on bike paths and on the trail. The fork with 100mm of travel absorbs rough terrain. Fat Kenda Booster tires give you grip in corners and on wet trails. The Shimano Tourney drivetrain offered enough gears for finding a comfortable pace to ride uphill, and the Tektro hydraulic disc brakes break smoothly. Whether you want an affordable bike that you can take to work, but also take trail riding on the weekends or you’re just after a stable,... |
|  | 0.46 | bikes:001 | Velorim | Jigger | Small and powerful, the Jigger is the best ride for the smallest of tikes! This is the tiniest kids’ pedal bike on the market available without a coaster brake, the Jigger is the vehicle of choice for the rare tenacious little rider raring to go. We say rare because this smokin’ little bike is not ideal for a nervous first-time rider, but it’s a true giddy up for a true speedster. The Jigger is a 12 inch lightweight kids bicycle and it will meet your little one’s need for speed. It’s a single... |
`...`

Sometimes, a picture is worth a thousand words. Using the dimensionality reduction technique [t-SNE](https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding) we can create a 3-d representation of our description embeddings, as well as the query embeddings which shows how well the MS MARCO sentence embeddings clustered our bicycle descriptions and we can visually judge a query's distance to a specific bike:

![t-SNE 3-D Embeddings Visualization](https://raw.githubusercontent.com/bsbodden/redis_vss_getting_started/3ac967dfbdd84dad25ade620826c0e01ac0251ca/embeddings-tsne.png)

#### Hybrid queries

Pure KNN queries, as described in the previous section, evaluate a query against the whole space of vectors in a data collection. The larger the collection, the more computationally expensive the KNN search will be. But in the real world, unstructured data does not live in isolation, and users expecting rich search experiences need to be able to search via a combination of structured and unstructured data. 

For example, users might arrive at your search interface with a brand preference in mind for the bikes dataset. Redis VSS queries can use this information to pre-filter the search space using a **primary filter query**. In the following query definition, we pre-filter using `brand` to consider only `Peaknetic` brand bikes. Before, our primary filter query was `(*)`, in other words, everything. But now we can narrow the search space using `(@brand:Peaknetic)` before the KNN query.

Filtering by the `Peaknetic` brand, for which there are 2 bikes in our collection, we can see the results returned for each of the query prompts. The query with the highest returned similarity score is "Comfortable commuter bike", followed by "Road bike for beginners". Using filtering by brand, we fulfill the users' preferences and reduce the KNN search space by 80%.

{{< clients-example search_vss run_hybrid_query />}}

| query | score | id | brand | model | description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Best Mountain bikes for kids | 0.30 | bikes:008 | Peaknetic | Soothe Electric bike | The Soothe is an everyday electric bike, from the makers of Exercycle  bikes, that conveys style while you get around the city. The Soothe lives up to its name by keeping your posture upright and relaxed for the ride ahead, keeping those aches and pains from riding at bay. It includes a low-step frame , our memory foam seat, bump-resistant shocks and conveniently placed thumb throttle. |
|  | 0.23 | bikes:009 | Peaknetic | Secto | If you struggle with stiff fingers or a kinked neck or back after a few minutes on the road, this lightweight, aluminum bike alleviates those issues and allows you to enjoy the ride. From the ergonomic grips to the lumbar-supporting seat position, the Roll Low-Entry offers incredible comfort. The rear-inclined seat tube facilitates stability by allowing you to put a foot on the ground to balance at a stop, and the low step-over frame makes it accessible for all ability and mobility levels. Th... |
| Bike for small kids | 0.37 | bikes:008 | Peaknetic | Soothe Electric bike | The Soothe is an everyday electric bike, from the makers of Exercycle  bikes, that conveys style while you get around the city. The Soothe lives up to its name by keeping your posture upright and relaxed for the ride ahead, keeping those aches and pains from riding at bay. It includes a low-step frame , our memory foam seat, bump-resistant shocks and conveniently placed thumb throttle. |
`...`

#### Creating a VSS range query 

Range queries in VSS involve retrieving items within a specific distance from a query vector. In this case, we consider "distance" to be the measure of similarity we've used to build our search indexes; the smaller the distance, the more similar the items.

Let's say you want to find the bikes whose descriptions are within a certain distance from a query vector. We can use a range query to achieve this.
For example, the query command to return the top `4` documents within a `0.55` radius of a vectorized query would be as follows: 

{{< clients-example search_vss run_range_query >}}
1️⃣ FT.SEARCH idx:bikes_vss 
2️⃣   @vector:[VECTOR_RANGE $range $query_vector]=>{$YIELD_DISTANCE_AS: vector_score} 
3️⃣   SORTBY vector_score ASC
4️⃣   LIMIT 0 4 
5️⃣   DIALECT 2 
6️⃣   PARAMS 4 range 0.55 query_vector "\x9d|\x99>bV#\xbfm\x86\x8a\xbd\xa7~$?*...."
{{< /clients-example >}}

Where:

1. We use the `FT.SEARCH` command with our `idx:bikes_vss`.
1. and filter by the `vector` using the `VECTOR_RANGE` operator pasing the `$range` parameter, yield the vector distance between the vector field and the query result in a field named `vector_score`.
1. We sort the results by the yielded `vector_score`.
1. Limit the results to at most 4.
1. Once again, we set the RediSearch dialect to `2` to enable VSS functionality.
1. Finally we set the parameter values, `range` (`$range`) to `0.55` and the `query_vector` (`$query_vector`) to the encoded vectorized query.

Here, we're using the first query prompt in our collection of queries, "Bike for small kids", using the VSS range query (`range_query`).

| query | score | id | brand | model | description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Bike for small kids | 0.52 | bikes:001 | Velorim | Jigger | Small and powerful, the Jigger is the best ride for the smallest of tikes! This is the tiniest kids’ pedal bike on the market available without a coaster brake, the Jigger is the vehicle of choice for the rare tenacious little rider raring to go. We say rare because this smokin’ little bike is not ideal for a nervous first-time rider, but it’s a true giddy up for a true speedster. The Jigger is a 12 inch lightweight kids bicycle and it will meet your little one’s need for speed. It’s a single... |
|  | 0.45 | bikes:007 | ScramBikes | WattBike | The WattBike is the best e-bike for people who still feel young at heart. It has a  Bafang 500 watt geared hub motor that can reach 20 miles per hour on both steep inclines and city streets. The lithium-ion battery, which gets nearly 40 miles per charge, has a lightweight form factor, making it easier for seniors to use. It comes fully assembled (no convoluted instructions!) and includes a sturdy helmet at no cost. The Plush saddle softens over time with use. The included Seatpost, however, i... |

The query returns two bikes in the specified range of our vectorized query, both with scores at or below `0.55`.

## Wrapping Up

In this guide, we learned how Redis, using the Redis Stack distribution, provides powerful search capabilities over structured and unstructured data. Redis support for vector data can enrich and enhance the user's search experience.
Although we focused on generating embeddings for unstructured data, the vector similarity approach can equally be employed with structure data, as long as a suitable vector generation technique is used.

The references below can help you learn more about Redis search capabilities:
* https://redis.io/docs/interact/search-and-query/
* https://redis.io/docs/interact/search-and-query/indexing/

The Jupyter notebook on which this documentation is based is available here:
* https://github.com/RedisVentures/redis-vss-getting-started