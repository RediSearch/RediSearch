# JRediSearch - RediSearch Java Client

[https://github.com/RedisLabs/JRediSearch](https://github.com/RedisLabs/JRediSearch)

## Overview 

JRediSearch is a Java library abstracting the API of the RediSearch Redis module, that implements a powerful in-memory search engine inside Redis. 
 
See full documentation at [https://github.com/RedisLabs/JRediSearch](https://github.com/RedisLabs/JRediSearch).

## Usage example

Initializing the client:

```java

import io.redisearch.client.Client;
import io.redisearch.Document;
import io.redisearch.SearchResult;
import io.redisearch.Query;
import io.redisearch.Schema;

...

Client client = new Client("testung", "localhost", 6379);

```

Defining a schema for an index and creating it:

```java

Schema sc = new Schema()
                .addTextField("title", 5.0)
                .addTextField("body", 1.0)
                .addNumericField("price");

client.createIndex(sc, Client.IndexOptions.Default());

```
 
Adding documents to the index:

```java
Map<String, Object> fields = new HashMap<>();
fields.put("title", "hello world");
fields.put("body", "lorem ipsum");
fields.put("price", 1337);

client.addDocument("doc1", fields);

```

Searching the index:

```java

// Creating a complex query
Query q = new Query("hello world")
                    .addFilter(new Query.NumericFilter("price", 0, 1000))
                    .limit(0,5);

// actual search
SearchResult res = client.search(q);


```

---
 
