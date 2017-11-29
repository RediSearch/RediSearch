# Document Payloads

Usually, RediSearch stores documents as HASH keys. But if you want to access some data for 
aggregation or scoring functions, you might want to store that data as an inline payload. 
This will allow you to evaluate properties of a document for scoring purposes, at a very low cost.

Since the scoring functions already have access to DocumentMetaData, which contains the document's flags and score,
you can add custom payloads that can be evaluated at runtime.

Payloads are NOT indexed and are not treated by the engine in any way; they are simply there for the purpose 
of evaluating them in query time, and optionally retrieving them. They can be JSON objects, strings, or preferably, 
if you are interested in fast evaluation, some sort of binary-encoded data (which is fast to decode).

## Adding Payloads for Documents

When inserting a document using FT.ADD, you can ask RediSearch to store an arbitrary binary-safe string as the document payload--
just use the PAYLOAD keyword:

```
FT.ADD {index_name} {doc_id} {score} PAYLOAD {payload} FIELDS {field} {data}...
```

## Evaluating Payloads in Query Time

When implementing a scoring function, the signature of the function exposed is:

```c
double (*ScoringFunction)(DocumentMetadata *dmd, IndexResult *h);
```

> NOTE: currently, scoring functions cannot be dynamically added; forking the engine and replacing them is required.

DocumentMetaData includes a few fields, one of which is the payload. It wraps a simple byte array with
arbitrary length:

```c
typedef struct  {
    char *data,
    uint32_t len;
} DocumentPayload;
```

If no payload was set to the document, it is simply NULL. If it is not NULL, you can go ahead and decode it.
We recommend that you encode some metadata about the payload inside it, such as a leading version number, etc.

## Retrieving Payloads from Documents

When searching, it is possible to request the document payloads from the engine. 

This is done by adding the keyword `WITHPAYLOADS` to `FT.SEARCH`. 

If `WITHPAYLOADS` is set, the payloads follow the document ID in the returned result. 
If `WITHSCORES` is set as well, the payloads follow the scores, e.g.:

```
127.0.0.1:6379> FT.CREATE foo SCHEMA bar TEXT
OK
127.0.0.1:6379> FT.ADD foo doc2 1.0 PAYLOAD "hi there!" FIELDS bar "hello"
OK
127.0.0.1:6379> FT.SEARCH foo "hello" WITHPAYLOADS WITHSCORES
1) (integer) 1
2) "doc2"           # id
3) "1"              # score
4) "hi there!"      # payload
5) 1) "bar"         # fields
   2) "hello"
```
