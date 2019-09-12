# Extending RediSearch

RediSearch supports an extension mechanism, much like Redis supports modules. The API is very minimal at the moment, and it does not yet support dynamic loading of extensions in run-time. Instead, extensions must be written in C (or a language that has an interface with C) and compiled into dynamic libraries that will be loaded at run-time.

There are two kinds of extension APIs at the moment: 

1. **Query Expanders**, whose role is to expand query tokens (i.e. stemmers).
2. **Scoring Functions**, whose role is to rank search results in query time.

## Registering and loading extensions

Extensions should be compiled into .so files, and loaded into RediSearch on initialization of the module. 

* Compiling 

    Extensions should be compiled and linked as dynamic libraries. An example Makefile for an extension [can be found here](https://github.com/RediSearch/RediSearch/blob/master/src/tests/ext-example/Makefile). 

    That folder also contains an example extension that is used for testing and can be taken as a skeleton for implementing your own extension.

* Loading 

    Loading an extension is done by apending `EXTLOAD {path/to/ext.so}` after the `loadmodule` configuration directive when loading RediSearch. For example:


    ```sh
    $ redis-server --loadmodule ./redisearch.so EXTLOAD ./ext/my_extension.so
    ```

    This causes RediSearch to automatically load the extension and register its expanders and scorers. 


## Initializing an extension

The entry point of an extension is a function with the signature:

```c
int RS_ExtensionInit(RSExtensionCtx *ctx);
```

When loading the extension, RediSearch looks for this function and calls it. This function is responsible for registering and initializing the expanders and scorers. 

It should return REDISEARCH_ERR on error or REDISEARCH_OK on success.

### Example init function

```c

#include <redisearch.h> //must be in the include path

int RS_ExtensionInit(RSExtensionCtx *ctx) {

  /* Register  a scoring function with an alias my_scorer and no special private data and free function */
  if (ctx->RegisterScoringFunction("my_scorer", MyCustomScorer, NULL, NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Register a query expander  */
  if (ctx->RegisterQueryExpander("my_expander", MyExpander, NULL, NULL) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}
```

## Calling your custom functions

When performing a query, you can tell RediSearch to use your scorers or expanders by specifying the SCORER or EXPANDER arguments, with the given alias.
e.g.:

```
FT.SEARCH my_index "foo bar" EXPANDER my_expander SCORER my_scorer
```

**NOTE**: Expander and scorer aliases are **case sensitive**.

## The query expander API

At the moment, we only support basic query expansion, one token at a time. An expander can decide to expand any given token with as many tokens it wishes, that will be Union-merged in query time.

The API for an expander is the following:

```c
#include <redisearch.h> //must be in the include path

void MyQueryExpander(RSQueryExpanderCtx *ctx, RSToken *token) {
    ...
}
```

### RSQueryExpanderCtx

RSQueryExpanderCtx is a context that contains private data of the extension, and a callback method to expand the query. It is defined as:

```c
typedef struct RSQueryExpanderCtx {

  /* Opaque query object used internally by the engine, and should not be accessed */
  struct RSQuery *query;

  /* Opaque query node object used internally by the engine, and should not be accessed */
  struct RSQueryNode **currentNode;

  /* Private data of the extension, set on extension initialization */
  void *privdata;

  /* The language of the query, defaults to "english" */
  const char *language;

  /* ExpandToken allows the user to add an expansion of the token in the query, that will be
   * union-merged with the given token in query time. str is the expanded string, len is its length,
   * and flags is a 32 bit flag mask that can be used by the extension to set private information on
   * the token */
  void (*ExpandToken)(struct RSQueryExpanderCtx *ctx, const char *str, size_t len,
                      RSTokenFlags flags);

  /* SetPayload allows the query expander to set GLOBAL payload on the query (not unique per token)
   */
  void (*SetPayload)(struct RSQueryExpanderCtx *ctx, RSPayload payload);

} RSQueryExpanderCtx;
```

### RSToken

RSToken represents a single query token to be expanded and is defined as:


```c
/* A token in the query. The expanders receive query tokens and can expand the query with more query
 * tokens */
typedef struct {
  /* The token string - which may or may not be NULL terminated */
  const char *str;
  /* The token length */
  size_t len;
  
  /* 1 if the token is the result of query expansion */
  uint8_t expanded:1;

  /* Extension specific token flags that can be examined later by the scoring function */
  RSTokenFlags flags;
} RSToken;

```

## The scoring function API

A scoring function receives each document being evaluated by the query, for final ranking. 
It has access to all the query terms that brought up the document,and to metadata about the
document such as its a-priory score, length, etc.

Since the scoring function is evaluated per each document, potentially millions of times, and since
redis is single threaded - it is important that it works as fast as possible and be heavily optimized. 

A scoring function is applied to each potential result (per document) and is implemented with the following signature:

```c
double MyScoringFunction(RSScoringFunctionCtx *ctx, RSIndexResult *res,
                                    RSDocumentMetadata *dmd, double minScore);
```

RSScoringFunctionCtx is a context that implements some helper methods. 

RSIndexResult is the result information - containing the document id, frequency, terms, and offsets. 

RSDocumentMetadata is an object holding global information about the document, such as its a-priory score. 

minSocre is the minimal score that will yield a result that will be relevant to the search. It can be used to stop processing mid-way of before we even start.

The return value of the function is double representing the final score of the result. 
Returning 0 causes the result to be counted, but if there are results with a score greater than 0, they will appear above it. 
To completely filter out a result and not count it in the totals, the scorer should return the special value `RS_SCORE_FILTEROUT` (which is internally set to negative infinity, or -1/0). 

### RSScoringFunctionCtx

This is an object containing the following members:

* **void *privdata**: a pointer to an object set by the extension on initialization time.
* **RSPayload payload**: A Payload object set either by the query expander or the client.
* **int GetSlop(RSIndexResult *res)**: A callback method that yields the total minimal distance between the query terms. This can be used to prefer results where the "slop" is smaller and the terms are nearer to each other.

### RSIndexResult

This is an object holding the information about the current result in the index, which is an aggregate of all the terms that resulted in the current document being considered a valid result.

See redisearch.h for details

### RSDocumentMetadata

This is an object describing global information, unrelated to the current query, about the document being evaluated by the scoring function. 


## Example query expander

This example query expander expands each token with the term foo:

```c
#include <redisearch.h> //must be in the include path

void DummyExpander(RSQueryExpanderCtx *ctx, RSToken *token) {
    ctx->ExpandToken(ctx, strdup("foo"), strlen("foo"), 0x1337);  
}
```

## Example scoring function

This is an actual scoring function, calculating TF-IDF for the document, multiplying that by the document score, and dividing that by the slop:

```c
#include <redisearch.h> //must be in the include path

double TFIDFScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                   double minScore) {
  // no need to evaluate documents with score 0 
  if (dmd->score == 0) return 0;

  // calculate sum(tf-idf) for each term in the result
  double tfidf = 0;
  for (int i = 0; i < h->numRecords; i++) {
    // take the term frequency and multiply by the term IDF, add that to the total
    tfidf += (float)h->records[i].freq * (h->records[i].term ? h->records[i].term->idf : 0);
  }
  // normalize by the maximal frequency of any term in the document   
  tfidf /=  (double)dmd->maxFreq;

  // multiply by the document score (between 0 and 1)
  tfidf *= dmd->score;

  // no need to factor the slop if tfidf is already below minimal score
  if (tfidf < minScore) {
    return 0;
  }

  // get the slop and divide the result by it, making sure we prefer results with closer terms
  tfidf /= (double)ctx->GetSlop(h);
  
  return tfidf;
}
```
