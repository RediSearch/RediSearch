# Package redisearch Documentation

## Overview

`redisearch-py` is a python search engine library that utilizes the RediSearch Redis Module API.

It is the "official" client of RediSearch, and should be regarded as its canonical client implementation.

The source code can be found at [http://github.com/RedisLabs/redisearch-py](http://github.com/RedisLabs/redisearch-py)

### Example: Using the Python Client

```py

from redisearch import Client, TextField, NumericField, Query

# Creating a client with a given index name
client = Client('myIndex')

# Creating the index definition and schema
client.create_index([TextField('title', weight=5.0), TextField('body')])

# Indexing a document
client.add_document('doc1', title = 'RediSearch', body = 'Redisearch implements a search engine on top of redis')

# Simple search
res = client.search("search engine")

# the result has the total number of results, and a list of documents
print res.total # "1"
print res.docs[0].title

# Searching with snippets
res = client.search("search engine", snippet_sizes = {'body': 50})

# Searching with complex parameters:
q = Query("search engine").verbatim().no_content().paging(0,5)
res = client.search(q)

```

### Example: Using the Auto Completer Client:

```py

# Using the auto-completer
ac = AutoCompleter('ac')

# Adding some terms
ac.add_suggestions(Suggestion('foo', 5.0), Suggestion('bar', 1.0))

# Getting suggestions
suggs = ac.get_suggestions('goo') # returns nothing

suggs = ac.get_suggestions('goo', fuzzy = True) # returns ['foo']

```

### Installing

1. Install Redis 4.0 or above

2. [Install RediSearch](https://oss.redislabs.com/redisearch/Quick_Start/#building-and-running)

3. Install the python client

```sh
$ pip install redisearch
```

## Class AutoCompleter
A client to RediSearch's AutoCompleter API

It provides prefix searches with optionally fuzzy matching of prefixes    
### \_\_init\_\_
```py

def __init__(self, key, host='localhost', port=6379, conn=None)

```



Create a new AutoCompleter client for the given key, and optional host and port

If conn is not None, we employ an already existing redis connection


### add\_suggestions
```py

def add_suggestions(self, *suggestions, **kwargs)

```



Add suggestion terms to the AutoCompleter engine. Each suggestion has a score and string.

If kwargs['increment'] is true and the terms are already in the server's dictionary, we increment their scores


### delete
```py

def delete(self, string)

```



Delete a string from the AutoCompleter index.
Returns 1 if the string was found and deleted, 0 otherwise


### get\_suggestions
```py

def get_suggestions(self, prefix, fuzzy=False, num=10, with_scores=False, with_payloads=False)

```



Get a list of suggestions from the AutoCompleter, for a given prefix

### Parameters:
- **prefix**: the prefix we are searching. **Must be valid ascii or utf-8**
- **fuzzy**: If set to true, the prefix search is done in fuzzy mode.
    **NOTE**: Running fuzzy searches on short (<3 letters) prefixes can be very slow, and even scan the entire index.
- **with_scores**: if set to true, we also return the (refactored) score of each suggestion.
  This is normally not needed, and is NOT the original score inserted into the index
- **with_payloads**: Return suggestion payloads
- **num**: The maximum number of results we return. Note that we might return less. The algorithm trims irrelevant suggestions.

Returns a list of Suggestion objects. If with_scores was False, the score of all suggestions is 1.


### len
```py

def len(self)

```



Return the number of entries in the AutoCompleter index




## Class Client
A client for the RediSearch module.
It abstracts the API of the module and lets you just use the engine
### \_\_init\_\_
```py

def __init__(self, index_name, host='localhost', port=6379, conn=None)

```



Create a new Client for the given index_name, and optional host and port

If conn is not None, we employ an already existing redis connection


### add\_document
```py

def add_document(self, doc_id, nosave=False, score=1.0, payload=None, replace=False, partial=False, **fields)

```



Add a single document to the index.

### Parameters

- **doc_id**: the id of the saved document.
- **nosave**: if set to true, we just index the document, and don't save a copy of it. This means that searches will just return ids.
- **score**: the document ranking, between 0.0 and 1.0
- **payload**: optional inner-index payload we can save for fast access in scoring functions
- **replace**: if True, and the document already is in the index, we perform an update and reindex the document
- **partial**: if True, the fields specified will be added to the existing document.
               This has the added benefit that any fields specified with `no_index`
               will not be reindexed again. Implies `replace`
- **fields** kwargs dictionary of the document fields to be saved and/or indexed.
             NOTE: Geo points should be encoded as strings of "lon,lat"


### batch\_indexer
```py

def batch_indexer(self, chunk_size=100)

```



Create a new batch indexer from the client with a given chunk size


### create\_index
```py

def create_index(self, fields, no_term_offsets=False, no_field_flags=False, stopwords=None)

```



Create the search index. Creating an existing index just updates its properties

### Parameters:

- **fields**: a list of TextField or NumericField objects
- **no_term_offsets**: If true, we will not save term offsets in the index
- **no_field_flags**: If true, we will not save field flags that allow searching in specific fields
- **stopwords**: If not None, we create the index with this custom stopword list. The list can be empty


### delete\_document
```py

def delete_document(self, doc_id, conn=None)

```



Delete a document from index
Returns 1 if the document was deleted, 0 if not


### drop\_index
```py

def drop_index(self)

```



Drop the index if it exists


### explain
```py

def explain(self, query)

```



### info
```py

def info(self)

```



Get info an stats about the the current index, including the number of documents, memory consumption, etc


### load\_document
```py

def load_document(self, id)

```



Load a single document by id


### search
```py

def search(self, query)

```



Search the index for a given query, and return a result of documents

### Parameters

- **query**: the search query. Either a text for simple queries with default parameters, or a Query object for complex queries.
             See RediSearch's documentation on query format
- **snippet_sizes**: A dictionary of {field: snippet_size} used to trim and format the result. e.g.e {'body': 500}


## Class BatchIndexer
A batch indexer allows you to automatically batch
document indexing in pipelines, flushing it every N documents.
### \_\_init\_\_
```py

def __init__(self, client, chunk_size=1000)

```



### add\_document
```py

def add_document(self, doc_id, nosave=False, score=1.0, payload=None, replace=False, partial=False, **fields)

```



Add a document to the batch query


### commit
```py

def commit(self)

```



Manually commit and flush the batch indexing query






## Class Document
Represents a single document in a result set
### \_\_init\_\_
```py

def __init__(self, id, payload=None, **fields)

```





## Class GeoField
GeoField is used to define a geo-indexing field in a schema definition
### \_\_init\_\_
```py

def __init__(self, name)

```



### redis\_args
```py

def redis_args(self)

```





## Class GeoFilter
None
### \_\_init\_\_
```py

def __init__(self, field, lon, lat, radius, unit='km')

```





## Class NumericField
NumericField is used to define a numeric field in a schema definition
### \_\_init\_\_
```py

def __init__(self, name, sortable=False, no_index=False)

```



### redis\_args
```py

def redis_args(self)

```





## Class NumericFilter
None
### \_\_init\_\_
```py

def __init__(self, field, minval, maxval, minExclusive=False, maxExclusive=False)

```





## Class Query
Query is used to build complex queries that have more parameters than just the query string.
The query string is set in the constructor, and other options have setter functions.

The setter functions return the query object, so they can be chained,
i.e. `Query("foo").verbatim().filter(...)` etc.
### \_\_init\_\_
```py

def __init__(self, query_string)

```



Create a new query object.
The query string is set in the constructor, and other options have setter functions.


### add\_filter
```py

def add_filter(self, flt)

```



Add a numeric or geo filter to the query.
**Currently only one of each filter is supported by the engine**

- **flt**: A NumericFilter or GeoFilter object, used on a corresponding field


### get\_args
```py

def get_args(self)

```



Format the redis arguments for this query and return them


### highlight
```py

def highlight(self, fields=None, tags=None)

```



Apply specified markup to matched term(s) within the returned field(s)

- **fields** If specified then only those mentioned fields are highlighted, otherwise all fields are highlighted
- **tags** A list of two strings to surround the match.


### in\_order
```py

def in_order(self)

```



Match only documents where the query terms appear in the same order in the document.
i.e. for the query 'hello world', we do not match 'world hello'


### limit\_fields
```py

def limit_fields(self, *fields)

```



Limit the search to specific TEXT fields only

- **fields**: A list of strings, case sensitive field names from the defined schema


### limit\_ids
```py

def limit_ids(self, *ids)

```



Limit the results to a specific set of pre-known document ids of any length


### no\_content
```py

def no_content(self)

```



Set the query to only return ids and not the document content


### no\_stopwords
```py

def no_stopwords(self)

```



Prevent the query from being filtered for stopwords.
Only useful in very big queries that you are certain contain no stopwords.


### paging
```py

def paging(self, offset, num)

```



Set the paging for the query (defaults to 0..10).

- **offset**: Paging offset for the results. Defaults to 0
- **num**: How many results do we want


### query\_string
```py

def query_string(self)

```



Return the query string of this query only


### return\_fields
```py

def return_fields(self, *fields)

```



Only return values from these fields


### slop
```py

def slop(self, slop)

```



Allow a maximum of N intervening non matched terms between phrase terms (0 means exact phrase)


### sort\_by
```py

def sort_by(self, field, asc=True)

```



Add a sortby field to the query

- **field** - the name of the field to sort by
- **asc** - when `True`, sorting will be done in ascending order


### summarize
```py

def summarize(self, fields=None, context_len=None, num_frags=None, sep=None)

```



Return an abridged format of the field, containing only the segments of
the field which contain the matching term(s).

If `fields` is specified, then only the mentioned fields are
summarized; otherwise all results are summarized.

Server side defaults are used for each option (except `fields`) if not specified

- **fields** List of fields to summarize. All fields are summarized if not specified
- **context_len** Amount of context to include with each fragment
- **num_frags** Number of fragments per document
- **sep** Separator string to separate fragments


### verbatim
```py

def verbatim(self)

```



Set the query to be verbatim, i.e. use no query expansion or stemming


### with\_payloads
```py

def with_payloads(self)

```



Ask the engine to return document payloads




## Class Result
Represents the result of a search query, and has an array of Document objects
### \_\_init\_\_
```py

def __init__(self, res, hascontent, duration=0, has_payload=False)

```



- **snippets**: An optional dictionary of the form {field: snippet_size} for snippet formatting




## Class SortbyField
None
### \_\_init\_\_
```py

def __init__(self, field, asc=True)

```





## Class Suggestion
Represents a single suggestion being sent or returned from the auto complete server
### \_\_init\_\_
```py

def __init__(self, string, score=1.0, payload=None)

```





## Class TagField
TagField is a tag-indexing field with simpler compression and tokenization.
See https://oss.redislabs.com/redisearch/Tags/
### \_\_init\_\_
```py

def __init__(self, name, separator=',', no_index=False)

```



### redis\_args
```py

def redis_args(self)

```





## Class TextField
TextField is used to define a text field in a schema definition
### \_\_init\_\_
```py

def __init__(self, name, weight=1.0, sortable=False, no_stem=False, no_index=False)

```



### redis\_args
```py

def redis_args(self)

```
