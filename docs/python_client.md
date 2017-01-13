# Package redisearch Documentation



## Overview 

`redisearch-py` is a python search engine library that utilizes the RediSearch Redis Module API.

It is the "official" client of redisearch, and should be regarded as its canonical client implementation.

The source code can be found at [http://github.com/RedisLabs/redisearch-py](http://github.com/RedisLabs/redisearch-py)

### Example: Using the Python Client

```py

from redisearch import Client, TextField, NumericField

# Creating a client with a given index name
client = Client('myIndex')

# Creating the index definition and schema
client.create_index([TextField('title', weight=5.0), TextField('body')])

# Indexing a document
client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

# Searching
res = client.search("search engine")

# the result has the total number of results, and a list of documents
print res.total # "1"
print res.docs[0].title 

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

1. Install redis 4.0 RC2 or above

2. [Install RediSearch](http://redisearch.io/Quick_Start/#building-and-running)

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

def get_suggestions(self, prefix, fuzzy=False, num=10, with_scores=False)

```



Get a list of suggestions from the AutoCompleter, for a given prefix

### Parameters:
- **prefix**: the prefix we are searching. **Must be valid ascii or utf-8**
- **fuzzy**: If set to true, the prefix search is done in fuzzy mode. 
    **NOTE**: Running fuzzy searches on short (<3 letters) prefixes can be very slow, and even scan the entire index.
- **with_scores**: if set to true, we also return the (refactored) score of each suggestion. 
  This is normally not needed, and is NOT the original score inserted into the index
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

def add_document(self, doc_id, nosave=False, score=1.0, **fields)

```



Add a single document to the index.

### Parameters

- **doc_id**: the id of the saved document.
- **nosave**: if set to true, we just index the document, and don't save a copy of it. This means that searches will just return ids.
- **score**: the document ranking, between 0.0 and 1.0 
- **fields** kwargs dictionary of the document fields to be saved and/or indexed


### batch\_indexer
```py

def batch_indexer(self, chunk_size=100)

```



Create a new batch indexer from the client with a given chunk size


### create\_index
```py

def create_index(self, fields, no_term_offsets=False, no_field_flags=False, no_score_indexes=False)

```



Create the search index. Creating an existing index juts updates its properties

### Parameters:

- **fields**: a list of TextField or NumericField objects
- **no_term_offsets**: If true, we will not save term offsets in the index
- **no_field_flags**: If true, we will not save field flags that allow searching in specific fields
- **no_score_indexes**: If true, we will not save optimized top score indexes for single word queries


### drop\_index
```py

def drop_index(self)

```



Drop the index if it exists


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

def search(self, query, offset=0, num=10, verbatim=False, no_content=False, no_stopwords=False, fields=None, snippet_size=500, **filters)

```



Search the index for a given query, and return a result of documents

### Parameters

- **query**: the search query, see RediSearch's documentation on query format
- **offset**: Paging offset for the results. Defaults to 0
- **num**: How many results do we want
- **verbatim**: If True, we do not attempt stemming on the query
- **no_content**: If True, we only return ids and not the document content
- **no_stopwords**: If True, we do not match the query against stopwords
- **fields**: An optional list/tuple of field names to focus the search in
- **snippet_size**: the size of the text snippet we attempt to extract from the document
- **filters**: optional numeric filters, in the format of `field = (min,max)`


## Class BatchIndexer
A batch indexer allows you to automatically batch 
document indexeing in pipelines, flushing it every N documents. 
### \_\_init\_\_
```py

def __init__(self, client, chunk_size=1000)

```



### add\_document
```py

def add_document(self, doc_id, nosave=False, score=1.0, **fields)

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

def __init__(self, id, **fields)

```



### snippetize
```py

def snippetize(self, field, size=500, bold_tokens=())

```



Create a shortened snippet from the document's content 
:param size: the szie of the snippet in characters. It might be a bit longer or shorter
:param boldTokens: a list of tokens we want to make bold (basically the query terms)




## Class NumericField
NumericField is used to define a numeric field in a schema defintion
### \_\_init\_\_
```py

def __init__(self, name)

```



### redis\_args
```py

def redis_args(self)

```





## Class Result
Represents the result of a search query, and has an array of Document objects
### \_\_init\_\_
```py

def __init__(self, res, hascontent, queryText, duration=0, snippet_size=500)

```





## Class Suggestion
Represents a single suggestion being sent or returned from the auto complete server
### \_\_init\_\_
```py

def __init__(self, string, score=1.0)

```





## Class TextField
TextField is used to define a text field in a schema definition
### \_\_init\_\_
```py

def __init__(self, name, weight=1.0)

```



### redis\_args
```py

def redis_args(self)

```





