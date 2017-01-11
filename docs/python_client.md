# Package redisearch Documentation



## Overview 

`redisearch-py` is a python search engine library that utilizes the RediSearch Redis Module API.

It is the "official" client of redisearch, and should be regarded as its canonical client implementation.

### Example: Using the Python Client

```py

from redisearch import Client, TextField, NumericField

# Creating a client with a given index name
client = Client('myIndex')

# Creating the index definition and schema
client.create_index(TextField('title', weight=5.0), TextField('body'))

# Indexing a document
client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

# Searching
res = client.search("search engine")

# the result has the total number of results, and a list of documents
print res.total # "1"
print res.docs[0].title 

```

### Installing

1. Install redis 4.0 RC2 or above

2. [Install and RediSearch] (http://redisearch.io/Quick_Start/#building-and-running)

3. Install the python client

```sh
$ pip install redisearch
```

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

def create_index(self, *fields)

```



Create the search index. Creating an existing index juts updates its properties

### Parameters:

- **fields**: a list of TextField or NumericField objects


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





