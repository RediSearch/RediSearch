# Synonyms Support

## Overview

RediSearch supports synonyms - that is searching for synonyms words defined by the synonym data structure.

The synonym data structure is a set of groups, each group contains synonym terms. For example, the following synonym data structure contains three groups, each group contains three synonym terms:

```
{boy, child, baby}
{girl, child, baby}
{man, person, adult}
```

When these three groups are located inside the synonym data structure, it is possible to search for 'child' and receive documents contains 'boy', 'girl', 'child' and 'baby'.

## The synonym search technique

We use a simple HashMap to map between the terms and the group ids. During building the index, we check if the current term appears in the synonym map, and if it does we take all the group ids that the term belongs to.

For each group id, we add another record to the inverted index called "\~\<id\>" that contains the same information as the term itself. When performing a search, we check if the searched term appears in the synonym map, and if it does we take all the group ids the term is belong to. For each group id, we search for "\~\<id\>" and return the combined results. This technique ensures that we return all the synonyms of a given term.

## Handling concurrency

Since the indexing is performed in a separate thread, the synonyms map may change during the indexing, which in turn may cause data corruption or crashes during indexing/searches. To solve this issue, we create a read-only copy for indexing purposes. The read-only copy is maintained using ref count.

As long as the synonyms map does not change, the original synonym map holds a reference to its read-only copy so it will not be freed. Once the data inside the synonyms map has changed, the synonyms map decreses the reference count of its read only copy. This ensures that when all the indexers are done using the read only copy, then the read only copy will automatically freed. Also it ensures that the next time an indexer asks for a read-only copy, the synonyms map will create a new copy (contains the new data) and return it.
