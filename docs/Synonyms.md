# Synonyms Support

## Overview
RediSearch supports synonyms - that is searching for synonyms words defined by the synonym data structure.

The synonym data structure is a set of groups, each group contains synonym terms.
for example: the following synonym data structure
contains three groups, each group contains three synonym terms:

```
{boy, child, baby}
{girl, child, baby}
{man, person, adult}
```
When those three groups are located inside the synonym data structure, it is possible to search for 'child' and receive documents
contains 'boy', 'girl', 'child' and 'baby'.

## The Synonym Search Technique

We use a simple HashMap to map between the terms and the group ids. On indexing we check if the current term appears in the synonym map, if it does we take all the group ids the term is belong to. For each group id we add another record to the inverted index called "\~\<id\>" which contains the same information as the term itself. On searching we check if the searched term appears in the synonym map, if it does we take all the group ids the term is belong to. For each group id we search for "\~\<id\>" and return the combined results. This technique will make sure we return all the synonyms of a given term.

## Handling Concurrency

Since the indexing is performed in another thread, the synonym map might change during the indexing which might cause data corruption or crashes during indexing/searches. To solve this issue we are creating a read only copy for indexing. The read only is maintained using ref count. As long as the synonym map did not change, the original synonym map hold a reference to its read only copy so it will not be free. Once the data inside the synonym map has changed, the synonym map drop the reference to its read only copy. This insures that when all the indexers will be done using the read only copy, then the read only copy will automatically freed. Also it insures that the next time an indexer will ask for a read only copy, the synonym map will create a new copy (contains the new data) and returns it.