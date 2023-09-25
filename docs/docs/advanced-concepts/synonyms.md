---
title: "Synonym"
linkTitle: "Synonym"
weight: 11
description: Synonym support
aliases: 
    - /docs/stack/search/reference/synonyms/
---

# Synonyms support

## Overview

Redis Stack supports synonyms. That is, searching for synonym words defined by the synonym data structure.

The synonym data structure is a set of groups, each of which contains synonym terms. For example, the following synonym data structure contains three groups, and each group contains three synonym terms:

```
{boy, child, baby}
{girl, child, baby}
{man, person, adult}
```

When these three groups are located inside the synonym data structure, it is possible to search for "child" and receive documents containing "boy", "girl", "child" and "baby".

## The synonym search technique

A simple HashMap is used to map between the terms and the group IDs. During index creation, a check is made to see if the current term appears in the synonym map, and if it does, take all the group IDs that the term belongs to.

For each group ID, another record is added to the inverted index called "\~\<id\>" that contains the same information as the term itself. When performing a search, a check is made to see if the searched term appears in the synonym map, and if it does, take all the group IDs the term belongs to. For each group ID, search for "\~\<id\>" and return the combined results. This technique ensures that all the synonyms of a given term will be returned.

## Handling concurrency

Since the indexing is performed in a separate thread, the synonyms map may change during indexing, which in turn may cause data corruption or crashes during indexing or searching. To solve this issue, a read-only copy is created for indexing purposes. The read-only copy is maintained using reference count.

As long as the synonyms map does not change, the original synonym map holds a reference to its read-only copy, so it will not be freed. After the data inside the synonyms map has changed, the synonyms map decreses the reference count of its read only copy. This ensures that when all the indexers are done using the read-only copy, it will automatically be freed. This ensures that the next time an indexer asks for a read-only copy, the synonyms map will create a new copy (containing the new data) and return it.

## Example

```
# Create an index
> FT.CREATE idx schema t text

# Create a synonym group 
> FT.SYNUPDATE idx group1 hello world

# Insert documents
> HSET foo t hello
(integer) 1
> HSET bar t world
(integer) 1

# Search
> FT.SEARCH idx hello
1) (integer) 2
2) "foo"
3) 1) "t"
   2) "hello"
4) "bar"
5) 1) "t"
   2) "world"
```
