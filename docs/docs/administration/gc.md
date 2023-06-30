---
title: "Garbage collection"
linkTitle: "Garbage collection"
weight: 2
description: Details about garbage collection
aliases:
    - /docs/stack/search/design/gc/
---

# Garbage Collection in Search and Query

## 1. The Need For GC

* Deleting documents is not really deleting them. It marks the document as deleted in the global document table, to make it fast.
* This means that basically an internal numeric id is no longer assigned to a document. When we traverse the index we check for deletion.
* Thus all inverted index entries belonging to this document id are just garbage. 
* We do not want to go and explicitly delete them when deleting a document because it will make this operation very long and depending on the length of the document.
* On top of that, updating a document is basically deleting it, and then adding it again with a new incremental internal id. We do not do any diffing, and only append to the indexes, so the ids remain incremental, and the updates fast.

All of the above means that if we have a lot of updates and deletes, a large portion of our inverted index will become garbage - both slowing things down and consuming unnecessary memory. 

Thus we want to optimize the index. But we also do not want to disturb the normal operation. This means that optimization or garbage collection should be a background process, that is non intrusive. It only needs to be faster than the deletion rate over a long enough period of time so that we don't create more garbage than we can collect.

## 2. Garbage Collecting a Single Term Index

A single term inverted index is consisted of an array of "blocks" each containing an encoded list of records - document id delta plus other data depending on the index encoding scheme. When some of these records refer to deleted documents, this is called garbage. 

The algorithm is pretty simple: 

0. Create a reader and writer for each block
1. Read each block's records one by one
2. If no record is invalid, do nothing
3. Once we found a garbage record, we advance the reader but not the writer.
4. Once we found at least one garbage record, we encode the next records to the writer, recalculating the deltas.

Pseudo code:

```
foreach index_block as block:
   
   reader = new_reader(block)
   writer = new_write(block)
   garbage = 0
   while not reader.end():
        record = reader.decode_next()
        if record.is_valid():
            if garbage != 0:
                # Write the record at the writer's tip with a newly calculated delta
                writer.write_record(record)
            else:
                writer.advance(record.length)
        else:
            garbage += record.length
```

### 2.1 Garbage Collection on Numeric Indexes

Numeric indexes are now a tree of inverted indexes with a special encoding of (docId delta,value). This means the same algorithm can be applied to them, only traversing each inverted index object in the tree.

## 3. FORK GC

Information about FORK GC can be found in this [blog](https://redislabs.com/blog/increased-garbage-collection-performance-redisearch-1-4-1/)

Since v1.6 the FORK GC is the default GC policy and was proven very efficient both in cleaning the index and not reduce query and indexing performance (even for a very write internsive use-cases)


