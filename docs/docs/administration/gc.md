---
title: "Garbage collection"
linkTitle: "Garbage collection"
weight: 2
description: Details about garbage collection
aliases:
    - /docs/stack/search/design/gc/
---

# Garbage collection (GC) in RediSearch

## The need for GC

* When documents are deleted by the user, Redis only marks them as deleted in the global document table rather than deleting them outright. This is done for efficiency. Depending on the length of a document, deletion can be a long operation.
* This means that it is no longer the case that an internal numeric id is assigned to a deleted document. When the index is traversed, a check is made for deletion.
* All inverted index entries belonging to deleted document ids are garbage.
* Updating a document is basically the same as deleting it and then adding it again with a new incremental internal ID. No diffing is performed, and the indexes are appended, so the IDs remain incremental, and the updates fast.

All of the above means that if there are a lot of updates and deletes, a large portion of our inverted index will become garbage, both slowing things down and consuming unnecessary memory.

You want to optimize the index, but you also don't want to disturb normal operation. This means that optimization or garbage collection should be a background process that's non-intrusive. It only needs to be faster than the deletion rate over a sufficient period of time so that you don't create more garbage than you can collect.

## Garbage collecting a single-term index

A single-term inverted index is an array of blocks, each of which containin an encoded list of records; e.g., a document id delta plus other data depending on the index encoding scheme. When some of these records refer to deleted documents this is called garbage. 

The algorithm is simple: 

0. Create a reader and writer for each block.
1. Read each block's records one by one.
2. If no record is invalid, do nothing.
3. When a garbage record is found, the reader is advanced, but not the writer.
4. When at least one garbage record is found, the next records are encoded to the writer, recalculating the deltas.

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

### GC on numeric indexes

Numeric indexes are a tree of inverted indexes with a special encoding of (docId delta, value). This means the same algorithm can be applied to them, only traversing each inverted index object in the tree.

## FORK GC

Information about FORK GC can be found in this [blog](https://redislabs.com/blog/increased-garbage-collection-performance-redisearch-1-4-1/).

Since v1.6, the FORK GC is the default GC policy and was proven very efficient both in cleaning the index and not reducing query and indexing performance, even for very write-internsive use cases.
