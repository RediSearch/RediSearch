# Garbage Collection in RediSearch

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

**NOTE**: Currently the algorithm does not delete empty blocks and does not merge small ones, this is a future improvement that shouldn't be very hard to do.

### 2.1 Garbage Collection on Numeric Indexes

Numeric indexes are now a tree of inverted indexes with a special encoding of (docId delta,value). This means the same algorithm can be applied to them, only traversing each inverted index object in the tree.

## 3. GC And Concurrency

Since RediSearch 0.17, we are using a multi threaded concurrent query execution model. This means that index iterators yield execution to other threads every given time (currently configured at 50us). 

It GC should also take advantage of this ability, and perform its task from a side thread, blocking the global redis lock for a short periods of time, incrementally processing indexes without interrupting execution and indexing.

This means, however, that we need to consider a few things:

1. From the Index Iterator's POV - a GC sweep might be performed while we are sleeping during iterating the index. This means that the offset of the reader in its current block might not be correct. In that case we need to do a slow search in the index for the last id we've read. This is an operation we don't want to do unless we have to.

2. From the GC thread's POC - an index might be written to or deleted while we are sleeping during a sweep.

To solve 1 we need to detect this in the reader, and adapt to this situation. Detection of a GC sweep while sleeping is simple:
* Each inverted index key has a "gc marker" variable that increments each time it has been GC'ed. 
* Before starting an index iterator, we copy the index's gc marker to the iterator's context.
* After waking up from sleep in the iterator, we check the gc markers in both objects.
* If they are the same we can simply trust the byte offset of the reader in the current block.
* IF not, we seek the reader to the previously read docId, which is slower. 

To solve 2 is simpler: 
* The GC will of course operate only while the GIL is locked.
* The GC will never yield execution while in the middle of a block.
* The GC will check whether the key has been deleted while it slept.
* The GC will get a new pointer to the next block on each read, assuring the pointer is safe.

## 4. Scheduling Garbage Collection

While the  GC process will run on a side thread and not interfere with normal operations, we do want to avoid running it when not necessary. 
The problem is that we do not know, when a document has been deleted, which terms now hold garbage, becuase for that we need a forward index or to re-tokenize the document, which are expensive in RAM and CPU respectively. 

So the GC will use sampling of random terms and collect them. 

This leaves two problems:
1. How to avoid GC when it's not needed (the user is not deleting any documents or doing it very little).
2. How to make sure we hit terms that are more likely to contain garbage. 

Solving 2 for now will be done by trying some sort of weighted random. We already have a dictionary with the frequency of terms in the index. However running a real weighted random on it is an expensive operation.

Thus, an approximation of a weighted random can be easily done by blindly selecting N terms in each round, and then applying a weighted random function to this small set of keys. This assures that even the least frequent terms will get visited.

Solving 1 can be done in the following way:

We start with a frequency F for the random sampler of the garbage collector. At first it is relatively infrequent or even 0. It can be configured.
Then, we do the following:
    * Each time a document is deleted or updated we increase the frequency a bit.
    * Each time we find a key with garbage we increase the frequency a bit.
    * Each time we sample a key with NO garbage found, we decrease the frequency a bit. This can be related to the frequency of the key, which can be seen as a hint to the probability of finding garbage in it. So if we don't find garbage in the most frequent term, it is a very strong indicator that the index contains little to no garbage.

The frequency is of course bounded to a maximum which we will never surpass. Thus when a lot of garbage is created, the frequency will be at its maximum, and will eventually decay to 0 or a pre-configured minimum - until more documents will be deleted. 


