# Document Indexing

This document describes how documents are added to the index.

## Components

* `Document` - this contains the actual document and its fields.
* `RSAddDocumentCtx` - this is the per-document state that is used while it
  is being indexed. The state is discarded once complete
* `ForwardIndex` - contains terms found in the document. The forward index
  is used to write the `InvertedIndex` (later on)
* `InvertedIndex` - an index mapping a term to occurrences within applicable
  documents.

##  Architecture

The indexing process begins by creating a new RSAddDocumentCtx and adding a
document to it. Internally this is divided into several steps.


1. Submission.
   A DocumentContext is created, and is associated with a document (as received)
   from input. The submission process will also perform some preliminary caching.

2. Preprocessing

   Once a document has been submitted, it is preprocessed. Preprocessing performs
   stateless processing on all document input fields. For text fields, this
   means tokenizing the document and creating a forward index. The preprocesors
   will store this information in per-field variables within the `AddDocumentCtx`.
   This computed result is then written to the (persistent) index later on during
   the indexing phase.

   If the document is sufficiently large, the preprocessing is done in a separate
   thread, which allows concurrent preprocessing and also avoids blocking other
   threads. If the document is smaller, the preprocessing is done within the main
   thread, avoiding the overhead of additional context switching.
   The `SELF_EXC_THRESHOLD` (macro) contains the threshold for 'sufficiently large'.

   Once the document is preprocessed,  it is submitted to be indexed.

3. Indexing

   Indexing proper consists of writing down the precomputed results of the
   preprocessing phase above. It is done in a single thread, and is in the form
   of a queue.

   Because documents must be written to the index in the exact order of their
   document ID assignment, and because we must also yield to other potential
   indexing processes, we may end up in a situation where document IDs are written
   to the index out-of-order. In order to solve that, the order in which documents
   are actually written must be well-defined. If there is only one thread writing
   documents, then this thread will not need to worry about out-of-order IDs
   while writing.

   Having a single background thread also helps optimize in several areas, as
   will be seen later on. The basic idea is that when there are a lot of
   documents queued for the indexing thread, the indexing thread may treat them
   as batch commands, greatly reducing the number of locks/unlocks of the GIL
   and the number of times term keys need to be opened and closed.

4. Skipping already indexed documents

   The phases below may operate on more than one document at a time. When a document
   is fully indexed, it is marked as done. When the thread iterates over the queue
   it will only perform processing/indexing on items not yet marked as done.

5. Term Merging

   Term merging, or forward index merging, is done when there is more than a
   single document in the queue. The forward index of each document in the queue
   is scanned, and a larger, 'master' forward index is constructed in its place.
   Each entry in the forward index contains a reference to the origin document
   as well as the normal offset/score/frequency information.
   
   Creating a 'master' forward index avoids opening common term keys once per
   document.

   If there is only one document within the queue, a 'master' forward index
   is not created.

   Note that the internal type of the master forward index is not actually
   `ForwardIndex`.

6. Document ID assignment
   
   At this point, the GIL is locked and every document in the queue is assigned
   a document ID. The assignment is done immediately before writing to the index
   so as to reduce the number of times the GIL is locked; thus, the GIL is
   locked only once - right before the index is written.

7. Writing to Indexes

   With the GIL being locked, any pending index data is written to the indexes.
   This usually involves opening one or more Redis keys, and writing/copying
   computed data into those keys.

   Once this is done, the reply for the given document is sent, and the
   `AddDocumentCtx` freed.