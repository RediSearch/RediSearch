#ifndef KHTABLE_H
#define KHTABLE_H

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

// KHTable - Minimalistic hash table without deletion support
// This uses a block allocator for its entries, so it's quite fast!

/**
 * Entry structure for KHTable. The datastructure stored in the hashtable
 * should contain this in some form another, e.g.
 *
 * struct myStuff {
 *  KHTableEntry base;
 *  const char *key;
 *  size_t keyLen;
 *  int foo;
 *  int bar;
 * };
 *
 * The entry should contain the key and the key length, or at least have a way
 * that it may be accessed (See Compare function below)
 */
typedef struct KHTableEntry {
  struct KHTableEntry *next;
} KHTableEntry;

typedef struct {
  // Compare two entries and see if they match
  // The `item` is an entry previously returned via `Alloc`.
  // s and n are the key data and length. h is the hash itself.
  // This function is used when retrieving items from the table, and also when
  // inserting new items - to check for duplicates.
  //
  // It is assumed that the `item` is part of a larger user structure, and that
  // you have a way to retrieve the actual key/length from the item.
  //
  // Note that the hash is provided for convenience, in the event that the user
  // structure also maintains the hash (rather than recomputing on demand). In
  // this case, the hash can also be compared with the existing hash, and if
  // they don't match, the actual string comparison can be bypassed.
  //
  // Should return 0 if the key matches, and nonzero otherwise
  int (*Compare)(const KHTableEntry *item, const void *s, size_t n, uint32_t h);

  // Retrieve the hash from the entry. This should extract the key and key length
  // from the entry and return the hash. Note that you may also cache the hash
  // value in the entry if you so desire.
  uint32_t (*Hash)(const KHTableEntry *ent);

  // Allocate memory for a new entry. This is called via KHTable_GetEntry, and
  // should allocate enough memory for the entry and the encompassing user
  // structure.
  //
  // The pointer passed is the `ctx` argument to KHTable_Init().
  // Note that there is no API to free an individual item.
  KHTableEntry *(*Alloc)(void *);

  // Print a textual representation of the entry to the given file. This is
  // used when printing the hash table. This function can be NULL
  void (*Print)(const KHTableEntry *ent, FILE *fp);
} KHTableProcs;

typedef struct KHTable {
  // Context (`ctx`) - usually an allocator of some sort
  void *alloc;

  // Buckets for the table
  KHTableEntry **buckets;

  // Number of buckets
  size_t numBuckets;

  // Number of items
  size_t numItems;

  // Item handling functions
  KHTableProcs procs;
} KHTable;

/**
 * Initialize a new table. Procs contains the routines for the table itself.
 * ctx is the allocator context passed to Alloc()
 * estSize is the approximate size of the table. This is used to estimate how
 * many buckets to initially create, and can help save on the number of rehashes.
 *
 * Note that currently there is no API to free individual elements. It is assumed
 * that the allocator is a block allocator.
 */
void KHTable_Init(KHTable *table, const KHTableProcs *procs, void *ctx, size_t estSize);

/**
 * Free the storage space used by the buckets array. This does not free your own
 * entries
 */
void KHTable_Free(KHTable *table);

/**
 * Resets the table but does not free the entries themselves
 */
void KHTable_Clear(KHTable *table);

/**
 * Free individual items. This is passed both the `ctx` (as the context from
 * KHTable_Init()), and the `arg` (for the current call).
 *
 * This function also has the effect of calling KHTable_Free()
 */
void KHTable_FreeEx(KHTable *table, void *arg,
                    void (*Free)(KHTableEntry *entry, void *ctx, void *arg));
/**
 * Get an entry from the hash table.
 * s, n are the buffer and length of the key. hash must be provided and is the
 * hashed value of the key. This should be consistent with whatever procs.Hash()
 * will give for the key.
 *
 * isNew is an in/out parameter. If isNew is not NULL, a new entry will be created
 * if it does not already exists; and isNew will be set to a nonzero value.
 *
 */
KHTableEntry *KHTable_GetEntry(KHTable *table, const void *s, size_t n, uint32_t hash, int *isNew);

/**
 * Dumps a textual representation of the hash table to the given output stream
 */
void KHTable_Dump(const KHTable *table, FILE *fp);

typedef struct {
  KHTable *ht;
  size_t curBucket;
  KHTableEntry *cur;
} KHTableIterator;

void KHTableIter_Init(KHTable *ht, KHTableIterator *iter);
KHTableEntry *KHtableIter_Next(KHTableIterator *iter);

#endif