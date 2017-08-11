#ifndef KHTABLE_H
#define KHTABLE_H

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

// KHTable - Minimalistic hash table without deletion support
// This uses a block allocator for its entries, so it's quite fast!
typedef struct KHTableEntry { struct KHTableEntry *next; } KHTableEntry;

typedef struct {
  // Compare two entries and see if they match
  int (*Compare)(const KHTableEntry *item, const void *s, size_t n, uint32_t h);

  // Retrieve the hash from the entry
  uint32_t (*Hash)(const KHTableEntry *ent);

  // Allocate a new entry
  KHTableEntry *(*Alloc)(void *);

  void (*Print)(const KHTableEntry *ent, FILE *fp);
} KHTableProcs;

typedef struct KHTable {
  void *alloc;
  // Buckets for the table
  KHTableEntry **buckets;
  size_t numBuckets;
  size_t numItems;
  KHTableProcs procs;
} KHTable;

/**
 * Initialize a new table. Procs contains the routines for the table itself.
 * ctx is the allocator context passed to Alloc()
 * estSize is the approximate size of the table. This is used to estimate how
 * many buckets to initially create.
 */
void KHTable_Init(KHTable *table, const KHTableProcs *procs, void *ctx, size_t estSize);

/**
 * Free the storage space used by the buckets array. This does not free your own
 * entries
 */
void KHTable_Free(KHTable *table);

/**
 * Resets the table but does not free the internals.
 */
void KHTable_Clear(KHTable *table);

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

void KHTable_Dump(const KHTable *table, FILE *fp);

#endif