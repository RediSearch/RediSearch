#include "khtable.h"
#include <stdlib.h>
#include <string.h>
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

static uint32_t primes[] = {5ul,         11ul,        23ul,      47ul,       97ul,       199ul,
                            409ul,       823ul,       1741ul,    3469ul,     6949ul,     14033ul,
                            28411ul,     57557ul,     116731ul,  236897ul,   480881ul,   976369ul,
                            1982627ul,   4026031ul,   8175383ul, 16601593ul, 33712729ul, 68460391ul,
                            139022417ul, 282312799ul, 0};

//---------------------------------------------------------------------------------------------

/**
 * Initialize a new table. Procs contains the routines for the table itself.
 * ctx is the allocator context passed to Alloc()
 * estSize is the approximate size of the table. This is used to estimate how
 * many buckets to initially create, and can help save on the number of rehashes.
 *
 * Note that currently there is no API to free individual elements. It is assumed
 * that the allocator is a block allocator.
 */

KHTable::KHTable(void *allocator, size_t estSize) {
  // Traverse a list of primes until we find one suitable
  uint32_t *p;
  for (p = primes; *p; p++) {
    if (*p > estSize) {
      numBuckets = *p;
      break;
    }
  }
  if (*p == 0) {
    p--;
    numBuckets = *p;
  }

  buckets = rm_calloc(sizeof(*buckets), numBuckets);
  numItems = 0;
  alloc = allocator;
}

//---------------------------------------------------------------------------------------------

// Free the storage space used by the buckets array. This does not free your own entries

KHTable::~KHTable() {
  rm_free(buckets);
}

//---------------------------------------------------------------------------------------------

// Resets the table but does not free the entries themselves

void KHTable::Clear() {
  memset(buckets, 0, sizeof(*buckets) * numBuckets);
  numItems = 0;
}

//---------------------------------------------------------------------------------------------

static int KHTable::Rehash() {
  // Find new capacity
  size_t newCapacity = 0;
  for (uint32_t *p = primes; *p; p++) {
    if (*p > numItems) {
      newCapacity = *p;
      break;
    }
  }

  if (!newCapacity) {
    return 0;
  }

  // printf("Rehashing %lu -> %lu\n", numBuckets, newCapacity);

  KHTableEntry **newEntries = rm_calloc(newCapacity, sizeof(*buckets));
  for (size_t ii = 0; ii < numBuckets; ++ii) {

    KHTableEntry *cur = buckets[ii];
    while (cur) {
      uint32_t hash = procs.Hash(cur);
      KHTableEntry **newBucket = newEntries + (hash % newCapacity);
      KHTableEntry *next = cur->next;
      if (*newBucket) {
        cur->next = *newBucket;
      } else {
        cur->next = NULL;
      }
      *newBucket = cur;
      cur = next;
    }
  }

  rm_free(buckets);
  buckets = newEntries;
  numBuckets = newCapacity;

  return 1;
}

//---------------------------------------------------------------------------------------------

KHTableEntry *KHTable::InsertNewEntry(uint32_t hash, KHTableEntry **bucketHead) {
  if (++numItems == numBuckets) {
    KHTable_Rehash(table);
    bucketHead = buckets + (hash % numBuckets);
  }
  KHTableEntry *entry = Alloc(alloc);
  if (*bucketHead) {
    entry->next = *bucketHead;
  } else {
    entry->next = NULL;
  }
  *bucketHead = entry;
  return entry;
}

//---------------------------------------------------------------------------------------------

/**
 * Return an entry for the given key, creating one if it does not already exist.
 * s, n are the buffer and length of the key. hash must be provided and is the
 * hashed value of the key. This should be consistent with whatever procs.Hash()
 * will give for the key.
 *
 * isNew is an in/out parameter. If isNew is not NULL, a new entry will be created
 * if it does not already exists; and isNew will be set to a nonzero value.
 *
 */

KHTableEntry *KHTable::GetEntry(const void *s, size_t n, uint32_t hash, int *isNew) {
  // Find the bucket
  uint32_t ix = hash % numBuckets;
  KHTableEntry **bucket = buckets + ix;

  if (*bucket == NULL) {
    if (!isNew) {
      return NULL;
    }
    *isNew = 1;
    // Most likely case - no need for rehashing
    if (++numItems != numBuckets) {
      *bucket = Alloc(alloc);
      (*bucket)->next = NULL;
      return *bucket;
    } else {
      Rehash();
      KHTableEntry *ret = InsertNewEntry(hash, buckets + (hash % numBuckets));
      // Decrement the count again
      numItems--;
      return ret;
    }
  }

  for (KHTableEntry *cur = *bucket; cur; cur = cur->next) {
    if (Compare(cur, s, n, hash) == 0) {
      return cur;
    }
  }

  if (!isNew) {
    return NULL;
  }

  *isNew = 1;
  return InsertNewEntry(hash, bucket);
}

//---------------------------------------------------------------------------------------------

KHTableIterator::KHTableIterator(KHTable *_ht) {
  ht = _ht;
  curBucket = 0;
  cur = _ht->buckets[curBucket];
}

//---------------------------------------------------------------------------------------------

KHTableEntry *KHTableIterator::Next() {
  KHTableEntry *ret = cur;

  if (!cur) {
    for (++curBucket; curBucket < ht->numBuckets; ++curBucket) {
      cur = ht->buckets[curBucket];
      if (cur) {
        ret = cur;
        break;
      }
    }
  }

  if (cur) {
    cur = cur->next;
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

/**
 * Free individual items. This is passed both the `ctx` (as the context from
 * KHTable_Init()), and the `arg` (for the current call).
 *
 * This function also has the effect of calling KHTable_Free()
 */

#if 0

void KHTable::FreeEx(void *arg, void (*Free)(KHTableEntry *e, void *ctx, void *arg)) {
  KHTableIterator iter(table);
  KHTableEntry *ent;
  while (ent = iter.Next()) {
    Free(ent, alloc, arg);
  }
  delete this;
}

#endif // 0

//---------------------------------------------------------------------------------------------

/**
 * Dumps a textual representation of the hash table to the given output stream
 */

void KHTable::Dump(FILE *fp) const {
  printf("Table=%p\n", this);
  printf("NumEntries: %lu\n", numItems);
  printf("NumBuckets: %lu\n", numBuckets);
  for (size_t ii = 0; ii < numBuckets; ++ii) {
    KHTableEntry *baseEnt = buckets[ii];
    if (!baseEnt) {
      continue;
    }
    printf("Bucket[%lu]\n", ii);
    for (; baseEnt; baseEnt = baseEnt->next) {
      printf("   => ");
      if (procs.Print) {
        procs.Print(baseEnt, fp);
      } else {
        fprintf(fp, "%p", baseEnt);
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
