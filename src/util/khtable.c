#include "khtable.h"
#include <stdlib.h>
#include <string.h>
#include "rmalloc.h"

static uint32_t primes[] = {5ul,         11ul,        23ul,      47ul,       97ul,       199ul,
                            409ul,       823ul,       1741ul,    3469ul,     6949ul,     14033ul,
                            28411ul,     57557ul,     116731ul,  236897ul,   480881ul,   976369ul,
                            1982627ul,   4026031ul,   8175383ul, 16601593ul, 33712729ul, 68460391ul,
                            139022417ul, 282312799ul, 0};

void KHTable_Init(KHTable *table, const KHTableProcs *procs, void *ctx, size_t estSize) {
  // Traverse a list of primes until we find one suitable
  uint32_t *p;
  for (p = primes; *p; p++) {
    if (*p > estSize) {
      table->numBuckets = *p;
      break;
    }
  }
  if (*p == 0) {
    p--;
    table->numBuckets = *p;
  }

  table->buckets = rm_calloc(sizeof(*table->buckets), table->numBuckets);
  table->numItems = 0;
  table->procs = *procs;
  table->alloc = ctx;
}

void KHTable_Free(KHTable *table) {
  rm_free(table->buckets);
}

void KHTable_Clear(KHTable *table) {
  memset(table->buckets, 0, sizeof(*table->buckets) * table->numBuckets);
  table->numItems = 0;
}

static int KHTable_Rehash(KHTable *table) {
  // Find new capacity
  size_t newCapacity = 0;
  for (uint32_t *p = primes; *p; p++) {
    if (*p > table->numItems) {
      newCapacity = *p;
      break;
    }
  }

  if (!newCapacity) {
    return 0;
  }

  // printf("Rehashing %lu -> %lu\n", table->numBuckets, newCapacity);

  KHTableEntry **newEntries = rm_calloc(newCapacity, sizeof(*table->buckets));
  for (size_t ii = 0; ii < table->numBuckets; ++ii) {

    KHTableEntry *cur = table->buckets[ii];
    while (cur) {
      uint32_t hash = table->procs.Hash(cur);
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

  rm_free(table->buckets);
  table->buckets = newEntries;
  table->numBuckets = newCapacity;

  return 1;
}

static KHTableEntry *KHTable_InsertNewEntry(KHTable *table, uint32_t hash,
                                            KHTableEntry **bucketHead) {
  if (++table->numItems == table->numBuckets) {
    KHTable_Rehash(table);
    bucketHead = table->buckets + (hash % table->numBuckets);
  }
  KHTableEntry *entry = table->procs.Alloc(table->alloc);
  if (*bucketHead) {
    entry->next = *bucketHead;
  } else {
    entry->next = NULL;
  }
  *bucketHead = entry;
  return entry;
}

/**
 * Return an entry for the given key, creating one if it does not already exist.
 */
KHTableEntry *KHTable_GetEntry(KHTable *table, const void *s, size_t n, uint32_t hash, int *isNew) {
  // Find the bucket
  uint32_t ix = hash % table->numBuckets;
  KHTableEntry **bucket = table->buckets + ix;

  if (*bucket == NULL) {
    if (!isNew) {
      return NULL;
    }
    *isNew = 1;
    // Most likely case - no need for rehashing
    if (++table->numItems != table->numBuckets) {
      *bucket = table->procs.Alloc(table->alloc);
      (*bucket)->next = NULL;
      return *bucket;
    } else {
      KHTable_Rehash(table);
      KHTableEntry *ret =
          KHTable_InsertNewEntry(table, hash, table->buckets + (hash % table->numBuckets));
      // Decrement the count again
      table->numItems--;
      return ret;
    }
  }

  for (KHTableEntry *cur = *bucket; cur; cur = cur->next) {
    if (table->procs.Compare(cur, s, n, hash) == 0) {
      return cur;
    }
  }

  if (!isNew) {
    return NULL;
  }

  *isNew = 1;
  return KHTable_InsertNewEntry(table, hash, bucket);
}

void KHTableIter_Init(KHTable *ht, KHTableIterator *iter) {
  iter->ht = ht;
  iter->curBucket = 0;
  iter->cur = ht->buckets[iter->curBucket];
}

KHTableEntry *KHtableIter_Next(KHTableIterator *iter) {
  KHTableEntry *ret = iter->cur;

  if (!iter->cur) {
    for (++iter->curBucket; iter->curBucket < iter->ht->numBuckets; ++iter->curBucket) {
      iter->cur = iter->ht->buckets[iter->curBucket];
      if (iter->cur) {
        ret = iter->cur;
        break;
      }
    }
  }

  if (iter->cur) {
    iter->cur = iter->cur->next;
  }
  return ret;
}

void KHTable_FreeEx(KHTable *table, void *arg,
                    void (*Free)(KHTableEntry *e, void *ctx, void *arg)) {
  KHTableIterator iter;
  KHTableIter_Init(table, &iter);
  KHTableEntry *ent;
  while ((ent = KHtableIter_Next(&iter))) {
    Free(ent, table->alloc, arg);
  }
  KHTable_Free(table);
}

static void khTable_Dump(const KHTable *table, FILE *fp) {
  printf("Table=%p\n", table);
  printf("NumEntries: %lu\n", table->numItems);
  printf("NumBuckets: %lu\n", table->numBuckets);
  for (size_t ii = 0; ii < table->numBuckets; ++ii) {
    KHTableEntry *baseEnt = table->buckets[ii];
    if (!baseEnt) {
      continue;
    }
    printf("Bucket[%lu]\n", ii);
    for (; baseEnt; baseEnt = baseEnt->next) {
      printf("   => ");
      if (table->procs.Print) {
        table->procs.Print(baseEnt, fp);
      } else {
        fprintf(fp, "%p", baseEnt);
      }
    }
  }
}
