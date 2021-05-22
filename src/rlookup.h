#pragma once

#include "spec.h"
#include "search_ctx.h"
#include "value.h"
#include "sortable.h"

#include "util/arr.h"

#include <stdint.h>
#include <assert.h>

///////////////////////////////////////////////////////////////////////////////////////////////

enum RLookupCoerceType {
  RLOOKUP_C_STR = 0,
  RLOOKUP_C_INT = 1,
  RLOOKUP_C_DBL = 2,
  RLOOKUP_C_BOOL = 3
};

//---------------------------------------------------------------------------------------------

/**
 * RLookup Key
 *
 * A lookup key is a structure which contains an array index at which the
 * data may be reliably located. This avoids needless string comparisons by
 * using quick objects rather than "dynamic" string comparison mechanisms.
 *
 * The basic workflow is that users of a given key (i.e. "foo") are expected
 * to first create the key by use of RLookup_GetKey(). This will provide
 * the consumer with an opaque object that is the slot of "foo". Once the
 * key is provided, it may then be use to both read and write the key.
 *
 * Using a pre-defined key also allows the query to maintain a central registry
 * of used names. If a user makes a typo in a query, this registry will easily
 * detect that the name was not used previously.
 *
 * Note that the same name can be registered twice, in which case it will simply
 * increment the reference to the same key.
 *
 * There are two arrays which are accessed to check for the key. Their use is
 * mutually exclusive per-key, though multiple keys may exist which can access
 * either one or the other array. The first array is the "sorting vector" for
 * a given document. The F_SVSRC flag is set on keys which are expected to be
 * found within the sorting vector.
 *
 * The second array is a "dynamic" array within a given result's row data.
 * This is used for data generated on the fly, or for data not stored within
 * the sorting vector.
 *
 * It is possible to use the same name in both the sorting vector, and also in
 * the dynamic array. To accomplish this, create a key with the F_OUTPUT flag
 * set. This flag forces the lookup mechanism to ignore the presence of the
 * sorting vector, so that they are essentially different keys, even though
 * they have the same name.
 */

struct RLookupKey : public Object {
  // The index into the array where the value resides
  uint16_t dstidx;

  // If the source of this value points to a sort vector, then this is the
  // index within the sort vector that the value is located
  uint16_t svidx;

  // Can be F_SVSRC which means the target array is a sorting vector, or
  // F_OUTPUT which means that the t... [Mozzart passed away here]
  uint16_t flags;

  // Type this lookup should be coerced to
  RLookupCoerceType fieldtype : 16;

  uint32_t refcnt; // TODO: refactor

  // Name of this field
  const char *name;

  // Pointer to next field in the list
  struct RLookupKey *next;

  ~RLookupKey();
};

//---------------------------------------------------------------------------------------------

struct RLookup {
  RLookupKey *head;
  RLookupKey *tail;

  // Length of the data row. This is not necessarily the number of lookup keys
  uint32_t rowlen;

  // Flags/options
  uint32_t options;

  // If present, then GetKey will consult this list if the value is not found in
  // the existing list of keys.
  IndexSpecCache *spcache; // TODO: ownership

  RLookup(struct IndexSpecCache *cache);
  ~RLookup();

  int LoadDocument(struct RLookupRow *dst, struct RLookupLoadOptions *options);

  RLookupKey *GetKey(const char *name, int flags);
  const RLookupKey *FindKeyWith(uint32_t f) const;

  // Get the amount of visible fields is the RLookup
  size_t GetLength(const RLookupRow *r, int requiredFlags, int excludeFlags) const;

  void WriteKeyByName(const char *name, RLookupRow *row, RSValue *value);

  // Like WriteKeyByName, but consumes a refcount
  void WriteOwnKeyByName(const char *name, RLookupRow *row, RSValue *value);

  void MoveRow(RLookupRow *src, RLookupRow *dst) const;
};

//---------------------------------------------------------------------------------------------

// If key cannot be found, do not mark it as an error, but create and mark it as F_UNRESOLVED
#define RLOOKUP_OPT_UNRESOLVED_OK 0x01

//---------------------------------------------------------------------------------------------

// Row data for a lookup key. This abstracts the question of "where" the data comes from

struct RLookupRow {
  // Sorting vector attached to document
  const RSSortingVector *sv;

  // Module key for data that derives directly from a Redis data type
  RedisModuleKey *rmkey;

  // Dynamic values obtained from prior processing
  RSValue **dyn;

  // How many values actually exist in dyn. Note that this is not the length of the array!
  size_t ndyn;

  RSValue *GetItem(const RLookupKey *key) const;

  void WriteKey(const RLookupKey *key, RSValue *value);
  void WriteOwnKey(const RLookupKey *key, RSValue *value);

  void Wipe();
  void Cleanup();
  void Dump() const;
};

//---------------------------------------------------------------------------------------------

#define RLOOKUP_F_OEXCL 0x01   // Error if name exists already
#define RLOOKUP_F_OCREAT 0x02  // Create key if it does not exit

// Force this key to be the output key, bypassing the sort vector
#define RLOOKUP_F_OUTPUT 0x04

// Check the sorting table, if necessary, for the index of the key
#define RLOOKUP_F_SVSRC 0x08

// Copy the key string via strdup. `name` may be freed
#define RLOOKUP_F_NAMEALLOC 0x10

// Do not increment the reference count of the returned key. Note that a single
// refcount is still retained within the lookup structure itself
#define RLOOKUP_F_NOINCREF 0x20

// This field needs to be loaded externally from a document. It is not natively present.
// The flag is intended to be used by you, the programmer. If you encounter
// a key with this flag set, then the value must be loaded externally and placed
// into the row in the corresponding index slot.
#define RLOOKUP_F_DOCSRC 0x40

// This field is hidden within the document and is only used as a transient
// field for another consumer. Don't output this field.
#define RLOOKUP_F_HIDDEN 0x80

// This key is used as sorting key for the result
#define RLOOKUP_F_SORTKEY 0x100

// This key is unresolved. It source needs to be derived from elsewhere
#define RLOOKUP_F_UNRESOLVED 0x200

// The opposite of F_HIDDEN.
// This field is specified as an explicit return in the RETURN list, so ensure this gets emitted.
// Only set if explicitReturn is true in the aggregation request.
#define RLOOKUP_F_EXPLICITRETURN 0x400

// These flags do not persist to the key, they are just options to GetKey()
#define RLOOKUP_TRANSIENT_FLAGS (RLOOKUP_F_OEXCL | RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF)

//---------------------------------------------------------------------------------------------

/** Get a value from the row, provided the key.
 *
 * This does not actually "search" for the key, but simply performs array lookups!
 *
 * @param lookup The lookup table containing the lookup table data
 * @param key the key that contains the index
 * @param row the row data which contains the value
 * @return the value if found, NULL otherwise.
 */

RSValue *RLookupRow::GetItem(const RLookupKey *key) const {
  RSValue *ret = NULL;
  if (dyn && array_len(dyn) > key->dstidx) {
    ret = dyn[key->dstidx];
  }
  if (!ret) {
    if (key->flags & RLOOKUP_F_SVSRC) {
      if (sv && sv->len > key->svidx) {
        ret = sv->values[key->svidx];
        if (ret != NULL && ret->t == RSValue_Null) {
          ret = NULL;
        }
      }
    }
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

enum RLookupLoadFlags {
  RLOOKUP_LOAD_KEYLIST,  // Use keylist (keys/nkeys) for the fields to list
  RLOOKUP_LOAD_SVKEYS,   // Load only cached keys (don't open keys)
  RLOOKUP_LOAD_ALLKEYS,  // Load all keys in the document
  RLOOKUP_LOAD_LKKEYS    // Load all the keys in the RLookup object
};

//---------------------------------------------------------------------------------------------

struct RLookupLoadOptions {
  struct RedisSearchCtx *sctx;

  // Needed for the key name, and perhaps the sortable
  const RSDocumentMetadata *dmd;

  /// Keys to load. If present, then loadNonCached and loadAllFields is ignored
  const RLookupKey **keys;

  // Number of keys in keys array
  size_t nkeys;

  // Control the loading of fields, in case non-SORTABLE fields are desired.
  RLookupLoadFlags mode;

  // Don't use sortables when loading documents. This might be used to ensure
  // that only the exact document and not a normalized version is employed
  int noSortables;

  // Force string return; don't coerce to native type
  int forceString;

  struct QueryError *status;
};

//---------------------------------------------------------------------------------------------

const RLookupKey *RLookup::FindKeyWith(uint32_t f) const {
  for (const RLookupKey *k = head; k; k = k->next) {
    if (k->flags & f) {
      return k;
    }
  }
  return NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////
