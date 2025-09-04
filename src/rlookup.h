/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RLOOKUP_H
#define RLOOKUP_H
#include <stdint.h>
#include <assert.h>

#include <spec.h>
#include <search_ctx.h>
#include "value.h"
#include "sortable.h"
#include "util/arr.h"
#include "rlookup_rs.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  RLOOKUP_C_STR = 0,
  RLOOKUP_C_INT = 1,
  RLOOKUP_C_DBL = 2,
  RLOOKUP_C_BOOL = 3
} RLookupCoerceType;

// If the key cannot be found, do not mark it as an error, but create it and
// mark it as F_UNRESOLVED
#define RLOOKUP_OPT_UNRESOLVED_OK 0x01

// If a loader was added to load the entire document, this flag will allow
// later calls to GetKey in read mode to create a key (from the schema) even if it is not sortable
#define RLOOKUP_OPT_ALL_LOADED 0x02

typedef enum {
  RLOOKUP_M_READ,   // Get key for reading (create only if in schema and sortable)
  RLOOKUP_M_WRITE,  // Get key for writing
  RLOOKUP_M_LOAD,   // Load key from redis keyspace (include known information on the key, fail if already loaded)
} RLookupMode;

#define RLOOKUP_F_NOFLAGS 0x0 // No special flags to pass.

/**
 * This field is (or assumed to be) part of the document itself.
 * This is a basic flag for a loaded key.
 */
#define RLOOKUP_F_DOCSRC 0x01

/**
 * This field is part of the index schema.
 */
#define RLOOKUP_F_SCHEMASRC 0x02

/** Check the sorting table, if necessary, for the index of the key. */
#define RLOOKUP_F_SVSRC 0x04

/**
 * This key was created by the query itself (not in the document)
 */
#define RLOOKUP_F_QUERYSRC 0x08

/** Copy the key string via strdup. `name` may be freed */
#define RLOOKUP_F_NAMEALLOC 0x10

/**
 * If the key is already present, then overwrite it (relevant only for LOAD or WRITE modes)
 */
#define RLOOKUP_F_OVERRIDE 0x20

/**
 * Request that the key is returned for loading even if it is already loaded.
 */
#define RLOOKUP_F_FORCE_LOAD 0x40

/**
 * This key is unresolved. Its source needs to be derived from elsewhere
 */
#define RLOOKUP_F_UNRESOLVED 0x80

/**
 * This field is hidden within the document and is only used as a transient
 * field for another consumer. Don't output this field.
 */
#define RLOOKUP_F_HIDDEN 0x100

/**
 * The opposite of F_HIDDEN. This field is specified as an explicit return in
 * the RETURN list, so ensure that this gets emitted. Only set if
 * explicitReturn is true in the aggregation request.
 */
#define RLOOKUP_F_EXPLICITRETURN 0x200

/**
 * This key's value is already available in the RLookup table,
 * if it was opened for read but the field is sortable and not normalized,
 * so the data should be exactly the same as in the doc.
 */
#define RLOOKUP_F_VAL_AVAILABLE 0x400

/**
 * This key's value was loaded (by a loader) from the document itself.
 */
#define RLOOKUP_F_ISLOADED 0x800

/**
 * This key type is numeric
 */
#define RLOOKUP_T_NUMERIC 0x1000

// Flags that are allowed to be passed to GetKey
#define RLOOKUP_GET_KEY_FLAGS (RLOOKUP_F_NAMEALLOC | RLOOKUP_F_OVERRIDE | RLOOKUP_F_HIDDEN | RLOOKUP_F_EXPLICITRETURN | \
                               RLOOKUP_F_FORCE_LOAD)
// Flags do not persist to the key, they are just options to GetKey()
#define RLOOKUP_TRANSIENT_FLAGS (RLOOKUP_F_OVERRIDE | RLOOKUP_F_FORCE_LOAD)

/**
 * Get the amount of visible fields is the RLookup
 */
size_t RLookup_GetLength(const RLookup *lookup, const RLookupRow *r, int *skipFieldIndex,
                         int requiredFlags, int excludeFlags, SchemaRule *rule);
/**
 * Write a value by-name to the lookup table. This is useful for 'dynamic' keys
 * for which it is not necessary to use the boilerplate of getting an explicit
 * key.
 *
 * The reference count of the value will be incremented.
 */
void RLookup_WriteKeyByName(RLookup *lookup, const char *name, size_t len, RLookupRow *row, RSValue *value);

/**
 * Like WriteKeyByName, but consumes a refcount
 */
void RLookup_WriteOwnKeyByName(RLookup *lookup, const char *name, size_t len, RLookupRow *row, RSValue *value);


typedef enum {
  /* Use keylist (keys/nkeys) for the fields to list */
  RLOOKUP_LOAD_KEYLIST,
  /* Load only cached keys (don't open keys) */
  RLOOKUP_LOAD_SVKEYS,
  /* Load all keys in the document */
  RLOOKUP_LOAD_ALLKEYS,
  /* Load all the keys in the RLookup object */
  RLOOKUP_LOAD_LKKEYS
} RLookupLoadFlags;

typedef struct {
  struct RedisSearchCtx *sctx;

  /** Needed for the key name, and perhaps the sortable */
  const RSDocumentMetadata *dmd;

  /* Needed for rule filter where dmd does not exist */
  const char *keyPtr;
  DocumentType type;

  /** Keys to load. If present, then loadNonCached and loadAllFields is ignored */
  const RLookupKey **keys;
  /** Number of keys in keys array */
  size_t nkeys;

  /**
   * The following options control the loading of fields, in case non-SORTABLE
   * fields are desired.
   */
  RLookupLoadFlags mode;

  /**
   * Don't use sortables when loading documents. This will enforce the loader to load
   * the fields from the document itself, even if they are sortables and un-normalized.
   */
  bool forceLoad;

  /**
   * Force string return; don't coerce to native type
   */
  bool forceString;

  struct QueryError *status;
} RLookupLoadOptions;

/**
 * Attempt to load a document into the row. The document's fields are placed into
 * their corresponding slots.
 *
 * @param lt Lookup table. Contains the keys to load.
 * @param dst row that should contain the data
 * @param options options controlling the load process
 */
int RLookup_LoadDocument(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);

/**
 * Initialize the lookup. If cache is provided, then it will be used as an
 * alternate source for lookups whose fields are absent
 */
void RLookup_Init(RLookup *l, IndexSpecCache *cache);

/**
 * Releases any resources created by this lookup object. Note that if there are
 * lookup keys created with RLOOKUP_F_NOINCREF, those keys will no longer be
 * valid after this call!
 */
void RLookup_Cleanup(RLookup *l);

/**
 * Frees an individual RLookupKey, cleaning up its allocated strings
 */
void RLookupKey_Free(RLookupKey *k);

/**
 * Initialize the lookup with fields from hash.
 */
int RLookup_LoadRuleFields(RedisModuleCtx *ctx, RLookup *it, RLookupRow *dst, IndexSpec *sp, const char *keyptr);

int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv);


/**
 * Search an index field by its name in the lookup table spec cache.
 */
const FieldSpec *findFieldInSpecCache(const RLookup *lookup, const char *name);

// exposed to be called from Rust, was inline before that.
int RLookup_JSON_GetAll(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

// exposed to be called from Rust, was inline before that.
int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

// exposed to be called from Rust, was inline before that.
RSValue *hvalToValue(const RedisModuleString *src, RLookupCoerceType type);

// exposed to be called from Rust, was inline before that.
RSValue *replyElemToValue(RedisModuleCallReply *rep, RLookupCoerceType otype);

// exposed to be called from Rust, is part of a dependency and was inline before that.
size_t sdslen__(const char* s);

#ifdef __cplusplus
}
#endif

#endif
