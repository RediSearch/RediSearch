#ifndef RULES_RULES_H
#define RULES_RULES_H

#include "redismodule.h"
#include "rmutil/args.h"
#include "query_error.h"
#include "util/dllist.h"
#include "document.h"
#include "spec.h"

#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SchemaRules SchemaRules;
typedef struct SchemaRule SchemaRule;

// Create the rule list. This is usually global..
SchemaRules *SchemaRules_Create(void);
void SchemaRules_Free(SchemaRules *rules);
void SchemaRules_CleanRules(SchemaRules *rules);

/**
 * Add rules pertaining to an index
 * @param rules
 * @param index the index name. If the rule matches, it will be dispatched to
 *              the index
 * @param name a globally unique name (across all indexes) for the rule. This
 *        can be used to further identify the rule
 * @param ac the arguments passed
 * @param[out] err if there is an error
 * @return REDISMODULE_OK/REDISMODULE_ERR
 *
 * Note, this function consumes only a single rule.
 */
int SchemaRules_AddArgs(const char *index, const char *name, ArgsCursor *ac, QueryError *err);

int SchemaRules_AddArgsInternal(SchemaRules *rules, IndexSpec *spec, const char *name,
                                ArgsCursor *ac, QueryError *err);
int SchemaRules_SetArgs(ArgsCursor *ac, QueryError *err);

size_t SchemaRules_IncrRevision(void);

typedef enum {
  SCATTR_TYPE_LANGUAGE = 0x01,
  SCATTR_TYPE_SCORE = 0x02,
  SCATTR_TYPE_PAYLOAD = 0x04
} SchemaAttrType;

typedef enum {
  SCATTR_FLD_LANGUAGE = 0,
  SCATTR_FLD_SCORE,
  SCATTR_FLD_PAYLOAD,
  SCATTR_FLD_MAX
} SchemaAttrField;

typedef struct {
  float score;
  RSLanguage language : 8;  // can be an enum??
  uint8_t predefMask;       // Mask of attributes which are predefined
  RedisModuleString *payload;
  struct SchemaAttrFieldpack *fp;
} IndexItemAttrs;

typedef struct {
  IndexSpec *spec;
  IndexItemAttrs attrs;
} MatchAction;

typedef struct {
  RedisModuleString *kstr;
  RedisModuleKey *kobj;  // Necessary? We are appending to the currect idx.
} RuleKeyItem;

typedef struct {
  SpecDocQueue **pending;  // List of indexes with documents to be indexed
  size_t interval;         // interval in milliseconds. sleep time when queue is empty
  size_t indexBatchSize;   // maximum documents to index at once. Prevents starvation
  size_t nactive;          // Number of active items
  pthread_t aiThread;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  int isCancelled;
  int nolock;
} AsyncIndexQueue;

typedef struct {
  DLLIST_node llnode;
  RedisModuleString *kstr;
  IndexItemAttrs iia;
} RuleIndexableDocument;

// Check if the given document matches any of the rule sets
int SchemaRules_Check(SchemaRules *rules, RedisModuleCtx *ctx, RuleKeyItem *item,
                      MatchAction **results, size_t *nresults);

extern SchemaRules *SchemaRules_g;

/** Submits all the keys in the database for indexing */
void SchemaRules_StartScan(int wait);

typedef enum {
  // Default state
  SC_INITSCAN_UNINIT = 0,
  // Scan required; set after rdb load
  SC_INITSCAN_REQUIRED,
  // Scan is done
  SC_INITSCAN_DONE
} SCInitScanStatus;
extern int SchemaRules_InitialScanStatus_g;

#define SchemaRules_IsLoading() (SchemaRules_InitialScanStatus_g == SC_INITSCAN_REQUIRED)

void SchemaRules_ReplySyncInfo(RedisModuleCtx *ctx, IndexSpec *sp);

RSAddDocumentCtx *SchemaRules_InitACTX(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                                       const IndexItemAttrs *attrs, QueryError *e);
int SchemaRules_IndexDocument(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                              const IndexItemAttrs *attrs, QueryError *e);

// Add the item to the queue rather than indexing immediately
#define RULES_PROCESS_F_ASYNC 0x01

// Do not process the item if it already exists within the index
#define RULES_PROCESS_F_NOREINDEX 0x02

// Indicate that we don't have the GIL
#define RULES_PROCESS_F_NOGIL 0x04

void SchemaRules_ProcessItem(RedisModuleCtx *ctx, RuleKeyItem *item, int flags);

// Get the number of items which are awaiting indexing
size_t SchemaRules_QueueSize(void);

/**
 * Initializes the global rule list and subscribes to keyspace events
 */
void SchemaRules_InitGlobal(RedisModuleCtx *ctx);
void SchemaRules_ShutdownGlobal();
int SchemaRules_RegisterType(RedisModuleCtx *ctx);
void SchemaRules_RegisterIndex(IndexSpec *);
void SchemaRules_UnregisterIndex(IndexSpec *);
IndexSpec **SchemaRules_GetRegisteredIndexes(size_t *n);
void SchemaRules_Pause(void);
void SchemaRules_Resume(void);

ssize_t SchemaRules_GetPendingCount(const IndexSpec *spec);

void SchemaRules_Save(RedisModuleIO *rdb, int when);
int SchemaRules_Load(RedisModuleIO *rdb, int encver, int when);
void SchemaRules_ReplyForIndex(RedisModuleCtx *ctx, IndexSpec *sp);

extern AsyncIndexQueue *asyncQueue_g;

AsyncIndexQueue *AIQ_Create(size_t interval, size_t batchSize);
void AIQ_Destroy(AsyncIndexQueue *aq);
void AIQ_Submit(AsyncIndexQueue *aq, IndexSpec *spec, MatchAction *result, RuleKeyItem *item);
int AIQ_LoadQueue(AsyncIndexQueue *aq, RedisModuleIO *rdb);
void AIQ_SaveQueue(AsyncIndexQueue *aq, RedisModuleIO *rdb);

/**
 * This function should be called when the main thread wishes to poll until
 * completion of indexing. In order to avoid a deadlock, the queue must not
 * lock the GIL because it is already held in the main thread (and releasing the
 * GIL in the main thread is generally considered bad practice).
 *
 * This function indicates to the async thread that it does not need to call
 * the various ContextLock/ContextUnlock functions. Assumptions are:
 *
 * 1) Nothing else has access to the associated spec for either read or write
 * 2) The GIL is implicitly held by the main thread, and so we are immune
 *    from other threads (for whatever reason) trying to manipulate the data
 *
 * In practice, this function is useful only in the context of RDB loading
 */
void AIQ_SetMainThread(AsyncIndexQueue *aq, int enable);
/**
 * Custom rules:
 *
 * Custom rules may be added by other parts of the subsystem.
 */
typedef struct SchemaCustomRule SchemaCustomRule;
typedef struct SchemaCustomCtx SchemaCustomCtx;
#define SCHEMA_CUSTOM_FIRST 1
#define SCHEMA_CUSTOM_LAST 0

typedef int (*SchemaCustomCallback)(RedisModuleCtx *, RuleKeyItem *, void *arg, SchemaCustomCtx *);

/**
 * Declare that the item should be indexed using provided attributes;
 * Any relevant data is copied. Note that this function does not actually index
 * the data; indexing will take place per the schema policy
 */
void SchemaCustomCtx_Index(SchemaCustomCtx *ctx, IndexSpec *spec, IndexItemAttrs *attrs);

SchemaCustomRule *SchemaRules_AddCustomRule(SchemaCustomCallback cb, void *arg, int pos);
void SchemaRules_RemoveCustomRule(SchemaCustomRule *p);
#ifdef __cplusplus
}
#endif
#endif