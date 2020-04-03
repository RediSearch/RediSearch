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
void SchemaRules_Clean(SchemaRules *rules);
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

int SchemaRules_AddArgsInternal(SchemaRules *rules, const char *index, const char *name,
                                ArgsCursor *ac, QueryError *err);
int SchemaRules_SetArgs(ArgsCursor *ac, QueryError *err);

typedef struct {
  RSLanguage language;  // can be an enum??
  float score;
} IndexItemAttrs;

typedef struct {
  const char *index;
  IndexItemAttrs attrs;
} MatchAction;

typedef struct {
  RedisModuleString *kstr;
  RedisModuleKey *kobj;  // Necessary? We are appending to the currect idx.
} RuleKeyItem;

typedef enum {
  AIQ_S_IDLE = 0x00,  // nl
  AIQ_S_PROCESSING = 0x01,
  AIQ_S_CANCELLED = 0x02
} AIQState;

typedef struct {
  SpecDocQueue **pending;  // List of indexes with documents to be indexed
  size_t interval;         // interval in milliseconds. sleep time when queue is empty
  size_t indexBatchSize;   // maximum documents to index at once. Prevents starvation
  pthread_t aiThread;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  volatile AIQState state;
} AsyncIndexQueue;

typedef struct {
  DLLIST_node llnode;
  RedisModuleString *kstr;
  IndexItemAttrs iia;
} RuleIndexableDocument;

// Check if the given document matches any of the rule sets
int SchemaRules_Check(const SchemaRules *rules, RedisModuleCtx *ctx, RuleKeyItem *item,
                      MatchAction **results, size_t *nresults);

extern SchemaRules *SchemaRules_g;

/** Submits all the keys in the database for indexing */
void SchemaRules_StartScan(void);

/** If scan is in progress */
int SchemaRules_IsScanRunning(void);

void SchemaRules_ReplySyncInfo(RedisModuleCtx *ctx, IndexSpec *sp);

RSAddDocumentCtx *SchemaRules_InitACTX(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                                       const IndexItemAttrs *attrs, QueryError *e);
int SchemaRules_IndexDocument(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                              const IndexItemAttrs *attrs, QueryError *e);

// Add the item to the queue rather than indexing immediately
#define RULES_PROCESS_F_ASYNC 0x01

// Do not process the item if it already exists within the index
#define RULES_PROCESS_F_NOREINDEX 0x02
void SchemaRules_ProcessItem(RedisModuleCtx *ctx, RuleKeyItem *item, int flags);

// Get the number of items which are awaiting indexing
size_t SchemaRules_QueueSize(void);

/**
 * Initializes the global rule list and subscribes to keyspace events
 */
void SchemaRules_InitGlobal();
void SchemaRules_ShutdownGlobal();
int SchemaRules_RegisterType(RedisModuleCtx *ctx);
void SchemaRules_RegisterIndex(IndexSpec *);
void SchemaRules_UnregisterIndex(IndexSpec *);
IndexSpec **SchemaRules_GetRegisteredIndexes(size_t *n);
void SchemaRules_Pause(void);
void SchemaRules_Resume(void);

typedef enum {
  SCRULES_MODE_DEFAULT = 0,  // lb
  SCRULES_MODE_SYNC,
  SCRULES_MODE_ASYNC
} SchemaIndexMode;

/** Change the desired mode; used for testing */
void SchemaRules_SetMode(SchemaIndexMode mode);
SchemaIndexMode SchemaRules_GetMode(void);

ssize_t SchemaRules_GetPendingCount(const IndexSpec *spec);

extern AsyncIndexQueue *asyncQueue_g;

AsyncIndexQueue *AIQ_Create(size_t interval, size_t batchSize);
void AIQ_Destroy(AsyncIndexQueue *aq);
void AIQ_Submit(AsyncIndexQueue *aq, IndexSpec *spec, MatchAction *result, RuleKeyItem *item);
int AIQ_LoadQueue(AsyncIndexQueue *aq, RedisModuleIO *rdb);
void AIQ_SaveQueue(AsyncIndexQueue *aq, RedisModuleIO *rdb);

#ifdef __cplusplus
}
#endif
#endif