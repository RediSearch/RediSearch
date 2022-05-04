
#ifndef RS_NO_RMAPI
#define REDISMODULE_MAIN
#endif

#include "redismodule.h"

#include "module.h"
#include "version.h"
#include "config.h"
#include "redisearch_api.h"
#include <assert.h>
#include <ctype.h>
#include "concurrent_ctx.h"
#include "cursor.h"
#include "extension.h"
#include "alias.h"
#include "notifications.h"
#include "aggregate/aggregate.h"
#include "ext/default.h"
#include "rwlock.h"
#include "json.h"
#include "VecSim/vec_sim.h"

#ifndef RS_NO_ONLOAD
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}
#endif

/**
 * Check if we can run under the current AOF configuration. Returns true
 * or false
 */
static int validateAofSettings(RedisModuleCtx *ctx) {
  int rc = 1;

  if (RedisModule_GetContextFlags == NULL) {
    RedisModule_Log(ctx, "warning",
                    "Could not determine if AOF is in use. AOF Rewrite will crash!");
    return 1;
  }

  if ((RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_AOF) == 0) {
    // AOF disabled. All is OK, and no further checks needed
    return rc;
  }

  // Can't execute commands on the loading context, so make a new one
  RedisModuleCallReply *reply =
      RedisModule_Call(RSDummyContext, "CONFIG", "cc", "GET", "aof-use-rdb-preamble");
  assert(reply);
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
  assert(RedisModule_CallReplyLength(reply) == 2);
  const char *value =
      RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(reply, 1), NULL);

  // I tried using strcasecmp, but it seems that the yes/no replies have a trailing
  // embedded newline in them
  if (tolower(*value) == 'n') {
    RedisModule_Log(RSDummyContext, "warning", "FATAL: aof-use-rdb-preamble required if AOF is used!");
    rc = 0;
  }
  RedisModule_FreeCallReply(reply);
  return rc;
}

static int initAsModule(RedisModuleCtx *ctx) {
  // Check that redis supports thread safe context. RC3 or below doesn't
  if (RedisModule_GetThreadSafeContext == NULL) {
    RedisModule_Log(ctx, "warning",
                    "***** FATAL: Incompatible version of redis 4.0 detected. *****\n"
                    "\t\t\t\tPlease use Redis 4.0.0 or later from https://redis.io/download\n"
                    "\t\t\t\tRedis will exit now!");
    return REDISMODULE_ERR;
  }

  if (RediSearch_ExportCapi(ctx) != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Could not initialize low level api");
  } else {
    RedisModule_Log(ctx, "notice", "Low level api version %d initialized successfully",
                    REDISEARCH_CAPI_VERSION);
  }

  if (RedisModule_GetContextFlags == NULL && RSGlobalConfig.concurrentMode) {
    RedisModule_Log(ctx, "warning",
                    "GetContextFlags unsupported (need Redis >= 4.0.6). Commands executed in "
                    "MULTI or LUA will "
                    "malfunction unless 'safe' functions are used or SAFEMODE is enabled.");
  }

  if (!validateAofSettings(ctx)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

static int initAsLibrary(RedisModuleCtx *ctx) {
  // Disable concurrent mode:
  RSGlobalConfig.concurrentMode = 0;
  RSGlobalConfig.minTermPrefix = 0;
  RSGlobalConfig.maxPrefixExpansions = LONG_MAX;
  return REDISMODULE_OK;
}

static void RS_moduleInfoFields(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "fields_statistics");

  if (RSGlobalConfig.fieldsStats.numTextFields > 0){
    RedisModule_InfoBeginDictField(ctx, "fields_text");
    RedisModule_InfoAddFieldLongLong(ctx, "Text", RSGlobalConfig.fieldsStats.numTextFields);
    if (RSGlobalConfig.fieldsStats.numTextFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numTextFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numTextFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numTextFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numNumericFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_numeric");
    RedisModule_InfoAddFieldLongLong(ctx, "Numeric", RSGlobalConfig.fieldsStats.numNumericFields);
    if (RSGlobalConfig.fieldsStats.numNumericFieldsSortable > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numNumericFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numTagFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_tag");
    RedisModule_InfoAddFieldLongLong(ctx, "Tag", RSGlobalConfig.fieldsStats.numTagFields);
    if (RSGlobalConfig.fieldsStats.numTagFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numTagFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numTagFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numTagFieldsNoIndex);
    if (RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "CaseSensitive", RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numGeoFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_geo");
    RedisModule_InfoAddFieldLongLong(ctx, "Geo", RSGlobalConfig.fieldsStats.numGeoFields);
    if (RSGlobalConfig.fieldsStats.numGeoFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numGeoFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numVectorFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_vector");
    RedisModule_InfoAddFieldLongLong(ctx, "Vector", RSGlobalConfig.fieldsStats.numVectorFields);
    if (RSGlobalConfig.fieldsStats.numVectorFieldsFlat > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Flat", RSGlobalConfig.fieldsStats.numVectorFieldsFlat);
    if (RSGlobalConfig.fieldsStats.numVectorFieldsHSNW > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "HSNW", RSGlobalConfig.fieldsStats.numVectorFieldsHSNW);
    RedisModule_InfoEndDictField(ctx);
  }
}

static void RS_moduleInfoConfig(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "run_time_configs");

  RedisModule_InfoAddFieldCString(ctx, "concurrent_mode", RSGlobalConfig.concurrentMode ? "ON" : "OFF");
  if (RSGlobalConfig.extLoad != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "extension_load", (char*)RSGlobalConfig.extLoad);
  }
  if (RSGlobalConfig.frisoIni != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "friso_ini", (char*)RSGlobalConfig.frisoIni);
  }
  RedisModule_InfoAddFieldCString(ctx, "enableGC", RSGlobalConfig.enableGC ? "ON" : "OFF");
  RedisModule_InfoAddFieldLongLong(ctx, "minimal_term_prefix", RSGlobalConfig.minTermPrefix);
  RedisModule_InfoAddFieldLongLong(ctx, "maximal_prefix_expansions", RSGlobalConfig.maxPrefixExpansions);
  RedisModule_InfoAddFieldLongLong(ctx, "query_timeout_ms", RSGlobalConfig.queryTimeoutMS);
  RedisModule_InfoAddFieldCString(ctx, "timeout_policy", (char*)TimeoutPolicy_ToString(RSGlobalConfig.timeoutPolicy));
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_read_size", RSGlobalConfig.cursorReadSize);
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_max_idle_time", RSGlobalConfig.cursorMaxIdle);

  RedisModule_InfoAddFieldLongLong(ctx, "max_doc_table_size", RSGlobalConfig.maxDocTableSize);
  RedisModule_InfoAddFieldLongLong(ctx, "max_search_results", RSGlobalConfig.maxSearchResults);
  RedisModule_InfoAddFieldLongLong(ctx, "max_aggregate_results", RSGlobalConfig.maxAggregateResults);
  RedisModule_InfoAddFieldLongLong(ctx, "search_pool_size", RSGlobalConfig.searchPoolSize);
  RedisModule_InfoAddFieldLongLong(ctx, "index_pool_size", RSGlobalConfig.indexPoolSize);
  RedisModule_InfoAddFieldLongLong(ctx, "gc_scan_size", RSGlobalConfig.gcScanSize);
  RedisModule_InfoAddFieldLongLong(ctx, "min_phonetic_term_length", RSGlobalConfig.minPhoneticTermLen);
}

static void RS_moduleInfoIndexInfo(RedisModuleInfoCtx *ctx, IndexSpec *sp) {
  char *temp = "info";
  char name[strlen(sp->name) + strlen(temp) + 2];
  sprintf(name, "%s_%s", temp, sp->name);
  RedisModule_InfoAddSection(ctx, name);

  // Index flags
  if (sp->flags & ~(Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | ~Index_WideSchema)) {
    RedisModule_InfoBeginDictField(ctx, "index_options");
    if (!(sp->flags & (Index_StoreFreqs)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOFREQS_STR, "ON");
    if (!(sp->flags & (Index_StoreFieldFlags)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOFIELDS_STR, "ON");
    if (!(sp->flags & (Index_StoreTermOffsets)))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOOFFSETS_STR, "ON");
    if (sp->flags & Index_WideSchema)
      RedisModule_InfoAddFieldCString(ctx, SPEC_SCHEMA_EXPANDABLE_STR, "ON");
    RedisModule_InfoEndDictField(ctx);
  }

  // Index defenition
  RedisModule_InfoBeginDictField(ctx, "index_definition");
  SchemaRule *rule = sp->rule;
  RedisModule_InfoAddFieldCString(ctx, "type", (char*)DocumentType_ToString(rule->type));
  if (rule->filter_exp_str)
    RedisModule_InfoAddFieldCString(ctx, "filter", rule->filter_exp_str);
  if (rule->lang_default)
    RedisModule_InfoAddFieldCString(ctx, "default_language", (char*)RSLanguage_ToString(rule->lang_default));
  if (rule->lang_field)
    RedisModule_InfoAddFieldCString(ctx, "language_field", rule->lang_field);
  if (rule->score_default)
    RedisModule_InfoAddFieldDouble(ctx, "default_score", rule->score_default);
  if (rule->score_field)
    RedisModule_InfoAddFieldCString(ctx, "score_field", rule->score_field);
  if (rule->payload_field)
    RedisModule_InfoAddFieldCString(ctx, "payload_field", rule->payload_field);
  // Prefixes
  int num_prefixes = array_len(rule->prefixes);
  if (num_prefixes && rule->prefixes[0][0] != '\0') {
    char prefixes[512];
    prefixes[0] = '\0';
    for (int i = 0; i < num_prefixes; ++i) {
      char temp[128];
      sprintf(temp, "%s\"%s\"", i == 0 ? "" : ",", rule->prefixes[i]);
      strncat(prefixes, temp, sizeof(prefixes));
      prefixes[sizeof(prefixes)-1] = '\0';
    }
    RedisModule_InfoAddFieldCString(ctx, "prefixes", prefixes);
  }
  RedisModule_InfoEndDictField(ctx);

  // Attributes
  for (int i = 0; i < sp->numFields; i++) {
    const FieldSpec *fs = sp->fields + i;
    char title[7 + sizeof(int)];
    sprintf(title, "%s_%d", "field", (i+1));
    RedisModule_InfoBeginDictField(ctx, title);

    RedisModule_InfoAddFieldCString(ctx, "identifier", fs->path);
    RedisModule_InfoAddFieldCString(ctx, "attribute", fs->name);

    if (fs->options & FieldSpec_Dynamic)
      RedisModule_InfoAddFieldCString(ctx, "type", "<DYNAMIC>");
    else
      RedisModule_InfoAddFieldCString(ctx, "type", FieldSpec_GetTypeNames(INDEXTYPE_TO_POS(fs->types)));

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT))
      RedisModule_InfoAddFieldDouble(ctx,  SPEC_WEIGHT_STR, fs->ftWeight);
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      char buf[4];
      sprintf(buf, "\"%c\"", fs->tagOpts.tagSep);
      RedisModule_InfoAddFieldCString(ctx, SPEC_TAG_SEPARATOR_STR, buf);
    }
    if (FieldSpec_IsSortable(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_SORTABLE_STR, "ON");
    if (FieldSpec_IsNoStem(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOSTEM_STR, "ON");
    if (!FieldSpec_IsIndexable(fs))
      RedisModule_InfoAddFieldCString(ctx, SPEC_NOINDEX_STR, "ON");

    RedisModule_InfoEndDictField(ctx);
  }

  // More properties
  RedisModule_InfoAddFieldLongLong(ctx, "number_of_docs", sp->stats.numDocuments);

  RedisModule_InfoBeginDictField(ctx, "index_properties");
  RedisModule_InfoAddFieldULongLong(ctx, "max_doc_id", sp->docs.maxDocId);
  RedisModule_InfoAddFieldLongLong(ctx, "num_terms", sp->stats.numTerms);
  RedisModule_InfoAddFieldLongLong(ctx, "num_records", sp->stats.numRecords);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoBeginDictField(ctx, "index_properties_in_mb");
  RedisModule_InfoAddFieldDouble(ctx, "inverted_size", sp->stats.invertedSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "vector_index_size", sp->stats.vectorIndexSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "offset_vectors_size", sp->stats.offsetVecsSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "doc_table_size", sp->docs.memsize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "sortable_values_size", sp->docs.sortablesSize / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "key_table_size", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoAddFieldULongLong(ctx, "total_inverted_index_blocks", TotalIIBlocks);

  RedisModule_InfoBeginDictField(ctx, "index_properties_averages");
  RedisModule_InfoAddFieldDouble(ctx, "records_per_doc_avg",(float)sp->stats.numRecords / (float)sp->stats.numDocuments);
  RedisModule_InfoAddFieldDouble(ctx, "bytes_per_record_avg",(float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  RedisModule_InfoAddFieldDouble(ctx, "offsets_per_term_avg",(float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  RedisModule_InfoAddFieldDouble(ctx, "offset_bits_per_record_avg",8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);
  RedisModule_InfoEndDictField(ctx);

  RedisModule_InfoAddFieldLongLong(ctx, "hash_indexing_failures", sp->stats.indexingFailures);

  RedisModule_InfoBeginDictField(ctx, "index_failures");
  RedisModule_InfoAddFieldLongLong(ctx, "indexing", !!global_spec_scanner || sp->scan_in_progress);
  IndexesScanner *scanner = global_spec_scanner ? global_spec_scanner : sp->scanner;
  double percent_indexed = IndexesScanner_IndexedPrecent(scanner, sp);
  RedisModule_InfoAddFieldDouble(ctx, "percent_indexed", percent_indexed);
  RedisModule_InfoEndDictField(ctx);

  // Garbage collector
  if (sp->gc)
    GCContext_RenderStatsForInfo(sp->gc, ctx);

  // Cursor stat
  Cursors_RenderStatsForInfo(&RSCursors, sp->name, ctx);

  // Stop words
  if (sp->flags & Index_HasCustomStopwords)
    ReplyWithStopWordsListForInfo(ctx, sp->stopwords);
}

void RS_moduleInfoFunc(RedisModuleInfoCtx *ctx, int for_crash_report) {
  // Module version
  RedisModule_InfoAddSection(ctx, "version");
  char rs_version[50];
  sprintf(rs_version, "%d.%d.%d", redisVersion.majorVersion, redisVersion.minorVersion, redisVersion.patchVersion);
  RedisModule_InfoAddFieldCString(ctx, "RedisSearch_version", rs_version);

  // Numer of indexes
  RedisModule_InfoAddSection(ctx, "index");
  RedisModule_InfoAddFieldLongLong(ctx, "number_of_indexes", dictSize(specDict_g));

  // Fields statistics
  RS_moduleInfoFields(ctx);

  // Run time configuration
  RS_moduleInfoConfig(ctx);

  // FT.INFO for some of the indexes
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;
  int count = 5;
  while (count-- && (entry = dictNext(iter))) {
    IndexSpec *spec = dictGetVal(entry);
    RS_moduleInfoIndexInfo(ctx, spec);
  }
}

static inline const char* RS_GetExtraVersion() {
#ifdef GIT_VERSPEC
  return GIT_VERSPEC;
#else
  return "";
#endif
}

int RS_Initialized = 0;
RedisModuleCtx *RSDummyContext = NULL;

int RediSearch_Init(RedisModuleCtx *ctx, int mode) {
#define DO_LOG(...)                                 \
  do {                                              \
    if (ctx && (mode != REDISEARCH_INIT_LIBRARY)) { \
      RedisModule_Log(ctx, ##__VA_ARGS__);          \
    }                                               \
  } while (false)

  if (RediSearch_LockInit(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  // Print version string!
  DO_LOG("notice", "RediSearch version %d.%d.%d (Git=%s)", REDISEARCH_VERSION_MAJOR,
         REDISEARCH_VERSION_MINOR, REDISEARCH_VERSION_PATCH, RS_GetExtraVersion());
  RS_Initialized = 1;

  if (!RSDummyContext) {
    if (RedisModule_GetDetachedThreadSafeContext) {
      RSDummyContext = RedisModule_GetDetachedThreadSafeContext(ctx);
    } else {
      RSDummyContext = RedisModule_GetThreadSafeContext(NULL);
    }
  }

  if (mode == REDISEARCH_INIT_MODULE && initAsModule(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  } else if (mode == REDISEARCH_INIT_LIBRARY && initAsLibrary(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  sds confstr = RSConfig_GetInfoString(&RSGlobalConfig);
  DO_LOG("notice", "%s", confstr);
  sdsfree(confstr);

  // Init extension mechanism
  Extensions_Init();

  Indexes_Init(ctx);

  if (RSGlobalConfig.concurrentMode) {
    ConcurrentSearch_ThreadPoolStart();
  }

  GC_ThreadPoolStart();

  CleanPool_ThreadPoolStart();
  // Init cursors mechanism
  CursorList_Init(&RSCursors);

  IndexAlias_InitGlobal();

  // Register aggregation functions
  RegisterAllFunctions();

  DO_LOG("notice", "Initialized thread pool!");

  /* Load extensions if needed */
  if (RSGlobalConfig.extLoad != NULL) {

    char *errMsg = NULL;
    // Load the extension so TODO: pass with param
    if (Extension_LoadDynamic(RSGlobalConfig.extLoad, &errMsg) == REDISMODULE_ERR) {
      DO_LOG("warning", "Could not load extension %s: %s", RSGlobalConfig.extLoad, errMsg);
      rm_free(errMsg);
      return REDISMODULE_ERR;
    }
    DO_LOG("notice", "Loaded RediSearch extension '%s'", RSGlobalConfig.extLoad);
  }

  // Register the default hard coded extension
  if (Extension_Load("DEFAULT", DefaultExtensionInit) == REDISEARCH_ERR) {
    DO_LOG("warning", "Could not register default extension");
    return REDISMODULE_ERR;
  }

  // Register Info function
  if (RedisModule_RegisterInfoFunc && RedisModule_RegisterInfoFunc(ctx, RS_moduleInfoFunc) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  Initialize_KeyspaceNotifications(ctx);
  Initialize_CommandFilter(ctx);
  GetJSONAPIs(ctx, 1);
  Initialize_RdbNotifications(ctx);

  // Register rm_malloc memory functions as vector similarity memory functions.
  VecSimMemoryFunctions vecsimMemoryFunctions = {.allocFunction = rm_malloc, .callocFunction = rm_calloc, .reallocFunction = rm_realloc, .freeFunction = rm_free};
  VecSim_SetMemoryFunctions(vecsimMemoryFunctions);
  return REDISMODULE_OK;
}
