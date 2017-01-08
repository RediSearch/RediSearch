#include "rmutil/strings.h"
#include "spec.h"
#include "util/logging.h"
#include <math.h>

RedisModuleType *IndexSpecType;

/*
* Get a field spec by field name. Case insensitive!
* Return the field spec if found, NULL if not
*/
FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len) {
  for (int i = 0; i < spec->numFields; i++) {
    if (!strncmp(spec->fields[i].name, name, len)) {
      return &spec->fields[i];
    }
  }

  return NULL;
};

/*
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relvant part of argv.
*
* The format currently is <field> <NUMERIC|weight>, <field> <NUMERIC|weight> ...
*/
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc) {
  if (argc % 2) {
    return NULL;
  }

  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }

  return IndexSpec_Parse(RedisModule_StringPtrLen(name, NULL), args, argc);
}

IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc) {

  IndexSpec *spec = NewIndexSpec(name, argc / 2);

  int id = 1;
  int n = 0;
  for (int i = 0; i < argc; i += 2, id *= 2) {
    // size_t sz;
    spec->fields[n].name = strdup(argv[i]);
    double d = 0;
    FieldType t = F_FULLTEXT;
    // printf("%s %s\n", argv[i], argv[i+1])
    if (!strncasecmp(argv[i + 1], NUMERIC_STR, strlen(NUMERIC_STR))) {
      t = F_NUMERIC;
    } else {
      d = strtod(argv[i + 1], NULL);
      if (d == 0 || d == HUGE_VAL || d == -HUGE_VAL) {
        goto failure;
      }
    }

    // spec->fields[n].name[sz] = '\0';

    spec->fields[n].weight = d;
    spec->fields[n].type = t;
    spec->fields[n].id = id;
    spec->numFields++;
    // printf("loaded field %s id %d\n", argv[i], id);
    n++;
  }

  return spec;

failure:  // on failure free the spec fields array and return an error

  IndexSpec_Free(spec);
  return NULL;
}

void IndexSpec_Free(void *ctx) {
  IndexSpec *spec = ctx;
  if (spec->fields != NULL) {
    for (int i = 0; i < spec->numFields; i++) {
      free(spec->fields[i].name);
    }
    free(spec->fields);
  }
  free(spec->name);
  free(spec);
}

/* Saves the spec as a LIST, containing basically the arguments needed to recreate the spec */
int IndexSpec_Save(RedisModuleCtx *ctx, IndexSpec *sp) {
  RedisModuleKey *k =
      RedisModule_OpenKey(ctx, RedisModule_CreateStringPrintf(ctx, "idx:%s", sp->name),
                          REDISMODULE_READ | REDISMODULE_WRITE);
  if (k == NULL) {
    return REDISMODULE_ERR;
  }
  // reset the list we'll be writing into
  if (RedisModule_DeleteKey(k) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < sp->numFields; i++) {
    RedisModule_ListPush(
        k, REDISMODULE_LIST_TAIL,
        RedisModule_CreateString(ctx, sp->fields[i].name, strlen(sp->fields[i].name)));
    if (sp->fields[i].type == F_FULLTEXT) {
      RedisModule_ListPush(k, REDISMODULE_LIST_TAIL,
                           RedisModule_CreateStringPrintf(ctx, "%f", sp->fields[i].weight));
    } else {
      RedisModule_ListPush(k, REDISMODULE_LIST_TAIL,
                           RedisModule_CreateString(ctx, NUMERIC_STR, strlen(NUMERIC_STR)));
    }
  }

  return REDISMODULE_OK;
}

/* Load the spec from the saved version */
IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite) {

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx, RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, name),
                          REDISMODULE_READ | (openWrite ? REDISMODULE_WRITE : 0));

  // we do not allow empty indexes when loading an existing index
  if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
    return NULL;
  }

  IndexSpec *ret = RedisModule_ModuleTypeGetValue(k);
  return ret;
}

u_char IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc) {
  u_char ret = 0;

  for (int i = 0; i < argc; i++) {
    size_t len;
    const char *p = RedisModule_StringPtrLen(argv[i], &len);

    FieldSpec *fs = IndexSpec_GetField(sp, p, len);
    if (fs != NULL) {
      LG_DEBUG("Found mask for %s: %d\n", p, fs->id);
      ret |= (fs->id & 0xff);
    }
  }

  return ret;
}

IndexSpec *NewIndexSpec(const char *name, size_t numFields) {
  IndexSpec *sp = malloc(sizeof(IndexSpec));
  sp->fields = calloc(sizeof(FieldSpec), numFields ? numFields : SPEC_MAX_FIELDS);
  sp->numFields = 0;
  sp->flags = Index_DefaultFlags;
  sp->name = strdup(name);
  memset(&sp->stats, 0, sizeof(sp->stats));
  return sp;
}

void __fieldSpec_rdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name, strlen(f->name));
  RedisModule_SaveUnsigned(rdb, f->id);
  RedisModule_SaveUnsigned(rdb, f->type);
  RedisModule_SaveDouble(rdb, f->weight);
}

void __fieldSpec_rdbLoad(RedisModuleIO *rdb, FieldSpec *f) {

  f->name = RedisModule_LoadStringBuffer(rdb, NULL);
  f->id = RedisModule_LoadUnsigned(rdb);
  f->type = RedisModule_LoadUnsigned(rdb);
  f->weight = RedisModule_LoadDouble(rdb);
}

void __indexStats_rdbLoad(RedisModuleIO *rdb, IndexStats *stats) {
  stats->numDocuments = RedisModule_LoadUnsigned(rdb);
  stats->numTerms = RedisModule_LoadUnsigned(rdb);
  stats->numRecords = RedisModule_LoadUnsigned(rdb);
  stats->invertedSize = RedisModule_LoadUnsigned(rdb);
  stats->invertedCap = RedisModule_LoadUnsigned(rdb);
  stats->skipIndexesSize = RedisModule_LoadUnsigned(rdb);
  stats->scoreIndexesSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecsSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecRecords = RedisModule_LoadUnsigned(rdb);
  stats->termsSize = RedisModule_LoadUnsigned(rdb);
}

void __indexStats_rdbSave(RedisModuleIO *rdb, IndexStats *stats) {
  RedisModule_SaveUnsigned(rdb, stats->numDocuments);
  RedisModule_SaveUnsigned(rdb, stats->numTerms);
  RedisModule_SaveUnsigned(rdb, stats->numRecords);
  RedisModule_SaveUnsigned(rdb, stats->invertedSize);
  RedisModule_SaveUnsigned(rdb, stats->invertedCap);
  RedisModule_SaveUnsigned(rdb, stats->skipIndexesSize);
  RedisModule_SaveUnsigned(rdb, stats->scoreIndexesSize);
  RedisModule_SaveUnsigned(rdb, stats->offsetVecsSize);
  RedisModule_SaveUnsigned(rdb, stats->offsetVecRecords);
  RedisModule_SaveUnsigned(rdb, stats->termsSize);
}

void *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver != INDEX_CURRENT_VERSION) {
    return NULL;
  }
  IndexSpec *sp = malloc(sizeof(IndexSpec));
  sp->name = RedisModule_LoadStringBuffer(rdb, NULL);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  sp->numFields = RedisModule_LoadUnsigned(rdb);
  sp->fields = calloc(sp->numFields, sizeof(FieldSpec));
  for (int i = 0; i < sp->numFields; i++) {
    __fieldSpec_rdbLoad(rdb, &sp->fields[i]);
  }

  __indexStats_rdbLoad(rdb, &sp->stats);
  return sp;
}

void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value) {

  IndexSpec *sp = value;
  RedisModule_SaveStringBuffer(rdb, sp->name, strlen(sp->name));
  RedisModule_SaveUnsigned(rdb, (uint)sp->flags);

  RedisModule_SaveUnsigned(rdb, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    __fieldSpec_rdbSave(rdb, &sp->fields[i]);
  }

  __indexStats_rdbSave(rdb, &sp->stats);
}
void IndexSpec_Digest(RedisModuleDigest *digest, void *value) {
}

void IndexSpec_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = IndexSpec_RdbLoad,
                               .rdb_save = IndexSpec_RdbSave,
                               .aof_rewrite = IndexSpec_AofRewrite,
                               .free = IndexSpec_Free};

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create index spec type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}