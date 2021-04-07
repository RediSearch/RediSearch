
#include "json.h"
#include "rmutil/rm_assert.h"

// REJSON APIs
RedisJSONAPI_V1 *japi = NULL;

///////////////////////////////////////////////////////////////////////////////////////////////

void ModuleChangeHandler(struct RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, 
                         RedisModuleModuleChange *ei) {
  REDISMODULE_NOT_USED(e);
  if (sub != REDISMODULE_SUBEVENT_MODULE_LOADED || strcmp(ei->module_name, "ReJSON") || japi)
    return;
  // If RedisJSON module is loaded after RediSearch need to get the API exported by RedisJSON

  if (GetJSONAPIs(ctx, 0)) {
    RedisModule_Log(NULL, "notice", "Detected RedisJSON: Acquired RedisJSON_V1 API");
  } else {
    RedisModule_Log(NULL, "error", "Detected RedisJSON: Failed to acquired RedisJSON_V1 API");
  }
  //TODO: Once registered we can unsubscribe from ServerEvent RedisModuleEvent_ModuleChange
  // Unless we want to hanle ReJSON module unload
}

int GetJSONAPIs(RedisModuleCtx *ctx, int subscribeToModuleChange) {
    japi = RedisModule_GetSharedAPI(ctx, "RedisJSON_V1");
    if (japi) {
        RedisModule_Log(NULL, "notice", "Acquired RedisJSON_V1 API");
        return 1;
    }
    if (subscribeToModuleChange) {
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ModuleChange,
                                           (RedisModuleEventCallback) ModuleChangeHandler);
    }
    return 0;
}

//---------------------------------------------------------------------------------------------

static RSLanguage SchemaRule_JsonLanguage(RedisModuleCtx *ctx, const SchemaRule *rule,
                                          RedisJSONKey jsonKey, const char *keyName) {
  int rv = REDISMODULE_ERR;
  RSLanguage lang = rule->lang_default;
  if (!rule->lang_field) {
    goto done;
  }

  const char *langStr;
  if (RedisJSON_GetString(jsonKey, rule->lang_field, &langStr, NULL) != REDISMODULE_OK) {
    goto done;
  }
  
  lang = RSLanguage_Find(langStr);
  if (lang == RS_LANG_UNSUPPORTED) {
    lang = rule->lang_default;
    goto done;
  }

done:
  return lang;
}

static RSLanguage SchemaRule_JsonScore(RedisModuleCtx *ctx, const SchemaRule *rule,
                                       RedisJSONKey jsonKey, const char *keyName) {
  double score = rule->score_default;
  if (!rule->score_field) {
    goto done;
  }

  if(japi->getDoubleFromKey(jsonKey, rule->score_field, &score) != REDISMODULE_OK) {
    RedisModule_Log(NULL, "warning", "invalid field %s for key %s", rule->score_field, keyName);
  }

done:
  return score;
}

int Document_LoadSchemaFieldJson(Document *doc, RedisSearchCtx *sctx) {
  int rv = REDISMODULE_ERR;
  IndexSpec *spec = sctx->spec;
  SchemaRule *rule = spec->rule;
  RedisModuleCtx *ctx = sctx->redisCtx;
  size_t nitems = sctx->spec->numFields;

  RedisJSONKey jsonKey = japi->openKey(ctx, doc->docKey);
  if (!jsonKey) {
    goto done;
  }
  Document_MakeStringsOwner(doc); // TODO: necessary??

  const char *keyName = RedisModule_StringPtrLen(doc->docKey, NULL);
  doc->language = SchemaRule_JsonLanguage(sctx->redisCtx, rule, jsonKey, keyName);
  doc->score = SchemaRule_JsonScore(sctx->redisCtx, rule, jsonKey, keyName);
  // No payload on JSON as RedisJSON does not support binary fields

  const char *jsonVal;

  size_t count;
  JSONType type;
  RedisJSON json = NULL;
  doc->fields = rm_calloc(nitems, sizeof(*doc->fields));
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    FieldSpec *field = &spec->fields[ii];
    const char *fpath = field->path;

    // retrive json pointer
    // TODO: check option to move to getStringFromKey
    json = japi->get(jsonKey, fpath, &type, &count);
    if (!json || type == JSONType_Array || type == JSONType_Object) {
      RedisModule_Log(ctx, "verbose", "Field contains array or object");
      continue;
    }

    size_t oix = doc->numFields++;
    doc->fields[oix].path = rm_strdup(fpath);
    doc->fields[oix].name = (field->name == field->path) ? doc->fields[oix].path
                                                         : rm_strdup(field->name);

    // on crdt the return value might be the underline value, we must copy it!!!
    // TODO: change `fs->text` to support hash or json not RedisModuleString
    if (japi->getRedisModuleString(json, &doc->fields[oix].text) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "verbose", "Failed to load value from field %s", fpath);
      goto done;
    }
    japi->close(json);
    json = NULL;
  }
  rv = REDISMODULE_OK;

done:
  if (jsonKey) {
    japi->closeKey(jsonKey);
  }
  if (json) {
    japi->close(json);
  }
  return rv;
}
