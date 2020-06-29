#include "document.h"
#include "stemmer.h"
#include "rmalloc.h"
#include "module.h"
#include "rmutil/rm_assert.h"

void Document_Init(Document *doc, RedisModuleString *docKey, double score, RSLanguage lang) {
  doc->docKey = docKey;
  doc->score = (float)score;
  doc->numFields = 0;
  doc->fields = NULL;
  doc->language = lang ? lang : DEFAULT_LANGUAGE;
  doc->payload = NULL;
  doc->payloadSize = 0;
}

static DocumentField *addFieldCommon(Document *d, const char *fieldname, uint32_t typemask) {
  d->fields = rm_realloc(d->fields, (++d->numFields) * sizeof(*d->fields));
  DocumentField *f = d->fields + d->numFields - 1;
  f->indexAs = typemask;
  if (d->flags & DOCUMENT_F_OWNSTRINGS) {
    f->name = rm_strdup(fieldname);
  } else {
    f->name = fieldname;
  }
  return f;
}

void Document_AddField(Document *d, const char *fieldname, RedisModuleString *fieldval,
                       uint32_t typemask) {
  DocumentField *f = addFieldCommon(d, fieldname, typemask);
  if (d->flags & DOCUMENT_F_OWNSTRINGS) {
    f->text = RedisModule_CreateStringFromString(RSDummyContext, fieldval);
  } else {
    f->text = fieldval;
  }
}

void Document_AddFieldC(Document *d, const char *fieldname, const char *val, size_t vallen,
                        uint32_t typemask) {
  RS_LOG_ASSERT(d->flags & DOCUMENT_F_OWNSTRINGS, "Document should own strings");
  DocumentField *f = addFieldCommon(d, fieldname, typemask);
  f->text = RedisModule_CreateString(RSDummyContext, val, vallen);
}

void Document_SetPayload(Document *d, const void *p, size_t n) {
  d->payload = p;
  d->payloadSize = n;
  if (d->flags & DOCUMENT_F_OWNSTRINGS) {
    d->payload = rm_malloc(n);
    memcpy((void *)d->payload, p, n);
  }
}

void Document_Move(Document *dst, Document *src) {
  if (dst == src) {
    return;
  }
  *dst = *src;
  src->flags |= DOCUMENT_F_DEAD;
}

void Document_MakeStringsOwner(Document *d) {
  if (d->flags & DOCUMENT_F_OWNSTRINGS) {
    // Already the owner
    return;
  }
  RedisModuleString *oldDocKey = d->docKey;
  d->docKey = RedisModule_CreateStringFromString(RSDummyContext, oldDocKey);
  if (d->flags & DOCUMENT_F_OWNREFS) {
    RedisModule_FreeString(RSDummyContext, oldDocKey);
  }

  for (size_t ii = 0; ii < d->numFields; ++ii) {
    DocumentField *f = d->fields + ii;
    f->name = rm_strdup(f->name);
    if (f->text) {
      RedisModuleString *oldText = f->text;
      f->text = RedisModule_CreateStringFromString(RSDummyContext, oldText);
      if (d->flags & DOCUMENT_F_OWNREFS) {
        RedisModule_FreeString(RSDummyContext, oldText);
      }
    }
  }
  if (d->payload) {
    void *tmp = rm_malloc(d->payloadSize);
    memcpy(tmp, d->payload, d->payloadSize);
    d->payload = tmp;
  }
  d->flags |= DOCUMENT_F_OWNSTRINGS;
  d->flags &= ~DOCUMENT_F_OWNREFS;
}

// TODO remove uncovered and clean DOCUMENT_F_OWNREFS from all code
void Document_MakeRefOwner(Document *doc) {
  doc->flags |= DOCUMENT_F_OWNREFS;
}

int Document_LoadSchemaFields(Document *doc, RedisSearchCtx *sctx) {
  RedisModuleKey *k = RedisModule_OpenKey(sctx->redisCtx, doc->docKey, REDISMODULE_READ);
  int rv = REDISMODULE_ERR;
  if (!k || RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH) {
    goto done;
  }

  size_t nitems = RedisModule_ValueLength(k);
  if (nitems == 0) {
    goto done;
  }

  Document_MakeStringsOwner(doc);
  doc->fields = rm_calloc(nitems, sizeof(*doc->fields));

  for (size_t ii = 0; ii < sctx->spec->numFields; ++ii) {
    const char *fname = sctx->spec->fields[ii].name;
    RedisModuleString *v = NULL;
    RedisModule_HashGet(k, REDISMODULE_HASH_CFIELDS, fname, &v, NULL);
    if (v == NULL) {
      continue;
    }
    size_t oix = doc->numFields++;
    doc->fields[oix].name = rm_strdup(fname);
    doc->fields[oix].text =
        v;  // HashGet gives us `v` with a refcount of 1, meaning we're the only owner
  }
  rv = REDISMODULE_OK;

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return rv;
}

int Document_LoadAllFields(Document *doc, RedisModuleCtx *ctx) {
  int rc = REDISMODULE_ERR;
  RedisModuleCallReply *rep = NULL;

  rep = RedisModule_Call(ctx, "HGETALL", "s", doc->docKey);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    goto done;
  }

  size_t len = RedisModule_CallReplyLength(rep);
  // Zero means the document does not exist in redis
  if (len == 0) {
    goto done;
  }

  Document_MakeStringsOwner(doc);

  doc->fields = rm_calloc(len / 2, sizeof(DocumentField));
  doc->numFields = len / 2;
  size_t n = 0;
  RedisModuleCallReply *k, *v;
  for (size_t i = 0; i < len; i += 2, ++n) {
    k = RedisModule_CallReplyArrayElement(rep, i);
    v = RedisModule_CallReplyArrayElement(rep, i + 1);
    size_t nlen = 0;
    const char *name = RedisModule_CallReplyStringPtr(k, &nlen);
    doc->fields[n].name = rm_strndup(name, nlen);
    doc->fields[n].text = RedisModule_CreateStringFromCallReply(v);
  }
  rc = REDISMODULE_OK;

done:
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }
  return rc;
}

void Document_LoadPairwiseArgs(Document *d, RedisModuleString **args, size_t nargs) {
  d->fields = rm_calloc(nargs / 2, sizeof(*d->fields));
  d->numFields = nargs / 2;
  size_t oix = 0;
  for (size_t ii = 0; ii < nargs; ii += 2, oix++) {
    DocumentField *dst = d->fields + oix;
    const char *name = RedisModule_StringPtrLen(args[ii], NULL);
    dst->name = name;
    dst->text = args[ii + 1];
  }
}

void Document_Clear(Document *d) {
  if (d->flags & (DOCUMENT_F_OWNSTRINGS | DOCUMENT_F_OWNREFS)) {
    for (size_t ii = 0; ii < d->numFields; ++ii) {
      if (d->flags & DOCUMENT_F_OWNSTRINGS) {
        rm_free((void *)d->fields[ii].name);
      }
      if (d->fields[ii].text) {
        RedisModule_FreeString(RSDummyContext, d->fields[ii].text);
      }
    }
  }
  rm_free(d->fields);
  d->numFields = 0;
  d->fields = NULL;
}

void Document_Free(Document *doc) {
  if (doc->flags & DOCUMENT_F_DEAD) {
    return;
  }

  Document_Clear(doc);
  if (doc->flags & (DOCUMENT_F_OWNREFS | DOCUMENT_F_OWNSTRINGS)) {
    RedisModule_FreeString(RSDummyContext, doc->docKey);
  }
  if (doc->flags & DOCUMENT_F_OWNSTRINGS) {
    if (doc->payload) {
      rm_free((void *)doc->payload);
    }
  }
}

static RedisModuleString **globalAddRSstrings = NULL;

static void initGlobalAddStrings() {
  globalAddRSstrings = rm_malloc(3 * sizeof(*globalAddRSstrings));
  const char *Sscore = "__score";
  const char *Slang = "__language";
  const char *Spayload = "__payload";

  globalAddRSstrings[0] = RedisModule_CreateString(NULL, Sscore, strlen(Sscore)); 
  globalAddRSstrings[1] = RedisModule_CreateString(NULL, Slang, strlen(Slang)); 
  globalAddRSstrings[2] = RedisModule_CreateString(NULL, Spayload, strlen(Spayload)); 
}

void freeGlobalAddStrings() {
  if (globalAddRSstrings == NULL) return;

  for (size_t i = 0; i < 3; ++i) {
    RedisModule_FreeString(NULL, globalAddRSstrings[i]);
  }
  rm_free(globalAddRSstrings);
  globalAddRSstrings = NULL;
}

int Redis_SaveDocument(RedisSearchCtx *ctx, const AddDocumentOptions *opts, QueryError *status) {
  if (!globalAddRSstrings) {
    initGlobalAddStrings();
  }

  // create an array for key + all field/value + score/language/payload
  RedisModuleString** arguments = array_new(RedisModuleString*, 1 + opts->numFieldElems + 6);

  arguments = array_append(arguments, opts->keyStr);
  arguments = array_ensure_append(arguments, opts->fieldsArray, opts->numFieldElems, RedisModuleString*);

  if (opts->score != 1.0) {
    arguments = array_append(arguments, globalAddRSstrings[0]);
    arguments = array_append(arguments, opts->scoreStr);
  }

  if (opts->languageStr) {
    arguments = array_append(arguments, globalAddRSstrings[1]);
    arguments = array_append(arguments, opts->languageStr);
  }

  if (opts->payload) {
    arguments = array_append(arguments, globalAddRSstrings[2]);
    arguments = array_append(arguments, opts->payload);
  }

  RedisModule_Call(RSDummyContext, "HSET", "!v", arguments, array_len(arguments));
  
  array_free(arguments);

  return REDISMODULE_OK;
}

int Document_ReplyFields(RedisModuleCtx *ctx, Document *doc) {
  RS_LOG_ASSERT(doc, "doc is NULL");
  RedisModule_ReplyWithArray(ctx, doc->numFields * 2);
  for (size_t j = 0; j < doc->numFields; ++j) {
    RedisModule_ReplyWithStringBuffer(ctx, doc->fields[j].name, strlen(doc->fields[j].name));
    if (doc->fields[j].text) {
      RedisModule_ReplyWithString(ctx, doc->fields[j].text);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }
  return REDISMODULE_OK;
}
