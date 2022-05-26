#include "document.h"
#include "stemmer.h"
#include "rmalloc.h"
#include "module.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Initialize document structure with the relevant fields. numFields will allocate
 * the fields array, but you must still actually copy the data along.
 *
 * Note that this function assumes that the pointers passed in will remain valid
 * throughout the lifetime of the document. If you need to make independent copies
 * of the data within the document, call Document_Detach on the document (after
 * calling this function).
 */

void Document::Document(RedisModuleString *docKey, double score, RSLanguage lang) {
  docKey = docKey;
  score = (float)score;
  numFields = 0;
  fields = NULL;
  language = lang ? lang : DEFAULT_LANGUAGE;
  payload = NULL;
  payloadSize = 0;
}

//---------------------------------------------------------------------------------------------

DocumentField *Document::addFieldCommon(const char *fieldname, uint32_t typemask) {
  DocumentField *f = fields + numFields - 1;
  indexAs = typemask;
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    name = rm_strdup(fieldname);
  } else {
    name = fieldname;
  }
  return f;
}

//---------------------------------------------------------------------------------------------

void Document::AddField(const char *fieldname, RedisModuleString *fieldval,
                        uint32_t typemask) {
  DocumentField *f = addFieldCommon(fieldname, typemask);
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    f->text = RedisModule_CreateStringFromString(RSDummyContext, fieldval);
  } else {
    f->text = fieldval;
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Add a simple char buffer value. This creates an RMString internally, so this
 * must be used with F_OWNSTRINGS
 */

void Document::AddFieldC(const char *fieldname, const char *val, size_t vallen,
                         uint32_t typemask) {
  RS_LOG_ASSERT(flags & DOCUMENT_F_OWNSTRINGS, "Document should own strings");
  DocumentField *f = addFieldCommon(fieldname, typemask);
  f->text = RedisModule_CreateString(RSDummyContext, val, vallen);
}

//---------------------------------------------------------------------------------------------

void Document::SetPayload(const void *p, size_t n) {
  payload = p;
  payloadSize = n;
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    payload = rm_malloc(n);
    memcpy((void *)payload, p, n);
  }
}

//---------------------------------------------------------------------------------------------

// Move the contents of one document to another. This also manages ownership semantics

static void Document::Move(Document *dst, Document *src) {
  if (dst == src) {
    return;
  }
  *dst = *src;
  src->flags |= DOCUMENT_F_DEAD;
}

//---------------------------------------------------------------------------------------------

/**
 * Make the document the owner of the strings it contains
 */

void Document::MakeStringsOwner() {
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    // Already the owner
    return;
  }
  RedisModuleString *oldDocKey = docKey;
  docKey = RedisModule_CreateStringFromString(RSDummyContext, oldDocKey);
  if (flags & DOCUMENT_F_OWNREFS) {
    RedisModule_FreeString(RSDummyContext, oldDocKey);
  }

  for (size_t ii = 0; ii < numFields; ++ii) {
    DocumentField *f = fields + ii;
    f->name = rm_strdup(f->name);
    if (f->text) {
      RedisModuleString *oldText = f->text;
      f->text = RedisModule_CreateStringFromString(RSDummyContext, oldText);
      if (flags & DOCUMENT_F_OWNREFS) {
        RedisModule_FreeString(RSDummyContext, oldText);
      }
    }
  }
  if (payload) {
    void *tmp = rm_malloc(payloadSize);
    memcpy(tmp, payload, payloadSize);
    payload = tmp;
  }
  flags |= DOCUMENT_F_OWNSTRINGS;
  flags &= ~DOCUMENT_F_OWNREFS;
}

//---------------------------------------------------------------------------------------------

/**
 * Make the document object steal references to the document's strings.
 */

// TODO remove uncovered and clean DOCUMENT_F_OWNREFS from all code

void Document::MakeRefOwner() {
  flags |= DOCUMENT_F_OWNREFS;
}

//---------------------------------------------------------------------------------------------

/**
 * Load all fields specified in the schema to the document. Note that
 * the document must then be freed using Document_Free().
 *
 * The document must already have the docKey set
 */

int Document::LoadSchemaFields(RedisSearchCtx *sctx) {
  RedisModuleKey *k = RedisModule_OpenKey(sctx->redisCtx, docKey, REDISMODULE_READ);
  int rv = REDISMODULE_ERR;
  if (!k || RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH) {
    goto done;
  }

  size_t nitems = RedisModule_ValueLength(k);
  if (nitems == 0) {
    goto done;
  }

  MakeStringsOwner();
  fields = new DocumentField();

  for (size_t ii = 0; ii < sctx->spec->numFields; ++ii) {
    const char *fname = sctx->spec->fields[ii].name;
    RedisModuleString *v = NULL;
    RedisModule_HashGet(k, REDISMODULE_HASH_CFIELDS, fname, &v, NULL);
    if (v == NULL) {
      continue;
    }
    size_t oix = numFields++;
    fields[oix].name = rm_strdup(fname);
    fields[oix].text = v;  // HashGet gives us `v` with a refcount of 1, meaning we're the only owner
  }
  rv = REDISMODULE_OK;

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return rv;
}

//---------------------------------------------------------------------------------------------

/**
 * Load all the fields into the document.
 */

int Document::LoadAllFields(RedisModuleCtx *ctx) {
  int rc = REDISMODULE_ERR;
  RedisModuleCallReply *rep = NULL;

  rep = RedisModule_Call(ctx, "HGETALL", "s", docKey);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    goto done;
  }

  size_t len = RedisModule_CallReplyLength(rep);
  // Zero means the document does not exist in redis
  if (len == 0) {
    goto done;
  }

  MakeStringsOwner();

  fields = rm_calloc(len / 2, sizeof(DocumentField));
  numFields = len / 2;
  size_t n = 0;
  RedisModuleCallReply *k, *v;
  for (size_t i = 0; i < len; i += 2, ++n) {
    k = RedisModule_CallReplyArrayElement(rep, i);
    v = RedisModule_CallReplyArrayElement(rep, i + 1);
    size_t nlen = 0;
    const char *name = RedisModule_CallReplyStringPtr(k, &nlen);
    fields[n].name = rm_strndup(name, nlen);
    fields[n].text = RedisModule_CreateStringFromCallReply(v);
  }
  rc = REDISMODULE_OK;

done:
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

void Document::LoadPairwiseArgs(RedisModuleString **args, size_t nargs) {
  fields = rm_calloc(nargs / 2, sizeof(*fields));
  numFields = nargs / 2;
  size_t oix = 0;
  for (size_t ii = 0; ii < nargs; ii += 2, oix++) {
    DocumentField *dst = fields + oix;
    const char *name = RedisModule_StringPtrLen(args[ii], NULL);
    dst->name = name;
    dst->text = args[ii + 1];
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Clear the document of its fields. This does not free the document or clear its name
 */

void Document::Clear() {
  if (flags & (DOCUMENT_F_OWNSTRINGS | DOCUMENT_F_OWNREFS)) {
    for (size_t ii = 0; ii < numFields; ++ii) {
      if (flags & DOCUMENT_F_OWNSTRINGS) {
        rm_free((void *)fields[ii].name);
      }
      if (fields[ii].text) {
        RedisModule_FreeString(RSDummyContext, fields[ii].text);
      }
    }
  }
  rm_free(fields);
  numFields = 0;
  fields = NULL;
}

//---------------------------------------------------------------------------------------------

// Free the document's internals (like the field array)

void Document::~Document() {
  if (flags & DOCUMENT_F_DEAD) {
    return;
  }

  Clear();
  if (flags & (DOCUMENT_F_OWNREFS | DOCUMENT_F_OWNSTRINGS)) {
    RedisModule_FreeString(RSDummyContext, docKey);
  }
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    if (payload) {
      rm_free((void *)payload);
    }
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Save a document in the index. Used for returning contents in search results.
 */

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc, int options, QueryError *status) {
  RedisModuleKey *k =
      RedisModule_OpenKey(ctx->redisCtx, doc->docKey, REDISMODULE_WRITE | REDISMODULE_READ);
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH)) {
    status->SetError(QUERY_EREDISKEYTYPE, NULL);
    if (k) {
      RedisModule_CloseKey(k);
    }
    return REDISMODULE_ERR;
  }
  if ((options & REDIS_SAVEDOC_NOCREATE) && RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_CloseKey(k);
    status->SetError(QUERY_ENODOC, "Document does not exist");
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < doc->numFields; i++) {
    RedisModule_HashSet(k, REDISMODULE_HASH_CFIELDS, doc->fields[i].name, doc->fields[i].text,
                        NULL);
  }
  RedisModule_CloseKey(k);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

/* Serialzie the document's fields to a redis client */

int Document::ReplyFields(RedisModuleCtx *ctx) {
  RS_LOG_ASSERT(this, "doc is NULL");
  RedisModule_ReplyWithArray(ctx, numFields * 2);
  for (size_t j = 0; j < numFields; ++j) {
    RedisModule_ReplyWithStringBuffer(ctx, fields[j].name, strlen(fields[j].name));
    if (fields[j].text) {
      RedisModule_ReplyWithString(ctx, fields[j].text);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
