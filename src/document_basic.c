#include "document.h"
#include "stemmer.h"
#include "rmalloc.h"
#include "module.h"

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

Document::Document(RedisModuleString *docKey, double score, RSLanguage lang)
  : docKey(docKey)
  , fields{}
  , language(lang ? lang : DEFAULT_LANGUAGE)
  , score((float) score)
  , docId{}
  , payload(nullptr)
  , flags{}
{}

//---------------------------------------------------------------------------------------------

DocumentField *Document::addFieldCommon(const char *fieldname, uint32_t typemask) {
  DocumentField f;
  f.indexAs = typemask;
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    f.name = rm_strdup(fieldname);
  } else {
    f.name = fieldname;
  }

  fields.push_back(&f);
  return &f;
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

// Add a simple char buffer value.
// This creates an RMString internally, so this must be used with F_OWNSTRINGS.

void Document::AddFieldC(const char *fieldname, const char *val, size_t vallen, uint32_t typemask) {
  if (!(flags & DOCUMENT_F_OWNSTRINGS)) throw Error("Document should own strings");
  DocumentField *f = addFieldCommon(fieldname, typemask);
  f->text = RedisModule_CreateString(RSDummyContext, val, vallen);
}

//---------------------------------------------------------------------------------------------

void Document::SetPayload(RSPayload *p) {
  payload = p;
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    // payload->data = rm_malloc(n);
    // memcpy((void *)payload->data, p, n);
    payload = std::move(p);
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

// Make the document the owner of the strings it contains

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

  for (auto const &f : fields) {
    if (f->text) {
      RedisModuleString *oldText = f->text;
      f->text = RedisModule_CreateStringFromString(RSDummyContext, oldText);
      if (flags & DOCUMENT_F_OWNREFS) {
        RedisModule_FreeString(RSDummyContext, oldText);
      }
    }
  }
  if (payload) {
    // void *tmp = rm_malloc(payload.memsize());
    // memcpy(tmp, payload, payload.memsize());
    // payload = tmp;
    payload = std::move(payload);
  }
  flags |= DOCUMENT_F_OWNSTRINGS;
  flags &= ~DOCUMENT_F_OWNREFS;
}

//---------------------------------------------------------------------------------------------

// Load all fields specified in the schema to the document.
// Note that the document must then be freed using Document_Free().
// The document must already have the docKey set.

int Document::LoadSchemaFields(RedisSearchCtx *sctx) {
  RedisModuleKey *k = RedisModule_OpenKey(sctx->redisCtx, docKey, REDISMODULE_READ);
  int rv = REDISMODULE_ERR;
  size_t nitems;

  if (!k || RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH) {
    goto done;
  }

  nitems = RedisModule_ValueLength(k);
  if (nitems == 0) {
    goto done;
  }

  MakeStringsOwner();
  fields.clear();

  for (size_t ii = 0; ii < sctx->spec->fields.size(); ++ii) {
    String fname = sctx->spec->fields[ii].name;
    RedisModuleString *v = nullptr;
    RedisModule_HashGet(k, REDISMODULE_HASH_CFIELDS, fname.c_str(), &v, nullptr);
    if (v == nullptr) {
      continue;
    }
    DocumentField* f;
    f->name = fname;
    f->text = v;  // HashGet gives us `v` with a refcount of 1, meaning we're the only owner
    fields.push_back(f);
  }
  rv = REDISMODULE_OK;

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return rv;
}

//---------------------------------------------------------------------------------------------

// Load all the fields into the document.

int Document::LoadAllFields(RedisModuleCtx *ctx) {
  int rc = REDISMODULE_ERR;
  RedisModuleCallReply *rep = nullptr;
  size_t n = 0, len;

  rep = RedisModule_Call(ctx, "HGETALL", "s", docKey);
  if (rep == nullptr || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    goto done;
  }

  len = RedisModule_CallReplyLength(rep);
  // Zero means the document does not exist in redis
  if (len == 0) {
    goto done;
  }

  MakeStringsOwner();

  fields.clear();
  fields.reserve(len / 2);

  RedisModuleCallReply *k, *v;
  for (size_t i = 0; i < len; i += 2, ++n) {
    k = RedisModule_CallReplyArrayElement(rep, i);
    v = RedisModule_CallReplyArrayElement(rep, i + 1);
    size_t nlen = 0;
    const char *name = RedisModule_CallReplyStringPtr(k, &nlen);
    fields[n]->name = rm_strndup(name, nlen);
    fields[n]->text = RedisModule_CreateStringFromCallReply(v);
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
  fields.reserve(nargs/2);
  for (size_t i = 0; i < nargs; i += 2) {
    const char *name = RedisModule_StringPtrLen(args[i], nullptr);
    fields.push_back(new DocumentField{name, args[i + 1]});
  }
}

//---------------------------------------------------------------------------------------------

// Clear the document of its fields. This does not free the document or clear its name

void Document::Clear() {
  if (flags & (DOCUMENT_F_OWNSTRINGS | DOCUMENT_F_OWNREFS)) {
    for (auto const &f : fields) {
      if (flags & DOCUMENT_F_OWNSTRINGS) {
        //rm_free(f->name);
      }
      if (f->text) {
        RedisModule_FreeString(RSDummyContext, f->text);
      }
    }
  }

  fields.clear();
}

//---------------------------------------------------------------------------------------------

// Free the document's internals (like the field array)

Document::~Document() {
  if (flags & DOCUMENT_F_DEAD) {
    return;
  }

  Clear();
  if (flags & (DOCUMENT_F_OWNREFS | DOCUMENT_F_OWNSTRINGS)) {
    RedisModule_FreeString(RSDummyContext, docKey);
  }
  if (flags & DOCUMENT_F_OWNSTRINGS) {
    if (payload) {
      delete payload;
    }
  }
}

//---------------------------------------------------------------------------------------------

// Save a document in the index. Used for returning contents in search results.

int Document::Save(RedisSearchCtx *ctx, int options, QueryError *status) {
  RedisModuleKey *k =
      RedisModule_OpenKey(ctx->redisCtx, docKey, REDISMODULE_WRITE | REDISMODULE_READ);
  if (k == nullptr || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                       RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH)) {
    status->SetError(QUERY_EREDISKEYTYPE, nullptr);
    if (k) {
      RedisModule_CloseKey(k);
    }
    return REDISMODULE_ERR;
  }

  for (auto field : fields) {
    RedisModule_HashSet(k, REDISMODULE_HASH_CFIELDS, +field->name, field->text, nullptr);
  }
  RedisModule_CloseKey(k);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

// Serialzie the document's fields to a redis client

int Document::ReplyFields(RedisModuleCtx *ctx) {
  RedisModule_ReplyWithArray(ctx, fields.size() * 2);
  for (auto const &f : fields) {
    RedisModule_ReplyWithStringBuffer(ctx, f->name.c_str(), f->name.length());
    if (f->text) {
      RedisModule_ReplyWithString(ctx, f->text);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
