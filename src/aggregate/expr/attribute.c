#include "attribute.h"
#include "exprast.h"
#include "util/arr.h"

typedef struct {
  char* name;
  ExprAttributeCallback cb;
} AttrRegistryEntry;

static AttrRegistryEntry* registry_g = NULL;

static void initBuiltins(void);

void Expr_AttributesInit(void) {
  registry_g = array_new(AttrRegistryEntry, 10);
  initBuiltins();
}

void Expr_AttributesDestroy(void) {
  size_t n = array_len(registry_g);
  for (size_t ii = 0; ii < n; ++ii) {
    rm_free(registry_g[ii].name);
  }
  array_free(registry_g);
  registry_g = NULL;
}

int Expr_FindAttributeByName(const char* name, size_t n) {
  size_t alen = array_len(registry_g);
  for (size_t ii = 0; ii < alen; ++ii) {
    if (!strncasecmp(registry_g[ii].name, name, n)) {
      return ii;
    }
  }
  return -1;
}

const char* Expr_FindAttributeByCode(int code) {
  if (code < 0 || code >= array_len(registry_g)) {
    return "<unknown>";
  }
  return registry_g[code].name;
}

ExprAttributeCallback Expr_GetAttributeCallback(int code) {
  assert(code >= 0 && code < array_len(registry_g));
  return registry_g[code].cb;
}

int Expr_RegisterAttribute(const char* name, ExprAttributeCallback cb) {
  int existing = Expr_FindAttributeByName(name, strlen(name));
  if (existing >= 0) {
    return -1;
  }
  size_t n = array_len(registry_g);
  char* s = rm_strdup(name);
  AttrRegistryEntry ent = {.name = s, .cb = cb};
  registry_g = array_append(registry_g, ent);
  return n;
}

/** actual attributes */
static int keyAttribute(int code, const void* ectx, const SearchResult* res, RSValue* out) {
  // get the key of the document
  const ExprEval* e = ectx;
  RSValue* rv = NULL;
  if (e->krstr) {
    rv = RS_OwnRedisStringVal(e->krstr);
    goto done;
  }
  if (e->kstr) {
    rv = RS_NewCopiedString(e->kstr, e->nkstr);
    goto done;
  }
  if (res->dmd && res->dmd->keyPtr) {
    rv = RS_NewCopiedString(res->dmd->keyPtr, sdslen(res->dmd->keyPtr));
    goto done;
  }
  rv = RS_NullVal();

done:
  if (rv) {
    RSValue_MakeOwnReference(out, rv);
  }
  return EXPR_EVAL_OK;
}

static int docScoreAttribute(int code, const void* ectx, const SearchResult* res, RSValue* out) {
  float score = 0;
  if (res->dmd) {
    score = res->dmd->score;
  }
  RSValue_SetNumber(out, score);
  return EXPR_EVAL_OK;
}

static int resultScoreAttribute(int code, const void* ectx, const SearchResult* res, RSValue* out) {
  RSValue_SetNumber(out, res->score);
  return EXPR_EVAL_OK;
}

static int internalIdAttribute(int code, const void* ectx, const SearchResult* res, RSValue* out) {
  RSValue_SetNumber(out, res->docId);
  return EXPR_EVAL_OK;
}

static void initBuiltins(void) {
  Expr_RegisterAttribute("key", keyAttribute);
  Expr_RegisterAttribute("doc_score", docScoreAttribute);
  Expr_RegisterAttribute("result_score", resultScoreAttribute);
  Expr_RegisterAttribute("internal_id", internalIdAttribute);
}