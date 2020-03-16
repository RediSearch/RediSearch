#include "metaop.h"
#include "expression.h"
#include "module.h"

static int createHasfieldProp(RSExprMeta *meta, RSArgList *args, QueryError *err) {
  if (args->len != 1) {
    QueryError_SetError(err, QUERY_EPARSEARGS, "hasfield needs one argument");
    return REDISMODULE_ERR;
  }
  RSValue v = {{0}};
  RSValue_ToString(&v, &args->args[0]->literal);
  size_t n;
  const char *s = RSValue_StringPtrLen(&v, &n);
  meta->u.hasfield = RedisModule_CreateString(RSDummyContext, s, n);
  RSValue_Clear(&v);
  return REDISMODULE_OK;
}

static int createHasPrefixProp(RSExprMeta *meta, RSArgList *args, QueryError *err) {
  if (args->len != 1) {
    QueryError_SetError(err, QUERY_EPARSEARGS, "hasfield needs one argument");
    return REDISMODULE_ERR;
  }
  RSValue v = {{0}};
  RSValue_ToString(&v, &args->args[0]->literal);
  size_t n;
  const char *s = RSValue_StringPtrLen(&v, &n);
  meta->u.prefix.s = rm_strdup(s);
  meta->u.prefix.n = n;
  RSValue_Clear(&v);
  return REDISMODULE_OK;
}

struct metaOpMapping {
  ExprMetaCode op;
  const char *name;
  int (*ctor)(RSExprMeta *, RSArgList *, QueryError *);
} metaopMapping_g[] = {
    {.op = EXPR_METAOP_HASFIELD, .name = "hasfield", .ctor = createHasfieldProp},
    {.op = EXPR_METAOP_PREFIXMATCH, .name = "hasprefix", .ctor = createHasPrefixProp},
    {.name = NULL}};

RSExpr *RS_NewMetaOp(const char *name, size_t n, RSArgList *args, QueryError *err) {
  struct metaOpMapping *mm = &metaopMapping_g[0];
  RSExpr *e = NULL;
  while (mm->name) {
    if (!strncasecmp(name, mm->name, n)) {
      break;
    }
  }
  if (!mm->name) {
    QueryError_SetError(err, QUERY_ENOFUNCTION, "No such function");
    return NULL;
  }
  e = rm_calloc(1, sizeof(*e));
  e->t = RSExpr_Metafunc;
  if (mm->ctor(&e->meta, args, err) != REDISMODULE_OK) {
    rm_free(e);
    return NULL;
  }
  return e;
}

static void evalPrefix(ExprEval *e, const RSExprMeta *m, RSValue *out) {
  const char *name = e->kstr;
  if (!e->kstr) {
    if (e->krstr) {
      name = RedisModule_StringPtrLen(e->krstr, NULL);
    } else if (e->res && e->res->dmd) {
      name = e->res->dmd->keyPtr;
    }
  }
  if (!name) {
    RSValue_MakeReference(out, RS_FalseValue);
    return;
  }
  if (!strncasecmp(m->u.prefix.s, name, m->u.prefix.n)) {
    RSValue_MakeReference(out, RS_TrueValue);
  } else {
    RSValue_MakeReference(out, RS_FalseValue);
  }
}

static void evalField(ExprEval *e, const RSExprMeta *m, RSValue *out) {
  RedisModuleKey *kk = e->rmkey;
  if (!kk && e->srcrow) {
    kk = e->srcrow->rmkey;
  }
  if (!kk) {
    RSValue_MakeReference(out, RS_FalseValue);
    return;
  }
  int exists = 0;
  RedisModule_HashGet(kk, REDISMODULE_HASH_EXISTS, m->u.hasfield, &exists);
  RSValue_MakeReference(out, exists ? RS_TrueValue : RS_FalseValue);
}

int RSMetaOp_Eval(ExprEval *e, const RSExprMeta *m, RSValue *out) {
  switch (m->op) {
    case EXPR_METAOP_PREFIXMATCH:
      evalPrefix(e, m, out);
      break;
    case EXPR_METAOP_HASFIELD:
      evalField(e, m, out);
      break;
  }
  return EXPR_EVAL_OK;
}

void RSMetaOp_Clear(RSExprMeta *m) {
  switch (m->op) {
    case EXPR_METAOP_HASFIELD:
      RedisModule_FreeString(RSDummyContext, m->u.hasfield);
      break;
    case EXPR_METAOP_PREFIXMATCH:
      rm_free(m->u.prefix.s);
      break;
  }
}

void RSMetaOp_Print(const RSExprMeta *m) {
  switch (m->op) {
    case EXPR_METAOP_HASFIELD:
      printf("hasfield(%s)", RedisModule_StringPtrLen(m->u.hasfield, NULL));
      break;
    case EXPR_METAOP_PREFIXMATCH:
      printf("hasprefix(%.*s)", (int)m->u.prefix.n, m->u.prefix.s);
  }
}