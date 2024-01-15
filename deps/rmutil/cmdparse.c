/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "cmdparse.h"
#include "alloc.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <sys/errno.h>
#include <ctype.h>
#include <math.h>
#include <stdarg.h>

#define __ignore__(X) \
    do { \
        int rc = (X); \
        if (rc == -1) \
            ; \
    } while(0)

int CmdString_CaseEquals(CmdString *str, const char *other) {
  if (!str || !other) return 0;
  size_t l = strlen(other);
  if (l != str->len) return 0;

  return !strncasecmp(str->str, other, l);
}

#define pad(depth)                                   \
  {                                                  \
    for (int pd = 0; pd < depth; pd++) putchar(' '); \
  }

void CmdArg_Print(CmdArg *n, int depth) {
  pad(depth);
  switch (n->type) {
    case CmdArg_Integer:
      printf("%zd", n->i);
      break;
    case CmdArg_Double:
      printf("%f", n->d);
      break;
    case CmdArg_String:
      printf("\"%.*s\"", (int)n->s.len, n->s.str);
      break;
    case CmdArg_Array:
      printf("[");
      for (int i = 0; i < n->a.len; i++) {
        CmdArg_Print(n->a.args[i], 0);
        if (i < n->a.len - 1) printf(",");
      }
      printf("]");
      break;
    case CmdArg_Object:
      printf("{\n");
      for (int i = 0; i < n->a.len; i++) {
        pad(depth + 2);
        printf("%s: =>", n->obj.entries[i].k);
        CmdArg_Print(n->obj.entries[i].v, depth + 2);
        printf("\n");
      }
      pad(depth);
      printf("}\n");
      break;
    case CmdArg_Flag:
      printf(n->b ? "TRUE" : "FALSE");
      break;
    case CmdArg_NullPtr:
      printf("NULL");
      break;
  }
}
static inline CmdArg *NewCmdArg(CmdArgType t) {
  CmdArg *ret =rm_malloc(sizeof(CmdArg));

  ret->type = t;
  return ret;
}

static CmdArg *NewCmdString(const char *s, size_t len) {
  CmdArg *ret = NewCmdArg(CmdArg_String);
  ret->s = (CmdString){.str = strdup(s), .len = len};
  return ret;
}

static CmdArg *NewCmdInteger(long long i) {
  CmdArg *ret = NewCmdArg(CmdArg_Integer);
  ret->i = i;
  return ret;
}

static CmdArg *NewCmdDouble(double d) {
  CmdArg *ret = NewCmdArg(CmdArg_Double);
  ret->d = d;
  return ret;
}

static CmdArg *NewCmdFlag(int val) {
  CmdArg *ret = NewCmdArg(CmdArg_Flag);
  ret->b = val;
  return ret;
}

static CmdArg *NewCmdArray(size_t cap) {
  CmdArg *ret = NewCmdArg(CmdArg_Array);
  ret->a.cap = cap;
  ret->a.len = 0;
  ret->a.args = rm_calloc(cap, sizeof(CmdArg *));

  return ret;
}

static CmdArg *NewCmdObject(size_t cap) {
  CmdArg *ret = NewCmdArg(CmdArg_Object);
  ret->obj = (CmdObject){
      .entries = rm_calloc(cap, sizeof(CmdKeyValue)),
      .cap = cap,
      .len = 0,
  };

  return ret;
}

/* return 1 if a flag with a given name exists in parent and is set to true */
int CmdArg_GetFlag(CmdArg *parent, const char *flag) {
  CmdArg *f = CmdArg_FirstOf(parent, flag);
  if (f && f->type == CmdArg_Flag) {
    return f->b;
  }
  return 0;
}

void CmdArg_Free(CmdArg *arg) {
  switch (arg->type) {
    case CmdArg_String:
      rm_free(arg->s.str);
      break;
    case CmdArg_Object:
      for (size_t i = 0; i < arg->obj.len; i++) {
        CmdArg_Free(arg->obj.entries[i].v);
      }
      rm_free(arg->obj.entries);
      break;
    case CmdArg_Array:
      for (size_t i = 0; i < arg->a.len; i++) {
        CmdArg_Free(arg->a.args[i]);
      }
      rm_free(arg->a.args);
      break;
    default:
      break;
  }
  rm_free(arg);
}

static int CmdObj_Set(CmdObject *obj, const char *key, CmdArg *val, int unique) {

  // if we enforce uniqueness, fail on duplicate records
  if (unique) {
    for (size_t i = 0; i < obj->len; i++) {
      if (!strcasecmp(key, obj->entries[i].k)) {
        return CMDPARSE_ERR;
      }
    }
  }

  if (obj->len + 1 > obj->cap) {
    obj->cap += obj->cap ? obj->cap : 2;
    obj->entries = realloc(obj->entries, obj->cap * sizeof(CmdKeyValue));
  }
  obj->entries[obj->len++] = (CmdKeyValue){.k = key, .v = val};
  return CMDPARSE_OK;
}

static int CmdArray_Append(CmdArray *arr, CmdArg *val) {

  if (arr->len == arr->cap) {
    arr->cap += arr->cap ? arr->cap : 2;
    arr->args = realloc(arr->args, arr->cap * sizeof(CmdArg *));
  }

  arr->args[arr->len++] = val;
  return CMDPARSE_OK;
}

static CmdSchemaElement *newSchemaElement(CmdSchemaElementType type) {
  CmdSchemaElement *ret = rm_calloc(1, sizeof(*ret));
  ret->type = type;
  ret->validator = NULL;
  ret->validatorCtx = NULL;
  return ret;
}

static CmdSchemaNode *NewSchemaNode(CmdSchemaNodeType type, const char *name,
                                    CmdSchemaElement *element, CmdSchemaFlags flags,
                                    const char *help) {
  CmdSchemaNode *ret = rm_malloc(sizeof(*ret));
  *ret = (CmdSchemaNode){
      .val = element,
      .flags = flags,
      .type = type,
      .name = name,
      .edges = NULL,
      .size = 0,
      .help = help,
  };

  return ret;
}

static int cmdSchema_addChild(CmdSchemaNode *parent, CmdSchemaNode *child) {
  // make sure we are not adding anything after a variadic vector
  if (parent->size > 0 && parent->edges[parent->size - 1]->val &&
      parent->edges[parent->size - 1]->val->type == CmdSchemaElement_Variadic) {
    return CMDPARSE_ERR;
  }
  parent->size++;
  parent->edges = realloc(parent->edges, parent->size * sizeof(CmdSchemaNode *));
  parent->edges[parent->size - 1] = child;
  return CMDPARSE_OK;
}

int cmdSchema_genericAdd(CmdSchemaNode *s, CmdSchemaNodeType type, const char *param,
                         CmdSchemaElement *elem, CmdSchemaFlags flags, const char *help) {
  if (s->type != CmdSchemaNode_Schema) {
    return CMDPARSE_ERR;
  }

  return cmdSchema_addChild(s, NewSchemaNode(type, param, elem, flags, help));
}

int CmdSchema_AddNamed(CmdSchemaNode *s, const char *param, CmdSchemaElement *elem,
                       CmdSchemaFlags flags) {

  return cmdSchema_genericAdd(s, CmdSchemaNode_NamedArg, param, elem, flags, NULL);
}

int CmdSchema_AddPostional(CmdSchemaNode *s, const char *param, CmdSchemaElement *elem,
                           CmdSchemaFlags flags) {

  return cmdSchema_genericAdd(s, CmdSchemaNode_PositionalArg, param, elem, flags, NULL);
}

int CmdSchema_AddNamedWithHelp(CmdSchemaNode *s, const char *param, CmdSchemaElement *elem,
                               CmdSchemaFlags flags, const char *help) {

  return cmdSchema_genericAdd(s, CmdSchemaNode_NamedArg, param, elem, flags, help);
}

int CmdSchema_AddPostionalWithHelp(CmdSchemaNode *s, const char *param, CmdSchemaElement *elem,
                                   CmdSchemaFlags flags, const char *help) {

  return cmdSchema_genericAdd(s, CmdSchemaNode_PositionalArg, param, elem, flags, help);
}

CmdSchemaElement *CmdSchema_Validate(CmdSchemaElement *e, CmdArgValidatorFunc f, void *privdata) {
  e->validator = f;
  e->validatorCtx = privdata;
  return e;
}

CmdSchemaElement *CmdSchema_NewTuple(const char *fmt, const char **names) {
  CmdSchemaElement *ret = newSchemaElement(CmdSchemaElement_Tuple);
  ret->tup.fmt = fmt;
  ret->tup.names = names;
  return ret;
}

CmdSchemaElement *CmdSchema_NewArg(const char type) {
  CmdSchemaElement *ret = newSchemaElement(CmdSchemaElement_Arg);
  ret->arg.type = type;

  return ret;
}

CmdSchemaElement *CmdSchema_NewArgAnnotated(const char type, const char *name) {
  CmdSchemaElement *ret = CmdSchema_NewArg(type);
  ret->arg.name = name;
  return ret;
}

CmdSchemaElement *CmdSchema_NewVector(const char type) {
  CmdSchemaElement *ret = newSchemaElement(CmdSchemaElement_Vector);
  ret->vec.type = type;
  return ret;
}

CmdSchemaElement *CmdSchema_NewVariadicVector(const char *fmt) {
  CmdSchemaElement *ret = newSchemaElement(CmdSchemaElement_Variadic);
  ret->var.fmt = fmt;
  return ret;
}

CmdSchemaElement *CmdSchema_NewOption(int num, const char **opts) {
  CmdSchemaElement *ret = newSchemaElement(CmdSchemaElement_Option);
  ret->opt.num = num;
  ret->opt.opts = opts;
  return ret;
}
// CmdSchemaElement *CmdSchema_NewOption(int num, ...) {}
// CmdSchemaElement *NewFlag(int deflt);
// CmdSchemaElement *CmdSchema_NewVariadicVector(const char **fmt);

CmdSchemaNode *NewSchema(const char *name, const char *help) {
  CmdSchemaNode *ret = NewSchemaNode(CmdSchemaNode_Schema, name, NULL, 0, help);
  return ret;
}

int CmdSchema_AddFlag(CmdSchemaNode *parent, const char *name) {
  CmdSchemaNode *ret = NewSchemaNode(
      CmdSchemaNode_Flag, name, newSchemaElement(CmdSchemaElement_Flag), CmdSchema_Optional, NULL);
  cmdSchema_addChild(parent, ret);
  return CMDPARSE_OK;
}

int CmdSchema_AddFlagWithHelp(CmdSchemaNode *parent, const char *name, const char *help) {
  CmdSchemaNode *ret = NewSchemaNode(
      CmdSchemaNode_Flag, name, newSchemaElement(CmdSchemaElement_Flag), CmdSchema_Optional, help);
  cmdSchema_addChild(parent, ret);
  return CMDPARSE_OK;
}

CmdSchemaNode *CmdSchema_AddSubSchema(CmdSchemaNode *parent, const char *param, int flags,
                                      const char *help) {
  CmdSchemaNode *ret = NewSchemaNode(CmdSchemaNode_Schema, param, NULL, flags, help);

  parent->size++;
  parent->edges = realloc(parent->edges, parent->size * sizeof(CmdSchemaNode *));
  parent->edges[parent->size - 1] = ret;
  return ret;
}

const char *typeString(char t) {
  switch (t) {
    case 's':
      return "string";
    case 'l':
      return "integer";
    case 'd':
      return "double";
    default:
      return "INVALID TYPE";
  }
}

void CmdSchemaElement_Print(const char *name, CmdSchemaElement *e) {
  switch (e->type) {
    case CmdSchemaElement_Arg:
      printf("{%s:%s}", e->arg.name ? e->arg.name : name, typeString(e->arg.type));
      break;
    case CmdSchemaElement_Tuple: {
      for (int i = 0; i < strlen(e->tup.fmt); i++) {
        printf("{%s:%s} ", e->tup.names ? e->tup.names[i] : "arg", typeString(e->tup.fmt[i]));
      }
      break;
    }
    case CmdSchemaElement_Variadic: {
      for (int i = 0; i < strlen(e->var.fmt); i++) {
        printf("{%s} ", typeString(e->var.fmt[i]));
      }
      printf("...");
      break;
    }
    case CmdSchemaElement_Vector:
      printf("{nargs:integer} {%s} ...", typeString(e->vec.type));
      break;
    case CmdSchemaElement_Flag:
      printf("{%s}", name);
      break;
    case CmdSchemaElement_Option: {
      for (int i = 0; i < e->opt.num; i++) {
        printf("%s", e->opt.opts[i]);
        if (i < e->opt.num - 1) printf("|");
      }
      break;
    }
  }
}

void CmdSchemaNode_Print(CmdSchemaNode *n, int depth) {

  pad(depth);
  if (n->flags & CmdSchema_Optional) {
    putchar('[');
  }
  switch (n->type) {
    case CmdSchemaNode_NamedArg:
      printf("%s ", n->name);
      CmdSchemaElement_Print(n->name, n->val);

      break;
    case CmdSchemaNode_PositionalArg:

      CmdSchemaElement_Print(n->name, n->val);

      break;
    case CmdSchemaNode_Schema:
      printf("%s\n", n->name);

      for (int i = 0; i < n->size; i++) {
        CmdSchemaNode_Print(n->edges[i], depth + 2);
      }

      pad(depth);
      break;

    case CmdSchemaNode_Flag:
      printf("%s", n->name);
      break;
  }

  if (n->flags & CmdSchema_Optional) {
    putchar(']');
  }
  if (n->flags & CmdSchema_Repeating) {

    printf(" ... ");
  }

  if (n->help) {

    printf(" (%s)", n->help);
  }
  putchar('\n');
}

void CmdSchema_Print(CmdSchemaNode *n) {
  CmdSchemaNode_Print(n, 0);
}

static int CmdSchemaNode_Match(CmdSchemaNode *n, CmdString *token) {
  switch (n->type) {
    case CmdSchemaNode_NamedArg:
    case CmdSchemaNode_Schema:
    case CmdSchemaNode_Flag:
      return CmdString_CaseEquals(token, n->name);
    case CmdSchemaNode_PositionalArg: {
      // Try to match optional positional args
      if (n->val && n->val->type == CmdSchemaElement_Option) {
        for (int i = 0; i < n->val->opt.num; i++) {
          if (CmdString_CaseEquals(token, n->val->opt.opts[i])) return 1;
        }
        return 0;
      }

      // all other positional args match
      return 1;
    }
  }
  return 0;
}

void CmdSchemaNode_Free(CmdSchemaNode *n) {
  if (n->type == CmdSchemaNode_Schema) {
    for (int i = 0; i < n->size; i++) {
      CmdSchemaNode_Free(n->edges[i]);
    }
    rm_free(n->edges);
  }
  rm_free(n->val);
  rm_free(n);
}
typedef enum {
  CmdParser_New = 0x00,
  CmdParser_Visited = 0x01,
  CmdParser_Blocked = 0x02,
} CmdParserStateFlags;

typedef struct CmdParserCtx {
  CmdSchemaNode *node;
  CmdParserStateFlags *edgeFlags;
} CmdParserCtx;

static int parseInt(const char *arg, long long *val) {

  char *e = NULL;
  *val = strtoll(arg, &e, 10);
  errno = 0;
  if ((errno == ERANGE && (*val == LONG_MAX || *val == LONG_MIN)) || (errno != 0 && *val == 0) ||
      *e != '\0') {
    *val = -1;
    return 0;
  }

  return 1;
}

static int parseDouble(const char *arg, double *d) {
  char *e;
  *d = strtod(arg, &e);
  errno = 0;

  if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0) ||
      *e != '\0') {
    return 0;
  }

  return 1;
}

int typedParse(CmdArg **node, CmdString *arg, char type, char **err) {
  switch (type) {
    case 's':
      *node = NewCmdString(arg->str, arg->len);
      break;
    case 'l': {
      long long i;
      if (!parseInt(arg->str, &i)) {
        __ignore__(asprintf(err, "Could not parse int value '%s'", arg->str));
        return CMDPARSE_ERR;
      }
      *node = NewCmdInteger(i);
      break;
    }

    case 'd': {
      double d;
      if (!parseDouble(arg->str, &d)) {
        __ignore__(asprintf(err, "Could not parse double value '%s'", arg->str));
        return CMDPARSE_ERR;
      }
      *node = NewCmdDouble(d);
      break;
    }
    default:
      __ignore__(asprintf(err, "Invalid type specifier '%c'", type));
      return CMDPARSE_ERR;
  }
  return CMDPARSE_OK;
}

int CmdArg_ParseDouble(CmdArg *arg, double *d) {
  if (!arg) return 0;
  switch (arg->type) {
    case CmdArg_Double:
      *d = arg->d;
      return 1;
    case CmdArg_Integer:
      *d = (double)arg->i;
    case CmdArg_String:
      return parseDouble(arg->s.str, d);
    default:
      return 0;
  }
}
int CmdArg_ParseInt(CmdArg *arg, int64_t *i) {
  if (!arg) return 0;
  switch (arg->type) {
    case CmdArg_Double:
      *i = round(arg->d);
      return 1;
    case CmdArg_Integer:
      *i = arg->i;
    case CmdArg_String: {
      long long ii;
      int rc = parseInt(arg->s.str, &ii);
      if (rc) *i = ii;
      return rc;
    }

    default:
      return 0;
  }
}

#define CMDPARSE_CHECK_POS(pos, argc, err, msg)            \
  {                                                        \
    if (pos >= argc) {                                     \
      *err = strdup(msg ? msg : "Insufficient Arguments"); \
      return CMDPARSE_ERR;                                 \
    }                                                      \
  }

static int parseArg(CmdSchemaArg *arg, CmdArg **current, CmdString *argv, int argc, int *pos,
                    char **err) {

  CMDPARSE_CHECK_POS(*pos, argc, err, NULL);
  int rc = typedParse(current, &argv[*pos], arg->type, err);
  if (rc == CMDPARSE_OK) (*pos)++;
  return rc;
}

static int parseTuple(CmdSchemaTuple *tup, CmdArg **current, CmdString *argv, int argc, int *pos,
                      char **err) {
  size_t len = strlen(tup->fmt);
  CMDPARSE_CHECK_POS(*pos + len - 1, argc, err, "Tuple length out of range");
  CmdArg *t = NewCmdArray(len);

  for (size_t i = 0; i < len; i++) {
    CmdArg *arg = NULL;
    if (CMDPARSE_ERR == typedParse(&arg, &argv[*pos], tup->fmt[i], err)) {
      CmdArg_Free(t);
      return CMDPARSE_ERR;
    }
    CmdArray_Append(&t->a, arg);
    (*pos)++;
  }
  *current = t;

  return CMDPARSE_OK;
}

static int parseVector(CmdSchemaVector *vec, CmdArg **current, CmdString *argv, int argc, int *pos,
                       char **err) {

  CMDPARSE_CHECK_POS(*pos, argc, err, "Vector length out of range");
  long long vlen = 0;
  if (!parseInt(argv[*pos].str, &vlen)) {
    __ignore__(asprintf(err, "Invalid vector length token '%s'", argv[*pos].str));
    return CMDPARSE_ERR;
  }

  if (vlen < 0 || *pos + vlen >= argc) {
    __ignore__(asprintf(err, "Invalid or out of range vector length: %lld", vlen));
    return CMDPARSE_ERR;
  }

  (*pos)++;

  CmdArg *t = NewCmdArray(vlen);

  for (size_t i = 0; i < vlen; i++) {
    CmdArg *n;
    if (CMDPARSE_ERR == typedParse(&n, &argv[*pos], vec->type, err)) {
      CmdArg_Free(t);
      return CMDPARSE_ERR;
    }
    CmdArray_Append(&t->a, n);

    (*pos)++;
  }
  *current = t;

  return CMDPARSE_OK;
}
static int parseVariadicVector(CmdSchemaVariadic *var, CmdArg **current, CmdString *argv, int argc,
                               int *pos, char **err) {

  CMDPARSE_CHECK_POS(*pos, argc, err, NULL);

  int len = strlen(var->fmt);
  CmdArg *t = NewCmdArray((1 + argc - *pos) / len);
  while (*pos + len <= argc) {

    CmdArg *elem = len > 1 ? NewCmdArray(len) : NULL;
    for (int i = 0; i < len && *pos < argc; i++) {
      CmdArg *n;
      if (CMDPARSE_ERR == typedParse(&n, &argv[*pos], var->fmt[i], err)) {
        if (elem) CmdArg_Free(elem);
        CmdArg_Free(t);
        return CMDPARSE_ERR;
      }
      CmdArray_Append(elem ? &elem->a : &t->a, n);

      (*pos)++;
    }
    if (elem) {
      CmdArray_Append(&t->a, elem);
    }
  }
  *current = t;

  return CMDPARSE_OK;
}

static int processFlag(int flagVal, CmdArg **current, CmdString *argv, int argc, int *pos,
                       char **err) {
  CMDPARSE_CHECK_POS(*pos, argc, err, NULL);
  (*pos)++;
  *current = NewCmdFlag(flagVal);

  return CMDPARSE_OK;
}

static int processOption(CmdSchemaOption *opt, CmdArg **current, CmdString *argv, int argc,
                         int *pos, char **err) {
  CMDPARSE_CHECK_POS(*pos, argc, err, NULL);

  *current = NewCmdString(argv[*pos].str, argv[*pos].len);
  (*pos)++;

  return CMDPARSE_OK;
}

static int cmdParser_ProcessElement(CmdSchemaElement *elem, CmdArg **out, CmdString *argv, int argc,
                                    int *pos, char **err) {
  int rc = CMDPARSE_ERR;
  switch (elem->type) {
    case CmdSchemaElement_Arg:
      rc = parseArg(&elem->arg, out, argv, argc, pos, err);
      break;
    case CmdSchemaElement_Tuple:
      rc = parseTuple(&elem->tup, out, argv, argc, pos, err);
      break;
    case CmdSchemaElement_Vector:
      rc = parseVector(&elem->vec, out, argv, argc, pos, err);
      break;
    case CmdSchemaElement_Flag:
      rc = processFlag(1, out, argv, argc, pos, err);
      break;
    case CmdSchemaElement_Option:
      rc = processOption(&elem->opt, out, argv, argc, pos, err);
      break;
    case CmdSchemaElement_Variadic:
      rc = parseVariadicVector(&elem->var, out, argv, argc, pos, err);
      break;
  }

  if (rc == CMDPARSE_ERR) return rc;
  // if needed - validate the element
  if (elem->validator) {
    if (!elem->validator(*out, elem->validatorCtx)) {
      __ignore__(asprintf(err, "Validation failed at offset %d near '%s'", *pos, argv[*pos - 1].str));
      return CMDPARSE_ERR;
    }
  }
  return CMDPARSE_OK;
}

static int cmdArg_AddChild(CmdArg *parent, const char *name, CmdArg *child, char **err) {

  switch (parent->type) {
    case CmdArg_Object:
      return CmdObj_Set(&parent->obj, name, child, 0);

    case CmdArg_Array:
      return CmdArray_Append(&parent->a, child);

    default: {
      __ignore__(asprintf(err, "Cannot add child to node of type %d", parent->type));
      return CMDPARSE_ERR;
    }
  }
  return CMDPARSE_OK;
}

static int cmdParser_Parse(CmdSchemaNode *node, CmdArg **parent, CmdString *argv, int argc,
                           int *pos, char **err, int depth) {

  // this is the root schema

  // skipe the node name if it's named
  if (node->type == CmdSchemaNode_NamedArg || node->type == CmdSchemaNode_Schema) {
    (*pos)++;
  }

  CmdArg *current = NULL;
  if (node->val) {

    // Parse the node value. This should consume tokens from the input array
    if (CMDPARSE_ERR == cmdParser_ProcessElement(node->val, &current, argv, argc, pos, err)) {
      if (current) CmdArg_Free(current);

      return CMDPARSE_ERR;
    }
    // Add the current node to the parent
    if (current) {
      if (CMDPARSE_ERR == cmdArg_AddChild(*parent, node->name, current, err)) {
        if (current) CmdArg_Free(current);

        return CMDPARSE_ERR;
      }
    }
  }

  if (node->type == CmdSchemaNode_Schema) {
    if (*parent) {
      current = NewCmdObject(1);

      // for sub-schemas - we append the schema to
      if (CMDPARSE_ERR == cmdArg_AddChild(*parent, node->name, current, err)) {
        CmdArg_Free(current);
        return CMDPARSE_ERR;
      }

    } else {
      // if this is the first layer of parsing - start from current and set parent to it
      *parent = NewCmdObject(1);
      current = *parent;
    }
  }

  // continue to parse any remaining transitional states until we consume the entire input array
  CmdParserStateFlags sf[node->size];
  int minEdge = 0;
  memset(sf, 0, sizeof(sf));

  while (*pos < argc) {
    // scan the next token
    CmdString *tok = &argv[*pos];
    int found = 0;
    // find the first appropriate matching node
    for (int i = minEdge; i < node->size; i++) {

      // skip nodes we can't process anymore
      if (sf[i] & CmdParser_Blocked) continue;

      CmdSchemaNode *edge = node->edges[i];

      if (CmdSchemaNode_Match(edge, tok)) {
        found = 1;
        // if parsing failed - just return immediately
        if (CMDPARSE_ERR == cmdParser_Parse(edge, &current, argv, argc, pos, err, depth + 2)) {
          return CMDPARSE_ERR;
        }
        // mark the node as visited
        sf[i] |= CmdParser_Visited;

        // if we got to a non repeating edge, make sure we do not enter it again
        if (!(edge->flags & CmdSchema_Repeating)) {
          sf[i] |= CmdParser_Blocked;
        }
        // If we just consumed a positional arg, we can't look for the next state behind it
        if (edge->type == CmdSchemaNode_PositionalArg) {
          minEdge = i + 1;
        }

        // continue scanning from the first valid position again
        break;
      }
    }

    if (!found) {
      goto end;
    }
  }

end:
  // check that all the required nodes have been visited!
  for (int i = 0; i < node->size; i++) {
    CmdSchemaNode *edge = node->edges[i];
    if (edge->flags & CmdSchema_Required && !(sf[i] & CmdParser_Visited)) {
      // set an error indicating the first missed required argument
      __ignore__(asprintf(err, "Missing required argument '%s' in '%s'", node->edges[i]->name, node->name));
      return CMDPARSE_ERR;
    }

    // find unvisited flags and "pseudo visit" them as value 0
    if (edge->type == CmdSchemaNode_Flag && !(sf[i] & CmdParser_Visited)) {
      if (CMDPARSE_ERR == cmdArg_AddChild(current, edge->name, NewCmdFlag(0), err)) {
        return CMDPARSE_ERR;
      }
    }
  }

  // all is okay!
  return CMDPARSE_OK;
}

int CmdParser_ParseCmd(CmdSchemaNode *schema, CmdArg **arg, CmdString *argv, int argc, char **err,
                       int strict) {
  int pos = 0;
  *arg = NULL;
  *err = NULL;
  if (CMDPARSE_ERR == cmdParser_Parse(schema, arg, argv, argc, &pos, err, 0)) {
    if (*arg) CmdArg_Free(*arg);
    *arg = NULL;
    return CMDPARSE_ERR;
  }
  if (strict && pos < argc) {
    __ignore__(asprintf(err, "Extra arguments not parsed. Only %d of %d args parsed", pos, argc));
    if (*arg) CmdArg_Free(*arg);
    *arg = NULL;
    return CMDPARSE_ERR;
  }
  return CMDPARSE_OK;
}

int CmdParser_ParseRedisModuleCmd(CmdSchemaNode *schema, CmdArg **cmd, RedisModuleString **argv,
                                  int argc, char **err, int strict) {
  CmdString *args = rm_calloc(argc, sizeof(CmdString));
  for (int i = 0; i < argc; i++) {
    size_t len;
    const char *arg = RedisModule_StringPtrLen(argv[i], &len);
    args[i] = (CmdString){.str = (char *)arg, .len = len};
  }

  int rc = CmdParser_ParseCmd(schema, cmd, args, argc, err, strict);
  rm_free(args);
  return rc;
}

CmdString *CmdParser_NewArgListV(size_t size, ...) {

  va_list ap;

  va_start(ap, size);
  CmdString *ret = rm_calloc(size, sizeof(CmdString));
  for (size_t i = 0; i < size; i++) {
    const char *arg = va_arg(ap, const char *);
    ret[i] = CMD_STRING((char *)arg);
  }

  va_end(ap);
  return ret;
}

CmdString *CmdParser_NewArgListC(const char **argv, size_t argc) {

  CmdString *ret = rm_calloc(argc, sizeof(CmdString));
  for (size_t i = 0; i < argc; i++) {
    ret[i] = CMD_STRING((char *)argv[i]);
  }
  return ret;
}

CmdArgIterator CmdArg_Select(CmdArg *arg, const char *key) {
  return (CmdArgIterator){
      .arg = arg,
      .key = key,
      .pos = 0,
  };
}

CmdArgIterator CmdArg_Children(CmdArg *arg) {
  return (CmdArgIterator){
      .arg = arg,
      .key = NULL,
      .pos = 0,
  };
}

CmdArg *CmdArgIterator_Next(CmdArgIterator *it, const char **key) {

  switch (it->arg->type) {
    case CmdArg_Object: {
      CmdObject *obj = &it->arg->obj;
      while (it->pos < obj->len) {
        if (it->key == NULL || !strcasecmp(it->key, obj->entries[it->pos].k)) {
          if (key) {
            *key = obj->entries[it->pos].k;
          }
          return obj->entries[it->pos++].v;
        }
        it->pos++;
      }
      break;
    }
    case CmdArg_Array: {
      CmdArray *arr = &it->arg->a;
      if (it->pos < arr->len) {
        if (key) {
          *key = NULL;
        }
        return arr->args[it->pos++];
      }
      break;
    }
    default:
      break;
  }

  return NULL;
}

CmdArg *CmdArg_FirstOf(CmdArg *arg, const char *key) {
  if (arg->type != CmdArg_Object) return NULL;
  for (size_t i = 0; i < arg->obj.len; i++) {
    if (!strcasecmp(key, arg->obj.entries[i].k)) {
      return arg->obj.entries[i].v;
    }
  }
  return NULL;
}

/* count the number of children of an object that correspond to a specific key */
size_t CmdArg_Count(CmdArg *arg, const char *key) {
  if (arg->type != CmdArg_Object) return 0;
  size_t ret = 0;
  for (size_t i = 0; i < arg->obj.len; i++) {
    if (!strcasecmp(key, arg->obj.entries[i].k)) {
      ++ret;
    }
  }
  return ret;
}

size_t CmdArg_NumChildren(CmdArg *arg) {
  if (arg->type == CmdArg_Array) {
    return arg->a.len;
  } else if (arg->type == CmdArg_Object) {
    return arg->obj.len;
  } else {
    return 0;
  }
}

/*
 *  - s: will be parsed as a string
 *  - l: Will be parsed as a long integer
 *  - d: Will be parsed as a double
 *  - !: will be skipped
 *  - ?: means evrything after is optional
 */

int CmdArg_ArrayAssign(CmdArray *arg, const char *fmt, ...) {

  va_list ap;
  va_start(ap, fmt);
  const char *p = fmt;
  size_t i = 0;
  int optional = 0;
  while (i < arg->len && *p) {
    switch (*p) {
      case 's': {
        char **ptr = va_arg(ap, char **);
        if (arg->args[i]->type != CmdArg_String) {
          goto err;
        }
        *ptr = CMDARG_STRPTR(arg->args[i]);
        break;
      }
      case 'l': {
        long long *lp = va_arg(ap, long long *);
        if (arg->args[i]->type != CmdArg_Integer) {
          goto err;
        }
        *lp = CMDARG_INT(arg->args[i]);
        break;
      }
      case 'd': {
        double *dp = va_arg(ap, double *);
        if (arg->args[i]->type != CmdArg_Double) {
          goto err;
        }
        *dp = CMDARG_DOUBLE(arg->args[i]);
        break;
      }
      case '!':
        // do nothing...
        break;
      case '?':
        optional = 1;
        // reduce i because it will be incremented soon
        i -= 1;
        break;
      default:
        goto err;
    }
    ++i;
    ++p;
  }
  // if we have stuff left to read in the format but we haven't gotten to the optional part -fail
  if (*p && !optional && i < arg->len) {
    printf("we have stuff left to read in the format but we haven't gotten to the optional part\n");
    goto err;
  }
  // if we don't have anything left to read from the format but we haven't gotten to the array's
  // end, fail
  if (*p == 0 && i < arg->len) {
    printf("we haven't read all the arguments\n");

    goto err;
  }

  va_end(ap);
  return CMDPARSE_OK;
err:
  va_end(ap);
  return CMDPARSE_ERR;
}