#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <sys/errno.h>
#include <ctype.h>
#include <math.h>

#include "cmdparse.h"

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
  }
}
static inline CmdArg *NewCmdArg(CmdArgType t) {
  CmdArg *ret = malloc(sizeof(CmdArg));

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
  ret->a.args = calloc(cap, sizeof(CmdArg *));

  return ret;
}

static CmdArg *NewCmdObject(size_t cap) {
  CmdArg *ret = NewCmdArg(CmdArg_Object);
  ret->obj = (CmdObject){
      .entries = calloc(cap, sizeof(CmdKeyValue)),
      .cap = cap,
      .len = 0,
  };

  return ret;
}

void CmdArg_Free(CmdArg *arg) {
  switch (arg->type) {
    case CmdArg_String:
      free(arg->s.str);
      break;
    case CmdArg_Object:
      for (size_t i = 0; i < arg->obj.len; i++) {
        CmdArg_Free(arg->obj.entries[i].v);
      }
      free(arg->obj.entries);
      break;
    case CmdArg_Array:
      for (size_t i = 0; i < arg->a.len; i++) {
        CmdArg_Free(arg->a.args[i]);
      }
      free(arg->a.args);
      break;
    default:
      break;
  }
  free(arg);
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
  CmdSchemaElement *ret = calloc(1, sizeof(*ret));
  ret->type = type;

  return ret;
}

static CmdSchemaNode *NewSchemaNode(CmdSchemaNodeType type, const char *name,
                                    CmdSchemaElement *element, CmdSchemaFlags flags,
                                    const char *help) {
  CmdSchemaNode *ret = malloc(sizeof(*ret));
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

static inline void cmdSchema_addChild(CmdSchemaNode *parent, CmdSchemaNode *child) {
  parent->size++;
  parent->edges = realloc(parent->edges, parent->size * sizeof(CmdSchemaNode *));
  parent->edges[parent->size - 1] = child;
}

int cmdSchema_genericAdd(CmdSchemaNode *s, CmdSchemaNodeType type, const char *param,
                         CmdSchemaElement *elem, CmdSchemaFlags flags, const char *help) {
  if (s->type != CmdSchemaNode_Schema) {
    return CMDPARSE_ERR;
  }

  cmdSchema_addChild(s, NewSchemaNode(type, param, elem, flags, help));
  return CMDPARSE_OK;
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

CmdSchemaElement *CmdSchema_NewVector(const char type) {
  CmdSchemaElement *ret = newSchemaElement(CmdSchemaElement_Vector);
  ret->vec.type = type;
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

CmdSchemaNode *CmdSchema_AddFlag(CmdSchemaNode *parent, const char *name) {
  CmdSchemaNode *ret = NewSchemaNode(
      CmdSchemaNode_Flag, name, newSchemaElement(CmdSchemaElement_Flag), CmdSchema_Optional, NULL);
  cmdSchema_addChild(parent, ret);
  return ret;
}

CmdSchemaNode *CmdSchema_AddFlagWithHelp(CmdSchemaNode *parent, const char *name,
                                         const char *help) {
  CmdSchemaNode *ret = NewSchemaNode(
      CmdSchemaNode_Flag, name, newSchemaElement(CmdSchemaElement_Flag), CmdSchema_Optional, help);
  cmdSchema_addChild(parent, ret);
  return ret;
}

CmdSchemaNode *CmdSchema_NewSubSchema(CmdSchemaNode *parent, const char *param, int flags,
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
      printf("{%s:%s}", name, typeString(e->arg.type));
      break;
    case CmdSchemaElement_Tuple: {
      for (int i = 0; i < strlen(e->tup.fmt); i++) {
        printf("{%s:%s} ", e->tup.names ? e->tup.names[i] : "arg", typeString(e->tup.fmt[i]));
      }
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
      pad(depth) break;

    case CmdSchemaNode_Flag:
      printf("%s", n->name);
      break;
  }
  if (n->flags & CmdSchema_Optional) {
    putchar(']');
  }

  if (n->help) {

    printf(" (%s)", n->help);
  }
  putchar('\n');
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
  *val = strtoll(arg, NULL, 10);
  if ((errno == ERANGE && (*val == LONG_MAX || *val == LONG_MIN)) || (errno != 0 && *val == 0)) {
    *val = -1;
    return 0;
  }

  return 1;
}

static int parseDouble(const char *arg, double *d) {
  const char *p = NULL;

  *d = strtod(arg, NULL);
  if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0)) {
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
        asprintf(err, "Could not parse int value '%s'", arg->str);
        return CMDPARSE_ERR;
      }
      *node = NewCmdInteger(i);
      break;
    }

    case 'd': {
      double d;
      if (!parseDouble(arg->str, &d)) {
        asprintf(err, "Could not parse double value '%s'", arg->str);
        return CMDPARSE_ERR;
      }
      *node = NewCmdDouble(d);
      break;
    }
    default:
      return CMDPARSE_ERR;
  }
  return CMDPARSE_OK;
}

#define CMDPARSE_CHECK_POS(pos, argc, err)     \
  {                                            \
    if (pos >= argc) {                         \
      *err = strdup("Arguments out of range"); \
      return CMDPARSE_ERR;                     \
    }                                          \
  }

static int parseArg(CmdSchemaArg *arg, CmdArg **current, CmdString *argv, int argc, int *pos,
                    char **err) {

  CMDPARSE_CHECK_POS(*pos, argc, err);
  int rc = typedParse(current, &argv[*pos], arg->type, err);
  if (rc == CMDPARSE_OK) (*pos)++;
  return rc;
}

static int parseTuple(CmdSchemaTuple *tup, CmdArg **current, CmdString *argv, int argc, int *pos,
                      char **err) {
  size_t len = strlen(tup->fmt);

  CMDPARSE_CHECK_POS(*pos + len, argc, err);
  CmdArg *t = NewCmdArray(len);

  for (size_t i = 0; i < len; i++) {
    CmdArg *arg = NULL;
    if (CMDPARSE_ERR == typedParse(&arg, &argv[*pos], tup->fmt[i], err)) {
      // TODO: Free until here
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

  CMDPARSE_CHECK_POS(*pos, argc, err);
  long long vlen = 0;
  if (!parseInt(argv[*pos].str, &vlen)) {
    asprintf(err, "Invalid vector length token '%s'", argv[*pos].str);
    return CMDPARSE_ERR;
  }

  if (vlen < 0 || *pos + vlen >= argc) {
    asprintf(err, "Invalid or out of range vector length: %lld", vlen);
    return CMDPARSE_ERR;
  }

  (*pos)++;

  CmdArg *t = NewCmdArray(vlen);

  for (size_t i = 0; i < vlen; i++) {
    CmdArg *n;
    if (CMDPARSE_ERR == typedParse(&n, &argv[*pos], vec->type, err)) {
      // TODO: Free until here
      return CMDPARSE_ERR;
    }
    CmdArray_Append(&t->a, n);

    (*pos)++;
  }
  *current = t;

  return CMDPARSE_OK;
}

static int processFlag(int flagVal, CmdArg **current, CmdString *argv, int argc, int *pos,
                       char **err) {
  CMDPARSE_CHECK_POS(*pos, argc, err);
  (*pos)++;
  *current = NewCmdFlag(flagVal);

  return CMDPARSE_OK;
}

static int processOption(CmdSchemaOption *opt, CmdArg **current, CmdString *argv, int argc,
                         int *pos, char **err) {
  CMDPARSE_CHECK_POS(*pos, argc, err);

  *current = NewCmdString(argv[*pos].str, argv[*pos].len);
  (*pos)++;

  return CMDPARSE_OK;
}

static int cmdParser_ProcessElement(CmdSchemaElement *elem, CmdArg **out, CmdString *argv, int argc,
                                    int *pos, char **err) {
  switch (elem->type) {
    case CmdSchemaElement_Arg:
      return parseArg(&elem->arg, out, argv, argc, pos, err);
    case CmdSchemaElement_Tuple:
      return parseTuple(&elem->tup, out, argv, argc, pos, err);
    case CmdSchemaElement_Vector:
      return parseVector(&elem->vec, out, argv, argc, pos, err);
    case CmdSchemaElement_Flag:
      return processFlag(1, out, argv, argc, pos, err);
    case CmdSchemaElement_Option:
      return processOption(&elem->opt, out, argv, argc, pos, err);
      return CMDPARSE_ERR;
  }
}

static int cmdArg_AddChild(CmdArg *parent, const char *name, CmdArg *child, char **err) {

  switch (parent->type) {
    case CmdArg_Object:
      return CmdObj_Set(&parent->obj, name, child, 0);

    case CmdArg_Array:
      return CmdArray_Append(&parent->a, child);

    default: {
      asprintf(err, "Cannot add child to node of type %d", parent->type);
      return CMDPARSE_ERR;
    }
  }
  return CMDPARSE_OK;
}

static int cmdParser_Parse(CmdSchemaNode *node, CmdArg **parent, CmdString *argv, int argc,
                           int *pos, char **err, int depth) {

  // this is the root schema
  if (!*parent && node->type == CmdSchemaNode_Schema) {
    *parent = NewCmdObject(1);
  }
  // skipe the node name if it's named
  if (node->type == CmdSchemaNode_NamedArg || node->type == CmdSchemaNode_Schema) {
    (*pos)++;
  }

  CmdArg *current = NULL;
  if (node->val) {

    // Parse the node value. This should consume tokens from the input array
    if (CMDPARSE_ERR == cmdParser_ProcessElement(node->val, &current, argv, argc, pos, err)) {
      return CMDPARSE_ERR;
    }
    // Add the current node to the parent
    if (current) {
      if (CMDPARSE_ERR == cmdArg_AddChild(*parent, node->name, current, err)) {
        return CMDPARSE_ERR;
      }
    }
  }

  if (node->type == CmdSchemaNode_Schema) {
    current = NewCmdObject(1);

    // for sub-schemas - we append the schema to
    if (CMDPARSE_ERR == cmdArg_AddChild(*parent, node->name, current, err)) {
      return CMDPARSE_ERR;
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
      asprintf(err, "Missing required argument '%s'", node->edges[i]->name);
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
  CmdArg *cmd = NULL;
  int pos = 0;
  *arg = NULL;
  if (CMDPARSE_ERR == cmdParser_Parse(schema, arg, argv, argc, &pos, err, 0)) {
    return CMDPARSE_ERR;
  }
  if (strict && pos < argc) {
    asprintf(err, "Extra arguments not parsed. Only %d of %d args parsed", pos, argc);
    if (*arg) CmdArg_Free(*arg);
    return CMDPARSE_ERR;
  }
  return CMDPARSE_OK;
}

int main() {

  CmdSchemaNode *root = NewSchema("FOO", "Just a test command");
  CmdSchema_AddPostional(root, "term", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddFlag(root, "NX");
  CmdSchema_AddFlag(root, "XX");
  CmdSchema_AddNamedWithHelp(root, "BAR", CmdSchema_NewArg('s'), CmdSchema_Required,
                             "The Command's BAR");
  CmdSchema_AddNamed(root, "XXX", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddNamed(root, "LIMIT", CmdSchema_NewTuple("ll", (const char *[]){"FIRST", "LIMIT"}),
                     CmdSchema_Optional);
  CmdSchema_AddNamed(root, "ARGS", CmdSchema_NewVector('s'), CmdSchema_Optional);

  CmdSchemaNode *sub = CmdSchema_NewSubSchema(root, "SUB", CmdSchema_Optional, "Sub Command");
  CmdSchema_AddNamed(sub, "MARINE", CmdSchema_NewArg('s'),
                     CmdSchema_Required | CmdSchema_Repeating);
  CmdSchema_AddFlag(sub, "YELLO");
  CmdSchema_AddPostional(root, "xmode", CmdSchema_NewOption(3, (const char *[]){"FX", "YX", "JX"}),
                         CmdSchema_Optional);
  CmdSchemaNode_Print(root, 0);

  CmdString args[] = {
      CMD_STRING("FOO"),   CMD_STRING("wat wat"), CMD_STRING("NX"),    CMD_STRING("XX"),
      CMD_STRING("BAR"),   CMD_STRING("hello"),   CMD_STRING("XXX"),   CMD_STRING("world"),
      CMD_STRING("LIMIT"), CMD_STRING("0"),       CMD_STRING("10"),    CMD_STRING("ARGS"),
      CMD_STRING("3"),     CMD_STRING("foo"),     CMD_STRING("bar"),   CMD_STRING("baz"),
      CMD_STRING("SUB"),   CMD_STRING("MARINE"),  CMD_STRING("yello"), CMD_STRING("MARINE"),
      CMD_STRING("blue"),  CMD_STRING("YELLO"),   CMD_STRING("JX"),    CMD_STRING("dfgsdfgsd")};
  char *err = NULL;

  CmdArg *cmd;

  int rc = CmdParser_ParseCmd(root, &cmd, args, sizeof(args) / sizeof(*args), &err, 0);
  if (rc == CMDPARSE_ERR) {
    printf("Failed parsing: %s\n", err);
  } else {
    CmdArg_Print(cmd, 0);
  }
  return 0;
}
