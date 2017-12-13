#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <sys/errno.h>
#include <ctype.h>
#include <math.h>

/*

  PROJECT floor 1 age

*/

#define CMDPARSE_OK 0
#define CMDPARSE_ERR 1

typedef enum {
  CmdArg_Integer,
  CmdArg_Double,
  CmdArg_String,
  CmdArg_Tuple,
  CmdArg_Vector,
  CmdArg_Object,
  CmdArg_Flag,
} CmdNodeType;

struct cmdnode;

typedef struct {
  const char *k;
  struct cmdnode *v;
} CmdKeyValue;

typedef struct {
  size_t len;
  size_t cap;
  CmdKeyValue *entries;
} CmdObject;

typedef struct {
  size_t len;
  size_t cap;
  struct cmdnode **args;
} CmdArray;

typedef struct {
  char *str;
  size_t len;
} CmdString;

// Variant value union
typedef struct cmdnode {
  union {
    // numeric value
    double d;
    int64_t i;
    // string value
    CmdString s;

    // array value
    CmdArray a;

    CmdObject obj;
  };
  CmdNodeType type;
} CmdNode;

void CmdNode_Printf(CmdNode *n, int depth) {}
static inline CmdNode *NewCommandNode(CmdNodeType t) {
  CmdNode *ret = malloc(sizeof(CmdNode));

  ret->type = t;
  return ret;
}

CmdNode *NewCmdString(const char *s) {
  CmdNode *ret = NewCommandNode(CmdArg_String);
  ret->s = (CmdString){.str = strdup(s), .len = strlen(s)};
  return ret;
}

CmdNode *NewCmdInteger(long long i) {
  CmdNode *ret = NewCommandNode(CmdArg_Integer);
  ret->i = i;
  return ret;
}

CmdNode *NewCmdDouble(double d) {
  CmdNode *ret = NewCommandNode(CmdArg_Double);
  ret->d = d;
  return ret;
}

CmdNode *NewCmdTuple(size_t len) {
  CmdNode *ret = NewCommandNode(CmdArg_Tuple);
  ret->a.cap = len;
  ret->a.len = len;
  ret->a.args = calloc(len, sizeof(*ret->a.args));
  
  return ret;
}


CmdNode *NewCmdObject(size_t cap) {
  CmdNode *ret = NewCommandNode(CmdArg_Object);
  ret->obj = (CmdObject){
      .entries = calloc(cap, sizeof(CmdKeyValue)),
      .cap = cap,
      .len = 0,
  };

  return ret;
}

CmdNode *NewCmdVector(size_t cap) {
  CmdNode *ret = NewCommandNode(CmdArg_Vector);
  ret->a = (CmdArray){
      .args = calloc(cap, sizeof(CmdNode *)),
      .cap = cap,
      .len = 0,
  };

  return ret;
}

int CmdObj_Set(CmdObject *obj, const char *key, CmdNode *val) {

  for (size_t i = 0; i < obj->len; i++) {
    if (!strcmp(key, obj->entries[i].k)) {
      // TODO: free memory
      obj->entries[i].v = val;
      return CMDPARSE_OK;
    }
  }
  if (obj->len + 1 >= obj->cap) {
    obj->cap *= 2;
    obj->entries = realloc(obj->entries, obj->cap * sizeof(*obj->entries));
  }
  obj->entries[obj->len++] = (CmdKeyValue){.k = key, .v = val};
  return CMDPARSE_OK;
}
int CmdVector_Append(CmdArray *arr, CmdNode *val) {

  if (arr->len == arr->cap) {
    return CMDPARSE_ERR;
  }
  arr->args[arr->len++] = val;
  return CMDPARSE_OK;
}

typedef struct CmdSchema {
  const char *name;
} CmdSchema;
typedef struct CmdSchemaArg {
  char type;
} CmdSchemaArg;

typedef struct CmdSchemaFlag {
  int deflt;
} CmdSchemaFlag;
typedef struct CmdSchemaOption {
  int num;
  const char **opts;
} CmdSchemaOption;
typedef struct CmdSchemaUnion {

} CmdSchemaUnion;

typedef struct CmdSchemaTuple {
  const char *fmt;
  const char **names;
} CmdSchemaTuple;
typedef struct CmdSchemaVector {
  char type;
} CmdSchemaVector;

typedef enum {
  CmdSchemaElement_Arg,
  CmdSchemaElement_Tuple,
  CmdSchemaElement_Vector,
  CmdSchemaElement_Flag,
  CmdSchemaElement_Option,
  CmdSchemaElement_Union,
} CmdSchemaElementType;

typedef struct {
  union {
    CmdSchemaArg arg;
    CmdSchemaTuple tup;
    CmdSchemaVector vec;
    CmdSchemaFlag flag;
    CmdSchemaOption opt;
    CmdSchemaUnion uni;
  };
  CmdSchemaElementType type;
} CmdSchemaElement;

static CmdSchemaElement *newSchemaElement(CmdSchemaElementType type) {
  CmdSchemaElement *ret = calloc(1, sizeof(*ret));
  ret->type = type;

  return ret;
}

typedef enum {
  CmdSchema_Required = 0x01,
  CmdSchema_Optional = 0x02,
  CmdSchema_Repeating = 0x04,
} CmdSchemaFlags;

typedef enum {
  CmdSchemaNode_Schema,
  CmdSchemaNode_Union,
  CmdSchemaNode_PositionalArg,
  CmdSchemaNode_NamedArg,

} CmdSchemaNodeType;

typedef struct CmdSchemaNode {
  CmdSchemaElement *val;
  CmdSchemaFlags flags;
  CmdSchemaNodeType type;
  const char *name;
  struct CmdSchemaNode **edges;
  int size;
} CmdSchemaNode;

CmdSchemaNode *NewSchemaNode(CmdSchemaNodeType type, const char *name, CmdSchemaElement *element,
                             CmdSchemaFlags flags) {
  CmdSchemaNode *ret = malloc(sizeof(*ret));
  *ret = (CmdSchemaNode){
      .val = element,
      .flags = flags,
      .type = type,
      .name = name,
      .edges = NULL,
      .size = 0,
  };

  return ret;
}

int cmdSchema_genericAdd(CmdSchemaNode *s, CmdSchemaNodeType type, const char *param,
                         CmdSchemaElement *elem, CmdSchemaFlags flags) {
  if (s->type != CmdSchemaNode_Schema) {
    return CMDPARSE_ERR;
  }

  s->size++;
  s->edges = realloc(s->edges, s->size * sizeof(CmdSchemaNode *));
  s->edges[s->size - 1] = NewSchemaNode(type, param, elem, flags);
  return CMDPARSE_OK;
}

int CmdSchema_AddNamed(CmdSchemaNode *s, const char *param, CmdSchemaElement *elem,
                       CmdSchemaFlags flags) {

  return cmdSchema_genericAdd(s, CmdSchemaNode_NamedArg, param, elem, flags);
}

int CmdSchema_AddPostional(CmdSchemaNode *s, const char *param, CmdSchemaElement *elem,
                           CmdSchemaFlags flags) {

  return cmdSchema_genericAdd(s, CmdSchemaNode_PositionalArg, param, elem, flags);
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
// CmdSchemaElement *CmdSchema_NewOption(int num, ...) {}
// CmdSchemaElement *NewFlag(int deflt);
// CmdSchemaElement *CmdSchema_NewVariadicVector(const char **fmt);
CmdSchemaNode *NewSchema(const char *name) {
  CmdSchemaNode *ret = NewSchemaNode(CmdSchemaNode_Schema, name, NULL, 0);
  return ret;
}

CmdSchemaNode *CmdSchema_NewSubSchema(CmdSchemaNode *parent, const char *param, int flags) {
  CmdSchemaNode *ret = NewSchemaNode(CmdSchemaNode_Schema, param, NULL, flags);

  parent->size++;
  parent->edges = realloc(parent->edges, parent->size * sizeof(CmdSchemaNode *));
  parent->edges[parent->size - 1] = ret;
  return ret;
}

#define pad(depth)                                    \
  {                                                   \
    for (int pd = 0; pd < depth; pd++) putchar('\t'); \
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
    case CmdSchemaElement_Option:
    case CmdSchemaElement_Union:
      printf("laterz");
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
      printf("%s \n", n->name);

      for (int i = 0; i < n->size; i++) {
        CmdSchemaNode_Print(n->edges[i], depth + 1);
      }

      break;

    case CmdSchemaNode_Union:

      for (int i = 0; i < n->size; i++) {
        CmdSchemaNode_Print(n->edges[i], depth + 1);
      }
      pad(depth);
      // printf("}\n");
      break;
  }
  if (n->flags & CmdSchema_Optional) {
    putchar(']');
  }
  putchar('\n');
}

int CmdSchemaNode_Match(CmdSchemaNode *n, const char *token) {
  switch (n->type) {
    case CmdSchemaNode_NamedArg:
    case CmdSchemaNode_Schema:
      return !(strcasecmp(n->name, token));
    case CmdSchemaNode_PositionalArg:
    case CmdSchemaNode_Union:
      return 1;
  }
}

typedef enum {
  CmdParser_New = 0x00,
  CmdParser_Visited = 0x01,
  CmdParser_Blocked = 0x02,
} CmdParserStateFlags;

typedef struct {
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

int typedParse(CmdNode **node, const char *arg, char type, char **err) {
    switch (type) {
    case 's':
      *node = NewCmdString(arg);
      break;
    case 'l': {
      long long i;
      if (!parseInt(arg, &i)) {
        asprintf(err, "Could not parse int value '%s'",arg);
        return CMDPARSE_ERR;
      }
      *node = NewCmdInteger(i);
      break;
    }

    case 'd': {
      double d;
      if (!parseDouble(arg, &d)) {
        asprintf(err, "Could not parse double value '%s'", arg);
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
int parseArg(CmdSchemaArg *arg, CmdNode **current, const char **argv, int argc, int *pos,
             char **err) {

  if (*pos + 2 > argc) {
    *err = strdup("Arguments out of range");
    return CMDPARSE_ERR;
  }
  (*pos)++;
  int rc = typedParse(current, argv[*pos], arg->type, err);
  if (rc == CMDPARSE_OK) (*pos)++;
  return rc;
}

int parseTuple(CmdSchemaTuple *tup, CmdNode **current, const char **argv, int argc, int *pos,
               char **err) {

    if (*pos + 2 > argc) {
        *err = strdup("Arguments out of range");
        return CMDPARSE_ERR;
    }     
    size_t len = strlen(tup->fmt);
    *current = NewCmdTuple(len);
    (*pos)++;

    for (size_t i = 0; i < len; i++) {
        if (CMDPARSE_ERR == typedParse(&(*current)->a.args[i], argv[*pos], tup->fmt[i], err)) {
            // TODO: Free until here
            return CMDPARSE_ERR;
        }
        (*pos)++;
    }
    return CMDPARSE_OK;
}

int CmdParser_ProcessElement(CmdSchemaElement *elem, CmdNode **out, const char **argv, int argc,
                             int *pos, char **err) {
  switch (elem->type) {
    case CmdSchemaElement_Arg:
      return parseArg(&elem->arg, out, argv, argc, pos, err);
    case CmdSchemaElement_Tuple:
      return parseTuple(&elem->tup, out, argv, argc, pos, err);
    default:
      printf("Not supported yet...\n");
      return CMDPARSE_ERR;
  }
}

int CmdNode_AddChild(CmdNode *parent, const char *name, CmdNode *child, char **err) {

  switch (parent->type) {
    case CmdArg_Object:
      return CmdObj_Set(&parent->obj, name, child);

    case CmdArg_Vector:
      return CmdVector_Append(&parent->a, child);

    default: {
      asprintf(err, "Cannot add child to node of type %d", parent->type);
      return CMDPARSE_ERR;
    }
  }
  return CMDPARSE_OK;
}
int CmdParser_Parse(CmdSchemaNode *node, CmdNode *parent, const char **argv, int argc, int *pos,
                    char **err) {

  printf("Processing node %s, pos %d/%d\n", node->name, *pos, argc);
  CmdNode *current = parent;
  if (node->val) {
    // Parse the node value. This should consume tokens from the input array
    if (CMDPARSE_ERR == CmdParser_ProcessElement(node->val, &current, argv, argc, pos, err)) {
      return CMDPARSE_ERR;
    }
    // Add the current node to the parent
    if (current) {
      if (CMDPARSE_ERR == CmdNode_AddChild(parent, node->name, current, err)) {
        return CMDPARSE_ERR;
      }
    }
  }

  // advance the position
  //(*pos)++;

  // continue to parse any remaining transitional states until we consume the entire input array
  CmdParserStateFlags sf[node->size];
  int minEdge = 0;
  memset(sf, 0, sizeof(sf));

  while (*pos < argc) {
    printf("Trying edges, pos now %d/%d (%s)\n", *pos, argc, argv[*pos]);

    const char *tok = argv[*pos];
    printf("Trying token %s\n", tok);
    int found = 0;
    for (int i = minEdge; i < node->size; i++) {
      if (sf[i] & CmdParser_Blocked) continue;
      CmdSchemaNode *edge = node->edges[i];
      printf("Tying edge[%d] ", i);
      CmdSchemaNode_Print(edge, 1);
      printf("\n---------\n");
      if (CmdSchemaNode_Match(edge, tok)) {
        printf("MATCH!\n");
        found = 1;
        // if parsing failed - just return immediately
        if (CMDPARSE_ERR == CmdParser_Parse(edge, current, argv, argc, pos, err)) {
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
          minEdge = i;
        }
        // continue scanning from the first valid position again
        break;
      }
    }

    if (!found) goto end;
  }

end:
  // check that all the required nodes have been visited!
  for (int i = 0; i < node->size; i++) {
    if (node->edges[i]->flags & CmdSchema_Required && !(sf[i] & CmdParser_Visited)) {
      // set an error indicating the first missed required argument
      asprintf(err, "Missing required argument '%s'", node->edges[i]->name);
      return CMDPARSE_ERR;
    }
  }

  // all is okay!
  return CMDPARSE_OK;
}

int main() {

  CmdSchemaNode *root = NewSchema("FOO");
  CmdSchema_AddNamed(root, "BAR", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddNamed(root, "XXX", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddNamed(root, "LIMIT", CmdSchema_NewTuple("ll", (const char *[]){"FIRST", "LIMIT"}), CmdSchema_Optional);
  
  CmdSchemaNode_Print(root, 0);

  const char *args[] = {"BAR", "hello", "XXX", "world", "LIMIT", "0", "10"};
  CmdNode *cmd = NewCmdObject(1);
  int pos = 0;
  char *err = NULL;
  int rc = CmdParser_Parse(root, cmd, args, sizeof(args) / sizeof(*args), &pos, &err);
  printf("rc: %d, err: %s, pos %d\n", rc, err, pos);
  /*
  FT.AGGREGATE {index_name}
  {
  FILTER {query_string}
  [ GROUP BY {nargs} {arg} ...
      [AS {alias}]
      GROUPREDUCE {func} {nargs} {arg} ...
      ...
  ]
  [SORT BY {nargs} {arg} ...]
  [LIMIT {offset} {num}]
  }
  ZADD key score value [score value ...]

  schema {
      name: "FT.AGGREGATE",
      edges: [
          {kind: positional, type: string, name: "index_name", edges: [,
              {key:"FILTER", kind: named, required: true, type:string, name: "query_string"},
              {kind: union, required: true, repeateable: true, children: [
                  {"kind": "schema", key: "group", optional, true, edges: [

                  ]
                  }
              ]

          }
      ]
  }


*/

  // CmdSchema *s = NewCommand("FT.AGGREGATE");
  // CmdSchema_AddPositional(s, "index_name", CmdSchema_NewArg('s', NULL), CmdSchema_Required);
  // CmdSchema_AddNamed(s, "FILTER", CmdSchema_NewArg('s', "query_string"), CmdSchema_Required);

  // CmdSchema *pipeline = CmdSchema_AddUnion(s, "pipeline");
  //     CmdSchema *groupBy = CmdSchema_NewSubSchema(pipeline, "GROUP", CmdSchema_Optional);
  //         CmdSchema_AddNamed(groupBy, "BY", CmdSchema_NewVector('s'), CmdSchema_Required);
  //         CmdSchema_AddNamed(groupBy, "AS", CmdSchema_NewArg('s', "alias"), CmdSchema_Optional);
  //         CmdSchema_AddNamed(groupBy, "GROUPREDUCE", CmdSchema_NewVector('s'), CmdSchema_Required
  //         | CmdSchema_Repeating);

  //     CmdSchema *sortBy = CmdSchema_NewSubSchema(pipeline, "SORT", CmdSchema_Optional);
  //     CmdSchema_AddNamed(sortBy, "BY", CmdSchema_NewVector('s'), CmdSchema_Required);
  //     CmdSchema_AddPositional(sortBy, "sort_mode", CmdSchema_NewOption(2, "ASC", "DESC"),
  //     CmdSchema_Optional);

  //     CmdSchema_AddNamed(pipeline, "LIMIT",CmdSchema_NewTuple("ll", (const char *[]){"offset",
  //     "num"}), CmdSchema_Optional);
}
