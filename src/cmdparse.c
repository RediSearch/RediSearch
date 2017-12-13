#include <stdlib.h>
#include <string.h>
#include <stdio.h>


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
}CmdObject;

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


static inline CmdNode *NewCommandNode(CmdNodeType t) {
    CmdNode *ret = calloc(1, sizeof(*ret));

    ret->type = t;
    return ret;
}

CmdNode *NewCmdString(const char *s) {
    CmdNode *ret = NewCommandNode(CmdArg_String);
    ret->s = (CmdString){.str = strdup(s), .len = strlen(s) };
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
        .args = calloc(cap, sizeof(CmdNode*)),
        .cap = cap,
        .len = 0,
    };
   
    return ret;
}


int CmdObj_Set(CmdObject *obj, const char *key, CmdNode *val){ 

    for (size_t i = 0; i < obj->len; i++) {
        if (!strcmp(key, obj->entries[i].k)) {
            // TODO: free memory
            obj->entries[i].v = val;
            return CMDPARSE_OK;
        }
    }
    if (obj->len + 1 >= obj->cap) {
        obj->cap *= 2;
        obj->entries = realloc(obj->entries, obj->cap*sizeof(*obj->entries));
    }
    obj->entries[obj->len++] = (CmdKeyValue){.k = key, .v = val}
    return CMDPARSE_OK;

}
int CmdVector_Append(CmdArray *arr, CmdNode *val) {

    if (arr->len== arr->cap) {
        return CMDPARSE_ERR;
    }
    arr->args[arr->len++] = val;
    return CMDPARSE_OK;
}


typedef struct CmdSchema {
    
}CmdSchema;
typedef struct CmdCommand {} CmdCommand;
typedef struct CmdSubCommand ;
typedef struct CmdSchemaArg{
    char type;
} CmdSchemaArg;
typedef struct CmdSchemaFlag{} CmdSchemaFlag;
typedef struct CmdSchemaOption{} CmdSchemaOption;
typedef struct CmdSchemaUnion{} CmdSchemaUnion;

typedef struct CmdSchemaTuple {}CmdSchemaTuple;
typedef struct CmdSchemaVector{} CmdSchemaVector;
CmdSchema *NewCommand(const char *name);

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
    int flags;
} CmdSchemaElement;

typedef enum {
    CmdSchema_Required = 0x01,
    CmdSchema_Optional = 0x02,
    CmdSchema_Repeating = 0x04,
}CmdSchemaFlags;

typedef enum {
    CmdSchemaNode_Command,
    CmdSchemaNode_Union,
    CmdSchemaNode_PositionalArg,
    CmdSchemaNode_NamedArg,
    
} CmdSchemaNodeType;

typedef struct CmdSchemaNode {
    CmdSchemaElement val;
    CmdSchemaFlags flags;
    CmdSchemaNodeType type; 
    const char *name;
    struct CmdSchemaNode **edges;
    int size;
} CmdSchemaNode;

int CmdSchemaNode_Match(CmdSchemaNode *n, const char *token);

typedef enum {
    CmdParser_New = 0x00,
    CmdParser_Visited = 0x01,
    CmdParser_Blocked = 0x02,
}CmdParserStateFlags;

typedef struct {
    CmdSchemaNode *node;
    CmdParserStateFlags *edgeFlags;
} CmdParserCtx;


int parseArg(CmdSchemaArg *arg, CmdNode **current, const char **argv, int  argc, int *pos, char **err) {

    if (*pos + 2 >= argc) {
        *err = strdup("Arguments out of range");
        return CMDPARSE_ERR;
    }
    (*pos)++;
    switch (arg->type) {
        case 's':
            *current = NewCmdString(argv[(*pos)++]);
        break;
        case 'l':
        case 'd':
        default:
        return CMDPARSE_ERR;
    }
}
int CmdParser_ProcessElement(CmdSchemaElement *elem, CmdNode **out, const char **argv, int  argc, int *pos, char **err) {
    switch(elem->type) {
        case CmdSchemaElement_Arg:
        return parseArg(&elem->arg, out, argv, argc, pos, err);
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
}
int CmdParser_Parse(CmdSchemaNode *node, CmdNode *parent, const char **argv, int  argc, int *pos, char **err) {

    
    CmdNode *current = NULL;
    // Parse the node value. This should consume tokens from the input array
    if (CMDPARSE_ERR == CmdParser_ProcessElement(&node->val, &current, argv, argc, pos, err)) {
        return CMDPARSE_ERR;
    }
    // Add the current node to the parent
    if (current) {
        if (CMDPARSE_ERR == CmdNode_AddChild(parent, node->name, current, err)) {
            return CMDPARSE_ERR;
        }   
    }

    // advance the position
    (*pos)++;

    // continue to parse any remaining transitional states until we consume the entire input array
    CmdParserStateFlags sf[node->size];
    int minEdge = 0;
    memset(sf, 0, sizeof(sf));
    while (*pos < argc) {

        const char *tok = argv[*pos];
        int found = 0;
        for (int  i = minEdge; i < node->size; i++) {
            if (sf[i] & CmdParser_Blocked) continue;
            CmdSchemaNode *edge = node->edges[i];
            if (CmdSchemaNode_Match(edge, tok)) {
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
    for (int  i = 0; i < node->size; i++) {
        if (node->edges[i]->flags & CmdSchema_Required && !(sf[i] & CmdParser_Visited)) {
            // set an error indicating the first missed required argument
            asprintf(err, "Missing required argument '%s'", node->edges[i]->name);
            return CMDPARSE_ERR;
        }
    }

    // all is okay!
    return CMDPARSE_OK;
    
}



int CmdSchema_AddNamed(CmdSchema *s, const char *param, CmdSchemaElement *elem, CmdSchemaFlags flags);
int CmdSchema_AddPositional(CmdSchema *s, const char *param, CmdSchemaElement *elem, CmdSchemaFlags flags);
CmdSchemaElement *CmdSchema_NewTuple(const char *fmt, const char **names);
CmdSchemaElement *CmdSchema_NewArg(const char type, const char *name);
CmdSchemaElement *CmdSchema_NewVector(const char type);
CmdSchemaElement *CmdSchema_NewOption(int num, ...);
CmdSchemaElement *NewFlag(int deflt);
CmdSchemaElement *CmdSchema_NewVariadicVector(const char **fmt);
CmdSchema *CmdSchema_NewSubSchema(CmdSchema *parent, const char *param, int flags);

int main() {

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


    CmdSchema *s = NewCommand("FT.AGGREGATE");
    CmdSchema_AddPositional(s, "index_name", CmdSchema_NewArg('s', NULL), CmdSchema_Required);
    CmdSchema_AddNamed(s, "FILTER", CmdSchema_NewArg('s', "query_string"), CmdSchema_Required);

    CmdSchema *pipeline = CmdSchema_AddUnion(s, "pipeline");
        CmdSchema *groupBy = CmdSchema_NewSubSchema(pipeline, "GROUP", CmdSchema_Optional);
            CmdSchema_AddNamed(groupBy, "BY", CmdSchema_NewVector('s'), CmdSchema_Required);
            CmdSchema_AddNamed(groupBy, "AS", CmdSchema_NewArg('s', "alias"), CmdSchema_Optional);
            CmdSchema_AddNamed(groupBy, "GROUPREDUCE", CmdSchema_NewVector('s'), CmdSchema_Required | CmdSchema_Repeating);

        CmdSchema *sortBy = CmdSchema_NewSubSchema(pipeline, "SORT", CmdSchema_Optional);
        CmdSchema_AddNamed(sortBy, "BY", CmdSchema_NewVector('s'), CmdSchema_Required);
        CmdSchema_AddPositional(sortBy, "sort_mode", CmdSchema_NewOption(2, "ASC", "DESC"), CmdSchema_Optional);

        CmdSchema_AddNamed(pipeline, "LIMIT",CmdSchema_NewTuple("ll", (const char *[]){"offset", "num"}), CmdSchema_Optional);


}
