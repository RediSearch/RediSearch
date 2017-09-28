%left TILDE.
%left TERM. 
%left QUOTE.
%left COLON.
%left MINUS.
%left NUMBER.
%left MODIFIER.

%left TERMLIST.
%right LP.
%left RP.
%left AND.
%left OR.
%left ORX.

%token_type {QueryToken}  

%syntax_error {  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'\n", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
}   
   
%include {   

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "parse.h"
#include "../rmutil/vector.h"
#include "../query_node.h"

char *strdupcase(const char *s, size_t len) {
  char *ret = strndup(s, len);
  for (int i = 0; i < len; i++) {
    ret[i] = tolower(ret[i]);
  }
  return ret;
}
   
} // END %include  

%extra_argument { parseCtx *ctx }
%default_type { QueryToken }
%default_destructor { }

%type expr { QueryNode * } 
%destructor expr { QueryNode_Free($$); }

%type termlist { QueryNode * } 
%destructor termlist { QueryNode_Free($$); }

%type union { QueryNode *}
%destructor union { QueryNode_Free($$); }

%type geo_filter { GeoFilter *}
%destructor geo_filter { GeoFilter_Free($$); }

%type modifierlist { Vector* }
%destructor modifierlist { 
    for (size_t i = 0; i < Vector_Size($$); i++) {
        char *s;
        Vector_Get($$, i, &s);
        free(s);
    }
    Vector_Free($$); 
}

%type num { RangeNumber }

%type numeric_range { NumericFilter * }
%destructor numeric_range {
    NumericFilter_Free($$);
}

query ::= expr(A) . { 
 /* If the root is a negative node, we intersect it with a wildcard node */
 
    ctx->root = A;
 
}
query ::= . {
 ctx->root = NULL;
}

expr(A) ::= expr(B) expr(C) . [AND] {
    if (B->type == QN_PHRASE && B->pn.exact == 0 && 
        B->fieldMask == RS_FIELDMASK_ALL ) {
        A = B;
    } else {
        A = NewPhraseNode(0);
        QueryPhraseNode_AddChild(A, B);
    } 
    QueryPhraseNode_AddChild(A, C);
} 

expr(A) ::= union(B) . [ORX] {
    A = B;
}


union(A) ::= expr(B) OR expr(C) . [OR] {
    
    if (B->type == QN_UNION && B->fieldMask == RS_FIELDMASK_ALL) {
        A =B;
    } else {
        A = NewUnionNode();
        QueryUnionNode_AddChild(A, B);
    }
    QueryUnionNode_AddChild(A, C); 
}



union(A) ::= union(B) OR expr(C). [ORX] {
    A = B;
    QueryUnionNode_AddChild(A, C); 
}

// expr(A) ::= term(B) . { 
//     A = NewTokenNode(ctx->q, strdupcase(B.s, B.len), B.len); 
// }

expr(A) ::= modifier(B) COLON expr(C) . [MODIFIER] {
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        C->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, B.s, B.len); 
    }
    A = C; 
}

expr(A) ::= modifier(B) COLON TERM(C). [MODIFIER]  {
    A = NewTokenNode(ctx->q, strdupcase(C.s, C.len), C.len);
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        A->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, B.s, B.len); 
    }
}



expr(A) ::= modifierlist(B) COLON expr(C) . [MODIFIER] {
    
    C->fieldMask = 0;
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        for (int i = 0; i < Vector_Size(B); i++) {
            char *p;
            Vector_Get(B, i, &p);
            C->fieldMask |= IndexSpec_GetFieldBit(ctx->q->ctx->spec, p, strlen(p)); 
            free(p);
        }
    }
    Vector_Free(B);
    A=C;
} 

expr(A) ::= LP expr(B) RP . {
    A = B;
}

expr(A) ::= QUOTE termlist(B) QUOTE. {
    B->pn.exact =1;
    A = B;
}

term(A) ::= QUOTE term(B) QUOTE. {
    A = B;
}

expr(A) ::= term(B) .  {
    A = NewTokenNode(ctx->q, strdupcase(B.s, B.len), B.len);
}

termlist(A) ::= term(B) term(C). [TERMLIST]  {
    
    A = NewPhraseNode(0);
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx->q, strdupcase(B.s, B.len), B.len));
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx->q, strdupcase(C.s, C.len), C.len));

}
termlist(A) ::= termlist(B) term(C) . [TERMLIST] {
    A = B;
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx->q, strdupcase(C.s, C.len), C.len));

}


expr(A) ::= MINUS expr(B) . { 
    A = NewNotNode(B);
}
expr(A) ::= TILDE expr(B) . { 
    A = NewOptionalNode(B);
}

expr(A) ::= term(B) STAR. {
    A = NewPrefixNode(ctx->q, strdupcase(B.s, B.len), B.len);
}

modifier(A) ::= MODIFIER(B) . {
    A = B;
 } 

modifierlist(A) ::= modifier(B) OR term(C). {
    A = NewVector(char *, 2);
    char *s = strndup(B.s, B.len);
    Vector_Push(A, s);
    s = strndup(C.s, C.len);
    Vector_Push(A, s);
}

modifierlist(A) ::= modifierlist(B) OR term(C). {
    char *s = strndup(C.s, C.len);
    Vector_Push(B, s);
    A = B;
}

expr(A) ::= modifier(B) COLON numeric_range(C). {
    // we keep the capitalization as is
    C->fieldName = strndup(B.s, B.len);
    A = NewNumericNode(C);
}

numeric_range(A) ::= LSQB num(B) num(C) RSQB. [NUMBER] {
    A = NewNumericFilter(B.num, C.num, B.inclusive, C.inclusive);
}

expr(A) ::= modifier(B) COLON geo_filter(C). {
    // we keep the capitalization as is
    C->property = strndup(B.s, B.len);
    A = NewGeofilterNode(C);
}

geo_filter(A) ::= LSQB num(B) num(C) num(D) TERM(E) RSQB. [NUMBER] {
    A = NewGeoFilter(B.num, C.num, D.num, strdupcase(E.s, E.len));
    char *err = NULL;
    if (!GeoFilter_IsValid(A, &err)) {
        ctx->ok = 0;
        ctx->errorMsg = strdup(err);
    }
}

num(A) ::= NUMBER(B). {
    A.num = B.numval;
    A.inclusive = 1;
}

num(A) ::= LP num(B). {
    A=B;
    A.inclusive = 0;
}

num(A) ::= MINUS num(B). {
    B.num = -B.num;
    A = B;
}

term(A) ::= TERM(B) . {
    A = B; 
}
term(A) ::= NUMBER(B) . {
    A = B; 
}

