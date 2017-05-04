
%left OR.
%right LP.
%left RP.
%nonassoc QUOTE.
%left TERM.
%left MODIFIER.
%left NUMBER.

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
%default_type { QueryNode *}
%default_destructor { QueryNode_Free($$); }
%type modifierlist { Vector* }
%type modifier { QueryToken }
%type TERM { QueryToken }
%type NUMBER { QueryToken }
%type MODIFIER { QueryToken }
%type term { QueryToken }
%type num { RangeNumber }
%type numeric_range { NumericFilter * }

%destructor num { }
%destructor TERM {  }
%destructor term {  }
%destructor NUMBER { }
%destructor MODIFIER {  }
%destructor modifier {  }
%destructor modifierlist { 
    for (size_t i = 0; i < Vector_Size($$); i++) {
        char *s;
        Vector_Get($$, i, &s);
        free(s);
    }
    Vector_Free($$); 
}
%destructor numeric_range {
    NumericFilter_Free($$);
}
query ::= exprlist(A). { ctx->root = A; }
query ::= expr(A). { ctx->root = A; }

// empty query rule
query ::= . { ctx->root = NULL; }

exprlist(A) ::= expr(B) expr(C). {
    A = NewPhraseNode(0);
    QueryPhraseNode_AddChild(A, B);
    QueryPhraseNode_AddChild(A, C);
}

exprlist(A) ::= exprlist(B) expr(C). {
    A = B;
    QueryPhraseNode_AddChild(A, C);
}


expr(A) ::= union(B). {  A = B;}
expr(A) ::= LP expr(B) RP .  { A = B; } 
expr(A) ::= LP exprlist(B) RP .  { A = B; } 
expr(A) ::= term(B). { 
 A = NewTokenNode(ctx->q, strdupcase(B.s, B.len), B.len);  
}

expr(A) ::= MINUS expr(B). {
    A = NewNotNode(B);
}

expr(A) ::= TILDE expr(B). {
    A = NewOptionalNode(B);
}

// field modifier -- @foo:bar
modifier(A) ::= MODIFIER(B). { A = B; }

modifierlist(A) ::= modifier(B) OR term(C). { 
    A = NewVector(char *, 2);
    char *s = strdupcase(B.s, B.len);
    Vector_Push(A, s);
    s = strdupcase(C.s, C.len);
    Vector_Push(A, s);
}

modifierlist(A) ::= modifierlist(B) OR term(C). {
    char *s = strdupcase(C.s, C.len);
    Vector_Push(B, s);
    A = B;
}

expr(A) ::= term(B) STAR. {
    A = NewPrefixNode(ctx->q, strdupcase(B.s, B.len), B.len);
}

expr(A) ::= modifierlist(B) COLON expr(C). {
    C->fieldMask = 0;
    for (int i = 0; i < Vector_Size(B); i++) {
        char *p;
        Vector_Get(B, i, &p);

        if (ctx->q->ctx && ctx->q->ctx->spec) {
            C->fieldMask |= IndexSpec_GetFieldBit(ctx->q->ctx->spec, p, strlen(p)); 
        }
        free(p);
    }
    Vector_Free(B);
    A=C;
    
}

expr(A) ::= modifier(B) COLON expr(C). {
    // gets the field mask from the query's spec. 
    // TODO: Avoid leaky abstraction here
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        C->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, B.s, B.len); 
    }
    //free((char *)B.s);
    A = C; 
} 

exact(A) ::= QUOTE term(B).  {
    A = NewPhraseNode(1);
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx->q, strdupcase(B.s, B.len), B.len));
}

exact(A) ::= exact(B) term(C). {
    QueryPhraseNode_AddChild(B, NewTokenNode(ctx->q, strdupcase(C.s, C.len), C.len));
    A = B;
}

expr(A) ::= exact(B) QUOTE. {
    A = B;
}

union(A) ::= union(B) OR term(C). {
    QueryUnionNode_AddChild(B, NewTokenNode(ctx->q, strdupcase(C.s, C.len), C.len));
    A = B;
}


union(A) ::= term(B) OR term(C). {
    A = NewUnionNode();
    QueryUnionNode_AddChild(A, NewTokenNode(ctx->q, strdupcase(B.s, B.len), B.len));
    QueryUnionNode_AddChild(A, NewTokenNode(ctx->q, strdupcase(C.s, C.len), C.len));
}


expr(A) ::= modifier(B) COLON numeric_range(C). {
    C->fieldName = strdupcase(B.s, B.len);
    A = NewNumericNode(C);
}

numeric_range(A) ::= LSQB num(B) num(C) RSQB. {
    A = NewNumericFilter(B.num, C.num, B.inclusive, C.inclusive);
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

term(A) ::= TERM(B). {
    A = B;
}

term(A) ::= NUMBER(B). {
    A = B;
}









