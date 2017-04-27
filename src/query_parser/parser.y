
%left OR.
%right LP.
%left RP.
%nonassoc QUOTE.
%left TERM.
%left AT.

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
#include <assert.h>
#include "parse.h"
#include "../rmutil/vector.h"
#include "../query_node.h"
   
} // END %include  

%extra_argument { parseCtx *ctx }
%default_type { QueryNode *}
%default_destructor { QueryNode_Free($$); }
%type modifierlist { Vector* }
%type modifier { QueryToken }
%type TERM { QueryToken }
%destructor TERM { free((char *)$$.s); }
%destructor modifier { free((char *)$$.s); }
%destructor modifierlist { Vector_Free($$); }
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
expr(A) ::= TERM(B). { 
 A = NewTokenNode(ctx->q, B.s, B.len);  
}

expr(A) ::= MINUS expr(B). {
    A = NewNotNode(B);
}

expr(A) ::= TILDE expr(B). {
    A = NewOptionalNode(B);
}

// field modifier -- @foo:bar
modifier(A) ::= AT TERM(B). { A = B; }

modifierlist(A) ::= modifier(B) OR TERM(C). { 
    A = NewVector(char *, 2);
    Vector_Push(A, B.s);
    Vector_Push(A, C.s);
}
modifierlist(A) ::= modifierlist(B) OR TERM(C). {
    Vector_Push(B, C.s);
    A = B;
}

expr(A) ::= TERM(B) STAR. {
    A = NewPrefixNode(ctx->q, B.s, B.len);
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
    free((char *)B.s);
    A = C; 
} 

exact(A) ::= QUOTE TERM(B).  {
    A = NewPhraseNode(1);
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx->q, B.s, B.len));
}

exact(A) ::= exact(B) TERM(C). {
    QueryPhraseNode_AddChild(B, NewTokenNode(ctx->q, C.s, C.len));
    A = B;
}

expr(A) ::= exact(B) QUOTE. {
    A = B;
}

union(A) ::= union(B) OR TERM(C). {
    QueryUnionNode_AddChild(B, NewTokenNode(ctx->q, C.s, C.len));
    A = B;
}


union(A) ::= TERM(B) OR TERM(C). {
    A = NewUnionNode();
    QueryUnionNode_AddChild(A, NewTokenNode(ctx->q, B.s, B.len));
    QueryUnionNode_AddChild(A, NewTokenNode(ctx->q, C.s, C.len));
}





