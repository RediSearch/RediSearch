
%left OR.
%right LP.
%left RP.
%nonassoc QUOTE.
%left TERM.

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
#include "../query_node.h"
   
} // END %include  

%extra_argument { parseCtx *ctx }
%default_type { QueryNode *}
%default_destructor { QueryNode_Free($$); }
%type mofidier { char * }
%destructor modifier {free ($$); }
query ::= exprlist(A). { ctx->root = A; }
query ::= expr(A). { ctx->root = A; }

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
expr(A) ::= TERM(B). {  A = NewTokenNode(ctx->q, B.s, B.len);  }
mofidier(A) ::= AT TERM(B). { A = strndup(B.s, B.len); }
expr(A) ::= mofidier(B) COLON expr(C). {
    if (ctx->q->ctx && ctx->q->ctx->spec) {
     C->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, B, strlen(B)); 
    }
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





