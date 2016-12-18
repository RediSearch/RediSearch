
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
   
} // END %include  

%extra_argument { parseCtx *ctx }
%default_type { QueryStage *}
%default_destructor { QueryStage_Free($$); }

query ::= exprlist(A). { ctx->root = A; }
query ::= expr(A). { ctx->root = A; }

exprlist(A) ::= expr(B) expr(C). {
    A = NewLogicStage(Q_INTERSECT);
    QueryStage_AddChild(A, B);
    QueryStage_AddChild(A, C);
}

exprlist(A) ::= exprlist(B) expr(C). {
    A = B;
    QueryStage_AddChild(A, C);
}

expr(A) ::= union(B). {  A = B;}
expr(A) ::= LP expr(B) RP .  { A = B; } 
expr(A) ::= LP exprlist(B) RP .  { A = B; } 
expr(A) ::= TERM(B). {  A = NewTokenStage(ctx->q, &B);  }


exact(A) ::= QUOTE TERM(B).  {
    A = NewLogicStage(Q_EXACT);
    QueryStage_AddChild(A, NewTokenStage(ctx->q, &B));
}

exact(A) ::= exact(B) TERM(C). {
    QueryStage_AddChild(B, NewTokenStage(ctx->q, &C));
    A = B;
}

expr(A) ::= exact(B) QUOTE. {
    A = B;
}

union(A) ::= union(B) OR TERM(C). {
    QueryStage_AddChild(B, NewTokenStage(ctx->q, &C));
    A = B;
}


union(A) ::= TERM(B) OR TERM(C). {
    A = NewLogicStage(Q_UNION);
    QueryStage_AddChild(A, NewTokenStage(ctx->q, &B));
    QueryStage_AddChild(A, NewTokenStage(ctx->q, &C));
}





