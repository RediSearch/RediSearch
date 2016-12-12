%left OR.
%nonassoc LP RP.
%left QUOTE.
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

query ::= stage(A). { ctx->root = A; }


stage(A) ::= TERM(B). {
    A = NewTokenStage(ctx->q, &B);
}

stage(A) ::= inter_stage(B). {
    A = B;
}

stage(A) ::= sub_stage(B). {
    A = B;
}

stage(A) ::= sub_stage(B) sub_stage(C). {
    A = NewLogicStage(Q_INTERSECT);
    QueryStage_AddChild(A, B);
    QueryStage_AddChild(A, C);
}

stage(A) ::= sub_stage(B) OR sub_stage(C). {
    A = NewLogicStage(Q_UNION);
    QueryStage_AddChild(A, B);
    QueryStage_AddChild(A, C);
}


sub_stage(A) ::= LP stage(B) RP. {
    A = B;
}

stage(A) ::= union_stage(B). {
    A = B;
}

sub_stage(A) ::= QUOTE inter_stage(B) QUOTE. {
    B->op = Q_EXACT;
    A = B;
}


union_stage(A) ::= TERM(B) OR TERM(C). {
    A = NewLogicStage(Q_UNION);
    QueryStage_AddChild(A, NewTokenStage(ctx->q, &B));
    QueryStage_AddChild(A, NewTokenStage(ctx->q, &C));
}

union_stage(A) ::= union_stage(B) OR TERM(C). {
    QueryStage_AddChild(B, NewTokenStage(ctx->q, &C));
    A = B;
}

inter_stage(A) ::= TERM(B) TERM(C). {
    A = NewLogicStage(Q_INTERSECT);
    QueryStage_AddChild(A, NewTokenStage(ctx->q, &B));
    QueryStage_AddChild(A, NewTokenStage(ctx->q, &C));
}

inter_stage(A) ::= inter_stage(B) TERM(C). {
    QueryStage_AddChild(B, NewTokenStage(ctx->q, &C));
    A = B;
}




