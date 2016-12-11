%left OR.
%nonassoc LP RP.
%left QUOTE.
%left TERM.


%token_type {Token}  
 

%syntax_error {  

    int len =strlen(yytext)+100; 
    char msg[len];

    snprintf(msg, len, "Syntax error in query near '%s'", yytext);

    ctx->ok = 0;
    ctx->errorMsg = strdup(msg);
}   
   
%include {   

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "token.h"
#include "parser.h"
#include "../query.h"
#include "../rmutil/alloc.h"

extern int yylineno;
extern char *yytext;

typedef struct {
    Query *q;
    QueryStage *root;
    int ok;
    char *errorMsg;
}parseCtx;


void yyerror(char *s);
    
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


sub_stage(A) ::= LP stage(B) RP. {
    A = B;
}

sub_stage(A) ::= union_stage(B). {
    A = B;
}

sub_stage(A) ::= QUOTE inter_stage(B) QUOTE. {
    B->op = Q_EXACT;
    A = B;
}

sub_stage(A) ::= inter_stage(B). {
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


%code {

  /* Definitions of flex stuff */
 // extern FILE *yyin;
  typedef struct yy_buffer_state *YY_BUFFER_STATE;
  int             yylex( void );
  YY_BUFFER_STATE yy_scan_string( const char * );
  YY_BUFFER_STATE yy_scan_bytes( const char *, size_t );
  void            yy_delete_buffer( YY_BUFFER_STATE );
  
  


ParseNode *ParseQuery(const char *c, size_t len, char **err)  {

    //printf("Parsing query %s\n", c);
    yy_scan_bytes(c, len);
    void* pParser = ParseAlloc (malloc);        
    int t = 0;

    parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL };
    //ParseNode *ret = NULL;
    //ParserFree(pParser);
    while (ctx.ok && 0 != (t = yylex())) {
        Parse(pParser, t, tok, &ctx);                
    }
    if (ctx.ok) {
        Parse (pParser, 0, tok, &ctx);
    }
    ParseFree(pParser, free);
    if (err) {
        *err = ctx.errorMsg;
    }
    return ctx.root;
  }
   


}
