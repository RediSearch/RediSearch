/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

%name RSExprParser_Parse

%left LOWEST.

%left OR.
%left AND.
%right NOT.

%nonassoc EQ NE LT LE GT GE.

%left PLUS MINUS.
%left DIVIDE TIMES MOD.
%right POW.

%left PROPERTY.
%right SYMBOL.
%left STRING.
%left NUMBER.
%right ARGLIST.

%extra_argument { RSExprParseCtx *ctx }

%token_type { RSExprToken }
%default_type {RSExpr *}
%default_destructor { RSExpr_Free($$); }

%type number {double}
%destructor number {}

%type arglist { RSArgList * }
%destructor arglist { RSArgList_Free($$); }

%include {
#include "token.h"
#include "expression.h"
#include "exprast.h"
#include "parser.h"

}

%syntax_error {

    if (ctx->errorMsg) {
        char *reason = ctx->errorMsg;
        rm_asprintf(&ctx->errorMsg, "Syntax error at offset %d near '%.*s': %s", TOKEN.pos, TOKEN.len, TOKEN.s, reason);
        rm_free(reason);
    } else {
        rm_asprintf(&ctx->errorMsg, "Syntax error at offset %d near '%.*s'", TOKEN.pos, TOKEN.len, TOKEN.s);
    }
    ctx->ok = 0;
}

program ::= expr(A). { ctx->root = A; }

expr(A) ::= LP expr(B) RP. { A = B; }

// "Manual" expansion of the arithmetic operators, to optimize the AST in-place when possible
// expr ::= expr OP expr
expr(A) ::= expr(B) PLUS   expr(C). { A = RS_NewOp('+', B, C); }
expr(A) ::= expr(B) DIVIDE expr(C). { A = RS_NewOp('/', B, C); }
expr(A) ::= expr(B) TIMES  expr(C). { A = RS_NewOp('*', B, C); }
expr(A) ::= expr(B) MINUS  expr(C). { A = RS_NewOp('-', B, C); }
expr(A) ::= expr(B) POW    expr(C). { A = RS_NewOp('^', B, C); }
expr(A) ::= expr(B) MOD    expr(C). { A = RS_NewOp('%', B, C); }
// expr ::= number OP expr
expr(A) ::= number(B) PLUS   expr(C). { A = RS_NewOp('+', RS_NewNumberLiteral(B), C); }
expr(A) ::= number(B) DIVIDE expr(C). { A = RS_NewOp('/', RS_NewNumberLiteral(B), C); }
expr(A) ::= number(B) TIMES  expr(C). { A = RS_NewOp('*', RS_NewNumberLiteral(B), C); }
expr(A) ::= number(B) MINUS  expr(C). { A = RS_NewOp('-', RS_NewNumberLiteral(B), C); }
expr(A) ::= number(B) POW    expr(C). { A = RS_NewOp('^', RS_NewNumberLiteral(B), C); }
expr(A) ::= number(B) MOD    expr(C). { A = RS_NewOp('%', RS_NewNumberLiteral(B), C); }
// expr ::= expr OP number
expr(A) ::= expr(B) PLUS   number(C). { A = RS_NewOp('+', B, RS_NewNumberLiteral(C)); }
expr(A) ::= expr(B) DIVIDE number(C). { A = RS_NewOp('/', B, RS_NewNumberLiteral(C)); }
expr(A) ::= expr(B) TIMES  number(C). { A = RS_NewOp('*', B, RS_NewNumberLiteral(C)); }
expr(A) ::= expr(B) MINUS  number(C). { A = RS_NewOp('-', B, RS_NewNumberLiteral(C)); }
expr(A) ::= expr(B) POW    number(C). { A = RS_NewOp('^', B, RS_NewNumberLiteral(C)); }
expr(A) ::= expr(B) MOD    number(C). { A = RS_NewOp('%', B, RS_NewNumberLiteral(C)); }
// number := number OP number. In-place arithmetic, to optimize the AST
number(A) ::= number(B) PLUS   number(C). { A = B + C; }
number(A) ::= number(B) DIVIDE number(C). { A = B / C; }
number(A) ::= number(B) TIMES  number(C). { A = B * C; }
number(A) ::= number(B) MINUS  number(C). { A = B - C; }
number(A) ::= number(B) POW    number(C). { A = pow(B, C); }
number(A) ::= number(B) MOD    number(C). { A = fmod(B, C); }

// Logical predicates
expr(A) ::= expr(B) EQ expr(C).  { A = RS_NewPredicate(RSCondition_Eq,  B, C); }
expr(A) ::= expr(B) NE expr(C).  { A = RS_NewPredicate(RSCondition_Ne,  B, C); }
expr(A) ::= expr(B) LT expr(C).  { A = RS_NewPredicate(RSCondition_Lt,  B, C); }
expr(A) ::= expr(B) LE expr(C).  { A = RS_NewPredicate(RSCondition_Le,  B, C); }
expr(A) ::= expr(B) GT expr(C).  { A = RS_NewPredicate(RSCondition_Gt,  B, C); }
expr(A) ::= expr(B) GE expr(C).  { A = RS_NewPredicate(RSCondition_Ge,  B, C); }
expr(A) ::= expr(B) OR expr(C).  { A = RS_NewPredicate(RSCondition_Or,  B, C); }
expr(A) ::= expr(B) AND expr(C). { A = RS_NewPredicate(RSCondition_And, B, C); }

expr(A) ::= NOT expr(B). {
    if (B->t == RSExpr_Inverted) {
        A = B->inverted.child; // double negation
        B->inverted.child = NULL;
        RSExpr_Free(B);
    } else {
        A = RS_NewInverted(B);
    }
}

expr(A) ::= STRING(B). { A = RS_NewStringLiteral(B.s, B.len); }
expr(A) ::= number(B). [LOWEST] { A = RS_NewNumberLiteral(B); }

number(A) ::= NUMBER(B). { A = B.numval; }

expr(A) ::= PROPERTY(B). { A = RS_NewProp(B.s, B.len); }
expr(A) ::= SYMBOL(B) LP arglist(C) RP. {
    RSFunctionInfo *cb = RSFunctionRegistry_Get(B.s, B.len);
    if (cb && cb->minArgs <= C->len && C->len <= cb->maxArgs) {
        A = RS_NewFunc(cb, C);
    } else { // Syntax error
        if (!cb) { // Function not found
            rm_asprintf(&ctx->errorMsg, "Unknown function name '%.*s'", B.len, B.s);
        } else { // Argument count mismatch
            if (cb->minArgs == cb->maxArgs) {
                rm_asprintf(&ctx->errorMsg, "Function '%.*s' expects %d arguments, but got %d", B.len, B.s, cb->minArgs, C->len);
            } else {
                rm_asprintf(&ctx->errorMsg, "Function '%.*s' expects between %d and %d arguments, but got %d",
                                                B.len, B.s, cb->minArgs, cb->maxArgs, C->len);
            }
        }
        A = NULL;
        ctx->ok = 0;
        RSArgList_Free(C);
    }
}

expr(A) ::= SYMBOL(B) . {
    if (B.len == 4 && !strncmp(B.s, "NULL", 4)) {
        A = RS_NewNullLiteral();
    } else {
        rm_asprintf(&ctx->errorMsg, "Unknown symbol '%.*s'", B.len, B.s);
        ctx->ok = 0;
        A = NULL;
    }
}

arglist(A) ::= . [ARGLIST] { A = RS_NewArgList(NULL); }
arglist(A) ::= expr(B) . [ARGLIST] { A = RS_NewArgList(B); }
arglist(A) ::= arglist(B) COMMA expr(C) . [ARGLIST] {
    A = RSArgList_Append(B, C);
}
