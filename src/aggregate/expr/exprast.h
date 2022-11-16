/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef EXPRAST_H
#define EXPRAST_H

#include "expression.h"
#ifdef __cplusplus
extern "C" {
#endif
/**
 * TODO: All of this belongs inside the parsing code. This is just AST stuff
 */
RSArgList *RS_NewArgList(RSExpr *e);
void RSArgList_Free(RSArgList *l);
RSArgList *RSArgList_Append(RSArgList *l, RSExpr *e);

RSExpr *RS_NewStringLiteral(const char *str, size_t len);
RSExpr *RS_NewNullLiteral();
RSExpr *RS_NewNumberLiteral(double n);
RSExpr *RS_NewOp(unsigned char op, RSExpr *left, RSExpr *right);
RSExpr *RS_NewFunc(const char *str, size_t len, RSArgList *args, RSFunction cb);
RSExpr *RS_NewProp(const char *str, size_t len);
RSExpr *RS_NewPredicate(RSCondition cond, RSExpr *left, RSExpr *right);
RSExpr *RS_NewInverted(RSExpr *child);
void RSExpr_GetProperties(RSExpr *e, char ***props);

#ifdef __cplusplus
}
#endif
#endif