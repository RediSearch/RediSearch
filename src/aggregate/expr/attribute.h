#ifndef EXPR_ATTRIBUTE_H
#define EXPR_ATTRIBUTE_H
#ifdef __cplusplus
extern "C" {
#endif
#include "result_processor.h"
#include "value.h"

typedef int (*ExprAttributeCallback)(int code, const void *evalctx, const SearchResult *,
                                     RSValue *out);

/**
 * Register an attribute by name. The returned value is the
 * code by which it is registered. A return value less than 0 means that
 * the attribute already exists
 */
int Expr_RegisterAttribute(const char *name, ExprAttributeCallback cb);

/**
 * Find an attribute by name, returns the name of the attribute, or <0
 * if the attribute does not exist
 */
int Expr_FindAttributeByName(const char *name, size_t n);
const char *Expr_FindAttributeByCode(int code);

/** Global init/destroy functions */
void Expr_AttributesInit(void);
void Expr_AttributesDestroy(void);
void Expr_AttributesRegisterAll(void);

ExprAttributeCallback Expr_GetAttributeCallback(int code);

#ifdef __cplusplus
}
#endif
#endif