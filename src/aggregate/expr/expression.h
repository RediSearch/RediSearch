#ifndef RS_AGG_EXPRESSION_H_
#define RS_AGG_EXPRESSION_H_
#include <redisearch.h>
#include <value.h>

typedef enum {
  RSExpr_Literal,
  RSExpr_Property,
  RSExpr_Op,
  RSExpr_Function,
} RSExprType;

typedef unsigned char RSExprOp;

struct RSExpr;

typedef struct {
  size_t len;
  struct RSExpr *args[];
} RSArgList;

typedef struct {
  const char *name;
  RSArgList *args;
} RSFunction;

typedef struct RSExpr {
  union {
    RSExprOp op;
    RSValue literal;
    RSFunction func;
    RSKey property;
  };
  RSExprType t;
} RSExpr;

RSArgList *RS_NewArgList(RSExpr *e);
RSExpr *RS_NewStringLiteral(char *str, size_t len);
RSExpr *RS_NewNumberLiteral(char *str, size_t len);
RSExpr *RS_NewOp(RSExprOp op, RSExpr *left, RSExpr *right);
RSExpr *RS_NewFunc(char *str, size_t len, RSArgList *args);
RSExpr *RS_NewProp(char *str, size_t len);
#endif