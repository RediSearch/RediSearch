#ifndef RS_AGG_EXPRESSION_H_
#define RS_AGG_EXPRESSION_H_
#include <redisearch.h>
#include <value.h>
#include <result_processor.h>

typedef enum {
  RSExpr_Literal,
  RSExpr_Property,
  RSExpr_Op,
  RSExpr_Function,
} RSExprType;

struct RSExpr;
typedef struct {
  unsigned char op;
  struct RSExpr *left;
  struct RSExpr *right;
} RSExprOp;

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

typedef int (*RSFunctionCallback)(SearchResult *r, RSValue *argv, int argc, void *privdata);
typedef struct {
} RSFunctionRegistry;

RSFunctionCallback RSFunctionRegistry_Get(const char *f);

typedef struct {
  SearchResult *r;
  RSFunctionRegistry *fr;
  RSSortingTable *sortables;
} RSExprEvalCtx;

#define EXPR_EVAL_ERR 1
#define EXPR_EVAL_OK 0
int RSExpr_Eval(RSExprEvalCtx *ctx, RSExpr *e, RSValue *result, char **err);

RSArgList *RS_NewArgList(RSExpr *e);
void RSArgList_Free(RSArgList *l);
RSArgList *RSArgList_Append(RSArgList *l, RSExpr *e);

RSExpr *RS_NewStringLiteral(char *str, size_t len);
RSExpr *RS_NewNumberLiteral(double n);
RSExpr *RS_NewOp(unsigned char op, RSExpr *left, RSExpr *right);
RSExpr *RS_NewFunc(char *str, size_t len, RSArgList *args);
RSExpr *RS_NewProp(char *str, size_t len);
void RSExpr_Free(RSExpr *expr);
#endif