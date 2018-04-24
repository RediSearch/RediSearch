#ifndef RS_AGG_EXPRESSION_H_
#define RS_AGG_EXPRESSION_H_
#include <redisearch.h>
#include <value.h>
#include <result_processor.h>
#include <aggregate/functions/function.h>
#include <util/block_alloc.h>

typedef struct {
  char *err;
  int code;
  int errAllocated;
} RSError;

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
  RSFunction Call;
} RSFunctionExpr;

typedef struct RSExpr {
  union {
    RSExprOp op;
    RSValue literal;
    RSFunctionExpr func;
    RSKey property;
  };
  RSExprType t;
} RSExpr;

typedef struct {
  SearchResult *r;
  RSSortingTable *sortables;
  RedisSearchCtx *sctx;
  RSFunctionEvalCtx *fctx;
} RSExprEvalCtx;

#define EXPR_EVAL_ERR 1
#define EXPR_EVAL_OK 0
int RSExpr_Eval(RSExprEvalCtx *ctx, RSExpr *e, RSValue *result, char **err);

RSArgList *RS_NewArgList(RSExpr *e);
void RSArgList_Free(RSArgList *l);
RSArgList *RSArgList_Append(RSArgList *l, RSExpr *e);

RSExpr *RS_NewStringLiteral(const char *str, size_t len);
RSExpr *RS_NewNumberLiteral(double n);
RSExpr *RS_NewOp(unsigned char op, RSExpr *left, RSExpr *right);
RSExpr *RS_NewFunc(const char *str, size_t len, RSArgList *args, RSFunction cb);
RSExpr *RS_NewProp(const char *str, size_t len);
void RSExpr_Free(RSExpr *expr);
void RSExpr_Print(RSExpr *expr);

/* Parse an expression string, returning a prased expression tree on success. On failure (syntax
 * err, etc) we set and error in err, and return NULL */
RSExpr *RSExpr_Parse(const char *expr, size_t len, char **err);

/* Get the return type of an expression. In the case of a property we do not try to guess but rather
 * just return String */
RSValueType GetExprType(RSExpr *expr, RSSortingTable *tbl);

#endif