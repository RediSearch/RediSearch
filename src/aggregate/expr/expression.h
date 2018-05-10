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

/* Expression type enum */
typedef enum {
  /* Literal constant expression */
  RSExpr_Literal,
  /* Property from the result (e.g. @foo) */
  RSExpr_Property,
  /* Arithmetic operator, e.g. @foo+@bar */
  RSExpr_Op,
  /* Built-in function call */
  RSExpr_Function,
  /* Predicate expression, e.g. @foo == 3 */
  RSExpr_Predicate,
} RSExprType;

struct RSExpr;
typedef struct {
  unsigned char op;
  struct RSExpr *left;
  struct RSExpr *right;
} RSExprOp;

typedef enum {
  /* Equality, == */
  RSCondition_Eq,
  /* Less than, < */
  RSCondition_Lt,
  /* Less than or equal, <= */
  RSCondition_Le,
  /* Greater than, > */
  RSCondition_Gt,
  /* Greater than or equal, >= */
  RSCondition_Ge,
  /* Not equal, != */
  RSCondition_Ne,
  /* Logical AND of 2 expressions, && */
  RSCondition_And,
  /* Logical OR of 2 expressions, || */
  RSCondition_Or,

  /* Logical NOT of the left expression only */
  RSCondition_Not,
} RSCondition;

static const char *RSConditionStrings[] = {
    [RSCondition_Eq] = "==",  [RSCondition_Lt] = "<",  [RSCondition_Le] = "<=",
    [RSCondition_Gt] = ">",   [RSCondition_Ge] = ">=", [RSCondition_Ne] = "!=",
    [RSCondition_And] = "&&", [RSCondition_Or] = "||", [RSCondition_Not] = "!",
};

typedef struct {
  struct RSExpr *left;
  struct RSExpr *right;
  RSCondition cond;
} RSPredicate;

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
    RSPredicate pred;
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
RSExpr *RS_NewNullLiteral();
RSExpr *RS_NewNumberLiteral(double n);
RSExpr *RS_NewOp(unsigned char op, RSExpr *left, RSExpr *right);
RSExpr *RS_NewFunc(const char *str, size_t len, RSArgList *args, RSFunction cb);
RSExpr *RS_NewProp(const char *str, size_t len);
RSExpr *RS_NewPredicate(RSCondition cond, RSExpr *left, RSExpr *right);
void RSExpr_Free(RSExpr *expr);
void RSExpr_Print(RSExpr *expr);

/* Parse an expression string, returning a prased expression tree on success. On failure (syntax
 * err, etc) we set and error in err, and return NULL */
RSExpr *RSExpr_Parse(const char *expr, size_t len, char **err);

/* Get the return type of an expression. In the case of a property we do not try to guess but rather
 * just return String */
RSValueType GetExprType(RSExpr *expr, RSSortingTable *tbl);

/* Return all the field names needed by the expression. Returns an array that should be freed with
 * array_free */
const char **Expr_GetRequiredFields(RSExpr *expr);

#endif