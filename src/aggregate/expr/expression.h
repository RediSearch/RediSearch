#ifndef RS_AGG_EXPRESSION_H_
#define RS_AGG_EXPRESSION_H_

#include "redisearch.h"
#include "value.h"
#include "aggregate/functions/function.h"

#ifdef __cplusplus
extern "C" {
#endif

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

  /* NOT expression, i.e. !(....) */
  RSExpr_Inverted
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
  RSCondition_Or
} RSCondition;

static const char *getRSConditionStrings(RSCondition type) {
  switch (type) {
  case RSCondition_Eq: return "==";
  case RSCondition_Lt: return "<";
  case RSCondition_Le: return "<=";
  case RSCondition_Gt: return ">";
  case RSCondition_Ge: return ">=";
  case RSCondition_Ne: return "!=";
  case RSCondition_And: return "&&";
  case RSCondition_Or: return "||";
  default:
    RS_LOG_ASSERT(0, "oops");
    break;
  }
}

typedef struct {
  struct RSExpr *left;
  struct RSExpr *right;
  RSCondition cond;
} RSPredicate;

typedef struct {
  struct RSExpr *child;
} RSInverted;

typedef struct {
  size_t len;
  struct RSExpr *args[];
} RSArgList;

typedef struct {
  const char *name;
  RSArgList *args;
  RSFunction Call;
} RSFunctionExpr;

typedef struct {
  const char *key;
  const RLookupKey *lookupObj;
} RSLookupExpr;

typedef struct RSExpr {
  RSExprType t;
  union {
    RSExprOp op;
    RSValue literal;
    RSFunctionExpr func;
    RSPredicate pred;
    RSLookupExpr property;
    RSInverted inverted;
  };
} RSExpr;

/**
 * Expression execution context/evaluator. I need to refactor this into something
 * nicer, but I think this will do.
 */
typedef struct ExprEval {
  QueryError *err;
  const RLookup *lookup;
  const SearchResult *res;
  const RLookupRow *srcrow;
  const RSExpr *root;
  BlkAlloc stralloc; // Optional. YNOT?
} ExprEval;

#define EXPR_EVAL_ERR 0
#define EXPR_EVAL_OK 1
#define EXPR_EVAL_NULL 2

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Alternative expression execution context/evaluator.
 */

typedef struct EvalCtx {
  RLookup lk;
  RLookupRow row;
  QueryError status;
  ExprEval ee;
  RSValue res;
  RSExpr *_expr;
  bool _own_expr;
} EvalCtx;

EvalCtx *EvalCtx_Create();
EvalCtx *EvalCtx_FromExpr(RSExpr *expr);
EvalCtx *EvalCtx_FromString(const char *exprstr);
void EvalCtx_Destroy(EvalCtx *r);
RLookupKey *EvalCtx_Set(EvalCtx *r, const char *name, RSValue *val);
int EvalCtx_AddHash(EvalCtx *r, RedisModuleCtx *ctx, RedisModuleString *key);
int EvalCtx_Eval(EvalCtx *r);
int EvalCtx_EvalExpr(EvalCtx *r, RSExpr *expr);
int EvalCtx_EvalExprStr(EvalCtx *r, const char *exprstr);

/**
 * Scan through the expression and generate any required lookups for the keys.
 * @param root Root iterator for scan start
 * @param lookup The lookup registry which will store the keys
 * @param err If this fails, EXPR_EVAL_ERR is returned, and this variable contains
 *  the error.
 */
int ExprAST_GetLookupKeys(RSExpr *root, RLookup *lookup, QueryError *err);
int ExprEval_Eval(ExprEval *evaluator, RSValue *result);

void ExprAST_Free(RSExpr *expr);
void ExprAST_Print(const RSExpr *expr);
RSExpr * ExprAST_Parse(const char *e, size_t n, QueryError *status);

/* Parse an expression string, returning a prased expression tree on success. On failure (syntax
 * err, etc) we set and error in err, and return NULL */
RSExpr *RSExpr_Parse(const char *expr, size_t len, char **err);
void RSExpr_Free(RSExpr *e);

/**
 * Helper functions for the evaluator context:
 */
void *ExprEval_UnalignedAlloc(ExprEval *ev, size_t n);
char *ExprEval_Strndup(ExprEval *ev, const char *s, size_t n);

/** Cleans up the allocator */
void ExprEval_Cleanup(ExprEval *ev);

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a new result processor in the form of a projector. The projector will
 * execute the expression in `ast` and write the result of that expression to the
 * appropriate place.
 * 
 * @param ast the parsed expression
 * @param lookup the lookup registry that contains the keys to search for
 * @param dstkey the target key (in lookup) to store the result.
 * 
 * @note The ast needs to be paired with the appropriate RLookupKey objects. This
 * can be done by calling EXPR_GetLookupKeys()
 */
ResultProcessor *RPEvaluator_NewProjector(const RSExpr *ast, const RLookup *lookup, const RLookupKey *dstkey);

/**
 * Creates a new result processor in the form of a filter. The filter will
 * execute the expression in `ast` on each upstream result. If the expression
 * evaluates to false, the result will not be propagated to the next processor.
 * 
 * @param ast the parsed expression
 * @param lookup lookup used to find the key for the value
 * 
 * See notes for NewProjector()
 */
ResultProcessor *RPEvaluator_NewFilter(const RSExpr *ast, const RLookup *lookup);

/** 
 * Reply with a string which describes the result processor.
 */
void RPEvaluator_Reply(RedisModuleCtx *ctx, const ResultProcessor *rp);

#ifdef __cplusplus
}
#endif
#endif
