#pragma once

#include "redisearch.h"
#include "value.h"
#include "aggregate/functions/function.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0
// Expression type enum
enum RSExprType {
  RSExpr_Literal,   // Literal constant expression
  RSExpr_Property,  // Property from the result (e.g. @foo)
  RSExpr_Op,        // Arithmetic operator, e.g. @foo+@bar
  RSExpr_Function,  // Built-in function call
  RSExpr_Predicate, // Predicate expression, e.g. @foo == 3
  RSExpr_Inverted   // NOT expression, i.e. !(....)
};
#endif

//---------------------------------------------------------------------------------------------

struct RSExpr : Object {
/*
  RSExprType t;
  union {
    RSExprOp op;
    RSValue literal;
    RSFunctionExpr func;
    RSPredicate pred;
    RSLookupExpr property;
    RSInverted inverted;
  };
*/
  RSExpr() {}
  RSExpr(const char *e, size_t n, QueryError *status);
  virtual ~RSExpr() {}

  int GetLookupKeys(RLookup *lookup, QueryError *err);
  virtual void Print() const;
};

//---------------------------------------------------------------------------------------------

struct RSExprOp : public RSExpr {
  RSExprOp(unsigned char op, RSExpr *left, RSExpr *right);
  ~RSExprOp();

  unsigned char op;
  struct RSExpr *left;
  struct RSExpr *right;

  virtual void Print() const;
};

//---------------------------------------------------------------------------------------------

enum RSCondition {
  RSCondition_Eq,  // Equality, ==
  RSCondition_Lt,  // Less than, <
  RSCondition_Le,  // Less than or equal, <=
  RSCondition_Gt,  // Greater than, >
  RSCondition_Ge,  // Greater than or equal, >=
  RSCondition_Ne,  // Not equal, !=
  RSCondition_And, // Logical AND of 2 expressions, &&
  RSCondition_Or   // Logical OR of 2 expressions, ||
};

extern const char *RSConditionStrings[];

//---------------------------------------------------------------------------------------------

struct RSPredicate : public RSExpr {
  RSPredicate(RSCondition cond, RSExpr *left, RSExpr *right);
  ~RSPredicate();

  struct RSExpr *left;
  struct RSExpr *right;
  RSCondition cond;

  virtual void Print() const;
};

//---------------------------------------------------------------------------------------------

struct RSInverted : public RSExpr {
  RSInverted(RSExpr *child);
  ~RSInverted();

  struct RSExpr *child;

  virtual void Print() const;
};

//---------------------------------------------------------------------------------------------

struct RSArgList {
  RSArgList(RSExpr *e);
  ~RSArgList();

  RSArgList *Append(RSExpr *e = 0);

  //size_t len;
  //RSExpr *args[];
  arrayof(RSExpr*) args;

  size_t length() const { return array_len(args); }
  const RSExpr *operator[](int i) const { return args[i]; }
};

//---------------------------------------------------------------------------------------------

struct RSFunctionExpr : public RSExpr {
  RSFunctionExpr(const char *str, size_t len, RSArgList *args, RSFunction cb);
  ~RSFunctionExpr();

  const char *name;
  RSArgList *args;
  RSFunction Call;

  virtual void Print() const;
};

//---------------------------------------------------------------------------------------------

struct RSLookupExpr : public RSExpr {
  RSLookupExpr(const char *str, size_t len);
  ~RSLookupExpr();

  const char *key;
  const RLookupKey *lookupKey;

  virtual void Print() const;
};

//---------------------------------------------------------------------------------------------

struct RSLiteral : public RSExpr {
  RSLiteral();
  virtual ~RSLiteral();

  RSValue literal;

  virtual void Print() const;
};

//---------------------------------------------------------------------------------------------

struct RSNumberLiteral : public RSLiteral {
  RSNumberLiteral(double n);
};

//---------------------------------------------------------------------------------------------

struct RSStringLiteral : public RSLiteral {
  RSStringLiteral(const char *str, size_t len);
};

//---------------------------------------------------------------------------------------------

struct RSNullLiteral : public RSLiteral {
  RSNullLiteral();
};

//---------------------------------------------------------------------------------------------

RSExpr *RSExpr_Parse(const char *expr, size_t len, char **err);

///////////////////////////////////////////////////////////////////////////////////////////////

// Expression execution context/evaluator. I need to refactor this into something
// nicer, but I think this will do.

struct ExprEval {
  QueryError *err;
  const RLookup *lookup;
  const SearchResult *res;
  const RLookupRow *srcrow;
  const RSExpr *root;
  BlkAlloc stralloc; // Optional. YNOT?

  int Eval(RSValue *result);

  void *UnalignedAlloc(size_t n);
  char *Strndup(const char *s, size_t n);

  void Cleanup();

protected:
  int evalFunc(const RSFunctionExpr *f, RSValue *result);
  int evalOp(const RSExprOp *op, RSValue *result);
  int getPredicateBoolean(const RSValue *l, const RSValue *r, RSCondition op);
  int evalInverted(const RSInverted *vv, RSValue *result);
  int evalPredicate(const RSPredicate *pred, RSValue *result);
  int evalProperty(const RSLookupExpr *e, RSValue *res);

  int evalInternal(const RSExpr *e, RSValue *res);
};

//---------------------------------------------------------------------------------------------

#define EXPR_EVAL_ERR 0
#define EXPR_EVAL_OK 1
#define EXPR_EVAL_NULL 2

///////////////////////////////////////////////////////////////////////////////////////////////

// ResultProcessor type which evaluates expressions

struct RPEvaluator : ResultProcessor {
  ExprEval eval;
  RSValue *val;
  const RLookupKey *outkey;

  RPEvaluator(const char *name, const RSExpr *ast, const RLookup *lookup, const RLookupKey *dstkey);
  virtual ~RPEvaluator();

  virtual int Next(SearchResult *res);
};

//---------------------------------------------------------------------------------------------

struct RPProjector : RPEvaluator {
  RPProjector(const RSExpr *ast, const RLookup *lookup, const RLookupKey *dstkey);

  virtual int Next(SearchResult *res);
};

//---------------------------------------------------------------------------------------------

struct RPFilter : RPEvaluator {
  RPFilter(const RSExpr *ast, const RLookup *lookup);

  virtual int Next(SearchResult *res);
};

///////////////////////////////////////////////////////////////////////////////////////////////
