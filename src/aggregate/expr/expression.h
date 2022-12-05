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
  //RSExpr(const char *e, size_t n, QueryError *status);
  virtual ~RSExpr() = default;

  virtual void Print() const = 0;

  virtual int Eval(ExprEval &eval, RSValue *res) = 0;
  virtual int GetLookupKeys(RLookup *lookup, QueryError *err);

  static RSExpr *ParseAST(const char *e, size_t n, QueryError *status);
  static RSExpr *Parse(const char *expr, size_t len, char **err);
};

//---------------------------------------------------------------------------------------------

struct RSExprOp : public RSExpr {
  RSExprOp(unsigned char op, RSExpr *left, RSExpr *right);
  ~RSExprOp();

  unsigned char op;
  struct RSExpr *left;
  struct RSExpr *right;

  virtual void Print() const;

  virtual int Eval(ExprEval &eval, RSValue *res);
  virtual int GetLookupKeys(RLookup *lookup, QueryError *err);
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

  virtual int Eval(ExprEval &eval, RSValue *res);
  virtual int GetLookupKeys(RLookup *lookup, QueryError *err);
};

//---------------------------------------------------------------------------------------------

struct RSInverted : public RSExpr {
  RSInverted(RSExpr *child);
  ~RSInverted();

  struct RSExpr *child;

  virtual void Print() const;

  virtual int Eval(ExprEval &eval, RSValue *res);
  virtual int GetLookupKeys(RLookup *lookup, QueryError *err);
};

//---------------------------------------------------------------------------------------------

struct RSArgList {
  RSArgList(RSExpr *e);
  ~RSArgList();

  RSArgList *Append(RSExpr *e = 0);

  Vector<RSExpr*> args;

  size_t length() const { return args.size(); }
  const RSExpr *operator[](int i) const { return args[i]; }
};

//---------------------------------------------------------------------------------------------

struct RSFunctionExpr : public RSExpr {
  RSFunctionExpr(const char *str, size_t len, RSArgList *args, RSFunction cb);
  ~RSFunctionExpr();

  const char *name;
  RSArgList *_args;
  RSFunction Call;

  virtual void Print() const;

  virtual int Eval(ExprEval &eval, RSValue *res);
  virtual int GetLookupKeys(RLookup *lookup, QueryError *err);
};

//---------------------------------------------------------------------------------------------

struct RSLookupExpr : public RSExpr {
  RSLookupExpr(const char *str, size_t len);
  ~RSLookupExpr();

  const char *key;
  const RLookupKey *lookupKey;

  virtual void Print() const;

  virtual int Eval(ExprEval &eval, RSValue *res);
  virtual int GetLookupKeys(RLookup *lookup, QueryError *err);
};

//---------------------------------------------------------------------------------------------

struct RSLiteral : public RSExpr {
  RSLiteral() {}
  RSLiteral(RSValue&& value) : literal{std::exchange(value, RSValue{})} {}
  virtual ~RSLiteral();

  RSValue literal;

  virtual void Print() const;

  virtual int Eval(ExprEval &eval, RSValue *res);
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

///////////////////////////////////////////////////////////////////////////////////////////////

// Expression execution context/evaluator. I need to refactor this into something
// nicer, but I think this will do.

struct ExprEval {
  QueryError *err;
  const RLookup *lookup;
  const SearchResult *res;
  const RLookupRow *srcrow;
  const RSExpr *root;
  StringBlkAlloc stralloc; // Optional. YNOT?

  ExprEval(QueryError *err, RLookup *lookup, RLookupRow *srcrow, RSExpr *root, SearchResult *res = NULL) :
    err(err), lookup(lookup), srcrow(srcrow), root(root), res(res), stralloc(1024) {}

  int Eval(RSValue *result);

  char *Strndup(const char *s, size_t n);

  void Cleanup();

//protected:
  int evalFunc(const RSFunctionExpr *f, RSValue *result);
  int evalOp(const RSExprOp *op, RSValue *result);
  int evalInverted(const RSInverted *vv, RSValue *result);
  int evalPredicate(const RSPredicate *pred, RSValue *result);
  int evalProperty(const RSLookupExpr *e, RSValue *res);

  int getPredicateBoolean(const RSValue *l, const RSValue *r, RSCondition op);

  int eval(const RSExpr *e, RSValue *res) { return e->Eval(*this, res); }
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
