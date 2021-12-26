
#include "exprast.h"

#include <ctype.h>


///////////////////////////////////////////////////////////////////////////////////////////////

// #define arglist_sizeof(l) (sizeof(RSArgList) + ((l) * sizeof(RSExpr *)))

RSArgList::RSArgList(RSExpr *e) {
  args = array_new(RSExpr*, e ? 1 : 0);
  if (e) args[0] = e;
}

RSArgList *RSArgList::Append(RSExpr *e) {
  *array_ensure_tail(&args, RSExpr*) = e;
  return this;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0
static RSExpr *newExpr(RSExprType t) {
  RSExpr *e = rm_calloc(1, sizeof(*e));
  e->t = t;
  return e;
}
#endif

//---------------------------------------------------------------------------------------------

// unquote and unescape a stirng literal, and return a cleaned copy of it
static char *unescpeStringDup(const char *s, size_t sz) {

  char *dst = rm_malloc(sz);
  char *dstStart = dst;
  char *src = (char *)s + 1;       // we start after the first quote
  char *end = (char *)s + sz - 1;  // we end at the last quote
  while (src < end) {
    // unescape
    if (*src == '\\' && src + 1 < end && (ispunct(*(src + 1)) || isspace(*(src + 1)))) {
      ++src;
      continue;
    }
    *dst++ = *src++;
  }
  *dst = '\0';
  return dstStart;
}

//---------------------------------------------------------------------------------------------

RSStringLiteral::RSStringLiteral(const char *str, size_t len) {
  literal = RS_StaticValue(RSValue_String);
  literal.strval.str = unescpeStringDup(str, len);
  literal.strval.len = strlen(literal.strval.str);
  literal.strval.stype = RSString_Malloc;
}

//---------------------------------------------------------------------------------------------

RSNullLiteral::RSNullLiteral() {
  RSValue_MakeReference(&literal, RS_NullVal());
}

//---------------------------------------------------------------------------------------------

RSNumberLiteral::RSNumberLiteral(double n) {
  literal = RS_StaticValue(RSValue_Number);
  literal.numval = n;
}

//---------------------------------------------------------------------------------------------

RSExprOp::RSExprOp(unsigned char op_, RSExpr *left, RSExpr *right) {
  op = op_;
  left = left;
  right = right;
}

//---------------------------------------------------------------------------------------------

RSPredicate::RSPredicate(RSCondition cond, RSExpr *left, RSExpr *right) {
  cond = cond;
  left = left;
  right = right;
}

//---------------------------------------------------------------------------------------------

RSFunctionExpr::RSFunctionExpr(const char *str, size_t len, RSArgList *args_, RSFunction cb) {
  args = args_; // @@ ownership
  name = rm_strndup(str, len);
  Call = cb;
}

//---------------------------------------------------------------------------------------------

RSLookupExpr::RSLookupExpr(const char *str, size_t len) {
  key = rm_strndup(str, len);
  lookupKey = NULL;
}

//---------------------------------------------------------------------------------------------

RSInverted::RSInverted(RSExpr *child) {
  child = child;
}

//---------------------------------------------------------------------------------------------

RSArgList::~RSArgList() {
  size_t len = array_len(args);
  for (size_t i = 0; i < len; i++) {
    delete args[i];
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

RSExprOp::~RSExprOp() {
  delete left;
  delete right;
}

RSPredicate::~RSPredicate() {
  delete left;
  delete right;
}

RSInverted::~RSInverted() {
  delete child;
}

RSFunctionExpr::~RSFunctionExpr() {
  rm_free((char *)name);
  delete args;
}

RSLookupExpr::~RSLookupExpr() {
  rm_free((char *)key);
}

RSLiteral::~RSLiteral() {
  literal.Clear();
}

//---------------------------------------------------------------------------------------------

void RSExprOp::Print() const {
  printf("(");
  left->Print();
  printf(" %c ", op);
  right->Print();
  printf(")");
}

void RSPredicate::Print() const {
  printf("(");
  left->Print();
  printf(" %s ", RSConditionStrings[cond]);
  right->Print();
  printf(")");
}

void RSInverted::Print() const {
  printf("!");
  child->Print();
}

void RSFunctionExpr::Print() const {
  printf("%s(", name);
  for (size_t i = 0; args != NULL && i < args->length(); i++) {
    (*args[i])->Print();
    if (i < args->length() - 1) printf(", ");
  }
  printf(")");
}

void RSLookupExpr::Print() const {
  printf("@%s", key);
}

void RSLiteral::Print() const {
  literal.Print();
}

//---------------------------------------------------------------------------------------------

#if 0
// @@ used in unit tests

void ExprAST_Free(RSExpr *e) {
  delete e;
}

void ExprAST_Print(const RSExpr *e) {
  e->Print(e);
}

RSExpr *ExprAST_Parse(const char *e, size_t n, QueryError *status) {
  char *errtmp = NULL;
  RS_LOG_ASSERT(!status->HasError(), "Query has error")

  RSExpr *ret = RSExpr_Parse(e, n, &errtmp);
  if (!ret) {
    status->SetError(QUERY_EEXPR, errtmp);
  }
  rm_free(errtmp);
  return ret;
}

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
