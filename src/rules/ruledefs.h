#include "util/dllist.h"
#include "aggregate/expr/expression.h"
#include "rules.h"

typedef enum {
  // Match a prefix
  SCRULE_TYPE_KEYPREFIX = 0x01,
  // Match an expression
  SCRULE_TYPE_EXPRESSION = 0x02,
  // Contains a field, but don't check its contents
  SCRULE_TYPE_HASFIELD = 0x03,
  // Matches all documents
  SCRULE_TYPE_MATCHALL = 0x04
} SchemaRuleType;

typedef enum {
  // Index the document
  SCACTION_TYPE_INDEX = 0x01,

  // Change the document property (while indexing)
  SCACTION_TYPE_SETATTR = 0x02,

  // Stop processing this document
  SCACTION_TYPE_ABORT = 0x03,

  // Goto another named chain
  SCACTION_TYPE_GOTO = 0x04,
} SchemaActionType;

typedef struct SchemaAction SchemaAction;

#define SCHEMA_RULE_HEAD       \
  SchemaRuleType rtype;        \
  const char *index;           \
  const char *name;            \
  struct SchemaAction *action; \
  char **rawrule;  // Raw text of the rule itself

struct SchemaRule {
  SCHEMA_RULE_HEAD;
};

typedef struct {
  SCHEMA_RULE_HEAD
  char *prefix;
  size_t nprefix;
} SchemaPrefixRule;

typedef struct {
  SCHEMA_RULE_HEAD
  char *exprstr;
  RSExpr *exprobj;
  RSValue *v;
  RLookup lk;
} SchemaExprRule;

typedef struct {
  SCHEMA_RULE_HEAD
  RedisModuleString *field;
} SchemaHasFieldRule;

struct SchemaRules {
  SchemaRule **rules;
  // Cached array...
  MatchAction *actions;
};

typedef enum {
  SCATTR_TYPE_LANGUAGE = 0x01,
  SCATTR_TYPE_SCORE = 0x02,
  SCATTR_TYPE_PAYLOAD = 0x03
} SchemaAttrType;

typedef struct {
  SchemaAttrType type;
  void *value;
} SchemaAttrValue;

struct SchemaAction {
  SchemaActionType atype;
  union {
    SchemaAttrValue *attr;
    char *goto_;
  } u;
};