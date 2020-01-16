#include "util/dllist.h"
#include "aggregate/expr/expression.h"
#include "rules.h"

typedef enum {
  // Match a prefix
  SCRULE_TYPE_KEYPREFIX = 0x01,
  // Match an expression
  SCRULE_TYPE_EXPRESSION = 0x02
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

struct SchemaAction;

#define SCHEMA_RULE_HEAD \
  DLLIST_node llnode;    \
  SchemaRuleType rtype;  \
  const char *index;     \
  const char *name;      \
  struct SchemaAction *action;

typedef struct SchemaRule {
  SCHEMA_RULE_HEAD;
} SchemaRule;

#define SCHEMA_ACTION_HEAD \
  SchemaActionType atype;  \
  // Nothing here..

typedef struct SchemaAction {
  SCHEMA_ACTION_HEAD;
} SchemaAction;

typedef struct {
  SCHEMA_RULE_HEAD
  char *prefix;
  size_t nprefix;
} SchemaPrefixRule;

typedef struct {
  SCHEMA_RULE_HEAD
  const char *exprstr;
  RSExpr *exprobj;
  RSValue *v;
} SchemaExprRule;

struct SchemaRules {
  DLLIST rules;
  // Cached array...
  MatchAction *actions;
};

typedef struct SchemaAction SchemaIndexAction;

typedef enum {
  SCATTR_TYPE_LANGUAGE = 0x01,
  SCATTR_TYPE_SCORE = 0x02,
  SCATTR_TYPE_PAYLOAD = 0x03
} SchemaAttrType;

typedef struct {
  SCHEMA_ACTION_HEAD;
  SchemaAttrType attr;
  void *value;
} SchemaSetattrAction;