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
  SCRULE_TYPE_MATCHALL = 0x04,
  // Custom match
  SCRULE_TYPE_CUSTOM = 0x05
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

  SCACTION_TYPE_LOADATTR = 0x05,

  // Custom rule
  SCACTION_TYPE_CUSTOM = 0x06
} SchemaActionType;

typedef struct SchemaAction SchemaAction;

/** Field pack for attributes which are loaded together */
typedef struct SchemaAttrFieldpack {
  /**
   * This can be used on multiple documents in a queue; rather than copy them
   * individually, we just increment the reference counter
   */
  size_t refcount;
  RedisModuleString *lang, *score, *payload;
} SchemaAttrFieldpack;

void SCAttrFields_Incref(SchemaAttrFieldpack *);
void SCAttrFields_Decref(SchemaAttrFieldpack *);

struct SchemaAction {
  SchemaActionType atype;
  union {
    struct SchemaSetattrSettings {
      IndexItemAttrs attrs;
      int mask;
    } setattr;
    SchemaAttrFieldpack *lattr;
    char *goto_;
  } u;
};

#define SCHEMA_RULE_HEAD      \
  SchemaRuleType rtype;       \
  IndexSpec *spec;            \
  char *name;                 \
  struct SchemaAction action; \
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

struct SchemaCustomRule {
  SCHEMA_RULE_HEAD
  void *arg;
  SchemaCustomCallback check;
};

struct SchemaCustomCtx {
  MatchAction **action;
};

struct SchemaRules {
  SchemaRule **rules;

  // Cached array...
  MatchAction *actions;

  // Incremented whenever the rules are changed
  uint64_t revision;
};

void SchemaRule_Free(SchemaRule *rule);
void MatchAction_Clear(MatchAction *action);