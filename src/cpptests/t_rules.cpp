#include <gtest/gtest.h>
#include "spec.h"
#include "common.h"
#include "rules/rules.h"
#include "rules/ruledefs.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"

class RulesTest : public ::testing::Test {};

TEST_F(RulesTest, testBasic) {
  auto *rules = SchemaRules_Create();
  ASSERT_TRUE(rules != NULL);
  ArgsCursorCXX args("PREFIX", "user:", "index");
  QueryError err = {QUERY_OK};
  IndexSpec *sp = NewIndexSpec("foo");
  int rc = SchemaRules_AddArgsInternal(rules, sp, "myrule", &args, &err);
  ASSERT_EQ(REDISMODULE_OK, rc) << QueryError_GetError(&err);

  SchemaRule *r = rules->rules[0];
  ASSERT_EQ(SCRULE_TYPE_KEYPREFIX, r->rtype);
  ASSERT_EQ(SCACTION_TYPE_INDEX, r->action.atype);
  SchemaPrefixRule *pr = (SchemaPrefixRule *)r;
  ASSERT_STREQ("user:", pr->prefix);

  MatchAction *actions = NULL;
  size_t nactions = 0;
  RuleKeyItem rki;
  const char *docname = "user:mnunberg";
  rki.kstr = RedisModule_CreateString(RSDummyContext, docname, strlen(docname));
  int matches = SchemaRules_Check(rules, RSDummyContext, &rki, &actions, &nactions);
  RedisModule_FreeString(RSDummyContext, rki.kstr);
  ASSERT_NE(0, matches);
  ASSERT_GT(nactions, 0);
  ASSERT_FALSE(actions == NULL);
  ASSERT_EQ(sp, actions[0].spec);
  SchemaRules_Free(rules);
  IndexSpec_Free(sp);
}