#include <stdio.h>
#include <string.h>
#include "test.h"
#include "cmdparse.h"

int testSchema() {
  CmdSchemaNode *root = NewSchema("FOO", "Test command");
  ASSERT(root);
  ASSERT_EQUAL(root->type, CmdSchemaNode_Schema);
  ASSERT_EQUAL(root->size, 0);
  ASSERT_STRING_EQ("Test command", root->help);
  ASSERT(root->val == NULL);
  ASSERT(root->edges == NULL);
  ASSERT_EQUAL(CMDPARSE_OK,
               CmdSchema_AddPostional(root, "term", CmdSchema_NewArg('s'), CmdSchema_Required));
  ASSERT(root->edges != NULL);
  ASSERT(root->size == 1);
  CmdSchema_AddNamed(root, "foo", CmdSchema_NewArg('s'), CmdSchema_Optional);
  ASSERT(root->size == 2);

  CmdSchema_AddFlag(root, "NX");
  ASSERT(root->size == 3);

  CmdSchemaNode *sub = CmdSchema_NewSubSchema(root, "SUB", CmdSchema_Optional, "No Help");
  ASSERT(sub != NULL);
  ASSERT_EQUAL(sub->type, CmdSchemaNode_Schema);
  ASSERT_EQUAL(CMDPARSE_OK,
               CmdSchema_AddNamed(sub, "bar", CmdSchema_NewArg('l'), CmdSchema_Required));
  printf("\n\n");
  CmdSchemaNode_Print(root, 0);
  return 0;
}

TEST_MAIN({ TESTFUNC(testSchema); })