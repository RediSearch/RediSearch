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

  CmdSchemaNode *sub = CmdSchema_AddSubSchema(root, "SUB", CmdSchema_Optional, "No Help");
  ASSERT(sub != NULL);
  ASSERT(root->edges[3] == sub);
  ASSERT_EQUAL(sub->type, CmdSchemaNode_Schema);
  ASSERT_EQUAL(CMDPARSE_OK,
               CmdSchema_AddNamed(sub, "bar", CmdSchema_NewArg('l'), CmdSchema_Required));
  ASSERT(sub->size == 1);

  ASSERT_EQUAL(CMDPARSE_OK, CmdSchema_AddFlag(root, "FLAG"));

  ASSERT(root->size == 5);
  ASSERT_EQUAL(CmdSchemaNode_Flag, root->edges[4]->type);

  ASSERT_EQUAL(CMDPARSE_OK,
               CmdSchema_AddPostional(root, "opt",
                                      CmdSchema_NewOption(3, (const char *[]){"FOO", "BAR", "BAZ"}),
                                      CmdSchema_Optional));
  ASSERT(root->size == 6);
  ASSERT_EQUAL(CmdSchemaNode_PositionalArg, root->edges[5]->type);
  ASSERT_EQUAL(CmdSchemaElement_Option, root->edges[5]->val->type);
  CmdSchemaOption opt = root->edges[5]->val->opt;
  ASSERT_EQUAL(3, opt.num);
  ASSERT_STRING_EQ("FOO", opt.opts[0]);
  ASSERT_STRING_EQ("BAR", opt.opts[1]);
  ASSERT_STRING_EQ("BAZ", opt.opts[2]);

  printf("\n\n");
  CmdSchemaNode_Print(root, 0);
  return 0;
}

int testTuple() {
  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddNamed(sc, "TUP", CmdSchema_NewTuple("lsd", (const char *[]){"foo", "bar", "baz"}),
                     CmdSchema_Optional);

  CmdString *args = CmdParser_NewArgListV(5, "FOO", "TUP", "2", "hello", "0.5");
  CmdSchemaNode_Print(sc, 0);
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 5, &err, 1);
  if (err != NULL) printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg_Print(cmd, 0);

  ASSERT(cmd->type == CmdArg_Object);
  ASSERT_EQUAL(cmd->obj.len, 1);
  ASSERT_STRING_EQ(cmd->obj.entries[0].k, "TUP");

  CmdArg *t = cmd->obj.entries[0].v;
  ASSERT_EQUAL(t->a.args[0]->type, CmdArg_Integer);
  ASSERT_EQUAL(t->a.args[0]->i, 2);

  ASSERT_EQUAL(t->a.args[1]->type, CmdArg_String);
  ASSERT_STRING_EQ(t->a.args[1]->s.str, "hello");

  ASSERT_EQUAL(t->a.args[2]->type, CmdArg_Double);
  ASSERT_EQUAL(t->a.args[2]->d, 0.5);

  free(args);
  CmdArg_Free(cmd);

  // test out of range
  err = NULL;
  cmd = NULL;
  args = CmdParser_NewArgListV(4, "FOO", "TUP", "2", "hello");
  rc = CmdParser_ParseCmd(sc, &cmd, args, 4, &err, 1);
  printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_ERR, rc);
  ASSERT(cmd == NULL);
  ASSERT(err != NULL);
  free(args);

  // Test invalid values
  err = NULL;
  cmd = NULL;
  args = CmdParser_NewArgListV(5, "FOO", "TUP", "xx", "hello", "xx");
  rc = CmdParser_ParseCmd(sc, &cmd, args, 5, &err, 1);
  printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_ERR, rc);
  ASSERT(cmd == NULL);
  ASSERT(err != NULL);
  free(args);
  RETURN_TEST_SUCCESS;
}

int testVector() {
  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddNamed(sc, "vec", CmdSchema_NewVector('l'), CmdSchema_Optional);

  CmdString *args = CmdParser_NewArgListV(5, "FOO", "VEC", "2", "1", "2", "3");
  CmdSchemaNode_Print(sc, 0);
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 5, &err, 1);
  if (err != NULL) printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg_Print(cmd, 0);
  free(args);
  CmdArg_Free(cmd);

  RETURN_TEST_SUCCESS;
}

int testNamed() {
  RETURN_TEST_SUCCESS;
}

int testPositional() {
  RETURN_TEST_SUCCESS;
}

int testFlag() {
  RETURN_TEST_SUCCESS
}

int testOption() {
  RETURN_TEST_SUCCESS
}

int testSubSchema() {
  RETURN_TEST_SUCCESS
}

int testRequired() {
  RETURN_TEST_SUCCESS
}

int testRepeating(){RETURN_TEST_SUCCESS} TEST_MAIN({
  TESTFUNC(testSchema);
  TESTFUNC(testTuple);
  TESTFUNC(testVector);
})