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

  CmdString *args = CmdParser_NewArgListV(6, "FOO", "VEC", "3", "1", "2", "3");
  CmdSchemaNode_Print(sc, 0);
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 6, &err, 1);
  if (err != NULL) printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg_Print(cmd, 0);

  CmdArg *v = CmdArg_FirstOf(cmd, "vec");
  ASSERT(v);
  CmdArgIterator it = CmdArg_Children(v);
  int i = 0;
  CmdArg *e = NULL;
  while (NULL != (e = CmdArgIterator_Next(&it))) {
    ASSERT_EQUAL(CmdArg_Integer, e->type);
    ASSERT_EQUAL(++i, e->i);
  }
  ASSERT_EQUAL(3, i);
  free(args);
  CmdArg_Free(cmd);

  // Test out of range
  args = CmdParser_NewArgListV(5, "FOO", "VEC", "3", "1", "2");
  cmd = NULL;
  rc = CmdParser_ParseCmd(sc, &cmd, args, 5, &err, 1);
  if (err != NULL) printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_ERR, rc);
  ASSERT(cmd == NULL);

  // Test parse error
  args = CmdParser_NewArgListV(6, "FOO", "VEC", "3", "1", "2", "x");
  cmd = NULL;
  rc = CmdParser_ParseCmd(sc, &cmd, args, 6, &err, 1);
  if (err != NULL) printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_ERR, rc);
  ASSERT(cmd == NULL);

  RETURN_TEST_SUCCESS;
}

int testNamed() {
  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddNamed(sc, "BAR", CmdSchema_NewArg('s'), CmdSchema_Optional);

  CmdString *args = CmdParser_NewArgListV(3, "FOO", "BAR", "baz");
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 3, &err, 1);
  if (err != NULL) printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg_Print(cmd, 0);

  CmdArg *bar = CmdArg_FirstOf(cmd, "BAR");
  ASSERT(bar);
  ASSERT(bar->type == CmdArg_String);
  ASSERT_STRING_EQ(bar->s.str, "baz");

  RETURN_TEST_SUCCESS;
}

int testPositional() {
  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddPostional(sc, "BAR", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddPostional(sc, "BAZ", CmdSchema_NewArg('l'), CmdSchema_Required);
  CmdSchemaNode_Print(sc, 0);
  CmdString *args = CmdParser_NewArgListV(3, "FOO", "xxx", "123");
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 3, &err, 1);
  printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);

  CmdArg *bar = CmdArg_FirstOf(cmd, "BAR");
  ASSERT(bar->type == CmdArg_String);
  ASSERT_STRING_EQ(bar->s.str, "xxx");

  CmdArg *baz = CmdArg_FirstOf(cmd, "BAZ");
  ASSERT(baz->type == CmdArg_Integer);
  ASSERT_EQUAL(baz->i, 123);
  CmdArg_Print(cmd, 0);
  CmdArg_Free(cmd);
  RETURN_TEST_SUCCESS;
}

int testFlag() {

  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddFlag(sc, "BAR");
  CmdSchema_AddFlag(sc, "BAZ");
  CmdSchemaNode_Print(sc, 0);
  CmdString *args = CmdParser_NewArgListV(2, "FOO", "BAR");
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 2, &err, 1);
  printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg *bar = CmdArg_FirstOf(cmd, "bar");
  ASSERT(bar);
  ASSERT_EQUAL(bar->type, CmdArg_Flag);
  ASSERT(bar->b == 1);

  CmdArg *baz = CmdArg_FirstOf(cmd, "baz");
  ASSERT(baz);
  ASSERT_EQUAL(baz->type, CmdArg_Flag);
  ASSERT(baz->b == 0);
  free(args);
  RETURN_TEST_SUCCESS
}

int testOption() {
  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddPostional(sc, "barvaz", CmdSchema_NewOption(2, (const char *[]){"BAR", "BAZ"}),
                         CmdSchema_Required);

  CmdSchemaNode_Print(sc, 0);
  CmdString *args = CmdParser_NewArgListV(2, "FOO", "BAR");
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 2, &err, 1);
  printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg *barvaz = CmdArg_FirstOf(cmd, "barvaz");
  ASSERT(barvaz);
  ASSERT(barvaz->type == CmdArg_String);
  ASSERT_STRING_EQ(barvaz->s.str, "BAR");
  CmdArg_Free(cmd);
  free(args);

  args = CmdParser_NewArgListV(2, "FOO", "BAZ");
  rc = CmdParser_ParseCmd(sc, &cmd, args, 2, &err, 1);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  barvaz = CmdArg_FirstOf(cmd, "barvaz");
  ASSERT(barvaz);
  ASSERT(barvaz->type == CmdArg_String);
  ASSERT_STRING_EQ(barvaz->s.str, "BAZ");
  CmdArg_Free(cmd);
  free(args);

  args = CmdParser_NewArgListV(2, "FOO", "BGZ");
  rc = CmdParser_ParseCmd(sc, &cmd, args, 2, &err, 1);
  ASSERT_EQUAL(CMDPARSE_ERR, rc);
  ASSERT(cmd == NULL);
  ASSERT(err);
  free(args);

  RETURN_TEST_SUCCESS
}

int testSubSchema() {

  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchemaNode *sub = CmdSchema_AddSubSchema(sc, "SUB", CmdSchema_Required, NULL);
  CmdSchema_AddNamedWithHelp(sub, "BAR", CmdSchema_NewArg('s'), CmdSchema_Required, "Sub Bar");
  CmdSchema_AddNamedWithHelp(sc, "BAR", CmdSchema_NewArg('s'), CmdSchema_Required, "Parent Bar");
  CmdSchemaNode_Print(sc, 0);

  CmdString *args = CmdParser_NewArgListV(6, "FOO", "SUB", "BAR", "baz", "BAR", "gaz");

  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 6, &err, 1);
  printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg *s = CmdArg_FirstOf(cmd, "sub");
  ASSERT(s);
  ASSERT_EQUAL(CmdArg_Object, s->type);
  CmdArg *bar = CmdArg_FirstOf(s, "bar");
  ASSERT(bar);
  ASSERT_EQUAL(CmdArg_String, bar->type);
  ASSERT_STRING_EQ("baz", bar->s.str);

  bar = CmdArg_FirstOf(cmd, "bar");
  ASSERT(bar);
  ASSERT_EQUAL(CmdArg_String, bar->type);
  ASSERT_STRING_EQ("gaz", bar->s.str);
  CmdArg_Free(cmd);
  free(args);
  RETURN_TEST_SUCCESS
}

int testRequired() {
  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddNamed(sc, "BAR", CmdSchema_NewArg('s'), CmdSchema_Optional);
  CmdSchema_AddNamed(sc, "BAZ", CmdSchema_NewArg('s'), CmdSchema_Required);

  CmdString *args = CmdParser_NewArgListV(3, "FOO", "BAZ", "123");
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 3, &err, 1);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  CmdArg_Print(cmd, 0);
  CmdArg_Free(cmd);
  free(args);
  cmd = NULL;
  args = CmdParser_NewArgListV(3, "FOO", "BAR", "123");

  rc = CmdParser_ParseCmd(sc, &cmd, args, 3, &err, 1);
  ASSERT_EQUAL(CMDPARSE_ERR, rc);
  ASSERT(cmd == NULL);
  ASSERT(err);
  free(args);
  free(err);
  RETURN_TEST_SUCCESS
}

int testRepeating() {

  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddNamed(sc, "BAR", CmdSchema_NewArg('l'), CmdSchema_Optional | CmdSchema_Repeating);
  CmdSchema_AddNamed(sc, "BAZ", CmdSchema_NewArg('s'), CmdSchema_Optional);
  CmdString *args =
      CmdParser_NewArgListV(9, "FOO", "BAR", "0", "BAZ", "abc", "BAR", "1", "BAR", "2");
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 9, &err, 1);
  printf("%s\n", err);
  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(cmd != NULL);
  ASSERT(cmd->obj.len == 4);
  CmdArg_Print(cmd, 0);

  CmdArgIterator it = CmdArg_Select(cmd, "bar");
  int i = 0;
  CmdArg *c = NULL;
  while (NULL != (c = CmdArgIterator_Next(&it))) {
    ASSERT_EQUAL(CmdArg_Integer, c->type);
    ASSERT_EQUAL(i++, c->i);
  }
  ASSERT_EQUAL(3, i);
  c = CmdArg_FirstOf(cmd, "baz");
  ASSERT(c);
  ASSERT_EQUAL(CmdArg_String, c->type);
  ASSERT_STRING_EQ("abc", c->s.str);
  CmdArg_Free(cmd);
  free(args);
  RETURN_TEST_SUCCESS;
}

int testStrict() {

  CmdSchemaNode *sc = NewSchema("FOO", "Test command");
  CmdSchema_AddNamed(sc, "BAR", CmdSchema_NewArg('l'), CmdSchema_Optional | CmdSchema_Repeating);
  CmdString *args =
      CmdParser_NewArgListV(9, "FOO", "BAR", "0", "BAR", "1", "BAR", "2", "BAZ", "bag");
  CmdArg *cmd = NULL;
  char *err = NULL;
  int rc = CmdParser_ParseCmd(sc, &cmd, args, 9, &err, 1);

  ASSERT_EQUAL(CMDPARSE_ERR, rc);
  ASSERT(err);
  ASSERT(!cmd);

  rc = CmdParser_ParseCmd(sc, &cmd, args, 9, &err, 0);

  ASSERT_EQUAL(CMDPARSE_OK, rc);
  ASSERT(!err);
  ASSERT(cmd);
  CmdArg_Free(cmd);
  free(args);

  RETURN_TEST_SUCCESS;
}
TEST_MAIN({
  TESTFUNC(testSchema);
  TESTFUNC(testTuple);
  TESTFUNC(testVector);
  TESTFUNC(testNamed);
  TESTFUNC(testPositional);
  TESTFUNC(testFlag);
  TESTFUNC(testOption);
  TESTFUNC(testSubSchema);
  TESTFUNC(testRequired);
  TESTFUNC(testRepeating);
  TESTFUNC(testStrict);

})