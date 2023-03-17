/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "test_util.h"
#include "src/synonym_map.h"
#include "rmutil/alloc.h"

int testSynonymMapAddGetId() {
  SynonymMap* smap = SynonymMap_New(false);
  const char* values1[] = {"val1", "val2", "val3", "val4"};
  const char* values2[] = {"val5", "val6", "val7", "val8"};
  SynonymMap_Add(smap, "g1", values1, 4);
  SynonymMap_Add(smap, "g2", values2, 4);
  TermData* ret_id;

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val1"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val2"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val3"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val4"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val5"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g2");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val6"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g2");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val7"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g2");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val8"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g2");
  SynonymMap_Free(smap);
  RETURN_TEST_SUCCESS;
}

int testSynonymUpdate() {
  SynonymMap* smap = SynonymMap_New(false);
  const char* values[] = {"val1", "val2", "val3", "val4"};
  const char* update_values[] = {"val5", "val6", "val7", "val8"};
  SynonymMap_Add(smap, "g1", values, 4);
  TermData* ret_id;

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val1"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val2"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val3"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val4"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");

  SynonymMap_Update(smap, update_values, 4, "g1");

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val5"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val6"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val7"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val8"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");

  SynonymMap_Free(smap);
  RETURN_TEST_SUCCESS
}

int testSynonymGetReadOnlyCopy() {
  SynonymMap* smap = SynonymMap_New(false);
  const char* values1[] = {"val1", "val2", "val3", "val4"};
  const char* values2[] = {"val5", "val6", "val7", "val8"};
  const char* values3[] = {"val9", "val10", "val11", "val12"};
  const char* values4[] = {"val13", "val14", "val15", "val16"};
  SynonymMap_Add(smap, "g1", values1, 4);
  SynonymMap_Add(smap, "g2", values2, 4);
  SynonymMap_Add(smap, "g3", values3, 4);

  SynonymMap* read_only_copy1 = SynonymMap_GetReadOnlyCopy(smap);
  SynonymMap* read_only_copy2 = SynonymMap_GetReadOnlyCopy(smap);

  ASSERT(read_only_copy1 == read_only_copy2);

  SynonymMap_Add(smap, "g4", values4, 4);

  SynonymMap* read_only_copy3 = SynonymMap_GetReadOnlyCopy(smap);

  ASSERT(read_only_copy3 != read_only_copy2);

  SynonymMap_Free(smap);
  SynonymMap_Free(read_only_copy1);
  SynonymMap_Free(read_only_copy2);
  SynonymMap_Free(read_only_copy3);
  RETURN_TEST_SUCCESS
}

TEST_MAIN({
  // LOGGING_INIT(L_INFO);
  RMUTil_InitAlloc();
  TESTFUNC(testSynonymMapAddGetId);
  TESTFUNC(testSynonymUpdate);
  TESTFUNC(testSynonymGetReadOnlyCopy);
});
