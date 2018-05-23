#include "test_util.h"
#include "../synonym_map.h"
#include "rmutil/alloc.h"

int testSynonymMapAddGetId() {
  SynonymMap* smap = SynonymMap_New(false);
  const char* values1[] = {"val1", "val2", "val3", "val4"};
  const char* values2[] = {"val5", "val6", "val7", "val8"};
  uint32_t id1 = SynonymMap_Add(smap, values1, 4);
  uint32_t id2 = SynonymMap_Add(smap, values2, 4);
  TermData* ret_id;

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val1"));
  ASSERT_EQUAL(ret_id->ids[0], id1);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val2"));
  ASSERT_EQUAL(ret_id->ids[0], id1);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val3"));
  ASSERT_EQUAL(ret_id->ids[0], id1);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val4"));
  ASSERT_EQUAL(ret_id->ids[0], id1);

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val5"));
  ASSERT_EQUAL(ret_id->ids[0], id2);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val6"));
  ASSERT_EQUAL(ret_id->ids[0], id2);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val7"));
  ASSERT_EQUAL(ret_id->ids[0], id2);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val8"));
  ASSERT_EQUAL(ret_id->ids[0], id2);
  SynonymMap_Free(smap);
  RETURN_TEST_SUCCESS;
}

int testSynonymUpdate() {
  SynonymMap* smap = SynonymMap_New(false);
  const char* values[] = {"val1", "val2", "val3", "val4"};
  const char* update_values[] = {"val5", "val6", "val7", "val8"};
  uint32_t id = SynonymMap_Add(smap, values, 4);
  TermData* ret_id;

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val1"));
  ASSERT_EQUAL(ret_id->ids[0], id);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val2"));
  ASSERT_EQUAL(ret_id->ids[0], id);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val3"));
  ASSERT_EQUAL(ret_id->ids[0], id);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val4"));
  ASSERT_EQUAL(ret_id->ids[0], id);

  SynonymMap_Update(smap, update_values, 4, id);

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val5"));
  ASSERT_EQUAL(ret_id->ids[0], id);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val6"));
  ASSERT_EQUAL(ret_id->ids[0], id);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val7"));
  ASSERT_EQUAL(ret_id->ids[0], id);
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val8"));
  ASSERT_EQUAL(ret_id->ids[0], id);

  SynonymMap_Free(smap);
  RETURN_TEST_SUCCESS
}

int testSynonymGetMaxId() {
  SynonymMap* smap = SynonymMap_New(false);
  const char* values1[] = {"val1", "val2", "val3", "val4"};
  const char* values2[] = {"val5", "val6", "val7", "val8"};
  const char* values3[] = {"val9", "val10", "val11", "val12"};
  SynonymMap_Add(smap, values1, 4);
  SynonymMap_Add(smap, values2, 4);
  SynonymMap_Add(smap, values3, 4);

  ASSERT_EQUAL(SynonymMap_GetMaxId(smap), 3);

  SynonymMap_Free(smap);
  RETURN_TEST_SUCCESS
}

int testSynonymGetReadOnlyCopy() {
  SynonymMap* smap = SynonymMap_New(false);
  const char* values1[] = {"val1", "val2", "val3", "val4"};
  const char* values2[] = {"val5", "val6", "val7", "val8"};
  const char* values3[] = {"val9", "val10", "val11", "val12"};
  const char* values4[] = {"val13", "val14", "val15", "val16"};
  SynonymMap_Add(smap, values1, 4);
  SynonymMap_Add(smap, values2, 4);
  SynonymMap_Add(smap, values3, 4);

  SynonymMap* read_only_copy1 = SynonymMap_GetReadOnlyCopy(smap);
  SynonymMap* read_only_copy2 = SynonymMap_GetReadOnlyCopy(smap);

  ASSERT(read_only_copy1 == read_only_copy2);

  SynonymMap_Add(smap, values4, 4);

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
  TESTFUNC(testSynonymGetMaxId);
  TESTFUNC(testSynonymGetReadOnlyCopy);
});
