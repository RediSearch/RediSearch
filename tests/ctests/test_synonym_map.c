/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "test_util.h"
#include "synonym_map.h"
#include "spec.h"
#include "rmutil/alloc.h"

int testSynonymMapAddGetId() {
  SynonymMap* smap = SynonymMap_New(false);
  const CharBuf values1[] = {{.buf = "val1", .len = sizeof("val1") - 1},
                             {.buf = "val2", .len = sizeof("val2") - 1},
                             {.buf = "val3", .len = sizeof("val3") - 1},
                             {.buf = "val4", .len = sizeof("val4") - 1}};
  const CharBuf values2[] = {{.buf = "val5", .len = sizeof("val5") - 1},
                             {.buf = "val6", .len = sizeof("val6") - 1},
                             {.buf = "val7", .len = sizeof("val7") - 1},
                             {.buf = "val8", .len = sizeof("val8") - 1}};
  ASSERT(SynonymMap_Update(smap, values1, 4, "g1") == SYNONYM_MAP_OK);
  ASSERT(SynonymMap_Update(smap, values2, 4, "g2") == SYNONYM_MAP_OK);
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
  const CharBuf values[] = {{.buf = "val1", .len = sizeof("val1") - 1},
                            {.buf = "val2", .len = sizeof("val2") - 1},
                            {.buf = "val3", .len = sizeof("val3") - 1},
                            {.buf = "val4", .len = sizeof("val4") - 1}};
  const CharBuf update_values[] = {{.buf = "val5", .len = sizeof("val5") - 1},
                                   {.buf = "val6", .len = sizeof("val6") - 1},
                                   {.buf = "val7", .len = sizeof("val7") - 1},
                                   {.buf = "val8", .len = sizeof("val8") - 1}};
  ASSERT(SynonymMap_Update(smap, values, 4, "g1") == SYNONYM_MAP_OK);
  TermData* ret_id;

  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val1"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val2"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val3"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");
  ASSERT(ret_id = SynonymMap_GetIdsBySynonym_cstr(smap, "val4"));
  ASSERT_STRING_EQ(ret_id->groupIds[0], "~g1");

  ASSERT(SynonymMap_Update(smap, update_values, 4, "g1") == SYNONYM_MAP_OK);

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
  const CharBuf values1[] = {{.buf = "val1", .len = sizeof("val1") - 1},
                             {.buf = "val2", .len = sizeof("val2") - 1},
                             {.buf = "val3", .len = sizeof("val3") - 1},
                             {.buf = "val4", .len = sizeof("val4") - 1}};
  const CharBuf values2[] = {{.buf = "val5", .len = sizeof("val5") - 1},
                             {.buf = "val6", .len = sizeof("val6") - 1},
                             {.buf = "val7", .len = sizeof("val7") - 1},
                             {.buf = "val8", .len = sizeof("val8") - 1}};
  const CharBuf values3[] = {{.buf = "val9", .len = sizeof("val9") - 1},
                             {.buf = "val10", .len = sizeof("val10") - 1},
                             {.buf = "val11", .len = sizeof("val11") - 1},
                             {.buf = "val12", .len = sizeof("val12") - 1}};
  const CharBuf values4[] = {{.buf = "val13", .len = sizeof("val13") - 1},
                             {.buf = "val14", .len = sizeof("val14") - 1},
                             {.buf = "val15", .len = sizeof("val15") - 1},
                             {.buf = "val16", .len = sizeof("val16") - 1}};
  ASSERT(SynonymMap_Update(smap, values1, 4, "g1") == SYNONYM_MAP_OK);
  ASSERT(SynonymMap_Update(smap, values2, 4, "g2") == SYNONYM_MAP_OK);
  ASSERT(SynonymMap_Update(smap, values3, 4, "g3") == SYNONYM_MAP_OK);

  SynonymMap* read_only_copy1 = SynonymMap_GetReadOnlyCopy(smap);
  SynonymMap* read_only_copy2 = SynonymMap_GetReadOnlyCopy(smap);

  ASSERT(read_only_copy1 == read_only_copy2);

  ASSERT(SynonymMap_Update(smap, values4, 4, "g4") == SYNONYM_MAP_OK);

  SynonymMap* read_only_copy3 = SynonymMap_GetReadOnlyCopy(smap);

  ASSERT(read_only_copy3 != read_only_copy2);

  SynonymMap_Free(smap);
  SynonymMap_Free(read_only_copy1);
  SynonymMap_Free(read_only_copy2);
  SynonymMap_Free(read_only_copy3);
  RETURN_TEST_SUCCESS
}

int testSynonymLookupBuffer() {
  SynonymMap* smap = SynonymMap_New(false);
  const CharBuf values[] = {{.buf = "val", .len = sizeof("val") - 1},
                            {.buf = "value", .len = sizeof("value") - 1},
                            {.buf = "", .len = sizeof("") - 1}};
  ASSERT(SynonymMap_Update(smap, values, 3, "g1") == SYNONYM_MAP_OK);
  const char token[] = {'v', 'a', 'l'};
  TermData* term = SynonymMap_GetIdsBySynonym(smap, token, sizeof(token));
  ASSERT(term);
  ASSERT_STRING_EQ(term->groupIds[0], "~g1");
  ASSERT(SynonymMap_GetIdsBySynonym(smap, "value suffix", 5));
  ASSERT(!SynonymMap_GetIdsBySynonym(smap, token, 2));
  ASSERT(SynonymMap_GetIdsBySynonym(smap, token, 0));
  const char embedded_nul[] = {'v', 'a', 'l', '\0', 'x'};
  ASSERT(!SynonymMap_GetIdsBySynonym(smap, embedded_nul, sizeof(embedded_nul)));
  SynonymMap_Free(smap);
  RETURN_TEST_SUCCESS;
}

static int checkSynonymLookupLongToken(SynonymMap** map, SynonymMap** copy, char* token,
                                       size_t len) {
  SynonymMap* smap = *map;
  const CharBuf values[] = {{.buf = "short", .len = sizeof("short") - 1}};
  ASSERT(SynonymMap_Update(smap, values, 1, "g1") == SYNONYM_MAP_OK);
  ASSERT(!SynonymMap_GetIdsBySynonym(smap, token, len));

  const CharBuf long_values[] = {{.buf = token, .len = len}};
  ASSERT(SynonymMap_Update(smap, long_values, 1, "g2") == SYNONYM_MAP_OK);
  TermData* term = SynonymMap_GetIdsBySynonym(smap, token, len);
  ASSERT(term);
  ASSERT_STRING_EQ(term->groupIds[0], "~g2");
  SynonymMap* snapshot = *copy = SynonymMap_GetReadOnlyCopy(smap);
  ASSERT(SynonymMap_Update(smap, long_values, 1, "g3") == SYNONYM_MAP_OK);
  ASSERT(array_len(SynonymMap_GetIdsBySynonym(smap, token, len)->groupIds) == 2);
  SynonymMap_Free(smap);
  *map = NULL;
  term = SynonymMap_GetIdsBySynonym(snapshot, token, len);
  ASSERT(term);
  ASSERT(array_len(term->groupIds) == 1);
  ASSERT_STRING_EQ(term->groupIds[0], "~g2");
  RETURN_TEST_SUCCESS;
}

int testSynonymLookupLongToken() {
  const size_t len = 16 * 1024 * 1024;
  char* token = rm_malloc(len + 1);
  memset(token, 'x', len);
  token[len] = '\0';
  SynonymMap* smap = SynonymMap_New(false);
  SynonymMap* snapshot = NULL;
  int result = checkSynonymLookupLongToken(&smap, &snapshot, token, len);
  if (snapshot) SynonymMap_Free(snapshot);
  if (smap) SynonymMap_Free(smap);
  rm_free(token);
  return result;
}

int testSynonymUpdateBuffers() {
  SynonymMap* smap = SynonymMap_New(false);
  char token[] = {'V', 'A', 'L'};
  const CharBuf terms[] = {
      {.buf = token, .len = sizeof(token)},
      {.buf = "Ⱥ", .len = sizeof("Ⱥ") - 1},
      {.buf = "K", .len = sizeof("K") - 1},
      {.buf = "VAL\0ignored", .len = sizeof("VAL\0ignored") - 1},
      {.buf = "\0ignored", .len = sizeof("\0ignored") - 1},
      {.buf = "", .len = 0},
  };
  ASSERT(SynonymMap_Update(smap, terms, sizeof(terms) / sizeof(*terms), "g1") == SYNONYM_MAP_OK);
  ASSERT(token[0] == 'V');
  ASSERT(dictSize(smap->h_table) == 4);
  const char* expected[] = {"val", "ⱥ", "k", ""};
  for (size_t i = 0; i < sizeof(expected) / sizeof(*expected); ++i) {
    TermData* term = SynonymMap_GetIdsBySynonym_cstr(smap, expected[i]);
    ASSERT(term);
    ASSERT_STRING_EQ(term->term, expected[i]);
    ASSERT(sdslen(term->term) == strlen(expected[i]));
    ASSERT(array_len(term->groupIds) == 1);
    ASSERT_STRING_EQ(term->groupIds[0], "~g1");
  }
  ASSERT(SynonymMap_Update(smap, terms, 1, "g2") == SYNONYM_MAP_OK);
  ASSERT(array_len(SynonymMap_GetIdsBySynonym_cstr(smap, "val")->groupIds) == 2);
  ASSERT(SynonymMap_Update(smap, NULL, 0, "unused") == SYNONYM_MAP_OK);
  SynonymMap_Free(smap);
  RETURN_TEST_SUCCESS;
}

TEST_MAIN({
  // LOGGING_INIT(L_INFO);
  RMUTil_InitAlloc();
  TESTFUNC(testSynonymMapAddGetId);
  TESTFUNC(testSynonymUpdate);
  TESTFUNC(testSynonymGetReadOnlyCopy);
  TESTFUNC(testSynonymLookupBuffer);
  TESTFUNC(testSynonymLookupLongToken);
  TESTFUNC(testSynonymUpdateBuffers);
});
