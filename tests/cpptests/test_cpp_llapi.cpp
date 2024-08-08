
#include "src/redisearch_api.h"
#include "gtest/gtest.h"
#include "common.h"

#include <set>
#include <string>

#define DOCID1 "doc1"
#define DOCID2 "doc2"
#define DOCID3 "doc3"
#define DOCID4 "doc4"
#define FIELD_NAME_1 "text1"
#define FIELD_NAME_2 "text2"
#define NUMERIC_FIELD_NAME "num"
#define GEO_FIELD_NAME "geo"
#define TAG_FIELD_NAME1 "tag1"
#define TAG_FIELD_NAME2 "tag2"

class LLApiTest : public ::testing::Test {
  virtual void SetUp() {
    RediSearch_Initialize();
    RSGlobalConfig.iteratorsConfigParams.minTermPrefix = 0;
    RSGlobalConfig.iteratorsConfigParams.maxPrefixExpansions = LONG_MAX;
  }

  virtual void TearDown() {
  }
};

using RS::search;

TEST_F(LLApiTest, testGetVersion) {
  ASSERT_EQ(RediSearch_GetCApiVersion(), REDISEARCH_CAPI_VERSION);
}

TEST_F(LLApiTest, testAddDocumentTextField) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  // adding text field to the index
  RediSearch_CreateField(index, FIELD_NAME_1, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  // adding document to the index
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "some test to index", RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  // searching on the index
#define SEARCH_TERM "index"
  RSQNode* qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, SEARCH_TERM);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);

  // test prefix search
  qn = RediSearch_CreatePrefixNode(index, FIELD_NAME_1, "in");
  iter = RediSearch_GetResultsIterator(qn, index);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);

  // search with no results
  qn = RediSearch_CreatePrefixNode(index, FIELD_NAME_1, "nn");
  ASSERT_TRUE(search(index, qn).empty());

  // adding another text field
  RediSearch_CreateField(index, FIELD_NAME_2, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  // adding document to the index with both fields
  d = RediSearch_CreateDocument(DOCID2, strlen(DOCID2), 1.0, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "another indexing testing",
                                     RSFLDTYPE_DEFAULT);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_2, "another indexing testing",
                                     RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  // test prefix search, should return both documents now
  qn = RediSearch_CreatePrefixNode(index, FIELD_NAME_1, "in");
  iter = RediSearch_GetResultsIterator(qn, index);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID2);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);

  // test prefix search on second field, should return only second document
  qn = RediSearch_CreatePrefixNode(index, FIELD_NAME_2, "an");
  iter = RediSearch_GetResultsIterator(qn, index);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID2);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);

  // delete the second document
  int ret = RediSearch_DropDocument(index, DOCID2, strlen(DOCID2));
  ASSERT_EQ(REDISMODULE_OK, ret);

  // searching again, make sure there is no results
  qn = RediSearch_CreatePrefixNode(index, FIELD_NAME_2, "an");
  iter = RediSearch_GetResultsIterator(qn, index);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testAddDocumentNumericField) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  // adding numeric field to the index
  RediSearch_CreateNumericField(index, NUMERIC_FIELD_NAME);

  // adding document to the index
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddFieldNumber(d, NUMERIC_FIELD_NAME, 20, RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  // searching on the index
  RSQNode* qn = RediSearch_CreateNumericNode(index, NUMERIC_FIELD_NAME, 30, 10, 0, 0);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter != NULL);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);
  RediSearch_ResultsIteratorFree(iter);

  // searching on the index
  qn = RediSearch_CreateNumericNode(index, NUMERIC_FIELD_NAME, RSRANGE_INF, 10, 0, 0);
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter != NULL);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testAddDocumentGeoField) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  // adding geo point field to the index
  RediSearch_CreateGeoField(index, GEO_FIELD_NAME);

  // adding document to the index
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  // check error on lat > GEO_LAT_MAX
  int res = RediSearch_DocumentAddFieldGeo(d, GEO_FIELD_NAME, 100, 0, RSFLDTYPE_DEFAULT);
  ASSERT_EQ(res, REDISMODULE_ERR);
  // check error on lon > GEO_LON_MAX
  res = RediSearch_DocumentAddFieldGeo(d, GEO_FIELD_NAME, 0, 200, RSFLDTYPE_DEFAULT);
  ASSERT_EQ(res, REDISMODULE_ERR);
  // valid geo point
  res = RediSearch_DocumentAddFieldGeo(d, GEO_FIELD_NAME, 20.654321, 0.123456, RSFLDTYPE_DEFAULT);
  ASSERT_EQ(res, REDISMODULE_OK);
  RediSearch_SpecAddDocument(index, d);

  // error while searching the index
  size_t len;
  const char* id;
  // radius < 0
  RSQNode* qn = RediSearch_CreateGeoNode(index, GEO_FIELD_NAME, 20.6543222, 0.123455, -1, RS_GEO_DISTANCE_M);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(iter);

  // lat > MAX_LAT
  qn = RediSearch_CreateGeoNode(index, GEO_FIELD_NAME, 100, 0.123455, 10, RS_GEO_DISTANCE_M);
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(iter);

  // 90 > lat > 85
  // we receive an EOF iterator
  qn = RediSearch_CreateGeoNode(index, GEO_FIELD_NAME, 87, 0.123455, 10, RS_GEO_DISTANCE_M);
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);
  RediSearch_ResultsIteratorFree(iter);

  // lon > MAX_LON
  qn = RediSearch_CreateGeoNode(index, GEO_FIELD_NAME, 20.6543222, 200, 10, RS_GEO_DISTANCE_M);
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(iter);

  // searching on the index
  qn = RediSearch_CreateGeoNode(index, GEO_FIELD_NAME, 20.6543222, 0.123455, 10, RS_GEO_DISTANCE_M);
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);
  RediSearch_ResultsIteratorFree(iter);

  // searching on the index and getting NULL result
  qn = RediSearch_CreateGeoNode(index, GEO_FIELD_NAME, 20.6543000, 0.123000, 10, RS_GEO_DISTANCE_M);
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter != NULL);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);
  RediSearch_ResultsIteratorFree(iter);

  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testAddDocumentNumericFieldWithMoreThenOneNode) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  // adding text field to the index
  RediSearch_CreateNumericField(index, NUMERIC_FIELD_NAME);

  // adding document to the index
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddFieldNumber(d, NUMERIC_FIELD_NAME, 20, RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  // adding document to the index
  d = RediSearch_CreateDocument(DOCID2, strlen(DOCID2), 1.0, NULL);
  RediSearch_DocumentAddFieldNumber(d, NUMERIC_FIELD_NAME, 40, RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  // searching on the index
  RSQNode* qn = RediSearch_CreateNumericNode(index, NUMERIC_FIELD_NAME, 30, 10, 0, 0);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter != NULL);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testAddDocumetTagField) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  // adding text field to the index
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);

  // adding document to the index
#define TAG_VALUE "tag_value"
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddFieldCString(d, TAG_FIELD_NAME1, TAG_VALUE, RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  // searching on the index
  RSQNode* qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* tqn = RediSearch_CreateTagTokenNode(index, TAG_VALUE);
  RediSearch_QueryNodeAddChild(qn, tqn);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);

  // prefix search on the index
  qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  tqn = RediSearch_CreateTagPrefixNode(index, "ta");
  RediSearch_QueryNodeAddChild(qn, tqn);
  iter = RediSearch_GetResultsIterator(qn, index);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testPhoneticSearch) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateField(index, FIELD_NAME_1, RSFLDTYPE_FULLTEXT, RSFLDOPT_TXTPHONETIC);
  RediSearch_CreateField(index, FIELD_NAME_2, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "felix", RSFLDTYPE_DEFAULT);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_2, "felix", RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  // make sure phonetic search works on field1
  RSQNode* qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "phelix");
  auto res = search(index, qn);
  ASSERT_EQ(1, res.size());
  ASSERT_EQ(DOCID1, res[0]);

  // make sure phonetic search on field2 do not return results
  qn = RediSearch_CreateTokenNode(index, FIELD_NAME_2, "phelix");
  res = search(index, qn);
  ASSERT_EQ(0, res.size());
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testMassivePrefix) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);

  char buff[1024];
  int NUM_OF_DOCS = 1000;
  for (int i = 0; i < NUM_OF_DOCS; ++i) {
    sprintf(buff, "doc%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    sprintf(buff, "tag-%d", i);
    RediSearch_DocumentAddFieldCString(d, TAG_FIELD_NAME1, buff, RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(index, d);
  }

  RSQNode* qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* pqn = RediSearch_CreateTagPrefixNode(index, "tag-");
  RediSearch_QueryNodeAddChild(qn, pqn);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter);

  for (size_t i = 0; i < NUM_OF_DOCS; ++i) {
    size_t len;
    const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
    ASSERT_TRUE(id);
  }

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

const char *words[] = {"he", "her", "hell", "help", "helper", "hello",
                        "hello world", "towel", "dealer", "bell"};

void loadDocsText(RSIndex *index) {
  char buff[16];
  for (int i = 0; i < 10; ++i) {
    sprintf(buff, "%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, words[i], RSFLDTYPE_DEFAULT);
    RediSearch_DocumentAddFieldCString(d, FIELD_NAME_2, words[i], RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(index, d);
  }
}

TEST_F(LLApiTest, testContainsText) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTextField(index, FIELD_NAME_1);
  RediSearch_CreateField(index, FIELD_NAME_2, RSFLDTYPE_FULLTEXT, RSFLDOPT_WITHSUFFIXTRIE);
  loadDocsText(index);

  RSQNode* qn = RediSearch_CreateContainsNode(index, FIELD_NAME_1, "el");
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(iter);

  qn = RediSearch_CreateContainsNode(index, FIELD_NAME_2, "el");
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter);

  const char* id;
  size_t len;
  int ii = 0;
  while ((id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len))) {
    ASSERT_TRUE(strstr(words[*id - '0'], "el") != NULL);
    ++ii;
  }
  ASSERT_EQ(ii, 7);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testSuffixText) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTextField(index, FIELD_NAME_1);
  RediSearch_CreateField(index, FIELD_NAME_2, RSFLDTYPE_FULLTEXT, RSFLDOPT_WITHSUFFIXTRIE);
  loadDocsText(index);

  RSQNode* qn = RediSearch_CreateSuffixNode(index, FIELD_NAME_1, "er");
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(iter);

  qn = RediSearch_CreateSuffixNode(index, FIELD_NAME_2, "er");
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter);

  const char* id;
  size_t len;
  int ii = 0;
  while ((id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len))) {
    ASSERT_TRUE(strstr(words[*id - '0'], "er") != NULL);
    ++ii;
  }
  ASSERT_EQ(ii, 3);

  RediSearch_ResultsIteratorFree(iter);

  RediSearch_DropIndex(index);
}


void loadDocsTag(RSIndex *index) {
  char buff[16];
  for (int i = 0; i < 10; ++i) {
    sprintf(buff, "%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    RediSearch_DocumentAddFieldCString(d, TAG_FIELD_NAME1, words[i], RSFLDTYPE_DEFAULT);
    RediSearch_DocumentAddFieldCString(d, TAG_FIELD_NAME2, words[i], RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(index, d);
  }
}

TEST_F(LLApiTest, testContainsTag) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);
  RediSearch_CreateField(index, TAG_FIELD_NAME2, RSFLDTYPE_TAG, RSFLDOPT_WITHSUFFIXTRIE);
  loadDocsTag(index);

  const char *fields[] = {TAG_FIELD_NAME1, TAG_FIELD_NAME2};
  for (int i = 0; i < 2; ++i) {
    RSQNode* qn = RediSearch_CreateTagNode(index, fields[i]);
    RSQNode* pqn = RediSearch_CreateTagContainsNode(index, "el");
    RediSearch_QueryNodeAddChild(qn, pqn);
    RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
    ASSERT_TRUE(iter);

    const char* id;
    size_t len;
    int ii = 0;
    while ((id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len))) {
      ASSERT_TRUE(strstr(words[*id - '0'], "el") != NULL);
      ++ii;
    }
    ASSERT_EQ(ii, 7);

    RediSearch_ResultsIteratorFree(iter);
  }
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testSuffixTag) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);
  RediSearch_CreateField(index, TAG_FIELD_NAME2, RSFLDTYPE_TAG, RSFLDOPT_WITHSUFFIXTRIE);
  loadDocsTag(index);

  const char *fields[] = {TAG_FIELD_NAME1, TAG_FIELD_NAME2};
  for (int i = 0; i < 2; ++i) {
    RSQNode* qn = RediSearch_CreateTagNode(index, fields[i]);
    RSQNode* pqn = RediSearch_CreateTagSuffixNode(index, "ell");
    RediSearch_QueryNodeAddChild(qn, pqn);
    RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
    ASSERT_TRUE(iter);

    const char* id;
    size_t len;
    int ii = 0;
    while ((id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len))) {
      ASSERT_TRUE(strstr(words[*id - '0'], "ell") != NULL);
      ++ii;
    }
    ASSERT_EQ(ii, 2);

    RediSearch_ResultsIteratorFree(iter);
  }
  RediSearch_DropIndex(index);
}

static void PopulateIndex(RSIndex* index) {
  char buf[] = {"Mark_"};
  size_t nbuf = strlen(buf);
  for (char c = 'a'; c <= 'z'; c++) {
    buf[nbuf - 1] = c;
    char did[64];
    sprintf(did, "doc%c", c);
    RSDoc* d = RediSearch_CreateDocument(did, strlen(did), 0, NULL);
    RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, buf, RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(index, d);
  }
}

static void ValidateResults(RSIndex* index, RSQNode* qn, char start, char end, int numResults) {
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(NULL == iter);
  std::set<std::string> results;
  const char* id;
  size_t nid;
  while ((id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &nid))) {
    std::string idstr(id, nid);
    ASSERT_EQ(results.end(), results.find(idstr));
    results.insert(idstr);
  }

  ASSERT_EQ(numResults, results.size());
  for (char c = start; c <= end; c++) {
    char namebuf[64];
    sprintf(namebuf, "doc%c", c);
    ASSERT_NE(results.end(), results.find(namebuf));
  }
  RediSearch_ResultsIteratorFree(iter);
}

TEST_F(LLApiTest, testRanges) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTextField(index, FIELD_NAME_1);

  PopulateIndex(index);

  RSQNode* qn = RediSearch_CreateLexRangeNode(index, FIELD_NAME_1, "MarkN", "MarkX", 1, 1);

  ValidateResults(index, qn, 'n', 'x', 11);

  qn = RediSearch_CreateLexRangeNode(index, FIELD_NAME_1, "MarkN", "MarkX", 0, 0);

  ValidateResults(index, qn, 'o', 'w', 9);

  qn = RediSearch_CreateLexRangeNode(index, FIELD_NAME_1, NULL, NULL, 1, 1);

  ValidateResults(index, qn, 'a', 'z', 26);

  // printf("Have %lu ids in range!\n", results.size());
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testRangesOnTags) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTagField(index, FIELD_NAME_1);

  PopulateIndex(index);

  // test with include max and min
  RSQNode* tagQn = RediSearch_CreateTagNode(index, FIELD_NAME_1);
  RSQNode* qn = RediSearch_CreateTagLexRangeNode(index, "Markn", "Markx", 1, 1);
  RediSearch_QueryNodeAddChild(tagQn, qn);

  ValidateResults(index, tagQn, 'n', 'x', 11);

  // test without include max and min
  tagQn = RediSearch_CreateTagNode(index, FIELD_NAME_1);
  qn = RediSearch_CreateTagLexRangeNode(index, "Markn", "Markx", 0, 0);
  RediSearch_QueryNodeAddChild(tagQn, qn);

  ValidateResults(index, tagQn, 'o', 'w', 9);

  tagQn = RediSearch_CreateTagNode(index, FIELD_NAME_1);
  qn = RediSearch_CreateTagLexRangeNode(index, NULL, NULL, 1, 1);
  RediSearch_QueryNodeAddChild(tagQn, qn);

  ValidateResults(index, tagQn, 'a', 'z', 26);

  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testRangesOnTagsWithOneNode) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateTagField(index, FIELD_NAME_1);

  RSDoc* d = RediSearch_CreateDocument("doc1", strlen("doc1"), 0, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "C", RSFLDTYPE_TAG);
  RediSearch_SpecAddDocument(index, d);

  // test with include max and min
  RSQNode* tagQn = RediSearch_CreateTagNode(index, FIELD_NAME_1);
  RSQNode* qn = RediSearch_CreateTagLexRangeNode(index, "C", RSLECRANGE_INF, 0, 1);
  RediSearch_QueryNodeAddChild(tagQn, qn);

  RSResultsIterator* iter = RediSearch_GetResultsIterator(tagQn, index);

  ASSERT_FALSE(NULL == iter);
  size_t nid;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &nid);
  ASSERT_TRUE(id == NULL);

  RediSearch_ResultsIteratorFree(iter);

  tagQn = RediSearch_CreateTagNode(index, FIELD_NAME_1);
  qn = RediSearch_CreateTagLexRangeNode(index, RSLECRANGE_INF, "C", 1, 0);
  RediSearch_QueryNodeAddChild(tagQn, qn);

  iter = RediSearch_GetResultsIterator(tagQn, index);

  ASSERT_FALSE(NULL == iter);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &nid);
  ASSERT_TRUE(id == NULL);

  RediSearch_ResultsIteratorFree(iter);

  RediSearch_DropIndex(index);
}

static char buffer[1024];

static int GetValue(void* ctx, const char* fieldName, const void* id, char** strVal,
                    double* doubleVal) {
  *strVal = buffer;
  int numId;
  sscanf((char*)id, "doc%d", &numId);
  if (strcmp(fieldName, TAG_FIELD_NAME1) == 0) {
    sprintf(*strVal, "tag1-%d", numId);
  } else {
    sprintf(*strVal, "tag2-%d", numId);
  }
  return RSVALTYPE_STRING;
}

TEST_F(LLApiTest, testMassivePrefixWithUnsortedSupport) {
  // creating the index
  RSIndexOptions* options = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetGetValueCallback(options, GetValue, NULL);
  RSIndex* index = RediSearch_CreateIndex("index", options);
  RediSearch_FreeIndexOptions(options);

  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);

  char buff[1024];
  int NUM_OF_DOCS = 10000;
  for (int i = 0; i < NUM_OF_DOCS; ++i) {
    sprintf(buff, "doc%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    sprintf(buff, "tag-%d", i);
    RediSearch_DocumentAddFieldCString(d, TAG_FIELD_NAME1, buff, RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(index, d);
  }

  RSQNode* qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* pqn = RediSearch_CreateTagPrefixNode(index, "tag-");
  RediSearch_QueryNodeAddChild(qn, pqn);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_TRUE(iter);

  for (size_t i = 0; i < NUM_OF_DOCS; ++i) {
    size_t len;
    const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
    ASSERT_TRUE(id);
  }

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testPrefixIntersection) {
  // creating the index
  RSIndexOptions* options = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetGetValueCallback(options, GetValue, NULL);
  RSIndex* index = RediSearch_CreateIndex("index", options);
  RediSearch_FreeIndexOptions(options);

  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME2);

  char buff[1024];
  int NUM_OF_DOCS = 1000;
  for (int i = 0; i < NUM_OF_DOCS; ++i) {
    sprintf(buff, "doc%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    sprintf(buff, "tag1-%d", i);
    RediSearch_DocumentAddFieldCString(d, TAG_FIELD_NAME1, buff, RSFLDTYPE_DEFAULT);
    sprintf(buff, "tag2-%d", i);
    RediSearch_DocumentAddFieldCString(d, TAG_FIELD_NAME2, buff, RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(index, d);
  }

  RSQNode* qn1 = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* pqn1 = RediSearch_CreateTagPrefixNode(index, "tag1-");
  RediSearch_QueryNodeAddChild(qn1, pqn1);
  RSQNode* qn2 = RediSearch_CreateTagNode(index, TAG_FIELD_NAME2);
  RSQNode* pqn2 = RediSearch_CreateTagPrefixNode(index, "tag2-");
  RediSearch_QueryNodeAddChild(qn2, pqn2);
  RSQNode* iqn = RediSearch_CreateIntersectNode(index, 0);
  RediSearch_QueryNodeAddChild(iqn, qn1);
  RediSearch_QueryNodeAddChild(iqn, qn2);

  RSResultsIterator* iter = RediSearch_GetResultsIterator(iqn, index);
  ASSERT_TRUE(iter);

  for (size_t i = 0; i < NUM_OF_DOCS; ++i) {
    size_t len;
    const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
    ASSERT_STRNE(id, NULL);
  }

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testMultitype) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  auto f = RediSearch_CreateField(index, "f1", RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);
  ASSERT_NE(RSFIELD_INVALID, f);
  f = RediSearch_CreateField(index, "f2", RSFLDTYPE_FULLTEXT | RSFLDTYPE_TAG | RSFLDTYPE_NUMERIC,
                             RSFLDOPT_NONE);

  // Add document...
  RSDoc* d = RediSearch_CreateDocumentSimple("doc1");
  RediSearch_DocumentAddFieldCString(d, "f1", "hello", RSFLDTYPE_FULLTEXT);
  RediSearch_DocumentAddFieldCString(d, "f2", "world", RSFLDTYPE_FULLTEXT | RSFLDTYPE_TAG);
  int rc = RediSearch_SpecAddDocument(index, d);
  ASSERT_EQ(REDISMODULE_OK, rc);

  // Done
  // Now search for them...
  auto qn = RediSearch_CreateTokenNode(index, "f1", "hello");
  auto results = search(index, qn);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("doc1", results[0]);

  qn = RediSearch_CreateTagNode(index, "f2");
  RediSearch_QueryNodeAddChild(qn, RediSearch_CreateTagTokenNode(index, "world"));
  results = search(index, qn);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("doc1", results[0]);

  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testMultitypeNumericTag) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RSFieldID f1 =
      RediSearch_CreateField(index, "f1", RSFLDTYPE_TAG | RSFLDTYPE_NUMERIC, RSFLDOPT_NONE);
  RSFieldID f2 =
      RediSearch_CreateField(index, "f2", RSFLDTYPE_TAG | RSFLDTYPE_NUMERIC, RSFLDOPT_NONE);

  RediSearch_TagFieldSetCaseSensitive(index, f1, 1);

  // Add document...
  RSDoc* d = RediSearch_CreateDocumentSimple("doc1");
  RediSearch_DocumentAddFieldCString(d, "f1", "World", RSFLDTYPE_TAG);
  RediSearch_DocumentAddFieldCString(d, "f2", "world", RSFLDTYPE_TAG);
  int rc = RediSearch_SpecAddDocument(index, d);
  ASSERT_EQ(REDISMODULE_OK, rc);

  auto qn = RediSearch_CreateTagNode(index, "f2");
  RediSearch_QueryNodeAddChild(qn,
                               RediSearch_CreateTagLexRangeNode(index, "world", "world", 1, 1));
  std::vector<std::string> results = search(index, qn);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("doc1", results[0]);

  qn = RediSearch_CreateTagNode(index, "f1");
  RediSearch_QueryNodeAddChild(qn,
                               RediSearch_CreateTagLexRangeNode(index, "world", "world", 1, 1));
  results = search(index, qn);
  ASSERT_EQ(0, results.size());

  qn = RediSearch_CreateTagNode(index, "f1");
  RediSearch_QueryNodeAddChild(qn,
                               RediSearch_CreateTagLexRangeNode(index, "World", "world", 1, 1));
  results = search(index, qn);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("doc1", results[0]);

  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testQueryString) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateField(index, "ft1", RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);
  RediSearch_CreateField(index, "ft2", RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);
  RediSearch_CreateField(index, "n1", RSFLDTYPE_NUMERIC, RSFLDOPT_NONE);
  RediSearch_CreateField(index, "tg1", RSFLDTYPE_TAG, RSFLDOPT_NONE);

  // Insert the documents...
  for (size_t ii = 0; ii < 100; ++ii) {
    char docbuf[1024] = {0};
    sprintf(docbuf, "doc%lu\n", ii);
    Document* d = RediSearch_CreateDocumentSimple(docbuf);
    // Fill with fields..
    sprintf(docbuf, "hello%lu\n", ii);
    RediSearch_DocumentAddFieldCString(d, "ft1", docbuf, RSFLDTYPE_DEFAULT);
    sprintf(docbuf, "world%lu\n", ii);
    RediSearch_DocumentAddFieldCString(d, "ft2", docbuf, RSFLDTYPE_DEFAULT);
    sprintf(docbuf, "tag%lu\n", ii);
    RediSearch_DocumentAddFieldCString(d, "tg1", docbuf, RSFLDTYPE_TAG);
    RediSearch_DocumentAddFieldNumber(d, "n1", ii, RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(index, d);
  }

  // Issue a query
  auto res = search(index, "hello*");
  ASSERT_EQ(100, res.size());

  res = search(index, "@ft1:hello*");
  ASSERT_EQ(100, res.size());

  res = search(index, "(@ft1:hello1)|(@ft1:hello50)");
  ASSERT_EQ(2, res.size());
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testDocumentExists) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  RediSearch_CreateField(index, "ft1", RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  const char* docid = "doc1";
  Document* d = RediSearch_CreateDocumentSimple(docid);
  RediSearch_DocumentAddFieldCString(d, "ft1", "test", RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  ASSERT_TRUE(RediSearch_DocumentExists(index, docid, strlen(docid)));

  RediSearch_DropIndex(index);
}

int RSGetValue(void* ctx, const char* fieldName, const void* id, char** strVal, double* doubleVal) {
  return 0;
}

TEST_F(LLApiTest, testNumericFieldWithCT) {
  RediSearch_SetCriteriaTesterThreshold(1);

  RSIndexOptions* opt = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetGetValueCallback(opt, RSGetValue, NULL);

  RSIndex* index = RediSearch_CreateIndex("index", opt);
  RediSearch_CreateField(index, "ft1", RSFLDTYPE_NUMERIC, RSFLDOPT_NONE);

  Document* d = RediSearch_CreateDocumentSimple("doc1");
  RediSearch_DocumentAddFieldNumber(d, "ft1", 20, RSFLDTYPE_NUMERIC);
  RediSearch_SpecAddDocument(index, d);

  d = RediSearch_CreateDocumentSimple("doc2");
  RediSearch_DocumentAddFieldNumber(d, "ft1", 0, RSFLDTYPE_NUMERIC);
  RediSearch_SpecAddDocument(index, d);

  RSQNode* qn1 = RediSearch_CreateNumericNode(index, "ft1", 70, -10, 0, 0);
  RSQNode* qn2 = RediSearch_CreateNumericNode(index, "ft1", 70, -10, 0, 0);
  RSQNode* un = RediSearch_CreateUnionNode(index);
  RediSearch_QueryNodeAddChild(un, qn1);
  RediSearch_QueryNodeAddChild(un, qn2);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(un, index);
  ASSERT_TRUE(iter != NULL);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, "doc1");
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, "doc2");
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
  RediSearch_FreeIndexOptions(opt);
  RediSearch_SetCriteriaTesterThreshold(0);
}

TEST_F(LLApiTest, testUnionWithEmptyNodes) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  RSQNode* qn1 = RediSearch_CreateEmptyNode(index);
  RSQNode* qn2 = RediSearch_CreateEmptyNode(index);

  RSQNode* un = RediSearch_CreateUnionNode(index);
  RediSearch_QueryNodeAddChild(un, qn1);
  RediSearch_QueryNodeAddChild(un, qn2);

  RSResultsIterator* iter = RediSearch_GetResultsIterator(un, index);
  ASSERT_TRUE(iter != NULL);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testIntersectWithEmptyNodes) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  RSQNode* qn1 = RediSearch_CreateEmptyNode(index);
  RSQNode* qn2 = RediSearch_CreateEmptyNode(index);

  RSQNode* un = RediSearch_CreateIntersectNode(index, 0);
  RediSearch_QueryNodeAddChild(un, qn1);
  RediSearch_QueryNodeAddChild(un, qn2);

  RSResultsIterator* iter = RediSearch_GetResultsIterator(un, index);
  ASSERT_TRUE(iter != NULL);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testNotNodeWithEmptyNode) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  RSQNode* qn1 = RediSearch_CreateEmptyNode(index);

  RSQNode* un = RediSearch_CreateNotNode(index);
  RediSearch_QueryNodeAddChild(un, qn1);

  RSResultsIterator* iter = RediSearch_GetResultsIterator(un, index);
  ASSERT_TRUE(iter != NULL);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testFreeDocument) {
  auto* d = RediSearch_CreateDocument("doc1", strlen("doc1"), 1, "turkish");
  RediSearch_FreeDocument(d);
}

TEST_F(LLApiTest, duplicateFieldAdd) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  // adding text field to the index
  RediSearch_CreateField(index, FIELD_NAME_1, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  // adding document to the index
  Document* d = RediSearch_CreateDocumentSimple("doc1");

  // adding same field twice
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "some test to field", RSFLDTYPE_DEFAULT);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "some test to same field", RSFLDTYPE_DEFAULT);
  ASSERT_EQ(RediSearch_SpecAddDocument(index, d), REDISMODULE_ERR);
  ASSERT_FALSE(RediSearch_DocumentExists(index, "doc1", strlen("doc1")));

  RediSearch_FreeDocument(d);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testScorer) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL);

  // adding text field to the index
  RediSearch_CreateField(index, FIELD_NAME_1, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  // adding documents to the index
  Document* d1 = RediSearch_CreateDocumentSimple("doc1");
  Document* d2 = RediSearch_CreateDocumentSimple("doc2");

  // adding document with a different TFIDF score
  RediSearch_DocumentAddFieldCString(d1, FIELD_NAME_1, "hello world hello world", RSFLDTYPE_DEFAULT);
  ASSERT_EQ(RediSearch_SpecAddDocument(index, d1), REDISMODULE_OK);
  RediSearch_DocumentAddFieldCString(d2, FIELD_NAME_1, "hello world hello", RSFLDTYPE_DEFAULT);
  ASSERT_EQ(RediSearch_SpecAddDocument(index, d2), REDISMODULE_OK);

  const char *s = "hello world";
  RSResultsIterator *it = RediSearch_IterateQuery(index, s, strlen(s), NULL);
  RediSearch_ResultsIteratorNext(it, index, NULL);
  ASSERT_EQ(RediSearch_ResultsIteratorGetScore(it), 2);
  RediSearch_ResultsIteratorNext(it, index, NULL);
  ASSERT_EQ(RediSearch_ResultsIteratorGetScore(it), 1.5);

  RediSearch_ResultsIteratorFree(it);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testStopwords) {
  // Check default stopword list
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  ASSERT_EQ(RediSearch_StopwordsList_Contains(index, "is", strlen("is")), 1);
  ASSERT_EQ(RediSearch_StopwordsList_Contains(index, "Redis", strlen("Redis")), 0);
  // check creation of token node
  RSQNode *node = RediSearch_CreateTokenNode(index, "doesnt_matter", "is");
  ASSERT_EQ((size_t)node, 0);
  node = RediSearch_CreateTokenNode(index, "doesnt_matter", "Redis");
  ASSERT_NE((size_t)node, 0);
  RediSearch_QueryNodeFree(node);
  RediSearch_DropIndex(index);

  // Check custom stopword list
  const char *words[] = { "Labs", "Redis"};
  RSIndexOptions *options = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetStopwords(options, words, 2);

  index = RediSearch_CreateIndex("index", options);
  ASSERT_EQ(RediSearch_StopwordsList_Contains(index, words[0], strlen(words[0])), 1);
  ASSERT_EQ(RediSearch_StopwordsList_Contains(index, words[1], strlen(words[1])), 1);
  ASSERT_EQ(RediSearch_StopwordsList_Contains(index, "RediSearch", strlen("RediSearch")), 0);

  size_t size;
  char **list = RediSearch_IndexGetStopwords(index, &size);
  ASSERT_EQ(size, 2);
  ASSERT_STRCASEEQ(list[0], words[0]);
  ASSERT_STRCASEEQ(list[1], words[1]);
  RediSearch_StopwordsList_Free(list, 2);

  RediSearch_FreeIndexOptions(options);
  RediSearch_DropIndex(index);

  // Check empty stopword list
  options = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetStopwords(options, NULL, 0);

  index = RediSearch_CreateIndex("index", options);
  ASSERT_EQ(RediSearch_StopwordsList_Contains(index, "is", strlen("is")), 0);
  ASSERT_EQ(RediSearch_StopwordsList_Contains(index, words[0], strlen(words[0])), 0);
  RediSearch_FreeIndexOptions(options);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testGetters) {
  // test defaults
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  ASSERT_EQ(DEFAULT_SCORE, RediSearch_IndexGetScore(index));
  ASSERT_STREQ(RSLanguage_ToString(DEFAULT_LANGUAGE), RediSearch_IndexGetLanguage(index));
  RediSearch_DropIndex(index);

  // test custom language and score
  RSIndexOptions *opt = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetScore(opt, 0.42);
  RediSearch_IndexOptionsSetLanguage(opt, "YIDDISH");

  index = RediSearch_CreateIndex("index", opt);
  ASSERT_EQ(0.42, RediSearch_IndexGetScore(index));
  ASSERT_STREQ("yiddish", RediSearch_IndexGetLanguage(index));

  RSDoc* d = RediSearch_CreateDocument2(DOCID1, strlen(DOCID1), index, NAN, NULL);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  ASSERT_EQ(d->score, (float)RediSearch_IndexGetScore(index));
  RediSearch_FreeDocument(d);

  RediSearch_FreeIndexOptions(opt);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testIndexWithDefaultLanguage) {
  // TEST using Default language: English
  RSIndex* index = RediSearch_CreateIndex("index_en", NULL);
  ASSERT_STREQ(RSLanguage_ToString(RS_LANG_ENGLISH), RediSearch_IndexGetLanguage(index));
  RediSearch_CreateField(index, FIELD_NAME_1, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  // create a doc without specifying the language, 
  // it should use the language per index: English
  RSDoc* d = RediSearch_CreateDocument2(DOCID1, strlen(DOCID1), index, NAN, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "cherry", RSFLDTYPE_DEFAULT);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  RediSearch_SpecAddDocument(index, d);

  d = RediSearch_CreateDocument2(DOCID2, strlen(DOCID2), index, NAN, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "cherries", RSFLDTYPE_DEFAULT);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  RediSearch_SpecAddDocument(index, d);

  d = RediSearch_CreateDocument2(DOCID3, strlen(DOCID3), index, NAN, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "cheers", RSFLDTYPE_DEFAULT);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  RediSearch_SpecAddDocument(index, d);

  // The search should use language per index, and stemming should work, 
  // returning 2 documents
  RSQNode* qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "cherries");
  std::vector<std::string> res = search(index, qn);
  ASSERT_EQ(2, res.size());

  qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "cherry");
  res = search(index, qn);
  ASSERT_EQ(2, res.size());

  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testIndexWithCustomLanguage) {
  // create index using language Italian
  RSIndexOptions *opt = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetLanguage(opt, RSLanguage_ToString(RS_LANG_ITALIAN));
  RSIndex* index = RediSearch_CreateIndex("index_it", opt);
  ASSERT_STREQ(RSLanguage_ToString(RS_LANG_ITALIAN), RediSearch_IndexGetLanguage(index));
  RediSearch_CreateField(index, FIELD_NAME_1, RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);

  // create a doc without specifying the language, 
  // it should use the language per index: Italian
  RSDoc* d = RediSearch_CreateDocument2(DOCID1, strlen(DOCID1), index, NAN, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "arance", RSFLDTYPE_DEFAULT);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  RediSearch_SpecAddDocument(index, d);

  d = RediSearch_CreateDocument2(DOCID2, strlen(DOCID2), index, NAN, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "arancia", RSFLDTYPE_DEFAULT);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  RediSearch_SpecAddDocument(index, d);

  d = RediSearch_CreateDocument2(DOCID3, strlen(DOCID3), index, NAN, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "cherry", RSFLDTYPE_DEFAULT);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  RediSearch_SpecAddDocument(index, d);

  d = RediSearch_CreateDocument2(DOCID4, strlen(DOCID4), index, NAN, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "cherries", RSFLDTYPE_DEFAULT);
  ASSERT_STREQ(RSLanguage_ToString(d->language), RediSearch_IndexGetLanguage(index));
  RediSearch_SpecAddDocument(index, d);

  // The search should use the language per index: Italian
  RSQNode* qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "arancia");
  std::vector<std::string> res = search(index, qn);
  ASSERT_EQ(2, res.size());

  qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "arance");
  res = search(index, qn);
  ASSERT_EQ(2, res.size());

  // The search for cherry/cherries should return 1 document, because the word is 
  // not stemmed correctly in Italian
  qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "cherry");
  res = search(index, qn);
  ASSERT_EQ(1, res.size());

  qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "cherries");
  res = search(index, qn);
  ASSERT_EQ(1, res.size());

  RediSearch_FreeIndexOptions(opt);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testInfo) {
  RSIndexOptions *opt = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetGCPolicy(opt, GC_POLICY_FORK);
  RediSearch_IndexOptionsSetScore(opt, 0.141);
  RediSearch_IndexOptionsSetLanguage(opt, "YIDDISH");

  RSIndex* index = RediSearch_CreateIndex("index", opt);

  RSFieldID fieldID;
  fieldID = RediSearch_CreateField(index, "ft1", RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);
  RediSearch_TextFieldSetWeight(index, fieldID, 2.3);
  RediSearch_CreateField(index, "ft2", RSFLDTYPE_FULLTEXT, RSFLDOPT_TXTNOSTEM);
  RediSearch_CreateField(index, "n1", RSFLDTYPE_NUMERIC, RSFLDOPT_SORTABLE | RSFLDOPT_NOINDEX);
  fieldID = RediSearch_CreateField(index, "tg1", RSFLDTYPE_TAG, RSFLDOPT_NONE);
  RediSearch_TagFieldSetSeparator(index, fieldID, '.');
  RediSearch_TagFieldSetCaseSensitive(index, fieldID, 1);
  RediSearch_CreateField(index, "dynamic1", RSFLDTYPE_FULLTEXT | RSFLDTYPE_NUMERIC |
                                            RSFLDTYPE_TAG | RSFLDTYPE_GEO, RSFLDOPT_NONE);

  const char *docKey1 = "doc1";
  Document* d = RediSearch_CreateDocumentSimple(docKey1);
  RediSearch_DocumentAddFieldCString(d, "ft1", "hello", RSFLDTYPE_FULLTEXT);
  RediSearch_DocumentAddFieldCString(d, "ft2", "world", RSFLDTYPE_FULLTEXT);
  RediSearch_DocumentAddFieldNumber(d, "n1", 42, RSFLDTYPE_DEFAULT);
  RediSearch_DocumentAddFieldCString(d, "tg1", "tag1", RSFLDTYPE_TAG);
  RediSearch_SpecAddDocument(index, d);

  const char *docKey2 = "doc2";
  d = RediSearch_CreateDocumentSimple(docKey2);
  RediSearch_DocumentAddFieldCString(d, "ft1", "redis", RSFLDTYPE_FULLTEXT);
  RediSearch_DocumentAddFieldCString(d, "ft2", "labs", RSFLDTYPE_FULLTEXT);
  RediSearch_DocumentAddFieldNumber(d, "n1", 42, RSFLDTYPE_DEFAULT);
  RediSearch_DocumentAddFieldCString(d, "tg1", "tag2", RSFLDTYPE_TAG);
  RediSearch_SpecAddDocument(index, d);

  // test invalid option
  RSIdxInfo info = { .version = 0 };
  ASSERT_EQ(RediSearch_IndexInfo(index, &info), REDISEARCH_ERR);

  info = { .version = RS_INFO_CURRENT_VERSION };
  ASSERT_EQ(RediSearch_IndexInfo(index, &info), REDISEARCH_OK);

  ASSERT_EQ(info.gcPolicy, GC_POLICY_FORK);
  ASSERT_EQ(info.score, 0.141);
  EXPECT_STRCASEEQ(info.lang, "YIDDISH");

  // fields stats
  ASSERT_EQ(info.numFields, 5);
  ASSERT_STREQ(info.fields[0].path, "ft1");
  ASSERT_EQ(info.fields[0].types, RSFLDTYPE_FULLTEXT);
  ASSERT_EQ(info.fields[0].options, RSFLDOPT_NONE);
  ASSERT_EQ(info.fields[0].textWeight, 2.3);

  ASSERT_STREQ(info.fields[1].path, "ft2");
  ASSERT_TRUE(info.fields[1].options & RSFLDOPT_TXTNOSTEM);
  ASSERT_EQ(info.fields[1].types, RSFLDTYPE_FULLTEXT);

  ASSERT_STREQ(info.fields[2].path, "n1");
  ASSERT_EQ(info.fields[2].types, RSFLDTYPE_NUMERIC);
  ASSERT_TRUE(info.fields[2].options & RSFLDOPT_SORTABLE);
  ASSERT_TRUE(info.fields[2].options & RSFLDOPT_NOINDEX);

  ASSERT_STREQ(info.fields[3].path, "tg1");
  ASSERT_EQ(info.fields[3].types, RSFLDTYPE_TAG);
  ASSERT_EQ(info.fields[3].tagSeperator, '.');
  ASSERT_EQ(info.fields[3].tagCaseSensitive, 1);

  ASSERT_STREQ(info.fields[4].path, "dynamic1");
  ASSERT_EQ(info.fields[4].types, (RSFLDTYPE_FULLTEXT | RSFLDTYPE_NUMERIC |
                                    RSFLDTYPE_TAG | RSFLDTYPE_GEO));

  // common stats
  ASSERT_EQ(info.numDocuments, 2);
  ASSERT_EQ(info.maxDocId, 2);
  ASSERT_EQ(info.docTableSize, 140);
  ASSERT_EQ(info.sortablesSize, 48);
  ASSERT_EQ(info.docTrieSize, 87);
  ASSERT_EQ(info.numTerms, 5);
  ASSERT_EQ(info.numRecords, 7);
  ASSERT_EQ(info.invertedSize, 682);
  ASSERT_EQ(info.invertedCap, 0);
  ASSERT_EQ(info.skipIndexesSize, 0);
  ASSERT_EQ(info.scoreIndexesSize, 0);
  ASSERT_EQ(info.offsetVecsSize, 5);
  ASSERT_EQ(info.offsetVecRecords, 5);
  ASSERT_EQ(info.termsSize, 24);
  ASSERT_EQ(info.indexingFailures, 0);

  RediSearch_IndexInfoFree(&info);

  RediSearch_FreeIndexOptions(opt);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testLanguage) {
  ASSERT_EQ(REDISEARCH_OK, RediSearch_ValidateLanguage("Hindi"));
  ASSERT_EQ(REDISEARCH_ERR, RediSearch_ValidateLanguage("Hebrew"));

  RSIndexOptions *opt = RediSearch_CreateIndexOptions();

  ASSERT_EQ(REDISEARCH_ERR, RediSearch_IndexOptionsSetLanguage(opt, NULL));
  ASSERT_STRCASEEQ(opt->lang, NULL);
  ASSERT_EQ(REDISEARCH_ERR, RediSearch_IndexOptionsSetLanguage(opt, "HEBREW"));
  ASSERT_STRCASEEQ(opt->lang, NULL);
  ASSERT_EQ(REDISEARCH_OK, RediSearch_IndexOptionsSetLanguage(opt, "YIDDISH"));
  ASSERT_STRCASEEQ(opt->lang, "YIDDISH");
  RediSearch_FreeIndexOptions(opt);
}

TEST_F(LLApiTest, testScore) {
  RSIndexOptions *opt = RediSearch_CreateIndexOptions();

  ASSERT_EQ(REDISEARCH_ERR, RediSearch_IndexOptionsSetScore(opt, 5));
  ASSERT_EQ(opt->score, 0);
  ASSERT_EQ(REDISEARCH_ERR, RediSearch_IndexOptionsSetScore(opt, -5));
  ASSERT_EQ(opt->score, 0);
  ASSERT_EQ(REDISEARCH_OK, RediSearch_IndexOptionsSetScore(opt, 1));
  ASSERT_EQ(opt->score, 1);
  ASSERT_EQ(REDISEARCH_OK, RediSearch_IndexOptionsSetScore(opt, 0.5));
  ASSERT_EQ(opt->score, 0.5);
  RediSearch_FreeIndexOptions(opt);
}

TEST_F(LLApiTest, testInfoSize) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL);
  GCContext *gc;

  // adding field to the index
  RediSearch_CreateNumericField(index, NUMERIC_FIELD_NAME);
  RediSearch_CreateTextField(index, FIELD_NAME_1);

  ASSERT_EQ(RediSearch_MemUsage(index), 0);

  // adding document to the index
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddFieldNumber(d, NUMERIC_FIELD_NAME, 20, RSFLDTYPE_DEFAULT);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "TEXT", RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  ASSERT_EQ(RediSearch_MemUsage(index), 343);

  d = RediSearch_CreateDocument(DOCID2, strlen(DOCID2), 2.0, NULL);
  RediSearch_DocumentAddFieldCString(d, FIELD_NAME_1, "TXT", RSFLDTYPE_DEFAULT);
  RediSearch_DocumentAddFieldNumber(d, NUMERIC_FIELD_NAME, 1, RSFLDTYPE_DEFAULT);
  RediSearch_SpecAddDocument(index, d);

  ASSERT_EQ(RediSearch_MemUsage(index), 612);

  // test MemUsage after deleting docs
  int ret = RediSearch_DropDocument(index, DOCID2, strlen(DOCID2));
  ASSERT_EQ(REDISMODULE_OK, ret);
  ASSERT_EQ(RediSearch_MemUsage(index), 484);
  RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold = 0;
  gc = get_spec(index)->gc;
  gc->callbacks.periodicCallback(gc->gcCtx);
  ASSERT_EQ(RediSearch_MemUsage(index), 335);

  ret = RediSearch_DropDocument(index, DOCID1, strlen(DOCID1));
  ASSERT_EQ(REDISMODULE_OK, ret);
  ASSERT_EQ(RediSearch_MemUsage(index), 236);
  gc = get_spec(index)->gc;
  gc->callbacks.periodicCallback(gc->gcCtx);
  ASSERT_EQ(RediSearch_MemUsage(index), 2);
  // we have 2 left over b/c of the offset vector size which we cannot clean
  // since the data is not maintained.

  RediSearch_DropIndex(index);
}
