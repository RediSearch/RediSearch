#include "../redisearch_api.h"
#include <gtest/gtest.h>
#include <set>
#include <string>

#define DOCID1 "doc1"
#define DOCID2 "doc2"
#define FIELD_NAME_1 "text1"
#define FIELD_NAME_2 "text2"
#define NUMERIC_FIELD_NAME "num"
#define TAG_FIELD_NAME1 "tag1"
#define TAG_FIELD_NAME2 "tag2"

REDISEARCH_API_INIT_SYMBOLS();

class LLApiTest : public ::testing::Test {
  virtual void SetUp() {
    RediSearch_Initialize();
  }

  virtual void TearDown() {
  }
};

TEST_F(LLApiTest, testGetVersion) {
  ASSERT_EQ(RediSearch_GetCApiVersion(), REDISEARCH_CAPI_VERSION);
}

TEST_F(LLApiTest, testAddDocumentTextField) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL, NULL);

  // adding text field to the index
  RediSearch_CreateTextField(index, FIELD_NAME_1);

  // adding document to the index
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddTextFieldC(d, FIELD_NAME_1, "some test to index");
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
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(iter);

  // adding another text field
  RediSearch_CreateTextField(index, FIELD_NAME_2);

  // adding document to the index with both fields
  d = RediSearch_CreateDocument(DOCID2, strlen(DOCID2), 1.0, NULL);
  RediSearch_DocumentAddTextFieldC(d, FIELD_NAME_1, "another indexing testing");
  RediSearch_DocumentAddTextFieldC(d, FIELD_NAME_2, "another indexing testing");
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
  ASSERT_TRUE(ret);

  // searching again, make sure there is no results
  qn = RediSearch_CreatePrefixNode(index, FIELD_NAME_2, "an");
  iter = RediSearch_GetResultsIterator(qn, index);

  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testAddDocumetNumericField) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL, NULL);

  // adding text field to the index
  RediSearch_CreateNumericField(index, NUMERIC_FIELD_NAME);

  // adding document to the index
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddNumericField(d, NUMERIC_FIELD_NAME, 20);
  RediSearch_SpecAddDocument(index, d);

  // searching on the index
  RSQNode* qn = RediSearch_CreateNumericNode(index, NUMERIC_FIELD_NAME, 30, 10, 0, 0);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);

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
  RSIndex* index = RediSearch_CreateIndex("index", NULL, NULL);

  // adding text field to the index
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);

  // adding document to the index
#define TAG_VALUE "tag_value"
  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddTextFieldC(d, TAG_FIELD_NAME1, TAG_VALUE);
  RediSearch_SpecAddDocument(index, d);

  // searching on the index
  RSQNode* qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* tqn = RediSearch_CreateTokenNode(index, NULL, TAG_VALUE);
  RediSearch_TagNodeAddChild(qn, tqn);
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);

  // prefix search on the index
  qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  tqn = RediSearch_CreatePrefixNode(index, NULL, "ta");
  RediSearch_TagNodeAddChild(qn, tqn);
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
  RSIndex* index = RediSearch_CreateIndex("index", NULL, NULL);
  RSField* f = RediSearch_CreateTextField(index, FIELD_NAME_1);
  RediSearch_TextFieldPhonetic(f, index);

  // creating none phonetic field
  RediSearch_CreateTextField(index, FIELD_NAME_2);

  RSDoc* d = RediSearch_CreateDocument(DOCID1, strlen(DOCID1), 1.0, NULL);
  RediSearch_DocumentAddTextFieldC(d, FIELD_NAME_1, "felix");
  RediSearch_DocumentAddTextFieldC(d, FIELD_NAME_2, "felix");
  RediSearch_SpecAddDocument(index, d);

  // make sure phonetic search works on field1
  RSQNode* qn = RediSearch_CreateTokenNode(index, FIELD_NAME_1, "phelix");
  RSResultsIterator* iter = RediSearch_GetResultsIterator(qn, index);

  size_t len;
  const char* id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, DOCID1);
  id = (const char*)RediSearch_ResultsIteratorNext(iter, index, &len);
  ASSERT_STREQ(id, NULL);

  RediSearch_ResultsIteratorFree(iter);

  // make sure phonetic search on field2 do not return results
  qn = RediSearch_CreateTokenNode(index, FIELD_NAME_2, "phelix");
  iter = RediSearch_GetResultsIterator(qn, index);
  ASSERT_FALSE(iter);
  RediSearch_DropIndex(index);
}

TEST_F(LLApiTest, testMassivePrefix) {
  // creating the index
  RSIndex* index = RediSearch_CreateIndex("index", NULL, NULL);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);

  char buff[1024];
  int NUM_OF_DOCS = 1000;
  for (int i = 0; i < NUM_OF_DOCS; ++i) {
    sprintf(buff, "doc%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    sprintf(buff, "tag-%d", i);
    RediSearch_DocumentAddTextFieldC(d, TAG_FIELD_NAME1, buff);
    RediSearch_SpecAddDocument(index, d);
  }

  RSQNode* qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* pqn = RediSearch_CreatePrefixNode(index, NULL, "tag-");
  RediSearch_TagNodeAddChild(qn, pqn);
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

TEST_F(LLApiTest, testRanges) {
  RSIndex* index = RediSearch_CreateIndex("index", NULL, NULL);
  RediSearch_CreateTextField(index, FIELD_NAME_1);
  char buf[] = {"Mark_"};
  size_t nbuf = strlen(buf);
  for (char c = 'a'; c < 'z'; c++) {
    buf[nbuf - 1] = c;
    char did[64];
    sprintf(did, "doc%c", c);
    RSDoc* d = RediSearch_CreateDocument(did, strlen(did), 0, NULL);
    RediSearch_DocumentAddTextFieldC(d, FIELD_NAME_1, buf);
    RediSearch_SpecAddDocument(index, d);
  }

  RSQNode* qn = RediSearch_CreateLexRangeNode(index, FIELD_NAME_1, "MarkN", "MarkX");
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

  ASSERT_EQ(10, results.size());
  for (char c = 'n'; c < 'x'; c++) {
    char namebuf[64];
    sprintf(namebuf, "doc%c", c);
    ASSERT_NE(results.end(), results.find(namebuf));
  }
  RediSearch_ResultsIteratorFree(iter);

  // printf("Have %lu ids in range!\n", results.size());
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
  RSIndex* index = RediSearch_CreateIndex("index", GetValue, NULL);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);

  char buff[1024];
  int NUM_OF_DOCS = 10000;
  for (int i = 0; i < NUM_OF_DOCS; ++i) {
    sprintf(buff, "doc%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    sprintf(buff, "tag-%d", i);
    RediSearch_DocumentAddTextFieldC(d, TAG_FIELD_NAME1, buff);
    RediSearch_SpecAddDocument(index, d);
  }

  RSQNode* qn = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* pqn = RediSearch_CreatePrefixNode(index, NULL, "tag-");
  RediSearch_TagNodeAddChild(qn, pqn);
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
  RSIndex* index = RediSearch_CreateIndex("index", GetValue, NULL);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME1);
  RediSearch_CreateTagField(index, TAG_FIELD_NAME2);

  char buff[1024];
  int NUM_OF_DOCS = 1000;
  for (int i = 0; i < NUM_OF_DOCS; ++i) {
    sprintf(buff, "doc%d", i);
    RSDoc* d = RediSearch_CreateDocument(buff, strlen(buff), 1.0, NULL);
    sprintf(buff, "tag1-%d", i);
    RediSearch_DocumentAddTextFieldC(d, TAG_FIELD_NAME1, buff);
    sprintf(buff, "tag2-%d", i);
    RediSearch_DocumentAddTextFieldC(d, TAG_FIELD_NAME2, buff);
    RediSearch_SpecAddDocument(index, d);
  }

  RSQNode* qn1 = RediSearch_CreateTagNode(index, TAG_FIELD_NAME1);
  RSQNode* pqn1 = RediSearch_CreatePrefixNode(index, NULL, "tag1-");
  RediSearch_TagNodeAddChild(qn1, pqn1);
  RSQNode* qn2 = RediSearch_CreateTagNode(index, TAG_FIELD_NAME2);
  RSQNode* pqn2 = RediSearch_CreatePrefixNode(index, NULL, "tag2-");
  RediSearch_TagNodeAddChild(qn2, pqn2);
  RSQNode* iqn = RediSearch_CreateIntersectNode(index, 0);
  RediSearch_IntersectNodeAddChild(iqn, qn1);
  RediSearch_IntersectNodeAddChild(iqn, qn2);

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
