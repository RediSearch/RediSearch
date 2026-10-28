/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"
extern "C" {
#include "rmr/reply.h"
}
#include "module.h"
extern "C" {
#include "coord/search_reducer.h"
}
#include "hiredis/hiredis.h"
#include "hiredis/read.h"
#include "util/arr/arr.h"

#include <string>
#include <vector>

#ifdef ENABLE_ASSERT
namespace {
std::string str(const std::string &s) {
  return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
std::string sequence(char type, const std::vector<std::string> &values) {
  std::string out = type + std::to_string(type == '%' ? values.size() / 2 : values.size()) + "\r\n";
  for (const auto &value : values) out += value;
  return out;
}
MRReply *parse(const std::string &wire) {
  redisReader *reader = redisReaderCreate();
  EXPECT_EQ(redisReaderFeed(reader, wire.data(), wire.size()), REDIS_OK);
  void *reply = nullptr;
  EXPECT_EQ(redisReaderGetReply(reader, &reply), REDIS_OK);
  redisReaderFree(reader);
  return static_cast<MRReply *>(reply);
}
struct Request {
  searchRequestCtx req = {};
  explicit Request(RSTimeoutPolicy policy = TimeoutPolicy_ReturnStrict) {
    RequestConfig config = {};
    TimeoutConfig timeout = {};
    timeout.timeoutPolicy = policy;
    QueryRequest_Init(&req.base, QUERY_REQUEST_KIND_COORD_SEARCH, &config, &timeout, nullptr, 0);
    req.limit = req.requestedResultsCount = 10;
    CoordSearchReply_Init(&req);
    CoordSearchReply_Prepare(&req, 2);
  }
  ~Request() {
    CoordSearchReducer_Free(&req);
    array_free(req.specialCases);
    QueryRequest_Destroy(&req.base);
  }
  MRReply *add(const std::string &wire) {
    MRReply *reply = parse(wire);
    CoordSearchReply_Accept(&req, reply);
    return reply;
  }
};
std::string chunk(bool resp3, const std::string &score) {
  if (!resp3)
    return sequence('*',
                    {":3\r\n", str("first"), str("1"), str("bad"), score, str("unread"), str("1")});
  auto row = [](const std::string &id, const std::string &score) {
    return sequence('%', {str("id"), str(id), str("score"), score});
  };
  return sequence(
      '%', {str("total_results"), ":3\r\n", str("warning"), "*0\r\n", str("results"),
            sequence('*', {row("first", ",1\r\n"), row("bad", score), row("unread", ",1\r\n")})});
}
}  // namespace

TEST(SearchReply, ParseErrorStopsAndOverridesPartialResultsAndShardTimeout) {
  for (bool resp3 : {false, true}) {
    for (auto policy : {TimeoutPolicy_Return, TimeoutPolicy_ReturnStrict, TimeoutPolicy_Fail}) {
      for (bool shardTimeout : {false, true}) {
        if (shardTimeout && policy == TimeoutPolicy_Fail) continue;
        Request request(policy);
        request.req.noContent = true;
        MRReply *root = request.add(chunk(resp3, str("not-a-score")));
        if (shardTimeout)
          request.add(std::string("-") + QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT) + "\r\n");
        // Exercise the main-thread timeout path too: an encountered parse error wins.
        QueryRequestTimeout_MarkTimedOut(&request.req.base.timeout);
        CoordSearchReducer_DebugReduce(&request.req);
        EXPECT_EQ(request.req.base.reply.rc, RS_RESULT_ERROR);
        EXPECT_EQ(QueryError_GetCode(&request.req.base.reply.err), QUERY_ERROR_CODE_GENERIC);
        EXPECT_STREQ(QueryError_GetUserError(&request.req.base.reply.err),
                     "SEARCH_GENERIC could not parse redisearch results");
        EXPECT_EQ(array_len(request.req.base.reply.results), 0);
        MRReply *unread =
            resp3 ? MRReply_MapElement(MRReply_ArrayElement(MRReply_MapElement(root, "results"), 2),
                                       "id")
                  : MRReply_ArrayElement(root, 5);
        EXPECT_EQ(std::string(MRReply_String(unread, nullptr)), "unread");
      }
    }
  }
}

TEST(SearchReply, MissingKnnDistanceStopsBothSorters) {
  for (auto policy : {TimeoutPolicy_Return, TimeoutPolicy_ReturnStrict, TimeoutPolicy_Fail}) {
    specialCaseCtx knn = {};
    knn.specialCaseType = SPECIAL_CASE_KNN;
    knn.knn.k = 2;
    knn.knn.fieldName = "distance";
    knn.knn.shouldSort = true;
    Request request(policy);
    request.req.noContent = true;
    request.req.specialCases = array_new(specialCaseCtx *, 1);
    array_append(request.req.specialCases, &knn);
    auto row = [](const char *id, bool hasDistance) {
      std::vector<std::string> fields = {str("id"), str(id), str("score"), ",1\r\n"};
      if (hasDistance) {
        fields.push_back(str("required_fields"));
        fields.push_back(sequence('%', {str("distance"), str("#1")}));
      }
      return sequence('%', fields);
    };
    MRReply *root = request.add(sequence(
        '%', {str("total_results"), ":3\r\n", str("warning"), "*0\r\n", str("results"),
              sequence('*', {row("first", true), row("bad", false), row("unread", true)})}));
    CoordSearchReducer_DebugReduce(&request.req);
    EXPECT_EQ(request.req.base.reply.rc, RS_RESULT_ERROR);
    EXPECT_EQ(QueryError_GetCode(&request.req.base.reply.err), QUERY_ERROR_CODE_GENERIC);
    EXPECT_STREQ(QueryError_GetUserError(&request.req.base.reply.err),
                 "SEARCH_GENERIC could not parse redisearch results");
    EXPECT_EQ(array_len(request.req.base.reply.results), 0);
    MRReply *rows = MRReply_MapElement(root, "results");
    // The first candidate reached the nearest-K sorter before the error stopped the pipeline.
    EXPECT_STREQ(MRReply_String(MRReply_MapElement(MRReply_ArrayElement(rows, 0), "id"), nullptr),
                 "first");
    MRReply *unread = MRReply_MapElement(MRReply_ArrayElement(rows, 2), "id");
    EXPECT_EQ(std::string(MRReply_String(unread, nullptr)), "unread");
  }
}

TEST(SearchReply, MalformedEnvelopeReportsError) {
  for (const auto &wire :
       {sequence('*', {":1\r\n", str("truncated")}),
        sequence('%', {str("warning"), "*0\r\n", str("results"), "*0\r\n"}),
        sequence('%', {str("warning"), "*0\r\n", str("total_results"), ":1\r\n"})}) {
    Request request;
    request.add(wire);
    CoordSearchReducer_DebugReduce(&request.req);
    EXPECT_EQ(request.req.base.reply.rc, RS_RESULT_ERROR);
    EXPECT_EQ(QueryError_GetCode(&request.req.base.reply.err), QUERY_ERROR_CODE_GENERIC);
    EXPECT_EQ(array_len(request.req.base.reply.results), 0);
  }
}

TEST(SearchReply, BorrowsBinaryAndNestedFieldsUntilRequestCleanup) {
  Request request;
  request.req.limit = request.req.requestedResultsCount = 1;
  request.req.withPayload = true;
  const std::string binary("a\0b", 3);
  const std::string attrs = sequence('*', {str("field"), str(binary), str("nested"),
                                           sequence('*', {str("9007199254740993"), "$-1\r\n"})});
  MRReply *root = request.add(sequence('*', {":2\r\n", str(binary), str("2"), str(binary), attrs,
                                             str("discarded"), str("1"), str(binary), attrs}));
  MRReply *fields = MRReply_ArrayElement(root, 4);
  const char *idBytes = MRReply_String(MRReply_ArrayElement(root, 1), nullptr);
  RLookup *lookup = CoordSearchReducer_DebugReduce(&request.req);
  ASSERT_EQ(request.req.base.reply.rc, RS_RESULT_EOF);
  ASSERT_EQ(array_len(request.req.base.reply.results), 1);
  const SearchResult *result = request.req.base.reply.results[0];
  EXPECT_EQ(SearchResult_GetDocId(result), 0);
  const auto *row = SearchResult_GetRowData(result);
  const RSValue *id =
      RLookupRow_Get(RLookup_GetKey_Read(lookup, "document_key", RLOOKUP_F_NOFLAGS), row);
  auto view = RSValue_GetReplyView(id, RSValueTrioSelection_Middle);
  EXPECT_EQ(view.string.bytes, idBytes);
  EXPECT_EQ(std::string(view.string.bytes, view.str_len), binary);
  EXPECT_EQ(RLookup_GetKey_Read(lookup, "fields", RLOOKUP_F_NOFLAGS), nullptr);
  EXPECT_EQ(RLookup_GetKey_Read(lookup, "payload", RLOOKUP_F_NOFLAGS), nullptr);
  EXPECT_EQ(MRReply_ArrayElement(root, 4), fields);
  EXPECT_EQ(MRReply_Length(root), 9);
  // Dropping returned candidates must not release backing used by profiles or cleanup.
  QueryRequest_ResetReply(&request.req.base);
  EXPECT_EQ(std::string(MRReply_String(MRReply_ArrayElement(root, 3), nullptr), 3), binary);
  EXPECT_EQ(std::string(MRReply_String(MRReply_ArrayElement(fields, 1), nullptr), 3), binary);
  MRReply *nested = MRReply_ArrayElement(fields, 3);
  EXPECT_STREQ(MRReply_String(MRReply_ArrayElement(nested, 0), nullptr), "9007199254740993");
  EXPECT_EQ(MRReply_Type(MRReply_ArrayElement(nested, 1)), MR_REPLY_NIL);
}

#endif
