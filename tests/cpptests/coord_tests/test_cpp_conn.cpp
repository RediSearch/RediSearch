/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include <cstring>

extern "C" {
#include "conn_internal.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
}

// MRConn_HelloCallback is invoked by hiredis when a HELLO reply arrives (or
// when the ac is torn down while HELLO is in flight). The four tests below
// cover every branch of the callback: happy path, server-side error, transport
// error, and hiredis-teardown with a NULL reply.

class HelloCallbackTest : public ::testing::Test {
protected:
  MRConn conn;
  redisAsyncContext ac;

  void SetUp() override {
    std::memset(&conn, 0, sizeof(conn));
    std::memset(&ac, 0, sizeof(ac));

    // CONN_LOG_WARNING dereferences ep.host / ep.port / state; keep a minimal
    // stack-allocated endpoint. No MREndpoint_Free is needed since we did not
    // go through MREndpoint_Copy.
    conn.ep.host = const_cast<char *>("testhost");
    conn.ep.port = 6379;
    conn.state = MRConn_Connected;
    conn.protocol = 3;
    ac.data = &conn;
  }

  // Parse a RESP payload into a heap-allocated redisReply so the callback's
  // freeReplyObject call matches the real async teardown path.
  static redisReply *makeReply(const char *payload) {
    redisReader *reader = redisReaderCreate();
    redisReaderFeed(reader, payload, std::strlen(payload));
    void *reply = nullptr;
    int rc = redisReaderGetReply(reader, &reply);
    redisReaderFree(reader);
    EXPECT_EQ(rc, REDIS_OK);
    return static_cast<redisReply *>(reply);
  }
};

// HELLO accepted: non-error reply, no transport error. The cached protocol
// must be preserved so subsequent sends skip HELLO.
TEST_F(HelloCallbackTest, HappyPath) {
  redisReply *reply = makeReply("+OK\r\n");
  ASSERT_NE(reply, nullptr);

  MRConn_HelloCallback(&ac, reply, nullptr);

  EXPECT_EQ(conn.protocol, 3);
}

// HELLO rejected by the server (e.g. unsupported protocol version). The
// callback must clear the cached protocol so the next MRConn_SendCommand
// re-issues HELLO.
TEST_F(HelloCallbackTest, ServerError) {
  redisReply *reply = makeReply("-NOPROTO unsupported protocol version\r\n");
  ASSERT_NE(reply, nullptr);

  MRConn_HelloCallback(&ac, reply, nullptr);

  EXPECT_EQ(conn.protocol, 0);
}

// Transport error surfaced on the async context (ac->c.err set) with a reply
// still delivered. Hiredis normally pairs this with a NULL reply, but the
// callback defensively handles a non-NULL reply too: protocol must be cleared.
TEST_F(HelloCallbackTest, TransportError) {
  // redisAsyncContext exposes its own err/errstr (char *) fields, distinct
  // from the embedded redisContext's. The callback reads these.
  ac.err = REDIS_ERR_IO;
  ac.errstr = const_cast<char *>("Connection reset by peer");
  redisReply *reply = makeReply("+OK\r\n");
  ASSERT_NE(reply, nullptr);

  MRConn_HelloCallback(&ac, reply, nullptr);

  EXPECT_EQ(conn.protocol, 0);
}

// Hiredis teardown: reply callbacks are flushed with r == NULL before the
// disconnect callback fires. The HELLO callback must be a no-op (no crash, no
// state mutation) in that case.
TEST_F(HelloCallbackTest, HiredisTeardownNullReply) {
  MRConn_HelloCallback(&ac, nullptr, nullptr);

  EXPECT_EQ(conn.protocol, 3);
}

// The conn may already have been detached from the ac (ac->data == NULL) when
// HELLO replies late. The callback must exit cleanly without dereferencing a
// NULL conn and must still free the reply.
TEST_F(HelloCallbackTest, DetachedConn) {
  ac.data = nullptr;
  redisReply *reply = makeReply("+OK\r\n");
  ASSERT_NE(reply, nullptr);

  // Reaching this line without a crash is the assertion.
  MRConn_HelloCallback(&ac, reply, nullptr);
}
