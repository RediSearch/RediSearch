/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Exercises the lock-free DocTable read path: multiple reader threads call
// DocTable_Borrow / DocTable_Exists WITHOUT any lock, bracketed only by
// DocTable_ReadBegin/End, while a single writer thread inserts and deletes
// documents (driving both the copy-on-grow of the bucket array and chain
// unlink + deferred DMD reclamation). Run under ASan/TSan to catch any
// use-after-free or torn read.

#include "gtest/gtest.h"

extern "C" {
#include "doc_table.h"
}

#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

class DocTableLockFreeTest : public ::testing::Test {};

// Sanity: growth publishes a fresh view and previously-inserted docs remain
// borrowable across the grow, single-threaded.
TEST_F(DocTableLockFreeTest, GrowKeepsEarlierDocsBorrowable) {
  DocTable dt = NewDocTable(4, 100000);
  const t_docId N = 5000;  // crosses several grow steps
  for (t_docId i = 1; i <= N; ++i) {
    char key[32];
    size_t n = snprintf(key, sizeof(key), "doc:%llu", (unsigned long long)i);
    RSDocumentMetadata *dmd =
        DocTable_Put(&dt, key, n, 1.0, Document_DefaultFlags, NULL, 0, DocumentType_Hash);
    ASSERT_TRUE(dmd != NULL);
    DMD_Return(dmd);
  }
  // Every doc must still be findable after all the array swaps.
  for (t_docId i = 1; i <= N; ++i) {
    const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, i);
    ASSERT_TRUE(dmd != NULL) << "missing docId " << i;
    ASSERT_EQ(dmd->id, i);
    ASSERT_TRUE(DocTable_Exists(&dt, i));
    DMD_Return(dmd);
  }
  DocTable_Free(&dt);
}

// Concurrency: lock-free readers vs a single writer that inserts and deletes.
TEST_F(DocTableLockFreeTest, ConcurrentLockFreeBorrowWhileWriting) {
  const size_t MAXSIZE = 4096;   // small, so docs beyond it share chains
  const t_docId MAXDOC = 40000;  // forces grows and chains
  DocTable dt = NewDocTable(16, MAXSIZE);

  std::atomic<bool> stop{false};
  std::atomic<t_docId> maxLive{0};
  std::atomic<bool> failed{false};

  auto reader = [&]() {
    unsigned seed = 0x9e3779b9u + (unsigned)(uintptr_t)&seed;
    while (!stop.load(std::memory_order_acquire)) {
      t_docId hi = maxLive.load(std::memory_order_acquire);
      if (hi == 0) {
        std::this_thread::yield();
        continue;
      }
      // A lock-free read section: no spec lock, only the reclamation guard.
      DocTable_ReadBegin();
      for (int k = 0; k < 128; ++k) {
        seed = seed * 1103515245u + 12345u;
        t_docId id = 1 + (seed % hi);
        const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, id);
        if (dmd) {
          // Borrow succeeded => node is alive and pinned by our ref; its fields
          // must be consistent and its key readable (would fault under ASan on
          // a use-after-free).
          if (dmd->id != id) failed.store(true, std::memory_order_relaxed);
          size_t klen = 0;
          const char *kp = DMD_KeyPtrLen(dmd, &klen);
          if (klen == 0 || kp == NULL) failed.store(true, std::memory_order_relaxed);
          volatile char sink = kp ? kp[0] : 0;
          (void)sink;
          DMD_Return(dmd);
        }
        // DocTable_Exists exercises the same lock-free walk without borrowing.
        (void)DocTable_Exists(&dt, id);
      }
      DocTable_ReadEnd();
    }
  };

  std::vector<std::thread> readers;
  for (int i = 0; i < 4; ++i) readers.emplace_back(reader);

  std::thread writer([&]() {
    for (t_docId i = 1; i <= MAXDOC; ++i) {
      char key[32];
      size_t n = snprintf(key, sizeof(key), "doc:%llu", (unsigned long long)i);
      RSDocumentMetadata *dmd =
          DocTable_Put(&dt, key, n, 1.0, Document_DefaultFlags, NULL, 0, DocumentType_Hash);
      DMD_Return(dmd);  // drop the caller ref; the table keeps its own
      maxLive.store(i, std::memory_order_release);

      // Delete a slightly older doc to exercise unlink + deferred reclamation
      // while readers may be walking through it.
      if (i > 20 && (i % 3 == 0)) {
        t_docId victim = i - 11;
        char vk[32];
        size_t vn = snprintf(vk, sizeof(vk), "doc:%llu", (unsigned long long)victim);
        RSDocumentMetadata *popped = DocTable_Pop(&dt, vk, vn);
        if (popped) {
          // DocTable_Pop already marked it Document_Deleted; just drop the
          // table's (transferred) ref, which retires the node for reclamation.
          DMD_Return(popped);
        }
      }
    }
    stop.store(true, std::memory_order_release);
  });

  writer.join();
  for (auto &t : readers) t.join();

  ASSERT_FALSE(failed.load(std::memory_order_relaxed))
      << "a lock-free borrow observed an inconsistent or freed DMD";

  DocTable_Free(&dt);
}
