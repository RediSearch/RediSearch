#include "gtest/gtest.h"
#include "redisearch.h"
#include "index.h"
#include "query.h"
#include "micro-benchmarks/deprecated_iterator_util.h"
#include <vector>
#include <algorithm>
#include <iostream>

class NotIteratorOldTest : public ::testing::Test {
protected:
  IndexIterator *iterator_base;
  std::vector<t_docId> childDocIds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<t_docId> wcDocIds = {3, 4, 9};
  std::vector<t_docId> resultSet;
  t_docId maxDocId = 10;

  void SetUp() override {
    // Create the child iterator with the specified document IDs
    IndexIterator *child = (IndexIterator *)new MockOldIterator(childDocIds);

    // Create the wildcard iterator with the specified document IDs
    IndexIterator *wcii = (IndexIterator *)new MockOldIterator(wcDocIds);

    // Set up the timeout
    struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout

    // Create the NOT iterator with the child and wildcard iterators
    iterator_base = _New_NotIterator_With_WildCardIterator(child, wcii, maxDocId, 1.0, timeout);

    // Compute the expected result set
    // For the optimized NOT iterator, the result set is all documents in the wildcard iterator
    // that are not in the child iterator
    resultSet.clear();
    for (auto wcId : wcDocIds) {
      if (std::find(childDocIds.begin(), childDocIds.end(), wcId) == childDocIds.end()) {
        resultSet.push_back(wcId);
      }
    }

    // Print test setup information
    std::cout << "Child Doc IDs: ";
    for (auto id : childDocIds) {
      std::cout << id << " ";
    }
    std::cout << std::endl;

    std::cout << "Wildcard Doc IDs: ";
    for (auto id : wcDocIds) {
      std::cout << id << " ";
    }
    std::cout << std::endl;

    std::cout << "Expected Result Set: ";
    if (resultSet.empty()) {
      std::cout << "(empty)";
    } else {
      for (auto id : resultSet) {
        std::cout << id << " ";
      }
    }
    std::cout << std::endl;
  }

  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_F(NotIteratorOldTest, ReadOptimized) {
  RSIndexResult *hit = iterator_base->current;
  int rc;

  std::cout << "=== READING RESULTS ===\n";
  size_t count = 0;

  // Read all results from the iterator
  while ((rc = iterator_base->Read(iterator_base->ctx, &hit)) == INDEXREAD_OK) {
    std::cout << "Read result: docId=" << hit->docId << std::endl;
    // Verify the result is in our expected result set
    //ASSERT_EQ(hit->docId, resultSet[count]);
    count++;
  }
  ASSERT_EQ(count, 0);

  // Verify we reached EOF
  ASSERT_EQ(rc, INDEXREAD_EOF);

  // Verify we read the expected number of results
  ASSERT_EQ(count, resultSet.size()) << "Expected to read " << resultSet.size() << " documents";
  std::cout << "Read " << count << " results (expected " << resultSet.size() << ")" << std::endl;

  // Try reading again after EOF
  rc = iterator_base->Read(iterator_base->ctx, &hit);
  ASSERT_EQ(rc, INDEXREAD_EOF);
  std::cout << "=== FINISHED READING ===\n";
}
