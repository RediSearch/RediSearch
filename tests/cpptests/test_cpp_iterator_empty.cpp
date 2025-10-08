/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <algorithm>
#include "rmutil/alloc.h"
#include "gtest/gtest.h"
#include "src/iterators/empty_iterator.h"

class EmptyIteratorTest : public ::testing::Test {
protected:
  QueryIterator *iterator_base;

  void SetUp() override {
    iterator_base = NewEmptyIterator();
  }

  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_F(EmptyIteratorTest, Read) {
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), 0);
  ASSERT_TRUE(iterator_base->atEOF);

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
}

TEST_F(EmptyIteratorTest, SkipTo) {
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 1), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 42), ITERATOR_EOF);
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, 1000), ITERATOR_EOF);
}

TEST_F(EmptyIteratorTest, Rewind) {
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  iterator_base->Rewind(iterator_base);
  ASSERT_TRUE(iterator_base->atEOF);

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
}

TEST_F(EmptyIteratorTest, Revalidate) {
  ASSERT_EQ(iterator_base->Revalidate(iterator_base), VALIDATE_OK);
}
