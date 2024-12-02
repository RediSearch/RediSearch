#include "gtest/gtest.h"
#include "src/util/circular_buffer.h"

class CircularBufferTest : public ::testing::Test {};

TEST_F(CircularBufferTest, testEmpty) {
    CircularBuffer buff = CircularBuffer_New(sizeof(int), 16);

	// a new circular buffer should be empty
	ASSERT_EQ(CircularBuffer_Empty(buff), true);

	// item count of an empty circular buffer should be 0
	ASSERT_EQ(CircularBuffer_ItemCount(buff), 0);

	// buffer should have available slots in it i.e. not full
	ASSERT_EQ(CircularBuffer_Full(buff), false);

	// clean up
	CircularBuffer_Free(buff);
}
