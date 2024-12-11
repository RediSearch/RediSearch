#include "gtest/gtest.h"
#include "src/util/circular_buffer.h"

class CircularBufferTest : public ::testing::Test {};

TEST_F(CircularBufferTest, testEmpty) {
    CircularBuffer buff = CircularBuffer_New(sizeof(int), 16);

	// a new circular buffer should be empty
	ASSERT_EQ(CircularBuffer_Empty(buff), true);

	// item count of an empty circular buffer should be 0
	ASSERT_EQ(CircularBuffer_ItemCount(buff), 0);

	// item size should be 4
	ASSERT_EQ(CircularBuffer_ItemSize(buff), 4);

	// buffer should have available slots in it i.e. not full
	ASSERT_EQ(CircularBuffer_Full(buff), false);

	// clean up
	CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBufferPopulation) {
	int n;
	int cap = 16;
	CircularBuffer buff = CircularBuffer_New(sizeof(int), cap);

	// remove item from an empty buffer should report failure
	ASSERT_TRUE(CircularBuffer_Read(buff, &n) == NULL);

	//--------------------------------------------------------------------------
	// fill buffer
	//--------------------------------------------------------------------------
	for (int i = 0; i < cap; i++) {
		// make sure item was added
		ASSERT_EQ(CircularBuffer_Add(buff, &i), 1);
		// validate buffer's item count
		ASSERT_EQ(CircularBuffer_ItemCount(buff), i+1);
	}
	ASSERT_EQ(CircularBuffer_Full(buff), true);

	// forcefully try to overflow buffer
	for (int i = 0; i < 10; i++) {
		ASSERT_EQ(CircularBuffer_Add(buff, &n), 0);
	}

	//--------------------------------------------------------------------------
	// empty buffer
	//--------------------------------------------------------------------------
	for (int i = 0; i < cap; i++) {
		// get item from buffer
		ASSERT_TRUE(CircularBuffer_Read(buff, &n) != NULL);

		// validate item's value
		ASSERT_EQ(n, i);
	}
	ASSERT_EQ(CircularBuffer_Empty(buff), true);

	// forcefully try to read an item from an empty buffer
	for (int i = 0; i < 10; i++) {
		ASSERT_TRUE(CircularBuffer_Read(buff, &n) == NULL);
	}

	// clean up
	CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBuffer_Circularity) {
	int n;
	int cap = 16;
	CircularBuffer buff = CircularBuffer_New(sizeof(int), cap);

	//--------------------------------------------------------------------------
	// fill buffer
	//--------------------------------------------------------------------------
	for (int i = 0; i < cap; i++) {
		// make sure item was added
		ASSERT_EQ(CircularBuffer_Add(buff, &i), 1);
	}
	ASSERT_EQ(CircularBuffer_Full(buff), true);

	// try to overflow buffer
	ASSERT_EQ(CircularBuffer_Add(buff, &n), 0);

	// removing an item should make space in the buffer
	ASSERT_TRUE(CircularBuffer_Read(buff, &n) != NULL);
	ASSERT_EQ(CircularBuffer_Add(buff, &n), 1);

	//--------------------------------------------------------------------------
	// clear buffer
	//--------------------------------------------------------------------------

	while (CircularBuffer_Empty(buff) == false) {
		CircularBuffer_Read(buff, &n);
	}

	// add/remove elements cycling through the buffer multiple times
	for (int i = 0; i < cap * 4; i++) {
		ASSERT_EQ(CircularBuffer_Add(buff, &i), 1);
		ASSERT_TRUE(CircularBuffer_Read(buff, &n) != NULL);
		ASSERT_EQ(n, i);
	}
	ASSERT_EQ(CircularBuffer_Empty(buff), true);

	// clean up
	CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBuffer_free) {
	//--------------------------------------------------------------------------
	// fill a buffer of size 16 with int *
	//--------------------------------------------------------------------------

	uint cap = 16;
	CircularBuffer buff = CircularBuffer_New(sizeof(int64_t *), cap);
	for (int i = 0; i < cap; i++) {
		int64_t *j = (int64_t *)malloc(sizeof(int64_t));
		CircularBuffer_Add(buff, (void*)&j);
	}

	//--------------------------------------------------------------------------
	// free the buffer
	//--------------------------------------------------------------------------

	for (int i = 0; i < cap; i++) {
		int64_t *item;
		CircularBuffer_Read(buff, &item);
		free(item);
	}

	CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBuffer_Reserve) {

	// -------------------------------------------------------------------------
	// fill a buffer of size 16 with 32 integers
	// -------------------------------------------------------------------------

	uint cap = 16;
	CircularBuffer buff = CircularBuffer_New(sizeof(int), cap);
	for (int i = 0; i < 2 * cap; i++) {
		int *item = (int *)CircularBuffer_Reserve(buff);
		*item = i;
	}

	// make sure item count did not exceeded buffer cap
	ASSERT_EQ(CircularBuffer_ItemCount(buff), CircularBuffer_Cap(buff));

	// -------------------------------------------------------------------------
	// assert override correctness
	// -------------------------------------------------------------------------

	for (uint i = 0; i < 16; i++) {
		int item;
		void *res = CircularBuffer_Read(buff, &item);
		ASSERT_TRUE(res != NULL);
		ASSERT_EQ(item, (i + 16));
		ASSERT_EQ(CircularBuffer_ItemCount(buff), 16-i-1);
	}

	// -------------------------------------------------------------------------
	// free the buffer
	// -------------------------------------------------------------------------

	CircularBuffer_Free(buff);
}
