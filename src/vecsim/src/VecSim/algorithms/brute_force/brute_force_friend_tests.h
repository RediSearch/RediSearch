
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/friend_test_decl.h"
// Allow the following tests to access the index private members.
INDEX_TEST_FRIEND_CLASS(BruteForceTest_brute_force_vector_update_test_Test)
INDEX_TEST_FRIEND_CLASS(BruteForceTest_resize_and_align_index_Test)
INDEX_TEST_FRIEND_CLASS(BruteForceTest_brute_force_empty_index_Test)
INDEX_TEST_FRIEND_CLASS(BruteForceTest_brute_force_reindexing_same_vector_Test)
INDEX_TEST_FRIEND_CLASS(BruteForceTest_test_delete_swap_block_Test)
INDEX_TEST_FRIEND_CLASS(BruteForceTest_test_dynamic_bf_info_iterator_Test)
INDEX_TEST_FRIEND_CLASS(BruteForceTest_brute_force_zero_minimal_capacity_Test)
INDEX_TEST_FRIEND_CLASS(BruteForceTest_preferAdHocOptimization_Test)
INDEX_TEST_FRIEND_CLASS(IndexAllocatorTest_test_bf_index_block_size_1_Test)
INDEX_TEST_FRIEND_CLASS(BM_VecSimBasics)
