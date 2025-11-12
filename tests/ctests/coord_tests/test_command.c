/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "minunit.h"
#include "command.h"
#include "rmutil/alloc.h"

// Test suite for MRCommand API functions

// Test the fallback case when replacement string is longer than original
// MRCommand_ReplaceArgSubstring has two code paths:
// 1. Optimization: pad with spaces when newLen <= oldLen (no reallocation)
// 2. Fallback: reallocate memory when newLen > oldLen
// This test covers the fallback reallocation path
void testReplaceArgSubstringFallback() {
    const char *test_arg = "hello world";
    const char *original = "ello";
    const char *replacement = "greetings";
    const char *expected = "hgreetings world";

    // Create a command with a test argument
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "myindex", test_arg);

    int arg_index = 2;
    size_t pos = 1;
    size_t oldLen = strlen(original);
    size_t newLen = strlen(replacement);
    MRCommand_ReplaceArgSubstring(&cmd, arg_index, pos, oldLen, replacement, newLen);

    // Verify the replacement worked correctly
    mu_check(!strcmp(expected, cmd.strs[2]));
    mu_assert_int_eq(strlen(expected), cmd.lens[2]);

    MRCommand_Free(&cmd);
}

// Test the optimization case when replacement string is same or shorter
// This uses the space-padding optimization to avoid memory reallocation
void testReplaceArgSubstringOptimization() {
    // Create a command with a test argument
    const char *test_arg = "hello world";
    const char *original = "ello";
    const char *replacement = "hi";
    const char *expected = "hhi   world";

    // Create a command with a test argument
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "myindex", test_arg);

    int arg_index = 2;
    size_t pos = 1;
    size_t oldLen = strlen(original);
    size_t newLen = strlen(replacement);
    MRCommand_ReplaceArgSubstring(&cmd, arg_index, pos, oldLen, replacement, newLen);

    // Verify the replacement worked with space padding
    mu_check(!strcmp(expected, cmd.strs[2]));
    mu_assert_int_eq(strlen(test_arg), cmd.lens[2]); // Original length unchanged

    MRCommand_Free(&cmd);
}

int main(int argc, char **argv) {
    RMUTil_InitAlloc();

    MU_RUN_TEST(testReplaceArgSubstringFallback);
    MU_RUN_TEST(testReplaceArgSubstringOptimization);

    MU_REPORT();
    return minunit_status;
}
