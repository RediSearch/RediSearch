/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "util/arg_parser.h"
#include "deps/rmutil/args.h"
#include <vector>
#include <string>

class ArgParserTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize test arguments
        test_args = {"COMMAND", "TIMEOUT", "5000", "VERBOSE", "FORMAT", "json", "LIMIT", "10", "20"};
        ArgsCursor_InitCString(&cursor, test_args.data(), test_args.size());

        // Create parser
        parser = ArgParser_New(&cursor, "COMMAND");
        ASSERT_NE(parser, nullptr);
    }

    void TearDown() override {
        // Clean up parser
        if (parser) {
            ArgParser_Free(parser);
            parser = nullptr;
        }
    }

    // Helper method to create a fresh cursor with custom arguments
    void SetupCustomArgs(const std::vector<const char*>& args) {
        if (parser) {
            ArgParser_Free(parser);
        }
        custom_args = args;
        ArgsCursor_InitCString(&custom_cursor, custom_args.data(), custom_args.size());
        parser = ArgParser_New(&custom_cursor, "COMMAND");
        ASSERT_NE(parser, nullptr);
    }

    std::vector<const char*> test_args;
    std::vector<const char*> custom_args;
    ArgsCursor cursor;
    ArgsCursor custom_cursor;
    ArgParser *parser = nullptr;
};

TEST_F(ArgParserTest, BasicCreationAndDestruction) {
    ASSERT_TRUE(ArgParser_HasMore(parser));
}

TEST_F(ArgParserTest, ParseBooleanFlag) {
    SetupCustomArgs({"COMMAND", "VERBOSE"});

    bool verbose = false;
    ArgParser_AddBoolV(parser, "VERBOSE", "Enable verbose output", &verbose,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_DEFAULT_FLAG, false,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_TRUE(verbose) << "VERBOSE flag should be set to true";
    ASSERT_TRUE(ArgParser_WasParsed(parser, "VERBOSE"));
}

TEST_F(ArgParserTest, ParseLongInteger) {
    SetupCustomArgs({"COMMAND", "TIMEOUT", "5000"});

    long long timeout = 0;
    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_RANGE, 100LL, 300000LL,
                      ARG_OPT_DEFAULT_INT, 1000LL,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_EQ(timeout, 5000LL) << "TIMEOUT should be parsed as 5000";
    ASSERT_TRUE(ArgParser_WasParsed(parser, "TIMEOUT"));
}

TEST_F(ArgParserTest, ParseString) {
    SetupCustomArgs({"COMMAND", "FORMAT", "json"});

    const char *format = nullptr;
    ArgParser_AddStringV(parser, "FORMAT", "Output format", &format,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_DEFAULT_STR, "text",
                         ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_STREQ(format, "json") << "FORMAT should be parsed as 'json'";
    ASSERT_TRUE(ArgParser_WasParsed(parser, "FORMAT"));
}

TEST_F(ArgParserTest, ParseSubArgs) {
    SetupCustomArgs({"COMMAND", "LIMIT", "10", "20"});

    ArgsCursor limit_args;
    ArgParser_AddSubArgsV(parser, "LIMIT", "Limit results", &limit_args, 2, 2,
                          ARG_OPT_OPTIONAL,
                          ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_TRUE(ArgParser_WasParsed(parser, "LIMIT"));

    // Verify the sub-arguments were parsed correctly
    int offset, limit;
    ASSERT_EQ(AC_GetInt(&limit_args, &offset, 0), AC_OK);
    ASSERT_EQ(AC_GetInt(&limit_args, &limit, 0), AC_OK);
    ASSERT_EQ(offset, 10);
    ASSERT_EQ(limit, 20);
}

TEST_F(ArgParserTest, MultipleArguments) {
    SetupCustomArgs({"COMMAND", "TIMEOUT", "5000", "VERBOSE", "FORMAT", "json", "LIMIT", "10", "20"});

    bool verbose = false;
    long long timeout = 0;
    const char *format = nullptr;
    ArgsCursor limit_args;

    ArgParser_AddBoolV(parser, "VERBOSE", "Enable verbose output", &verbose,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_END);

    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_RANGE, 100LL, 300000LL,
                      ARG_OPT_END);

    ArgParser_AddStringV(parser, "FORMAT", "Output format", &format,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_END);

    ArgParser_AddSubArgsV(parser, "LIMIT", "Limit results", &limit_args, 2, 2,
                          ARG_OPT_OPTIONAL,
                          ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);

    // Verify all arguments were parsed correctly
    ASSERT_TRUE(verbose);
    ASSERT_EQ(timeout, 5000LL);
    ASSERT_STREQ(format, "json");

    int offset, limit;
    ASSERT_EQ(AC_GetInt(&limit_args, &offset, 0), AC_OK);
    ASSERT_EQ(AC_GetInt(&limit_args, &limit, 0), AC_OK);
    ASSERT_EQ(offset, 10);
    ASSERT_EQ(limit, 20);
}

TEST_F(ArgParserTest, RequiredArgumentMissing) {
    SetupCustomArgs({"COMMAND", "TIMEOUT", "5000"});

    const char *required_arg = nullptr;
    ArgParser_AddStringV(parser, "REQUIRED_ARG", "A required argument", &required_arg,
                         ARG_OPT_REQUIRED,
                         ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_FALSE(result.success) << "Parse should fail for missing required argument";
    ASSERT_NE(result.error_message, nullptr);
}

TEST_F(ArgParserTest, ValidationFailure) {
    SetupCustomArgs({"COMMAND", "TIMEOUT", "50"});  // Below minimum

    long long timeout = 0;
    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_RANGE, 100LL, 300000LL,  // Min 100, value is 50
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_FALSE(result.success) << "Parse should fail for value below minimum";
    ASSERT_NE(result.error_message, nullptr);
}

TEST_F(ArgParserTest, StrictModeUnknownArgument) {
    SetupCustomArgs({"COMMAND", "UNKNOWN_ARG", "value"});

    // Strict mode is enabled by default
    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_FALSE(result.success) << "Parse should fail for unknown argument in strict mode";
    ASSERT_NE(result.error_message, nullptr);
}

TEST_F(ArgParserTest, DefaultValues) {
    SetupCustomArgs({"COMMAND"});  // No arguments provided

    long long timeout = 0;
    const char *format = nullptr;
    bool verbose = true;  // Will be overridden by default

    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_DEFAULT_INT, 1000LL,
                      ARG_OPT_END);

    ArgParser_AddStringV(parser, "FORMAT", "Output format", &format,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_DEFAULT_STR, "text",
                         ARG_OPT_END);

    ArgParser_AddBoolV(parser, "VERBOSE", "Enable verbose output", &verbose,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_DEFAULT_FLAG, false,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);

    // Verify default values were applied
    ASSERT_EQ(timeout, 1000LL);
    ASSERT_STREQ(format, "text");
    ASSERT_FALSE(verbose);
}

TEST_F(ArgParserTest, PositionalArguments) {
    SetupCustomArgs({"COMMAND", "FIRST", "first_pos_value", "SECOND", "second_pos_value", "TIMEOUT", "5000"});

    const char *first_arg = nullptr;
    const char *second_arg = nullptr;
    long long timeout = 0;

    // Add positional arguments
    ArgParser_AddStringV(parser, "FIRST", "First positional argument", &first_arg,
                         ARG_OPT_REQUIRED,
                         ARG_OPT_POSITION, 1,  // First position after command
                         ARG_OPT_END);

    ArgParser_AddStringV(parser, "SECOND", "Second positional argument", &second_arg,
                         ARG_OPT_REQUIRED,
                         ARG_OPT_POSITION, 2,  // Second position after command
                         ARG_OPT_END);

    // Add named argument
    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);

    ASSERT_STREQ(first_arg, "first_pos_value");
    ASSERT_STREQ(second_arg, "second_pos_value");
    ASSERT_EQ(timeout, 5000LL);
}

TEST_F(ArgParserTest, BitflagArguments) {
    SetupCustomArgs({"COMMAND", "FLAG1", "FLAG3", "TIMEOUT", "5000"});

    int flags = 0;
    long long timeout = 0;

    // Define some flag masks
    const unsigned long long FLAG1_MASK = 0x01;
    const unsigned long long FLAG2_MASK = 0x02;
    const unsigned long long FLAG3_MASK = 0x04;

    ArgParser_AddBitflagV(parser, "FLAG1", "Enable flag 1", &flags, sizeof(flags), FLAG1_MASK,
                          ARG_OPT_OPTIONAL,
                          ARG_OPT_END);

    ArgParser_AddBitflagV(parser, "FLAG2", "Enable flag 2", &flags, sizeof(flags), FLAG2_MASK,
                          ARG_OPT_OPTIONAL,
                          ARG_OPT_END);

    ArgParser_AddBitflagV(parser, "FLAG3", "Enable flag 3", &flags, sizeof(flags), FLAG3_MASK,
                          ARG_OPT_OPTIONAL,
                          ARG_OPT_END);

    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);

    // Check that FLAG1 and FLAG3 are set, but not FLAG2
    ASSERT_EQ(flags & FLAG1_MASK, FLAG1_MASK) << "FLAG1 should be set";
    ASSERT_EQ(flags & FLAG2_MASK, 0) << "FLAG2 should not be set";
    ASSERT_EQ(flags & FLAG3_MASK, FLAG3_MASK) << "FLAG3 should be set";
    ASSERT_EQ(timeout, 5000LL);
}

// Callback function for testing
static void test_callback(ArgParser *parser, void *target, void *user_data) {
    int *callback_count = (int*)user_data;
    (*callback_count)++;
}

TEST_F(ArgParserTest, CallbackExecution) {
    SetupCustomArgs({"COMMAND", "VERBOSE"});

    bool verbose = false;
    int callback_count = 0;

    ArgParser_AddBoolV(parser, "VERBOSE", "Enable verbose output", &verbose,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_CALLBACK, test_callback, &callback_count,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_TRUE(verbose);
    ASSERT_EQ(callback_count, 1) << "Callback should have been called once";
}

// Custom validator function for testing
static int validate_even_number(void *target, const char **error_msg) {
    long long *value = (long long*)target;
    if (*value % 2 != 0) {
        *error_msg = "Value must be even";
        return -1;
    }
    return 0;
}

TEST_F(ArgParserTest, CustomValidator) {
    SetupCustomArgs({"COMMAND", "NUMBER", "42"});

    long long number = 0;
    ArgParser_AddLongLongV(parser, "NUMBER", "An even number", &number,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_VALIDATOR, validate_even_number,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_EQ(number, 42LL);
}

TEST_F(ArgParserTest, CustomValidatorFailure) {
    SetupCustomArgs({"COMMAND", "NUMBER", "43"});  // Odd number

    long long number = 0;
    ArgParser_AddLongLongV(parser, "NUMBER", "An even number", &number,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_VALIDATOR, validate_even_number,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_FALSE(result.success) << "Parse should fail for odd number";
    ASSERT_NE(result.error_message, nullptr);
}

TEST_F(ArgParserTest, RepeatableArguments) {
    SetupCustomArgs({"COMMAND", "TAG", "tag1", "TAG", "tag2", "TAG", "tag3"});

    // For repeatable arguments, we need to handle them differently
    // This is a simplified test - in practice you'd use callbacks to collect multiple values
    const char *tag = nullptr;
    int callback_count = 0;

    ArgParser_AddStringV(parser, "TAG", "Tag value", &tag,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_REPEATABLE,
                         ARG_OPT_CALLBACK, test_callback, &callback_count,
                         ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_EQ(callback_count, 3) << "Callback should have been called three times";
}

TEST_F(ArgParserTest, ErrorReporting) {
    SetupCustomArgs({"COMMAND", "TIMEOUT", "invalid_number"});

    long long timeout = 0;
    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_FALSE(result.success);
    ASSERT_NE(result.error_message, nullptr);
    ASSERT_STREQ(result.error_arg, "TIMEOUT");

    const char *error_str = ArgParser_GetErrorString(parser);
    ASSERT_NE(error_str, nullptr);
}

TEST_F(ArgParserTest, DoubleArgument) {
    SetupCustomArgs({"COMMAND", "SCORE", "3.14159"});

    double score = 0.0;
    ArgParser_AddDoubleV(parser, "SCORE", "Score value", &score,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_DOUBLE_EQ(score, 3.14159);
}

TEST_F(ArgParserTest, IntegerArgument) {
    SetupCustomArgs({"COMMAND", "COUNT", "42"});

    int count = 0;
    ArgParser_AddIntV(parser, "COUNT", "Count value", &count,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_EQ(count, 42);
}

TEST_F(ArgParserTest, UnsignedLongArgument) {
    SetupCustomArgs({"COMMAND", "SIZE", "1024"});

    unsigned long long size = 0;
    ArgParser_AddULongLongV(parser, "SIZE", "Size value", &size,
                        ARG_OPT_OPTIONAL,
                        ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_EQ(size, 1024ULL);
}

TEST_F(ArgParserTest, EmptyArguments) {
    SetupCustomArgs({"COMMAND"});

    // No arguments defined, should parse successfully
    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
}

TEST_F(ArgParserTest, AllowedValuesValid) {
    SetupCustomArgs({"COMMAND", "FORMAT", "json"});

    const char *format = nullptr;
    const char *allowed_formats[] = {"json", "xml", "csv", nullptr};

    ArgParser_AddStringV(parser, "FORMAT", "Output format", &format,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_ALLOWED_VALUES, allowed_formats,
                         ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_TRUE(result.success) << "Parse failed: " << ArgParser_GetErrorString(parser);
    ASSERT_STREQ(format, "json");
}

TEST_F(ArgParserTest, AllowedValuesInvalid) {
    SetupCustomArgs({"COMMAND", "FORMAT", "invalid"});

    const char *format = nullptr;
    const char *allowed_formats[] = {"json", "xml", "csv", nullptr};

    ArgParser_AddStringV(parser, "FORMAT", "Output format", &format,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_ALLOWED_VALUES, allowed_formats,
                         ARG_OPT_END);

    ArgParseResult result = ArgParser_Parse(parser);
    ASSERT_FALSE(result.success) << "Parse should fail for invalid value";
    ASSERT_STREQ(result.error_arg, "FORMAT");
}