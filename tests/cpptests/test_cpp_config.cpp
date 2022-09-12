
#include "gtest/gtest.h"
#include "src/config.h"

extern "C" int setMultiTextOffsetDelta(RSConfig *config, ArgsCursor *ac, QueryError *status);

class ConfigTest : public ::testing::Test {};

TEST_F(ConfigTest, testconfigMultiTextOffsetDeltaSlopNeg) {
    ArgsCursor ac;
    QueryError status = {.code = QUERY_OK};
    const char *args[] = {"-1"};
    ArgsCursor_InitCString(&ac, &args[0], 1);
    int res = setMultiTextOffsetDelta(&RSGlobalConfig, &ac, &status);
    // Setter should fail with a negative value
    ASSERT_EQ(res, REDISMODULE_ERR);
    ASSERT_EQ(status.code, QUERY_EPARSEARGS);
    QueryError_ClearError(&status);

    const char *args2[] = {"50"};
    ArgsCursor_InitCString(&ac, &args2[0], 1);
    res = setMultiTextOffsetDelta(&RSGlobalConfig, &ac, &status);
    ASSERT_EQ(res, REDISMODULE_OK);
}
