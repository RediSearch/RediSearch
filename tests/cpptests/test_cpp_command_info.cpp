#include "gtest/gtest.h"
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>

class CommandInfoTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Get paths relative to the test binary location
        std::filesystem::path test_dir = std::filesystem::current_path();
        while (test_dir.filename() != "RediSearch" && !test_dir.empty()) {
            test_dir = test_dir.parent_path();
        }
        
        commands_json_path = test_dir / "commands.json";
        command_info_h_path = test_dir / "src" / "command_info" / "command_info.h";
        command_info_c_path = test_dir / "src" / "command_info" / "command_info.c";
    }
    
    std::filesystem::path commands_json_path;
    std::filesystem::path command_info_h_path;
    std::filesystem::path command_info_c_path;
};

TEST_F(CommandInfoTest, CommandsJsonExists) {
    ASSERT_TRUE(std::filesystem::exists(commands_json_path)) 
        << "commands.json should exist at: " << commands_json_path;
}

TEST_F(CommandInfoTest, GeneratedFilesExist) {
    ASSERT_TRUE(std::filesystem::exists(command_info_h_path))
        << "command_info.h should be generated at: " << command_info_h_path;
    
    ASSERT_TRUE(std::filesystem::exists(command_info_c_path))
        << "command_info.c should be generated at: " << command_info_c_path;
}

TEST_F(CommandInfoTest, CommandsJsonIsValid) {
    std::ifstream file(commands_json_path);
    ASSERT_TRUE(file.is_open()) << "Could not open commands.json";

    // Basic validation - check that it's not empty and contains expected patterns
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());

    ASSERT_GT(content.size(), 100) << "commands.json should not be empty";
    ASSERT_TRUE(content.find("FT.CREATE") != std::string::npos) << "Should contain FT.CREATE command";
    ASSERT_TRUE(content.find("FT.SEARCH") != std::string::npos) << "Should contain FT.SEARCH command";
    ASSERT_TRUE(content.find("FT.AGGREGATE") != std::string::npos) << "Should contain FT.AGGREGATE command";
}

TEST_F(CommandInfoTest, GeneratedFunctionsExist) {
    // Read the generated C file
    std::ifstream file(command_info_c_path);
    ASSERT_TRUE(file.is_open()) << "Could not open command_info.c";
    
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    
    // Check for key command functions
    std::vector<std::string> expected_functions = {
        "SetFtCreateInfo",
        "SetFtSearchInfo", 
        "SetFtAggregateInfo"
    };
    
    for (const auto& func_name : expected_functions) {
        EXPECT_TRUE(content.find(func_name) != std::string::npos)
            << "Generated C file should contain function: " << func_name;
    }
}

TEST_F(CommandInfoTest, CommandTipsAreGenerated) {
    // Read generated C file
    std::ifstream c_file(command_info_c_path);
    ASSERT_TRUE(c_file.is_open());
    std::string c_content((std::istreambuf_iterator<char>(c_file)),
                          std::istreambuf_iterator<char>());

    // Check that tips are generated for cursor commands (we know these have tips)
    EXPECT_TRUE(c_content.find(".tips = \"request_policy:special\"") != std::string::npos)
        << "Should have tips for cursor commands";

    // Check that .tips field appears in the generated code
    EXPECT_TRUE(c_content.find(".tips = ") != std::string::npos)
        << "Should have at least some commands with tips";
}

TEST_F(CommandInfoTest, GeneratedCodeStructure) {
    // Read generated C file
    std::ifstream c_file(command_info_c_path);
    ASSERT_TRUE(c_file.is_open());
    std::string c_content((std::istreambuf_iterator<char>(c_file)),
                          std::istreambuf_iterator<char>());

    // Check for expected patterns in generated code
    EXPECT_TRUE(c_content.find("RedisModuleCommandInfo info") != std::string::npos)
        << "Should contain RedisModuleCommandInfo structures";

    EXPECT_TRUE(c_content.find("REDISMODULE_COMMAND_INFO_VERSION") != std::string::npos)
        << "Should set proper version";

    EXPECT_TRUE(c_content.find("RedisModule_SetCommandInfo") != std::string::npos)
        << "Should call RedisModule_SetCommandInfo";

    // Count the number of Set*Info functions
    size_t pos = 0;
    int function_count = 0;
    while ((pos = c_content.find("int Set", pos)) != std::string::npos) {
        if (c_content.find("Info(RedisModuleCommand *cmd)", pos) != std::string::npos) {
            function_count++;
        }
        pos++;
    }

    EXPECT_GT(function_count, 20) << "Should have generated many Set*Info functions";
}
