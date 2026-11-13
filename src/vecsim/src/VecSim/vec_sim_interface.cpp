/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/vec_sim_interface.h"
#include <cassert>
#include <cstdarg>
#include <iostream>
#include <fstream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <filesystem>
#include <sstream>
#include <string>
#include <string_view>

// Global variable to store the current log context
struct TestNameLogContext {
    std::string test_name = "";
    std::string test_type = "";
};

static TestNameLogContext test_name_log_context;

extern "C" void VecSim_SetTestLogContext(const char *test_name, const char *test_type) {
    test_name_log_context.test_name = std::string(test_name);
    test_name_log_context.test_type = std::string(test_type);
}

/* Example:
 *   createLogString("ERROR", "Failed to open file");
 *   → "[2025-06-11 09:13:47.237] [ERROR] Failed to open file"
 */
static std::string createLogString(const char *level, const char *message) {
    // Get current timestamp
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    // Format timestamp
    std::tm *tm_info = std::localtime(&time_t);
    char timestamp[100];
    std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    // Format log entry
    std::ostringstream oss;
    oss << "[" << timestamp << "." << std::setw(3) << std::setfill('0') << ms.count() << "] ["
        << level << "] " << message;
    return oss.str();
}

// writes the logs to a file
void Vecsim_Log(void *ctx, const char *level, const char *message) {
    std::string log_entry = createLogString(level, message);
    // If test name context is not provided, write it to stdout
    if (test_name_log_context.test_name.empty() || test_name_log_context.test_type.empty()) {
        std::cout << log_entry << std::endl;
        return;
    }

    std::ostringstream path_stream;
    path_stream << "logs/tests/" << test_name_log_context.test_type << "/"
                << test_name_log_context.test_name << ".log";

    // Write to file
    std::ofstream log_file(path_stream.str(), std::ios::app);
    if (log_file.is_open()) {
        log_file << log_entry << std::endl;
        log_file.close();
    }
}

timeoutCallbackFunction VecSimIndexInterface::timeoutCallback = [](void *ctx) { return 0; };
logCallbackFunction VecSimIndexInterface::logCallback = Vecsim_Log;
VecSimWriteMode VecSimIndexInterface::asyncWriteMode = VecSim_WriteAsync;
