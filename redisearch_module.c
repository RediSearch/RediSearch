/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/**
 * @file redisearch_module.c
 * @brief Wrapper module that combines RediSearch with disk storage
 *
 * This file serves as the entry point for the combined RediSearch + disk storage module.
 * It simply re-exports the RediSearch module entry point, allowing the static RediSearch
 * library to be linked into our shared module.
 */

// The RedisModule_OnLoad function is defined in the RediSearch static library
// We don't need to redefine it here - the linker will find it in the static library
// This file just ensures we have a source file for the shared library target

// Optional: We could add disk storage initialization here in the future
// For now, this file just serves as a placeholder to satisfy CMake's requirement
// that shared library targets have at least one source file

// Dummy function to ensure this compilation unit is not empty
void __redisearch_module_placeholder(void) {
    // This function is never called - it just ensures the object file is not empty
}
