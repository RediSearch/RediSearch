/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

typedef enum { RSBuildType_OSS, RSBuildType_Enterprise } RSBuildType;

// Defines the build type - determined at load time
extern const RSBuildType RSBuildType_g;