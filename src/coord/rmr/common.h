/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#ifndef STRINGIFY
#define __STRINGIFY(x) #x
#define STRINGIFY(x) __STRINGIFY(x)
#endif

#ifndef __ignore__
#define __ignore__(X) \
    do { \
        int rc = (X); \
        if (rc == -1) \
            ; \
    } while(0)
#endif
