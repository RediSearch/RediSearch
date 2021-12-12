
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
