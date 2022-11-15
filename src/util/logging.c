/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "logging.h"

int LOGGING_LEVEL = 0;
// L_DEBUG | L_INFO

void LOGGING_INIT(int level) {
  LOGGING_LEVEL = level;
}
