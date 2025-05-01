/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "test_util.h"

#include <stdlib.h>

int testLeak() {
	void *p = malloc(1024);
	return 0;
}

TEST_MAIN({
  TESTFUNC(testLeak);
})
