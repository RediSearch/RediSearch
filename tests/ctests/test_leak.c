
#include "test_util.h"

#include <stdlib.h>

int testLeak() {
	void *p = malloc(1024);
	return 0;
}

TEST_MAIN({
  TESTFUNC(testLeak);
})
