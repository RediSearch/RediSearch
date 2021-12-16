#include "minunit.h"
#include <chan.h>

void testChan() {
  MRChannel *c = MR_NewChannel(0);
  mu_check(c != NULL);
  mu_assert_int_eq(0, MRChannel_Size(c));
  mu_assert_int_eq(0, MRChannel_MaxSize(c));

  for (int i = 0; i < 100; i++) {
    int *ptr = malloc(sizeof(*ptr));
    *ptr = i;
    int rc = MRChannel_Push(c, ptr);
    mu_assert_int_eq(1, rc);
    mu_assert_int_eq(i + 1, MRChannel_Size(c));
  }

  int count = 0;
  void *p;
  while (MRChannel_Size(c) && (p = MRChannel_Pop(c))) {
    mu_assert_int_eq(*(int *)p, count);
    count++;
    free(p);
  }
  mu_assert_int_eq(100, count);
  mu_assert_int_eq(0, MRChannel_Size(c));

  MRChannel_Free(c);
}

int main(int argc, char **argv) {
  MU_RUN_TEST(testChan);
  MU_REPORT();

  return minunit_status;
}