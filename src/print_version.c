#ifdef PRINT_VERSION_TARGET
#include <stdio.h>
#include "version.h"

/* This is a utility that prints the current semantic version string, to be used in make files */

int main(int argc, char **argv) {
  printf("%d.%d.%d\n", REDISEARCH_VERSION_MAJOR, REDISEARCH_VERSION_MINOR,
         REDISEARCH_VERSION_PATCH);
  return 0;
}
#endif