#include <stdio.h>
#include "version.h"

/* This is a utility that prints the current semantic version string, to be used in make files */

#define XSTR(x) #x
#define STR(x) XSTR(x)

#define REDISEARCH_VERSION_STRING \
  STR(REDISEARCH_VERSION_MAJOR) "." STR(REDISEARCH_VERSION_MINOR) "." STR(REDISEARCH_VERSION_PATCH)

int main(int argc, char **argv) {
  printf("%s\n", REDISEARCH_VERSION_STRING);
  return 0;
}