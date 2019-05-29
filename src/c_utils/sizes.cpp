#include <redisearch.h>
#include <inverted_index.h>
#include <cstdio>
#include <c_utils/api_stubs.h>

pthread_rwlock_t RWLock = PTHREAD_RWLOCK_INITIALIZER;

int main(int, char **) {
  printf("Size of document metadata: %lu\n", sizeof(RSDocumentMetadata));
  printf("Size of inverted index: %lu\n", sizeof(InvertedIndex));
  printf("Size of index block: %lu\n", sizeof(IndexBlock));
  return 0;
}
