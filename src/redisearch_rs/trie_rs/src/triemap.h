
#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

typedef struct RS_TrieMap RS_TrieMap;
typedef struct RS_SubTrieIterator RS_SubTrieIterator;
typedef struct RS_MatchesPrefixesIterator RS_MatchesPrefixesIterator;

RS_TrieMap *RS_NewTrieMap();
void* RS_TrieMap_Add(RS_TrieMap *t, const char *str, size_t len, void *value);
void* RS_TrieMap_Delete(RS_TrieMap *t, const char *str, size_t len);
void RS_TrieMap_Free(RS_TrieMap *t, void(*free_func)(void*));
void* RS_TrieMap_Get(RS_TrieMap *t, const char *str, size_t len);
size_t RS_TrieMap_Size(RS_TrieMap *t);

RS_SubTrieIterator* RS_TrieMap_Find(RS_TrieMap *t, const char *str, size_t len);
int RS_SubTrieIterator_Next(RS_SubTrieIterator* iter, char **str, size_t *len, void **data);
void RS_SubTrieIterator_Free(RS_SubTrieIterator* iter);

RS_MatchesPrefixesIterator* RS_TrieMap_FindPrefixes(RS_TrieMap *t, const char *str, size_t len);
int RS_MatchesPrefixesIterator_Next(RS_MatchesPrefixesIterator* iter, char **str, size_t *len, void **data);
void RS_MatchesPrefixesIterator_Free(RS_MatchesPrefixesIterator* iter);

size_t RS_TrieMap_MemUsage(RS_TrieMap *t);

#ifdef __cplusplus
}
#endif