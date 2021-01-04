#ifndef __REDISEARCH_STOPWORDS_H___
#define __REDISEARCH_STOPWORDS_H___

#include <stdlib.h>
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

static const char *DEFAULT_STOPWORDS[] = {
    "a",    "is",    "the",   "an",   "and",  "are", "as",  "at",   "be",   "but",  "by",   "for",
    "if",   "in",    "into",  "it",   "no",   "not", "of",  "on",   "or",   "such", "that", "their",
    "then", "there", "these", "they", "this", "to",  "was", "will", "with", NULL};

#ifndef __REDISEARCH_STOPORWORDS_C__
typedef struct StopWordList StopWordList;
#else
struct StopWordList;
#endif

/* Check if a stopword list contains a term. The term must be already lowercased */
int StopWordList_Contains(const struct StopWordList *sl, const char *term, size_t len);

struct StopWordList *DefaultStopWordList();
void StopWordList_FreeGlobals(void);

/* Create a new stopword list from a list of redis strings */
struct StopWordList *NewStopWordList(RedisModuleString **strs, size_t len);

/* Create a new stopword list from a list of NULL-terminated C strings */
struct StopWordList *NewStopWordListCStr(const char **strs, size_t len);

/* Free a stopword list's memory */
void StopWordList_Unref(struct StopWordList *sl);

#define StopWordList_Free StopWordList_Unref

/* Load a stopword list from RDB */
struct StopWordList *StopWordList_RdbLoad(RedisModuleIO *rdb, int encver);

/* Save a stopword list to RDB */
void StopWordList_RdbSave(RedisModuleIO *rdb, struct StopWordList *sl);

void StopWordList_Ref(struct StopWordList *sl);

void ReplyWithStopWordsList(RedisModuleCtx *ctx, struct StopWordList *sl);

#ifdef __cplusplus
}
#endif
#endif