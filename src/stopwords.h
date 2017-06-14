#ifndef __REDISEARCH_STOPWORDS_H___
#define __REDISEARCH_STOPWORDS_H___

#include <stdlib.h>
#include "redismodule.h"

static const char *DEFAULT_STOPWORDS[] = {
    "a",    "is",    "the",   "an",   "and",  "are", "as",  "at",   "be",   "but",  "by",   "for",
    "if",   "in",    "into",  "it",   "no",   "not", "of",  "on",   "or",   "such", "that", "their",
    "then", "there", "these", "they", "this", "to",  "was", "will", "with", NULL};

struct StopWordList;

/* Check if a stopword list contains a term. The term must be already lowercased */
int StopWordList_Contains(struct StopWordList *sl, const char *term, size_t len);

/* Create a new stopword list from a list of redis strings */
struct StopWordList *NewStopWordList(RedisModuleString **strs, size_t len);

/* Free a stopword list's memory */
void StopWordList_Free(struct StopWordList *sl);

/* Load a stopword list from RDB */
struct StopWordList *StopWordList_RdbLoad(RedisModuleIO *rdb, int encver);

/* Save a stopword list to RDB */
void StopWordList_RdbSave(RedisModuleIO *rdb, struct StopWordList *sl);

#endif