#include "redisearch.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/args.h"
#include "trie/trie_type.h"
#include "query_error.h"

/*
## FT.SUGGADD key string score [INCR] [PAYLOAD {payload}]

Add a suggestion string to an auto-complete suggestion dictionary. This is
disconnected from the
index definitions, and leaves creating and updating suggestino dictionaries to
the user.

### Parameters:

   - key: the suggestion dictionary key.

   - string: the suggestion string we index

   - score: a floating point number of the suggestion string's weight

   -INCR: if set, we increment the existing entry of the suggestion by the
given
score, instead
of
    replacing the score. This is useful for updating the dictionary based on
user queries in
real
    time

   - PAYLOAD: Add a payload to the suggestion string that will be used as additional information.

### Returns:

Integer reply: the current size of the suggestion dictionary.
*/
int RSSuggestAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4 || argc > 7) {
    return RedisModule_WrongArity(ctx);
  }

  int incr = 0, rv = AC_OK;
  RSPayload payload = {0};
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 4, argc - 4);
  while (!AC_IsAtEnd(&ac)) {
    const char *s = AC_GetStringNC(&ac, NULL);
    if (!strcasecmp(s, "INCR")) {
      incr = 1;
    } else if (!strcasecmp(s, "PAYLOAD")) {
      if ((rv = AC_GetString(&ac, (const char **)&payload.data, &payload.len, 0)) != AC_OK) {
        return RMUtil_ReplyWithErrorFmt(ctx, "Invalid payload: %s", AC_Strerror(rv));
      }
    } else {
      return RMUtil_ReplyWithErrorFmt(ctx, "Unknown argument `%s`", s);
    }
  }

  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  RedisModuleString *val = argv[2];
  double score;
  if ((RedisModule_StringToDouble(argv[3], &score) != REDISMODULE_OK)) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid score");
  }

  /* Create an empty value object if the key is currently empty. */
  Trie *tree;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    tree = NewTrie();
    RedisModule_ModuleTypeSetValue(key, TrieType, tree);
  } else {
    tree = RedisModule_ModuleTypeGetValue(key);
  }

  /* Insert the new element. */
  Trie_Insert(tree, val, score, incr, &payload);

  RedisModule_ReplyWithLongLong(ctx, tree->size);
  RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}

/*
## FT.SUGLEN key

Get the size of an autoc-complete suggestion dictionary

### Parameters:

   - key: the suggestion dictionary key.

### Returns:

Integer reply: the current size of the suggestion dictionary.
*/
int RSSuggestLenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc != 2) return RedisModule_WrongArity(ctx);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  Trie *tree = RedisModule_ModuleTypeGetValue(key);
  return RedisModule_ReplyWithLongLong(ctx, tree ? tree->size : 0);
}

/*
## FT.SUGDEL key str

Delete a string from a suggestion index.

### Parameters:

   - key: the suggestion dictionary key.

   - str: the string to delete

### Returns:

Integer reply: 1 if the string was found and deleted, 0 otherwise.
*/
int RSSuggestDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_ReplicateVerbatim(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  Trie *tree = RedisModule_ModuleTypeGetValue(key);
  if (!tree) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  size_t len;
  const char *str = RedisModule_StringPtrLen(argv[2], &len);
  return RedisModule_ReplyWithLongLong(ctx, Trie_Delete(tree, str, len));
}

/*
## FT.SUGGET key prefix [FUZZY] [MAX num] [WITHSCORES] [TRIM] [OPTIMIZE] [WITHPAYLOADS]

Get completion suggestions for a prefix

### Parameters:

   - key: the suggestion dictionary key

   - prefix: the prefix to complete on

   - FUZZY: if set,we do a fuzzy prefix search, including prefixes at
     levenshtein distance of 1  from the prefix sent

   - MAX num: If set, we limit the results to a maximum of `num`. The default
     is 5, and the number   cannot be greater than 10.

   - WITHSCORES: If set, we also return each entry's score

   - TRIM: If set, we remove very unlikely results

   - WITHPAYLOADS: If set, we also return each entry's payload as they were inserted, or nil if no
payload
    exists.
### Returns:

Array reply: a list of the top suggestions matching the prefix
*/

typedef struct {
  int fuzzy;
  int withScores;
  int trim;
  int optimize;
  int withPayloads;
  unsigned maxDistance;
  unsigned numResults;
} SuggestOptions;

int parseSuggestOptions(RedisModuleString **argv, int argc, SuggestOptions *options,
                        QueryError *status) {
  ACArgSpec argList[] = {
      {.name = "FUZZY", .type = AC_ARGTYPE_BOOLFLAG, .target = &options->fuzzy},
      {.name = "MAX",
       .type = AC_ARGTYPE_UINT,
       .target = &options->numResults,
       .intflags = AC_F_COALESCE | AC_F_GE1},
      {
          .name = "WITHSCORES",
          .type = AC_ARGTYPE_BOOLFLAG,
          .target = &options->withScores,
      },
      {.name = "OPTIMIZE", .type = AC_ARGTYPE_BOOLFLAG, .target = &options->optimize},
      {.name = "TRIM", .type = AC_ARGTYPE_BOOLFLAG, .target = &options->trim},
      {.name = "WITHPAYLOADS", .type = AC_ARGTYPE_BOOLFLAG, .target = &options->withPayloads},
      {NULL}};

  ACArgSpec *errArg = NULL;
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv, argc);
  int rv = AC_ParseArgSpec(&ac, argList, &errArg);
  if (rv != AC_OK) {
    if (rv == AC_ERR_ENOENT) {
      // Argument not recognized
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Unrecognized argument: %s",
                             AC_GetStringNC(&ac, NULL));
    } else if (errArg) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "%s: %s", errArg->name, AC_Strerror(rv));
    } else {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Error parsing arguments: %s",
                             AC_Strerror(rv));
    }
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

int RSSuggestGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc < 3 || argc > 10) return RedisModule_WrongArity(ctx);

  // get the string to search for
  size_t len;
  const char *s = RedisModule_StringPtrLen(argv[2], &len);
  if (len >= TRIE_MAX_PREFIX * sizeof(rune)) {
    return RedisModule_ReplyWithError(ctx, "Invalid query length");
  }

  SuggestOptions options = {.numResults = 5};
  QueryError status = {0};
  if (parseSuggestOptions(argv + 3, argc - 3, &options, &status) != REDISMODULE_OK) {
    goto parse_error;
  }

  if (options.fuzzy) {
    options.maxDistance = 1;
  }

parse_error:
  if (QueryError_HasError(&status)) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
    return REDISMODULE_OK;
  }

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  // make sure the key is a trie
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  Trie *tree = RedisModule_ModuleTypeGetValue(key);
  if (tree == NULL) {
    return RedisModule_ReplyWithNull(ctx);
  }

  Vector *res = Trie_Search(tree, s, len, options.numResults, options.maxDistance, 1, options.trim,
                            options.optimize);
  if (!res) {
    return RedisModule_ReplyWithError(ctx, "Invalid query");
  }
  // if we also need to return scores, we need double the records
  unsigned mul = 1;
  mul = options.withScores ? mul + 1 : mul;
  mul = options.withPayloads ? mul + 1 : mul;
  RedisModule_ReplyWithArray(ctx, Vector_Size(res) * mul);

  for (size_t i = 0; i < Vector_Size(res); i++) {
    TrieSearchResult *e;
    Vector_Get(res, i, &e);

    RedisModule_ReplyWithStringBuffer(ctx, e->str, e->len);
    if (options.withScores) {
      RedisModule_ReplyWithDouble(ctx, e->score);
    }
    if (options.withPayloads) {
      if (e->payload)
        RedisModule_ReplyWithStringBuffer(ctx, e->payload, e->plen);
      else
        RedisModule_ReplyWithNull(ctx);
    }

    TrieSearchResult_Free(e);
  }
  Vector_Free(res);

  return REDISMODULE_OK;
}
