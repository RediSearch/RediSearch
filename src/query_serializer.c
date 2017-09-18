#include "query.h"

int QueryResult_Serialize(QueryResult *r, RedisSearchCtx *sctx, RSSearchRequest *req) {
  RedisModuleCtx *ctx = sctx->redisCtx;

  if (r->errorString != NULL) {
    return RedisModule_ReplyWithError(ctx, r->errorString);
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithLongLong(ctx, (long long)r->totalResults);
  size_t arrlen = 1;

  const int withDocs = (req->fields.numFields || (req->flags & Search_NoContent) == 0);

  const char **fieldList = NULL;
  size_t numFieldList = 0;

  if (req->fields.numFields) {
    fieldList = (const char **)req->fields.rawFields;
    numFieldList = req->fields.numRawFields;
  }

  for (size_t i = 0; i < r->numResults; ++i) {

    Document doc = {NULL};
    RedisModuleKey *rkey = NULL;
    const ResultEntry *result = r->results + i;

    if (withDocs) {
      // Current behavior skips entire result if document does not exist.
      // I'm unusre if that's intentional or an oversight.
      RedisModuleString *idstr = RedisModule_CreateString(ctx, result->id, strlen(result->id));
      Redis_LoadDocumentEx(sctx, idstr, fieldList, numFieldList, &doc, &rkey);
      RedisModule_FreeString(ctx, idstr);
    }

    ++arrlen;

    RedisModule_ReplyWithStringBuffer(ctx, result->id, strlen(result->id));

    if (req->flags & Search_WithScores) {
      ++arrlen;
      RedisModule_ReplyWithDouble(ctx, result->score);
    }

    if (req->flags & Search_WithPayloads) {
      ++arrlen;
      const RSPayload *payload = result->payload;
      if (payload) {
        RedisModule_ReplyWithStringBuffer(ctx, payload->data, payload->len);
      } else {
        RedisModule_ReplyWithNull(ctx);
      }
    }

    if (req->flags & Search_WithSortKeys) {
      ++arrlen;
      const RSSortableValue *sortkey = result->sortKey;
      if (sortkey) {
        if (sortkey->type == RS_SORTABLE_NUM) {
          RedisModule_ReplyWithDouble(ctx, sortkey->num);
        } else {
          // RS_SORTABLE_NIL, RS_SORTABLE_STR
          RedisModule_ReplyWithStringBuffer(ctx, sortkey->str, strlen(sortkey->str));
        }
      } else {
        RedisModule_ReplyWithNull(ctx);
      }
    }

    if (withDocs) {
      ++arrlen;
      RedisModule_ReplyWithArray(ctx, doc.numFields * 2);
      for (size_t j = 0; j < doc.numFields; ++j) {
        RedisModule_ReplyWithStringBuffer(ctx, doc.fields[j].name, strlen(doc.fields[j].name));
        if (doc.fields[j].text) {
          RedisModule_ReplyWithString(ctx, doc.fields[j].text);
        } else {
          RedisModule_ReplyWithNull(ctx);
        }
      }
      if (rkey) {
        RedisModule_CloseKey(rkey);
      }
      Document_Free(&doc);
    }
  }

  RedisModule_ReplySetArrayLength(ctx, arrlen);

  return REDISMODULE_OK;
}
