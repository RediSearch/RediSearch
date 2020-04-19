#include "document.h"
#include "geo_index.h"
#include "err.h"
#include "util/logging.h"
#include "commands.h"
#include "rmutil/rm_assert.h"
#include "rmutil/strings.h"

/* FT.DEL {index} {doc_id}
 *  Delete a document from the index. Returns 1 if the document was in the index, or 0 if not.
 *
 *  **NOTE**: This does not actually delete the document from the index, just marks it as deleted
 * If DD (Delete Document) is set, we also delete the document.
 */

int RS_DelDocument(RedisModuleCtx *ctx, IndexSpec *sp, RedisModuleString *docKey, int delDoc) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);

  // Get the doc ID
  t_docId id = DocTable_GetIdR(&sp->docs, docKey);
  if (id == 0) {
    return 0;
    // ID does not exist.
  }

  for (size_t i = 0; i < sp->numFields; ++i) {
    FieldSpec *fs = sp->fields + i;
    if (!FIELD_IS(fs, INDEXFLD_T_GEO)) {
      continue;
    }
    GeoIndex gi = {.ctx = &sctx, .sp = fs};
    GeoIndex_RemoveEntries(&gi, sctx.spec, id);
  }

  int rc = DocTable_DeleteR(&sp->docs, docKey);
  if (rc) {
    sp->stats.numDocuments--;

    // If needed - delete the actual doc
    if (delDoc) {
      RedisModuleKey *dk = RedisModule_OpenKey(ctx, docKey, REDISMODULE_WRITE);
      if (dk && RedisModule_KeyType(dk) == REDISMODULE_KEYTYPE_HASH) {
        RedisModule_DeleteKey(dk);
      } else {
        RedisModule_Log(ctx, "warning", "Document %s doesn't exist",
                        RedisModule_StringPtrLen(docKey, NULL));
      }
      RedisModule_CloseKey(dk);
    }

    // Increment the index's garbage collector's scanning frequency after document deletions
    if (sp->gc) {
      GCContext_OnDelete(sp->gc);
    }
  }
  return rc;
}

int DeleteCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  int delDoc = 0;

  if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);
  if (argc == 4 && RMUtil_StringEqualsCaseC(argv[3], "DD")) {
    delDoc = 1;
  }

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisModuleString *docKey = argv[2];

  int rc = RS_DelDocument(ctx, sp, docKey, delDoc);
  if (rc) {
    if (!delDoc) {
      RedisModule_Replicate(ctx, RS_DEL_CMD, "cs", sp->name, argv[2]);
    } else {
      RedisModule_Replicate(ctx, RS_DEL_CMD, "csc", sp->name, argv[2], "dd");
    }
  }
  return RedisModule_ReplyWithLongLong(ctx, rc);
}