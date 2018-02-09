#include "sql.h"
#include "spec.h"
#include <assert.h>

#define RQL_SET_ERR(tbl, ...)                           \
  printf(__VA_ARGS__);                                  \
  printf("\n");                                         \
  if (!(tbl)->base.zErrMsg) {                           \
    (tbl)->base.zErrMsg = sqlite3_mprintf(__VA_ARGS__); \
  }

#define CURSOR_TABLE(cur) ((SQLTable *)((cur)->base.pVtab))
#define CURSOR_RCTX(cur) ((CURSOR_TABLE(cur))->sctx.redisCtx)
#define CURSOR_DOCS(cur) (cur)->docs
#define TABLE_SPEC(tbl) ((tbl)->sctx.spec)
#define NUM_BUILTIN_COLS 4

static char *GetSQLSchema(const IndexSpec *spec) {
  char *schema = sqlite3_mprintf(
      "CREATE TABLE %s (__RSID__ text primary key, __SCORE__ NUMERIC HIDDEN, __ROWID__ INTEGER "
      "HIDDEN, __QUERY__ TEXT HIDDEN",
      spec->name);
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    const FieldSpec *fs = spec->fields + ii;
    if (fs->type == FIELD_FULLTEXT) {
      schema = sqlite3_mprintf("%z,%s TEXT", schema, fs->name);
    } else if (fs->type == FIELD_NUMERIC) {
      schema = sqlite3_mprintf("%z,%s NUMERIC", schema, fs->name);
    } else {
      // Cannot use non text or numeric fields!
      schema = sqlite3_mprintf("%z __DISABLED_%s TEXT HIDDEN", schema, fs->name);
    }
  }

  schema = sqlite3_mprintf("%z);", schema);
  return schema;
}

static int connectCommon(sqlite3 *db, void *ptr, int argc, const char *const *argv,
                         sqlite3_vtab **ppvTab, char **errp) {
  if (argc < 3) {
    *errp = sqlite3_mprintf("Need table name (and possibly ft index)");
    return SQLITE_ERROR;
  }

  RedisModuleCtx *mctx = ptr;
  RedisSearchCtx sctx = {0};
  const char *indexName = argv[2];

  sctx.keyName = RedisModule_CreateStringPrintf(mctx, INDEX_SPEC_KEY_FMT, indexName);
  sctx.redisCtx = mctx;
  sctx.spec = IndexSpec_LoadEx(mctx, sctx.keyName, 0, &sctx.key);

  if (!sctx.spec) {
    *errp = sqlite3_mprintf("No such index `%s`", indexName);
    return SQLITE_ERROR;
  }

  char *schema = GetSQLSchema(sctx.spec);
  SQLTable *tab = calloc(1, sizeof(*tab));
  tab->sctx = sctx;
  *ppvTab = &tab->base;

  // Create the table
  int rc = sqlite3_declare_vtab(db, schema);
  assert(rc == SQLITE_OK);
  sqlite3_free(schema);
  return SQLITE_OK;
}

static int sql_Create(sqlite3 *db, void *ptr, int argc, const char *const *argv,
                      sqlite3_vtab **ppvTab, char **errp) {
  return connectCommon(db, ptr, argc, argv, ppvTab, errp);
}

static int sql_Connect(sqlite3 *db, void *ptr, int argc, const char *const *argv,
                       sqlite3_vtab **ppvTab, char **errp) {
  return connectCommon(db, ptr, argc, argv, ppvTab, errp);
}

static int sql_Free(sqlite3_vtab *vtab) {
  return SQLITE_OK;
}

typedef struct {
  uint8_t opType;
  uint8_t col;
} ParamConstraint;

typedef struct {
  uint32_t numParams;
  uint8_t type;
  uint8_t flags;
  ParamConstraint data[0];
} ParamHeader;

/** Make a PK only request */
static void makeSpecialSpec(sqlite3_vtab *tab, sqlite3_index_info *info, ParamHeader *header,
                            size_t index, RequestType type) {
  for (size_t ii = 0; ii < info->nConstraint; ++ii) {
    if (ii != index) {

      // Don't pass parameter - this can't be combined with other queries
      info->aConstraintUsage[ii].argvIndex = 0;

      // SQLite needs to double check this on its own.
      info->aConstraintUsage[ii].omit = 0;
    }
  }

  // Don't check the constraint itself
  info->aConstraintUsage[index].argvIndex = 1;
  info->aConstraintUsage[index].omit = 1;

  header->type = type;
  header->numParams = 1;
  header->flags = 0;
  header->data[0].opType = info->aConstraint[index].op;
}

static int sql_BestIndex(sqlite3_vtab *tab, sqlite3_index_info *info) {
  SQLTable *sql = (SQLTable *)tab;
  size_t hdrsize = sizeof(ParamHeader) + (sizeof(ParamConstraint) * info->nConstraint);
  ParamHeader *hdr = sqlite3_malloc(hdrsize);
  memset(hdr, 0, hdrsize);
  ParamConstraint *outConstraints = (void *)hdr->data;

  // Construct the query string
  size_t oix = 1;
  for (size_t ii = 0; ii < info->nConstraint; ++ii) {
    const struct sqlite3_index_constraint *constraint = info->aConstraint + ii;
    if (constraint->iColumn == SQL_COLIDX_KEY) {
      if (constraint->op == SQLITE_INDEX_CONSTRAINT_EQ ||
          constraint->op == SQLITE_INDEX_CONSTRAINT_IS) {
        makeSpecialSpec(tab, info, hdr, SQL_REQ_PK, ii);
        break;
      } else {
        // Can't handle this now!
        continue;
      }
    } else if (constraint->iColumn == SQL_COLIDX_QUERY) {
      if (constraint->op == SQLITE_INDEX_CONSTRAINT_EQ ||
          constraint->op == SQLITE_INDEX_CONSTRAINT_IS) {
        makeSpecialSpec(tab, info, hdr, SQL_REQ_QUERY, ii);
        break;
      } else {
        RQL_SET_ERR(sql, "Query cannot be used for negative column constraints");
        return SQLITE_MISUSE;
      }
    } else if (constraint->iColumn < SQL_COLIDX_SPECIAL_MAX) {
      RQL_SET_ERR(sql, "Column is not queryable");
      return SQLITE_MISUSE;
    }

    ParamConstraint *outC = hdr->data + hdr->numParams++;
    outC->col = constraint->iColumn - NUM_BUILTIN_COLS;
    outC->opType = constraint->op;

    switch (outC->opType) {
      case SQLITE_INDEX_CONSTRAINT_EQ:
      case SQLITE_INDEX_CONSTRAINT_IS:
      case SQLITE_INDEX_CONSTRAINT_NE:
      case SQLITE_INDEX_CONSTRAINT_ISNOT:
      case SQLITE_INDEX_CONSTRAINT_LE:
      case SQLITE_INDEX_CONSTRAINT_LT:
      case SQLITE_INDEX_CONSTRAINT_GE:
      case SQLITE_INDEX_CONSTRAINT_GT:
        // Exact constraint checks don't need to be
        info->aConstraintUsage[ii].omit = 1;
        break;
      default:
        break;
    }

    // Write output information
    info->aConstraintUsage[ii].argvIndex = oix++;
  }

  if (hdr->type == SQL_REQ_INVALID) {
    if (hdr->numParams) {
      hdr->type = SQL_REQ_CONSTRAINTS;
    } else {
      hdr->type = SQL_REQ_SCAN;
    }
  }

  info->needToFreeIdxStr = 1;
  info->idxStr = (char *)hdr;
  info->estimatedCost = 9999;
  return SQLITE_OK;
}

static char *buildWhere(SQLTable *sql, const ParamHeader *hdr, int argc, sqlite3_value **argv) {
  char *where = NULL;
  for (size_t ii = 0; ii < hdr->numParams; ++ii) {
    const ParamConstraint *c = hdr->data + ii;
    const FieldSpec *fs = TABLE_SPEC(sql)->fields + c->col;
    const char *space = ii == 0 ? "" : " ";
    const char *prepend = "";
    if (c->opType == SQLITE_INDEX_CONSTRAINT_ISNOT || c->opType == SQLITE_INDEX_CONSTRAINT_NE) {
      prepend = "-";
    }

#define BASE_FORMAT               \
  "%z"   /*< prev */              \
  "%s"   /*<space*/               \
  "("    /*< Open query */        \
  "%s"   /*< possibly negative */ \
  "@%s:" /*< column name */

#define APPEND_WHERE(fmt, ...) \
  where = sqlite3_mprintf(BASE_FORMAT fmt ")", where, space, prepend, fs->name, ##__VA_ARGS__)

    // Direct comparisons:
    if (fs->type == FIELD_FULLTEXT) {
      switch (c->opType) {
        case SQLITE_INDEX_CONSTRAINT_EQ:
        case SQLITE_INDEX_CONSTRAINT_NE:
        case SQLITE_INDEX_CONSTRAINT_IS:
        case SQLITE_INDEX_CONSTRAINT_ISNOT:
          APPEND_WHERE("%s", sqlite3_value_text(argv[ii]));
          break;
        default:
          // Can't do anything with this param. Needs to be sorted by SQLite directly!
          break;
      }
    } else if (fs->type == FIELD_NUMERIC) {
      double val = sqlite3_value_double(argv[ii]);
      switch (c->opType) {
        case SQLITE_INDEX_CONSTRAINT_EQ:
        case SQLITE_INDEX_CONSTRAINT_NE:
        case SQLITE_INDEX_CONSTRAINT_IS:
        case SQLITE_INDEX_CONSTRAINT_ISNOT:
          APPEND_WHERE("[%f %f]", val, val);
          break;
        case SQLITE_INDEX_CONSTRAINT_GT:
          APPEND_WHERE("[(%f inf]", val);
          break;
        case SQLITE_INDEX_CONSTRAINT_GE:
          APPEND_WHERE("[%f inf]", val);
          break;
        case SQLITE_INDEX_CONSTRAINT_LT:
          APPEND_WHERE("[-inf (%f]", val);
          break;
        case SQLITE_INDEX_CONSTRAINT_LE:
          APPEND_WHERE("[-inf %f]", val);
          break;
        default:
          break;  // Can't do anything here!
      }
    } else {
      abort();  // Shouldn't happen!
    }
  }
  return where;
}

static IndexIterator *getWhereIterator(SQLTable *tbl, ParamHeader *hdr, RSSearchOptions *options,
                                       int argc, sqlite3_value **argv) {
  char *where = NULL;
  int whereNeedsFree = 0;
  char *err;

  if (hdr->type == SQL_REQ_QUERY) {
    where = (char *)sqlite3_value_text(argv[0]);
    whereNeedsFree = 0;
  } else {
    where = buildWhere(tbl, hdr, argc, argv);
    whereNeedsFree = 1;
  }

  QueryParseCtx *q = NewQueryParseCtx(&tbl->sctx, where, strlen(where), options);
  assert(q);
  QueryNode *rootNode = Query_Parse(q, &err);
  assert(rootNode);
  Query_Expand(q, NULL);
  QueryEvalCtx ev = {.docTable = &TABLE_SPEC(tbl)->docs,
                     .numTokens = q->numTokens,
                     .tokenId = 1,
                     .sctx = &tbl->sctx,
                     .opts = options};
  IndexIterator *rootIter = Query_EvalNode(&ev, rootNode);
  if (whereNeedsFree) {
    sqlite3_free(where);
  }
  Query_Free(q);

  return rootIter;
}

static void cursor_Reset(SQLCursor *cursor) {
  if (cursor->iter) {
    cursor->iter->Free(cursor->iter);
    cursor->iter = NULL;
  }

  if (cursor->key) {
    RedisModule_CloseKey(cursor->key);
  }

  cursor->dmd = NULL;
  cursor->did = 0;
  cursor->type = SQL_REQ_INVALID;
}

static int cursor_Next(sqlite3_vtab_cursor *curbase);

static int cursor_ScanIter(SQLCursor *cur) {
  // docid is 0
  while (++cur->did <= cur->maxId) {
    RSDocumentMetadata *dmd = cur->docs->docs + cur->did;
    if (dmd->flags & Document_Deleted) {
      continue;
    }
    cur->dmd = dmd;
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int sql_Filter(sqlite3_vtab_cursor *curBase, int idxNum, const char *idxStr, int argc,
                      sqlite3_value **argv) {
  // Construct the query itself, then the root filter, and then execute it!
  ParamHeader *hdr = (ParamHeader *)idxStr;
  ParamConstraint *constraints = (ParamConstraint *)hdr->data;
  SQLCursor *cur = (SQLCursor *)curBase;
  SQLTable *tbl = CURSOR_TABLE(cur);
  cursor_Reset(cur);

  cur->type = hdr->type;

  // Now that we have the WHERE string, let's build our options
  RSSearchOptions options = RS_DEFAULT_SEARCHOPTS;
  options.indexName = TABLE_SPEC(tbl)->name;
  options.concurrentMode = 0;
  options.flags |= Search_NoSort;
  if (hdr->type != SQL_REQ_QUERY) {
    options.flags |= Search_Verbatim;
  }

  // Filter simple query types:
  if (hdr->type == SQL_REQ_PK) {
    // Get the ID for the given key:
    const char *keystr = (const char *)sqlite3_value_text(argv[0]);
    t_docId did = DocTable_GetId(&TABLE_SPEC(tbl)->docs,
                                 (RSDocumentKey){.str = keystr, .len = strlen(keystr)});
    if (did == 0) {
      cur->iter = NULL;
      return SQLITE_OK;
    } else {
      abort();
    }
  } else if (hdr->type == SQL_REQ_SCAN) {
    // Wildcard iterator
    cur->maxId = CURSOR_DOCS(cur)->maxDocId;
    return cursor_ScanIter(cur);
    cur->iter = NewWildcardIterator(TABLE_SPEC(tbl)->docs.maxDocId);
  } else if (hdr->type == SQL_REQ_CONSTRAINTS || hdr->type == SQL_REQ_QUERY) {
    cur->iter = getWhereIterator(tbl, hdr, &options, argc, argv);
  } else {
    abort();
  }
  if (cur->iter) {
    cursor_Next(curBase);
  }
  return SQLITE_OK;
}

static int cursor_Next(sqlite3_vtab_cursor *curbase) {
  SQLCursor *cursor = (SQLCursor *)curbase;
  SQLTable *table = CURSOR_TABLE(cursor);

  if (cursor->key) {
    RedisModule_CloseKey(cursor->key);
    cursor->key = NULL;
  }

  if (cursor->type == SQL_REQ_SCAN) {
    return cursor_ScanIter(cursor);
  }

  while (1) {
    RSIndexResult *result = NULL;
    int rc = cursor->iter->Read(cursor->iter->ctx, &result);
    if (rc != INDEXREAD_OK) {
      break;
    }
    if (!result) {
      result = cursor->iter->Current(cursor->iter->ctx);
    }
    assert(result);
    cursor->did = result->docId;
    cursor->dmd = DocTable_Get(CURSOR_DOCS(cursor), cursor->did);
    if (cursor->dmd == NULL || cursor->dmd->flags & Document_Deleted) {
      continue;
    } else {
      return SQLITE_OK;
    }
  }

  // Hit an EOF
  cursor->iter->Free(cursor->iter);
  cursor->iter = NULL;
  cursor->did = 0;
  return SQLITE_OK;
}

static int cursor_IsEof(sqlite3_vtab_cursor *curbase) {
  SQLCursor *cursor = (SQLCursor *)curbase;
  if (cursor->type == SQL_REQ_SCAN) {
    return cursor->did > cursor->maxId;
  } else {
    return cursor->iter == NULL;
  }
}

static int cursor_Open(sqlite3_vtab *tbase, sqlite3_vtab_cursor **curpp) {
  SQLCursor *cursor = calloc(1, sizeof(*cursor));
  SQLTable *table = (SQLTable *)tbase;
  cursor->base.pVtab = tbase;
  cursor->fields = TABLE_SPEC(table)->fields;
  cursor->docs = &TABLE_SPEC(table)->docs;
  *curpp = &cursor->base;
  return SQLITE_OK;
}

static int cursor_Close(sqlite3_vtab_cursor *curbase) {
  // Leak
  SQLCursor *cursor = (SQLCursor *)curbase;
  if (cursor->key) {
    RedisModule_CloseKey(cursor->key);
  }
  if (cursor->iter) {
    cursor->iter->Free(cursor->iter);
  }
  free(cursor);
  return SQLITE_OK;
}

static int cursor_ColumnValue(sqlite3_vtab_cursor *curbase, sqlite3_context *ctx, int N) {
  SQLCursor *cursor = (SQLCursor *)curbase;
  RSDocumentMetadata *dmd = cursor->dmd;

  if (N < SQL_COLIDX_SPECIAL_MAX) {
    if (N == SQL_COLIDX_DOCID) {
      sqlite3_result_int64(ctx, cursor->did);
      return SQLITE_OK;
    }

    // Look up the document metadata for the item
    if (N == SQL_COLIDX_KEY) {
      sqlite3_result_text(ctx, dmd->keyPtr, -1, NULL);
    } else if (N == SQL_COLIDX_SCORE) {
      sqlite3_result_double(ctx, dmd->score);
    } else {
      RQL_SET_ERR(CURSOR_TABLE(cursor), "Unknown built-in column %d", N);
      return SQLITE_ERROR;
    }
    return SQLITE_OK;
  }

  N -= NUM_BUILTIN_COLS;
  const FieldSpec *fs = cursor->fields + N;
  if (FieldSpec_IsSortable(fs) && dmd->sortVector && dmd->sortVector->len > N) {
    RSValue *val = dmd->sortVector->values[N];
    if (val != NULL && (val = RSValue_Dereference(val))) {
      if (fs->type == FIELD_FULLTEXT && RSValue_IsString(val)) {
        size_t n;
        const char *s = RSValue_StringPtrLen(val, &n);
        sqlite3_result_text(ctx, s, n, NULL);
        return SQLITE_OK;
      } else if (fs->type == FIELD_NUMERIC && val->t == RSValue_Number) {
        sqlite3_result_double(ctx, val->numval);
        return SQLITE_OK;
      }
    }
  }

  if (!cursor->key) {
    RedisModuleString *tmpkey =
        RedisModule_CreateString(CURSOR_RCTX(cursor), dmd->keyPtr, strlen(dmd->keyPtr));
    cursor->key = RedisModule_OpenKey(CURSOR_RCTX(cursor), tmpkey, REDISMODULE_READ);
    RedisModule_FreeString(CURSOR_RCTX(cursor), tmpkey);
    if (!cursor->key) {
      printf("Couldn't open %s\n", dmd->keyPtr);
      RQL_SET_ERR(CURSOR_TABLE(cursor), "Couldn't open '%s'", dmd->keyPtr);
    }
  }
  RedisModuleString *value = NULL;
  int rc = RedisModule_HashGet(cursor->key, REDISMODULE_HASH_CFIELDS, fs->name, &value, NULL);
  if (rc != REDISMODULE_OK || value == NULL) {
    return SQLITE_OK;  // Can't get result for column
  }

  // Otherwise, return the result per the type:
  if (fs->type == FIELD_FULLTEXT) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(value, &n);
    sqlite3_result_text(ctx, s, n, NULL);
    return SQLITE_OK;
  } else if (fs->type == FIELD_NUMERIC) {
    double d;
    if (RedisModule_StringToDouble(value, &d) == REDISMODULE_OK) {
      sqlite3_result_double(ctx, d);
      return SQLITE_OK;
    }
  }
  return SQLITE_OK;
}

static int cursor_Rowid(sqlite3_vtab_cursor *curbase, sqlite_int64 *pRowId) {
  SQLCursor *cursor = (SQLCursor *)curbase;
  *pRowId = cursor->did;
  return SQLITE_OK;
}

static sqlite3_module sqlModule = {.xConnect = sql_Connect,
                                   .xCreate = sql_Create,
                                   .xDestroy = sql_Free,
                                   .xBestIndex = sql_BestIndex,
                                   .xDisconnect = sql_Free,
                                   .xOpen = cursor_Open,
                                   .xClose = cursor_Close,
                                   .xFilter = sql_Filter,
                                   .xNext = cursor_Next,
                                   .xEof = cursor_IsEof,
                                   .xColumn = cursor_ColumnValue,
                                   .xRowid = cursor_Rowid};

static sqlite3 *sqlite_db_g = NULL;

static int initDb(RedisModuleCtx *ctx) {
  RedisModuleCtx *ownCtx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_SelectDb(ownCtx, RedisModule_GetSelectedDb(ctx));

  int rc = sqlite3_open(":memory:", &sqlite_db_g);
  assert(rc == SQLITE_OK);
  rc = sqlite3_enable_load_extension(sqlite_db_g, 1);
  assert(rc == SQLITE_OK);
  rc = sqlite3_create_module(sqlite_db_g, "FT", &sqlModule, ownCtx);
  assert(rc == SQLITE_OK);
  return 0;
}

static void outputRow(sqlite3_stmt *stmt, size_t ncols, RedisModuleCtx *ctx) {
  RedisModule_ReplyWithArray(ctx, ncols);
  for (size_t ii = 0; ii < ncols; ++ii) {
    switch (sqlite3_column_type(stmt, ii)) {
      case SQLITE_BLOB:
      case SQLITE_TEXT: {
        const char *s = sqlite3_column_blob(stmt, ii);
        size_t n = sqlite3_column_bytes(stmt, ii);
        RedisModule_ReplyWithStringBuffer(ctx, s, n);
        break;
      }
      case SQLITE_FLOAT:
        RedisModule_ReplyWithDouble(ctx, sqlite3_column_double(stmt, ii));
        break;
      case SQLITE_INTEGER:
        RedisModule_ReplyWithLongLong(ctx, sqlite3_column_int64(stmt, ii));
        break;
      case SQLITE_NULL:
      default:
        RedisModule_ReplyWithNull(ctx);
        break;
    }
  }
}

int SQLRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Ensure that an SQL schema is created for each database
  if (!sqlite_db_g) {
    initDb(ctx);
  }

  sqlite3 *db = sqlite_db_g;

  if (argc < 2) {
    return RedisModule_ReplyWithError(ctx, "Simply need SQL text to execute!");
  }

  size_t nsql;
  char *errmsg;
  const char *sql = RedisModule_StringPtrLen(argv[1], &nsql);

  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    printf("Prepare '%s': failed (%d, %s)\n", sql, rc, sqlite3_errmsg(db));
    goto error;
  }

  for (size_t ii = 2; ii < argc; ++ii) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(argv[ii], &n);
    int rc = sqlite3_bind_text(stmt, ii - 1, s, n, NULL);
    if (rc != REDISMODULE_OK) {
      goto error;
    }
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_ROW) {
    // Empty response?
    RedisModule_ReplyWithArray(ctx, 0);
    sqlite3_finalize(stmt);
    return SQLITE_OK;
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  // Column header
  size_t ncols = sqlite3_column_count(stmt);
  RedisModule_ReplyWithArray(ctx, ncols * 2);
  for (size_t ii = 0; ii < ncols; ++ii) {
    RedisModule_ReplyWithSimpleString(ctx, sqlite3_column_name(stmt, ii));
    switch (sqlite3_column_type(stmt, ii)) {
      case SQLITE_BLOB:
      case SQLITE_TEXT:
        RedisModule_ReplyWithSimpleString(ctx, "$");
        break;
      case SQLITE_INTEGER:
        RedisModule_ReplyWithSimpleString(ctx, "i");
        break;
      case SQLITE_FLOAT:
        RedisModule_ReplyWithSimpleString(ctx, "f");
        break;
      case SQLITE_NULL:
      default:
        RedisModule_ReplyWithSimpleString(ctx, "-");
        break;
    }
  }

  size_t rowcount = 0;
  do {
    rowcount++;
    outputRow(stmt, ncols, ctx);
  } while ((rc = sqlite3_step(stmt)) == SQLITE_ROW);

  // Add one to count for header
  RedisModule_ReplySetArrayLength(ctx, rowcount + 1);
  sqlite3_finalize(stmt);
  return REDISMODULE_OK;

error:
  RedisModule_ReplyWithError(ctx, sqlite3_errmsg(db));
  if (stmt) {
    sqlite3_finalize(stmt);
  }
  return REDISMODULE_OK;
}
