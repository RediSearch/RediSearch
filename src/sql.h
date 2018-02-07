#include <sqlite3.h>
#include "spec.h"
#include "index_iterator.h"
#include "query.h"

typedef enum {
  SQL_COLIDX_KEY = 0,  // Column for PK
  SQL_COLIDX_SCORE = 1,
  SQL_COLIDX_DOCID = 2,
  SQL_COLIDX_QUERY = 3,
  SQL_COLIDX_SPECIAL_MAX
} PredefColumns;

typedef enum {
  SQL_REQ_INVALID = 0,  // blank request
  SQL_REQ_PK,           // PK lookup
  SQL_REQ_SCAN,         // Just scan the index without any constraints
  SQL_REQ_QUERY,        // Direct query string
  SQL_REQ_CONSTRAINTS   // traditional translated SQL
} RequestType;

typedef struct {
  sqlite3_vtab base;
  RedisSearchCtx sctx;
} SQLTable;

typedef struct {
  sqlite3_vtab_cursor base;
  RequestType type;
  IndexIterator *iter;
  const FieldSpec *fields;
  RedisModuleKey *key;  // Key for current result. Cached so we don't need to recreate
  RSDocumentMetadata *dmd;
  t_docId did;
  t_docId maxId;
  DocTable *docs;
} SQLCursor;
