#include "rules.h"
#include "ruledefs.h"
#include "module.h"
#include "rmutil/util.h"
#include <sched.h>

#define SCAN_BATCH_SIZE 100
#define SCAN_MODE_R5 0
#define SCAN_MODE_R6 1

typedef enum {
  SCAN_STATE_UNINIT = 0,
  SCAN_STATE_STOPPED = 1,
  SCAN_STATE_RUNNING = 2,
  SCAN_STATE_RESTART = 3,  // lb
} ScanState;

typedef struct {
  size_t n;
  int isDone;
  RedisModuleScanCursor *cursor;
} scanCursor;

typedef struct {
  ScanState state;
  scanCursor cursor;
  uint64_t syncedRevision;
} Scanner;

static Scanner scanner_g;

static void scanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *keyobj,
                         void *privdata) {
  // body here should be similar to keyspace notification callback, except that
  // async is always forced
  scanCursor *c = privdata;
  RuleKeyItem item = {.kstr = keyname, .kobj = keyobj};
  SchemaRules_ProcessItem(ctx, &item, RULES_PROCESS_F_NOREINDEX | RULES_PROCESS_F_ASYNC);
  c->n++;
}

static void initScanner(Scanner *s) {
  memset(s, 0, sizeof(*s));
  s->state = SCAN_STATE_RUNNING;
}

static void onInitDone(RedisModuleCtx *unused1, void *ptr) {
  SchemaRules_InitialScanStatus_g = SC_INITSCAN_DONE;
  Scanner *s = ptr;
  Indexes_OnScanDone(s->syncedRevision);
}

static pthread_mutex_t statelock = PTHREAD_MUTEX_INITIALIZER;
#define GET_GIL()                                      \
  if (isThread) {                                      \
    RedisModule_ThreadSafeContextLock(RSDummyContext); \
  }
#define RELEASE_GIL()                                    \
  if (isThread) {                                        \
    RedisModule_ThreadSafeContextUnlock(RSDummyContext); \
  }

static void scanloop(Scanner *s, int isThread) {
begin:;
  scanCursor *c = &s->cursor;
  GET_GIL()
  uint64_t curRevision = SchemaRules_g->revision;
  c->cursor = RedisModule_ScanCursorCreate();
  RELEASE_GIL()
  while (s->state == SCAN_STATE_RUNNING && !c->isDone) {
    size_t nmax = c->n + SCAN_BATCH_SIZE;
    GET_GIL()
    while (c->n < nmax) {
      int rv = RedisModule_Scan(RSDummyContext, c->cursor, scanCallback, c);
      if (!rv) {
        c->isDone = 1;
        break;
      }
    }
    RELEASE_GIL()
    sched_yield();
  }

  RedisModule_ScanCursorDestroy(s->cursor.cursor);

  if (s->cursor.isDone) {
    // If the cursor is done, let's do the proper cleanup
    // but only in the main thread with the GIL locked; this way there
    // will be no surprises.
    GET_GIL()
    RedisModule_CreateTimer(RSDummyContext, 0, onInitDone, s);
    s->syncedRevision = curRevision;
    RELEASE_GIL()
  }
  pthread_mutex_lock(&statelock);

  if (s->state != SCAN_STATE_RESTART) {
    s->state = SCAN_STATE_STOPPED;
    pthread_mutex_unlock(&statelock);

  } else {
    initScanner(s);
    pthread_mutex_unlock(&statelock);
    goto begin;
  }
}

static void *scanThread(void *arg) {
  Scanner *s = arg;
  scanloop(s, 1);
  return NULL;
}

void SchemaRules_StartScan(int wait) {
  Scanner *s = &scanner_g;
  if (wait) {
    assert(s->state == SCAN_STATE_UNINIT || s->state == SCAN_STATE_STOPPED);
  }

  pthread_mutex_lock(&statelock);
  int doReturn = 0;
  if (s->state == SCAN_STATE_RUNNING) {
    // Thread has not yet exited
    s->state = SCAN_STATE_RESTART;
    doReturn = 1;
  }
  pthread_mutex_unlock(&statelock);
  if (doReturn) {
    return;
  }

  initScanner(&scanner_g);

  if (wait) {
    scanloop(s, 0);
  } else {
    pthread_t thr;
    pthread_create(&thr, NULL, scanThread, &scanner_g);
    pthread_detach(thr);
  }
}

void SchemaRules_ReplySyncInfo(RedisModuleCtx *ctx, IndexSpec *sp) {
  if (!(sp->flags & Index_UseRules)) {
    RedisModule_ReplyWithError(
        ctx, "This command can only be used on indexes created using `WITHRULES`");
    return;
  }

  RedisModule_ReplyWithArray(ctx, 2);
  if (scanner_g.state == SCAN_STATE_RUNNING) {
    RedisModule_ReplyWithSimpleString(ctx, "SCANNING");
    RedisModule_ReplyWithLongLong(ctx, LLONG_MAX);
  } else if (scanner_g.state == SCAN_STATE_STOPPED || scanner_g.state == SCAN_STATE_UNINIT) {
    ssize_t ret = SchemaRules_GetPendingCount(sp);
    if (ret) {
      RedisModule_ReplyWithSimpleString(ctx, "INDEXING");
      RedisModule_ReplyWithLongLong(ctx, ret);
    } else {
      RedisModule_ReplyWithSimpleString(ctx, "SYNCED");
      RedisModule_ReplyWithLongLong(ctx, 0);
    }
  } else if (scanner_g.state == SCAN_STATE_RESTART) {
    RedisModule_ReplyWithSimpleString(ctx, "Restarting");
    RedisModule_ReplyWithLongLong(ctx, LLONG_MAX);
  } else {
    abort();  // bad state
  }
}
