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
  SCAN_STATE_CANCELLED = 3,  // lb
} ScanState;

typedef struct {
  size_t n;
  int isDone;
  int mode;
  union {
    RedisModuleScanCursor *r6;
    unsigned long r5;
  } cursor;
} scanCursor;

typedef struct {
  ScanState state;
  pthread_t thr;
  scanCursor cursor;
  uint64_t rulesRevision;
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

static void scanRedis6(scanCursor *c) {
  size_t nmax = c->n + SCAN_BATCH_SIZE;
  if (!c->cursor.r6) {
    c->cursor.r6 = RedisModule_ScanCursorCreate();
  }
  while (c->n < nmax) {
    int rv = RedisModule_Scan(RSDummyContext, c->cursor.r6, scanCallback, c);
    if (!rv) {
      c->isDone = 1;
      break;
    }
  }
}

static void scanRedis5(scanCursor *c) {
  char cursorbuf[1024] = {};
  RedisModuleCtx *ctx = RSDummyContext;
  size_t nmax = c->n += SCAN_BATCH_SIZE;

  do {

    sprintf(cursorbuf, "%lu", c->cursor.r5);
    // RedisModuleCallReply *r =
    //     RedisModule_Call(ctx, "SCAN", "lcccl", cursor, "TYPE", "hash", "COUNT", 100);
    // printf("cursor: %s\n", cursorbuf);
    RedisModuleCallReply *r = RedisModule_Call(ctx, "SCAN", "c", cursorbuf);

    assert(r != NULL);
    assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ARRAY);

    if (RedisModule_CallReplyLength(r) < 2) {
      c->isDone = 1;
      RedisModule_FreeCallReply(r);
      break;
    }

    // cursor is the first element. ew.
    size_t ncursor = 0;
    const char *newcur =
        RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(r, 0), &ncursor);
    memcpy(cursorbuf, newcur, ncursor);
    cursorbuf[ncursor] = 0;
    sscanf(cursorbuf, "%lu", &c->cursor.r5);

    RedisModuleCallReply *keys = RedisModule_CallReplyArrayElement(r, 1);
    assert(RedisModule_CallReplyType(keys) == REDISMODULE_REPLY_ARRAY);
    size_t nelem = RedisModule_CallReplyLength(keys);

    for (size_t ii = 0; ii < nelem; ++ii) {
      size_t len;
      const char *kcstr =
          RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(keys, ii), &len);
      RuleKeyItem rki = {.kstr = RedisModule_CreateString(NULL, kcstr, len)};
      SchemaRules_ProcessItem(ctx, &rki, RULES_PROCESS_F_NOREINDEX | RULES_PROCESS_F_ASYNC);
      RedisModule_FreeString(ctx, rki.kstr);
    }

    RedisModule_FreeCallReply(r);
    c->n += nelem;
  } while (c->cursor.r5 != 0 && c->n < nmax);
}

static void *scanThread(void *arg) {
  Scanner *s = arg;
  // Initialize the cursor
  if (RedisModule_Scan) {
    s->cursor.mode = SCAN_MODE_R6;
  } else {
    s->cursor.mode = SCAN_MODE_R5;
  }

  while (s->state != SCAN_STATE_CANCELLED && !s->cursor.isDone) {
    RedisModule_ThreadSafeContextLock(RSDummyContext);
    if (s->cursor.mode == SCAN_MODE_R6) {
      scanRedis6(&s->cursor);
    } else {
      scanRedis5(&s->cursor);
    }
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    sched_yield();
  }

  if (s->cursor.mode == SCAN_MODE_R6 && s->cursor.cursor.r6) {
    RedisModule_ScanCursorDestroy(s->cursor.cursor.r6);
  }
  if (s->state != SCAN_STATE_CANCELLED) {
    s->state = SCAN_STATE_STOPPED;
  }
  return NULL;
}

void SchemaRules_StartScan(void) {
  // note: we assume this is called only once from a single thread!
  ScanState oldstate =
      __sync_val_compare_and_swap(&scanner_g.state, SCAN_STATE_RUNNING, SCAN_STATE_CANCELLED);
  if (oldstate == SCAN_STATE_RUNNING) {
    pthread_join(scanner_g.thr, NULL);
  }

  memset(&scanner_g, 0, sizeof(scanner_g));
  scanner_g.state = SCAN_STATE_RUNNING;
  scanner_g.rulesRevision = SchemaRules_g->revision;
  pthread_create(&scanner_g.thr, NULL, scanThread, &scanner_g);
}

uint64_t SchemaRules_ScanRevision(void) {
  ScanState state = scanner_g.state;
  if (state == SCAN_STATE_STOPPED || state == SCAN_STATE_UNINIT) {
    return scanner_g.rulesRevision;
  } else {
    return scanner_g.rulesRevision - 1;
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
  } else if (scanner_g.state == SCAN_STATE_CANCELLED) {
    RedisModule_ReplyWithSimpleString(ctx, "CANCELLED");
    RedisModule_ReplyWithLongLong(ctx, LLONG_MAX);
  } else {
    abort();  // bad state
  }
}
