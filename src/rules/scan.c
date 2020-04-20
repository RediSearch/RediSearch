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
  int mode;
  union {
    RedisModuleScanCursor *r6;
    unsigned long r5;
  } cursor;
} scanCursor;

typedef struct {
  ScanState state;
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
  // char cursorbuf[1024] = {'0', 0};
  RedisModuleCtx *ctx = RSDummyContext;
  size_t nmax = c->n + SCAN_BATCH_SIZE;

  RedisModule_Log(NULL, "notice", "Start scanning for keys");

  RedisModuleCallReply *info = RedisModule_Call(ctx, "info", "c", "KEYSPACE");

  size_t len;
  const char* infoRes = RedisModule_CallReplyStringPtr(info, &len);
  RedisModule_Log(NULL, "warning", "info: %.*s", len, infoRes);

  do {
    RedisModuleCallReply *r = RedisModule_Call(ctx, "SCAN", "l", c->cursor.r5);
    // printf("cursor: %s\n", cursorbuf);
    // RedisModuleCallReply *r = NULL;
    // r = RedisModule_Call(ctx, "SCAN", "c", cursorbuf);
    // if (RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING ||
    //     RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR) {
    //   printf("Got non-aerray in reply: %s\n", RedisModule_CallReplyStringPtr(r, NULL));
    //   c->isDone = 1;
    // }
    if (r == NULL || RedisModule_CallReplyLength(r) < 2) {
      RedisModule_Log(NULL, "warning", "Failed Scan");
      if(r == NULL){
        RedisModule_Log(NULL, "warning", "Got a NULL reply");
      }
      else if(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING || RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char* res = RedisModule_CallReplyStringPtr(r, &len);
        RedisModule_Log(NULL, "warning", "Scan reply: %.*s", len, res);
      }
      c->isDone = 1;
      if (r) {
        RedisModule_FreeCallReply(r);
      }
      break;
    }

    size_t ncursor = 0;
    char buf[1024] = {0};
    const char *newcur =
        RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(r, 0), &ncursor);
    memcpy(buf, newcur, ncursor);
    // Read from the numeric reply into the cursor buffer
    sscanf(buf, "%lu", &c->cursor.r5);

    RedisModuleCallReply *keys = RedisModule_CallReplyArrayElement(r, 1);
    assert(RedisModule_CallReplyType(keys) == REDISMODULE_REPLY_ARRAY);
    size_t nelem = RedisModule_CallReplyLength(keys);

    RedisModule_Log(NULL, "notice", "Found %d elements in scan", nelem);

    for (size_t ii = 0; ii < nelem; ++ii) {
      size_t len;
      const char *kcstr =
          RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(keys, ii), &len);
      RedisModule_Log(NULL, "debug", "Found key %s", kcstr);
      RuleKeyItem rki = {.kstr = RedisModule_CreateString(NULL, kcstr, len)};
      SchemaRules_ProcessItem(ctx, &rki, RULES_PROCESS_F_NOREINDEX | RULES_PROCESS_F_ASYNC);
      RedisModule_FreeString(ctx, rki.kstr);
    }

    RedisModule_FreeCallReply(r);
    c->n += nelem;
  } while (c->cursor.r5 != 0 && c->n < nmax);

  if (c->cursor.r5 == 0) {
    c->isDone = 1;
  }
}

static void initScanner(Scanner *s) {
  memset(s, 0, sizeof(*s));
  s->state = SCAN_STATE_RUNNING;
  s->rulesRevision = SchemaRules_g->revision;
}

static pthread_mutex_t statelock = PTHREAD_MUTEX_INITIALIZER;

static void scanloop(Scanner *s, int isThread) {
begin:
  // Initialize the cursor
  // if (RedisModule_Scan) {
  if (0) {
    s->cursor.mode = SCAN_MODE_R6;
  } else {
    s->cursor.mode = SCAN_MODE_R5;
  }
  while (s->state == SCAN_STATE_RUNNING && !s->cursor.isDone) {
    if (isThread) {
      RedisModule_ThreadSafeContextLock(RSDummyContext);
    }
    if (s->cursor.mode == SCAN_MODE_R6) {
      scanRedis6(&s->cursor);
    } else {
      scanRedis5(&s->cursor);
    }
    if (isThread) {
      RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    }
    sched_yield();
  }

  if (s->cursor.mode == SCAN_MODE_R6 && s->cursor.cursor.r6) {
    RedisModule_ScanCursorDestroy(s->cursor.cursor.r6);
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
  } else if (scanner_g.state == SCAN_STATE_RESTART) {
    RedisModule_ReplyWithSimpleString(ctx, "Restarting");
    RedisModule_ReplyWithLongLong(ctx, LLONG_MAX);
  } else {
    abort();  // bad state
  }
}
