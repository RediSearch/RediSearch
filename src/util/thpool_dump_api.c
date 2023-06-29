/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdatomic.h>
#include <execinfo.h>
#include <time.h>

#include "thpool_dump_api.h"

#include "util/workers.h"
#include "gc.h"
#include "spec.h"
#include "concurrent_ctx.h"
/* ========================== STRUCTURES ============================ */

typedef struct {
  int trace_size;                   /* number of address in the backtrace */
  statusOnCrash status_on_crash;    /* thread's status when the crash happened */
  char **printable_bt;              /* backtrace symbols */
} thread_bt_data;

/* ============================ GLOBALS ============================== */

// Maximum number of addresses to backtrace.
#define BT_BUF_SIZE 100

// Dump container.
static thread_bt_data *printable_bt_buffer = NULL;

// Save the curr backtrace buffer size (in terms of thread_bt_data struct)
// to check if we need to increase the dump container size
static volatile size_t g_curr_bt_buffer_size = 0;

// An atomic counter of the number of threads done logging their backtrace.
static atomic_size_t g_threads_done_writing = 0;

// This flag should be set if we are trying to get the backtrace
// during run time (e.g with FT.DEBUG DUMP_THREADPOOL_BACKTRACE) to return immediatly
// from crash report callbacks trying to get the backtrace.
static volatile int g_collecting_state_in_progress = 0;

/* ========================== HELPER FUNCTIONS ============================ */

static void wait_for_writing(redisearch_threadpool thpool_p, size_t threads_to_wait_cnt);

// ThpoolDump_log_to_info() wraps these functions:

// Initialize all the data structures required to log the state of each thread in the threadpool
// and mark the the threads they can start writing to them.
static void redisearch_thpool_StateLog_init(redisearch_threadpool);
// Print the data collected by the threads to the crash report.
static void redisearch_thpool_StateLog_log_to_RSinfo(redisearch_threadpool,
                                             RedisModuleInfoCtx *ctx);
// Reply with the data collected by the threads.
static void redisearch_thpool_StateLog_RSreply(redisearch_threadpool,
                                                      RedisModule_Reply *reply);
// Cleanups related to a specific threadpool dump
static void redisearch_thpool_StateLog_cleanup(redisearch_threadpool);


bool ThpoolDump_test_and_start() {
  // Mark the **process** as in "collecting state" mode

  // __atomic_test_and_set sets the bool to true and returns true if g_collecting_state_in_progress was already true.
  // In this case we can't start collecting the data.
  bool is_in_progress = __atomic_test_and_set(&g_collecting_state_in_progress, __ATOMIC_RELAXED);

  // If __atomic_test_and_set returned false we can now proceed with collecting data from the threadpools.
  return !is_in_progress;
}

static void thread_bt_buffer_init(uint32_t thread_id, void *bt_addresses_buf, int trace_size, statusOnCrash status_on_crash) {
    // NOTE: backtrace_symbols() returns a pointer to the array malloced by the call
    // and should be freed by us.
    printable_bt_buffer[thread_id].printable_bt = backtrace_symbols(bt_addresses_buf, trace_size);
    printable_bt_buffer[thread_id].trace_size = trace_size;
    printable_bt_buffer[thread_id].status_on_crash = status_on_crash;
}

void ThpoolDump_log_backtrace(statusOnCrash status_on_crash, size_t thread_id) {
  void *bt_addresses_buf[BT_BUF_SIZE];
  // Get the stacktrace addresses first.
  int trace_size = backtrace(bt_addresses_buf, BT_BUF_SIZE);

  // Translate addresses into symbols and write them to the backtraces array.
  thread_bt_buffer_init(thread_id, bt_addresses_buf, trace_size, status_on_crash);

  // Atomically increase finished threads count.
  ++g_threads_done_writing;
}

static void wait_for_writing(redisearch_threadpool thpool_p, size_t threads_to_wait_cnt) {

  // when g_threads_done_writing == threads_to_wait_cnt all the threads marked that they have done
  // writing their backtrace to the buffer.

  clock_t start = clock();
  size_t threads_done = g_threads_done_writing;
  while (threads_done != threads_to_wait_cnt) {
    int waiting_time = (clock() - start)/CLOCKS_PER_SEC;
    if (waiting_time && waiting_time % LOG_WAITING_TIME_INTERVAL == 0 ) {
      RedisModule_Log(NULL, "warning",
                      "%s threadpool:something is wrong: expected %lu threads to finish, but only %lu are done. "
                      "continue waiting",
                      redisearch_thpool_get_name(thpool_p), threads_to_wait_cnt, threads_done);
    }
    threads_done = g_threads_done_writing;
  }
}

/* ======================== THREADS STATE LOG HELPERS ========================= */
void ThpoolDump_log_to_reply(redisearch_threadpool thpool_p,
                                       RedisModule_Reply *reply) {
  if (!thpool_p) {
    return;
  }

  if (!(redisearch_thpool_ISSET_flag(thpool_p, RS_THPOOL_F_PAUSE))) {
    RedisModule_Log(
        NULL, "warning",
        "%s threadpool: ThpoolDump_log_to_reply(): the threadpool must be paused to print backtrace",
        redisearch_thpool_get_name(thpool_p));
    return;
  }

  // Save all threads data
  redisearch_thpool_StateLog_init(thpool_p);

  // Print the backtrace of each thread
  redisearch_thpool_StateLog_RSreply(thpool_p, reply);

  // cleanup
  redisearch_thpool_StateLog_cleanup(thpool_p);
}


void ThpoolDump_log_to_info(redisearch_threadpool thpool_p, RedisModuleInfoCtx *ctx) {
  if (!thpool_p) {
    return;
  }

  if (!(redisearch_thpool_ISSET_flag(thpool_p, RS_THPOOL_F_PAUSE))) {
    RedisModule_Log(
        NULL, "warning",
        "%s threadpool: ThpoolDump_log_to_info(): the threadpool must be paused to dump state log",
        redisearch_thpool_get_name(thpool_p));
    return;
  }

  // Save all threads data
  redisearch_thpool_StateLog_init(thpool_p);

  // Print the backtrace of each thread
  redisearch_thpool_StateLog_log_to_RSinfo(thpool_p, ctx);

  // cleanup
  redisearch_thpool_StateLog_cleanup(thpool_p);
}

void ThpoolDump_done() {
  // release the backtraces buffer
  rm_free(printable_bt_buffer);
  g_curr_bt_buffer_size = 0;
  printable_bt_buffer = NULL;

  // Turn off the flag to indicate that the data collection process is done
  __atomic_clear(&g_collecting_state_in_progress, __ATOMIC_RELAXED);
}

void ThpoolDump_pause(redisearch_threadpool thpool_p) {

  if (!thpool_p) {
    return;
  }

  // set thpool signal handler mode to collect the current threads' state.
  redisearch_thpool_TURNON_flag(thpool_p, RS_THPOOL_F_COLLECT_STATE_INFO);

  // The threads should wait until we initialize data structures and flags used to log
  // the dump info.
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);

  // Raise a signal to all the threads to check the flags above.
  redisearch_thpool_pause(thpool_p);
}


/* Initialize all data structures required to log the backtrace of each thread in the threadpool
 * and pause the threads.
 */
static void redisearch_thpool_StateLog_init(redisearch_threadpool thpool_p) {

  size_t threadpool_size = redisearch_thpool_num_threads_alive_unsafe(thpool_p);

  // realloc backtraces buffer array if needed
  if(threadpool_size > g_curr_bt_buffer_size) {
    printable_bt_buffer = rm_realloc(printable_bt_buffer, threadpool_size * sizeof(thread_bt_data));
    g_curr_bt_buffer_size = threadpool_size;
  }

  if (printable_bt_buffer == NULL) {
    RedisModule_Log(NULL, "warning",
                    "%s threadpool: can't realloc printable_bt_buffer, returning with no dump.",
                    redisearch_thpool_get_name(thpool_p));
  }

  // Initialize the finished threads counter
  g_threads_done_writing = 0;

  // All the data structures are ready, "signal" all the threads that they can start writing.
  redisearch_thpool_TURNON_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);

  // If handling the crash is called by one of the threads in the thpool it won't be signaled
  // so we need to get its backtrace here.
  if (redisearch_thpool_ISSET_flag(thpool_p, RS_THPOOL_F_CONTAINS_HANDLING_THREAD)) {
    // The calling thread is always the last
    size_t thread_id = threadpool_size - 1;
    ThpoolDump_log_backtrace(CRASHED, thread_id);
  }
}

/* Prints the log for each thread to the crash log file*/
static void redisearch_thpool_StateLog_log_to_RSinfo(redisearch_threadpool thpool_p,
                                                     RedisModuleInfoCtx *ctx) {

  size_t threadpool_size = redisearch_thpool_num_threads_alive_unsafe(thpool_p);

  wait_for_writing(thpool_p, threadpool_size);

  char info_section_title[100];
  sprintf(info_section_title, "=== %s THREADS LOG: ===", redisearch_thpool_get_name(thpool_p));
  RedisModule_InfoAddSection(ctx, info_section_title);

  // for each thread in g_threads_done_cnt
  for(size_t i = 0; i < g_threads_done_writing; i++) {
    thread_bt_data curr_bt = printable_bt_buffer[i];
    char buff[100];
    if(curr_bt.status_on_crash == CRASHED) {
      sprintf(buff, "CRASHED thread #%lu backtrace: \n",i);
    } else {
      sprintf(buff, "thread #%lu backtrace: \n",i);
    }
    RedisModule_InfoAddSection(ctx, buff);

    // print the backtrace
    for(int j = 0; j < curr_bt.trace_size; j++) {
      sprintf(buff, "%d",j);
      RedisModule_InfoAddFieldCString(ctx, buff, curr_bt.printable_bt[j]);
    }

    // clean up inner backtrace strings array malloc'd by backtrace_symbols()
    free(curr_bt.printable_bt);
  }
}

static void redisearch_thpool_StateLog_cleanup(redisearch_threadpool thpool_p) {
  // clear counters and turn off flags.
  g_threads_done_writing = 0;
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_CONTAINS_HANDLING_THREAD);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_COLLECT_STATE_INFO);
}

static void redisearch_thpool_StateLog_RSreply(redisearch_threadpool thpool_p,
                                                      RedisModule_Reply *reply) {
  size_t threadpool_size = redisearch_thpool_num_threads_alive_unsafe(thpool_p);

  // Print the back trace of each thread
  wait_for_writing(thpool_p, threadpool_size);

  char thpool_title[100];
  sprintf(thpool_title, "=== %s THREADS BACKTRACE: ===", redisearch_thpool_get_name(thpool_p));

  RedisModule_ReplyKV_Map(reply, thpool_title); // >threads dict
  // for each thread in threads_ids
  for(size_t i = 0; i < g_threads_done_writing; i++) {
    thread_bt_data curr_bt = printable_bt_buffer[i];
    char buff[100];
    sprintf(buff, "thread #%lu backtrace:",i);
    RedisModule_ReplyKV_Array(reply, buff); // >>Thread's backtrace

    // print the backtrace
    for(int j = 0; j < curr_bt.trace_size; j++) {
      RedisModule_Reply_SimpleString(reply, curr_bt.printable_bt[j]); // >>> backtrace line

    }
    RedisModule_Reply_ArrayEnd(reply); // >>Thread's backtrace

    // clean up inner backtrace strings array malloc'd by backtrace_symbols()
    free(curr_bt.printable_bt);
  }
  RedisModule_Reply_MapEnd(reply); // >threads dict
}

/* ============================ REDISEARCH THPOOLS ============================== */

void RS_ThreadpoolsPauseBeforeDump() {
#ifdef MT_BUILD
  workersThreadPool_PauseBeforeDump();
#endif // MT_BUILD
  CleanPool_ThreadPoolPauseBeforeDump();
  ConcurrentSearch_PauseBeforeDump();
  GC_ThreadPoolPauseBeforeDump();
}

void RS_ThreadpoolsResume() {
#ifdef MT_BUILD
  workersThreadPool_Resume();
#endif // MT_BUILD
  CleanPool_ThreadPoolResume();
  ConcurrentSearch_Resume();
  GC_ThreadPoolResume();
}
