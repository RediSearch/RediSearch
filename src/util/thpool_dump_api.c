/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdatomic.h>
#include <execinfo.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/prctl.h>
#include <stdlib.h>


#include "thpool_dump_api.h"
#include "util/arr.h"
#include "util/proc_file.h"
#include "rmutil/rm_assert.h"

#include "util/workers.h"
#include "gc.h"
#include "spec.h"
#include "concurrent_ctx.h"
/* ========================== STRUCTURES ============================ */

typedef struct {
  int trace_size;                   /* number of address in the backtrace */
  statusOnCrash status_on_crash;    /* thread's status when the crash happened */
  char **printable_bt;              /* backtrace symbols */

#if defined(__linux__) || (defined(__APPLE__) && defined(__MACH__))
  char thread_name[16]; /* The name of the thread as assigned when created.
                            Not supported on all platforms */
#endif
} thread_bt_data;

/* ============================ GLOBALS ============================== */

// Maximum number of addresses to backtrace.
#define BT_BUF_SIZE 100

// Dump container.
static thread_bt_data *printable_bt_buffer = NULL;

// An atomic counter of the number of threads done logging their backtrace.
static atomic_size_t g_threads_done_writing = 0;

// This flag should be set if we are trying to get the backtrace
// during run time (e.g with FT.DEBUG DUMP_THREADPOOL_BACKTRACE) to return immediatly
// from crash report callbacks trying to get the backtrace.
static volatile int g_collecting_state_in_progress = 0;

static volatile bool g_all_ready_to_dump = 1;

static volatile bool g_collect_all_mode = 0;

/* ========================== HELPERS FUNCTIONS ============================ */

/* ============== HELPERS FOR GENERAL DUMP ================ */

static void wait_for_writing(size_t threads_to_wait_cnt, const char *error_log_title);

static void ThpoolDump_all_prepare();

// Cleanups after logging is done.
static void ThpoolDump_all_done();
/* ============== HELPERS FOR THPOOLS ================ */

// Prepare the threadpool for dump report and pause it.
static void ThpoolDump_pause(redisearch_threadpool);

// Initialize all the data structures required to log the state of each thread in the threadpool
// and mark the the threads they can start writing to them.
static void ThpoolDump_init(redisearch_threadpool);

// Print the data collected by the threads to the crash report.
static void ThpoolDump_wait_and_reply(redisearch_threadpool thpool_p,
                                                      RedisModule_Reply *reply);

static void ThpoolDump_log_to_reply(RedisModule_Reply *reply);

// Cleanups related to a specific threadpool dump
static void ThpoolDump_thpool_cleanups(redisearch_threadpool);

// General cleanups after all the threadpools are done dumping their state data.
static void ThpoolDump_done();

/* ========================== API ============================ */

bool ThpoolDump_test_and_start() {
  // Mark the **process** as in "collecting state" mode

  // __atomic_test_and_set sets the bool to true and returns true if g_collecting_state_in_progress was already true.
  // In this case we can't start collecting the data.
  bool is_in_progress = __atomic_test_and_set(&g_collecting_state_in_progress, __ATOMIC_RELAXED);

  // If __atomic_test_and_set returned false we can now proceed with collecting data from the threadpools.
  return !is_in_progress;
}

void ThpoolDump_finish() {
  __atomic_clear(&g_collecting_state_in_progress, __ATOMIC_RELAXED);
}

static void thread_bt_buffer_init(uint32_t thread_id, void *bt_addresses_buf, int trace_size, statusOnCrash status_on_crash) {
  // NOTE: backtrace_symbols() returns a pointer to the array malloced by the call
  // and should be freed by us.
  printable_bt_buffer[thread_id].printable_bt = backtrace_symbols(bt_addresses_buf, trace_size);
  printable_bt_buffer[thread_id].trace_size = trace_size;
  printable_bt_buffer[thread_id].status_on_crash = status_on_crash;
#if defined(__linux__)
  prctl(PR_GET_NAME, printable_bt_buffer[thread_id].thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
	pthread_t caller = pthread_self();
  pthread_getname_np(caller, printable_bt_buffer[thread_id].thread_name);
#endif
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

static void wait_for_writing(size_t threads_to_wait_cnt, const char *error_log_title) {

  // when g_threads_done_writing == threads_to_wait_cnt all the threads marked that they have done
  // writing their backtrace to the buffer.

  clock_t start = clock();
  size_t threads_done = g_threads_done_writing;
  while (threads_done != threads_to_wait_cnt) {
    RS_LOG_ASSERT_FMT(
        (clock() - start) / CLOCKS_PER_SEC < WAIT_FOR_THPOOL_TIMEOUT,
        "%s: expected %lu threads to finish, but only %lu are done. ", error_log_title,
        threads_to_wait_cnt, threads_done);
    threads_done = g_threads_done_writing;
  }
}

int ThpoolDump_collect_and_log_to_reply(redisearch_threadpool thpool_p,
                                       RedisModule_Reply *reply) {
  if (!thpool_p) {
    return REDISMODULE_ERR;
  }

  ThpoolDump_pause(thpool_p);

  // Save all threads data
  ThpoolDump_init(thpool_p);

  // Print the backtrace of each thread
  ThpoolDump_wait_and_reply(thpool_p, reply);

  // cleanup
  ThpoolDump_thpool_cleanups(thpool_p);

  // resume
  redisearch_thpool_resume(thpool_p);

  ThpoolDump_done();

  return REDISMODULE_OK;
}

/* ======================== HELPERS ========================= */
static void ThpoolDump_done() {
  // release the backtraces buffer
  rm_free(printable_bt_buffer);
  printable_bt_buffer = NULL;

  g_threads_done_writing = 0;

  // Turn off the flag to indicate that the data collection process is done
  ThpoolDump_finish();
}

static void ThpoolDump_pause(redisearch_threadpool thpool_p) {

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
static void ThpoolDump_init(redisearch_threadpool thpool_p) {

  size_t threadpool_size = redisearch_thpool_num_threads_alive_unsafe(thpool_p);

  // alloc backtraces buffer array
  printable_bt_buffer = rm_calloc(threadpool_size, sizeof(thread_bt_data));

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

static void ThpoolDump_thpool_cleanups(redisearch_threadpool thpool_p) {
  // clear counters and turn off flags.
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_CONTAINS_HANDLING_THREAD);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_COLLECT_STATE_INFO);
}

static void ThpoolDump_wait_and_reply(redisearch_threadpool thpool_p,
                                                      RedisModule_Reply *reply) {
  size_t threadpool_size = redisearch_thpool_num_threads_alive_unsafe(thpool_p);

  // Print the back trace of each thread
  wait_for_writing(threadpool_size, redisearch_thpool_get_name(thpool_p));

  ThpoolDump_log_to_reply(reply);

}

static void ThpoolDump_log_to_reply(RedisModule_Reply *reply) {
  RedisModule_Reply_Map(reply); // >threads dict
  // for each thread in threads_ids
  for(size_t i = 0; i < g_threads_done_writing; i++) {
    thread_bt_data curr_bt = printable_bt_buffer[i];
#if defined(__linux__) || (defined(__APPLE__) && defined(__MACH__))
    char *name = curr_bt.thread_name;
#else
    // generate a title
    char name[16];
    sprintf(name, "thread-%lu",i);
#endif

    RedisModule_ReplyKV_Array(reply, name); // >>Thread's backtrace

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
/* ============================ DUMP ALL API ============================== */

bool ThpoolDump_all_ready() {
  return g_all_ready_to_dump;
}

bool ThpoolDump_collect_all_mode() {
  return g_collect_all_mode;
}

#if defined(__linux__)
int ThpoolDump_all_to_reply(RedisModule_Reply *reply) {
  ThpoolDump_all_prepare();

  ThpoolDump_log_to_reply(reply);

  ThpoolDump_all_done();

  return REDISMODULE_OK;
}

static void ThpoolDump_all_prepare() {

  g_collect_all_mode = 1;
  g_all_ready_to_dump = 0;

  size_t threads_to_collect = pause_all_process_threads() + 1;

  // Pause all threads. We need to collect data from all the paused threads + the current thread
  // (can be either the main thread or a background thread)

  printable_bt_buffer = rm_calloc(threads_to_collect, sizeof(thread_bt_data));

  // let them write to the buffer, each will have a unique index
  g_all_ready_to_dump = 1;

  // In the meantime we can write to the buffer as well...
  // The caller gets the last thread_id.
  ThpoolDump_log_backtrace(CRASHED, threads_to_collect - 1);

  // wait for all the threads, including the caller, to finish
  wait_for_writing(threads_to_collect, "Prepare all process' threads");

}

void ThpoolDump_all_to_info(RedisModuleInfoCtx *ctx) {

  ThpoolDump_all_prepare();

  // print the buffer
  RedisModule_InfoAddSection(ctx, "=== THREADS LOG: ===");

  // for each thread in g_threads_done_cnt
  for(size_t i = 0; i < g_threads_done_writing; i++) {
    thread_bt_data curr_bt = printable_bt_buffer[i];
    char *name = curr_bt.thread_name;
    char buff[25] = {0};
    if(curr_bt.status_on_crash == CRASHED) {
      sprintf(buff, "CRASHED_");
    }
    strcat(buff, name);
    RedisModule_InfoAddSection(ctx, buff);
    // print the backtrace
    for(int j = 0; j < curr_bt.trace_size; j++) {
      sprintf(buff, "%d", j);
      RedisModule_InfoAddFieldCString(ctx, buff, curr_bt.printable_bt[j]);
    }

    // clean up inner backtrace strings array malloc'd by backtrace_symbols()
    free(curr_bt.printable_bt);
  }

  ThpoolDump_all_done();

}

static void ThpoolDump_all_done() {
  // reset global variables
  g_collect_all_mode = 0;

  //resume all the threads.
  resume_all_process_threads();

  ThpoolDump_done();
}
#endif // defined(__linux__)
