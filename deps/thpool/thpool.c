/* ********************************
 * Author:       Johan Hanssen Seferidis
 * License:	     MIT
 * Description:  Library providing a threading pool where you can add
 *               work. For usage, check the thpool.h file or README.md
 *
 */
/** @file thpool.h */ /*
                       *
                       ********************************/

//#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <stdatomic.h>
#include <execinfo.h>

#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include "thpool.h"
#include "rmalloc.h"

#ifdef THPOOL_DEBUG
#define THPOOL_DEBUG 1
#else
#define THPOOL_DEBUG 0
#endif

// Maximum number of addresses to backtrace.
#define BT_BUF_SIZE 100

// Time (in seconds) to print we are waiting for some sync operation too long.
#define LOG_WAITING_TIME_INTERVAL 3


#if !defined(DISABLE_PRINT) || defined(THPOOL_DEBUG)
#define err(str, ...) fprintf(stderr, str, ##__VA_ARGS__)
#else
#define err(str, ...)
#endif

// Save the current threadpool.
typedef struct redisearch_thpool_t redisearch_thpool_t;
redisearch_thpool_t *g_curr_threadpool = NULL;

// Save the curr backtrace buffer size (in terms of thread_bt_data struct)
// to check if we need to increase the dump container size
static volatile size_t g_curr_bt_buffer_size = 0;
// The number of threads in the current threadpool that are paused.
static atomic_size_t g_threads_paused_cnt = 0;
// The number of threads in the threadpool that done writing their current state to the dump container.
static atomic_size_t g_threads_done_cnt = 0;

// This flag should be set if we are trying to get the backtrace
// during run time (e.g with FT.DEBUG DUMP_THREADPOOL_BACKTRACE) to return immediatly
// from crash report callbacks to get the backtrace.
static volatile int g_get_backtrace_mode = 0;

// Dump container.
typedef struct thread_bt_data thread_bt_data;
static thread_bt_data *printable_bt_buffer = NULL;

/* ========================== STRUCTURES ============================ */

/* Binary semaphore */
typedef struct bsem {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int v;
} bsem;

/* Job */
typedef struct job {
  struct job* prev;            /* pointer to previous job   */
  void (*function)(void* arg); /* function pointer          */
  void* arg;                   /* function's argument       */
} job;

/* Job queue */
typedef struct jobqueue {
  job* front;              /* pointer to front of queue */
  job* rear;               /* pointer to rear  of queue */
  int len;                 /* number of jobs in queue   */
} jobqueue;

typedef struct priority_queue {
  jobqueue high_priority_jobqueue;    /* job queue for high priority tasks */
  jobqueue low_priority_jobqueue;     /* job queue for low priority tasks */
  pthread_mutex_t jobqueues_rwmutex;  /* used for queue r/w access */
  bsem* has_jobs;                     /* flag as binary semaphore  */
  unsigned char pulls;                /* number of pulls from queue */
} priority_queue;

/* Thread */
typedef struct thread {
  int id;                   /* friendly id               */
  pthread_t pthread;        /* pointer to actual thread  */
  struct redisearch_thpool_t* thpool_p; /* access to thpool          */
} thread;

/* On crash backtrace report */
typedef enum {
  FINE,
  CRASHED,
} statusOnCrash;
struct thread_bt_data{
  int trace_size;                   /* number of address in the backtrace */
  statusOnCrash status_on_crash;    /* thead's status when the crash happened */
  char **printable_bt;              /* back trace symbols */
};

/* Threadpool flags */
#define RS_THPOOL_F_KEEP_ALIVE 0x01 /* keep pool alive */

#define RS_THPOOL_F_TERMINATE_WHEN_EMPTY 0x02  /* terminate thread when there are
                                                      no more pending jobs  */
#define RS_THPOOL_F_READY_TO_DUMP 0x04 /* turn on to signal the threads
                                       they can write to the dump container */
#define RS_THPOOL_F_CONTAINS_HANDLING_THREAD 0x08  /* the thread to start dump report is in
                                                      this threadpool  */
#define RS_THPOOL_F_RESUME 0x10  /* Signal the threads to resume.
                                RS_THPOOL_F_PAUSE flag is turned off only when all the
                                threads resume */
#define RS_THPOOL_F_PAUSE 0x20  /* All the threads in the threadpool are paused.  */
#define RS_THPOOL_F_COLLECT_STATE_INFO 0x40  /* The threadpool is in 'collecting current threads' state' mode  */

/* Threadpool */
struct redisearch_thpool_t {
  thread** threads;                 /* pointer to threads        */
  size_t total_threads_count;
  volatile size_t num_threads_alive;   /* threads currently alive   */
  volatile size_t num_threads_working; /* threads currently working */
  pthread_mutex_t thcount_lock;     /* used for thread count etc */
  pthread_cond_t threads_all_idle;  /* signal to thpool_wait     */
  priority_queue jobqueue;          /* job queue                 */
  volatile uint16_t flags;                   /* threadpool flags */
  char *name;                 /* threadpool name */
};

/* ========================== PROTOTYPES ============================ */

static int thread_init(redisearch_thpool_t* thpool_p, struct thread** thread_p, int id);
static void* thread_do(struct thread* thread_p);
static void thread_hold(int sig_id);
static void thread_destroy(struct thread* thread_p);

// If collect_bt == 0, this function does nothing.
static void get_backtrace(statusOnCrash status_on_crash, int collect_bt, size_t thread_id);
static void thread_bt_buffer_init(uint32_t thread_id, void *bt_addresses_buf, int trace_size, statusOnCrash status_on_crash);
static void reset_global_vars();
// wait g_threads_done_cnt to reach threads_to_wait_cnt threads.
static void wait_for_threads(redisearch_thpool_t* thpool_p, size_t threads_to_wait_cnt);

// redisearch_thpool_StateLog() wraps these functions:

// Initialize all the data structures required to log the state of each thread in the threadpool
// and mark the the threads they can start writing to them.
static void redisearch_thpool_StateLog_init(redisearch_threadpool);
// Print the data collected by the threads to the crash report.
static void redisearch_thpool_StateLog_log_to_RSinfo(redisearch_thpool_t* thpool_p,
                                             RedisModuleInfoCtx *ctx);
// Reply with the data collected by the threads.
static void redisearch_thpool_StateLog_RSreply(redisearch_thpool_t* thpool_p,
                                                      RedisModule_Reply *reply);
// Cleanups related to a specific threadpool dump
static void redisearch_thpool_StateLog_cleanup(redisearch_threadpool);

// turn off threadpool flag
static void redisearch_thpool_TURNOFF_flag(redisearch_thpool_t* thpool_p, uint16_t flag) {
  thpool_p->flags &= ~flag;
}


static int jobqueue_init(jobqueue* jobqueue_p);
static void jobqueue_clear(jobqueue* jobqueue_p);
static void jobqueue_push_chain(jobqueue* jobqueue_p, struct job* first_newjob, struct job* last_newjob, size_t num);
static struct job* jobqueue_pull(jobqueue* jobqueue_p);
static void jobqueue_destroy(jobqueue* jobqueue_p);

static int priority_queue_init(priority_queue* priority_queue_p);
static void priority_queue_clear(priority_queue* priority_queue_p);
static void priority_queue_push_chain(priority_queue* priority_queue_p, struct job* first_newjob, struct job* last_newjob, size_t num, thpool_priority priority);
static struct job* priority_queue_pull(priority_queue* priority_queue_p);
static void priority_queue_destroy(priority_queue* priority_queue_p);
static size_t priority_queue_len(priority_queue* priority_queue_p);

static void bsem_init(struct bsem* bsem_p, int value);
static void bsem_reset(struct bsem* bsem_p);
static void bsem_post(struct bsem* bsem_p);
static void bsem_post_all(struct bsem* bsem_p);
static void bsem_wait(struct bsem* bsem_p);

/* ========================== GENERAL ============================ */
/* Register the process to a signal handler */
void register_process_to_pause_handler(RedisModuleCtx *ctx) {
  /* Register signal handler */
  struct sigaction act;
  struct sigaction oldact;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = thread_hold;
  if (sigaction(SIGUSR2, &act, &oldact) == -1) {
    err("thread_do(): cannot handle SIGUSR2");
  }
  // Notify if SIGUSR2 is already assigned to a user defined signal handler,
  // or was marked as ignored.
  if (oldact.sa_handler != SIG_DFL) {
    RedisModule_Log(ctx, "notice", "register_process_to_pause_handler(): changing SIGUSR2 handler");
  }
}

static void reset_global_vars() {

  g_threads_paused_cnt = 0;
  g_threads_done_cnt = 0;
  g_curr_threadpool = NULL;

}

/* ========================== THREADPOOL ============================ */
/* Create thread pool */
struct redisearch_thpool_t* redisearch_thpool_create(size_t num_threads, const char *thpool_name) {
  /* Make new thread pool */
  redisearch_thpool_t* thpool_p;
  thpool_p = (struct redisearch_thpool_t*)rm_calloc(1, sizeof(struct redisearch_thpool_t));
  if (thpool_p == NULL) {
    err("redisearch_thpool_create(): Could not allocate memory for %s thread pool\n", thpool_name);
    return NULL;
  }
  thpool_p->total_threads_count = num_threads;
  thpool_p->num_threads_alive = 0;
  thpool_p->num_threads_working = 0;
  thpool_p->flags = 0;

  /* copy the threadpool name */
  thpool_p->name = rm_strdup(thpool_name);

  /* Initialise the job queue */
  if(priority_queue_init(&thpool_p->jobqueue) == -1) {
    err("redisearch_thpool_create(): Could not allocate memory for job queue\n");
    rm_free(thpool_p->name);
    rm_free(thpool_p);
    return NULL;
  }

  /* Make threads in pool */
  thpool_p->threads = (struct thread**)rm_malloc(num_threads * sizeof(struct thread*));
  if (thpool_p->threads == NULL) {
    err("redisearch_thpool_create(): Could not allocate memory for threads\n");
    priority_queue_destroy(&thpool_p->jobqueue);
    rm_free(thpool_p->name);
    rm_free(thpool_p);
    return NULL;
  }

  for (size_t i = 0; i < num_threads; i++) {
    thpool_p->threads[i] = (struct thread*)rm_malloc(sizeof(struct thread));
    if (thpool_p->threads[i] == NULL) {
      err("thread_create(): Could not allocate memory for thread\n");
      priority_queue_destroy(&thpool_p->jobqueue);
      for (size_t j = 0; j < i; j++) {
        rm_free(thpool_p->threads[j]);
      }
      rm_free(thpool_p->name);
      rm_free(thpool_p);
      return NULL;
    }
  }

  pthread_mutex_init(&(thpool_p->thcount_lock), NULL);
  pthread_cond_init(&thpool_p->threads_all_idle, NULL);

  return thpool_p;
}

/* initialize thread pool */
void redisearch_thpool_init(struct redisearch_thpool_t* thpool_p) {
  assert(!(thpool_p->flags & RS_THPOOL_F_KEEP_ALIVE));
  thpool_p->flags |= RS_THPOOL_F_KEEP_ALIVE;
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_TERMINATE_WHEN_EMPTY);

  /* Thread init */
  size_t n;
  for (n = 0; n < thpool_p->total_threads_count; n++) {
    thread_init(thpool_p, &thpool_p->threads[n], n);
#if THPOOL_DEBUG
    printf("THPOOL_DEBUG: Created thread %d in pool \n", n);
#endif
  }

  /* Wait for threads to initialize */
  while (thpool_p->num_threads_alive != thpool_p->total_threads_count) {
  }
}

/* Add work to the thread pool */
int redisearch_thpool_add_work(redisearch_thpool_t* thpool_p, void (*function_p)(void*), void* arg_p, thpool_priority priority) {
  job* newjob;

  newjob = (struct job*)rm_malloc(sizeof(struct job));
  if (newjob == NULL) {
    err("thpool_add_work(): Could not allocate memory for new job\n");
    return -1;
  }

  /* add function and argument */
  newjob->function = function_p;
  newjob->arg = arg_p;

  /* add job to queue */
  priority_queue_push_chain(&thpool_p->jobqueue, newjob, newjob, 1, priority);

  return 0;
}

/* Add n work to the thread pool */
int redisearch_thpool_add_n_work(redisearch_threadpool thpool_p, redisearch_thpool_work_t* jobs, size_t n_jobs, thpool_priority priority) {
  if (n_jobs == 0) return 0;
  job* first_newjob = (struct job*)rm_malloc(sizeof(struct job));
  if (first_newjob == NULL) goto fail;

  first_newjob->function = jobs[0].function_p;
  first_newjob->arg = jobs[0].arg_p;
  first_newjob->prev = NULL;
  job* last_newjob = first_newjob;

  for (size_t i = 1; i < n_jobs; i++) {
    job* cur_newjob = (struct job*)rm_malloc(sizeof(struct job));
    if (cur_newjob == NULL) goto fail;

    /* add function and argument */
    cur_newjob->function = jobs[i].function_p;
    cur_newjob->arg = jobs[i].arg_p;
    cur_newjob->prev = NULL;

    /* link jobs */
    last_newjob->prev = cur_newjob;
    last_newjob = cur_newjob;
  }

  /* add jobs to queue */
  priority_queue_push_chain(&thpool_p->jobqueue, first_newjob, last_newjob, n_jobs, priority);

  return 0;

fail:
  err("redisearch_thpool_add_n_work(): Could not allocate memory for %zu new jobs\n", n_jobs);
  while (first_newjob) {
    job* tmp = first_newjob->prev;
    rm_free(first_newjob);
    first_newjob = tmp;
  }
  return -1;
}

/* Wait until all jobs have finished */
void redisearch_thpool_wait(redisearch_thpool_t* thpool_p) {
  pthread_mutex_lock(&thpool_p->thcount_lock);
  while (priority_queue_len(&thpool_p->jobqueue) || thpool_p->num_threads_working) {
    pthread_cond_wait(&thpool_p->threads_all_idle, &thpool_p->thcount_lock);
  }
  pthread_mutex_unlock(&thpool_p->thcount_lock);
}

void redisearch_thpool_timedwait(redisearch_thpool_t* thpool_p, long timeout,
                                 yieldFunc yieldCB, void *yield_ctx) {

  // Set the *absolute* time for waiting the condition variable
  struct timespec time_spec;
  struct timeval tp;
  gettimeofday(&tp, NULL);
  time_spec.tv_sec = tp.tv_sec;
  time_spec.tv_nsec = tp.tv_usec * 1000;
  time_spec.tv_nsec += timeout * 1000000;
  // Check if the nanoseconds field has a carry-out (it cannot hold a value which is lager
  // than 1 sec).
  if (time_spec.tv_nsec >= 1000000000) {
    time_spec.tv_sec++;
    time_spec.tv_nsec -= 1000000000;
  }

  pthread_mutex_lock(&thpool_p->thcount_lock);
  while (priority_queue_len(&thpool_p->jobqueue) || thpool_p->num_threads_working) {
    int rc = pthread_cond_timedwait(&thpool_p->threads_all_idle, &thpool_p->thcount_lock, &time_spec);
    if (rc == ETIMEDOUT) {
      pthread_mutex_unlock(&thpool_p->thcount_lock);
      yieldCB(yield_ctx);
      gettimeofday(&tp, NULL);
      time_spec.tv_sec = tp.tv_sec;
      time_spec.tv_nsec = tp.tv_usec * 1000;
      time_spec.tv_nsec += timeout * 1000000;
      // Check if the nanoseconds field has a carry-out (it cannot hold a value which is lager
      // than 1 sec).
      if (time_spec.tv_nsec >= 1000000000) {
        time_spec.tv_sec++;
        time_spec.tv_nsec -= 1000000000;
      }
      pthread_mutex_lock(&thpool_p->thcount_lock);
    } else {
      assert(rc == 0);
    }
  }
  pthread_mutex_unlock(&thpool_p->thcount_lock);
}

void redisearch_thpool_terminate_threads(redisearch_thpool_t* thpool_p) {
  RedisModule_Assert(thpool_p);

  /* End each thread 's infinite loop */
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_KEEP_ALIVE);

  /* Give one second to kill idle threads */
  double TIMEOUT = 1.0;
  time_t start, end;
  double tpassed = 0.0;
  time(&start);
  while (tpassed < TIMEOUT && thpool_p->num_threads_alive) {
    bsem_post_all(thpool_p->jobqueue.has_jobs);
    time(&end);
    tpassed = difftime(end, start);
  }

  /* Poll remaining threads */
  while (thpool_p->num_threads_alive) {
    bsem_post_all(thpool_p->jobqueue.has_jobs);
    sleep(1);
  }
}

void redisearch_thpool_terminate_when_empty(redisearch_thpool_t* thpool_p) {
  thpool_p->flags |= RS_THPOOL_F_TERMINATE_WHEN_EMPTY;
}

/* Destroy the threadpool */
void redisearch_thpool_destroy(redisearch_thpool_t* thpool_p) {

  // No need to destroy if it's NULL
  if (!thpool_p) return;

  redisearch_thpool_terminate_threads(thpool_p);

  /* Job queue cleanup */
  priority_queue_destroy(&thpool_p->jobqueue);
  /* Deallocs */
  size_t n;
  for (n = 0; n < thpool_p->total_threads_count; n++) {
    thread_destroy(thpool_p->threads[n]);
  }
  rm_free(thpool_p->threads);
  rm_free(thpool_p->name);
  rm_free(thpool_p);
}

/* Pause all threads in threadpool */
void redisearch_thpool_pause(redisearch_thpool_t* thpool_p) {
  if (!thpool_p) return;

  while (thpool_p->flags & RS_THPOOL_F_RESUME) {
    sleep(1);
    RedisModule_Log(
        NULL, "warning",
        "redisearch_thpool_pause(): waiting for %s threadpool to finish resuming process.",
        thpool_p->name);
  }
  size_t n;
	pthread_t caller = pthread_self();

  // save the current thpool to the global pointer
  g_curr_threadpool = thpool_p;

  size_t threadpool_size = thpool_p->num_threads_alive;

  int called_by_threadpool = 0;

  // set hold flag
  thpool_p->flags |= RS_THPOOL_F_PAUSE;

  // zero the number of paused threads in the current threadpool.
  // This number will be increased atomically by each thread to be used
  // as an unique index in the output array.
  g_threads_paused_cnt = 0;

	for(n = 0; n < threadpool_size; n++) {
		// do not pause caller
		if(thpool_p->threads[n]->pthread != caller) {
			pthread_kill(thpool_p->threads[n]->pthread, SIGUSR2);
		} else {
      // The calling thread belongs to this thread pool
      called_by_threadpool = 1;
      thpool_p->flags |= RS_THPOOL_F_CONTAINS_HANDLING_THREAD;
    }
	}

  if (threadpool_size) {
    // wait until all the threads in the thpool are paused (except for the caller)
    size_t expected_new_paused_count = threadpool_size - called_by_threadpool;
    clock_t start = clock();
    size_t paused_threads = g_threads_paused_cnt;
    while(g_threads_paused_cnt < expected_new_paused_count) {
      int waiting_time = (clock() - start)/CLOCKS_PER_SEC;
      if (waiting_time && waiting_time % LOG_WAITING_TIME_INTERVAL == 0) {
          RedisModule_Log(NULL, "warning",
                  "something is wrong: expected to pause %lu threads, but only %lu are paused."
                  "continue waiting",
                  expected_new_paused_count, paused_threads);
      }
      paused_threads = g_threads_paused_cnt;
    }
  }

  // Now all the paused threads have their local copy of their threapool pointer and thread id
  // we can reset the global variables.
  reset_global_vars();
}

/* Resume all threads in threadpool */
void redisearch_thpool_resume(redisearch_thpool_t* thpool_p) {

  if (!thpool_p) return;

  if (!(thpool_p->flags & RS_THPOOL_F_PAUSE)) {
    RedisModule_Log(NULL, "warning", "%s threadpool: redisearch_thpool_resume(): threadpool is not paused",
    thpool_p->name);
    return;
  }
  size_t n;
	pthread_t caller = pthread_self();

  size_t threadpool_size = thpool_p->num_threads_alive;

  int called_by_threadpool = 0;
	for(n = 0; n < threadpool_size; n++) {
		// search for the caller in the thread pool
		if(thpool_p->threads[n]->pthread == caller) {
      called_by_threadpool = 1;
      break;
    }
	}

  // Tell the threads they can resume.
  thpool_p->flags |= RS_THPOOL_F_RESUME;

  wait_for_threads(thpool_p, threadpool_size - called_by_threadpool);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_PAUSE);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_RESUME);
  g_threads_done_cnt = 0;
}

/* ============ COLLECT THREADS DATA AND LOG API ============ */

int redisearch_thpool_safe_to_collect_state() {
  return g_get_backtrace_mode == 0;
}

void redisearch_thpool_pause_before_dump(redisearch_thpool_t* thpool_p) {

  if (!thpool_p) {
    return;
  }

  // Turn on the flag that indicates that data collection process has started.
  g_get_backtrace_mode = 1;

  // set thpool signal handler mode to collect the current threads' state.
  thpool_p->flags |= RS_THPOOL_F_COLLECT_STATE_INFO;

  // The threads should wait until we initialize data structures and flags used to log
  // the dump info.
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);

  // Raise a signal to all the threads to check the flags above.
  redisearch_thpool_pause(thpool_p);
}

void redisearch_thpool_StateLog( redisearch_thpool_t* thpool_p, RedisModuleInfoCtx *ctx) {
  if (!thpool_p) {
    return;
  }

  if (!(thpool_p->flags & RS_THPOOL_F_PAUSE)) {
    RedisModule_Log(
        NULL, "warning",
        "%s threadpool: redisearch_thpool_StateLog(): the threadpool must be paused to dump state log",
        thpool_p->name);
    return;
  }

  // Save all threads data
  redisearch_thpool_StateLog_init(thpool_p);

  // Print the backtrace of each thread
  redisearch_thpool_StateLog_log_to_RSinfo(thpool_p, ctx);

  // cleanup
  redisearch_thpool_StateLog_cleanup(thpool_p);
}


void redisearch_thpool_print_backtrace(redisearch_thpool_t* thpool_p,
                                       RedisModule_Reply *reply) {
  if (!thpool_p) {
    return;
  }

  if (!(thpool_p->flags & RS_THPOOL_F_PAUSE)) {
    RedisModule_Log(
        NULL, "warning",
        "%s threadpool: redisearch_thpool_print_backtrace(): the threadpool must be paused to print backtrace",
        thpool_p->name);
    return;
  }

  // Save all threads data
  redisearch_thpool_StateLog_init(thpool_p);

  // Print the backtrace of each thread
  redisearch_thpool_StateLog_RSreply(thpool_p, reply);

  // cleanup
  redisearch_thpool_StateLog_cleanup(thpool_p);
}

void redisearch_thpool_StateLog_done() {
  // release the backtraces buffer
  rm_free(printable_bt_buffer);
  g_curr_bt_buffer_size = 0;
  printable_bt_buffer = NULL;

  // Turn off the flag to indicate that the data collection process is done
  g_get_backtrace_mode = 0;
}

size_t redisearch_thpool_num_threads_working(redisearch_thpool_t* thpool_p) {
  pthread_mutex_lock(&thpool_p->thcount_lock);
  int res = thpool_p->num_threads_working;
  pthread_mutex_unlock(&thpool_p->thcount_lock);
  return res;
}
/* ======================== THREADS STATE LOG HELPERS ========================= */
/* Initialize all data structures required to log the backtrace of each thread in the threadpool
 * and pause the threads.
 */
static void redisearch_thpool_StateLog_init(redisearch_thpool_t* thpool_p) {

  size_t threadpool_size = thpool_p->num_threads_alive;

  // realloc backtraces buffer array if needed
  if(threadpool_size > g_curr_bt_buffer_size) {
    printable_bt_buffer = rm_realloc(printable_bt_buffer, threadpool_size * sizeof(thread_bt_data));
    g_curr_bt_buffer_size = threadpool_size;
  }

  if (printable_bt_buffer == NULL) {
    RedisModule_Log(NULL, "warning",
                    "%s threadpool: can't realloc printable_bt_buffer, returning with no dump.",
                    thpool_p->name);
  }

  // Initialize the finished threads counter
  g_threads_done_cnt = 0;

  // All the data structures are ready, "signal" all the threads that they can start writing.
  thpool_p->flags |= RS_THPOOL_F_READY_TO_DUMP;

  // If handling the crash is called by one of the threads in the thpool it won't be signaled
  // so we need to get its backtrace here.
  if (thpool_p->flags & RS_THPOOL_F_CONTAINS_HANDLING_THREAD) {
    // The calling thread is always the last
    size_t thread_id = threadpool_size - 1;
    get_backtrace(CRASHED, thpool_p->flags & RS_THPOOL_F_COLLECT_STATE_INFO, thread_id);
  }
}

static void wait_for_threads(redisearch_thpool_t* thpool_p, size_t threads_to_wait_cnt) {

  // when g_threads_done_cnt == threadpool_size all the threads marked that they have done the task.
  clock_t start = clock();
  size_t threads_done = g_threads_done_cnt;
  while (threads_done != threads_to_wait_cnt) {
    int waiting_time = (clock() - start)/CLOCKS_PER_SEC;
    if (waiting_time && waiting_time % LOG_WAITING_TIME_INTERVAL == 0 ) {
      RedisModule_Log(NULL, "warning",
                      "%s threadpool:something is wrong: expected %lu threads to finish, but only %lu are done. "
                      "continue waiting",
                      thpool_p->name, threads_to_wait_cnt, threads_done);
    }
    threads_done = g_threads_done_cnt;
  }
}

/* Prints the log for each thread to the crash log file*/
static void redisearch_thpool_StateLog_log_to_RSinfo(redisearch_thpool_t* thpool_p,
                                                     RedisModuleInfoCtx *ctx) {

  size_t threadpool_size = thpool_p->num_threads_alive;

  wait_for_threads(thpool_p, threadpool_size);

  char info_section_title[100];
  sprintf(info_section_title, "=== %s THREADS LOG: ===", thpool_p->name);
  RedisModule_InfoAddSection(ctx, info_section_title);

  // for each thread in g_threads_done_cnt
  for(size_t i = 0; i < g_threads_done_cnt; i++) {
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

static void redisearch_thpool_StateLog_cleanup(redisearch_thpool_t* thpool_p) {
  // clear counters and turn off flags.
  reset_global_vars();
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_CONTAINS_HANDLING_THREAD);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_COLLECT_STATE_INFO);
}

static void redisearch_thpool_StateLog_RSreply(redisearch_thpool_t* thpool_p,
                                                      RedisModule_Reply *reply) {
  size_t threadpool_size = thpool_p->num_threads_alive;

  // Print the back trace of each thread
  wait_for_threads(thpool_p, threadpool_size);

  char thpool_title[100];
  sprintf(thpool_title, "=== %s THREADS BACKTRACE: ===", thpool_p->name);

  RedisModule_ReplyKV_Map(reply, thpool_title); // >threads dict
  // for each thread in threads_ids
  for(size_t i = 0; i < g_threads_done_cnt; i++) {
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

/* ============================ THREAD ============================== */

/* Initialize a thread in the thread pool
 *
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * @return 0 on success, -1 otherwise.
 */
static int thread_init(redisearch_thpool_t* thpool_p, struct thread** thread_p, int id) {

  (*thread_p)->thpool_p = thpool_p;
  (*thread_p)->id = id;

  pthread_create(&(*thread_p)->pthread, NULL, (void*)thread_do, (*thread_p));
  pthread_detach((*thread_p)->pthread);
  return 0;
}

/* Sets the calling thread on hold */
static void thread_hold(int sig_id) {
  (void)sig_id;
  // save locally the current thpool
  // NOTE: we assume here that pause is waiting for all the threads in the threadpool
  // to be paused.
  redisearch_thpool_t* threadpool = g_curr_threadpool;

  // atomically load and increase paused threads count and use it as the thread index in the
  // output array.
  size_t thread_id = g_threads_paused_cnt++;

  // If we pause to collect current information state, wait until all data structure
  // required for the report are initalized.
  int collect_bt = threadpool->flags & RS_THPOOL_F_COLLECT_STATE_INFO;
  if (collect_bt) {
    while (!(threadpool->flags & RS_THPOOL_F_READY_TO_DUMP)) {
    }
  }

  // Dump the thread's backtrace if required.
  get_backtrace(FINE, collect_bt, thread_id);

  while (!(threadpool->flags & RS_THPOOL_F_RESUME)) {
    sleep(1);
  }

  // Mark that the thread is done waiting
  ++g_threads_done_cnt;
}

/* What each thread is doing
 *
 * In principle this is an endless loop. The only time this loop gets interuppted is once
 * thpool_destroy() is invoked or the program exits.
 *
 * @param  thread        thread that will run this function
 * @return nothing
 */
static void* thread_do(struct thread* thread_p) {

  /* Set thread name for profiling and debuging */
  char thread_name[128] = {0};
  sprintf(thread_name, "thread-pool-%d", thread_p->id);

#if defined(__linux__)
  /* Use prctl instead to prevent using _GNU_SOURCE flag and implicit declaration */
  prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
  pthread_setname_np(thread_name);
#else
  err("thread_do(): pthread_setname_np is not supported on this system");
#endif

  /* Assure all threads have been created before starting serving */
  redisearch_thpool_t* thpool_p = thread_p->thpool_p;

  /* Mark thread as alive (initialized) */
  pthread_mutex_lock(&thpool_p->thcount_lock);
  thpool_p->num_threads_alive += 1;
  pthread_mutex_unlock(&thpool_p->thcount_lock);

  while (thpool_p->flags & RS_THPOOL_F_KEEP_ALIVE) {

    bsem_wait(thpool_p->jobqueue.has_jobs);

    if (thpool_p->flags & RS_THPOOL_F_KEEP_ALIVE) {

      pthread_mutex_lock(&thpool_p->thcount_lock);
      thpool_p->num_threads_working++;
      pthread_mutex_unlock(&thpool_p->thcount_lock);

      /* Read job from queue and execute it */
      void (*func_buff)(void*);
      void* arg_buff;
      job* job_p = priority_queue_pull(&thpool_p->jobqueue);
      if (job_p) {
        func_buff = job_p->function;
        arg_buff = job_p->arg;
        func_buff(arg_buff);
        rm_free(job_p);
      }

      pthread_mutex_lock(&thpool_p->thcount_lock);
      thpool_p->num_threads_working--;
      if (!thpool_p->num_threads_working) {
        pthread_cond_signal(&thpool_p->threads_all_idle);
        if (thpool_p->flags & RS_THPOOL_F_TERMINATE_WHEN_EMPTY) {
          redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_KEEP_ALIVE);
        }
      }
      pthread_mutex_unlock(&thpool_p->thcount_lock);
    }
  }
  pthread_mutex_lock(&thpool_p->thcount_lock);
  thpool_p->num_threads_alive--;
  pthread_mutex_unlock(&thpool_p->thcount_lock);

  return NULL;
}

/* Frees a thread  */
static void thread_destroy(thread* thread_p) {
  rm_free(thread_p);
}

static void thread_bt_buffer_init(uint32_t thread_id, void *bt_addresses_buf, int trace_size, statusOnCrash status_on_crash) {
    // NOTE: backtrace_symbols() returns a pointer to the array malloced by the call
    // and should be freed by us.
    printable_bt_buffer[thread_id].printable_bt = backtrace_symbols(bt_addresses_buf, trace_size);
    printable_bt_buffer[thread_id].trace_size = trace_size;
    printable_bt_buffer[thread_id].status_on_crash = status_on_crash;
}

static void get_backtrace(statusOnCrash status_on_crash, int collect_bt, size_t thread_id) {
  // if we want to collect current state information of the threads
  if(collect_bt) {

    void *bt_addresses_buf[BT_BUF_SIZE];
    // Get the stack trace addresses first.
    int trace_size = backtrace(bt_addresses_buf, BT_BUF_SIZE);

    // Translate addresses into symbols and write them to the backtraces array.
    thread_bt_buffer_init(thread_id, bt_addresses_buf, trace_size, status_on_crash);

    // Atomically increase finished threads count.
    ++g_threads_done_cnt;
  }
}
/* ============================ JOB QUEUE =========================== */

/* Initialize queue */
static int jobqueue_init(jobqueue* jobqueue_p) {
  jobqueue_p->len = 0;
  jobqueue_p->front = NULL;
  jobqueue_p->rear = NULL;
  return 0;
}

/* Clear the queue */
static void jobqueue_clear(jobqueue* jobqueue_p) {

  while (jobqueue_p->len) {
    rm_free(jobqueue_pull(jobqueue_p));
  }
  jobqueue_p->front = NULL;
  jobqueue_p->rear = NULL;
  jobqueue_p->len = 0;
}

/* Add (allocated) chain of jobs to queue
 */
static void jobqueue_push_chain(jobqueue* jobqueue_p, struct job* first_newjob, struct job* last_newjob, size_t num) {
  last_newjob->prev = NULL;

  switch (jobqueue_p->len) {

    case 0: /* if no jobs in queue */
      jobqueue_p->front = first_newjob;
      jobqueue_p->rear = last_newjob;
      break;

    default: /* if jobs in queue */
      jobqueue_p->rear->prev = first_newjob;
      jobqueue_p->rear = last_newjob;
  }
  jobqueue_p->len += num;
}

/* Get first job from queue(removes it from queue)
 *
 * Notice: Caller MUST hold a mutex
 */
static struct job* jobqueue_pull(jobqueue* jobqueue_p) {
  job* job_p = jobqueue_p->front;

  switch (jobqueue_p->len) {

    case 0: /* if no jobs in queue */
      break;

    case 1: /* if one job in queue */
      jobqueue_p->front = NULL;
      jobqueue_p->rear = NULL;
      jobqueue_p->len = 0;
      break;

    default: /* if >1 jobs in queue */
      jobqueue_p->front = job_p->prev;
      jobqueue_p->len--;
      /* more than one job in queue -> post it */
  }

  return job_p;
}

/* Free all queue resources back to the system */
static void jobqueue_destroy(jobqueue* jobqueue_p) {
  jobqueue_clear(jobqueue_p);
}

/* ======================== PRIORITY QUEUE ========================== */

static int priority_queue_init(priority_queue* priority_queue_p) {

  priority_queue_p->has_jobs = (struct bsem*)rm_malloc(sizeof(struct bsem));
  if (priority_queue_p->has_jobs == NULL) {
    return -1;
  }
  jobqueue_init(&priority_queue_p->high_priority_jobqueue);
  jobqueue_init(&priority_queue_p->low_priority_jobqueue);
  bsem_init(priority_queue_p->has_jobs, 0);
  pthread_mutex_init(&priority_queue_p->jobqueues_rwmutex, NULL);
  priority_queue_p->pulls = 0;
  return 0;
}

static void priority_queue_clear(priority_queue* priority_queue_p) {
  jobqueue_clear(&priority_queue_p->high_priority_jobqueue);
  jobqueue_clear(&priority_queue_p->low_priority_jobqueue);
  bsem_reset(priority_queue_p->has_jobs);
}

static void priority_queue_push_chain(priority_queue* priority_queue_p, struct job* f_newjob_p, struct job* l_newjob_p, size_t n, thpool_priority priority) {
  pthread_mutex_lock(&priority_queue_p->jobqueues_rwmutex);
  switch (priority) {
    case THPOOL_PRIORITY_HIGH:
      jobqueue_push_chain(&priority_queue_p->high_priority_jobqueue, f_newjob_p, l_newjob_p, n);
      break;
    case THPOOL_PRIORITY_LOW:
      jobqueue_push_chain(&priority_queue_p->low_priority_jobqueue, f_newjob_p, l_newjob_p, n);
      break;
  }
  bsem_post(priority_queue_p->has_jobs);
  pthread_mutex_unlock(&priority_queue_p->jobqueues_rwmutex);
}

static struct job* priority_queue_pull(priority_queue* priority_queue_p) {
  struct job* job_p = NULL;
  pthread_mutex_lock(&priority_queue_p->jobqueues_rwmutex);
  // We want to pull from the lower priority queue every 3rd time
  if (priority_queue_p->pulls % 3 == 2) {
    job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
    // If the lower priority queue is empty, pull from the higher priority queue
    if(!job_p) {
      job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
    }
  } else {
    job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
    // If the higher priority queue is empty, pull from the lower priority queue
    if(!job_p) {
      job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
    }
  }
  priority_queue_p->pulls++;
  if(priority_queue_p->high_priority_jobqueue.len ||  priority_queue_p->low_priority_jobqueue.len ) {
    bsem_post(priority_queue_p->has_jobs);
  }
  pthread_mutex_unlock(&priority_queue_p->jobqueues_rwmutex);

  return job_p;

}

static void priority_queue_destroy(priority_queue* priority_queue_p) {
  jobqueue_destroy(&priority_queue_p->high_priority_jobqueue);
  jobqueue_destroy(&priority_queue_p->low_priority_jobqueue);
  rm_free(priority_queue_p->has_jobs);
  pthread_mutex_destroy(&priority_queue_p->jobqueues_rwmutex);
}

static size_t priority_queue_len(priority_queue* priority_queue_p) {
  size_t len;
  pthread_mutex_lock(&priority_queue_p->jobqueues_rwmutex);
  len = priority_queue_p->high_priority_jobqueue.len + priority_queue_p->low_priority_jobqueue.len;
  pthread_mutex_unlock(&priority_queue_p->jobqueues_rwmutex);
  return len;
}

/* ======================== SYNCHRONISATION ========================= */

/* Init semaphore to 1 or 0 */
static void bsem_init(bsem* bsem_p, int value) {
  if (value < 0 || value > 1) {
    err("bsem_init(): Binary semaphore can take only values 1 or 0");
    exit(1);
  }
  pthread_mutex_init(&(bsem_p->mutex), NULL);
  pthread_cond_init(&(bsem_p->cond), NULL);
  bsem_p->v = value;
}

/* Reset semaphore to 0 */
static void bsem_reset(bsem* bsem_p) {
  bsem_init(bsem_p, 0);
}

/* Post to at least one thread */
static void bsem_post(bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  bsem_p->v = 1;
  pthread_cond_signal(&bsem_p->cond);
  pthread_mutex_unlock(&bsem_p->mutex);
}

/* Post to all threads */
static void bsem_post_all(bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  bsem_p->v = 1;
  pthread_cond_broadcast(&bsem_p->cond);
  pthread_mutex_unlock(&bsem_p->mutex);
}

/* Wait on semaphore until semaphore has value 0 */
static void bsem_wait(bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  while (bsem_p->v != 1) {
    pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
  }
  bsem_p->v = 0;
  pthread_mutex_unlock(&bsem_p->mutex);
}
