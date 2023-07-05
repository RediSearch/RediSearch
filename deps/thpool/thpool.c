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

#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include "util/thpool_dump_api.h"
#include "util/proc_file.h"
#include "thpool.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#ifdef THPOOL_DEBUG
#define THPOOL_DEBUG 1
#else
#define THPOOL_DEBUG 0
#endif

#if !defined(DISABLE_PRINT) || defined(THPOOL_DEBUG)
#define err(str, ...) fprintf(stderr, str, ##__VA_ARGS__)
#else
#define err(str, ...)
#endif

#define LOG_IF_EXISTS(str) if (thread_p->log) {thread_p->log(str);}

/* Internal threadpool flags */
#define RS_THPOOL_F_KEEP_ALIVE 0x01 /* keep pool alive */

#define RS_THPOOL_F_TERMINATE_WHEN_EMPTY 0x02  /* terminate thread when there are
                                                      no more pending jobs  */

 #define MAX_THPOOL_NAME_LEN 10
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
  LogFunc log;
} thread;

/* Threadpool */
typedef struct redisearch_thpool_t {
  thread** threads;                 /* pointer to threads        */
  size_t total_threads_count;
  volatile size_t num_threads_alive;   /* threads currently alive   */
  volatile size_t num_threads_working; /* threads currently working */
  pthread_mutex_t thcount_lock;     /* used for thread count etc */
  pthread_cond_t threads_all_idle;  /* signal to thpool_wait     */
  priority_queue jobqueue;          /* job queue                 */
  volatile uint16_t flags;                   /* threadpool flags */
  char name[MAX_THPOOL_NAME_LEN + 1];     /* threadpool name */
} redisearch_thpool_t;

/* ========================== GLOBALS ============================ */

// Save the current threadpool.
redisearch_thpool_t *g_curr_threadpool = NULL;
// The number of threads in the current pause call that are paused.
static size_t g_threads_paused_cnt = 0;

static volatile bool g_pause_all = 0;

static volatile bool g_resume_all = 1;

/* ========================== PROTOTYPES ============================ */

// Cleanup allocations related to the threadpool
static void redisearch_thpool_cleanup(redisearch_thpool_t* thpool_p);
static int thread_init(redisearch_thpool_t* thpool_p, struct thread** thread_p, int id, LogFunc log);
static void* thread_do(struct thread* thread_p);
static void thread_hold(int sig_id);
static void thread_destroy(struct thread* thread_p);

static void reset_global_vars();
static void wait_to_resume(size_t threads_to_wait_cnt);

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

static bool pause_all() {
  return g_pause_all;
}
static bool resume_all() {
  return g_resume_all;
}
/* ========================== GENERAL ============================ */
/* Register the process to a signal handler */
void register_process_to_pause_handler(LogFunc log_cb) {
  /* Register signal handler */
  struct sigaction act;
  struct sigaction oldact;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = thread_hold;
  if (sigaction(SIGUSR2, &act, &oldact) == -1) {
    log_cb("register_process_to_pause_handler(): cannot handle SIGUSR2");
  }
  // Notify if SIGUSR2 is already assigned to a user defined signal handler,
  // or was marked as ignored.
  if (oldact.sa_handler != SIG_DFL) {
    log_cb("register_process_to_pause_handler(): changing SIGUSR2 handler");
  }
}

#if defined(__linux__)
size_t pause_all_process_threads() {
  pid_t pid = getpid();
  pid_t caller_tid = gettid();

  // make sure resume is turned off
  g_resume_all = 0;

  g_pause_all = 1;

  pid_t *tids = ProcFile_send_signal_to_all_threads(pid, caller_tid, SIGUSR2);

  if (!tids) {
    return 0;
  }

  size_t ret = array_len(tids);

  // Go over the threads to check if any of the threads blocks or ignores this signal
  for (size_t i = 0; i < array_len(tids) ; i++) {
    int status = REDISMODULE_OK;
    thread_signals_mask thread_masks = ProcFile_get_signals_masks(pid, tids[i], &status);
    if (status == REDISMODULE_ERR) {
      array_free(tids);
      return 0;
    }
    // if SIGUSR2 is block or ignored
    if ((thread_masks.sigBlk & SIGUSR2) || (thread_masks.sigIgn & SIGUSR2)) {
      --ret;
    }
  }

  array_free(tids);
  return ret;
}

#endif // defined(__linux__)

static void reset_global_vars() {
  g_threads_paused_cnt = 0;
  g_curr_threadpool = NULL;
}

/* ========================== THREADPOOL ============================ */
/* Create thread pool */
struct redisearch_thpool_t* redisearch_thpool_create(size_t num_threads, const char *thpool_name) {
  /* Make new thread pool */
  redisearch_thpool_t* thpool_p;
  thpool_p = (struct redisearch_thpool_t*)rm_calloc(1, sizeof(struct redisearch_thpool_t));
  if (thpool_p == NULL) {
    RedisModule_Log(NULL, "notice", "redisearch_thpool_create(): Could not allocate memory for %s thread pool\n", thpool_name);
    return NULL;
  }
  thpool_p->total_threads_count = num_threads;
  thpool_p->num_threads_alive = 0;
  thpool_p->num_threads_working = 0;
  thpool_p->flags = 0;

  /* copy the threadpool name */
  /* If the name is too long, it will be truncated */
  if (strlen(thpool_name) > MAX_THPOOL_NAME_LEN) {
    RedisModule_Log(NULL, "notice", "redisearch_thpool_create(): thpool name is too long, truncating it\n");
  }
  strncpy(thpool_p->name, thpool_name, MAX_THPOOL_NAME_LEN);

  /* Initialise the job queue */
  if(priority_queue_init(&thpool_p->jobqueue) == -1) {
    RedisModule_Log(NULL, "notice", "redisearch_thpool_create(): Could not allocate memory for job queue\n");
    redisearch_thpool_cleanup(thpool_p);
    return NULL;
  }

  /* Make threads in pool */
  thpool_p->threads = (struct thread**)rm_calloc(num_threads, sizeof(struct thread*));
  if (thpool_p->threads == NULL) {
    RedisModule_Log(NULL, "notice", "redisearch_thpool_create(): Could not allocate memory for threads\n");
    redisearch_thpool_cleanup(thpool_p);
    return NULL;
  }

  for (size_t i = 0; i < num_threads; i++) {
    thpool_p->threads[i] = (struct thread*)rm_malloc(sizeof(struct thread));
    if (thpool_p->threads[i] == NULL) {
      RedisModule_Log(NULL, "notice", "thread_create(): Could not allocate memory for thread\n");
      redisearch_thpool_cleanup(thpool_p);
      return NULL;
    }
  }

  pthread_mutex_init(&(thpool_p->thcount_lock), NULL);
  pthread_cond_init(&thpool_p->threads_all_idle, NULL);

  return thpool_p;
}

/* initialize thread pool */
void redisearch_thpool_init(struct redisearch_thpool_t* thpool_p, LogFunc log) {
  assert(!(thpool_p->flags & RS_THPOOL_F_KEEP_ALIVE));
  redisearch_thpool_TURNON_flag(thpool_p, RS_THPOOL_F_KEEP_ALIVE);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_TERMINATE_WHEN_EMPTY);

  /* Thread init */
  size_t n;
  for (n = 0; n < thpool_p->total_threads_count; n++) {
    thread_init(thpool_p, &thpool_p->threads[n], n, log);
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

void redisearch_thpool_drain(redisearch_thpool_t* thpool_p, long timeout,
                                 yieldFunc yieldCB, void *yield_ctx, size_t threshold) {

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
  while (priority_queue_len(&thpool_p->jobqueue) > threshold) {
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
  redisearch_thpool_TURNON_flag(thpool_p, RS_THPOOL_F_TERMINATE_WHEN_EMPTY);
}

/* Destroy the threadpool */
void redisearch_thpool_destroy(redisearch_thpool_t* thpool_p) {

  // No need to destroy if it's NULL
  if (!thpool_p) return;

  redisearch_thpool_terminate_threads(thpool_p);

  /* Allocations cleanup */
  redisearch_thpool_cleanup(thpool_p);
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
  redisearch_thpool_TURNON_flag(thpool_p, RS_THPOOL_F_PAUSE);

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
      redisearch_thpool_TURNON_flag(thpool_p, RS_THPOOL_F_CONTAINS_HANDLING_THREAD);
    }
	}

  if (threadpool_size > 0) {
    // wait until all the threads in the thpool are paused (except for the caller)
    size_t expected_new_paused_count = threadpool_size - called_by_threadpool;
    clock_t start = clock();
    size_t paused_threads = __atomic_load_n(&g_threads_paused_cnt, __ATOMIC_RELAXED);
    while(paused_threads < expected_new_paused_count) {
      RS_LOG_ASSERT_FMT((clock() - start)/CLOCKS_PER_SEC < WAIT_FOR_THPOOL_TIMEOUT,
          "expected to pause %lu threads, but only %lu are paused.",
          expected_new_paused_count, paused_threads);
      paused_threads = __atomic_load_n(&g_threads_paused_cnt, __ATOMIC_RELAXED);
    }
  }

  // Now all the paused threads have their local copy of their threapool pointer and thread id
  // we can reset the global variables.
  reset_global_vars();
}

void resume_all_process_threads() {
  size_t paused_threads = g_threads_paused_cnt;
  g_pause_all = 0;
  g_resume_all = 1;
  wait_to_resume(paused_threads);
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

  g_threads_paused_cnt = threadpool_size - called_by_threadpool;

  // Tell the threads they can resume.
  redisearch_thpool_TURNON_flag(thpool_p, RS_THPOOL_F_RESUME);

  if (threadpool_size > 0) {
    wait_to_resume(threadpool_size - called_by_threadpool);
  }
  // If we got here all the threads were resumed.
  // Flags cleanup
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_PAUSE);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_RESUME);
}

// turn off threadpool flag
void redisearch_thpool_TURNOFF_flag(redisearch_thpool_t* thpool_p, uint16_t flag) {
  thpool_p->flags &= ~flag;
}

// turn on threadpool flag
void redisearch_thpool_TURNON_flag(redisearch_thpool_t* thpool_p, uint16_t flag) {
  thpool_p->flags |= flag;
}

// Check if flag is set
bool redisearch_thpool_ISSET_flag(redisearch_thpool_t* thpool_p, uint16_t flag) {
  return thpool_p->flags & flag;
}

const char *redisearch_thpool_get_name(redisearch_thpool_t* thpool_p) {
  return thpool_p->name;
}

size_t redisearch_thpool_num_threads_working(redisearch_thpool_t* thpool_p) {
  pthread_mutex_lock(&thpool_p->thcount_lock);
  int res = thpool_p->num_threads_working;
  pthread_mutex_unlock(&thpool_p->thcount_lock);
  return res;
}

size_t redisearch_thpool_num_threads_alive_unsafe(redisearch_thpool_t* thpool_p) {
  return thpool_p->num_threads_alive;
}

/* ========================== THPOOL HELPERS ============================ */

static void redisearch_thpool_cleanup(redisearch_thpool_t* thpool_p) {
  priority_queue_destroy(&thpool_p->jobqueue);
  for (size_t i = 0; i < thpool_p->total_threads_count; i++) {
    rm_free(thpool_p->threads[i]);
  }
  rm_free(thpool_p->threads);
  rm_free(thpool_p);
}

static void wait_to_resume(size_t threads_to_wait_cnt) {
  clock_t start = clock();
  size_t paused_threads = __atomic_load_n(&g_threads_paused_cnt, __ATOMIC_RELAXED);
  while(paused_threads > 0) {
    RS_LOG_ASSERT_FMT((clock() - start)/CLOCKS_PER_SEC < WAIT_FOR_THPOOL_TIMEOUT,
        "expected %lu threads to resume, but only %lu were resumed.",
        threads_to_wait_cnt, paused_threads);
    paused_threads = __atomic_load_n(&g_threads_paused_cnt, __ATOMIC_RELAXED);
    }
}

/* ============================ THREAD ============================== */

/* Initialize a thread in the thread pool
 *
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * @return 0 on success, -1 otherwise.
 */
static int thread_init(redisearch_thpool_t* thpool_p, struct thread** thread_p, int id, LogFunc log) {

  (*thread_p)->thpool_p = thpool_p;
  (*thread_p)->id = id;
  (*thread_p)->log = log;

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
  size_t thread_id = __atomic_fetch_add(&g_threads_paused_cnt, 1, __ATOMIC_RELAXED);

  // If we pause to collect current information state, wait until all data structure
  // required for the report are initalized.
  if (ThpoolDump_collect_all_mode() || (threadpool && (threadpool->flags & RS_THPOOL_F_COLLECT_STATE_INFO))) {
    while (!ThpoolDump_all_ready() || (threadpool && !(threadpool->flags & RS_THPOOL_F_READY_TO_DUMP))) {
    }
    ThpoolDump_log_backtrace(FINE, thread_id);
  }


  while (!resume_all() || (threadpool && !(threadpool->flags & RS_THPOOL_F_RESUME))) {
  }

  // Mark that the thread is resumed.
  __atomic_fetch_add(&g_threads_paused_cnt, -1, __ATOMIC_RELAXED);

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

  redisearch_thpool_t* thpool_p = thread_p->thpool_p;
  /* Set thread name for profiling and debuging */
  // thread's name len is restricted to 16 characters, including the terminating null byte.
  char thread_name[16] = {0};
  const char *thpool_name = redisearch_thpool_get_name(thpool_p);
  sprintf(thread_name, "%s-%d", thpool_name, thread_p->id);

#if defined(__linux__)
  /* Use prctl instead to prevent using _GNU_SOURCE flag and implicit declaration */
  // if the name id too long  the string is silently truncated
  prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
  pthread_setname_np(thread_name);
#else
  err("thread_do(): pthread_setname_np is not supported on this system");
#endif

  /* Assure all threads have been created before starting serving */

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
        LOG_IF_EXISTS("thpool contains no more jobs")
        pthread_cond_signal(&thpool_p->threads_all_idle);
        if (thpool_p->flags & RS_THPOOL_F_TERMINATE_WHEN_EMPTY) {
          LOG_IF_EXISTS("terminating thread pool after there are no more jobs")
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
