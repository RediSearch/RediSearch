/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include "barrier.h"
#include "rmalloc.h"
#include "thpool.h"

#define LOG_IF_EXISTS(level, str, ...)                                         \
  if (thpool_p->log) {                                                         \
    thpool_p->log(level, str, ##__VA_ARGS__);                                  \
  }

#define MAX_THPOOL_NAME_BUFFER_SIZE 11
/* ========================== ENUMS ============================ */

typedef enum {
  THPOOL_INITIALIZED = (1 << 0),
  THPOOL_UNINITIALIZED = 0,   /** Can be one of two states:
                                * 1. There are no threads alive
                                * 2. There are threads alive in THREAD_TERMINATE_WHEN_EMPTY state. */
} ThpoolState;

typedef enum {
  THREAD_RUNNING = 0,
  THREAD_TERMINATE_WHEN_EMPTY = (1 << 0),
  THREAD_TERMINATE_ASAP = (1 << 1),
} ThreadState;

typedef enum {
  JOBQ_RUNNING = 0,
  JOBQ_PAUSED = (1 << 0),
} JobqueueState;
/* ========================== STRUCTURES ============================ */

/* Job */
typedef struct job {
  struct job *prev;            /* pointer to previous job   */
  void (*function)(void *arg); /* function pointer          */
  void *arg;                   /* function's argument       */
} job;

/* JobCtx pulled from the priority jobqueue */
typedef struct {
  job *job;
  bool is_admin;
  bool has_priority_ticket;
} priorityJobCtx;

typedef struct {
  job *first_job;
  job *last_job;
} jobsChain;

typedef struct {
  ThreadState thread_state;
} threadCtx;

typedef struct {
  void *arg;
  threadCtx *thread_ctx;
} adminJobArg;

/* Job queue */
typedef struct {
  job *front; /* pointer to front of queue */
  job *rear;  /* pointer to rear  of queue */
  int len;    /* number of jobs in queue   */
} jobqueue;
typedef struct priority_queue {
  jobqueue high_priority_jobqueue;  /* job queue for high priority tasks */
  jobqueue low_priority_jobqueue;   /* job queue for low priority tasks */
  jobqueue admin_priority_jobqueue; /* job queue for administration tasks */
  pthread_mutex_t lock;             /* used for queue r/w access */
  unsigned char alternating_pulls;  /* number of pulls by non-bias threads from queue */
  unsigned char n_high_priority_bias; /* minimal number of high priority jobs to run in
                                       * parallel (if there are enough threads) */
  atomic_uchar high_priority_tickets; /* number of currently available priority
                                       * tickets to reach the high priority bias */
  pthread_cond_t has_jobs; /* Conditional variable to wake up threads waiting
                              for new jobs */
  volatile atomic_size_t num_jobs_in_progress; /* threads currently working */
  volatile JobqueueState state; /* Indicates whether the threads should pull
                                   jobs from the jobq or sleep */
} priorityJobqueue;

/* Threadpool */
typedef struct redisearch_thpool_t {
  size_t n_threads;
  volatile atomic_size_t num_threads_alive;   /* threads currently alive   */
  ThpoolState state;                          /* threadpool state, accessed only by the main thread */
  priorityJobqueue jobqueues;                 /* job queue                 */
  LogFunc log;                                /* log callback              */
  volatile atomic_size_t total_jobs_done;     /* statistics for observability */
  char name[MAX_THPOOL_NAME_BUFFER_SIZE];     /* thpool identifier to name its threads.
                                                limited to 11 bytes length (including the
                                                null byte) to leave room for
                                                '-<thread id>'*/
} redisearch_thpool_t;
/* ========================== PROTOTYPES ============================ */

static void redisearch_thpool_verify_init(redisearch_thpool_t *thpool_p);
static void redisearch_thpool_lock(redisearch_thpool_t *thpool_p);
static void redisearch_thpool_unlock(redisearch_thpool_t *thpool_p);
static void redisearch_thpool_push_chain_verify_init_threads(redisearch_thpool_t *thpool_p,
                                                             job *first_newjob,
                                                             job *last_newjob,
                                                             size_t num,
                                                             thpool_priority priority);

static int thread_init(redisearch_thpool_t *thpool_p);
static void *thread_do(redisearch_thpool_t *thpool_p);

static int jobqueue_init(jobqueue *jobqueue_p);
static void jobqueue_clear(jobqueue *jobqueue_p);
static void jobqueue_push_chain(jobqueue *jobqueue_p, job *first_newjob,
                                job *last_newjob, size_t num);
static job *jobqueue_pull(jobqueue *jobqueue_p);
static void jobqueue_destroy(jobqueue *jobqueue_p);
static jobsChain create_jobs_chain(redisearch_thpool_work_t *jobs,
                                   size_t n_jobs);

static int priority_queue_init(priorityJobqueue *priority_queue_p,
                               size_t n_threads,
                               size_t high_priority_bias_threshold);
static void priority_queue_clear(priorityJobqueue *priority_queue_p);
static void priority_queue_push_chain_unsafe(priorityJobqueue *priority_queue_p,
                                             job *first_newjob,
                                             job *last_newjob, size_t num,
                                             thpool_priority priority);
static priorityJobCtx priority_queue_pull(priorityJobqueue *priority_queue_p);
static inline priorityJobCtx priority_queue_pull_from_queues_unsafe(priorityJobqueue *priority_queue_p);
static priorityJobCtx priority_queue_pull_no_wait(priorityJobqueue *priority_queue_p);
static void priority_queue_destroy(priorityJobqueue *priority_queue_p);
static size_t priority_queue_len(priorityJobqueue *priority_queue_p);
static size_t priority_queue_len_unsafe(priorityJobqueue *priority_queue_p);
static size_t
priority_queue_num_incomplete_jobs(priorityJobqueue *priority_queue_p);
static bool priority_queue_is_empty(priorityJobqueue *jobqueue_p);
static bool priority_queue_is_empty_unsafe(priorityJobqueue *jobqueue_p);

/* ========================== GLOBALS ============================ */

/** Hashtable to hold the two pull functions. Aligned with the 'threadState'
 * enum values Not very pretty, but allows us to avoid if statements in the
 * thread loop. */
static priorityJobCtx (*pull_and_execute_ht[2])(priorityJobqueue *) = {
    // THREAD_RUNNING
    priority_queue_pull,
    // THREAD_TERMINATE_WHEN_EMPTY Change to pull mechanism to return
    // immediately id jobq is empty
    priority_queue_pull_no_wait};

/* ========================== THREADS MANAGER API ============================
 */
typedef struct {
  barrier_t *barrier; /* The calling thread blocks until the required number of
                                threads have called barrier_wait() */
  const ThreadState new_state;
} SignalThreadCtx;
static void admin_job_change_state(void *job_arg);
static void redisearch_thpool_broadcast_new_state(redisearch_thpool_t *thpool,
                                                  size_t n_threads,
                                                  ThreadState new_state);
/* ========================== THREADPOOL ============================ */

/* Create thread pool */
struct redisearch_thpool_t *redisearch_thpool_create(size_t num_threads, size_t high_priority_bias_threshold,
                         LogFunc log, const char *thpool_name) {
  /* Make new thread pool */
  redisearch_thpool_t *thpool_p;
  thpool_p = (struct redisearch_thpool_t *)rm_malloc(
      sizeof(struct redisearch_thpool_t));
  if (thpool_p == NULL) {
    if (log)
      log("warning", "redisearch_thpool_create(): Could not allocate memory "
                     "for thread pool");
    return NULL;
  }
  thpool_p->log = log;
  thpool_p->n_threads = num_threads;
  thpool_p->num_threads_alive = 0;
  thpool_p->state = THPOOL_UNINITIALIZED;
  thpool_p->total_jobs_done = 0;

  /* Seed the random number generator for the threads ids. */
  srand(time(NULL));
  snprintf(thpool_p->name, MAX_THPOOL_NAME_BUFFER_SIZE, "%s", thpool_name);

  /* Initialise the job queue */
  priority_queue_init(&thpool_p->jobqueues, num_threads,
                      high_priority_bias_threshold);

  return thpool_p;
}

/* Initialise thread pool. This function is not thread safe. */
static void redisearch_thpool_verify_init(struct redisearch_thpool_t *thpool_p) {
  if (thpool_p->state == THPOOL_INITIALIZED)
    return; // Already initialized and all threads are active.

  /** Else, either:
   * case 1: There are no threads alive, just add n_threads threads.
   * case 2: There are threads alive in terminate_when_empty state.
   * In this case, we need to add the missing threads to adjust
   * `num_threads_alive` to n_threads
   *    case 1.a: num_threads_alive >= n_threads (we have set the thpool to
   *              terminate when empty and then decreased n_threads)
   *              - n_threads_to_revive = n_threads
   *              - n_threads_to_kill = n_threads_alive - n_threads
   *    case 1.b: num_threads_alive < n_threads ( we have set the thpool to
   *              terminate when empty and *might also* increased n_threads)
   *              - n_threads_to_revive = num_threads_alive
   *              - n_new_threads = n_threads - num_threads_alive new threads */
  redisearch_thpool_lock(thpool_p);
  size_t curr_num_threads_alive = thpool_p->num_threads_alive;
  size_t n_threads = thpool_p->n_threads;
  size_t n_new_threads = 0;
  if (curr_num_threads_alive) { // Case 1 - some or all threads are alive in
                                // TERMINATE_WHEN_EMPTY state
    size_t n_threads_to_revive = 0;
    size_t n_threads_to_kill = 0;
    if (curr_num_threads_alive >= n_threads) { // Case 1.a
      // Revive n_threads
      n_threads_to_revive = n_threads;
      // Kill extra threads
      n_threads_to_kill = curr_num_threads_alive - n_threads;
    } else {                                  // Case 1.b
      // Revive all threads
      n_threads_to_revive = curr_num_threads_alive;
      // Add missing threads
      n_new_threads = n_threads - curr_num_threads_alive;
    }

    /* In both cases we send `curr_num_threads_alive` jobs. */
    barrier_t barrier;
    barrier_init(&barrier, NULL, curr_num_threads_alive);

    /* Create jobs and their args */
    redisearch_thpool_work_t jobs[curr_num_threads_alive];

    /* Set new state of `n_threads_to_revive` threads state to 'THREAD_RUNNING' */
    SignalThreadCtx job_arg_revive = {.barrier = &barrier, .new_state = THREAD_RUNNING};
    for (size_t i = 0; i < n_threads_to_revive; i++) {
      jobs[i].arg_p = &job_arg_revive;
      jobs[i].function_p = admin_job_change_state;
    }

    /* Set new state of `n_threads_to_kill` threads state to 'THREAD_TERMINATE_ASAP' */
    SignalThreadCtx job_arg_kill = {.barrier = &barrier, .new_state = THREAD_TERMINATE_ASAP};
    for (size_t i = n_threads_to_revive; i < curr_num_threads_alive; i++) {
      jobs[i].arg_p = &job_arg_kill;
      jobs[i].function_p = admin_job_change_state;
    }

    jobsChain jobs_chain = create_jobs_chain(jobs, curr_num_threads_alive);
    priority_queue_push_chain_unsafe(&thpool_p->jobqueues, jobs_chain.first_job, jobs_chain.last_job,
        curr_num_threads_alive, THPOOL_PRIORITY_ADMIN);

    /* Unlock to allow the threads to pull from the jobq */
    redisearch_thpool_unlock(thpool_p);
    /* Wait on for the threads to pass the barrier and destroy the barrier*/
    barrier_wait_for_threads_and_destroy(&barrier);
  } else { // Case 2 - no threads alive
    redisearch_thpool_unlock(thpool_p);
    n_new_threads = n_threads;
  }

  /* Add new threads if needed */
  for (size_t n = 0; n < n_new_threads; n++) {
    thread_init(thpool_p);
  }

  /* Wait for threads to initialize */
  while (thpool_p->num_threads_alive != thpool_p->n_threads) {
    usleep(1); // avoid busy loop, wait for a very small amount of time.
  }

  thpool_p->state = THPOOL_INITIALIZED;

  LOG_IF_EXISTS("verbose", "Thread pool of size %zu created successfully",
                thpool_p->n_threads)
}

size_t redisearch_thpool_remove_threads(redisearch_thpool_t *thpool_p,
                                        size_t n_threads_to_remove) {
  /* n_threads is only configured and read by the main thread (protected by the GIL). */
  assert(thpool_p->n_threads >= n_threads_to_remove && "Number of threads can't be negative");
  thpool_p->n_threads -= n_threads_to_remove;
  size_t n_threads = thpool_p->n_threads;

  /** THPOOL_UNINITIALIZED means either:
   * 1. There are no threads alive
   * 2. There are threads alive in terminate_when_empty state.
   * In both cases only calling `verify_init` will add/remove threads to adjust
   * `num_threads_alive` to `n_threads` */
  if (thpool_p->state == THPOOL_UNINITIALIZED)
    return n_threads;

  size_t jobs_count = priority_queue_len(&thpool_p->jobqueues);
  if (n_threads == 0 && jobs_count > 0) {
    LOG_IF_EXISTS("warning",
                  "redisearch_thpool_remove_threads(): "
                  "Killing all threads while jobqueue contains %zu jobs",
                  jobs_count);
  }

  assert(thpool_p->jobqueues.state == JOBQ_RUNNING && "Can't remove threads while jobq is paused");


  redisearch_thpool_broadcast_new_state(thpool_p, n_threads_to_remove,
                                        THREAD_TERMINATE_ASAP);

  /* Wait until `num_threads_alive` == `n_threads` */
  while (thpool_p->num_threads_alive != n_threads) {
    usleep(1);
  }

  LOG_IF_EXISTS("verbose", "Thread pool size decreased to %zu successfully", n_threads)

  return n_threads;
}

size_t redisearch_thpool_add_threads(redisearch_thpool_t *thpool_p,
                                     size_t n_threads_to_add) {
  /* n_threads is only configured and read by the main thread (protected by the GIL). */
  thpool_p->n_threads += n_threads_to_add;
  size_t n_threads = thpool_p->n_threads;

  /** THPOOL_UNINITIALIZED means either:
   * 1. There are no threads alive
   * 2. There are threads alive in terminate_when_empty state.
   * In both cases only calling `verify_init` will add/remove threads to adjust
   * `num_threads_alive` to `n_threads` */
  if (thpool_p->state == THPOOL_UNINITIALIZED)
    return n_threads;

  /* Add new threads */
  for (size_t n = 0; n < n_threads_to_add; n++) {
    thread_init(thpool_p);
  }

  /* Wait until `num_threads_alive` == `n_threads` */
  while (thpool_p->num_threads_alive != n_threads) {
    usleep(1);
  }

  LOG_IF_EXISTS("verbose", "Thread pool size increased to %zu successfully", n_threads)

  return n_threads;
}

/* Add work to the thread pool */
int redisearch_thpool_add_work(redisearch_thpool_t *thpool_p,
                               void (*function_p)(void *), void *arg_p,
                               thpool_priority priority) {
  job *newjob;

  newjob = (job *)rm_malloc(sizeof(job));
  if (newjob == NULL) {
    LOG_IF_EXISTS("warning",
                  "thpool_add_work(): Could not allocate memory for new job");
    return -1;
  }

  /* Add function and argument */
  newjob->function = function_p;
  newjob->arg = arg_p;

  /* Add job to queue */
  redisearch_thpool_push_chain_verify_init_threads(thpool_p, newjob, newjob, 1,
                                                   priority);

  return 0;
}

/* Add n work to the thread pool */
int redisearch_thpool_add_n_work(redisearch_threadpool thpool_p,
                                 redisearch_thpool_work_t *jobs, size_t n_jobs,
                                 thpool_priority priority) {
  if (n_jobs == 0)
    return 0;
  jobsChain jobs_chain = create_jobs_chain(jobs, n_jobs);

  job *first_newjob = jobs_chain.first_job;
  job *last_newjob = jobs_chain.last_job;
  if (!jobs_chain.first_job || !jobs_chain.last_job) {
    LOG_IF_EXISTS("warning",
                  "redisearch_thpool_add_n_work(): Could not allocate memory "
                  "for %zu new jobs",
                  n_jobs);
    return -1;
  }

  /* Add jobs to queue */
  redisearch_thpool_push_chain_verify_init_threads(
      thpool_p, first_newjob, last_newjob, n_jobs, priority);

  return 0;

fail:
  LOG_IF_EXISTS("warning",
                "redisearch_thpool_add_n_work(): Could not allocate memory for "
                "%zu new jobs",
                n_jobs);
  while (first_newjob) {
    job *tmp = first_newjob->prev;
    rm_free(first_newjob);
    first_newjob = tmp;
  }
  return -1;
}

static void redisearch_thpool_push_chain_verify_init_threads(
    redisearch_thpool_t *thpool_p, job *f_newjob_p, job *l_newjob_p, size_t n,
    thpool_priority priority) {
  redisearch_thpool_lock(thpool_p);
  priority_queue_push_chain_unsafe(&thpool_p->jobqueues, f_newjob_p, l_newjob_p,
                                   n, priority);
  redisearch_thpool_unlock(thpool_p);

  /* Initialize threads if needed */
  redisearch_thpool_verify_init(thpool_p);
}

/* Wait until all jobs have finished */
void redisearch_thpool_wait(redisearch_thpool_t *thpool_p) {
  priorityJobqueue *priority_queue_p = &thpool_p->jobqueues;
  long msec_timeout = 100;
  redisearch_thpool_drain(thpool_p, msec_timeout, NULL, NULL, 0);
}

void redisearch_thpool_drain(redisearch_thpool_t *thpool_p, long timeout,
                             yieldFunc yieldCB, void *yield_ctx,
                             size_t threshold) {
  long usec_timeout = 1000 * timeout;
  while (priority_queue_num_incomplete_jobs(&thpool_p->jobqueues) > threshold) {
    usleep(usec_timeout);
    if (yieldCB)
      yieldCB(yield_ctx);
  }
}

void redisearch_thpool_terminate_threads(redisearch_thpool_t *thpool_p) {
  RedisModule_Assert(thpool_p);
  /** Threads might be in terminate when empty state, we must lock before we
   * read `num_threads_alive` to ensure they don't die (i.e check that jobq is
   * not empty) to read `num_threads_alive` */
  redisearch_thpool_lock(thpool_p);

  if (!priority_queue_is_empty_unsafe(&thpool_p->jobqueues)) {
    LOG_IF_EXISTS(
        "warning",
        "Terminate threadpool's thread was called when the jobq is not empty");
  }
  size_t curr_num_threads_alive = thpool_p->num_threads_alive;
  if (curr_num_threads_alive) {
    /* Ensure jobq is running */
    thpool_p->jobqueues.state = JOBQ_RUNNING;

    /* Create a barrier. */
    barrier_t barrier;
    barrier_init(&barrier, NULL, curr_num_threads_alive);

    /* Create jobs and their args */
    redisearch_thpool_work_t jobs[curr_num_threads_alive];
    SignalThreadCtx job_arg = {.barrier = &barrier, .new_state = THREAD_TERMINATE_ASAP};
    /* Set new state of all threads to 'THREAD_TERMINATE_ASAP' */
    for (size_t i = 0; i < curr_num_threads_alive; i++) {
      jobs[i].arg_p = &job_arg;
      jobs[i].function_p = admin_job_change_state;
    }

    jobsChain jobs_chain = create_jobs_chain(jobs, curr_num_threads_alive);
    priority_queue_push_chain_unsafe(
        &thpool_p->jobqueues, jobs_chain.first_job, jobs_chain.last_job,
        curr_num_threads_alive, THPOOL_PRIORITY_ADMIN);

    /* Unlock to allow the threads to pull from the jobq */
    redisearch_thpool_unlock(thpool_p);
    /* Wait on for the threads to pass the barrier and destroy the barrier*/
    barrier_wait_for_threads_and_destroy(&barrier);

    while (thpool_p->num_threads_alive) {
      usleep(1);
    }
  } else {
    redisearch_thpool_unlock(thpool_p);
  }

  thpool_p->state = THPOOL_UNINITIALIZED;
}

void redisearch_thpool_terminate_when_empty(redisearch_thpool_t *thpool_p) {
  assert(thpool_p->jobqueues.state == JOBQ_RUNNING);
  if (thpool_p->state == THPOOL_UNINITIALIZED)
    return;
  size_t n_threads = thpool_p->n_threads;
  redisearch_thpool_broadcast_new_state(thpool_p, n_threads,
                                        THREAD_TERMINATE_WHEN_EMPTY);

  /* Change thpool state to uninitialized */
  thpool_p->state = THPOOL_UNINITIALIZED;
}

/* Destroy the threadpool */
void redisearch_thpool_destroy(redisearch_thpool_t *thpool_p) {

  /* No need to destroy if it's NULL */
  if (!thpool_p)
    return;

  redisearch_thpool_terminate_threads(thpool_p);

  /* Job queue cleanup */
  priority_queue_destroy(&thpool_p->jobqueues);

  rm_free(thpool_p);
}

/* ============ STATS ============ */

size_t redisearch_thpool_num_jobs_in_progress(redisearch_thpool_t *thpool_p) {
  return thpool_p->jobqueues.num_jobs_in_progress;
}

size_t redisearch_thpool_get_n_threads(redisearch_thpool_t *thpool_p) {
  return thpool_p->n_threads;
}

thpool_stats redisearch_thpool_get_stats(redisearch_thpool_t *thpool_p) {
  /* Locking must be done in the following order to prevent deadlocks. */
  redisearch_thpool_lock(thpool_p);
  thpool_stats res = {
      .total_jobs_done = thpool_p->total_jobs_done,
      .high_priority_pending_jobs =
          thpool_p->jobqueues.high_priority_jobqueue.len,
      .low_priority_pending_jobs =
          thpool_p->jobqueues.low_priority_jobqueue.len,
      .admin_priority_pending_jobs =
          thpool_p->jobqueues.admin_priority_jobqueue.len,
      .total_pending_jobs = priority_queue_len_unsafe(&thpool_p->jobqueues),
      .num_threads_alive = thpool_p->num_threads_alive,
  };
  redisearch_thpool_unlock(thpool_p);
  return res;
}

/* ============ INTERNAL UTILS ============ */
static void redisearch_thpool_lock(redisearch_thpool_t *thpool_p) {
  pthread_mutex_lock(&thpool_p->jobqueues.lock);
}

static void redisearch_thpool_unlock(redisearch_thpool_t *thpool_p) {
  pthread_mutex_unlock(&thpool_p->jobqueues.lock);
}

/* ============ DEBUG ============ */

void redisearch_thpool_pause_threads(redisearch_thpool_t *thpool_p) {
  redisearch_thpool_lock(thpool_p);
  thpool_p->jobqueues.state = JOBQ_PAUSED;
  redisearch_thpool_unlock(thpool_p);

  while (redisearch_thpool_num_jobs_in_progress(thpool_p)) {
    usleep(1);
  }
}

void redisearch_thpool_pause_threads_no_wait(redisearch_thpool_t *thpool_p) {
  redisearch_thpool_lock(thpool_p);
  thpool_p->jobqueues.state = JOBQ_PAUSED;
  redisearch_thpool_unlock(thpool_p);
}

int redisearch_thpool_paused(redisearch_thpool_t *thpool_p) {
  return thpool_p->jobqueues.state == JOBQ_PAUSED;
}

int redisearch_thpool_is_initialized(redisearch_thpool_t *thpool_p) {
  return thpool_p->state == THPOOL_INITIALIZED;
}

void redisearch_thpool_resume_threads(redisearch_thpool_t *thpool_p) {
  redisearch_thpool_lock(thpool_p);
  assert(redisearch_thpool_paused(thpool_p));
  thpool_p->jobqueues.state = JOBQ_RUNNING;
  pthread_cond_broadcast(&thpool_p->jobqueues.has_jobs);
  redisearch_thpool_unlock(thpool_p);
}

/* ============================ THREAD ============================== */

/* Initialize a thread in the thread pool
 *
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * @return 0 on success, -1 otherwise.
 */
static int thread_init(redisearch_thpool_t *thpool_p) {
  pthread_t thread_id;
  pthread_create(&thread_id, NULL, (void *(*)(void *))thread_do, thpool_p);
  pthread_detach(thread_id);
  return 0;
}

/* What each thread is doing
 *
 * In principle this is an endless loop. The only time this loop gets
 * interrupted is once thpool_destroy() is invoked or the program exits.
 *
 * @param  thread        thread that will run this function
 * @return nothing
 */
static void *thread_do(redisearch_thpool_t *thpool_p) {

  /* Set thread name for profiling and debugging */
  char thread_name[16] = {0};
  unsigned short random_id = rand() % 10000U;
  snprintf(thread_name, sizeof(thread_name), "%s-%04u", thpool_p->name, random_id);

#if defined(__linux__)
  /* Use prctl instead to prevent using _GNU_SOURCE flag and implicit
   * declaration */
  prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
  pthread_setname_np(thread_name);
#else
  LOG_IF_EXISTS(
      "warning",
      "thread_do(): pthread_setname_np is not supported on this system")
#endif

  LOG_IF_EXISTS("verbose", "Creating background thread: %s", thread_name)

  /* Mark thread as alive (initialized) */
  thpool_p->num_threads_alive += 1;
  threadCtx thread_ctx = {.thread_state = THREAD_RUNNING};

  while (true) {
    LOG_IF_EXISTS("debug", "Thread %s is running iteration", thread_name)

    /* Read job from queue and execute it */
    priorityJobCtx job_ctx = pull_and_execute_ht[thread_ctx.thread_state](&thpool_p->jobqueues);
    if (job_ctx.job) {
      job *job_p = job_ctx.job;
      void *arg = job_p->arg;

      adminJobArg admin_job_arg = {0};
      if (job_ctx.is_admin) {
        admin_job_arg.arg = arg;
        admin_job_arg.thread_ctx = &thread_ctx;
        arg = &admin_job_arg;
      }

      job_p->function(arg);
      rm_free(job_p);

      /* These variables are atomic, so we can do this without a lock. */
      if (job_ctx.has_priority_ticket) {
        thpool_p->jobqueues.high_priority_tickets++;
      }
      thpool_p->total_jobs_done += !job_ctx.is_admin;
      thpool_p->jobqueues.num_jobs_in_progress--;
    }

    if (thread_ctx.thread_state != THREAD_RUNNING) {
      if (thread_ctx.thread_state == THREAD_TERMINATE_WHEN_EMPTY) {
        /*  We need to lock pulling from the jobqueue and update
        num_threads_alive together to make sure num_threads_alive won't
        change while we are pushing admin jobs to the queue. */
        redisearch_thpool_lock(thpool_p);
        if (priority_queue_len_unsafe(&thpool_p->jobqueues) == 0) {
          break;
        }
        redisearch_thpool_unlock(thpool_p);
      } else if (thread_ctx.thread_state == THREAD_TERMINATE_ASAP) {
        redisearch_thpool_lock(thpool_p);
        break;
      }
    }
  }

  LOG_IF_EXISTS("verbose", "Terminating thread %s", thread_name)
  thpool_p->num_threads_alive--;
  redisearch_thpool_unlock(thpool_p);

  return NULL;
}

/* ============================ JOB QUEUE =========================== */

/* Initialize queue */
static int jobqueue_init(jobqueue *jobqueue_p) {
  jobqueue_p->len = 0;
  jobqueue_p->front = NULL;
  jobqueue_p->rear = NULL;
  return 0;
}

/* Clear the queue */
static void jobqueue_clear(jobqueue *jobqueue_p) {

  while (jobqueue_p->len) {
    rm_free(jobqueue_pull(jobqueue_p));
  }
  jobqueue_p->front = NULL;
  jobqueue_p->rear = NULL;
  jobqueue_p->len = 0;
}

/* Add (allocated) chain of jobs to queue */
static void jobqueue_push_chain(jobqueue *jobqueue_p, job *first_newjob,
                                job *last_newjob, size_t num) {
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
static job *jobqueue_pull(jobqueue *jobqueue_p) {
  job *job_p = jobqueue_p->front;

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
  }

  return job_p;
}

/* Free all queue resources back to the system */
static void jobqueue_destroy(jobqueue *jobqueue_p) {
  jobqueue_clear(jobqueue_p);
}

static jobsChain create_jobs_chain(redisearch_thpool_work_t *jobs,
                                   size_t n_jobs) {
  jobsChain jobs_chain = {.first_job = NULL, .last_job = NULL};

  job *first_newjob = (job *)rm_malloc(sizeof(job));
  if (first_newjob == NULL)
    goto fail;

  jobs_chain.first_job = first_newjob;

  first_newjob->function = jobs[0].function_p;
  first_newjob->arg = jobs[0].arg_p;
  first_newjob->prev = NULL;
  job *last_newjob = first_newjob;

  for (size_t i = 1; i < n_jobs; i++) {
    job *cur_newjob = (job *)rm_malloc(sizeof(job));
    if (cur_newjob == NULL)
      goto fail;

    /* Add function and argument */
    cur_newjob->function = jobs[i].function_p;
    cur_newjob->arg = jobs[i].arg_p;
    cur_newjob->prev = NULL;

    /* Link jobs */
    last_newjob->prev = cur_newjob;
    last_newjob = cur_newjob;
  }

  jobs_chain.last_job = last_newjob;

  return jobs_chain;

fail:
  while (first_newjob) {
    job *tmp = first_newjob->prev;
    rm_free(first_newjob);
    first_newjob = tmp;
  }

  return jobs_chain;
}

/* ======================== PRIORITY QUEUE ========================== */

static int priority_queue_init(priorityJobqueue *priority_queue_p,
                               size_t num_threads,
                               size_t high_priority_bias_threshold) {

  jobqueue_init(&priority_queue_p->high_priority_jobqueue);
  jobqueue_init(&priority_queue_p->low_priority_jobqueue);
  jobqueue_init(&priority_queue_p->admin_priority_jobqueue);
  pthread_mutex_init(&priority_queue_p->lock, NULL);
  priority_queue_p->alternating_pulls = 0;
  priority_queue_p->n_high_priority_bias = high_priority_bias_threshold;
  priority_queue_p->high_priority_tickets = high_priority_bias_threshold;
  priority_queue_p->state = JOBQ_RUNNING;
  priority_queue_p->num_jobs_in_progress = 0;
  pthread_cond_init(&(priority_queue_p->has_jobs), NULL);

  return 0;
}

static void priority_queue_clear(priorityJobqueue *priority_queue_p) {
  jobqueue_clear(&priority_queue_p->high_priority_jobqueue);
  jobqueue_clear(&priority_queue_p->low_priority_jobqueue);
  jobqueue_clear(&priority_queue_p->admin_priority_jobqueue);
}

static void priority_queue_push_chain_unsafe(priorityJobqueue *priority_queue_p,
                                             job *f_newjob_p, job *l_newjob_p,
                                             size_t n,
                                             thpool_priority priority) {
  switch (priority) {
  case THPOOL_PRIORITY_HIGH:
    jobqueue_push_chain(&priority_queue_p->high_priority_jobqueue, f_newjob_p,
                        l_newjob_p, n);
    break;
  case THPOOL_PRIORITY_LOW:
    jobqueue_push_chain(&priority_queue_p->low_priority_jobqueue, f_newjob_p,
                        l_newjob_p, n);
    break;
  case THPOOL_PRIORITY_ADMIN:
    jobqueue_push_chain(&priority_queue_p->admin_priority_jobqueue, f_newjob_p,
                        l_newjob_p, n);
    break;
  }
  if (n > 1) {
    pthread_cond_broadcast(&priority_queue_p->has_jobs);
  } else {
    pthread_cond_signal(&priority_queue_p->has_jobs);
  }
}

static priorityJobCtx priority_queue_pull_no_wait(priorityJobqueue *priority_queue_p) {
  priorityJobCtx job_ctx;
  pthread_mutex_lock(&priority_queue_p->lock);
  job_ctx = priority_queue_pull_from_queues_unsafe(priority_queue_p);
  pthread_mutex_unlock(&priority_queue_p->lock);
  return job_ctx;
}
static priorityJobCtx priority_queue_pull(priorityJobqueue *priority_queue_p) {
  priorityJobCtx job_ctx;
  pthread_mutex_lock(&priority_queue_p->lock);
  while (priority_queue_len_unsafe(priority_queue_p) == 0 ||
         (priority_queue_p->state == JOBQ_PAUSED)) {
    pthread_cond_wait(&priority_queue_p->has_jobs, &priority_queue_p->lock);
  }

  job_ctx = priority_queue_pull_from_queues_unsafe(priority_queue_p);
  pthread_mutex_unlock(&priority_queue_p->lock);
  return job_ctx;
}
static inline priorityJobCtx priority_queue_pull_from_queues_unsafe(priorityJobqueue *priority_queue_p) {
  bool is_admin = true, has_priority_ticket = false;
  job *job_p = NULL;
  /* Pull from the admin queue first */
  job_p = jobqueue_pull(&priority_queue_p->admin_priority_jobqueue);

  if (!job_p) {
    is_admin = false;
    /* When taking a high priority ticket, we must hold the lock
     (read-and-then-update not atomic) */
    if (priority_queue_p->high_priority_tickets > 0) {
      /* Prefer high priority jobs, try taking from the high priority queue. */
      job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
      if (job_p) {
        has_priority_ticket = true;
        priority_queue_p->high_priority_tickets--;
      } else {
        /* If the higher priority queue is empty, pull from the low priority
        queue (without taking a ticket). */
        job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
      }
    } else {
      /* For non-bias threads, alternate between both queues every iteration */
      if (priority_queue_p->alternating_pulls % 2 == 1) {
        job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
        /* If the lower priority queue is empty, pull from the higher priority
        queue. */
        if (!job_p) {
          job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
        }
      } else {
        job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
        /* If the higher priority queue is empty, pull from the lower priority
        queue. */
        if (!job_p) {
          job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
        }
      }
      if (job_p) priority_queue_p->alternating_pulls++;
    }
  }

  /** Increasing the counter should be guarded in the same code block as pulling
   * from the queue since we may want to check the jobq length and
   * num_jobs_in_progress together. */
  if (job_p) priority_queue_p->num_jobs_in_progress++;
  priorityJobCtx job_ctx = {
      .job = job_p,
      .is_admin = is_admin,
      .has_priority_ticket = has_priority_ticket,
  };
  return job_ctx;
}

static void priority_queue_destroy(priorityJobqueue *priority_queue_p) {
  jobqueue_destroy(&priority_queue_p->high_priority_jobqueue);
  jobqueue_destroy(&priority_queue_p->low_priority_jobqueue);
  jobqueue_destroy(&priority_queue_p->admin_priority_jobqueue);
  pthread_mutex_destroy(&priority_queue_p->lock);
}

static size_t priority_queue_len(priorityJobqueue *priority_queue_p) {
  size_t len;
  pthread_mutex_lock(&priority_queue_p->lock);
  len = priority_queue_len_unsafe(priority_queue_p);
  pthread_mutex_unlock(&priority_queue_p->lock);
  return len;
}

static size_t priority_queue_len_unsafe(priorityJobqueue *priority_queue_p) {
  return priority_queue_p->high_priority_jobqueue.len +
         priority_queue_p->low_priority_jobqueue.len +
         priority_queue_p->admin_priority_jobqueue.len;
}

static bool priority_queue_is_empty(priorityJobqueue *jobqueue_p) {
  return priority_queue_len(jobqueue_p) == 0;
}

static bool priority_queue_is_empty_unsafe(priorityJobqueue *jobqueue_p) {
  return priority_queue_len_unsafe(jobqueue_p) == 0;
}

static size_t priority_queue_num_incomplete_jobs(priorityJobqueue *priority_queue_p) {
  size_t ret = 0;
  pthread_mutex_lock(&priority_queue_p->lock);
  ret = priority_queue_len_unsafe(priority_queue_p) +
        priority_queue_p->num_jobs_in_progress;
  pthread_mutex_unlock(&priority_queue_p->lock);
  return ret;
}
/* ========================== THREADS MANAGER ============================ */

static void admin_job_change_state(void *job_arg_) {
  adminJobArg *job_arg = job_arg_;
  SignalThreadCtx *signal_struct = job_arg->arg;
  ThreadState new_state = signal_struct->new_state;
  job_arg->thread_ctx->thread_state = new_state;

  /* Wait all threads to get the barrier */
  barrier_wait(signal_struct->barrier);
}
static void redisearch_thpool_broadcast_new_state(redisearch_thpool_t *thpool,
                                                  size_t n_threads,
                                                  ThreadState new_state) {
  /* Create a barrier. */
  barrier_t barrier;
  barrier_init(&barrier, NULL, n_threads);
  /* Create jobs and their args. */
  redisearch_thpool_work_t jobs[n_threads];
  SignalThreadCtx job_arg = {.barrier = &barrier, .new_state = new_state};
  /* Set new state of all threads to 'new_state'. */
  for (size_t i = 0; i < n_threads; i++) {
    jobs[i].arg_p = &job_arg;
    jobs[i].function_p = admin_job_change_state;
  }

  redisearch_thpool_add_n_work(thpool, jobs, n_threads, THPOOL_PRIORITY_ADMIN);

  /* Wait on for the threads to pass the barrier and then destroy the barrier*/
  barrier_wait_for_threads_and_destroy(&barrier);
}
