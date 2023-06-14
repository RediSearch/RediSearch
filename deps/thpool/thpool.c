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

#if !defined(DISABLE_PRINT) || defined(THPOOL_DEBUG)
#define err(str, ...) fprintf(stderr, str, ##__VA_ARGS__)
#else
#define err(str, ...)
#endif

// Save the current threadpool.
typedef struct redisearch_thpool_t redisearch_thpool_t;
redisearch_thpool_t *curr_threadpool = NULL;
// A flag that the threads will be waiting on when paused.
static volatile int threads_on_hold;
static volatile int register_to_crash_log;
// Global number of paused threads in all threadpools.
static atomic_uint total_paused_threads = 0; 

// Save the curr backtrace buffer size (in terms of thread_bt_data struct)
// to check if we need to increase the dump container size
static volatile size_t curr_bt_buffer_size = 0;
// The number of threads in the current threadpool that are paused.
static uint32_t threads_ids; 
// The number of threads in the threadpool that done writing their current state to the dump container.
static atomic_uint threads_done_log_cnt; 

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
  ACTIVE, //TODO: print only active threads.
  NOT_ACTIVE,
  FINE,
  CRASHED,
} statusOnCrash;
struct thread_bt_data{
  int trace_size;                   /* number of address in the backtrace */
  statusOnCrash status_on_crash;    /* thead's status when the crash happened */
  char **printable_bt;              /* back trace symbols */
};

/* Threadpool flags */
#define RS_THPOOL_F_KEEP_ALIVE 0x02 /* keep pool alive */ 

#define RS_THPOOL_F_READY_TO_DUMP 0x02 /* turn on to signal the threads 
                                       they can write to the crash log buffer */

#define RS_THPOOL_F_CONTAINS_CRASHED_THREAD 0x04  /* the thread to start crash report is in 
                                                      this threadpool  */

#define RS_THPOOL_F_TERMINATE_WHEN_EMPTY 0x08  /* terminate thread when there are
                                                      no more pending jobs  */

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
};

/* ========================== PROTOTYPES ============================ */

static int thread_init(redisearch_thpool_t* thpool_p, struct thread** thread_p, int id);
static void* thread_do(struct thread* thread_p);
static void thread_hold(int sig_id);
static void thread_destroy(struct thread* thread_p);

static void log_backtrace(statusOnCrash status_on_crash);
static void thread_bt_buffer_init(uint32_t thread_id, void *bt_addresses_buf, int trace_size, statusOnCrash status_on_crash);
static void reset_dump_counters();
static void wait_for_threads_log(redisearch_thpool_t* thpool_p);

// redisearch_thpool_ShutdownLog() wraps these functions:

// Initialize all DS required to log bt of each thread in the threadpool
// and let the threads continue to dump their current state.
// The threads will not continue execution after done writing.
static void redisearch_thpool_ShutdownLog_init(redisearch_threadpool);
// Print the data collected by the threads to the crash report.
static void redisearch_thpool_ShutdownLog_print(redisearch_thpool_t* thpool_p, RedisModuleInfoCtx *ctx);
// Cleanups related to a specific threadpool dump
static void redisearch_thpool_ShutdownLog_cleanup(redisearch_threadpool);

// turn off flag 
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

/* ========================== THREADPOOL ============================ */

/* Create thread pool */
struct redisearch_thpool_t* redisearch_thpool_create(size_t num_threads) {
  threads_on_hold = 0;

  /* Make new thread pool */
  redisearch_thpool_t* thpool_p;
  thpool_p = (struct redisearch_thpool_t*)rm_calloc(1, sizeof(struct redisearch_thpool_t));
  if (thpool_p == NULL) {
    err("redisearch_thpool_create(): Could not allocate memory for thread pool\n");
    return NULL;
  }
  thpool_p->total_threads_count = num_threads;
  thpool_p->num_threads_alive = 0;
  thpool_p->num_threads_working = 0;
  thpool_p->flags = 0;

  /* Initialise the job queue */
  if(priority_queue_init(&thpool_p->jobqueue) == -1) {
    err("redisearch_thpool_create(): Could not allocate memory for job queue\n");
    rm_free(thpool_p);
    return NULL;
  }

  /* Make threads in pool */
  thpool_p->threads = (struct thread**)rm_malloc(num_threads * sizeof(struct thread*));
  if (thpool_p->threads == NULL) {
    err("redisearch_thpool_create(): Could not allocate memory for threads\n");
    priority_queue_destroy(&thpool_p->jobqueue);
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
      rm_free(thpool_p);
      return NULL;
    }
  }

  pthread_mutex_init(&(thpool_p->thcount_lock), NULL);
  pthread_cond_init(&thpool_p->threads_all_idle, NULL);

  return thpool_p;
}

/* Initialise thread pool */
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
  rm_free(thpool_p);
}

void redisearch_thpool_pause_before_dump(redisearch_thpool_t* thpool_p) {
  
  if (!thpool_p) {
    return;
  }
  // set hold flag
  threads_on_hold = 1;

  // set register_to_crash_log flag.
  register_to_crash_log = 1;

  // The threads should wait until we initialize ds and flags used to log 
  // dump info.
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);

  // Raise a signal to all the threads to check the flags above.
  redisearch_thpool_pause(thpool_p);
}

void reset_dump_counters() {
  // zero threads' ids. This number will be increased atomically by each
  // thread so it can write to its own unique idx in the output array.
  threads_ids = 0;

  // Initialize the finished threads counter
  threads_done_log_cnt = 0;
}

void redisearch_thpool_ShutdownLog( redisearch_thpool_t* thpool_p,
                                    RedisModuleInfoCtx *ctx, 
                                    const char *info_section_title) {
  if (!thpool_p) {
    return;
  }

  RedisModule_InfoAddSection(ctx, info_section_title);

  // Save all threads data
  redisearch_thpool_ShutdownLog_init(thpool_p);

  // Print the back trace of each thread
  redisearch_thpool_ShutdownLog_print(thpool_p, ctx);
  
  // cleanup
  redisearch_thpool_ShutdownLog_cleanup(thpool_p);
}

/* Initialize all DS required to log bt of each thread in the threadpool 
 * and pause the threads.
 */
static void redisearch_thpool_ShutdownLog_init(redisearch_thpool_t* thpool_p) {
	
  size_t threadpool_size = thpool_p->num_threads_alive;

  // realloc bt buffer array if needed
  if(threadpool_size > curr_bt_buffer_size) {
    printable_bt_buffer = rm_realloc(printable_bt_buffer, threadpool_size * sizeof(thread_bt_data));
    curr_bt_buffer_size = threadpool_size;
  }

  if(printable_bt_buffer == NULL) {
	  RedisModule_Log(NULL, "warning", "cant realloc printable_bt_buffer, returning with no dump.");
  }

  reset_dump_counters();

  // All the ds are ready, the threads can start writing, so turn off wait_for_crash_log_init
  thpool_p->flags |= RS_THPOOL_F_READY_TO_DUMP;

  // write the current thread bt
  if(thpool_p->flags & RS_THPOOL_F_CONTAINS_CRASHED_THREAD) {
    log_backtrace(CRASHED);
  }
}

static void wait_for_threads_log(redisearch_thpool_t* thpool_p) {
  // wait for all the threads to finish writing to the bt buffer
  size_t threadpool_size = thpool_p->num_threads_alive;

  // when threads_done_log_cnt == threadpool_size all the threads are done writing.
  while(threads_done_log_cnt != threadpool_size) {
// TODO: don't wait indefentily!
  }

  if(threads_ids != threadpool_size) {
    // log error, but anyway proceed to log and clean the data already collected
	  RedisModule_Log(NULL, "warning", "something is wrong: number of threads' log != threadpool_size. Dumping partial data");
  }
}

/* Prints the log for each thread */
static void redisearch_thpool_ShutdownLog_print(redisearch_thpool_t* thpool_p, RedisModuleInfoCtx *ctx) {

  wait_for_threads_log(thpool_p);

  // for each thread in threads_ids
  for(size_t i = 0; i < threads_ids; i++) {
    thread_bt_data curr_bt = printable_bt_buffer[i];
    char buff[100];
    if(curr_bt.status_on_crash == CRASHED) {
      sprintf(buff, "CRASHED thread #%lu backtrace: \n",i);
    } else {
      sprintf(buff, "thread #%lu backtrace: \n",i);
    }
    RedisModule_InfoAddSection(ctx, buff);

    // print the bt
    for(int j = 0; j < curr_bt.trace_size; j++) {
      sprintf(buff, "%d",j);
      RedisModule_InfoAddFieldCString(ctx, buff, curr_bt.printable_bt[j]);
    }

    // clean up inner bt strings malloc'd by backtrace_symbols()
    free(curr_bt.printable_bt);
  }
}

static void redisearch_thpool_ShutdownLog_cleanup(redisearch_thpool_t* thpool_p) {
  // clear counters and turn off flags.
  reset_dump_counters();
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_READY_TO_DUMP);
  redisearch_thpool_TURNOFF_flag(thpool_p, RS_THPOOL_F_CONTAINS_CRASHED_THREAD);
}


void redisearch_thpool_ShutdownLog_done() {
  // turn off flags
  register_to_crash_log = 0;

  total_paused_threads = 0;
  // release bt buffer
  rm_free(printable_bt_buffer);
  curr_bt_buffer_size = 0;
}

/* Pause all threads in threadpool */
void redisearch_thpool_pause(redisearch_thpool_t* thpool_p) {
  size_t n;
	pthread_t caller = pthread_self();

  // save current number of paused threads
  uint32_t prev_paused_threads = total_paused_threads;

  // save the current thpool to the global pointer
  curr_threadpool = thpool_p;

  size_t thread_pool_size = thpool_p->num_threads_alive;

  int called_by_threadpool = 0;

	for(n = 0; n < thread_pool_size; n++) {
		// do not pause caller
		if(thpool_p->threads[n]->pthread != caller) {
			pthread_kill(thpool_p->threads[n]->pthread, SIGUSR2);
		} else {
      // The calling thread belongs to this thread pool
      called_by_threadpool = 1;
      thpool_p->flags |= RS_THPOOL_F_CONTAINS_CRASHED_THREAD;
    }
	}  

  if(thread_pool_size) {
    // wait for all the threads in the thpool to be paused (except for the caller)
    uint32_t expected_new_paused_count = prev_paused_threads + thread_pool_size - called_by_threadpool;
    while(total_paused_threads < expected_new_paused_count){
      // TODO: dont wait idefently!
    }
  }
}

/* Resume all threads in threadpool */
void redisearch_thpool_resume(redisearch_thpool_t* thpool_p) {
  // resuming a single threadpool hasn't been
  // implemented yet, meanwhile this supresses
  // the warnings
  (void)thpool_p;

  threads_on_hold = 0;
}

size_t redisearch_thpool_num_threads_working(redisearch_thpool_t* thpool_p) {
  pthread_mutex_lock(&thpool_p->thcount_lock);
  int res = thpool_p->num_threads_working;
  pthread_mutex_unlock(&thpool_p->thcount_lock);
  return res;
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
  redisearch_thpool_t* threadpool = curr_threadpool;

  // increase number of paused threads
  ++total_paused_threads;

 // If we pause to dump info on crash, wait until all data structure
 // required for the report are initalized.
  while(threads_on_hold && register_to_crash_log && !(threadpool->flags & RS_THPOOL_F_READY_TO_DUMP)) {

  } 

  // If we paused to collect crash info, this call will dump the thread's backtrace.
  log_backtrace(FINE);
    
  while (threads_on_hold) {
    sleep(1);
  }
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

  /* Register signal handler */
  struct sigaction act;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = thread_hold;
  if (sigaction(SIGUSR2, &act, NULL) == -1) {
    err("thread_do(): cannot handle SIGUSR2");
  }

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

void thread_bt_buffer_init(uint32_t thread_id, void *bt_addresses_buf, int trace_size, statusOnCrash status_on_crash) {
    // NOTE: backtrace_symbols() returns a pointer to the array malloced by the call
    // and should be freed by us.
    printable_bt_buffer[thread_id].printable_bt = backtrace_symbols(bt_addresses_buf, trace_size);
    printable_bt_buffer[thread_id].trace_size = trace_size;
    printable_bt_buffer[thread_id].status_on_crash = status_on_crash;
}

void log_backtrace(statusOnCrash status_on_crash) {
  // if register_to_shutdown_log is on
  if(register_to_crash_log) {
    // atomically load and increase counter
    uint32_t thread_id = __atomic_fetch_add(&threads_ids, 1, __ATOMIC_RELAXED); 

    void *bt_addresses_buf[BT_BUF_SIZE];
    // Get the stack trace addresses first.
    int trace_size = backtrace(bt_addresses_buf, BT_BUF_SIZE);
    
    // Translate addresses into symbols and write them to the backtraces array.
    thread_bt_buffer_init(thread_id, bt_addresses_buf, trace_size, status_on_crash);

    // increase finished threads count.
    ++threads_done_log_cnt;
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
