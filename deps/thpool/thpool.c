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

#define LOG_IF_EXISTS(level, str, ...) if (thpool_p->log) {thpool_p->log(level, str, ##__VA_ARGS__);}

static volatile int threads_on_hold;

/* ========================== STRUCTURES ============================ */

/* Binary semaphore */
typedef struct bsem {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int v;
  size_t total_threads_count;
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
  unsigned char n_privileged_threads; /* number of threads that always run high priority tasks */
} priority_queue;

/* Thread */
typedef struct thread {
  int id;                   /* friendly id               */
  pthread_t pthread;        /* pointer to actual thread  */
  struct redisearch_thpool_t* thpool_p; /* access to thpool          */
} thread;

/* Threadpool */
typedef struct redisearch_thpool_t {
  thread** threads;                 /* pointer to threads        */
  size_t total_threads_count;
  volatile size_t num_threads_alive;   /* threads currently alive   */
  volatile size_t num_threads_working; /* threads currently working */
  volatile int keepalive;           /* keep pool alive           */
  volatile int terminate_when_empty; /* terminate thread when there are no more pending jobs */
  pthread_mutex_t thcount_lock;     /* used for thread count etc */
  pthread_cond_t threads_all_idle;  /* signal to thpool_wait     */
  priority_queue jobqueue;          /* job queue                 */
  LogFunc log;                      /* log callback              */
} redisearch_thpool_t;

/* ========================== PROTOTYPES ============================ */

static int thread_init(redisearch_thpool_t* thpool_p, struct thread** thread_p, int id);
static void* thread_do(struct thread* thread_p);
static void thread_hold(int sig_id);
static void thread_destroy(struct thread* thread_p);

static int jobqueue_init(jobqueue* jobqueue_p);
static void jobqueue_clear(jobqueue* jobqueue_p);
static void jobqueue_push_chain(jobqueue* jobqueue_p, struct job* first_newjob, struct job* last_newjob, size_t num);
static struct job* jobqueue_pull(jobqueue* jobqueue_p);
static void jobqueue_destroy(jobqueue* jobqueue_p);

static int priority_queue_init(priority_queue* priority_queue_p, size_t n_threads,
                               size_t num_privileged_threads);
static void priority_queue_clear(priority_queue* priority_queue_p);
static void priority_queue_push_chain(priority_queue* priority_queue_p, struct job* first_newjob, struct job* last_newjob, size_t num, thpool_priority priority);
static struct job* priority_queue_pull(priority_queue* priority_queue_p, int thread_id);
static void priority_queue_destroy(priority_queue* priority_queue_p);
static size_t priority_queue_len(priority_queue* priority_queue_p);

static void bsem_init(struct bsem* bsem_p, int value, size_t n_threads);
static void bsem_reset(struct bsem* bsem_p);
static void bsem_post(struct bsem* bsem_p);
static void bsem_post_all(struct bsem* bsem_p);
static void bsem_wait(struct bsem* bsem_p);

/* ========================== THREADPOOL ============================ */

/* Create thread pool */
struct redisearch_thpool_t* redisearch_thpool_create(size_t num_threads, size_t num_privileged_threads, LogFunc log) {
  threads_on_hold = 0;

  /* Make new thread pool */
  redisearch_thpool_t* thpool_p;
  thpool_p = (struct redisearch_thpool_t*)rm_malloc(sizeof(struct redisearch_thpool_t));
  if (thpool_p == NULL) {
    if (log) log("warning", "redisearch_thpool_create(): Could not allocate memory for thread pool");
    return NULL;
  }
  thpool_p->log = log;
  thpool_p->total_threads_count = num_threads;
  thpool_p->num_threads_alive = 0;
  thpool_p->num_threads_working = 0;
  thpool_p->keepalive = 0;
  thpool_p->terminate_when_empty = 0;

  /* Initialise the job queue */
  if (num_privileged_threads > num_threads) num_privileged_threads = num_threads;
  if(priority_queue_init(&thpool_p->jobqueue, num_threads, num_privileged_threads) == -1) {
    LOG_IF_EXISTS("warning", "redisearch_thpool_create(): Could not allocate memory for job queue")
    rm_free(thpool_p);
    return NULL;
  }

  /* Make threads in pool */
  thpool_p->threads = (struct thread**)rm_malloc(num_threads * sizeof(struct thread*));
  if (thpool_p->threads == NULL) {
    LOG_IF_EXISTS("warning", "redisearch_thpool_create(): Could not allocate memory for threads")
    priority_queue_destroy(&thpool_p->jobqueue);
    rm_free(thpool_p);
    return NULL;
  }

  for (size_t i = 0; i < num_threads; i++) {
    thpool_p->threads[i] = (struct thread*)rm_malloc(sizeof(struct thread));
    if (thpool_p->threads[i] == NULL) {
	  LOG_IF_EXISTS("warning", "thread_create(): Could not allocate memory for thread")
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
  assert(thpool_p->keepalive == 0);
  thpool_p->keepalive = 1;
  thpool_p->terminate_when_empty = 0;

  /* Thread init */
  size_t n;
  for (n = 0; n < thpool_p->total_threads_count; n++) {
    thread_init(thpool_p, &thpool_p->threads[n], n);
  }
  /* Wait for threads to initialize */
  while (thpool_p->num_threads_alive != thpool_p->total_threads_count) {
  }
  LOG_IF_EXISTS("verbose", "Thread pool of size %zu created successfully",
                thpool_p->total_threads_count)
}

/* Add work to the thread pool */
int redisearch_thpool_add_work(redisearch_thpool_t* thpool_p, void (*function_p)(void*), void* arg_p, thpool_priority priority) {
  job* newjob;

  newjob = (struct job*)rm_malloc(sizeof(struct job));
  if (newjob == NULL) {
    LOG_IF_EXISTS("warning", "thpool_add_work(): Could not allocate memory for new job");
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
  LOG_IF_EXISTS("warning", "redisearch_thpool_add_n_work(): Could not allocate memory for %zu new jobs", n_jobs);
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
  thpool_p->keepalive = 0;

  /* Poll all threads and wait for them to finish */
  bsem_post_all(thpool_p->jobqueue.has_jobs);
  while (thpool_p->num_threads_alive) {
    sleep(1);
  }
}

void redisearch_thpool_terminate_when_empty(redisearch_thpool_t* thpool_p) {
  thpool_p->terminate_when_empty = 1;
  // Wake up all threads (in case threads are waiting since there are not enough jobs on the queue).
  // If there are jobs in the queue, the semaphore value should become n_jobs+n_threads, so that each thread
  // will run one more time after the job queue is empty and then terminate.
  bsem_post_all(thpool_p->jobqueue.has_jobs);
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

/* Pause all threads in threadpool */
void redisearch_thpool_pause(redisearch_thpool_t* thpool_p) {
  size_t n;
  for (n = 0; n < thpool_p->num_threads_alive; n++) {
    pthread_kill(thpool_p->threads[n]->pthread, SIGUSR2);
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
  threads_on_hold = 1;
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
	LOG_IF_EXISTS("warning", "thread_do(): pthread_setname_np is not supported on this system")
#endif

  /* Assure all threads have been created before starting serving */
  redisearch_thpool_t* thpool_p = thread_p->thpool_p;
  LOG_IF_EXISTS("verbose", "Creating background thread-%d", thread_p->id)

  /* Register signal handler */
  struct sigaction act;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = thread_hold;
  if (sigaction(SIGUSR2, &act, NULL) == -1) {
    LOG_IF_EXISTS("warning", "thread_do(): cannot handle SIGUSR1")
  }

  /* Mark thread as alive (initialized) */
  pthread_mutex_lock(&thpool_p->thcount_lock);
  thpool_p->num_threads_alive += 1;
  pthread_mutex_unlock(&thpool_p->thcount_lock);

  while (thpool_p->keepalive) {

    bsem_wait(thpool_p->jobqueue.has_jobs);
    LOG_IF_EXISTS("debug", "Thread-%d is running iteration", thread_p->id)
    if (thpool_p->keepalive) {

      pthread_mutex_lock(&thpool_p->thcount_lock);
      thpool_p->num_threads_working++;
      pthread_mutex_unlock(&thpool_p->thcount_lock);

      /* Read job from queue and execute it */
      void (*func_buff)(void*);
      void* arg_buff;
      job* job_p = priority_queue_pull(&thpool_p->jobqueue, thread_p->id);
      if (job_p) {
        func_buff = job_p->function;
        arg_buff = job_p->arg;
        func_buff(arg_buff);
        rm_free(job_p);
      }

      pthread_mutex_lock(&thpool_p->thcount_lock);
      thpool_p->num_threads_working--;
      if (thpool_p->num_threads_working == 0) {
	      LOG_IF_EXISTS("debug", "All threads are idle")
	      pthread_cond_signal(&thpool_p->threads_all_idle);
      }
	  if (priority_queue_len(&thpool_p->jobqueue) == 0 && thpool_p->terminate_when_empty) {
		  LOG_IF_EXISTS("verbose", "Job queue is empty - terminating thread %d", thread_p->id);
          thpool_p->keepalive = 0;
	  }
      pthread_mutex_unlock(&thpool_p->thcount_lock);
    }
  }
  pthread_mutex_lock(&thpool_p->thcount_lock);
  LOG_IF_EXISTS("verbose", "Terminating thread %d", thread_p->id)
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

static int priority_queue_init(priority_queue* priority_queue_p, size_t num_threads,
                               size_t num_privileged_threads) {

  priority_queue_p->has_jobs = (struct bsem*)rm_malloc(sizeof(struct bsem));
  if (priority_queue_p->has_jobs == NULL) {
    return -1;
  }
  jobqueue_init(&priority_queue_p->high_priority_jobqueue);
  jobqueue_init(&priority_queue_p->low_priority_jobqueue);
  bsem_init(priority_queue_p->has_jobs, 0, num_threads);
  pthread_mutex_init(&priority_queue_p->jobqueues_rwmutex, NULL);
  priority_queue_p->pulls = 0;
  priority_queue_p->n_privileged_threads = num_privileged_threads;
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

static struct job* priority_queue_pull(priority_queue* priority_queue_p, int thread_id) {
  struct job* job_p = NULL;
  pthread_mutex_lock(&priority_queue_p->jobqueues_rwmutex);

  if (thread_id < priority_queue_p->n_privileged_threads) {
    // This is a privileged thread id, try taking from the high priority queue.
    job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
    // If the higher priority queue is empty, pull from the low priority queue.
    if (!job_p) {
      job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
    }
  } else {
    // For non-privileged threads, alternate between both queues every iteration.
    if (priority_queue_p->pulls % 2 == 1) {
      job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
      // If the lower priority queue is empty, pull from the higher priority queue
      if (!job_p) {
        job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
      }
    } else {
      job_p = jobqueue_pull(&priority_queue_p->high_priority_jobqueue);
      // If the higher priority queue is empty, pull from the lower priority queue
      if (!job_p) {
        job_p = jobqueue_pull(&priority_queue_p->low_priority_jobqueue);
      }
    }
    priority_queue_p->pulls++;
  }
  if (priority_queue_p->high_priority_jobqueue.len ||  priority_queue_p->low_priority_jobqueue.len) {
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

/* Init semaphore */
static void bsem_init(bsem* bsem_p, int value, size_t n_threads) {
  pthread_mutex_init(&(bsem_p->mutex), NULL);
  pthread_cond_init(&(bsem_p->cond), NULL);
  bsem_p->v = value;
  bsem_p->total_threads_count = n_threads;
}

/* Reset semaphore to 0 */
static void bsem_reset(bsem* bsem_p) {
  bsem_p->v = 0;
}

/* Post to exactly one thread */
static void bsem_post(bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  bsem_p->v++;
  pthread_cond_signal(&bsem_p->cond);
  pthread_mutex_unlock(&bsem_p->mutex);
}

/* Post to all threads */
static void bsem_post_all(bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  bsem_p->v += bsem_p->total_threads_count;
  pthread_cond_broadcast(&bsem_p->cond);
  pthread_mutex_unlock(&bsem_p->mutex);
}

/* Wait on semaphore until semaphore has value 0 */
static void bsem_wait(bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  while (bsem_p->v == 0) {
    pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
  }
  bsem_p->v--;
  pthread_mutex_unlock(&bsem_p->mutex);
}
