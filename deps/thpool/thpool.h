/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#ifndef _THPOOL_
#define _THPOOL_
#include <stddef.h>

#define DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD 1

#ifdef __cplusplus
extern "C" {
#endif

/* ======================= API ======================= */

typedef struct redisearch_thpool_t *redisearch_threadpool;
typedef struct timespec timespec;

typedef enum {
  THPOOL_PRIORITY_HIGH,
  THPOOL_PRIORITY_LOW,
  THPOOL_PRIORITY_ADMIN,
} thpool_priority;

typedef struct {
  unsigned long total_jobs_done;
  unsigned long total_pending_jobs;
  unsigned long high_priority_pending_jobs;
  unsigned long low_priority_pending_jobs;
  unsigned long admin_priority_pending_jobs;
  unsigned long num_threads_alive;
} thpool_stats;

// A callback to call redis log.
typedef void (*LogFunc)(const char *, const char *, ...);

/**
 * @brief  Create a new threadpool (without initializing the threads)
 *
 * @param num_threads number of threads to be created in the threadpool
 * @param high_priority_bias_threshold number of high priority tasks that will be executed
 * at any given time before threads start pulling low priority jobs as well.
 * @param log callback to be called for printing debug messages to the log
 * @param name thpool identifier used to name the threads in thpool. limited to
 * 11 characters including the null terminator. Each thread will be named
 * <name>-<thread_id>. thread_id is a random number from 0 to 9,999.
 * @return Newly allocated threadpool, or NULL if creation failed.
 */
redisearch_threadpool redisearch_thpool_create(size_t num_threads,
                                               size_t high_priority_bias_threshold,
                                               LogFunc log, const char *name);

/**
 * @brief Add work to the job queue
 *
 * Takes an action and its argument and adds it to the threadpool's job queue.
 * If you want to add to work a function with more than one arguments then
 * a way to implement this is by passing a pointer to a structure.
 * This function is not thread safe.
 *
 * NOTICE: You have to cast both the function and argument to not get warnings.
 *
 * @example
 *
 *    void print_num(int num){
 *       printf("%d\n", num);
 *    }
 *
 *    int main() {
 *       ..
 *       int a = 10;
 *       thpool_add_work(thpool, (void*)print_num, (void*)a);
 *       ..
 *    }
 *
 * @param  threadpool    threadpool to which the work will be added
 * @param  function_p    pointer to function to add as work
 * @param  arg_p         pointer to an argument
 * @param  priority      priority of the work, default is high
 * @return 0 on success, -1 otherwise.
 */
typedef void (*redisearch_thpool_proc)(void *);
int redisearch_thpool_add_work(redisearch_threadpool,
                               redisearch_thpool_proc function_p, void *arg_p,
                               thpool_priority priority);

/**
 * @brief Add n jobs to the job queue
 *
 * Takes an action and its argument and adds it to the threadpool's job queue.
 * If you want to add to work a function with more than one arguments then
 * a way to implement this is by passing a pointer to a structure.
 * This function is not thread safe.
 *
 * NOTICE: You have to cast both the function and argument to not get warnings.
 *
 * @example
 *
 *    void print_num(int num){
 *       printf("%d\n", num);
 *    }
 *
 *    int main() {
 *       ..
 *       int data = {10, 20, 30};
 *       redisearch_thpool_work_t jobs[] = {{print_num, data + 0}, {print_num, data + 1}, {print_num, data + 2}};
 *
 *       thpool_add_n_work(thpool, jobs, 3, THPOOL_PRIORITY_LOW);
 *       ..
 *    }
 *
 * @param  threadpool    threadpool to which the work will be added
 * @param  function_pp   array of pointers to function to add as work
 * @param  arg_pp        array of  pointer to an argument
 * @param  n             number of elements in the array
 * @param  priority      priority of the jobs
 * @return 0 on success, -1 otherwise.
 */
typedef struct thpool_work_t {
  redisearch_thpool_proc function_p;
  void *arg_p;
} redisearch_thpool_work_t;
int redisearch_thpool_add_n_work(redisearch_threadpool,
                                 redisearch_thpool_work_t *jobs, size_t n_jobs,
                                 thpool_priority priority);

/**
 * @brief Remove threads from a threadpool
 *
 * If the threadpool in initialized, the operation will be performed immediately.
 * Otherwise, the operation will be performed when the threadpool is initialized.
 * @note calling this function after calling terminate when empty, will have no effect
 * on the current running threads.
 *
 *
 * @param threadpool     the threadpool to wait for
 * @param n_threads_to_remove     number of theads to remove
 * @return The new number of threads in the threadpool
 */
size_t redisearch_thpool_remove_threads(redisearch_threadpool, size_t n_threads_to_remove);

/**
 * @brief Wait for all queued jobs to finish
 *
 * Will wait for all jobs - both queued and currently running to finish.
 * Once the queue is empty and all work has completed, the calling thread
 * (probably the main program) will continue.
 *
 * @example
 *
 *    ..
 *    threadpool thpool = thpool_init(4);
 *    ..
 *    // Add a bunch of work
 *    ..
 *    thpool_wait(thpool);
 *    puts("All added work has finished");
 *    ..
 *
 * @param threadpool     the threadpool to wait for
 * @return nothing
 */
void redisearch_thpool_wait(redisearch_threadpool);

// A callback to be called periodically when waiting for the thread pool to finish.
typedef void (*yieldFunc)(void *);

/**
 * @brief Wait until the job queue contains no more than a given number of jobs,
 * yield periodically while we wait.
 *
 * The same as redisearch_thpool_wait, but with a timeout and a threshold, so
 * that if time passed and we're still waiting, we run a yield callback
 * function, and go back waiting again. We do so until the queue contains no
 * more than the number of jobs specified in the threshold.
 *
 * @example
 *
 *    ..
 *    threadpool thpool = thpool_create(4, 1);
 *    thpool_init(&thpool);
 *    ..
 *    // Add a bunch of work
 *    ..
 *    long time_to_wait = 100;  // 100 ms
 *    redisearch_thpool_drain(&thpool, time_to_wait, yieldCallback, ctx);
 *
 *    puts("All added work has finished");
 *    ..
 *
 * @param threadpool    the threadpool to wait for it to finish
 * @param timeout       indicates the time in ms to wait before we wake up and call yieldCB
 * @param yieldCB       A callback to be called periodically whenever we wait for the jobs
 *                      to finish, every <x> time (as specified in timeout). might be NULL.
 * @param yieldCtx      The context to send to yieldCB
 * @param threshold     The maximum number of jobs to be left in the job queue after the drain.
 * @return nothing
 */

void redisearch_thpool_drain(redisearch_threadpool, long timeout,
                             yieldFunc yieldCB, void *yieldCtx,
                             size_t threshold);

/**
 * @brief Terminate the working threads (without deallocating the threadpool members).
 */
void redisearch_thpool_terminate_threads(redisearch_threadpool);

/**
 * @brief Pause pulling from the jobq. The function returns when no jobs are in progress.
 */
void redisearch_thpool_terminate_pause_threads(redisearch_threadpool);

/**
 * @brief Resume the working threads after they were paused by
 * redisearch_thpool_terminate_pause_threads.
 */
void redisearch_thpool_resume_threads(redisearch_threadpool);

/**
 * @brief Signal all threads to terminate when there are
 * no more pending jobs in the queue.
 * NOTICE: Jobs added to the jobq after this call might not be executed.
 */
void redisearch_thpool_terminate_when_empty(redisearch_threadpool thpool_p);

/**
 * @brief Destroy the threadpool
 *
 * This will wait for the currently active threads to finish and free all the
 * threadpool resources.
 *
 * @example
 * int main() {
 *    threadpool thpool1 = thpool_init(2);
 *    threadpool thpool2 = thpool_init(2);
 *    ..
 *    thpool_destroy(thpool1);
 *    ..
 *    return 0;
 * }
 *
 * @param threadpool     the threadpool to destroy
 * @return nothing
 */
void redisearch_thpool_destroy(redisearch_threadpool);

/**
 * @brief Show currently working threads
 *
 * Working threads are the threads that are performing work (not idle).
 *
 * @example
 * int main() {
 *    threadpool thpool1 = thpool_init(2);
 *    threadpool thpool2 = thpool_init(2);
 *    ..
 *    printf("Working threads: %d\n", redisearch_thpool_num_jobs_in_progress(thpool1));
 *    ..
 *    return 0;
 * }
 *
 * @param threadpool     the threadpool of interest
 * @return integer       number of threads working
 */
size_t redisearch_thpool_num_jobs_in_progress(redisearch_threadpool);

int redisearch_thpool_paused(redisearch_threadpool);

int redisearch_thpool_is_initialized(redisearch_threadpool);

thpool_stats redisearch_thpool_get_stats(redisearch_threadpool);

#ifdef __cplusplus
}
#endif

#endif
