/**********************************
 * @author      Johan Hanssen Seferidis
 * License:     MIT
 *
 **********************************/

#ifndef _THPOOL_
#define _THPOOL_
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =================================== API ======================================= */

typedef struct redisearch_thpool_t* redisearch_threadpool;
typedef struct timespec timespec;

typedef enum {
  THPOOL_PRIORITY_HIGH,
  THPOOL_PRIORITY_LOW,
} thpool_priority;

/**
 * @brief  Create a new threadpool (without initializing the threads)
 *
 * @param num_threads number of threads to be created in the threadpool
 * @return Newly allocated threadpool, or NULL if creation failed.
 */
redisearch_threadpool redisearch_thpool_create(size_t num_threads);

// A callback to call redis log.
typedef void (*LogFunc)(const char *, const char *);

/**
 * @brief  Initialize an existing threadpool
 *
 * Initializes a threadpool. This function will not return until all
 * threads have initialized successfully.
 *
 * @example
 *
 *    ..
 *    threadpool thpool;                       //First we declare a threadpool
 *    thpool = thpool_create(4);               //Next we create it with 4 threads
 *    thpool_init(&thpool, logCB);             //Then we initialize the threads
 *    ..
 *
 * @param threadpool    threadpool to initialize
 * @param threadpool    callback to be called for printing debug messages to the log
 */
void redisearch_thpool_init(redisearch_threadpool, LogFunc logCB);

/**
 * @brief Add work to the job queue
 *
 * Takes an action and its argument and adds it to the threadpool's job queue.
 * If you want to add to work a function with more than one arguments then
 * a way to implement this is by passing a pointer to a structure.
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
 * @return 0 on successs, -1 otherwise.
 */
typedef void (*redisearch_thpool_proc)(void*);
int redisearch_thpool_add_work(redisearch_threadpool, redisearch_thpool_proc function_p,
                               void* arg_p, thpool_priority priority);

/**
 * @brief Add n jobs to the job queue
 *
 * Takes an action and its argument and adds it to the threadpool's job queue.
 * If you want to add to work a function with more than one arguments then
 * a way to implement this is by passing a pointer to a structure.
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
  void* arg_p;
} redisearch_thpool_work_t;
int redisearch_thpool_add_n_work(redisearch_threadpool, redisearch_thpool_work_t* jobs,
                                 size_t n_jobs, thpool_priority priority);

/**
 * @brief Wait for all queued jobs to finish
 *
 * Will wait for all jobs - both queued and currently running to finish.
 * Once the queue is empty and all work has completed, the calling thread
 * (probably the main program) will continue.
 *
 * Smart polling is used in wait. The polling is initially 0 - meaning that
 * there is virtually no polling at all. If after 1 seconds the threads
 * haven't finished, the polling interval starts growing exponentially
 * untill it reaches max_secs seconds. Then it jumps down to a maximum polling
 * interval assuming that heavy processing is being used in the threadpool.
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
 * @brief Wait until the job queue contains no more than a given number of jobs, yield periodically
 * while we wait.
 *
 * The same as redisearch_thpool_wait, but with a timeout and a threshold, so that if time passed
 * and we're still waiting, we run a yield callback function, and go back waiting again.
 * We do so until the queue contains no more than the number of jobs specified in the threshold.
 *
 * @example
 *
 *    ..
 *    threadpool thpool = thpool_create(4);
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
 *                      to finish, every <x> time (as specified in timeout).
 * @param yieldCtx      The context to send to yieldCB
 * @param threshold     The maximum number of jobs to be left in the job queue after the drain.
 * @return nothing
 */

void redisearch_thpool_drain(redisearch_threadpool, long timeout, yieldFunc yieldCB,
                                 void *yieldCtx, size_t threshold);

/**
 * @brief Pauses all threads immediately
 *
 * The threads will be paused no matter if they are idle or working.
 * The threads return to their previous states once thpool_resume
 * is called.
 *
 * While the thread is being paused, new work can be added.
 *
 * @example
 *
 *    threadpool thpool = thpool_init(4);
 *    thpool_pause(thpool);
 *    ..
 *    // Add a bunch of work
 *    ..
 *    thpool_resume(thpool); // Let the threads start their magic
 *
 * @param threadpool    the threadpool where the threads should be paused
 * @return nothing
 */
void redisearch_thpool_pause(redisearch_threadpool);

/**
 * @brief Unpauses all threads if they are paused
 *
 * @example
 *    ..
 *    thpool_pause(thpool);
 *    sleep(10);              // Delay execution 10 seconds
 *    thpool_resume(thpool);
 *    ..
 *
 * @param threadpool     the threadpool where the threads should be unpaused
 * @return nothing
 */
void redisearch_thpool_resume(redisearch_threadpool);

/**
 * @brief Terminate the working threads (without deallocating the job queue and the thread objects).
 */
void redisearch_thpool_terminate_threads(redisearch_threadpool);

/**
 * @brief Set the terminate_when_empty flag, so that all threads are terminated when there are
 * no more pending jobs in the queue.
 */
void redisearch_thpool_terminate_when_empty(redisearch_threadpool thpool_p);

/**
 * @brief Destroy the threadpool
 *
 * This will wait for the currently active threads to finish and then 'kill'
 * the whole threadpool to free up memory.
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
 *    printf("Working threads: %d\n", redisearch_thpool_num_threads_working(thpool1));
 *    ..
 *    return 0;
 * }
 *
 * @param threadpool     the threadpool of interest
 * @return integer       number of threads working
 */
size_t redisearch_thpool_num_threads_working(redisearch_threadpool);

#ifdef __cplusplus
}
#endif

#endif
