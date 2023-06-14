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
 *    thpool_init(&thpool);                    //Then we initialize the threads
 *    ..
 *
 * @param threadpool    threadpool to initialize
 */
void redisearch_thpool_init(redisearch_threadpool);

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
 * @brief Wait for all queued jobs to finish, yield periodically while we wait.
 *
 * The same as redisearch_thpool_wait, but with a timeout, so that if time passed and
 * we're still waiting, we run a yield callback function, and go back waiting again.
 * We do so until the queue is empty and all work has completed.
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
 *    redisearch_thpool_timedwait(&thpool, time_to_wait, yieldCallback, ctx);
 *
 *    puts("All added work has finished");
 *    ..
 *
 * @param threadpool    the threadpool to wait for it to finish
 * @param timeout       indicates the time in ms to wait before we wake up and call yieldCB
 * @param yieldCB       A callback to be called periodically whenever we wait for the jobs
 *                      to finish, every <x> time (as specified in timeout).
 * @param yieldCtx      The context to send to yieldCB
 * @return nothing
 */

void redisearch_thpool_timedwait(redisearch_threadpool, long timeout, yieldFunc yieldCB,
                                 void *yieldCtx);

/**
 * @brief Pauses all threads immediately
 *
 * The threads will be signaled no matter if they are idle or working.
 * NOTE: The signal kills the thread, meaning it won't continue its execution after being paused.
 * Calling resume is necessary to finish handling the signal.
 *
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

/* ============ CRASH LOG API ============ */
/**
 * @brief Pause the threadpool for crash report
 * 
 * @param threadpool     the threadpool where the threads should be paused
 * @return nothing
 */
void redisearch_thpool_pause_before_dump(redisearch_threadpool);
/* ====== EXAMPLE OUTPUT ON CRASH ====== 

        # search_=== GC THREADS LOG: ===

        # search_thread #0 backtrace: 

        search_0:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b7698) [0xffffaf827698]
        search_1:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b72b4) [0xffffaf8272b4]
        search_2:linux-vdso.so.1(__kernel_rt_sigreturn+0) [0xffffb0ec2790]
        search_3:/lib/aarch64-linux-gnu/libc.so.6(+0x79dfc) [0xffffb0cb9dfc]
        search_4:/lib/aarch64-linux-gnu/libc.so.6(pthread_cond_wait+0x208) [0xffffb0cbc8fc]
        search_5:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b7cfc) [0xffffaf827cfc]
        search_6:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b7414) [0xffffaf827414]
        search_7:/lib/aarch64-linux-gnu/libc.so.6(+0x7d5c8) [0xffffb0cbd5c8]
        search_8:/lib/aarch64-linux-gnu/libc.so.6(+0xe5d1c) [0xffffb0d25d1c]

====== END OF EXAMPLE ====== **/
/**
 * @brief Collect and print data from all the threads in the thread pool to the crash log.
 * 
 * @param threadpool            the threadpool of threads to print dump data from.
 * @param ctx                   the info ctx to print the data to.
 * @param info_section_title    the title to print before the dump log. Probably includes
 *                              the name of the thread pool. 
 * 
 */
typedef struct RedisModuleInfoCtx RedisModuleInfoCtx;
void redisearch_thpool_ShutdownLog( redisearch_threadpool,
                                    RedisModuleInfoCtx *ctx, 
                                    const char *info_section_title);


/**
 * 
 * @brief General cleanups after all the threadpools are done dumping crash data.
 * 
 * @param ctx             the info ctx to print the data to.
 * @param threadpool      the threadpool of threads to collect dump data from.
 */
void redisearch_thpool_ShutdownLog_done();
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
