#pragma once
#if defined(__linux__)

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
typedef unsigned long int signal_mask;

typedef struct {
    signal_mask sigBlk;
    signal_mask sigIgn;
} thread_signals_mask;
/**
 * send signal sig_num to all threads known to the process except the main thread
 * and the calling thread.
 *
 * @param pid   the process pid, also the main thread tid
 * @param caller_tid   the calling thread tid
 * @param sig_num the signal to send all the threads except pid and caller_id
 *
 * @return returns array of all the threads that were signaled.
 * NOTE: The function doesn't check the signal wasn't ignored or blocked by the threads.
 *
 * the tids array is freed by calling array_free function
*/
pid_t *ProcFile_send_signal_to_all_threads(pid_t pid, pid_t caller_tid, int sig_num);

/**
 * Get the signals mask described in struct thread_signals_mask.
 *
 * @param pid   the process id
 * @param tid   the thread of interest
 * @param status    output status to be updated in case of error
 *
 * @return On success, the function returns the struct containing the masks. On error
 * status will be updated to REDISMODULE_ERR.
*/
thread_signals_mask ProcFile_get_signals_masks(pid_t pid, pid_t tid, int *status);

/**
 * Get the signals mask described in struct thread_signals_mask.
 *
 * @param pid   the process id
 * @param tid   the thread of interest
 * @param name_output_buff  a buffer to write the name in.
 *
 * @return On success, the function returns REDISMODULE_OK and name_output_buff contains the
 * thread's name. Otherwise, REDISMODULE_ERR is returned and name_output_buff remains unchanged.
*/
int ProcFile_get_thread_name(pid_t pid, pid_t tid, char *name_output_buff);
#endif // defined(__linux__)
