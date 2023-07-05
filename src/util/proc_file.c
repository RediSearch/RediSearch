
#if defined(__linux__)

#include "util/proc_file.h"

#include <sys/types.h>
#include <dirent.h>

#include "util/arr.h"
#include "redismodule.h"

#define TIDS_INITIAL_SIZE 50
#define MAX_BUFF_LENGTH 256

pid_t *ProcFile_send_signal_to_all_threads(pid_t pid, pid_t caller_tid, int sig_num) {
  // Initialize the path the process threads' directory.
  char path_buff[MAX_BUFF_LENGTH];
  sprintf(path_buff, "/proc/%d/task", pid);

  // Get the directory handler.
  DIR *dir;
  if ((dir = opendir(path_buff)) == NULL) {
    RedisModule_Log(NULL, "notice", "ProcFile_send_signal_to_all_threads: failed to open %s directory",
                    path_buff);
    return NULL;
  }

  pid_t *tids = array_new(pid_t, TIDS_INITIAL_SIZE);
  struct dirent *entry;
  // Iterate all the entries in the directory.
  while ((entry = readdir(dir)) != NULL) {
    // Each thread is represented by a directory
    if (entry->d_type == DT_DIR) {
      // Skip irrelevant directories.
      if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
        // A thread directory name is equivalent to its tid.
        pid_t tid = atoi(entry->d_name);
        // Skip current thread
        if (tid != caller_tid) {
          tgkill(pid, tid, sig_num);
          array_ensure_append_1(tids, tid);
        }
      }
    }
  }

  closedir(dir);

  return tids;
}

// TODO: consider saving the line number to avoid strncmp for next threads.
thread_signals_mask ProcFile_get_signals_masks(pid_t pid, pid_t tid, int *status) {

  thread_signals_mask ret = {0};

  // go to the threads status file
  char path_buff[MAX_BUFF_LENGTH];
  sprintf(path_buff, "/proc/%d/task/%d/status", pid, tid);
  FILE *thread_status_file = fopen(path_buff, "r");
  if (thread_status_file == NULL) {
    RedisModule_Log(NULL, "notice", "fopen() error: can't open %s", path_buff);
		*status = REDISMODULE_ERR;
    return ret;
  }

  char line[MAX_BUFF_LENGTH];

	size_t field_name_len = strlen("SigBlk:");
  while (fgets(line, MAX_BUFF_LENGTH, thread_status_file)) {
    // iterate the file until we reach SigBlk field line
    if (!strncmp(line, "SigBlk:", field_name_len)) {
			ret.sigBlk = strtoul(line + field_name_len, NULL, 16);

			// get the next line, which contains SigIgn
			char *line_ret = fgets(line, MAX_BUFF_LENGTH, thread_status_file);
			ret.sigIgn = strtoul(line + field_name_len, NULL, 16);
			break;
    }
  }

	return ret;
}

int ProcFile_get_thread_name(pid_t pid, pid_t tid, char *name_output_buff) {

  thread_signals_mask ret = {0};

  // go to the threads stat file
  char path_buff[MAX_BUFF_LENGTH];
  sprintf(path_buff, "/proc/%d/task/%d/stat", pid, tid);
  FILE *thread_stat_file = fopen(path_buff, "r");
  if (thread_stat_file == NULL) {
    RedisModule_Log(NULL, "notice", "fopen() error: can't open %s", path_buff);
		return REDISMODULE_ERR;
  }

  char line[MAX_BUFF_LENGTH];

	// skip the first entry of the file
	int unused;

	int output_items_cnt = fscanf(thread_stat_file, "%d %s", &unused, name_output_buff);
  if (output_items_cnt != 2) {
    RedisModule_Log(NULL, "notice", "failed to read name out of %s file", path_buff);
		return REDISMODULE_ERR;
  }

	return REDISMODULE_OK;
}
#endif // defined(__linux__)
