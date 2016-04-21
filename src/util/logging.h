#ifndef __MDMA_LOGGING__
#define __MDMA_LOGGING__

#define L_DEBUG 1
#define L_INFO 2
#define L_WARN 4
#define L_ERROR 8
#define L_TRACE 16


static int LOGGING_LEVEL = 0; 
//L_DEBUG | L_INFO



#define LG_MSG(...) fprintf(stdout, __VA_ARGS__)
#define LG_DEBUG(...) if (LOGGING_LEVEL & L_DEBUG) { LG_MSG("[DEBUG %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }
#define LG_INFO(...) if (LOGGING_LEVEL & L_INFO) { LG_MSG("[INFO %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }
#define LG_WARN(...) if (LOGGING_LEVEL & L_WARN) { LG_MSG("[WARNING %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }
#define LG_ERROR(...) if (LOGGING_LEVEL & L_ERROR) { LG_MSG("[ERROR %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }

#endif