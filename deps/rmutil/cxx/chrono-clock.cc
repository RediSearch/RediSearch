
#include <ctime>
#include <chrono>

///////////////////////////////////////////////////////////////////////////////////////////////

typedef std::chrono::time_point<std::chrono::steady_clock> steady_clock_t;

extern "C" void steady_clock_get(steady_clock_t *t) {
  *t = std::chrono::steady_clock::now();
}

extern "C" double steady_clock_diff_msec(steady_clock_t *t1, steady_clock_t *t0) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(*t1 - *t0).count();
}

extern "C" double steady_clock_since_msec(steady_clock_t *t0) {
  auto t1 = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(t1 - *t0).count();
}

extern "C" long double steady_clock_since_usec(steady_clock_t *t0) {
  auto t1 = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(t1 - *t0).count();
}

extern "C" long double steady_clock_since_nsec(steady_clock_t *t0) {
  auto t1 = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - *t0).count();
}

//---------------------------------------------------------------------------------------------

typedef std::chrono::time_point<std::chrono::high_resolution_clock> hires_clock_t;

extern "C" void hires_clock_get(hires_clock_t *t) {
  *t = std::chrono::high_resolution_clock::now();
}

extern "C" double hires_clock_diff_msec(hires_clock_t *t1, hires_clock_t *t0) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(*t1 - *t0).count();
}

extern "C" double hires_clock_since_msec(hires_clock_t *t0) {
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(t1 - *t0).count();
}

extern "C" long double hires_clock_since_usec(hires_clock_t *t0) {
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(t1 - *t0).count();
}

extern "C" long double hires_clock_since_nsec(hires_clock_t *t0) {
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - *t0).count();
}

///////////////////////////////////////////////////////////////////////////////////////////////
