/*
 * Test-only helper: install a SIGSEGV handler that prints a stack trace and
 * then re-raises SIGSEGV, so crashes are still visible to ctest/CI but we
 * also get diagnostics on stderr.
 *
 * The raw backtrace output can be decoded by the decode_stacktrace.sh script
 * which parses the addresses and calls addr2line.
 */

#include "common.h"

// execinfo.h (backtrace) is a glibc extension, not available on musl (Alpine)
#if defined(__APPLE__) || (defined(__unix__) && defined(__GLIBC__))

#include <execinfo.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

namespace RS {
namespace {

// Helper to silence warn_unused_result for write() - in a signal handler
// there's nothing we can do if write fails.
static inline void WriteIgnore(int fd, const void *buf, size_t count) {
  if (write(fd, buf, count)) {}
}

// Generic crash handler for C++ tests: prints a stack trace on stderr and then
// re-raises the original signal so normal crash behaviour (exit status, core
// dumps, etc.) is preserved.
void CrashSignalHandler(int sig, siginfo_t *info, void *ucontext) {
  (void)info;
  (void)ucontext;

  const char header[] =
      "=== Caught fatal signal in C++ test, stack trace ===\n";
  WriteIgnore(STDERR_FILENO, header, sizeof(header) - 1);

  void *frames[64];
  int n = backtrace(frames,
                    static_cast<int>(sizeof(frames) / sizeof(frames[0])));
  backtrace_symbols_fd(frames, n, STDERR_FILENO);

  const char footer[] = "=== End of C++ test stack trace ===\n";
  WriteIgnore(STDERR_FILENO, footer, sizeof(footer) - 1);

  // Restore default handler and re-raise the same signal to preserve normal
  // crash behaviour (signal number, potential core dumps, etc.).
  signal(sig, SIG_DFL);
  raise(sig);
}

}  // namespace

void InstallSegvStackTraceHandler() {
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = CrashSignalHandler;
  sa.sa_flags = SA_SIGINFO | SA_RESETHAND;
  sigemptyset(&sa.sa_mask);

  // Install the same crash handler for a set of common fatal signals. This
  // covers typical crash scenarios (segfaults, bus errors, abort(), illegal
  // instructions, divide-by-zero, traps used by sanitizers, etc.).
  sigaction(SIGSEGV, &sa, nullptr);
#ifdef SIGBUS
  sigaction(SIGBUS, &sa, nullptr);
#endif
#ifdef SIGABRT
  sigaction(SIGABRT, &sa, nullptr);
#endif
#ifdef SIGILL
  sigaction(SIGILL, &sa, nullptr);
#endif
#ifdef SIGFPE
  sigaction(SIGFPE, &sa, nullptr);
#endif
#ifdef SIGTRAP
  sigaction(SIGTRAP, &sa, nullptr);
#endif
}

}  // namespace RS

#else  // non-POSIX stub

namespace RS {

void InstallSegvStackTraceHandler() {
  // No-op on non-POSIX platforms.
}

}  // namespace RS

#endif
