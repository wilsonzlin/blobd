#pragma once

#include <stdarg.h>
#include <stdlib.h>
#include "exit.h"

typedef enum {
  DEBUG = 7,
  INFO = 6,
  NOTICE = 5,
  WARN = 4,
  ERR = 3,
  CRIT = 2,
  ALERT = 1,
  EMERG = 0,
} log_level_t;

// Don't use `log_direct`, instead create a wrapper called `log` in each file that provides a `subsystem` value. Note that the wrapper cannot simply be a macro as otherwise CORRUPT wouldn't work. Use LOGGER macro to declare the function.
void log_direct(char const* subsystem, log_level_t lvl, char const* format, va_list argptr);

// Avoid conflicting with builtin "log" function.
#define LOGGER(subsystem) \
  static void ts_log(log_level_t lvl, char const* format, ...) __attribute__((unused)); \
  static void ts_log(log_level_t lvl, char const* format, ...) { \
    va_list args; \
    va_start(args, format); \
    log_direct(subsystem, lvl, format, args); \
    va_end(args); \
  }

#define CORRUPT(fmt, ...) \
  ts_log(CRIT, "Corruption detected: "fmt". This may be due to bit flip/rot, external tampering, or imminent hardware failure. It may not be safe to continue using the device, so exiting now for safety.", ##__VA_ARGS__); \
  exit(EXIT_CORRUPT)
