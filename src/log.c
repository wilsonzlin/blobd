#include <time.h>
#include <stdarg.h>
#include <stdio.h>
#include "log.h"

void log_direct(char const* subsystem, log_level_t lvl, char const* format, va_list argptr) {
  time_t now;
  time(&now);
  char timefmtbuf[sizeof("2345-10-06T07:08:09Z")];
  strftime(timefmtbuf, sizeof(timefmtbuf), "%FT%TZ", gmtime(&now));
  fprintf(stderr, "<%d>[%s %s] ", lvl, timefmtbuf, subsystem);
  vfprintf(stderr, format, argptr);
  fprintf(stderr, "\n");
}
