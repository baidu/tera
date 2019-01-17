// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#include "leveldb/slog.h"

#include <cstdio>
#include <cstdarg>
#include <cstdlib>

namespace leveldb {

static LogLevel s_log_level = ::leveldb::LOG_LEVEL_ERROR;

LogLevel GetLogLevel() { return s_log_level; }

void SetLogLevel(LogLevel level) { s_log_level = level; }

void LogHandler(LogLevel level, const char* filename, int line, const char* fmt, ...) {
  static const char* level_names[] = {"FATAL", "ERROR", "WARNNING", "INFO", "TRACE", "DEBUG"};
  char buf[1024];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, 1024, fmt, ap);
  va_end(ap);

  fprintf(stderr, "[LevelDB %s %s:%d] %s\n", level_names[level], filename, line, buf);
  fflush(stderr);

  if (level == ::leveldb::LOG_LEVEL_FATAL) {
    abort();
  }
}

}  // namespace leveldb
