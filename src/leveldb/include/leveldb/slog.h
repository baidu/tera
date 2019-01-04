// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#ifndef STORAGE_LEVELDB_UTIL_SLOG_H
#define STORAGE_LEVELDB_UTIL_SLOG_H

namespace leveldb {

enum LogLevel {
  LOG_LEVEL_FATAL = 0,
  LOG_LEVEL_ERROR = 1,
  LOG_LEVEL_WARNING = 2,
  LOG_LEVEL_NOTICE = 3,
  LOG_LEVEL_INFO = 3,
  LOG_LEVEL_TRACE = 4,
  LOG_LEVEL_DEBUG = 5,
};

LogLevel GetLogLevel();

void SetLogLevel(LogLevel level);

void LogHandler(LogLevel level, const char* filename, int line, const char* fmt, ...);

}  // namespace leveldb

#define LEVELDB_SET_LOG_LEVEL(level) ::leveldb::SetLogLevel(::leveldb::LOG_LEVEL_##level)

#define LDB_SLOG(level, fmt, arg...)                        \
  (::leveldb::LOG_LEVEL_##level > ::leveldb::GetLogLevel()) \
      ? (void)0                                             \
      : ::leveldb::LogHandler(::leveldb::LOG_LEVEL_##level, __FILE__, __LINE__, fmt, ##arg)

#define LDB_SLOG_IF(condition, level, fmt, arg...)                                        \
  !(condition) ? (void)0 : ::leveldb::log_handler(::leveldb::LOG_LEVEL_##level, __FILE__, \
                                                  __LINE__, fmt, ##arg)

#define LDB_SCHECK(expression) LDB_SLOG_IF(!(expression), FATAL, "CHECK failed: " #expression)

#define LDB_SCHECK_EQ(a, b) LDB_SCHECK((a) == (b))
#define LDB_SCHECK_NE(a, b) LDB_SCHECK((a) != (b))
#define LDB_SCHECK_LT(a, b) LDB_SCHECK((a) < (b))
#define LDB_SCHECK_LE(a, b) LDB_SCHECK((a) <= (b))
#define LDB_SCHECK_GT(a, b) LDB_SCHECK((a) > (b))
#define LDB_SCHECK_GE(a, b) LDB_SCHECK((a) >= (b))

#endif  // STORAGE_LEVELDB_UTIL_SLOG_H
