// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <algorithm>
#include <set>
#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

class PosixLogger : public Logger {
 private:
  FILE* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread
 public:
  PosixLogger(FILE* f, uint64_t (*gettid)()) : file_(f), gettid_(gettid) { }
  virtual ~PosixLogger() {
    fclose(file_);
  }
  virtual void Logv(const char* format, va_list ap) {
    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, NULL);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
#ifdef OS_LINUX
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llu ",
#else
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
#endif
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      fwrite(base, 1, p - base, file_);
      fflush(file_);
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};

class EnhancePosixLogger : public Logger {
  private:
    port::Mutex mutex_;
    std::string fname_;
    uint64_t (*gettid_)();  // Return the thread id for the current thread
    int64_t log_size_;
    uint64_t log_count_;
    int64_t cur_log_size_;
    Logger* logger_;
    std::set<std::string> log_list_;
    char* logbuf_;
  public:
    EnhancePosixLogger(const std::string& fname, uint64_t (*gettid)(),
        int64_t log_size = 2000000000, int64_t log_count = 30)
      : fname_(fname),
        gettid_(gettid),
        log_size_(log_size),
        log_count_(log_count),
        cur_log_size_(0),
        logger_(NULL),
        logbuf_(new char[30000]) {}
    virtual ~EnhancePosixLogger() {
      if (logger_) {
        delete logger_;
      }
      delete[] logbuf_;
    }
    virtual void Logv(const char* format, va_list ap) {
      TrySwitchLog(format, ap);
      assert(logger_);
      logger_->Logv(format, ap);
    }

  private:
    void TrySwitchLog(const char* format, va_list ap) {
      // try switch log
      MutexLock l(&mutex_);
      if (logger_ == NULL || cur_log_size_ > log_size_) {
        const uint64_t thread_id = (*gettid_)();
        char timebuf[64];
        char* p = timebuf;

        struct timeval now_tv;
        gettimeofday(&now_tv, NULL);
        const time_t seconds = now_tv.tv_sec;
        struct tm t;
        localtime_r(&seconds, &t);
        p += snprintf(p, 64,
#ifdef OS_LINUX
            "%04d-%02d-%02d-%02d:%02d:%02d:%06d.%llu",
#else
            "%04d/%02d/%02d-%02d:%02d:%02d.%06d.%llx",
#endif
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec),
            static_cast<long long unsigned int>(thread_id));
        std::string logname(fname_ + ".");
        logname.append(timebuf);

        Logger* slog = NULL;
        FILE* f = fopen(logname.c_str(), "w");
        if (f) {
          slog = new PosixLogger(f, gettid_);
          if (logger_) {
            delete logger_;
          }
          logger_ = slog;
          cur_log_size_= 0;
          log_list_.insert(logname);

          // try delete log
          if (log_list_.size() > log_count_) {
            std::set<std::string>::iterator it = log_list_.begin();
            if (it != log_list_.end()) {
              remove(it->c_str());
            }
          } // evict oldest log
        } // new log open suc
      }

      char* pl = logbuf_;
      char* limit = logbuf_ + 30000;
      va_list backup_ap;
      va_copy(backup_ap, ap);
      pl += vsnprintf(pl, limit - pl, format, backup_ap);
      va_end(backup_ap);
      cur_log_size_ += (pl >= limit) ? 30000: (pl - logbuf_);
    }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
