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

#include <errno.h>
#include <fcntl.h>  // for fcntl()
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>  // for getpid()

#include <algorithm>
#include <condition_variable>
#include <iomanip>  // for std::setw()
#include <mutex>
#include <sstream>  // for std::ostringstream
#include <thread>

#include "leveldb/env.h"

namespace leveldb {

class PosixLogger : public Logger {
 private:
  std::string fname_;
  LogOption opt_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread
  FILE* file_;
  uint64_t file_length_;
  uint64_t size_since_flush_;
  uint64_t last_flush_time_ms_;
  bool bg_flush_running_;
  std::thread bg_flush_;
  std::condition_variable flush_cv_;
  std::mutex mutex_;

 public:
  PosixLogger(const std::string& fname, const LogOption& opt, uint64_t (*gettid)())
      : fname_(fname),
        opt_(opt),
        gettid_(gettid),
        file_(nullptr),
        file_length_(0),
        size_since_flush_(0),
        last_flush_time_ms_(0),
        bg_flush_running_(true) {
    assert(opt_.max_log_size > 0);
    bg_flush_ = std::thread{&PosixLogger::BGFlushWork, this};
  }

  virtual ~PosixLogger() {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (bg_flush_running_ == false) {
        return;
      }
    }
    Exit();
  }

  void Exit() {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      bg_flush_running_ = false;
    }
    flush_cv_.notify_one();
    bg_flush_.join();

    if (file_ != nullptr) {
      fflush(file_);
      fclose(file_);
      file_ = nullptr;
    }
  }

  virtual void Logv(const char* file, int64_t line, const char* format, va_list ap) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (file_length_ >= opt_.max_log_size) {
      if (file_ != nullptr) {
        fflush(file_);
        fclose(file_);
        file_ = nullptr;
        file_length_ = 0;
        size_since_flush_ = 0;
      }
    }

    if (file_ == nullptr) {
      if (!CreateLogFile()) {
        return;
      }
      // TODO
      // write a log header
    }

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
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llu %s:%ld] ",
#else
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx %s:%ld] ",
#endif
                    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id), FileBaseName(file), line);

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
          continue;  // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      mutex_.unlock();
      fwrite(base, 1, p - base, file_);
      mutex_.lock();
      file_length_ += p - base;
      size_since_flush_ += p - base;
      if (base != buffer) {
        delete[] base;
      }
      break;
    }

    if (size_since_flush_ >= opt_.flush_trigger_size) {
      mutex_.unlock();
      flush_cv_.notify_one();
      mutex_.lock();
    }
  }

 private:
  bool NeedFlush(bool time_triggered) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (size_since_flush_ == 0) {
      return false;
    }

    // triggered by size
    if (size_since_flush_ >= opt_.flush_trigger_size) {
      return true;
    }

    // triggered by time interval
    if (time_triggered) {
      return true;
    }

    return false;
  }

  void Flush() {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      size_since_flush_ = 0;
    }

    if (file_ != nullptr) {
      fflush(file_);
    }
    last_flush_time_ms_ = Env::Default()->NowMicros() / 1000;
  }

  void BGFlushWork() {
    while (bg_flush_running_) {
      uint64_t next_flush_time_ms = last_flush_time_ms_ + opt_.flush_trigger_interval_ms;
      uint64_t current_time_ms = Env::Default()->NowMicros() / 1000;
      bool time_triggered = (current_time_ms >= next_flush_time_ms) ? true : false;
      if (NeedFlush(time_triggered)) {
        Flush();
      } else {
        if (time_triggered) {
          // time triggered, but size_since_flush_ == 0, no data to flush, just refresh the flush
          // time, wait a flush_trigger_inteval
          last_flush_time_ms_ = Env::Default()->NowMicros() / 1000;
        }
        uint64_t wait_timeout_ms =
            last_flush_time_ms_ + opt_.flush_trigger_interval_ms - current_time_ms;
        // size not enough, wait for (next_flush_time_ms - current_time_ms) at most
        std::unique_lock<std::mutex> lk(mutex_);
        flush_cv_.wait_for(lk, std::chrono::milliseconds(wait_timeout_ms), [this] {
          return size_since_flush_ >= opt_.flush_trigger_size || bg_flush_running_ == false;
        });
      }
    }
  }
  const char* FileBaseName(const char* filepath) {
    const char* base = strrchr(filepath, '/');
    return base ? (base + 1) : filepath;
  }

  bool CreateLogFile() {
    std::string filename_string = fname_ + "." + GetTimePIDString();
    const char* filename = filename_string.c_str();
    int fd = open(filename, O_WRONLY | O_CREAT, 0664);
    if (fd == -1) {
      fprintf(stderr, "open failed! file:%s, errno:%d, err_msg:%s\n", filename, errno,
              strerror(errno));
      return false;
    }

    // Mark the file close-on-exec. We don't really care if this fails
    fcntl(fd, F_SETFD, FD_CLOEXEC);

    file_ = fdopen(fd, "a");  // Make a FILE*.
    if (file_ == nullptr) {   // We're screwed!
      fprintf(stderr, "fdopen failed! fd:%d, errno:%d, err_msg:%s\n", fd, errno, strerror(errno));
      close(fd);
      unlink(filename);  // Erase the half-baked evidence: an unusable log file
      return false;
    }
    fprintf(stderr, "create a new log file:%s success\n", filename);

    // We try to create a symlink called fname_,
    // which is easier to use.  (Every time we create a new logfile,
    // we destroy the old symlink and create a new one, so it always
    // points to the latest logfile.)  If it fails, we're sad but it's
    // no error.
    std::string linkpath = fname_;
    unlink(linkpath.c_str());  // delete old one if it exists

    // We must have unistd.h.
    // Make the symlink be relative (in the same dir) so that if the
    // entire log directory gets relocated the link is still valid.
    const char* slash = strrchr(filename, '/');
    const char* linkdest = slash ? (slash + 1) : filename;
    if (symlink(linkdest, linkpath.c_str()) != 0) {
      // silently ignore failures
    }
    fprintf(stderr, "create a link:%s pointed to:%s success\n", linkpath.c_str(), linkdest);

    return true;
  }

  std::string GetTimePIDString() const {
    time_t timestamp = time(nullptr);
    struct ::tm tm_time;
    localtime_r(&timestamp, &tm_time);
    std::ostringstream time_pid_stream;

    time_pid_stream.fill('0');
    time_pid_stream << 1900 + tm_time.tm_year << std::setw(2) << 1 + tm_time.tm_mon << std::setw(2)
                    << tm_time.tm_mday << '-' << std::setw(2) << tm_time.tm_hour << std::setw(2)
                    << tm_time.tm_min << std::setw(2) << tm_time.tm_sec << '.'
                    << static_cast<int32_t>(getpid());

    return time_pid_stream.str();
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
