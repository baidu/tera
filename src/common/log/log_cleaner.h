// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_LOG_CLEANER_H_
#define TERA_COMMON_LOG_CLEANER_H_

#include <pthread.h>
#include <set>
#include <string>

#include "common/mutex.h"
#include "common/thread_pool.h"

namespace common {

class LogCleaner {
 private:
  // set private since singleton
  LogCleaner(const std::string& log_dir, int64_t period_second, int64_t expire_second,
             ThreadPool* thread_pool);
  ~LogCleaner();
  // disallow copy
  LogCleaner(const LogCleaner& other) = delete;
  LogCleaner& operator=(const LogCleaner& other) = delete;

 public:
  bool CheckOptions() const;
  bool Start();
  void Stop();
  bool IsRunning() const { return bg_task_id_ > 0; }

  bool AddPrefix(const std::string& prefix) {
    if (prefix.empty()) {
      // empty prefix is not allowed
      return false;
    } else {
      MutexLock l(&mutex_);
      log_prefix_list_.insert(prefix);
      return true;
    }
  }

  void RemovePrefix(const std::string& prefix) {
    MutexLock l(&mutex_);
    log_prefix_list_.erase(prefix);
  }

 private:
  // singleton
  static Mutex inst_init_mutex_;
  static LogCleaner* singleton_instance_;

  // get singleton instance but not start
  // for unittest
  static LogCleaner* GetInstance(ThreadPool* thread_pool = NULL);

 public:
  static bool StartCleaner(ThreadPool* thread_pool = NULL);
  static void StopCleaner();

 private:
  // do under lock
  void NewThreadPool() {
    if (NULL == thread_pool_) {
      thread_pool_ = new ThreadPool(1);
      thread_pool_own_ = true;
    }
  }
  void DestroyOwnThreadPool() {
    if (thread_pool_own_ && NULL != thread_pool_) {
      thread_pool_->Stop(true);
      delete thread_pool_;
      thread_pool_ = NULL;
      thread_pool_own_ = false;
    }
  }

  void CleanTaskWrap();

  bool CheckLogPrefix(const std::string& filename) const;

  bool DoCleanLocalLogs();

  bool GetCurrentOpendLogs(std::set<std::string>* opend_logs);

 private:
  ThreadPool* thread_pool_;
  bool thread_pool_own_;
  mutable Mutex mutex_;

  // options
  std::string info_log_dir_;
  std::set<std::string> log_prefix_list_;
  int64_t info_log_clean_period_ms_;  // milli second
  int64_t info_log_expire_sec_;       // second

  bool stop_;
  bool bg_exit_;
  CondVar bg_cond_;
  const ThreadPool::Task bg_func_;
  int64_t bg_task_id_;

  std::string proc_fd_path_;
};

}  // end namespace common

#endif  // TERA_COMMON_LOG_CLEANER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
