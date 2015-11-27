// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: leiliyuan@baidu.com

#ifndef LEVELDB_THREAD_POOL_H_
#define LEVELDB_THREAD_POOL_H_

#include <map>
#include <queue>
#include <pthread.h>
#include "leveldb/env.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"

namespace leveldb {

class ThreadPool {
public:
  ThreadPool();
  ~ThreadPool();

  // A task will be put in queue and being processed by background thread
  // Return value is the task's id number
  int64_t Schedule(void (*function)(void*), void* arg, double priority,
                   int64_t wait_time_millisec);
  // Modify a task's priority or execute time
  void ReSchedule(int64_t id, double priority, int64_t wait_time_millisec);
  // Set background threads number. 'num' needs to greater than zero
  void SetBackgroundThreads(int num);
  // Return maximal allowed thread number
  int GetThreadNumber();
  void SetLogger(Logger* info_log) { info_log_ = info_log; }

private:
  struct BGItem {
    void* arg;
    void (*function)(void*);
    double priority;
    int64_t id;
    int64_t exe_time;
    bool operator<(const BGItem& item) const {
      if (exe_time != item.exe_time) {
        return exe_time > item.exe_time;
      } else if (priority != item.priority) {
        return priority < item.priority;
      } else {
        return id > item.id;
      }
    }
  };

  typedef std::priority_queue<BGItem> BGQueue;
  typedef std::vector<pthread_t> ThreadVector;
  typedef std::map<int64_t, BGItem> BGMap;

  void Timer();
  void BGThread();
  void PutInQueue(BGItem& bg_item, int64_t wait_time_millisec);
  bool IsLatest(const BGItem& latest, double priority, int64_t exe_time);

  static void* TimerWrapper(void* arg);
  static void* BGThreadWrapper(void* arg);

  int total_threads_limit_;
  bool exit_all_threads_;
  int64_t last_item_id_;
  int active_number_;

  Logger* info_log_;
  port::Mutex mutex_;
  port::CondVar timer_cv_;
  port::CondVar work_cv_;

  pthread_t timer_id_;
  ThreadVector bg_threads_;
  BGQueue pri_queue_;
  BGQueue time_queue_;
  BGMap latest_;
};

} // namespace leveldb

#endif // LEVELDB_THREAD_POOL_H_
