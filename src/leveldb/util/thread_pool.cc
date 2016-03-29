// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: leiliyuan@baidu.com

#include "leveldb/env.h"
#include "util/thread_pool.h"
#include "util/mutexlock.h"
#include "port/port_posix.h"

namespace leveldb {

ThreadPool::ThreadPool()
    : total_threads_limit_(5),
      exit_all_threads_(false),
      last_item_id_(0),
      active_number_(0),
      info_log_(NULL),
      timer_cv_(&mutex_),
      work_cv_(&mutex_) {
  int err = pthread_create(&timer_id_, NULL, &ThreadPool::TimerWrapper, static_cast<void*>(this));
  assert(err == 0);
}

ThreadPool::~ThreadPool() {
  {
    MutexLock lock(&mutex_);
    assert(!exit_all_threads_);
    exit_all_threads_ = true;
    work_cv_.SignalAll();
    timer_cv_.Signal();
  }
  for (size_t i = 0; i < bg_threads_.size(); ++i) {
    pthread_join(bg_threads_[i], NULL);
  }
  pthread_join(timer_id_, NULL);
}

void* ThreadPool::TimerWrapper(void* arg) {
  static_cast<ThreadPool*>(arg)->Timer();
  return NULL;
}

void* ThreadPool::BGThreadWrapper(void* arg) {
  static_cast<ThreadPool*>(arg)->BGThread();
  return NULL;
}

void ThreadPool::SetBackgroundThreads(int num) {
  assert(num > 0);
  MutexLock lock(&mutex_);
  total_threads_limit_ = num;
}

int64_t ThreadPool::Schedule(void (*function)(void*), void* arg,
                             double priority, int64_t wait_time_millisec) {
  assert(wait_time_millisec >= 0);
  MutexLock lock(&mutex_);
  if (exit_all_threads_) {
    return 0;
  }
  int alive_number = bg_threads_.size();
  if (active_number_ == alive_number && alive_number < total_threads_limit_) {
    pthread_t t;
    pthread_create(&t, NULL, &ThreadPool::BGThreadWrapper, this);
    bg_threads_.push_back(t);
  }

  if (wait_time_millisec == 0) {
    ++pending_task_num_;
  }
  int64_t now_time = static_cast<int64_t>(Env::Default()->NowMicros() / 1000);
  int64_t exe_time = (wait_time_millisec == 0) ? 0 : now_time + wait_time_millisec;
  BGItem bg_item = {arg, function, priority, ++last_item_id_, exe_time};
  PutInQueue(bg_item, wait_time_millisec);
  return bg_item.id;
}

void ThreadPool::ReSchedule(int64_t id, double priority, int64_t wait_time_millisec) {
  MutexLock lock(&mutex_);
  BGMap::iterator it = latest_.find(id);
  if (it == latest_.end()) {
    return;
  }

  BGItem& bg_item = it->second;
  int64_t now_time = static_cast<int64_t>(Env::Default()->NowMicros() / 1000);
  int64_t exe_time = 0;
  // set exe_time to 0 if
  // the task is already in pri_queue or need to push into pri_queue
  if (bg_item.exe_time != 0 && wait_time_millisec != 0) {
    exe_time = now_time + wait_time_millisec;
  }
  if (IsLatest(bg_item, priority, exe_time)) {
    return;
  }

  if (bg_item.exe_time != 0 && exe_time == 0) {
    ++pending_task_num_;
  }
  bg_item.exe_time = exe_time;
  bg_item.priority = priority;
  PutInQueue(bg_item, exe_time);
}

int ThreadPool::GetThreadNumber() {
  MutexLock lock(&mutex_);
  return total_threads_limit_;
}

int64_t ThreadPool::GetPendingTaskNum() {
  MutexLock lock(&mutex_);
  return pending_task_num_;
}

void ThreadPool::Timer() {
  while (true) {
    BGItem bg_item;
    MutexLock lock(&mutex_);
    // wait until the next job is due
    while (!exit_all_threads_) {
      if (time_queue_.empty()) {
        timer_cv_.Wait();
        continue;
      } else {
        int64_t now_time = static_cast<int64_t>(Env::Default()->NowMicros()) / 1000;
        bg_item = time_queue_.top();
        if (now_time > bg_item.exe_time) {
          break;
        } else {
          timer_cv_.Wait(bg_item.exe_time - now_time);
        }
      }
    }
    if (exit_all_threads_) {
      break;
    }

    BGMap::iterator it = latest_.find(bg_item.id);
    if (IsLatest(it->second, bg_item.priority, bg_item.exe_time)) {
      // set time to 0 to make sure exe_time does not effect comparision
      // in pri_queue
      ++pending_task_num_;
      bg_item.exe_time = 0;
      it->second.exe_time = 0;
      pri_queue_.push(bg_item);
      work_cv_.Signal();
    }
    time_queue_.pop();
  }
}

void ThreadPool::BGThread() {
  while (true) {
    MutexLock lock(&mutex_);
    while (pri_queue_.empty() && !exit_all_threads_) {
      work_cv_.Wait();
    }
    if (exit_all_threads_) {
      break;
    }
    ++active_number_;
    BGItem bg_item = pri_queue_.top();
    pri_queue_.pop();
    BGMap::iterator it = latest_.find(bg_item.id);
    // only execute the function if the task is the latest one
    if (IsLatest(it->second, bg_item.priority, bg_item.exe_time)) {
      --pending_task_num_;
      void (*function)(void*) = bg_item.function;
      void* arg = bg_item.arg;
      latest_.erase(it);
      mutex_.Unlock();
      Log(info_log_, "[ThreadPool(%d/%d)] Do thread id = %ld score = %.2f",
          active_number_, total_threads_limit_, bg_item.id, bg_item.priority);
      (*function)(arg);
      mutex_.Lock();
    }
    --active_number_;
    if (static_cast<int>(bg_threads_.size()) > total_threads_limit_) {
      pthread_t current = pthread_self();
      ThreadVector::iterator it = bg_threads_.begin();
      while (*it != current) {
        ++it;
      }
      bg_threads_.erase(it);
      break;
    }
  }
}

void ThreadPool::PutInQueue(BGItem& bg_item, int64_t wait_time_millisec) {
  if (wait_time_millisec == 0) {
    pri_queue_.push(bg_item);
    work_cv_.Signal();
  } else {
    time_queue_.push(bg_item);
    timer_cv_.Signal();
  }
  latest_[bg_item.id] = bg_item;
}

bool ThreadPool::IsLatest(const BGItem& latest, double priority, int64_t exe_time) {
  double priority_diff = std::max(latest.priority - priority,
                                  priority - latest.priority);
  bool same_time = (latest.exe_time == exe_time);
  bool in_pri_queue = latest.exe_time == 0;
  return (priority_diff < 1e-4) && (in_pri_queue || same_time);
}

} // namespace leveldb
