// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  TERA_COMMON_THREAD_POOL_H_
#define  TERA_COMMON_THREAD_POOL_H_

#include <deque>
#include <map>
#include <queue>
#include <sstream>
#include <vector>
#include <boost/function.hpp>
#include "mutex.h"
#include "timer.h"

namespace common {

// An unscalable thread pool implimention.
class ThreadPool {
public:
    ThreadPool(int thread_num = 10)
        : threads_num_(thread_num),
          pending_num_(0),
          work_cv_(&mutex_),
          stop_(false),
          last_task_id_(0),
          running_task_id_(0),
          schedule_cost_sum_(0),
          schedule_count_(0),
          task_cost_sum_(0),
          task_count_(0) {
        Start();
    }
    ~ThreadPool() {
        Stop(false);
    }
    // Start a thread_num threads pool.
    bool Start() {
        MutexLock lock(&mutex_);
        if (tids_.size()) {
            return false;
        }
        stop_ = false;
        for (int i = 0; i < threads_num_; i++) {
            pthread_t tid;
            int ret = pthread_create(&tid, NULL, ThreadWrapper, this);
            if (ret) {
                abort();
            }
            tids_.push_back(tid);
        }
        return true;
    }

    // Stop the thread pool.
    // Wait for all pending task to complete if wait is true.
    bool Stop(bool wait) {
        if (wait) {
            while (pending_num_ > 0) {
                usleep(10000);
            }
        }

        {
            MutexLock lock(&mutex_);
            stop_ = true;
            work_cv_.Broadcast();
        }
        for (uint32_t i = 0; i < tids_.size(); i++) {
            pthread_join(tids_[i], NULL);
        }
        tids_.clear();
        return true;
    }

    // Task definition.
    typedef boost::function<void (int64_t)> Task;

    // Add a task to the thread pool.
    void AddTask(const Task& task) {
        MutexLock lock(&mutex_, "AddTask");
        queue_.push_back(BGItem(0, timer::get_micros(), task));
        ++pending_num_;
        work_cv_.Signal();
    }
    void AddPriorityTask(const Task& task) {
        MutexLock lock(&mutex_);
        queue_.push_front(BGItem(0, timer::get_micros(), task));
        ++pending_num_;
        work_cv_.Signal();
    }
    int64_t DelayTask(int64_t delay, const Task& task) {
        MutexLock lock(&mutex_);
        int64_t now_time = timer::get_micros();
        int64_t exe_time = now_time + delay * 1000;
        BGItem bg_item(++last_task_id_, exe_time, task);
        time_queue_.push(bg_item);
        latest_[bg_item.id] = bg_item;
        work_cv_.Signal();
        return bg_item.id;
    }
    /// Cancel a delayed task
    /// if running, wait if non_block==false; return immediately if non_block==true
    bool CancelTask(int64_t task_id, bool non_block = false, bool* is_running = NULL) {
        if (task_id == 0) {
            if (is_running != NULL) {
                *is_running = false;
            }
            return false;
        }
        while (1) {
            {
                MutexLock lock(&mutex_);
                if (running_task_id_ != task_id) {
                    BGMap::iterator it = latest_.find(task_id);
                    if (it == latest_.end()) {
                        if (is_running != NULL) {
                            *is_running = false;
                        }
                        return false;
                    }
                    latest_.erase(it);
                    return true;
                } else if (non_block) {
                    if (is_running != NULL) {
                        *is_running = true;
                    }
                    return false;
                }
            }
            timespec ts = {0, 100000};
            nanosleep(&ts, &ts);
        }
    }
    int64_t PendingNum() const {
        return pending_num_;
    }

    // log format: 3 numbers seperated by " ", e.g. "15 24 32"
    // 1st: thread pool schedule average cost (ms)
    // 2nd: user task average cost (ms)
    // 3rd: total task count since last ProfilingLog called
    std::string ProfilingLog() {
        int64_t schedule_cost_sum;
        int64_t schedule_count;
        int64_t task_cost_sum;
        int64_t task_count;
        {
            MutexLock lock(&mutex_);
            schedule_cost_sum = schedule_cost_sum_;
            schedule_cost_sum_ = 0;
            schedule_count = schedule_count_;
            schedule_count_ = 0;
            task_cost_sum = task_cost_sum_;
            task_cost_sum_ = 0;
            task_count = task_count_;
            task_count_ = 0;
        }
        std::stringstream ss;
        ss << (schedule_count == 0 ? 0 : schedule_cost_sum / schedule_count / 1000)
            << " " << (task_count == 0 ? 0 : task_cost_sum / task_count / 1000)
            << " " << task_count;
        return ss.str();
    }

private:
    ThreadPool(const ThreadPool&);
    void operator=(const ThreadPool&);

    static void* ThreadWrapper(void* arg) {
        reinterpret_cast<ThreadPool*>(arg)->ThreadProc();
        return NULL;
    }
    void ThreadProc() {
        while (true) {
            Task task;
            MutexLock lock(&mutex_, "ThreadProc");
            while (time_queue_.empty() && queue_.empty() && !stop_) {
                work_cv_.Wait("ThreadProcWait");
            }
            if (stop_) {
                break;
            }
            // Timer task
            if (!time_queue_.empty()) {
                int64_t now_time = timer::get_micros();
                BGItem bg_item = time_queue_.top();
                int64_t wait_time = (bg_item.exe_time - now_time) / 1000; // in ms
                if (wait_time <= 0) {
                    time_queue_.pop();
                    BGMap::iterator it = latest_.find(bg_item.id);
                    if (it != latest_.end() && it->second.exe_time == bg_item.exe_time) {
                        schedule_cost_sum_ += now_time - bg_item.exe_time;
                        schedule_count_++;
                        task = bg_item.task;
                        latest_.erase(it);
                        running_task_id_ = bg_item.id;
                        mutex_.Unlock();
                        task(bg_item.id);
                        task_cost_sum_ += timer::get_micros() - now_time;
                        task_count_++;
                        mutex_.Lock("ThreadProcRelock");
                        running_task_id_ = 0;
                    }
                    continue;
                } else if (queue_.empty() && !stop_) {
                    work_cv_.TimeWait(wait_time, "ThreadProcTimeWait");
                    continue;
                }
            }
            // Normal task;
            if (!queue_.empty()) {
                task = queue_.front().task;
                int64_t exe_time = queue_.front().exe_time;
                queue_.pop_front();
                --pending_num_;
                int64_t start_time = timer::get_micros();
                schedule_cost_sum_ += start_time - exe_time;
                schedule_count_++;
                mutex_.Unlock();
                task(0);
                int64_t finish_time = timer::get_micros();
                task_cost_sum_ += finish_time - start_time;
                task_count_++;
                mutex_.Lock("ThreadProcRelock2");
            }
        }
    }

private:
    struct BGItem {
        int64_t id;
        int64_t exe_time;
        Task task;
        bool operator<(const BGItem& item) const {
            if (exe_time != item.exe_time) {
                return exe_time > item.exe_time;
            } else {
                return id > item.id;
            }
        }

        BGItem() {}
        BGItem(int64_t id_t, int64_t exe_time_t, const Task& task_t)
            : id(id_t), exe_time(exe_time_t), task(task_t) {}
    };
    typedef std::priority_queue<BGItem> BGQueue;
    typedef std::map<int64_t, BGItem> BGMap;

    int32_t threads_num_;
    std::deque<BGItem> queue_;
    volatile int pending_num_;
    Mutex mutex_;
    CondVar work_cv_;
    bool stop_;
    std::vector<pthread_t> tids_;

    BGQueue time_queue_;
    BGMap latest_;
    int64_t last_task_id_;
    int64_t running_task_id_;

    // for profiling
    int64_t schedule_cost_sum_;
    int64_t schedule_count_;
    int64_t task_cost_sum_;
    int64_t task_count_;
};

} // namespace common

using common::ThreadPool;

#endif  // TERA_COMMON_THREAD_POOL_H_
