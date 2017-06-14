// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_task.h"

#include <glog/logging.h>

#include "utils/timer.h"

DECLARE_int32(tera_sdk_timeout_precision);

namespace tera {

int64_t SdkTask::GetRef() {
    MutexLock l(&mutex_);
    return ref_;
}

void SdkTask::IncRef() {
    MutexLock l(&mutex_);
    ++ref_;
}

void SdkTask::DecRef() {
    MutexLock l(&mutex_);
    CHECK_GT(ref_, 1);
    if (--ref_ == 1) {
        cond_.Signal();
    }
}

void SdkTask::ExcludeOtherRef() {
    MutexLock l(&mutex_);
    while (ref_ > 1) {
        cond_.Wait();
    }
    CHECK_EQ(ref_, 1);
}

SdkTimeoutManager::SdkTimeoutManager(ThreadPool* thread_pool)
    : thread_pool_(thread_pool),
      timeout_precision_(FLAGS_tera_sdk_timeout_precision),
      stop_(false),
      bg_exit_(false),
      bg_cond_(&bg_mutex_),
      bg_func_id_(0),
      bg_func_(std::bind(&SdkTimeoutManager::CheckTimeout, this)) {
    if (timeout_precision_ <= 0) {
        timeout_precision_ = 1;
    }
    if (timeout_precision_ > 1000) {
        timeout_precision_ = 1000;
    }
    bg_func_id_ = thread_pool_->DelayTask(timeout_precision_, bg_func_);
}

SdkTimeoutManager::~SdkTimeoutManager() {
    MutexLock l(&bg_mutex_);
    stop_ = true;
    if (bg_func_id_ > 0) {
        bool non_block = true;
        bool is_running = false;
        if (thread_pool_->CancelTask(bg_func_id_, non_block, &is_running)) {
            bg_exit_ = true;
        } else {
            CHECK(is_running);
        }
    }
    while (!bg_exit_) {
        bg_cond_.Wait();
    }
}

bool SdkTimeoutManager::PutTask(SdkTask* task, int64_t timeout,
                                SdkTask::TimeoutFunc timeout_func) {
    int64_t task_id = task->GetId();
    CHECK_GE(task_id, 0);
    if (timeout > 0) {
        task->SetDueTime(get_millis() + timeout);
        task->SetTimeoutFunc(timeout_func);
    }

    uint32_t shard_id = Shard(task_id);
    TaskMap& map = map_shard_[shard_id];
    Mutex& mutex = mutex_shard_[shard_id];

    MutexLock l(&mutex);
    std::pair<IdHashMap::iterator, bool> insert_ret;
    insert_ret = map.id_hash_map.insert(std::pair<int64_t, SdkTask*>(task_id, task));
    bool insert_success = insert_ret.second;
    if (insert_success) {
        map.due_time_map.insert(task);
        task->IncRef();
    }
    return insert_success;
}

SdkTask* SdkTimeoutManager::GetTask(int64_t task_id) {
    uint32_t shard_id = Shard(task_id);
    TaskMap& map = map_shard_[shard_id];
    Mutex& mutex = mutex_shard_[shard_id];

    MutexLock l(&mutex);
    IdHashMap::iterator it = map.id_hash_map.find(task_id);
    if (it != map.id_hash_map.end()) {
        SdkTask* task = it->second;
        CHECK_EQ(task->GetId(), task_id);
        task->IncRef();
        return task;
    } else {
        return NULL;
    }
}

SdkTask* SdkTimeoutManager::PopTask(int64_t task_id) {
    uint32_t shard_id = Shard(task_id);
    TaskMap& map = map_shard_[shard_id];
    Mutex& mutex = mutex_shard_[shard_id];

    MutexLock l(&mutex);
    IdHashMap::iterator it = map.id_hash_map.find(task_id);
    if (it != map.id_hash_map.end()) {
        SdkTask* task = it->second;
        CHECK_EQ(task->GetId(), task_id);
        map.id_hash_map.erase(it);
        map.due_time_map.erase(task);
        return task;
    } else {
        return NULL;
    }
}

void SdkTimeoutManager::CheckTimeout() {
    int64_t now_ms = get_millis();
    for (uint32_t shard_id = 0; shard_id < kShardNum; shard_id++) {
        TaskMap& map = map_shard_[shard_id];
        Mutex& mutex = mutex_shard_[shard_id];

        MutexLock l(&mutex);
        while (!map.due_time_map.empty()) {
            DueTimeMap::iterator it = map.due_time_map.begin();
            SdkTask* task = *it;
            if (task->DueTime() > (uint64_t)now_ms) {
                break;
            }
            map.due_time_map.erase(it);
            map.id_hash_map.erase(task->GetId());
            mutex.Unlock();
            thread_pool_->AddTask(std::bind(&SdkTimeoutManager::RunTimeoutFunc, this, task));
            mutex.Lock();
        }
    }

    MutexLock l(&bg_mutex_);
    if (stop_) {
        bg_exit_ = true;
        bg_cond_.Signal();
        return;
    }

    bg_func_id_ = thread_pool_->DelayTask(timeout_precision_, bg_func_);
}

void SdkTimeoutManager::RunTimeoutFunc(SdkTask* sdk_task) {
    sdk_task->GetTimeoutFunc()(sdk_task);
}

uint32_t SdkTimeoutManager::Shard(int64_t task_id) {
    return (uint64_t)task_id & ((1ull << kShardBits) - 1);
}

} // namespace tera
