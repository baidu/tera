// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_task.h"

#include <glog/logging.h>

#include "utils/timer.h"

namespace tera {

int64_t SdkTask::GetRef() {
    MutexLock l(&_mutex);
    return _ref;
}

void SdkTask::IncRef() {
    MutexLock l(&_mutex);
    ++_ref;
}

void SdkTask::DecRef() {
    MutexLock l(&_mutex);
    CHECK_GT(_ref, 1);
    if (--_ref == 1) {
        _cond.Signal();
    }
}

void SdkTask::ExcludeOtherRef() {
    MutexLock l(&_mutex);
    while (_ref > 1) {
        _cond.Wait();
    }
    CHECK_EQ(_ref, 1);
}

int64_t GetSdkTaskId(SdkTask* task) {
    return task->GetId();
}

uint64_t GetSdkTaskDueTime(SdkTask* task) {
    return task->DueTime();
}

SdkTimeoutManager::SdkTimeoutManager(ThreadPool* thread_pool)
    : _thread_pool(thread_pool) {
    CheckTimeout();
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
    TaskMap& map = _map_shard[shard_id];
    Mutex& mutex = _mutex_shard[shard_id];

    MutexLock l(&mutex);
    std::pair<TaskMap::iterator, bool> insert_ret;
    insert_ret = map.insert(task);
    bool insert_success = insert_ret.second;
    if (insert_success) {
        task->IncRef();
    }
    return insert_success;
}

SdkTask* SdkTimeoutManager::GetTask(int64_t task_id) {
    uint32_t shard_id = Shard(task_id);
    TaskMap& map = _map_shard[shard_id];
    Mutex& mutex = _mutex_shard[shard_id];

    MutexLock l(&mutex);
    TaskIdIndex& id_index = map.get<INDEX_BY_ID>();
    TaskIdIndex::iterator it = id_index.find(task_id);
    if (it != id_index.end()) {
        SdkTask* task = *it;
        CHECK_EQ(task->GetId(), task_id);
        task->IncRef();
        return task;
    } else {
        return NULL;
    }
}

SdkTask* SdkTimeoutManager::PopTask(int64_t task_id) {
    uint32_t shard_id = Shard(task_id);
    TaskMap& map = _map_shard[shard_id];
    Mutex& mutex = _mutex_shard[shard_id];

    MutexLock l(&mutex);
    TaskIdIndex& id_index = map.get<INDEX_BY_ID>();
    TaskIdIndex::iterator it = id_index.find(task_id);
    if (it != id_index.end()) {
        SdkTask* task = *it;
        CHECK_EQ(task->GetId(), task_id);
        id_index.erase(it);
        return task;
    } else {
        return NULL;
    }
}

void SdkTimeoutManager::CheckTimeout() {
    int64_t now_ms = get_millis();
    for (uint32_t shard_id = 0; shard_id < kShardNum; shard_id++) {
        TaskMap& map = _map_shard[shard_id];
        Mutex& mutex = _mutex_shard[shard_id];

        MutexLock l(&mutex);
        while (!map.empty()) {
            TaskDueTimeIndex& due_time_index = map.get<INDEX_BY_DUE_TIME>();
            TaskDueTimeIndex::iterator it = due_time_index.begin();
            SdkTask* task = *it;
            if (task->DueTime() > (uint64_t)now_ms) {
                break;
            }
            due_time_index.erase(it);
            mutex.Unlock();
            _thread_pool->AddTask(boost::bind(&SdkTimeoutManager::RunTimeoutFunc, this, task));
            mutex.Lock();
        }
    }
    if (get_millis() == now_ms) {
        _thread_pool->DelayTask(1, boost::bind(&SdkTimeoutManager::CheckTimeout, this));
    } else {
        _thread_pool->AddTask(boost::bind(&SdkTimeoutManager::CheckTimeout, this));
    }
}

void SdkTimeoutManager::RunTimeoutFunc(SdkTask* sdk_task) {
    sdk_task->GetTimeoutFunc()(sdk_task);
}

uint32_t SdkTimeoutManager::Shard(int64_t task_id) {
    return (uint64_t)task_id & ((1ull << kShardBits) - 1);
}

} // namespace tera
