// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/sdk_task.h"

#include <glog/logging.h>

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

bool SdkTaskHashMap::PutTask(SdkTask* task) {
    int64_t task_id = task->GetId();
    CHECK_GE(task_id, 0);

    uint32_t shard_id = Shard(task_id);
    TaskHashMap& map = _map_shard[shard_id];
    Mutex& mutex = _mutex_shard[shard_id];

    MutexLock l(&mutex);
    std::pair<TaskHashMap::iterator, bool> insert_ret;
    insert_ret = map.insert(std::pair<int64_t, SdkTask*>(task_id, task));
    bool insert_success = insert_ret.second;
    if (insert_success) {
        task->IncRef();
    }
    return insert_success;
}

SdkTask* SdkTaskHashMap::GetTask(int64_t task_id) {
    uint32_t shard_id = Shard(task_id);
    TaskHashMap& map = _map_shard[shard_id];
    Mutex& mutex = _mutex_shard[shard_id];

    MutexLock l(&mutex);
    TaskHashMap::iterator it = map.find(task_id);
    if (it != map.end()) {
        SdkTask* task = it->second;
        CHECK_EQ(task->GetId(), task_id);
        task->IncRef();
        return task;
    } else {
        return NULL;
    }
}

SdkTask* SdkTaskHashMap::PopTask(int64_t task_id) {
    uint32_t shard_id = Shard(task_id);
    TaskHashMap& map = _map_shard[shard_id];
    Mutex& mutex = _mutex_shard[shard_id];

    MutexLock l(&mutex);
    TaskHashMap::iterator it = map.find(task_id);
    if (it != map.end()) {
        SdkTask* task = it->second;
        CHECK_EQ(task->GetId(), task_id);
        map.erase(it);
        return task;
    } else {
        return NULL;
    }
}

uint32_t SdkTaskHashMap::Shard(int64_t task_id) {
    return (uint64_t)task_id >> (64 - kShardBits);
}

} // namespace tera
