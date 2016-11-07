// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SDK_TASK_H_
#define  TERA_SDK_SDK_TASK_H_

#include <boost/function.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/global_fun.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/indexed_by.hpp>
#include <boost/multi_index/ordered_index.hpp>

#include "common/base/stdint.h"
#include "common/mutex.h"
#include "common/thread_pool.h"

#include "proto/table_meta.pb.h"
#include "tera.h"

namespace tera {

class SdkTask {
public:
    typedef boost::function<void (SdkTask*)> TimeoutFunc;
    enum TYPE {
        READ,
        MUTATION,
        SCAN
    };
    TYPE Type() { return type_; }

    void SetInternalError(StatusCode err) { internal_err_ = err; }
    StatusCode GetInternalError() { return internal_err_; }

    void SetMetaTimeStamp(int64_t meta_ts) { meta_timestamp_ = meta_ts; }
    int64_t GetMetaTimeStamp() { return meta_timestamp_; }

    void SetId(int64_t id) { id_ = id; }
    int64_t GetId() { return id_; }

    void SetDueTime(uint64_t due_time) { due_time_ms_ = due_time; }
    uint64_t DueTime() { return due_time_ms_; }

    void SetTimeoutFunc(TimeoutFunc timeout_func) { timeout_func_ = timeout_func; }
    TimeoutFunc GetTimeoutFunc() { return timeout_func_; }

    int64_t GetRef();
    void IncRef();
    void DecRef();
    void ExcludeOtherRef();

protected:
    SdkTask(TYPE type)
        : type_(type),
          internal_err_(kTabletNodeOk),
          meta_timestamp_(0),
          id_(-1),
          due_time_ms_(UINT64_MAX),
          cond_(&mutex_),
          ref_(1) {}
    virtual ~SdkTask() {}

private:
    TYPE type_;
    StatusCode internal_err_;
    int64_t meta_timestamp_;
    int64_t id_;
    uint64_t due_time_ms_; // timestamp of timeout
    TimeoutFunc timeout_func_;

    Mutex mutex_;
    CondVar cond_;
    int64_t ref_;
};

int64_t GetSdkTaskId(SdkTask* task);

uint64_t GetSdkTaskDueTime(SdkTask* task);

class SdkTimeoutManager {
public:
    SdkTimeoutManager(ThreadPool* thread_pool);
    ~SdkTimeoutManager();

    // timeout <= 0 means NEVER timeout
    bool PutTask(SdkTask* task, int64_t timeout = 0,
                 SdkTask::TimeoutFunc timeout_func = NULL);
    SdkTask* GetTask(int64_t task_id);
    SdkTask* PopTask(int64_t task_id);

    void CheckTimeout();
    void RunTimeoutFunc(SdkTask* sdk_task);

private:
    uint32_t Shard(int64_t task_id);

private:
    const static uint32_t kShardBits = 6;
    const static uint32_t kShardNum = (1 << kShardBits);
    typedef boost::multi_index_container<
        SdkTask*,
        boost::multi_index::indexed_by<
            // hashed on SdkTask::id_
            boost::multi_index::hashed_unique<
                boost::multi_index::global_fun<SdkTask*, int64_t, &GetSdkTaskId> >,

            // sort by less<int64_t> on SdkTask::due_time_ms_
            boost::multi_index::ordered_non_unique<
                boost::multi_index::global_fun<SdkTask*, uint64_t, &GetSdkTaskDueTime> >
        >
    > TaskMap;
    enum {
        INDEX_BY_ID = 0,
        INDEX_BY_DUE_TIME = 1,
    };
    typedef TaskMap::nth_index<INDEX_BY_ID>::type TaskIdIndex;
    typedef TaskMap::nth_index<INDEX_BY_DUE_TIME>::type TaskDueTimeIndex;
    TaskMap map_shard_[kShardNum];
    mutable Mutex mutex_shard_[kShardNum];
    ThreadPool* thread_pool_;

    mutable Mutex bg_mutex_;
    bool stop_;
    bool bg_exit_;
    CondVar bg_cond_;
    int64_t bg_func_id_;
    const ThreadPool::Task bg_func_;
};

} // namespace tera

#endif  // TERA_SDK_SDK_TASK_H_
