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
#include "sdk/tera.h"

namespace tera {

class SdkTask {
public:
    typedef boost::function<void (SdkTask*)> TimeoutFunc;
    enum TYPE {
        READ,
        MUTATION,
        SCAN
    };
    TYPE Type() { return _type; }

    void SetInternalError(StatusCode err) { _internal_err = err; }
    StatusCode GetInternalError() { return _internal_err; }

    void SetMetaTimeStamp(int64_t meta_ts) { _meta_timestamp = meta_ts; }
    int64_t GetMetaTimeStamp() { return _meta_timestamp; }

    void SetId(int64_t id) { _id = id; }
    int64_t GetId() { return _id; }

    void SetDueTime(uint64_t due_time) { _due_time_ms = due_time; }
    uint64_t DueTime() { return _due_time_ms; }

    void SetTimeoutFunc(TimeoutFunc timeout_func) { _timeout_func = timeout_func; }
    TimeoutFunc GetTimeoutFunc() { return _timeout_func; }

    int64_t GetRef();
    void IncRef();
    void DecRef();
    void ExcludeOtherRef();

protected:
    SdkTask(TYPE type)
        : _type(type),
          _internal_err(kTabletNodeOk),
          _meta_timestamp(0),
          _id(-1),
          _due_time_ms(UINT64_MAX),
          _cond(&_mutex),
          _ref(1) {}
    virtual ~SdkTask() {}

private:
    TYPE _type;
    StatusCode _internal_err;
    int64_t _meta_timestamp;
    int64_t _id;
    uint64_t _due_time_ms; // timestamp of timeout
    TimeoutFunc _timeout_func;

    Mutex _mutex;
    CondVar _cond;
    int64_t _ref;
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
            // hashed on SdkTask::_id
            boost::multi_index::hashed_unique<
                boost::multi_index::global_fun<SdkTask*, int64_t, &GetSdkTaskId> >,

            // sort by less<int64_t> on SdkTask::_due_time_ms
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
    TaskMap _map_shard[kShardNum];
    mutable Mutex _mutex_shard[kShardNum];
    ThreadPool* _thread_pool;

    mutable Mutex _bg_mutex;
    bool _stop;
    bool _bg_exit;
    CondVar _bg_cond;
    int64_t _bg_func_id;
    const ThreadPool::Task _bg_func;
};

} // namespace tera

#endif  // TERA_SDK_SDK_TASK_H_
