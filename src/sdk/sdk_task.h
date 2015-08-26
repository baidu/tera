// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SDK_TASK_H_
#define  TERA_SDK_SDK_TASK_H_

#include <boost/heap/fibonacci_heap.hpp>
#include <boost/unordered_map.hpp>

#include "common/mutex.h"

#include "proto/table_meta.pb.h"
#include "sdk/tera.h"

namespace tera {

class SdkTask {

public:
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

    void SetTimeout(int64_t timeout) { _timeout = timeout; }
    int64_t Timeout() { return _timeout; }

    bool operator<(const SdkTask& rhs) const { return _timeout < rhs._timeout; }

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
          _timeout(0),
          _cond(&_mutex),
          _ref(1) {}
    virtual ~SdkTask() {}

private:
    TYPE _type;
    StatusCode _internal_err;
    int64_t _meta_timestamp;
    int64_t _id;
    int64_t _timeout;

    Mutex _mutex;
    CondVar _cond;
    int64_t _ref;
};

class SdkTaskHashMap {
public:
    bool PutTask(SdkTask* task);
    SdkTask* GetTask(int64_t task_id);
    SdkTask* PopTask(int64_t task_id);

private:
    uint32_t Shard(int64_t task_id);

private:
    const static uint32_t kShardBits = 6;
    const static uint32_t kShardNum = (1 << kShardBits);
    typedef boost::unordered_map<int64_t, SdkTask*> TaskHashMap;
    typedef boost::heap::fibonacci_heap<SdkTask*> TaskTimeoutQueue;
    TaskHashMap _map_shard[kShardNum];
    TaskTimeoutQueue _queue_shard[kShardNum];
    mutable Mutex _mutex_shard[kShardNum];
};

} // namespace tera

#endif  // TERA_SDK_SDK_TASK_H_
