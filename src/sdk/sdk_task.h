// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_SDK_TASK_H_
#define TERA_SDK_SDK_TASK_H_

#include <functional>
#include <set>
#include <unordered_map>

#include "common/base/stdint.h"
#include "common/mutex.h"
#include "common/thread_pool.h"

#include "proto/table_meta.pb.h"
#include "tera.h"

namespace tera {

class SdkTask {
 public:
  typedef std::function<void(SdkTask*)> TimeoutFunc;
  enum TYPE {
    READ,
    MUTATION,
    SCAN,
    TASKBATCH,
    BATCH_MUTATION,
  };
  TYPE Type() { return type_; }

  static std::string GetTypeName(TYPE type);

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

  uint32_t RetryTimes() const { return retry_times_; }
  void IncRetryTimes() { retry_times_++; }
  void ResetRetryTimes() { retry_times_ = 0; }
  void SetServerAddr(const std::string& server_addr) { server_addr_ = server_addr; }
  std::string GetServerAddr() { return server_addr_; }

  int64_t GetRef();
  void IncRef();
  void DecRef();
  void ExcludeOtherRef();

  virtual bool IsAsync() = 0;
  virtual uint32_t Size() = 0;
  virtual void SetTimeOut(int64_t timeout) = 0;
  virtual int64_t TimeOut() = 0;
  virtual void Wait() = 0;
  virtual void SetError(ErrorCode::ErrorCodeType err, const std::string& reason) = 0;

  virtual std::string InternalRowKey() = 0;

  // only for user callback
  virtual void RunCallback() = 0;

  virtual int64_t GetCommitTimes() = 0;

 protected:
  SdkTask(TYPE type)
      : type_(type),
        internal_err_(kTabletNodeOk),
        meta_timestamp_(0),
        id_(-1),
        due_time_ms_(UINT64_MAX),
        retry_times_(0),
        cond_(&mutex_),
        ref_(1),
        server_addr_("") {}
  virtual ~SdkTask() {}

 private:
  TYPE type_;
  StatusCode internal_err_;
  int64_t meta_timestamp_;
  int64_t id_;
  uint64_t due_time_ms_;  // timestamp of timeout
  TimeoutFunc timeout_func_;
  uint32_t retry_times_;

  Mutex mutex_;
  CondVar cond_;
  int64_t ref_;
  std::string server_addr_;
};

typedef void (*StatCallback)(Table* table, SdkTask* task);

struct SdkTaskDueTimeComp {
  bool operator()(SdkTask* lhs, SdkTask* rhs) {
    if (lhs->DueTime() != rhs->DueTime()) {
      return lhs->DueTime() < rhs->DueTime();
    }
    return lhs->GetId() < rhs->GetId();
  }
};

class SdkTimeoutManager {
 public:
  SdkTimeoutManager(ThreadPool* thread_pool);
  ~SdkTimeoutManager();

  // timeout <= 0 means NEVER timeout
  bool PutTask(SdkTask* task, int64_t timeout = 0, SdkTask::TimeoutFunc timeout_func = NULL);
  SdkTask* GetTask(int64_t task_id);
  SdkTask* PopTask(int64_t task_id);

  void CheckTimeout();
  void RunTimeoutFunc(SdkTask* sdk_task);

 private:
  uint32_t Shard(int64_t task_id);

 private:
  const static uint32_t kShardBits = 6;
  const static uint32_t kShardNum = (1 << kShardBits);

  typedef std::multiset<SdkTask*, SdkTaskDueTimeComp> DueTimeMap;
  typedef std::unordered_map<int64_t, SdkTask*> IdHashMap;
  struct TaskMap {
    DueTimeMap due_time_map;
    IdHashMap id_hash_map;
  };

  TaskMap map_shard_[kShardNum];
  mutable Mutex mutex_shard_[kShardNum];
  ThreadPool* thread_pool_;
  int32_t timeout_precision_;

  mutable Mutex bg_mutex_;
  bool stop_;
  bool bg_exit_;
  CondVar bg_cond_;
  int64_t bg_func_id_;
  const ThreadPool::Task bg_func_;
};

}  // namespace tera

#endif  // TERA_SDK_SDK_TASK_H_
