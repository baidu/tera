// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#pragma once

#include <string>
#include <vector>
#include <map>

#include "common/mutex.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/sdk_task.h"
#include "tera.h"
#include "types.h"
#include "common/timer.h"

namespace tera {

class TableImpl;

class BatchMutationImpl : public BatchMutation, public SdkTask {
 public:
  BatchMutationImpl(Table* table);
  ~BatchMutationImpl();

 public:  // from BatchMutation
  virtual void Put(const std::string& row_key, const std::string& value, int32_t ttl = -1);

  virtual void Put(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const std::string& value, int64_t timestamp = -1);

  virtual void Put(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const int64_t value, int64_t timestamp = -1);

  virtual void Add(const std::string& row_key, const std::string& family,
                   const std::string& qualifier, const int64_t delta);

  virtual void PutIfAbsent(const std::string& row_key, const std::string& family,
                           const std::string& qualifier, const std::string& value);

  virtual void Append(const std::string& row_key, const std::string& family,
                      const std::string& qualifier, const std::string& value);

  virtual void DeleteRow(const std::string& row_key, int64_t timestamp = -1);

  virtual void DeleteFamily(const std::string& row_key, const std::string& family,
                            int64_t timestamp = -1);

  virtual void DeleteColumns(const std::string& row_key, const std::string& family,
                             const std::string& qualifier, int64_t timestamp = -1);

  virtual void DeleteColumn(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, int64_t timestamp);

  virtual const ErrorCode& GetError();

  virtual void SetCallBack(Callback callback);
  virtual Callback GetCallBack();
  virtual void SetContext(void* context);
  virtual void* GetContext();

  virtual void SetTimeOut(int64_t timeout_ms);
  virtual int64_t TimeOut();

  virtual uint32_t MutationNum(const std::string& row_key);

  virtual uint32_t Size();

  virtual bool IsAsync();

  virtual void Reset();

 public:  // from SdkTask
  virtual void RunCallback();

 public:
  /// 异步操作是否完成
  bool IsFinished() const;

  /// 返回mutation
  const RowMutation::Mutation& GetMutation(const std::string& row_key, uint32_t index);

  std::vector<std::string> GetRows();

 public:
  /// 以下接口仅内部使用，不开放给用户

  void Prepare(StatCallback cb);

  int64_t GetStartTime() { return start_ts_; }

  /// 设置错误码
  void SetError(ErrorCode::ErrorCodeType err, const std::string& reason = "");

  /// 等待结束
  void Wait();

  void AddCommitTimes() { commit_times_++; }
  int64_t GetCommitTimes() { return commit_times_; }

  std::string InternalRowKey();

 protected:
  /// 增加一个操作
  RowMutation::Mutation& AddMutation(const std::string& rowkey);

 private:
  Table* table_;
  // the first row key add in this BatchMutation
  std::string update_meta_key_;
  std::map<std::string, std::vector<RowMutation::Mutation>> mu_map_;

  BatchMutation::Callback callback_;
  void* user_context_;
  int64_t timeout_ms_;

  bool finish_;
  ErrorCode error_code_;
  mutable Mutex finish_mutex_;
  common::CondVar finish_cond_;

  /// 记录此mutation被提交到ts的次数
  int64_t commit_times_;

  StatCallback on_finish_callback_;
  int64_t start_ts_;
};

}  // namespace tera
