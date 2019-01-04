// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "common/base/string_format.h"
#include "io/coding.h"
#include "sdk/batch_mutation_impl.h"
#include "sdk/sdk_utils.h"
#include "common/timer.h"

namespace tera {

BatchMutationImpl::BatchMutationImpl(Table* table)
    : SdkTask(SdkTask::BATCH_MUTATION),
      table_(table),
      update_meta_key_(""),
      callback_(NULL),
      user_context_(NULL),
      timeout_ms_(0),
      finish_(false),
      finish_cond_(&finish_mutex_),
      commit_times_(0),
      on_finish_callback_(NULL),
      start_ts_(get_micros()) {}

BatchMutationImpl::~BatchMutationImpl() {}

void BatchMutationImpl::Put(const std::string& row_key, const std::string& value, int32_t ttl) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kPut;
  mutation.family = "";
  mutation.qualifier = "";
  mutation.timestamp = kLatestTimestamp;
  mutation.value = value;
  mutation.ttl = ttl;
}

void BatchMutationImpl::Put(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, const std::string& value,
                            int64_t timestamp) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kPut;
  mutation.family = family;
  mutation.qualifier = qualifier;
  if (timestamp == -1) {
    mutation.timestamp = kLatestTimestamp;
  } else {
    mutation.timestamp = timestamp;
  }
  mutation.value = value;
  mutation.ttl = -1;
}

void BatchMutationImpl::Put(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, const int64_t value, int64_t timestamp) {
  std::string value_str((char*)&value, sizeof(int64_t));
  Put(row_key, family, qualifier, value_str, timestamp);
}

void BatchMutationImpl::Add(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, const int64_t delta) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  char delta_buf[sizeof(int64_t)];
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kAdd;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = get_micros();  // 为了避免retry引起的重复加，所以自带时间戳
  io::EncodeBigEndian(delta_buf, delta);
  mutation.value.assign(delta_buf, sizeof(delta_buf));
}

void BatchMutationImpl::PutIfAbsent(const std::string& row_key, const std::string& family,
                                    const std::string& qualifier, const std::string& value) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kPutIfAbsent;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = kLatestTimestamp;
  mutation.value = value;
}

void BatchMutationImpl::Append(const std::string& row_key, const std::string& family,
                               const std::string& qualifier, const std::string& value) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kAppend;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = get_micros();
  mutation.value = value;
}

void BatchMutationImpl::DeleteRow(const std::string& row_key, int64_t timestamp) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kDeleteRow;
  mutation.timestamp = (timestamp == -1 ? kLatestTimestamp : timestamp);
}

void BatchMutationImpl::DeleteFamily(const std::string& row_key, const std::string& family,
                                     int64_t timestamp) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kDeleteFamily;
  mutation.family = family;
  mutation.timestamp = (timestamp == -1 ? kLatestTimestamp : timestamp);
}

void BatchMutationImpl::DeleteColumns(const std::string& row_key, const std::string& family,
                                      const std::string& qualifier, int64_t timestamp) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kDeleteColumns;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = (timestamp == -1 ? kLatestTimestamp : timestamp);
}

/// 删除一个列的指定版本
void BatchMutationImpl::DeleteColumn(const std::string& row_key, const std::string& family,
                                     const std::string& qualifier, int64_t timestamp) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  RowMutation::Mutation& mutation = AddMutation(row_key);
  mutation.type = RowMutation::kDeleteColumn;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = (timestamp == -1 ? kLatestTimestamp : timestamp);
}

/// 获得结果错误码
const ErrorCode& BatchMutationImpl::GetError() { return error_code_; }

void BatchMutationImpl::Prepare(StatCallback cb) {
  on_finish_callback_ = cb;
  start_ts_ = get_micros();
}

void BatchMutationImpl::Reset() {
  mu_map_.clear();
  update_meta_key_ = "";
  callback_ = NULL;
  timeout_ms_ = 0;
  SdkTask::ResetRetryTimes();
  finish_ = false;
  error_code_.SetFailed(ErrorCode::kOK);
  commit_times_ = 0;
}

/// 设置异步回调, 操作会异步返回
void BatchMutationImpl::SetCallBack(BatchMutation::Callback callback) { callback_ = callback; }

/// 获得回调函数
BatchMutation::Callback BatchMutationImpl::GetCallBack() { return callback_; }

/// 设置用户上下文，可在回调函数中获取
void BatchMutationImpl::SetContext(void* context) { user_context_ = context; }

/// 获得用户上下文
void* BatchMutationImpl::GetContext() { return user_context_; }

/// 设置超时时间(只影响当前操作,不影响Table::SetWriteTimeout设置的默认写超时)
void BatchMutationImpl::SetTimeOut(int64_t timeout_ms) { timeout_ms_ = timeout_ms; }

int64_t BatchMutationImpl::TimeOut() { return timeout_ms_; }

/// 是否异步操作
bool BatchMutationImpl::IsAsync() { return (callback_ != NULL); }

/// 异步操作是否完成
bool BatchMutationImpl::IsFinished() const {
  MutexLock lock(&finish_mutex_);
  return finish_;
}

std::string BatchMutationImpl::InternalRowKey() {
  if (table_ && table_->IsHashTable()) {
    return table_->GetHashMethod()(update_meta_key_);
  }
  return update_meta_key_;
}

/// mutation数量
uint32_t BatchMutationImpl::MutationNum(const std::string& row_key) {
  return (mu_map_[row_key]).size();
}

/// mutation总大小
uint32_t BatchMutationImpl::Size() {
  uint32_t total_size = 0;
  for (const auto& mu_seq : mu_map_) {
    total_size += mu_seq.first.size();
    for (const auto& mu : mu_seq.second) {
      total_size += mu.family.size() + mu.qualifier.size() + mu.value.size() + sizeof(mu.timestamp);
    }
  }
  return total_size;
}

/// 返回mutation
const RowMutation::Mutation& BatchMutationImpl::GetMutation(const std::string& rowkey,
                                                            uint32_t index) {
  if (mu_map_.find(rowkey) == mu_map_.end() || index >= mu_map_[rowkey].size()) {
    abort();
  }
  return mu_map_[rowkey][index];
}

std::vector<std::string> BatchMutationImpl::GetRows() {
  std::vector<std::string> rows;
  for (const auto& mu_seq : mu_map_) {
    rows.emplace_back(mu_seq.first);
  }
  return rows;
}

/// 设置错误码
void BatchMutationImpl::SetError(ErrorCode::ErrorCodeType err, const std::string& reason) {
  error_code_.SetFailed(err, reason);
}

/// 等待结束
void BatchMutationImpl::Wait() {
  MutexLock lock(&finish_mutex_);
  while (!finish_) {
    finish_cond_.Wait();
  }
}

void BatchMutationImpl::RunCallback() {
  // staticstic
  if (on_finish_callback_) {
    on_finish_callback_(table_, this);
  }
  if (callback_) {
    callback_(this);
  } else {
    MutexLock lock(&finish_mutex_);
    finish_ = true;
    finish_cond_.Signal();
  }
}

RowMutation::Mutation& BatchMutationImpl::AddMutation(const std::string& row_key) {
  update_meta_key_ = (update_meta_key_ == "" ? row_key : update_meta_key_);
  std::vector<RowMutation::Mutation>& mu_seq = mu_map_[row_key];
  mu_seq.resize(mu_seq.size() + 1);
  return mu_seq.back();
}

}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
