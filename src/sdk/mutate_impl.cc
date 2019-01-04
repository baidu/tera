// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/string_format.h"
#include "io/coding.h"
#include "sdk/mutate_impl.h"
#include "sdk/sdk_utils.h"
#include "common/timer.h"

namespace tera {

RowMutationImpl::RowMutationImpl(Table* table, const std::string& row_key)
    : SdkTask(SdkTask::MUTATION),
      table_(table),
      row_key_(row_key),
      callback_(NULL),
      user_context_(NULL),
      timeout_ms_(0),
      finish_(false),
      finish_cond_(&finish_mutex_),
      commit_times_(0),
      on_finish_callback_(NULL),
      start_ts_(get_micros()),
      txn_(NULL) {
  SetMutationErrorIfInvalid(row_key, FieldType::kRowkey, &error_code_);
}

RowMutationImpl::~RowMutationImpl() {}

void RowMutationImpl::Prepare(StatCallback cb) {
  on_finish_callback_ = cb;
  start_ts_ = get_micros();
}

/// 重置，复用前必须调用
void RowMutationImpl::Reset(const std::string& row_key) {
  row_key_ = row_key;
  mu_seq_.clear();
  callback_ = NULL;
  SdkTask::ResetRetryTimes();
  timeout_ms_ = 0;
  finish_ = false;
  error_code_.SetFailed(ErrorCode::kOK);
  commit_times_ = 0;
}

// 原子加一个Cell
void RowMutationImpl::AddInt64(const std::string& family, const std::string& qualifier,
                               const int64_t delta) {
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  std::string delta_str((char*)&delta, sizeof(int64_t));
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kAddInt64;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = get_micros();  // 为了避免retry引起的重复加，所以自带时间戳
  mutation.value.assign(delta_str);
}

// 原子加一个Cell
void RowMutationImpl::Add(const std::string& family, const std::string& qualifier,
                          const int64_t delta) {
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  char delta_buf[sizeof(int64_t)];
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kAdd;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = get_micros();  // 为了避免retry引起的重复加，所以自带时间戳
  io::EncodeBigEndian(delta_buf, delta);
  mutation.value.assign(delta_buf, sizeof(delta_buf));
}

// 原子操作：如果不存在才能Put成功
void RowMutationImpl::PutIfAbsent(const std::string& family, const std::string& qualifier,
                                  const std::string& value) {
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kPutIfAbsent;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = kLatestTimestamp;
  mutation.value = value;
}

void RowMutationImpl::Append(const std::string& family, const std::string& qualifier,
                             const std::string& value) {
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kAppend;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = get_micros();
  mutation.value = value;
}

/// 修改一个列
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          const int64_t value, int64_t timestamp) {
  std::string value_str((char*)&value, sizeof(int64_t));
  Put(family, qualifier, value_str, timestamp);
}

/// 带TTL修改一个列
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          const std::string& value, int32_t ttl) {
  SetMutationErrorIfInvalid(family, FieldType::kKVColumnFamily, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kKVQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kPut;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = kLatestTimestamp;
  mutation.value = value;
  mutation.ttl = ttl;
}

/// 修改一个列的特定版本
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          const std::string& value, int64_t timestamp) {
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation();
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

void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          int64_t timestamp, const std::string& value) {
  Put(family, qualifier, value, timestamp);
}

/// 带TTL的修改一个列的特定版本
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          int64_t timestamp, const std::string& value, int32_t ttl) {
  SetMutationErrorIfInvalid(family, FieldType::kKVColumnFamily, &error_code_);
  SetMutationErrorIfInvalid(qualifier, FieldType::kKVQualifier, &error_code_);
  SetMutationErrorIfInvalid(value, FieldType::kValue, &error_code_);
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kPut;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = timestamp;
  mutation.value = value;
  mutation.ttl = ttl;
}

/// 修改默认列
void RowMutationImpl::Put(const int64_t value) {
  std::string value_str((char*)&value, sizeof(int64_t));
  Put(value_str, -1);
}

/// 带TTL的修改默认列
void RowMutationImpl::Put(const std::string& value, int32_t ttl) {
  Put("", "", kLatestTimestamp, value, ttl);
}

/// 修改默认列的特定版本
void RowMutationImpl::Put(int64_t timestamp, const std::string& value) {
  Put("", "", timestamp, value);
}

/// 删除一个列的最新版本
void RowMutationImpl::DeleteColumn(const std::string& family, const std::string& qualifier) {
  DeleteColumn(family, qualifier, kLatestTimestamp);
}

/// 删除一个列的指定版本
void RowMutationImpl::DeleteColumn(const std::string& family, const std::string& qualifier,
                                   int64_t timestamp) {
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kDeleteColumn;
  mutation.family = family;
  mutation.qualifier = qualifier;
  mutation.timestamp = timestamp;
}

/// 删除一个列的指定范围版本
void RowMutationImpl::DeleteColumns(const std::string& family, const std::string& qualifier,
                                    int64_t timestamp) {
  SetMutationErrorIfInvalid(qualifier, FieldType::kQualifier, &error_code_);
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kDeleteColumns;
  mutation.family = family;
  mutation.qualifier = qualifier;
  if (timestamp == -1) {
    mutation.timestamp = kLatestTimestamp;
  } else {
    mutation.timestamp = timestamp;
  }
}

/// 删除一个列族的所有列的指定范围版本
void RowMutationImpl::DeleteFamily(const std::string& family, int64_t timestamp) {
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kDeleteFamily;
  mutation.family = family;
  if (timestamp == -1) {
    mutation.timestamp = kLatestTimestamp;
  } else {
    mutation.timestamp = timestamp;
  }
}

/// 删除整行的指定范围版本
void RowMutationImpl::DeleteRow(int64_t timestamp) {
  RowMutation::Mutation& mutation = AddMutation();
  mutation.type = RowMutation::kDeleteRow;
  if (timestamp == -1) {
    mutation.timestamp = kLatestTimestamp;
  } else {
    mutation.timestamp = timestamp;
  }
}

/// 修改锁住的行, 必须提供行锁
void RowMutationImpl::SetLock(RowLock* rowlock) {}

/// 设置超时时间(只影响当前操作,不影响Table::SetWriteTimeout设置的默认写超时)
void RowMutationImpl::SetTimeOut(int64_t timeout_ms) { timeout_ms_ = timeout_ms; }

int64_t RowMutationImpl::TimeOut() { return timeout_ms_; }

/// 设置异步回调, 操作会异步返回
void RowMutationImpl::SetCallBack(RowMutation::Callback callback) { callback_ = callback; }

/// 获得回调函数
RowMutation::Callback RowMutationImpl::GetCallBack() { return callback_; }

/// 设置用户上下文，可在回调函数中获取
void RowMutationImpl::SetContext(void* context) { user_context_ = context; }

/// 获得用户上下文
void* RowMutationImpl::GetContext() { return user_context_; }

/// 获得结果错误码
const ErrorCode& RowMutationImpl::GetError() { return error_code_; }

/// 是否异步操作
bool RowMutationImpl::IsAsync() { return (callback_ != NULL); }

/// 异步操作是否完成
bool RowMutationImpl::IsFinished() const {
  MutexLock lock(&finish_mutex_);
  return finish_;
}

/// 返回row_key
const std::string& RowMutationImpl::RowKey() { return row_key_; }

std::string RowMutationImpl::InternalRowKey() {
  if (table_ && table_->IsHashTable()) {
    return table_->GetHashMethod()(RowKey());
  }
  return RowKey();
}

/// mutation数量
uint32_t RowMutationImpl::MutationNum() { return mu_seq_.size(); }

/// mutation总大小
uint32_t RowMutationImpl::Size() {
  uint32_t total_size = 0;
  for (size_t i = 0; i < mu_seq_.size(); ++i) {
    total_size += +InternalRowKey().size() + mu_seq_[i].family.size() +
                  mu_seq_[i].qualifier.size() + mu_seq_[i].value.size() +
                  sizeof(mu_seq_[i].timestamp);
  }
  return total_size;
}

/// 返回mutation
const RowMutation::Mutation& RowMutationImpl::GetMutation(uint32_t index) { return mu_seq_[index]; }

/// 设置错误码
void RowMutationImpl::SetError(ErrorCode::ErrorCodeType err, const std::string& reason) {
  error_code_.SetFailed(err, reason);
}

/// 等待结束
void RowMutationImpl::Wait() {
  MutexLock lock(&finish_mutex_);
  while (!finish_) {
    finish_cond_.Wait();
  }
}

void RowMutationImpl::RunCallback() {
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

void RowMutationImpl::Concatenate(RowMutationImpl& row_mu) {
  uint32_t mutation_num = row_mu.MutationNum();
  for (size_t i = 0; i < mutation_num; i++) {
    AddMutation() = row_mu.GetMutation(i);
  }
}

RowMutation::Mutation& RowMutationImpl::AddMutation() {
  mu_seq_.resize(mu_seq_.size() + 1);
  return mu_seq_.back();
}

void SerializeMutation(const RowMutation::Mutation& src, tera::Mutation* dst) {
  switch (src.type) {
    case RowMutation::kPut:
      dst->set_type(tera::kPut);
      dst->set_family(src.family);
      dst->set_qualifier(src.qualifier);
      dst->set_timestamp(src.timestamp);
      dst->set_value(src.value);
      dst->set_ttl(src.ttl);
      break;
    case RowMutation::kAdd:
      dst->set_type(tera::kAdd);
      dst->set_family(src.family);
      dst->set_qualifier(src.qualifier);
      dst->set_timestamp(src.timestamp);
      dst->set_value(src.value);
      break;
    case RowMutation::kAddInt64:
      dst->set_type(tera::kAddInt64);
      dst->set_family(src.family);
      dst->set_qualifier(src.qualifier);
      dst->set_timestamp(src.timestamp);
      dst->set_value(src.value);
      break;
    case RowMutation::kPutIfAbsent:
      dst->set_type(tera::kPutIfAbsent);
      dst->set_family(src.family);
      dst->set_qualifier(src.qualifier);
      dst->set_timestamp(src.timestamp);
      dst->set_value(src.value);
      break;
    case RowMutation::kAppend:
      dst->set_type(tera::kAppend);
      dst->set_family(src.family);
      dst->set_qualifier(src.qualifier);
      dst->set_timestamp(src.timestamp);
      dst->set_value(src.value);
      break;
    case RowMutation::kDeleteColumn:
      dst->set_type(tera::kDeleteColumn);
      dst->set_family(src.family);
      dst->set_qualifier(src.qualifier);
      dst->set_timestamp(src.timestamp);
      break;
    case RowMutation::kDeleteColumns:
      dst->set_type(tera::kDeleteColumns);
      dst->set_family(src.family);
      dst->set_qualifier(src.qualifier);
      dst->set_timestamp(src.timestamp);
      break;
    case RowMutation::kDeleteFamily:
      dst->set_type(tera::kDeleteFamily);
      dst->set_family(src.family);
      dst->set_timestamp(src.timestamp);
      break;
    case RowMutation::kDeleteRow:
      dst->set_type(tera::kDeleteRow);
      dst->set_timestamp(src.timestamp);
      break;
    default:
      assert(false);
      break;
  }
}

}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
