// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/string_format.h"
#include "io/coding.h"
#include "sdk/mutate_impl.h"
#include "utils/timer.h"

namespace tera {

RowMutationImpl::RowMutationImpl(TableImpl* table, const std::string& row_key)
    : SdkTask(SdkTask::MUTATION),
      _table(table),
      _row_key(row_key),
      _callback(NULL),
      _timeout_ms(0),
      _retry_times(0),
      _finish(false),
      _finish_cond(&_finish_mutex),
      _commit_times(0) {
    SetErrorIfInvalid(row_key, kRowkey);
}

RowMutationImpl::~RowMutationImpl() {
}

/// 重置，复用前必须调用
void RowMutationImpl::Reset(const std::string& row_key) {
    _row_key = row_key;
    _mu_seq.clear();
    _callback = NULL;
    _timeout_ms = 0;
    _retry_times = 0;
    _finish = false;
    _error_code.SetFailed(ErrorCode::kOK);
    _commit_times = 0;
}

void RowMutationImpl::SetErrorIfInvalid(const std::string& str,
                                        const FieldLimit& field) {
    std::string reason = _error_code.GetReason();
    switch (field) {
    case kRowkey: {
        if (str.size() >= kRowkeySize) {
            reason.append(" Bad parameters: rowkey should < 64KB");
            _error_code.SetFailed(ErrorCode::kBadParam, reason);
        }
    } break;
    case kQualifier: {
        if (str.size() >= kQualifierSize) {
            reason.append(" Bad parameters: qualifier should < 64KB");
            _error_code.SetFailed(ErrorCode::kBadParam, reason);
        }
    } break;
    case kValue: {
        if (str.size() >= kValueSize) {
            reason.append(" Bad parameters: value should < 32MB");
            _error_code.SetFailed(ErrorCode::kBadParam, reason);
        }
    } break;
    default: {
        abort();
    }
    }
}

// 原子加一个Cell
void RowMutationImpl::AddInt64(const std::string& family, const std::string& qualifier,
                               const int64_t delta) {
    SetErrorIfInvalid(qualifier, kQualifier);
    std::string delta_str((char*)&delta, sizeof(int64_t));
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kAddInt64;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = get_micros(); // 为了避免retry引起的重复加，所以自带时间戳
    mutation.value.assign(delta_str);
}

// 原子加一个Cell
void RowMutationImpl::Add(const std::string& family, const std::string& qualifier,
                          const int64_t delta) {
    SetErrorIfInvalid(qualifier, kQualifier);
    char delta_buf[sizeof(int64_t)];
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kAdd;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = get_micros(); // 为了避免retry引起的重复加，所以自带时间戳
    io::EncodeBigEndian(delta_buf, delta);
    mutation.value.assign(delta_buf, sizeof(delta_buf));
}

// 原子操作：如果不存在才能Put成功
void RowMutationImpl::PutIfAbsent(const std::string& family, const std::string& qualifier,
                                  const std::string& value) {
    SetErrorIfInvalid(qualifier, kQualifier);
    SetErrorIfInvalid(value, kValue);
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kPutIfAbsent;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = kLatestTimestamp;
    mutation.value = value;
}

void RowMutationImpl::Append(const std::string& family, const std::string& qualifier,
                             const std::string& value) {
    SetErrorIfInvalid(qualifier, kQualifier);
    SetErrorIfInvalid(value, kValue);
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kAppend;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = get_micros();
    mutation.value = value;
}

/// 修改一个列
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          const std::string& value) {
    Put(family, qualifier, kLatestTimestamp, value);
}

/// 修改一个列
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          const int64_t value) {
    std::string value_str((char*)&value, sizeof(int64_t));
    Put(family, qualifier, value_str);
}

/// 带TTL修改一个列
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          const std::string& value, int32_t ttl) {
    SetErrorIfInvalid(qualifier, kQualifier);
    SetErrorIfInvalid(value, kValue);
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
                          int64_t timestamp, const std::string& value) {
    SetErrorIfInvalid(qualifier, kQualifier);
    SetErrorIfInvalid(value, kValue);
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kPut;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = timestamp;
    mutation.value = value;
    mutation.ttl = -1;
    /*
    mutation.set_type(kPut);
    mutation.set_family(family);
    mutation.set_qualifier(qualifier);
    mutation.set_timestamp(timestamp);
    mutation.set_value(value);
    */
}

/// 带TTL的修改一个列的特定版本
void RowMutationImpl::Put(const std::string& family, const std::string& qualifier,
                          int64_t timestamp, const std::string& value, int32_t ttl) {
    SetErrorIfInvalid(qualifier, kQualifier);
    SetErrorIfInvalid(value, kValue);
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kPut;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = timestamp;
    mutation.value = value;
    mutation.ttl = ttl;
}

/// 修改默认列
void RowMutationImpl::Put(const std::string& value) {
    Put("", "", kLatestTimestamp, value);
}

/// 修改默认列
void RowMutationImpl::Put(const int64_t value) {
    std::string value_str((char*)&value, sizeof(int64_t));
    Put(value_str);
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
void RowMutationImpl::DeleteColumn(const std::string& family,
                                   const std::string& qualifier) {
    DeleteColumn(family, qualifier, kLatestTimestamp);
}

/// 删除一个列的指定版本
void RowMutationImpl::DeleteColumn(const std::string& family,
                                   const std::string& qualifier,
                                   int64_t timestamp) {
    SetErrorIfInvalid(qualifier, kQualifier);
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kDeleteColumn;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = timestamp;

    /*
    mutation.set_type(kDeleteColumn);
    mutation.set_family(family);
    mutation.set_qualifier(qualifier);
    mutation.set_timestamp(timestamp);
    */
}

/// 删除一个列的全部版本
void RowMutationImpl::DeleteColumns(const std::string& family,
                                    const std::string& qualifier) {
    DeleteColumns(family, qualifier, kLatestTimestamp);
}

/// 删除一个列的指定范围版本
void RowMutationImpl::DeleteColumns(const std::string& family,
                                    const std::string& qualifier,
                                    int64_t timestamp) {
    SetErrorIfInvalid(qualifier, kQualifier);
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kDeleteColumns;
    mutation.family = family;
    mutation.qualifier = qualifier;
    mutation.timestamp = timestamp;

    /*
    mutation.set_type(kDeleteColumns);
    mutation.set_family(family);
    mutation.set_qualifier(qualifier);
    mutation.set_ts_end(ts_end);
    mutation.set_ts_start(ts_start);
    */
}

/// 删除一个列族的所有列的全部版本
void RowMutationImpl::DeleteFamily(const std::string& family) {
    DeleteFamily(family, kLatestTimestamp);
}

/// 删除一个列族的所有列的指定范围版本
void RowMutationImpl::DeleteFamily(const std::string& family,
                                   int64_t timestamp) {
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kDeleteFamily;
    mutation.family = family;
    mutation.timestamp = timestamp;
    /*
    mutation.set_type(kDeleteFamily);
    mutation.set_family(family);
    mutation.set_ts_end(ts_end);
    mutation.set_ts_start(ts_start);
    */
}

/// 删除整行的全部数据
void RowMutationImpl::DeleteRow() {
    DeleteRow(kLatestTimestamp);
}

/// 删除整行的指定范围版本
void RowMutationImpl::DeleteRow(int64_t timestamp) {
    RowMutation::Mutation& mutation = AddMutation();
    mutation.type = RowMutation::kDeleteRow;
    mutation.timestamp = timestamp;
    /*
    mutation.set_type(kDeleteRow);
    mutation.set_ts_end(ts_end);
    mutation.set_ts_start(ts_start);
    */
}

/// 修改锁住的行, 必须提供行锁
void RowMutationImpl::SetLock(RowLock* rowlock) {
}

/// 设置超时时间(只影响当前操作,不影响Table::SetWriteTimeout设置的默认写超时)
void RowMutationImpl::SetTimeOut(int64_t timeout_ms) {
    _timeout_ms = timeout_ms;
}

int64_t RowMutationImpl::TimeOut() {
    return _timeout_ms;
}

/// 设置异步回调, 操作会异步返回
void RowMutationImpl::SetCallBack(RowMutation::Callback callback) {
    _callback = callback;
}

/// 获得回调函数
RowMutation::Callback RowMutationImpl::GetCallBack() {
    return _callback;
}

/// 设置用户上下文，可在回调函数中获取
void RowMutationImpl::SetContext(void* context) {
    _user_context = context;
}

/// 获得用户上下文
void* RowMutationImpl::GetContext() {
    return _user_context;
}

/// 获得结果错误码
const ErrorCode& RowMutationImpl::GetError() {
    return _error_code;
}

/// 是否异步操作
bool RowMutationImpl::IsAsync() {
    return (_callback != NULL);
}

/// 异步操作是否完成
bool RowMutationImpl::IsFinished() const {
    MutexLock lock(&_finish_mutex);
    return _finish;
}

/// 返回row_key
const std::string& RowMutationImpl::RowKey() {
    return _row_key;
}

/// mutation数量
uint32_t RowMutationImpl::MutationNum() {
    return _mu_seq.size();
}

/// mutation总大小
uint32_t RowMutationImpl::Size() {
    uint32_t total_size = 0;
    for (size_t i = 0; i < _mu_seq.size(); ++i) {
        total_size +=
            + _row_key.size()
            + _mu_seq[i].family.size()
            + _mu_seq[i].qualifier.size()
            + _mu_seq[i].value.size()
            + sizeof(_mu_seq[i].timestamp);
    }
    return total_size;
}

/// 返回mutation
const RowMutation::Mutation& RowMutationImpl::GetMutation(uint32_t index) {
    return _mu_seq[index];
}

/// 重试次数
uint32_t RowMutationImpl::RetryTimes() {
    return _retry_times;
}

/// 重试计数加一
void RowMutationImpl::IncRetryTimes() {
    _retry_times++;
}

/// 设置错误码
void RowMutationImpl::SetError(ErrorCode::ErrorCodeType err,
                               const std::string& reason) {
    _error_code.SetFailed(err, reason);
}

/// 等待结束
void RowMutationImpl::Wait() {
    MutexLock lock(&_finish_mutex);
    while (!_finish) {
        _finish_cond.Wait();
    }
}

void RowMutationImpl::RunCallback() {
    if (_callback) {
        _callback(this);
    } else {
        MutexLock lock(&_finish_mutex);
        _finish = true;
        _finish_cond.Signal();
    }
}

RowMutation::Mutation& RowMutationImpl::AddMutation() {
    _mu_seq.resize(_mu_seq.size() + 1);
    return _mu_seq.back();
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

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
