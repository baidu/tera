// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/read_impl.h"

namespace tera {

/// 读取操作
RowReaderImpl::RowReaderImpl(Table* table, const std::string& row_key)
    : SdkTask(SdkTask::READ),
      _row_key(row_key),
      _callback(NULL),
      _user_context(NULL),
      _finish(false),
      _finish_cond(&_finish_mutex),
      _ts_start(kOldestTs),
      _ts_end(kLatestTs),
      _max_version(1),
      _snapshot_id(0),
      _timeout_ms(0),
      _retry_times(0),
      _result_pos(0),
      _commit_times(0) {
}

RowReaderImpl::~RowReaderImpl() {
}

/// 设置读取特定版本
void RowReaderImpl::SetTimestamp(int64_t ts) {
    SetTimeRange(ts, ts);
}

int64_t RowReaderImpl::GetTimestamp() {
    return _ts_start;
}

void RowReaderImpl::AddColumnFamily(const std::string& cf_name) {
    _family_map[cf_name].clear();
}

void RowReaderImpl::AddColumn(const std::string& cf_name,
                              const std::string& qualifier) {
    QualifierSet& qualifier_set = _family_map[cf_name];
    qualifier_set.insert(qualifier);
}

void RowReaderImpl::SetTimeRange(int64_t ts_start, int64_t ts_end) {
    _ts_start = ts_start;
    _ts_end = ts_end;
}

void RowReaderImpl::GetTimeRange(int64_t* ts_start, int64_t* ts_end) {
    if (NULL != ts_start) {
        *ts_start = _ts_start;
    }
    if (NULL != ts_end) {
        *ts_end = _ts_end;
    }
}

void RowReaderImpl::SetMaxVersions(uint32_t max_version) {
    _max_version = max_version;
}

uint32_t RowReaderImpl::GetMaxVersions() {
    return _max_version;
}


/// 设置超时时间(只影响当前操作,不影响Table::SetReadTimeout设置的默认读超时)
void RowReaderImpl::SetTimeOut(int64_t timeout_ms) {
    _timeout_ms = timeout_ms;
}

void RowReaderImpl::SetCallBack(RowReader::Callback callback) {
    _callback = callback;
}

/// 设置用户上下文，可在回调函数中获取
void RowReaderImpl::SetContext(void* context) {
    _user_context = context;
}

void* RowReaderImpl::GetContext() {
    return _user_context;
}
/// 设置异步返回
void RowReaderImpl::SetAsync() {
}

/// 异步操作是否完成
bool RowReaderImpl::IsFinished() const {
    MutexLock lock(&_finish_mutex);
    return _finish;
}

/// 获得结果错误码
ErrorCode RowReaderImpl::GetError() {
    return _error_code;
}

/// 是否到达结束标记
bool RowReaderImpl::Done() {
    if (_result_pos < _result.key_values_size()) {
         return false;
    }
    return true;
}

/// 迭代下一个cell
void RowReaderImpl::Next() {
    _result_pos++;
}

/// 读取的结果
std::string RowReaderImpl::Value() {
    if (_result.key_values(_result_pos).has_value()) {
        return _result.key_values(_result_pos).value();
    } else {
        return "";
    }
}

/// Timestamp
int64_t RowReaderImpl::Timestamp() {
    if (_result.key_values(_result_pos).has_timestamp()) {
        return _result.key_values(_result_pos).timestamp();
    } else {
        return 0L;
    }
}

const std::string& RowReaderImpl::RowName() {
    return _row_key;
}

/// Column cf:qualifier
std::string RowReaderImpl::ColumnName() {
    std::string column;
    if (_result.key_values(_result_pos).has_column_family()) {
        column =  _result.key_values(_result_pos).column_family();
    } else {
        return column = "";
    }

    if (_result.key_values(_result_pos).has_qualifier()) {
        column += ":" + _result.key_values(_result_pos).qualifier();
    }
    return column;
}

/// Column family
std::string RowReaderImpl::Family() {
    if (_result.key_values(_result_pos).has_column_family()) {
        return _result.key_values(_result_pos).column_family();
    } else {
        return "";
    }
}

/// Qualifier
std::string RowReaderImpl::Qualifier() {
    if (_result.key_values(_result_pos).has_qualifier()) {
        return _result.key_values(_result_pos).qualifier();
    } else {
        return "";
    }
}

void RowReaderImpl::ToMap(Map* rowmap) {
    for (int32_t i = 0; i < _result.key_values_size(); ++i) {

        std::string column;
        if (_result.key_values(i).has_column_family()) {
            column = _result.key_values(i).column_family();
        } else {
            column = "";
        }
        if (_result.key_values(i).has_qualifier()) {
            column += ":" + _result.key_values(i).qualifier();
        }
        std::map<int64_t, std::string>& value_map = (*rowmap)[column];
        int64_t timestamp = 0L;
        if (_result.key_values(i).has_timestamp()) {
            timestamp = _result.key_values(i).timestamp();
        }
        value_map[timestamp] = _result.key_values(i).value();
    }
}

void RowReaderImpl::SetResult(const RowResult& result) {
    int32_t num = result.key_values_size();
    for (int32_t i = 0; i < num; ++i) {
        const std::string& key = result.key_values(i).key();
        CHECK(_row_key == key) << "FATAL: rowkey[" << _row_key
                << "] vs result[" << key << "]";
    }
    return _result.CopyFrom(result);
}


/// 重试计数加一
void RowReaderImpl::IncRetryTimes() {
    _retry_times++;
}

/// 设置错误码
void RowReaderImpl::SetError(ErrorCode::ErrorCodeType err,
                             const std::string& reason) {
    _error_code.SetFailed(err, reason);
}

uint32_t RowReaderImpl::RetryTimes() {
    return _retry_times;
}

bool RowReaderImpl::IsAsync() {
    return (_callback != NULL);
}

int64_t RowReaderImpl::TimeOut() {
    return _timeout_ms;
}

void RowReaderImpl::RunCallback() {
    if (_callback) {
        _callback(this);
    } else {
        MutexLock lock(&_finish_mutex);
        _finish = true;
        _finish_cond.Signal();
    }
}

void RowReaderImpl::Wait() {
    MutexLock lock(&_finish_mutex);
    while (!_finish) {
        _finish_cond.Wait();
    }
}

/// Get数量
uint32_t RowReaderImpl::GetReadColumnNum() {
    return _family_map.size();
}

/// 返回Get
const RowReader::ReadColumnList& RowReaderImpl::GetReadColumnList() {
    return _family_map;
}

/// 序列化
void RowReaderImpl::ToProtoBuf(RowReaderInfo* info) {
    info->set_key(_row_key);
    info->set_max_version(_max_version);
    info->mutable_time_range()->set_ts_start(_ts_start);
    info->mutable_time_range()->set_ts_end(_ts_end);

    FamilyMap::iterator f_it = _family_map.begin();
    for (; f_it != _family_map.end(); ++f_it) {
        const std::string& family = f_it->first;
        const QualifierSet& qualifier_set = f_it->second;

        ColumnFamily* family_info = info->add_cf_list();
        family_info->set_family_name(family);

        QualifierSet::iterator q_it = qualifier_set.begin();
        for (; q_it != qualifier_set.end(); ++q_it) {
            family_info->add_qualifier_list(*q_it);
        }
    }
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
