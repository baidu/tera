// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/read_impl.h"
#include "sdk/table_impl.h"

namespace tera {

/// 读取操作
RowReaderImpl::RowReaderImpl(TableImpl* table, const std::string& row_key)
    : SdkTask(SdkTask::READ),
      table_(table),
      row_key_(row_key),
      callback_(NULL),
      user_context_(NULL),
      finish_(false),
      finish_cond_(&finish_mutex_),
      ts_start_(kOldestTs),
      ts_end_(kLatestTs),
      max_version_(1),
      snapshot_id_(0),
      timeout_ms_(0),
      retry_times_(0),
      result_pos_(0),
      commit_times_(0),
      start_ts_(get_micros()),
      txn_(NULL) {
}

RowReaderImpl::~RowReaderImpl() {
}

/// 设置读取特定版本
void RowReaderImpl::SetTimestamp(int64_t ts) {
    SetTimeRange(ts, ts);
}

int64_t RowReaderImpl::GetTimestamp() {
    return ts_start_;
}

void RowReaderImpl::AddColumnFamily(const std::string& cf_name) {
    family_map_[cf_name].clear();
}

void RowReaderImpl::AddColumn(const std::string& cf_name,
                              const std::string& qualifier) {
    QualifierSet& qualifier_set = family_map_[cf_name];
    qualifier_set.insert(qualifier);
}

void RowReaderImpl::SetTimeRange(int64_t ts_start, int64_t ts_end) {
    ts_start_ = ts_start;
    ts_end_ = ts_end;
}

void RowReaderImpl::GetTimeRange(int64_t* ts_start, int64_t* ts_end) {
    if (NULL != ts_start) {
        *ts_start = ts_start_;
    }
    if (NULL != ts_end) {
        *ts_end = ts_end_;
    }
}

void RowReaderImpl::SetMaxVersions(uint32_t max_version) {
    max_version_ = max_version;
}

uint32_t RowReaderImpl::GetMaxVersions() {
    return max_version_;
}


/// 设置超时时间(只影响当前操作,不影响Table::SetReadTimeout设置的默认读超时)
void RowReaderImpl::SetTimeOut(int64_t timeout_ms) {
    timeout_ms_ = timeout_ms;
}

void RowReaderImpl::SetCallBack(RowReader::Callback callback) {
    callback_ = callback;
}

RowReader::Callback RowReaderImpl::GetCallBack() {
    return callback_;
}

/// 设置用户上下文，可在回调函数中获取
void RowReaderImpl::SetContext(void* context) {
    user_context_ = context;
}

void* RowReaderImpl::GetContext() {
    return user_context_;
}
/// 设置异步返回
void RowReaderImpl::SetAsync() {
}

/// 异步操作是否完成
bool RowReaderImpl::IsFinished() const {
    MutexLock lock(&finish_mutex_);
    return finish_;
}

/// 获得结果错误码
ErrorCode RowReaderImpl::GetError() {
    return error_code_;
}

/// 是否到达结束标记
bool RowReaderImpl::Done() {
    if (result_pos_ < result_.key_values_size()) {
         return false;
    }
    return true;
}

/// 迭代下一个cell
void RowReaderImpl::Next() {
    result_pos_++;
}

/// 读取的结果
std::string RowReaderImpl::Value() {
    if (result_.key_values(result_pos_).has_value()) {
        return result_.key_values(result_pos_).value();
    } else {
        return "";
    }
}

/// 读取的结果
int64_t RowReaderImpl::ValueInt64() {
    std::string v = Value();
    return (v.size() == sizeof(int64_t)) ? *(int64_t*)v.c_str() : 0;
}

/// Timestamp
int64_t RowReaderImpl::Timestamp() {
    if (result_.key_values(result_pos_).has_timestamp()) {
        return result_.key_values(result_pos_).timestamp();
    } else {
        return 0L;
    }
}

const std::string& RowReaderImpl::RowName() {
    return row_key_;
}

const std::string& RowReaderImpl::RowKey() {
    return row_key_;
}

/// Column cf:qualifier
std::string RowReaderImpl::ColumnName() {
    std::string column;
    if (result_.key_values(result_pos_).has_column_family()) {
        column =  result_.key_values(result_pos_).column_family();
    } else {
        return column = "";
    }

    if (result_.key_values(result_pos_).has_qualifier()) {
        column += ":" + result_.key_values(result_pos_).qualifier();
    }
    return column;
}

/// Column family
std::string RowReaderImpl::Family() {
    if (result_.key_values(result_pos_).has_column_family()) {
        return result_.key_values(result_pos_).column_family();
    } else {
        return "";
    }
}

/// Qualifier
std::string RowReaderImpl::Qualifier() {
    if (result_.key_values(result_pos_).has_qualifier()) {
        return result_.key_values(result_pos_).qualifier();
    } else {
        return "";
    }
}

void RowReaderImpl::ToMap(Map* rowmap) {
    for (int32_t i = 0; i < result_.key_values_size(); ++i) {

        std::string column;
        if (result_.key_values(i).has_column_family()) {
            column = result_.key_values(i).column_family();
        } else {
            column = "";
        }
        if (result_.key_values(i).has_qualifier()) {
            column += ":" + result_.key_values(i).qualifier();
        }
        std::map<int64_t, std::string>& value_map = (*rowmap)[column];
        int64_t timestamp = 0L;
        if (result_.key_values(i).has_timestamp()) {
            timestamp = result_.key_values(i).timestamp();
        }
        value_map[timestamp] = result_.key_values(i).value();
    }
}

void RowReaderImpl::ToMap(TRow* rowmap) {
    for (int32_t i = 0; i < result_.key_values_size(); ++i) {
        std::string cf, qu, value;
        if (result_.key_values(i).has_column_family()) {
            cf = result_.key_values(i).column_family();
        } else {
            cf = "";
        }
        TColumnFamily& tcf = (*rowmap)[cf];
        if (result_.key_values(i).has_qualifier()) {
            qu = result_.key_values(i).qualifier();
        }
        TColumn& tqu = tcf[qu];
        int64_t timestamp = 0L;
        if (result_.key_values(i).has_timestamp()) {
            timestamp = result_.key_values(i).timestamp();
        }
        tqu[timestamp] = result_.key_values(i).value();
    }
}

void RowReaderImpl::SetResult(const RowResult& result) {
    int32_t num = result.key_values_size();
    for (int32_t i = 0; i < num; ++i) {
        const std::string& key = result.key_values(i).key();
        CHECK(row_key_ == key) << "FATAL: rowkey[" << row_key_
                << "] vs result[" << key << "]";
    }
    return result_.CopyFrom(result);
}


/// 重试计数加一
void RowReaderImpl::IncRetryTimes() {
    retry_times_++;
}

/// 设置错误码
void RowReaderImpl::SetError(ErrorCode::ErrorCodeType err,
                             const std::string& reason) {
    error_code_.SetFailed(err, reason);
}

uint32_t RowReaderImpl::RetryTimes() {
    return retry_times_;
}

bool RowReaderImpl::IsAsync() {
    return (callback_ != NULL);
}

int64_t RowReaderImpl::TimeOut() {
    return timeout_ms_;
}

void RowReaderImpl::RunCallback() {
    table_->StatUserPerfCounter(SdkTask::READ, error_code_.GetType(), get_micros() - start_ts_);
    if (callback_) {
        callback_(this);
    } else {
        MutexLock lock(&finish_mutex_);
        finish_ = true;
        finish_cond_.Signal();
    }
}

void RowReaderImpl::Wait() {
    MutexLock lock(&finish_mutex_);
    while (!finish_) {
        finish_cond_.Wait();
    }
}

/// Get数量
uint32_t RowReaderImpl::GetReadColumnNum() {
    return family_map_.size();
}

/// 返回Get
const RowReader::ReadColumnList& RowReaderImpl::GetReadColumnList() {
    return family_map_;
}

/// 序列化
void RowReaderImpl::ToProtoBuf(RowReaderInfo* info) {
    info->set_key(row_key_);
    info->set_max_version(max_version_);
    info->mutable_time_range()->set_ts_start(ts_start_);
    info->mutable_time_range()->set_ts_end(ts_end_);

    FamilyMap::iterator f_it = family_map_.begin();
    for (; f_it != family_map_.end(); ++f_it) {
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
