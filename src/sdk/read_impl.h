// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_READ_IMPL_H_
#define  TERA_SDK_READ_IMPL_H_

#include <string>
#include <vector>

#include "common/mutex.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/sdk_task.h"
#include "sdk/tera.h"
#include "types.h"
#include "utils/timer.h"

namespace tera {

class TableImpl;

class RowReaderImpl : public RowReader, public SdkTask {
public:
    RowReaderImpl(Table* table, const std::string& row_key);
    ~RowReaderImpl();
    /// 设置读取特定版本
    void SetTimestamp(int64_t ts);
    /// 返回读取时间戳
    int64_t GetTimestamp();

    void SetSnapshot(uint64_t snapshot_id) { _snapshot_id = snapshot_id; }

    uint64_t GetSnapshot() { return _snapshot_id; }

    /// 设置读取CF
    void AddColumnFamily(const std::string& cf_name);
    /// 设置读取Column(CF:Qualifier)
    void AddColumn(const std::string& cf_name, const std::string& qualifier);
    /// 设置读取time_range
    void SetTimeRange(int64_t ts_start, int64_t ts_end);
    /// 返回time_range
    void GetTimeRange(int64_t* ts_start, int64_t* ts_end = NULL);
    /// 设置读取max_version
    void SetMaxVersions(uint32_t max_version);
    /// 返回max_version
    uint32_t GetMaxVersions();
    /// 设置超时时间(只影响当前操作,不影响Table::SetReadTimeout设置的默认读超时)
    void SetTimeOut(int64_t timeout_ms);
    /// 设置异步回调, 操作会异步返回
    void SetCallBack(RowReader::Callback callback);
    /// 设置用户上下文，可在回调函数中获取
    void SetContext(void* context);
    void* GetContext();
    /// 设置异步返回
    void SetAsync();
    /// 异步操作是否完成
    bool IsFinished() const;
    /// 获取读超时时间
    int64_t TimeOut();
    /// 设置错误吗
    void SetError(ErrorCode::ErrorCodeType err , const std::string& reason = "");
    /// 获得结果错误码
    ErrorCode GetError();
    /// 是否到达结束标记
    bool Done();
    /// 迭代下一个cell
    void Next();
    /// Row
    const std::string& RowName();
    /// 读取的结果
    std::string Value();
    /// Timestamp
    int64_t Timestamp();
    /// Column cf:qualifier
    std::string ColumnName();
    /// Column family
    std::string Family();
    /// Qualifier
    std::string Qualifier();
    /// 将结果转存到一个std::map中, 格式为: map<column, map<timestamp, value>>
    typedef std::map< std::string, std::map<int64_t, std::string> > Map;
    void ToMap(Map* rowmap);

    void SetResult(const RowResult& result);

    void IncRetryTimes();

    uint32_t RetryTimes();

    bool IsAsync();

    void Wait();

    /// 执行异步回调
    void RunCallback();
    /// Get数量
    uint32_t GetReadColumnNum();
    /// 返回Get引用
    const ReadColumnList& GetReadColumnList();
    /// 序列化
    void ToProtoBuf(RowReaderInfo* info);

    void AddCommitTimes() { _commit_times++; }
    int64_t GetCommitTimes() { return _commit_times; }

private:
    std::string _row_key;
    RowReader::Callback _callback;
    void* _user_context;

    bool _finish;
    ErrorCode _error_code;
    mutable Mutex _finish_mutex;
    common::CondVar _finish_cond;

    typedef std::set<std::string> QualifierSet;
    typedef std::map<std::string, QualifierSet> FamilyMap;
    FamilyMap _family_map;
    int64_t _ts_start;
    int64_t _ts_end;
    uint32_t _max_version;
    uint64_t _snapshot_id;

    int64_t _timeout_ms;
    uint32_t _retry_times;
    int32_t _result_pos;
    RowResult _result;

    /// 记录此reader被提交到ts的次数
    int64_t _commit_times;
};

} // namespace tera

#endif  // TERA_SDK_READ_IMPL_H_
