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
#include "tera.h"
#include "types.h"
#include "utils/timer.h"

namespace tera {

class TableImpl;

class RowReaderImpl : public RowReader, public SdkTask {
public:
    RowReaderImpl(TableImpl* table, const std::string& row_key);
    ~RowReaderImpl();
    /// 设置读取特定版本
    void SetTimestamp(int64_t ts);
    /// 返回读取时间戳
    int64_t GetTimestamp();

    void SetSnapshot(uint64_t snapshot_id) { snapshot_id_ = snapshot_id; }

    uint64_t GetSnapshot() { return snapshot_id_; }

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
    RowReader::Callback GetCallBack();
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
    const std::string& RowKey();
    /// 读取的结果
    std::string Value();
    /// 读取的结果
    int64_t ValueInt64();
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
    void ToMap(TRow* rowmap);

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

    void AddCommitTimes() { commit_times_++; }
    int64_t GetCommitTimes() { return commit_times_; }

    /// 重置result游标
    void ResetResultPos() { result_pos_ = 0; }
    /// 返回所属事务
    Transaction* GetTransaction() { return txn_; }
    /// 设置所属事务
    void SetTransaction(Transaction* txn) { txn_ = txn; }

private:
    TableImpl* table_;
    std::string row_key_;
    RowReader::Callback callback_;
    void* user_context_;

    bool finish_;
    ErrorCode error_code_;
    mutable Mutex finish_mutex_;
    common::CondVar finish_cond_;

    typedef std::set<std::string> QualifierSet;
    typedef std::map<std::string, QualifierSet> FamilyMap;
    FamilyMap family_map_;
    int64_t ts_start_;
    int64_t ts_end_;
    uint32_t max_version_;
    uint64_t snapshot_id_;

    int64_t timeout_ms_;
    uint32_t retry_times_;
    int32_t result_pos_;
    RowResult result_;

    /// 记录此reader被提交到ts的次数
    int64_t commit_times_;

    int64_t start_ts_;

    /// 所属事务
    Transaction* txn_;
};

} // namespace tera

#endif  // TERA_SDK_READ_IMPL_H_
