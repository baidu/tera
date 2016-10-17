// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_MUTATE_IMPL_H_
#define  TERA_SDK_MUTATE_IMPL_H_

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

class RowMutationImpl : public RowMutation, public SdkTask {
public:
    enum FieldLimit {
        kRowkey = 0,
        kColumnFamily,
        kQualifier,
        kTimeStamp,
        kValue
    };

    RowMutationImpl(TableImpl* table, const std::string& row_key);
    ~RowMutationImpl();

    /// 重置
    void Reset(const std::string& row_key);

    /// 修改一个列
    void Put(const std::string& family, const std::string& qualifier,
             const std::string& value, int64_t timestamp);

    void Put(const std::string& family, const std::string& qualifier,
             const int64_t value, int64_t timestamp);

    /// 带TTL的修改一个列
    void Put(const std::string& family, const std::string& qualifier,
             const std::string& value, int32_t ttl);

    /// 修改一个列的特定版本
    void Put(const std::string& family, const std::string& qualifier,
             int64_t timestamp, const std::string& value);

    /// 带TTL的修改一个列的特定版本
    virtual void Put(const std::string& family, const std::string& qualifier,
                     int64_t timestamp, const std::string& value, int32_t ttl);

    /// 修改默认列
    void Put(const int64_t value);

    /// 带TTL的修改默认列
    virtual void Put(const std::string& value, int32_t ttl);

    /// 修改默认列的特定版本
    void Put(int64_t timestamp, const std::string& value);

    /// 原子加一个Cell
    void Add(const std::string& family, const std::string& qualifier, const int64_t delta);
    /// 原子加一个Cell
    void AddInt64(const std::string& family, const std::string& qualifier, const int64_t delta);

    //  原子操作：如果不存在才能Put成功
    void PutIfAbsent(const std::string& family, const std::string& qualifier,
                     const std::string& value);

    /// 原子操作：追加内容到一个Cell
    void Append(const std::string& family, const std::string& qualifier,
                const std::string& value);

    /// 删除一个列的最新版本
    void DeleteColumn(const std::string& family, const std::string& qualifier);

    /// 删除一个列的指定版本
    void DeleteColumn(const std::string& family, const std::string& qualifier,
                      int64_t timestamp);

    /// 删除一个列的指定范围版本
    void DeleteColumns(const std::string& family, const std::string& qualifier,
                       int64_t timestamp);

    /// 删除一个列族的所有列的指定范围版本
    void DeleteFamily(const std::string& family, int64_t timestamp);

    /// 删除整行的指定范围版本
    void DeleteRow(int64_t timestamp);

    /// 修改锁住的行, 必须提供行锁
    void SetLock(RowLock* rowlock);

    /// 设置超时时间(只影响当前操作,不影响Table::SetWriteTimeout设置的默认写超时)
    void SetTimeOut(int64_t timeout_ms);

    int64_t TimeOut();

    /// 设置异步回调, 操作会异步返回
    void SetCallBack(RowMutation::Callback callback);

    RowMutation::Callback GetCallBack();

    /// 设置用户上下文，可在回调函数中获取
    void SetContext(void* context);

    void* GetContext();

    /// 获得结果错误码
    const ErrorCode& GetError();

    /// 设置异步返回
    bool IsAsync();

    /// 异步操作是否完成
    bool IsFinished() const;

    /// 返回row_key
    const std::string& RowKey();

    /// mutation数量
    uint32_t MutationNum();

    /// mutation总大小
    uint32_t Size();

    /// 返回mutation
    const RowMutation::Mutation& GetMutation(uint32_t index);

    /// 重试次数
    uint32_t RetryTimes();

    /// 返回所属事务
    Transaction* GetTransaction() { return txn_; }

public:
    /// 以下接口仅内部使用，不开放给用户

    /// 重试计数加一
    void IncRetryTimes();

    /// 设置错误码
    void SetError(ErrorCode::ErrorCodeType err , const std::string& reason = "");

    /// 等待结束
    void Wait();

    /// 执行异步回调
    void RunCallback();

    /// 增加引用
    void Ref();

    /// 释放引用
    void Unref();

    void SetErrorIfInvalid(const std::string& str,
                           const FieldLimit& field);

    void AddCommitTimes() { commit_times_++; }
    int64_t GetCommitTimes() { return commit_times_; }

    /// 设置所属事务
    void SetTransaction(Transaction* txn) { txn_ = txn; }

    /// 连接
    void Concatenate(RowMutationImpl& row_mu);

protected:
    /// 增加一个操作
    RowMutation::Mutation& AddMutation();

private:
    TableImpl* table_;
    std::string row_key_;
    std::vector<RowMutation::Mutation> mu_seq_;

    RowMutation::Callback callback_;
    void* user_context_;
    int64_t timeout_ms_;
    uint32_t retry_times_;

    bool finish_;
    ErrorCode error_code_;
    mutable Mutex finish_mutex_;
    common::CondVar finish_cond_;

    /// 记录此mutation被提交到ts的次数
    int64_t commit_times_;

    /// 所属事务
    Transaction* txn_;
};

void SerializeMutation(const RowMutation::Mutation& src, tera::Mutation* dst);

} // namespace tera

#endif  // TERA_SDK_MUTATE_IMPL_H_
