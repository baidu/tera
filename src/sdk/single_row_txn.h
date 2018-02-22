// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SINGLE_ROW_TXN_H_
#define  TERA_SDK_SINGLE_ROW_TXN_H_

#include <string>

#include "sdk/mutate_impl.h"
#include "tera.h"

namespace common {
class ThreadPool;
}

namespace tera {

class TableImpl;
class Cell;

class SingleRowTxn : public Transaction {
public:
    SingleRowTxn(Table* table, const std::string& row_key,
                 common::ThreadPool* thread_pool);
    virtual ~SingleRowTxn();

    /// 提交一个修改操作
    virtual void ApplyMutation(RowMutation* row_mu);
    /// 读取操作
    virtual ErrorCode Get(RowReader* row_reader);

    /// 设置提交回调, 提交操作会异步返回
    virtual void SetCommitCallback(Callback callback);
    /// 获取提交回调
    virtual Callback GetCommitCallback();

    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context);
    /// 获取用户上下文
    virtual void* GetContext();

    /// 获得结果错误码
    virtual const ErrorCode& GetError();

    /// 提交事务
    virtual ErrorCode Commit();

    virtual int64_t GetStartTimestamp() { return start_timestamp_; }
    
    virtual int64_t GetCommitTimestamp() { return commit_timestamp_; }

    virtual void Ack(Table* t, 
                     const std::string& row_key, 
                     const std::string& column_family, 
                     const std::string& qualifier);

    virtual void Notify(Table* t,
                        const std::string& row_key, 
                        const std::string& column_family, 
                        const std::string& qualifier);

    // not support
    virtual void SetIsolation(const IsolationLevel& isolation_level) { abort(); }

    // use default isolation level snapshot 
    virtual IsolationLevel Isolation() { return IsolationLevel::kSnapshot; }

    virtual void SetTimeout(int64_t timeout_ms) { 
        mutation_buffer_.SetTimeOut(timeout_ms); 
    }

    virtual int64_t Timeout() {
        return mutation_buffer_.TimeOut();
    }

public:
    /// 内部读操作回调
    void ReadCallback(RowReaderImpl* reader_impl);
    /// 内部提交回调
    void CommitCallback(RowMutationImpl* mu_impl);
    /// 序列化
    void Serialize(RowMutationSequence* mu_seq);

private:
    // prevent users from reading more than once in one single-row-txn
    bool MarkHasRead();

    void MarkNoRead();

    void InternalNotify();
private:
    Table* table_;
    const std::string row_key_;
    common::ThreadPool* thread_pool_;

    bool has_read_;
    RowReader::Callback user_reader_callback_;
    void* user_reader_context_;
    RowReader::ReadColumnList read_column_list_;
    //               columnfamily          qualifier             timestamp  value
    typedef std::map<std::string, std::map<std::string, std::map<int64_t, std::string>> > ReadResult;
    ReadResult read_result_;
    uint32_t reader_max_versions_;
    int64_t reader_start_timestamp_;
    int64_t reader_end_timestamp_;

    int64_t start_timestamp_;
    int64_t commit_timestamp_;

    RowMutationImpl mutation_buffer_;
    Callback user_commit_callback_;
    void* user_commit_context_;

    std::vector<Cell> notify_cells_;

    mutable Mutex mu_;
};

} // namespace tera

#endif  // TERA_SDK_SINGLE_ROW_TXN_H_
