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

class SingleRowTxn : public Transaction {
public:
    SingleRowTxn(TableImpl* table, const std::string& row_key,
                 common::ThreadPool* thread_pool);
    virtual ~SingleRowTxn();

    /// 提交一个修改操作
    virtual void ApplyMutation(RowMutation* row_mu);
    /// 读取操作
    virtual void Get(RowReader* row_reader);

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

public:
    /// 内部读操作回调
    void ReadCallback(RowReaderImpl* reader_impl);
    /// 提交事务
    virtual void Commit();
    /// 内部提交回调
    void CommitCallback(RowMutationImpl* mu_impl);
    /// 序列化
    void Serialize(RowMutationSequence* mu_seq);

private:
    TableImpl* table_;
    const std::string row_key_;
    common::ThreadPool* thread_pool_;

    bool has_read_;
    RowReader::Callback user_reader_callback_;
    void* user_reader_context_;
    RowReader::ReadColumnList read_column_list_;
    typedef std::map<std::string, std::map<std::string, int64_t> > ReadResult;
    ReadResult read_result_;

    RowMutationImpl mutation_buffer_;
    Callback user_commit_callback_;
    void* user_commit_context_;
};

} // namespace tera

#endif  // TERA_SDK_SINGLE_ROW_TXN_H_
