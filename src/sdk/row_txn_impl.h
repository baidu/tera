// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_ROW_TXN_IMPL_H_
#define  TERA_SDK_ROW_TXN_IMPL_H_

#include <string>

#include "sdk/mutate_impl.h"
#include "sdk/tera.h"

namespace common {
class ThreadPool;
}

namespace tera {

class TableImpl;

class RowTransactionImpl : public Transaction {
    friend void ReadCallbackWrapper(RowReader* row_reader);
    friend void MutateCallbackWrapper(RowMutation* row_mu);
public:
    RowTransactionImpl(TableImpl* table, const std::string& row_key,
                       common::ThreadPool* thread_pool);
    virtual ~RowTransactionImpl();

    /// 提交一个修改操作
    virtual void ApplyMutation(RowMutation* row_mu);
    /// 读取操作
    virtual void Get(RowReader* row_reader);

    /// 设置提交回调, 提交操作会异步返回
    virtual void SetCommitCallback(Callback callback);
    /// 获取提交回调
    virtual Callback GetCommitCallback();
    /// 设置回滚回调, 回滚操作会异步返回
    virtual void SetRollbackCallback(Callback callback);
    /// 获取回滚回调
    virtual Callback GetRollbackCallback();

    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context);
    /// 获取用户上下文
    virtual void* GetContext();

    /// 获得结果错误码
    virtual const ErrorCode& GetError();

public:
    /// 提交事务
    virtual void Commit();
    /// 回滚事务
    virtual void Rollback();

private:
    TableImpl* _table;
    const std::string _row_key;
    Callback _commit_callback;
    Callback _rollback_callback;
    void* _user_context;
    common::ThreadPool* _thread_pool;

    RowMutationImpl _mutation_buffer;
    uint64_t _last_read_sequence;
    uint64_t _read_snapshot_id;
    RowReader::Callback _user_reader_callback;
    void* _user_reader_context;
};

} // namespace tera

#endif  // TERA_SDK_ROW_TXN_IMPL_H_
