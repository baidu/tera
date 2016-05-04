// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_TXN_IMPL_H_
#define  TERA_SDK_TXN_IMPL_H_

#include <string>

#include "sdk/tera.h"

namespace common {
class ThreadPool;
}

namespace tera {

class RowTransactionImpl;
class TableImpl;

class TransactionImpl : public Transaction {
    friend void RowTxnCommitCallbackWrapper(Transaction* row_txn);
    friend void RowTxnRollbackCallbackWrapper(Transaction* row_txn);
public:
    TransactionImpl(common::ThreadPool* thread_pool);

    virtual ~TransactionImpl();

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
    Callback _commit_callback;
    Callback _rollback_callback;
    void* _user_context;
    common::ThreadPool* _thread_pool;

    // only support single row txn
    TableImpl* _table;
    std::string _row_key;
    RowTransactionImpl* _row_txn;
    ErrorCode _ok;
};

} // namespace tera

#endif  // TERA_SDK_TXN_IMPL_H_
