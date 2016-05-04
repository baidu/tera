// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/txn_impl.h"

#include "common/thread_pool.h"
#include "common/base/string_format.h"
#include "io/coding.h"
#include "sdk/read_impl.h"
#include "sdk/row_txn_impl.h"
#include "sdk/table_impl.h"
#include "utils/timer.h"

namespace tera {

TransactionImpl::TransactionImpl(common::ThreadPool* thread_pool)
    : _commit_callback(NULL),
      _rollback_callback(NULL),
      _user_context(NULL),
      _thread_pool(thread_pool),
      _table(NULL),
      _row_txn(NULL) {
}

TransactionImpl::~TransactionImpl() {
    delete _row_txn;
}

/// 提交一个修改操作
void TransactionImpl::ApplyMutation(RowMutation* row_mu) {
    RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(row_mu);
    TableImpl* table = static_cast<TableImpl*>(row_mu_impl->GetTable());
    const std::string& row_key = row_mu_impl->RowKey();
    if (_table == NULL) {
        _table = table;
        _row_key = row_key;
        _row_txn = new RowTransactionImpl(_table, _row_key, _thread_pool);
    }
    if (_table != table || _row_key != row_key) {
        row_mu_impl->SetError(ErrorCode::kNotImpl, "not support multi-tables/rows transaction");
        ThreadPool::Task task = boost::bind(&RowMutationImpl::RunCallback, row_mu_impl);
        _thread_pool->AddTask(task);
        return;
    }
    _row_txn->ApplyMutation(row_mu);
}

/// 读取操作
void TransactionImpl::Get(RowReader* row_reader) {
    RowReaderImpl* row_reader_impl = static_cast<RowReaderImpl*>(row_reader);
    TableImpl* table = static_cast<TableImpl*>(row_reader_impl->GetTable());
    const std::string& row_key = row_reader_impl->RowName();
    if (_table == NULL) {
        _table = table;
        _row_key = row_key;
        _row_txn = new RowTransactionImpl(_table, _row_key, _thread_pool);
    }
    if (_table != table || _row_key != row_key) {
        row_reader_impl->SetError(ErrorCode::kNotImpl, "not support multi-tables/rows transaction");
        ThreadPool::Task task = boost::bind(&RowReaderImpl::RunCallback, row_reader_impl);
        _thread_pool->AddTask(task);
        return;
    }
    _row_txn->Get(row_reader);
}

/// 设置提交回调, 提交操作会异步返回
void TransactionImpl::SetCommitCallback(Callback callback) {
    _commit_callback = callback;
}

/// 获取提交回调
Transaction::Callback TransactionImpl::GetCommitCallback() {
    return _commit_callback;
}

/// 设置回滚回调, 回滚操作会异步返回
void TransactionImpl::SetRollbackCallback(Callback callback) {
    _rollback_callback = callback;
}

/// 获取回滚回调
Transaction::Callback TransactionImpl::GetRollbackCallback() {
    return _rollback_callback;
}

/// 设置用户上下文，可在回调函数中获取
void TransactionImpl::SetContext(void* context) {
    _user_context = context;
}

/// 获取用户上下文
void* TransactionImpl::GetContext() {
    return _user_context;
}

/// 获得结果错误码
const ErrorCode& TransactionImpl::GetError() {
    if (_row_txn != NULL) {
        return _row_txn->GetError();
    }
    return _ok;
}

void RowTxnCommitCallbackWrapper(Transaction* row_txn) {
    TransactionImpl* txn_impl = static_cast<TransactionImpl*>(row_txn->GetContext());
    CHECK_EQ(txn_impl->_row_txn, static_cast<RowTransactionImpl*>(row_txn));
    CHECK_NOTNULL(txn_impl->_commit_callback);
    txn_impl->_commit_callback(txn_impl);
}

/// 提交事务
void TransactionImpl::Commit() {
    if (_row_txn != NULL) {
        if (_commit_callback != NULL) {
            _row_txn->SetCommitCallback(RowTxnCommitCallbackWrapper);
            _row_txn->SetContext(this);
        }
        _row_txn->Commit();
    }
}

void RowTxnRollbackCallbackWrapper(Transaction* row_txn) {
    TransactionImpl* txn_impl = static_cast<TransactionImpl*>(row_txn->GetContext());
    CHECK_EQ(txn_impl->_row_txn, static_cast<RowTransactionImpl*>(row_txn));
    CHECK_NOTNULL(txn_impl->_rollback_callback);
    txn_impl->_rollback_callback(txn_impl);
}

/// 回滚事务
void TransactionImpl::Rollback() {
    if (_row_txn != NULL) {
        if (_rollback_callback != NULL) {
            _row_txn->SetRollbackCallback(RowTxnRollbackCallbackWrapper);
            _row_txn->SetContext(this);
        }
        _row_txn->Rollback();
    }
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
