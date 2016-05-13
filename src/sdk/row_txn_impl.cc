// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/row_txn_impl.h"

#include <boost/bind.hpp>

#include "common/thread_pool.h"
#include "common/base/string_format.h"

#include "io/coding.h"
#include "sdk/read_impl.h"
#include "sdk/table_impl.h"
#include "types.h"
#include "utils/timer.h"

namespace tera {

RowTransactionImpl::RowTransactionImpl(TableImpl* table, const std::string& row_key,
                                       common::ThreadPool* thread_pool)
    : _table(table),
      _row_key(row_key),
      _commit_callback(NULL),
      _rollback_callback(NULL),
      _user_context(NULL),
      _thread_pool(thread_pool),
      _mutation_buffer(table, row_key),
      _user_reader_callback(NULL),
      _user_reader_context(NULL) {
}

RowTransactionImpl::~RowTransactionImpl() {
}

/// 提交一个修改操作
void RowTransactionImpl::ApplyMutation(RowMutation* row_mu) {
    RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(row_mu);
    _mutation_buffer.Concatenate(*row_mu_impl);

    row_mu_impl->SetError(ErrorCode::kOK);
    if (row_mu->IsAsync()) {
        ThreadPool::Task task = boost::bind(&RowMutationImpl::RunCallback, row_mu_impl);
        _thread_pool->AddTask(task);
    }
}

/// 读取操作
void RowTransactionImpl::Get(RowReader* row_reader) {
    _table->Get(row_reader);
}

/// 设置提交回调, 提交操作会异步返回
void RowTransactionImpl::SetCommitCallback(Callback callback) {
    _commit_callback = callback;
}

/// 获取提交回调
Transaction::Callback RowTransactionImpl::GetCommitCallback() {
    return _commit_callback;
}

/// 设置回滚回调, 回滚操作会异步返回
void RowTransactionImpl::SetRollbackCallback(Callback callback) {
    _rollback_callback = callback;
}

/// 获取回滚回调
Transaction::Callback RowTransactionImpl::GetRollbackCallback() {
    return _rollback_callback;
}

/// 设置用户上下文，可在回调函数中获取
void RowTransactionImpl::SetContext(void* context) {
    _user_context = context;
}

/// 获取用户上下文
void* RowTransactionImpl::GetContext() {
    return _user_context;
}

/// 获得结果错误码
const ErrorCode& RowTransactionImpl::GetError() {
    return _mutation_buffer.GetError();
}

void MutateCallbackWrapper(RowMutation* row_mu) {
    RowTransactionImpl* row_txn = static_cast<RowTransactionImpl*>(row_mu->GetContext());
    CHECK_EQ(&row_txn->_mutation_buffer, row_mu);
    CHECK_NOTNULL(row_txn->_commit_callback);
    row_txn->_commit_callback(row_txn);
}

/// 提交事务
void RowTransactionImpl::Commit() {
    if (_mutation_buffer.MutationNum() > 0) {
        if (_commit_callback != NULL) {
            // use our callback wrapper
            _mutation_buffer.SetCallBack(MutateCallbackWrapper);
            _mutation_buffer.SetContext(this);
        }
        _table->ApplyMutation(&_mutation_buffer);
    } else {
        if (_commit_callback != NULL) {
            ThreadPool::Task task = boost::bind(_commit_callback, this);
            _thread_pool->AddTask(task);
        }
    }
}

/// 回滚事务
void RowTransactionImpl::Rollback() {
    // nothing need to do
    if (_rollback_callback != NULL) {
        ThreadPool::Task task = boost::bind(_rollback_callback, this);
        _thread_pool->AddTask(task);
    }
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
