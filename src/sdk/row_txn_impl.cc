// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/row_txn_impl.h"

#include "common/thread_pool.h"
#include "common/base/string_format.h"
#include "io/coding.h"
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
      _last_read_sequence(0),
      _read_snapshot_id(0),
      _user_reader_callback(NULL),
      _user_reader_context(NULL) {
}

RowTransactionImpl::~RowTransactionImpl() {
}

/// 提交一个修改操作
void RowTransactionImpl::ApplyMutation(RowMutation* row_mu) {
    RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(row_mu);
    _mutation_buffer.Concatenate(row_mu_impl);

    row_mu_impl->SetError(ErrorCode::kOK);
    ThreadPool::Task task = boost::bind(&RowMutationImpl::RunCallback, row_mu_impl);
    _thread_pool->AddTask(task);
}

void ReadCallbackWrapper(RowReader* row_reader) {
    RowReaderImpl* reader_impl = static_cast<RowMutationImpl*>(row_reader);
    RowTransactionImpl* txn_impl = reader_impl->GetContext();

    // restore user's callback & context
    reader_impl->SetCallBack(txn_impl->_user_reader_callback);
    reader_impl->SetContext(txn_impl->_user_reader_context);

    // save last sequence
    txn_impl->_last_read_sequence = reader_impl->GetLastSequence();
    txn_impl->_read_snapshot_id = reader_impl->GetSnapshot();
    reader_impl->RunCallback();
}

/// 读取操作
void RowTransactionImpl::Get(RowReader* row_reader) {
    RowReaderImpl* reader_impl = static_cast<RowReaderImpl*>(row_reader);
    bool is_async = reader_impl->IsAsync();

    // save user's callback & context
    _user_reader_callback = reader_impl->GetCallBack();
    _user_reader_context = reader_impl->GetContext();

    // use our callback wrapper
    reader_impl->SetCallBack(ReadCallbackWrapper);
    reader_impl->SetContext(this);

    if (_read_snapshot_id == 0) {
        reader_impl->SetGetSnapshot(true);
    } else {
        reader_impl->SetSnapshot(_read_snapshot_id);
    }

    _table->Get(row_reader);
    if (!is_async) {
        reader_impl->Wait();
    }
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
    return _error_code;
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
